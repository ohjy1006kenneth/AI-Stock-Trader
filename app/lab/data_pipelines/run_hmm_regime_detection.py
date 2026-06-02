"""Modal-ready Layer 1.5 HMM regime detection runner.

This entrypoint keeps HMM execution in the cloud/lab surface. It reads Layer 0
R2 archives, emits market-wide regime probabilities, and writes a pipeline
manifest. The pure HMM implementation remains in `core.features`.
"""
from __future__ import annotations

import argparse
import importlib
import io
import json
import math
import os
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Protocol

from loguru import logger

if TYPE_CHECKING:
    import pandas as pd


def _resolve_repo_root() -> Path:
    """Return the repository root for local runs and Modal-mounted runs."""
    env_root = os.getenv("AI_STOCK_TRADER_REPO_ROOT")
    if env_root:
        return Path(env_root).resolve()
    resolved = Path(__file__).resolve()
    return resolved.parents[3] if len(resolved.parents) > 3 else resolved.parent


_REPO_ROOT = _resolve_repo_root()
sys.path.insert(0, str(_REPO_ROOT))

from core.contracts.schemas import PipelineManifestRecord, RunStatus  # noqa: E402
from core.features.loaders import load_macro_frame, load_ohlcv_frame  # noqa: E402
from core.features.regime_detection import (  # noqa: E402
    HMM_REGIME_COLUMNS,
    HMMRegimeConfig,
    HMMRegimeReadiness,
    fit_and_emit_hmm_regime_features,
    inspect_hmm_regime_readiness,
    validate_hmm_regime_probabilities,
)
from core.features.regime_training import build_hmm_training_frame  # noqa: E402
from services.modal.secrets import (  # noqa: E402
    SIMFIN_MODAL_ENV_FILE,
    SIMFIN_MODAL_ENV_KEYS,
    build_modal_secrets,
)
from services.r2.paths import (  # noqa: E402
    layer1_regime_path,
    pipeline_manifest_path,
    raw_price_path,
)
from services.r2.writer import R2Writer  # noqa: E402

MODAL_CONFIG_PATH = _REPO_ROOT / "config" / "modal.json"
REGIME_STAGE = "layer1_5_regime"
MODAL_REPO_ROOT = "/workspace/AI-Stock-Trader"
HMM_MACRO_CONTEXT_LOOKBACK_BDAYS = 252


class ObjectStore(Protocol):
    """Object-store operations required by the HMM regime runner."""

    def put_object(self, key: str, data: bytes | str) -> None:
        """Write an object to storage."""

    def get_object(self, key: str) -> bytes:
        """Read an object from storage."""

    def list_keys(self, prefix: str) -> list[str]:
        """List object keys beneath a prefix."""


@dataclass(frozen=True)
class HMMRegimePipelineConfig:
    """Configuration for one Layer 1.5 HMM regime run."""

    run_id: str
    train_end_date: str
    inference_dates: tuple[str, ...]
    train_start_date: str | None = None
    benchmark_ticker: str = "SPY"
    max_iterations: int = 100
    min_training_rows: int = 30

    def __post_init__(self) -> None:
        """Validate run identifiers, dates, and HMM fit limits."""
        if not self.run_id.strip():
            raise ValueError("run_id cannot be empty")
        _validate_iso_date(self.train_end_date, "train_end_date")
        if self.train_start_date is not None:
            _validate_iso_date(self.train_start_date, "train_start_date")
            if self.train_start_date >= self.train_end_date:
                raise ValueError("train_start_date must be before train_end_date")
        if not self.inference_dates:
            raise ValueError("inference_dates must not be empty")
        for inference_date in self.inference_dates:
            _validate_iso_date(inference_date, "inference_dates")
            if inference_date <= self.train_end_date:
                raise ValueError("inference_dates must be after train_end_date")
        if not self.benchmark_ticker.strip():
            raise ValueError("benchmark_ticker cannot be empty")
        if self.max_iterations <= 0:
            raise ValueError("max_iterations must be positive")
        if self.min_training_rows <= 0:
            raise ValueError("min_training_rows must be positive")


@dataclass(frozen=True)
class HMMRegimePipelineResult:
    """Storage summary for one completed HMM regime run."""

    run_id: str
    output_key: str
    manifest_key: str
    training_rows: int
    complete_training_rows: int
    regime_rows: int


@dataclass(frozen=True)
class ModalRuntimeConfig:
    """Modal app configuration loaded from repository config."""

    hmm_regime_app_name: str
    r2_secret_name: str
    timeout_seconds: int
    python_version: str = "3.11"
    requirements_path: str = "requirements/modal.txt"

    def __post_init__(self) -> None:
        """Validate Modal runtime settings loaded from repository config."""
        if not self.hmm_regime_app_name.strip():
            raise ValueError("hmm_regime_app_name cannot be empty")
        if not self.r2_secret_name.strip():
            raise ValueError("r2_secret_name cannot be empty")
        if self.timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        if not self.python_version.strip():
            raise ValueError("python_version cannot be empty")
        if not self.requirements_path.strip():
            raise ValueError("requirements_path cannot be empty")


def run_hmm_regime_detection(
    config: HMMRegimePipelineConfig,
    *,
    writer: ObjectStore | None = None,
) -> HMMRegimePipelineResult:
    """Run Layer 1.5 HMM regime detection against R2 archives."""
    active_writer = writer or R2Writer()
    started_at = datetime.now(UTC)
    output_keys_by_date = {
        date_text: hmm_regime_output_path(config.run_id, date_text)
        for date_text in config.inference_dates
    }
    output_key = output_keys_by_date[config.inference_dates[0]]
    manifest_key = pipeline_manifest_path(REGIME_STAGE, config.run_id)
    metadata: dict[str, object] = {
        "benchmark_ticker": config.benchmark_ticker.upper(),
        "train_start_date": config.train_start_date,
        "train_end_date": config.train_end_date,
        "inference_dates": list(config.inference_dates),
        "output_key": output_key,
        "output_keys_by_date": output_keys_by_date,
    }

    try:
        benchmark = load_ohlcv_frame(
            config.benchmark_ticker,
            writer=active_writer,  # type: ignore[arg-type]
        )
        macro_start_date = _macro_context_start_date(
            benchmark,
            train_start_date=config.train_start_date,
        )
        macro_end_date = max(config.inference_dates)
        macro = load_macro_frame(  # type: ignore[arg-type]
            writer=active_writer,
            start_date=macro_start_date,
            end_date=macro_end_date,
        )
        training_frame = build_hmm_training_frame(benchmark, macro)
        hmm_config = HMMRegimeConfig(
            max_iterations=config.max_iterations,
            min_training_rows=config.min_training_rows,
        )
        readiness = inspect_hmm_regime_readiness(
            training_frame,
            train_start_date=config.train_start_date,
            train_end_date=config.train_end_date,
            inference_dates=list(config.inference_dates) or None,
            config=hmm_config,
        )
        if readiness.can_fit_model:
            regime_frame = fit_and_emit_hmm_regime_features(
                training_frame,
                train_start_date=config.train_start_date,
                train_end_date=config.train_end_date,
                inference_dates=list(config.inference_dates) or None,
                config=hmm_config,
            )
        else:
            regime_frame = _empty_regime_frame(readiness.inference_dates)
        regime_frame = _with_regime_readiness_columns(regime_frame, readiness=readiness)
        probability_errors = validate_hmm_regime_probabilities(regime_frame)
        if probability_errors:
            raise ValueError(
                "Invalid HMM regime probabilities emitted: "
                + "; ".join(probability_errors[:5])
            )
        _write_regime_outputs_by_date(
            writer=active_writer,
            frame=regime_frame,
            output_keys_by_date=output_keys_by_date,
        )
        metadata.update(
            {
                "training_rows": readiness.training_rows,
                "complete_training_rows": readiness.complete_training_rows,
                "macro_load_start_date": macro_start_date,
                "macro_load_end_date": macro_end_date,
                "dropped_feature_columns": list(readiness.dropped_feature_columns),
                "ready_inference_dates": list(readiness.complete_inference_dates),
                "inference_feature_gaps": {
                    date_text: list(columns)
                    for date_text, columns in sorted(
                        readiness.incomplete_inference_feature_gaps.items()
                    )
                },
                "regime_readiness_by_date": _regime_readiness_by_date(regime_frame),
                "warning_inference_dates": [
                    date_text
                    for date_text in readiness.inference_dates
                    if (not readiness.can_fit_model)
                    or date_text in readiness.incomplete_inference_feature_gaps
                ],
                "regime_layer2_ready": bool(
                    readiness.can_fit_model
                    and len(readiness.complete_inference_dates) == len(readiness.inference_dates)
                ),
                "regime_rows": len(regime_frame),
            }
        )
        _write_manifest(
            writer=active_writer,
            key=manifest_key,
            config=config,
            status=RunStatus.COMPLETED,
            started_at=started_at,
            output_key=output_key,
            metadata=metadata,
        )
        logger.info("Layer 1.5 HMM regime run complete: {}", output_key)
        return HMMRegimePipelineResult(
            run_id=config.run_id,
            output_key=output_key,
            manifest_key=manifest_key,
            training_rows=readiness.training_rows,
            complete_training_rows=readiness.complete_training_rows,
            regime_rows=len(regime_frame),
        )
    except Exception as exc:
        metadata["error"] = {"type": type(exc).__name__, "message": str(exc)}
        _write_manifest(
            writer=active_writer,
            key=manifest_key,
            config=config,
            status=RunStatus.FAILED,
            started_at=started_at,
            output_key=output_key,
            metadata=metadata,
        )
        logger.exception("Layer 1.5 HMM regime run failed")
        raise


def _macro_context_start_date(
    benchmark: pd.DataFrame,
    *,
    train_start_date: str | None,
) -> str | None:
    """Return the earliest macro snapshot date needed for one HMM window.

    The HMM training rows are optionally bounded by `train_start_date`, but the
    macro feature family also needs an extra one-year business-day context to
    compute lagged change features such as CPI year-over-year safely.
    """
    if train_start_date is None or len(benchmark.index) == 0:
        return None

    date_values = benchmark["date"].astype(str).tolist()
    try:
        anchor_index = next(
            index for index, date_text in enumerate(date_values) if date_text >= train_start_date
        )
    except StopIteration:
        return date_values[0]
    start_index = max(0, anchor_index - HMM_MACRO_CONTEXT_LOOKBACK_BDAYS)
    return date_values[start_index]


def hmm_regime_output_path(run_id: str, as_of_date: str) -> str:
    """Return the canonical R2 output key for one HMM regime date/run."""
    return layer1_regime_path(as_of_date, run_id)


def _write_regime_outputs_by_date(
    *,
    writer: ObjectStore,
    frame: pd.DataFrame,
    output_keys_by_date: dict[str, str],
) -> None:
    """Write one colocated regime parquet artifact per inference date."""
    for date_text, output_key in output_keys_by_date.items():
        date_frame = frame[frame["date"].astype(str) == date_text].reset_index(drop=True)
        writer.put_object(output_key, _frame_to_parquet_bytes(date_frame))


def load_modal_runtime_config(path: Path = MODAL_CONFIG_PATH) -> ModalRuntimeConfig:
    """Load Modal app and secret names from repository config."""
    payload = json.loads(path.read_text(encoding="utf-8"))
    return ModalRuntimeConfig(
        hmm_regime_app_name=str(payload["hmm_regime_app_name"]),
        r2_secret_name=str(payload["r2_secret_name"]),
        timeout_seconds=int(payload["hmm_regime_timeout_seconds"]),
        python_version=str(payload.get("python_version", "3.11")),
        requirements_path=str(payload.get("requirements_path", "requirements/modal.txt")),
    )


def _empty_regime_frame(inference_dates: Sequence[str]) -> pd.DataFrame:
    """Return one explicit null regime row for each requested inference date."""
    try:
        import pandas as pd
    except ModuleNotFoundError as exc:
        raise ModuleNotFoundError(
            "pandas and pyarrow are required to write HMM regime outputs."
        ) from exc

    rows = [
        {
            "date": date_text,
            "regime_label": None,
            "regime_confidence": math.nan,
            "regime_prob_bear": math.nan,
            "regime_prob_sideways": math.nan,
            "regime_prob_bull": math.nan,
        }
        for date_text in inference_dates
    ]
    return pd.DataFrame(rows, columns=list(HMM_REGIME_COLUMNS))


def _with_regime_readiness_columns(
    frame: pd.DataFrame,
    *,
    readiness: HMMRegimeReadiness,
) -> pd.DataFrame:
    """Annotate the regime artifact with per-date readiness diagnostics."""
    try:
        import pandas as pd
    except ModuleNotFoundError as exc:
        raise ModuleNotFoundError(
            "pandas and pyarrow are required to write HMM regime outputs."
        ) from exc
    if not isinstance(frame, pd.DataFrame):
        raise TypeError("frame must be a pandas DataFrame")

    annotated = frame.copy()
    annotated["regime_required_for_layer2"] = False
    annotated["regime_readiness_status"] = "warning"
    annotated["regime_readiness_reason"] = "insufficient_training_history"
    annotated["regime_missing_features"] = ""
    annotated["regime_probability_sum"] = math.nan
    annotated["training_rows"] = readiness.training_rows
    annotated["complete_training_rows"] = readiness.complete_training_rows
    annotated["min_training_rows"] = readiness.min_training_rows

    for index, row in annotated.iterrows():
        date_text = str(row["date"])
        missing_features = readiness.incomplete_inference_feature_gaps.get(date_text, ())
        if not readiness.can_fit_model:
            reason = "insufficient_training_history"
        elif missing_features:
            reason = "incomplete_inference_features"
        else:
            reason = "ready"
        annotated.loc[index, "regime_readiness_reason"] = reason
        annotated.loc[index, "regime_missing_features"] = ",".join(missing_features)
        if reason == "ready":
            annotated.loc[index, "regime_required_for_layer2"] = True
            annotated.loc[index, "regime_readiness_status"] = "ready"
        else:
            annotated.loc[index, "regime_required_for_layer2"] = False
            annotated.loc[index, "regime_readiness_status"] = "warning"
        probability_values = [
            row.get("regime_prob_bear"),
            row.get("regime_prob_sideways"),
            row.get("regime_prob_bull"),
        ]
        if all(value is not None and not pd.isna(value) for value in probability_values):
            annotated.loc[index, "regime_probability_sum"] = float(sum(probability_values))
    return annotated


def _regime_readiness_by_date(frame: pd.DataFrame) -> dict[str, dict[str, object]]:
    """Return manifest-ready per-date readiness metadata for one regime artifact."""
    pandas_module = importlib.import_module("pandas")
    if not isinstance(frame, pandas_module.DataFrame):
        raise TypeError("frame must be a pandas DataFrame")

    readiness_by_date: dict[str, dict[str, object]] = {}
    for row in frame.to_dict(orient="records"):
        date_text = str(row["date"])
        readiness_by_date[date_text] = {
            "status": str(row.get("regime_readiness_status") or ""),
            "reason": str(row.get("regime_readiness_reason") or ""),
            "required_for_layer2": bool(row.get("regime_required_for_layer2")),
            "missing_features": [
                item
                for item in str(row.get("regime_missing_features") or "").split(",")
                if item
            ],
            "probability_sum": (
                None
                if pandas_module.isna(row.get("regime_probability_sum"))
                else float(row["regime_probability_sum"])
            ),
        }
    return readiness_by_date


def _frame_to_parquet_bytes(frame: object) -> bytes:
    """Serialize a pandas DataFrame to Parquet bytes."""
    try:
        import pandas as pd

        importlib.import_module("pyarrow")
    except ModuleNotFoundError as exc:
        raise ModuleNotFoundError(
            "pandas and pyarrow are required to write HMM regime outputs."
        ) from exc

    if not isinstance(frame, pd.DataFrame):
        raise TypeError("frame must be a pandas DataFrame")
    buffer = io.BytesIO()
    frame.to_parquet(buffer, index=False)
    return buffer.getvalue()


def _write_manifest(
    *,
    writer: ObjectStore,
    key: str,
    config: HMMRegimePipelineConfig,
    status: RunStatus,
    started_at: datetime,
    output_key: str,
    metadata: dict[str, object],
) -> None:
    """Write a pipeline manifest for one HMM regime run."""
    manifest = PipelineManifestRecord(
        run_id=config.run_id,
        stage=REGIME_STAGE,
        status=status,
        started_at=started_at,
        finished_at=datetime.now(UTC),
        input_path=f"{raw_price_path(config.benchmark_ticker)},raw/macro/",
        output_path=output_key,
        metadata=metadata,
    )
    writer.put_object(key, manifest.model_dump_json())


def _validate_iso_date(value: str, field_name: str) -> None:
    """Validate a YYYY-MM-DD date string."""
    try:
        datetime.strptime(value, "%Y-%m-%d")
    except ValueError as exc:
        raise ValueError(f"{field_name} must be YYYY-MM-DD") from exc


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """Parse CLI arguments for local or Modal-triggered runs."""
    parser = argparse.ArgumentParser(description="Run Layer 1.5 HMM regime detection.")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--train-start-date", default=None, metavar="YYYY-MM-DD")
    parser.add_argument("--train-end-date", required=True, metavar="YYYY-MM-DD")
    parser.add_argument("--inference-date", action="append", default=[], metavar="YYYY-MM-DD")
    parser.add_argument("--benchmark-ticker", default="SPY")
    parser.add_argument("--max-iterations", type=int, default=100)
    parser.add_argument("--min-training-rows", type=int, default=30)
    return parser.parse_args(argv)


def _config_from_args(args: argparse.Namespace) -> HMMRegimePipelineConfig:
    """Build a validated pipeline config from CLI arguments."""
    return HMMRegimePipelineConfig(
        run_id=args.run_id,
        train_start_date=args.train_start_date,
        train_end_date=args.train_end_date,
        inference_dates=tuple(args.inference_date),
        benchmark_ticker=args.benchmark_ticker.strip().upper(),
        max_iterations=args.max_iterations,
        min_training_rows=args.min_training_rows,
    )


def main(argv: Sequence[str] | None = None) -> int:
    """Run HMM regime detection from the local command line."""
    result = run_hmm_regime_detection(_config_from_args(_parse_args(argv)))
    logger.info("Manifest written to {}", result.manifest_key)
    return 0


def modal_main(
    run_id: str,
    train_end_date: str,
    inference_dates: str = "",
    train_start_date: str | None = None,
    benchmark_ticker: str = "SPY",
    max_iterations: int = 100,
    min_training_rows: int = 30,
) -> None:
    """Submit an HMM regime run to Modal from the local CLI."""
    parsed_inference_dates = [item.strip() for item in inference_dates.split(",") if item.strip()]
    globals()["modal_run_hmm_regime_detection"].remote(
        run_id=run_id,
        train_start_date=train_start_date,
        train_end_date=train_end_date,
        inference_dates=",".join(parsed_inference_dates),
        benchmark_ticker=benchmark_ticker.strip().upper(),
        max_iterations=max_iterations,
        min_training_rows=min_training_rows,
    )


def _modal_run_hmm_regime_detection_entry(
    run_id: str,
    train_end_date: str,
    inference_dates: str = "",
    train_start_date: str | None = None,
    benchmark_ticker: str = "SPY",
    max_iterations: int = 100,
    min_training_rows: int = 30,
) -> dict[str, object]:
    """Run Layer 1.5 HMM regime detection on Modal."""
    parsed_inference_dates = tuple(
        item.strip() for item in inference_dates.split(",") if item.strip()
    )
    result = run_hmm_regime_detection(
        HMMRegimePipelineConfig(
            run_id=run_id,
            train_start_date=train_start_date,
            train_end_date=train_end_date,
            inference_dates=parsed_inference_dates,
            benchmark_ticker=benchmark_ticker.strip().upper(),
            max_iterations=max_iterations,
            min_training_rows=min_training_rows,
        )
    )
    return {
        "run_id": result.run_id,
        "output_key": result.output_key,
        "manifest_key": result.manifest_key,
        "training_rows": result.training_rows,
        "complete_training_rows": result.complete_training_rows,
        "regime_rows": result.regime_rows,
    }


def _define_modal_app() -> object | None:
    """Create the Modal app when the modal package is installed."""
    try:
        modal = importlib.import_module("modal")
    except ModuleNotFoundError:
        return None

    runtime = load_modal_runtime_config()
    image = _build_modal_image(modal, runtime)
    app = modal.App(runtime.hmm_regime_app_name)
    secrets = build_modal_secrets(
        modal,
        named_secret_names=(runtime.r2_secret_name,),
        env_file=SIMFIN_MODAL_ENV_FILE,
        env_keys=SIMFIN_MODAL_ENV_KEYS,
    )

    modal_run_hmm_regime_detection = app.function(
        image=image,
        secrets=secrets,
        timeout=runtime.timeout_seconds,
    )(_modal_run_hmm_regime_detection_entry)

    app.local_entrypoint()(modal_main)
    globals()["modal_run_hmm_regime_detection"] = modal_run_hmm_regime_detection
    return app


def _build_modal_image(modal_module: object, runtime: ModalRuntimeConfig):
    """Build the Modal image while preserving local `-r base.txt` includes."""
    requirements_path = Path(runtime.requirements_path)
    requirements_dir = requirements_path.parent
    remote_requirements_path = f"{MODAL_REPO_ROOT}/{requirements_path.as_posix()}"
    return (
        modal_module.Image.debian_slim(python_version=runtime.python_version)
        .add_local_dir(_REPO_ROOT / "app", f"{MODAL_REPO_ROOT}/app", copy=True)
        .add_local_dir(_REPO_ROOT / "core", f"{MODAL_REPO_ROOT}/core", copy=True)
        .add_local_dir(_REPO_ROOT / "services", f"{MODAL_REPO_ROOT}/services", copy=True)
        .add_local_dir(_REPO_ROOT / "config", f"{MODAL_REPO_ROOT}/config", copy=True)
        .add_local_dir(
            _REPO_ROOT / requirements_dir,
            f"{MODAL_REPO_ROOT}/{requirements_dir.as_posix()}",
            copy=True,
        )
        .env(
            {
                "AI_STOCK_TRADER_REPO_ROOT": MODAL_REPO_ROOT,
                "PYTHONPATH": MODAL_REPO_ROOT,
            }
        )
        .workdir(MODAL_REPO_ROOT)
        .run_commands(f"python -m pip install -r {remote_requirements_path}")
    )


app = _define_modal_app()


if __name__ == "__main__":
    sys.exit(main())
