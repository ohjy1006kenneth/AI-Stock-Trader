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
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Protocol

from loguru import logger

_REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(_REPO_ROOT))

from core.contracts.schemas import PipelineManifestRecord, RunStatus  # noqa: E402
from core.features.loaders import load_macro_frame, load_ohlcv_frame  # noqa: E402
from core.features.regime_detection import (  # noqa: E402
    HMMRegimeConfig,
    fit_and_emit_hmm_regime_features,
)
from core.features.regime_training import build_hmm_training_frame  # noqa: E402
from services.r2.paths import build_r2_key, pipeline_manifest_path, raw_price_path  # noqa: E402
from services.r2.writer import R2Writer  # noqa: E402

MODAL_CONFIG_PATH = _REPO_ROOT / "config" / "modal.json"
REGIME_STAGE = "layer1_5_regime"


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
    output_key = hmm_regime_output_path(config.run_id)
    manifest_key = pipeline_manifest_path(REGIME_STAGE, config.run_id)
    metadata: dict[str, object] = {
        "benchmark_ticker": config.benchmark_ticker.upper(),
        "train_start_date": config.train_start_date,
        "train_end_date": config.train_end_date,
        "inference_dates": list(config.inference_dates),
        "output_key": output_key,
    }

    try:
        benchmark = load_ohlcv_frame(config.benchmark_ticker, writer=active_writer)  # type: ignore[arg-type]
        macro = load_macro_frame(writer=active_writer)  # type: ignore[arg-type]
        training_frame = build_hmm_training_frame(benchmark, macro)
        regime_frame = fit_and_emit_hmm_regime_features(
            training_frame,
            train_start_date=config.train_start_date,
            train_end_date=config.train_end_date,
            inference_dates=list(config.inference_dates) or None,
            config=HMMRegimeConfig(
                max_iterations=config.max_iterations,
                min_training_rows=config.min_training_rows,
            ),
        )
        active_writer.put_object(output_key, _frame_to_parquet_bytes(regime_frame))
        complete_training_rows = int(training_frame["is_complete"].astype(bool).sum())
        metadata.update(
            {
                "training_rows": len(training_frame),
                "complete_training_rows": complete_training_rows,
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
            training_rows=len(training_frame),
            complete_training_rows=complete_training_rows,
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


def hmm_regime_output_path(run_id: str) -> str:
    """Return the canonical R2 output key for one HMM regime run."""
    return build_r2_key("features", "layer1_5", "regime", f"{run_id}.parquet")


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
        inference_dates=parsed_inference_dates,
        benchmark_ticker=benchmark_ticker.strip().upper(),
        max_iterations=max_iterations,
        min_training_rows=min_training_rows,
    )


def _define_modal_app() -> object | None:
    """Create the Modal app when the modal package is installed."""
    try:
        modal = importlib.import_module("modal")
    except ModuleNotFoundError:
        return None

    runtime = load_modal_runtime_config()
    image = modal.Image.debian_slim(
        python_version=runtime.python_version
    ).pip_install_from_requirements(runtime.requirements_path)
    app = modal.App(runtime.hmm_regime_app_name)

    @app.function(
        image=image,
        secrets=[modal.Secret.from_name(runtime.r2_secret_name)],
        timeout=runtime.timeout_seconds,
        serialized=True,
    )
    def modal_run_hmm_regime_detection(
        run_id: str,
        train_end_date: str,
        inference_dates: list[str],
        train_start_date: str | None = None,
        benchmark_ticker: str = "SPY",
        max_iterations: int = 100,
        min_training_rows: int = 30,
    ) -> dict[str, object]:
        """Run Layer 1.5 HMM regime detection on Modal."""
        result = run_hmm_regime_detection(
            HMMRegimePipelineConfig(
                run_id=run_id,
                train_start_date=train_start_date,
                train_end_date=train_end_date,
                inference_dates=tuple(inference_dates),
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

    app.local_entrypoint()(modal_main)
    globals()["modal_run_hmm_regime_detection"] = modal_run_hmm_regime_detection
    return app


app = _define_modal_app()


if __name__ == "__main__":
    sys.exit(main())
