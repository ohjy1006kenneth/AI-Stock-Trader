"""Modal-ready daily Layer 1 feature runner driven by Layer 0 manifests."""
from __future__ import annotations

import argparse
import csv
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

from app.lab.data_pipelines.backfill_layer1 import (  # noqa: E402
    Layer1BackfillConfig,
    backfill_layer1,
)
from core.contracts.schemas import PipelineManifestRecord, RunStatus  # noqa: E402
from services.r2.paths import (  # noqa: E402
    build_r2_key,
    pipeline_manifest_path,
    raw_universe_path,
)
from services.r2.writer import R2Writer  # noqa: E402

LAYER1_STAGE = "layer1"
MODAL_CONFIG_PATH = _REPO_ROOT / "config" / "modal.json"
_modal_run_daily_layer1: ModalRemoteFunction | None = None


class ObjectStore(Protocol):
    """Object-store operations required by the daily Layer 1 runner."""

    def put_object(self, key: str, data: bytes | str) -> None:
        """Write an object to storage."""

    def get_object(self, key: str) -> bytes:
        """Read an object from storage."""

    def exists(self, key: str) -> bool:
        """Return True when the object exists."""


class ModalRemoteFunction(Protocol):
    """Minimal Modal remote-call surface used by the local entrypoint."""

    def remote(
        self,
        *,
        run_id: str,
        as_of_date: str,
        layer0_run_id: str,
        benchmark_ticker: str = "SPY",
    ) -> None:
        """Submit the configured Modal function asynchronously."""


@dataclass(frozen=True)
class DailyLayer1PipelineConfig:
    """Configuration for one daily Layer 1 feature-generation run."""

    run_id: str
    as_of_date: str
    layer0_run_id: str
    benchmark_ticker: str = "SPY"

    def __post_init__(self) -> None:
        """Validate run identifiers and the target as-of date."""
        if not self.run_id.strip():
            raise ValueError("run_id cannot be empty")
        if not self.layer0_run_id.strip():
            raise ValueError("layer0_run_id cannot be empty")
        _validate_iso_date(self.as_of_date, "as_of_date")
        if not self.benchmark_ticker.strip():
            raise ValueError("benchmark_ticker cannot be empty")


@dataclass(frozen=True)
class DailyLayer1PipelineResult:
    """Storage summary for one completed daily Layer 1 run."""

    run_id: str
    as_of_date: str
    manifest_key: str
    upstream_manifest_key: str
    backfill_manifest_key: str
    tickers_requested: int
    tickers_processed: int


@dataclass(frozen=True)
class ModalRuntimeConfig:
    """Modal app configuration loaded from repository config."""

    app_name: str
    r2_secret_name: str
    timeout_seconds: int


def run_daily_layer1(
    config: DailyLayer1PipelineConfig,
    *,
    writer: ObjectStore | None = None,
) -> DailyLayer1PipelineResult:
    """Run the daily Layer 1 feature job from Layer 0 R2 archives."""
    active_writer = writer or R2Writer()
    started_at = datetime.now(UTC)
    manifest_key = pipeline_manifest_path(LAYER1_STAGE, config.run_id)
    upstream_manifest_key = pipeline_manifest_path("layer0", config.layer0_run_id)
    metadata: dict[str, object] = {
        "as_of_date": config.as_of_date,
        "layer0_run_id": config.layer0_run_id,
        "layer0_manifest_key": upstream_manifest_key,
        "universe_key": raw_universe_path(config.as_of_date),
        "benchmark_ticker": config.benchmark_ticker.upper(),
    }

    try:
        upstream_manifest = _load_required_layer0_manifest(
            writer=active_writer,
            key=upstream_manifest_key,
            as_of_date=config.as_of_date,
        )
        tickers = _load_eligible_tickers(active_writer, config.as_of_date)
        if not tickers:
            raise ValueError("No eligible universe tickers found for daily Layer 1 run")

        backfill_result = backfill_layer1(
            Layer1BackfillConfig(
                run_id=f"{config.run_id}-backfill",
                tickers=tuple(tickers),
                benchmark_ticker=config.benchmark_ticker.upper(),
            ),
            writer=active_writer,
        )
        metadata.update(
            {
                "tickers_requested": len(tickers),
                "tickers_processed": backfill_result.tickers_processed,
                "ticker_files_written": backfill_result.ticker_files_written,
                "feature_rows_written": backfill_result.feature_rows_written,
                "backfill_manifest_key": backfill_result.manifest_key,
                "layer0_finished_at": upstream_manifest.finished_at.isoformat()
                if upstream_manifest.finished_at is not None
                else None,
            }
        )
        _write_manifest(
            writer=active_writer,
            key=manifest_key,
            config=config,
            status=RunStatus.COMPLETED,
            started_at=started_at,
            metadata=metadata,
        )
        logger.info("Daily Layer 1 feature generation complete: {}", manifest_key)
        return DailyLayer1PipelineResult(
            run_id=config.run_id,
            as_of_date=config.as_of_date,
            manifest_key=manifest_key,
            upstream_manifest_key=upstream_manifest_key,
            backfill_manifest_key=backfill_result.manifest_key,
            tickers_requested=len(tickers),
            tickers_processed=backfill_result.tickers_processed,
        )
    except Exception as exc:
        metadata["error"] = {"type": type(exc).__name__, "message": str(exc)}
        _write_manifest(
            writer=active_writer,
            key=manifest_key,
            config=config,
            status=RunStatus.FAILED,
            started_at=started_at,
            metadata=metadata,
        )
        logger.exception("Daily Layer 1 feature generation failed")
        raise


def load_modal_runtime_config(path: Path = MODAL_CONFIG_PATH) -> ModalRuntimeConfig:
    """Load Modal app and secret names from repository config."""
    payload = json.loads(path.read_text(encoding="utf-8"))
    return ModalRuntimeConfig(
        app_name=str(payload["layer1_daily_app_name"]),
        r2_secret_name=str(payload["r2_secret_name"]),
        timeout_seconds=int(payload["timeout_seconds"]),
    )


def _load_required_layer0_manifest(
    *,
    writer: ObjectStore,
    key: str,
    as_of_date: str,
) -> PipelineManifestRecord:
    """Load and validate the required upstream Layer 0 manifest."""
    manifest = PipelineManifestRecord.model_validate_json(writer.get_object(key).decode("utf-8"))
    if manifest.stage != "layer0":
        raise ValueError(f"Expected stage=layer0 manifest, got {manifest.stage!r}")
    if manifest.status != RunStatus.COMPLETED:
        raise RuntimeError(f"Layer 0 manifest is not completed: {manifest.status}")
    metadata = manifest.metadata
    if metadata.get("from_date") != as_of_date or metadata.get("to_date") != as_of_date:
        raise RuntimeError(
            "Layer 0 manifest is stale for daily Layer 1 run: "
            f"expected {as_of_date}, got {metadata.get('from_date')}..{metadata.get('to_date')}"
        )
    if manifest.finished_at is None:
        raise RuntimeError("Layer 0 manifest must include finished_at before Layer 1 runs")
    return manifest


def _load_eligible_tickers(writer: ObjectStore, as_of_date: str) -> list[str]:
    """Load point-in-time eligible tickers from the Layer 0 universe mask."""
    payload = writer.get_object(raw_universe_path(as_of_date)).decode("utf-8")
    tickers: list[str] = []
    for row in csv.DictReader(io.StringIO(payload)):
        if (
            _truthy(row.get("in_universe"))
            and _truthy(row.get("tradable"), default=True)
            and _truthy(row.get("liquid"), default=True)
            and _truthy(row.get("data_quality_ok"), default=True)
            and not _truthy(row.get("halted"))
        ):
            tickers.append(str(row["ticker"]).strip().upper())
    return tickers


def _write_manifest(
    *,
    writer: ObjectStore,
    key: str,
    config: DailyLayer1PipelineConfig,
    status: RunStatus,
    started_at: datetime,
    metadata: dict[str, object],
) -> None:
    """Write one PipelineManifestRecord for the daily Layer 1 run."""
    manifest = PipelineManifestRecord(
        run_id=config.run_id,
        stage=LAYER1_STAGE,
        status=status,
        started_at=started_at,
        finished_at=datetime.now(UTC),
        input_path=f"{pipeline_manifest_path('layer0', config.layer0_run_id)},{raw_universe_path(config.as_of_date)}",
        output_path=build_r2_key("features", "layer1"),
        metadata=metadata,
    )
    writer.put_object(key, manifest.model_dump_json())


def _truthy(value: str | None, *, default: bool = False) -> bool:
    """Return True for common CSV boolean truth values."""
    if value is None:
        return default
    normalized = str(value).strip().lower()
    if not normalized:
        return default
    return normalized in {"1", "true", "t", "yes", "y"}


def _validate_iso_date(value: str, field_name: str) -> None:
    """Validate a YYYY-MM-DD date string."""
    try:
        datetime.strptime(value, "%Y-%m-%d")
    except ValueError as exc:
        raise ValueError(f"{field_name} must be YYYY-MM-DD") from exc


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """Parse CLI arguments for local or Modal-triggered runs."""
    parser = argparse.ArgumentParser(description="Run daily Layer 1 feature generation.")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--as-of-date", required=True, metavar="YYYY-MM-DD")
    parser.add_argument("--layer0-run-id", required=True)
    parser.add_argument("--benchmark-ticker", default="SPY")
    return parser.parse_args(argv)


def _config_from_args(args: argparse.Namespace) -> DailyLayer1PipelineConfig:
    """Build a validated pipeline config from CLI arguments."""
    return DailyLayer1PipelineConfig(
        run_id=args.run_id,
        as_of_date=args.as_of_date,
        layer0_run_id=args.layer0_run_id,
        benchmark_ticker=args.benchmark_ticker,
    )


def main(argv: Sequence[str] | None = None) -> int:
    """Run daily Layer 1 generation from the local command line."""
    result = run_daily_layer1(_config_from_args(_parse_args(argv)))
    logger.info("Manifest written to {}", result.manifest_key)
    return 0


def modal_main(
    run_id: str,
    as_of_date: str,
    layer0_run_id: str,
    benchmark_ticker: str = "SPY",
) -> None:
    """Submit a daily Layer 1 run to Modal from the local CLI."""
    if _modal_run_daily_layer1 is None:
        raise RuntimeError("Modal app is unavailable because the modal package is not installed")
    _modal_run_daily_layer1.remote(
        run_id=run_id,
        as_of_date=as_of_date,
        layer0_run_id=layer0_run_id,
        benchmark_ticker=benchmark_ticker,
    )


def _define_modal_app() -> object | None:
    """Create the Modal app when the modal package is installed."""
    global _modal_run_daily_layer1

    try:
        modal = importlib.import_module("modal")
    except ModuleNotFoundError:
        return None

    runtime = load_modal_runtime_config()
    image = modal.Image.debian_slim(python_version="3.11").pip_install_from_requirements(
        "requirements/modal.txt"
    )
    app = modal.App(runtime.app_name)

    @app.function(
        image=image,
        secrets=[modal.Secret.from_name(runtime.r2_secret_name)],
        timeout=runtime.timeout_seconds,
        serialized=True,
    )
    def modal_run_daily_layer1(
        run_id: str,
        as_of_date: str,
        layer0_run_id: str,
        benchmark_ticker: str = "SPY",
    ) -> dict[str, object]:
        """Run the daily Layer 1 feature job on Modal."""
        result = run_daily_layer1(
            DailyLayer1PipelineConfig(
                run_id=run_id,
                as_of_date=as_of_date,
                layer0_run_id=layer0_run_id,
                benchmark_ticker=benchmark_ticker,
            )
        )
        return {
            "run_id": result.run_id,
            "as_of_date": result.as_of_date,
            "manifest_key": result.manifest_key,
            "upstream_manifest_key": result.upstream_manifest_key,
            "backfill_manifest_key": result.backfill_manifest_key,
            "tickers_requested": result.tickers_requested,
            "tickers_processed": result.tickers_processed,
        }

    app.local_entrypoint()(modal_main)
    _modal_run_daily_layer1 = modal_run_daily_layer1
    return app


app = _define_modal_app()


if __name__ == "__main__":
    sys.exit(main())
