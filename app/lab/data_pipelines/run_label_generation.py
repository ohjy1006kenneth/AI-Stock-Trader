"""Modal-ready Layer 1 label-generation runner.

Reads OHLCV archives from R2 (`raw/prices/{TICKER}.parquet`), computes forward
returns + survival flags via `core.labels`, and persists per-(date, ticker)
shards to `labels/layer1/{date}/{ticker}.parquet`. A pipeline manifest is
written on completion or failure.

This module operates exclusively on Layer 0 R2 archives — no external provider
calls — in line with AGENTS.md and `docs/data_contracts.md`.
"""
from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Protocol

from loguru import logger

_REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(_REPO_ROOT))

from core.contracts.schemas import PipelineManifestRecord, RunStatus  # noqa: E402
from core.features.loaders import load_ohlcv_frame  # noqa: E402
from core.labels import (  # noqa: E402
    compute_forward_return_labels,
    forward_return_labels_to_records,
    write_label_record,
)
from services.r2.paths import pipeline_manifest_path  # noqa: E402
from services.r2.writer import R2Writer  # noqa: E402

LABEL_STAGE = "layer1_labels"


class ObjectStore(Protocol):
    """Object-store operations required by the label runner."""

    def put_object(self, key: str, data: bytes | str) -> None:
        """Write an object to storage."""

    def get_object(self, key: str) -> bytes:
        """Read an object from storage."""


@dataclass(frozen=True)
class LabelGenerationConfig:
    """Configuration for one label-generation run."""

    run_id: str
    tickers: tuple[str, ...]

    def __post_init__(self) -> None:
        """Validate run identity and ticker list."""
        if not self.run_id.strip():
            raise ValueError("run_id cannot be empty")
        if not self.tickers:
            raise ValueError("at least one ticker must be supplied")
        for ticker in self.tickers:
            if not ticker.strip():
                raise ValueError("tickers cannot contain empty strings")


@dataclass(frozen=True)
class LabelGenerationResult:
    """Summary of one label-generation run."""

    run_id: str
    tickers_processed: int
    shards_written: int
    started_at: datetime
    finished_at: datetime
    manifest_key: str


def run_label_generation(
    config: LabelGenerationConfig,
    *,
    writer: ObjectStore | None = None,
    now: datetime | None = None,
) -> LabelGenerationResult:
    """Compute forward-return labels for every requested ticker and persist them."""
    started = (now or datetime.now(UTC)).replace(microsecond=0)
    active_writer = writer or R2Writer()

    shards_written = 0
    try:
        for ticker in config.tickers:
            logger.info("Generating labels for ticker={}", ticker)
            ohlcv = load_ohlcv_frame(ticker, writer=active_writer)
            labels = compute_forward_return_labels(ohlcv, ticker)
            for record in forward_return_labels_to_records(labels):
                write_label_record(record, writer=active_writer)
                shards_written += 1
        finished = (now or datetime.now(UTC)).replace(microsecond=0)
        manifest_key = _write_manifest(
            active_writer,
            run_id=config.run_id,
            status=RunStatus.COMPLETED,
            started_at=started,
            finished_at=finished,
            metadata={
                "tickers_processed": len(config.tickers),
                "shards_written": shards_written,
            },
        )
        logger.info(
            "Label generation finished run_id={} shards={}",
            config.run_id,
            shards_written,
        )
        return LabelGenerationResult(
            run_id=config.run_id,
            tickers_processed=len(config.tickers),
            shards_written=shards_written,
            started_at=started,
            finished_at=finished,
            manifest_key=manifest_key,
        )
    except Exception:
        finished = (now or datetime.now(UTC)).replace(microsecond=0)
        _write_manifest(
            active_writer,
            run_id=config.run_id,
            status=RunStatus.FAILED,
            started_at=started,
            finished_at=finished,
            metadata={
                "tickers_processed": len(config.tickers),
                "shards_written": shards_written,
            },
        )
        raise


def _write_manifest(
    writer: ObjectStore,
    *,
    run_id: str,
    status: RunStatus,
    started_at: datetime,
    finished_at: datetime,
    metadata: dict,
) -> str:
    """Persist a pipeline manifest entry for the label run."""
    manifest = PipelineManifestRecord(
        run_id=run_id,
        stage=LABEL_STAGE,
        status=status,
        started_at=started_at,
        finished_at=finished_at,
        metadata=metadata,
    )
    key = pipeline_manifest_path(LABEL_STAGE, run_id)
    payload = manifest.model_dump_json(indent=2).encode("utf-8")
    writer.put_object(key, payload)
    return key


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """Parse CLI arguments for the label runner."""
    parser = argparse.ArgumentParser(description="Generate Layer 1 forward-return labels.")
    parser.add_argument("--run-id", required=True, help="Run identifier for the label batch.")
    parser.add_argument(
        "--tickers",
        required=True,
        help="Comma-separated tickers, or @path/to/tickers.json for a JSON array.",
    )
    return parser.parse_args(argv)


def _resolve_tickers(value: str) -> tuple[str, ...]:
    """Resolve the --tickers argument either inline or from a JSON file."""
    if value.startswith("@"):
        with Path(value[1:]).open("r", encoding="utf-8") as fp:
            payload = json.load(fp)
        if not isinstance(payload, list):
            raise ValueError("Ticker JSON file must contain an array of strings")
        return _validate_tickers(payload)
    return _validate_tickers([token.strip() for token in value.split(",") if token.strip()])


def _validate_tickers(values: Iterable[object]) -> tuple[str, ...]:
    """Coerce an iterable to a tuple of non-empty ticker strings."""
    cleaned: list[str] = []
    for value in values:
        if not isinstance(value, str):
            raise ValueError("ticker entries must be strings")
        stripped = value.strip().upper()
        if not stripped:
            raise ValueError("ticker entries cannot be empty")
        cleaned.append(stripped)
    return tuple(cleaned)


def main(argv: Sequence[str] | None = None) -> int:
    """CLI entry point for `python -m app.lab.data_pipelines.run_label_generation`."""
    args = _parse_args(argv)
    tickers = _resolve_tickers(args.tickers)
    config = LabelGenerationConfig(run_id=args.run_id.strip(), tickers=tickers)
    run_label_generation(config)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
