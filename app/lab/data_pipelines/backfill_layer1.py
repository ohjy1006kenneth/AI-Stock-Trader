"""Modal-ready Layer 1 production-run orchestrator.

Reads Layer 0 R2 archives and produces aligned feature shards for every
`(date, ticker)` pair in the supplied ticker list. Per-branch feature
computation respects each module's documented leakage invariant; the final
assembly step also runs `assemble_layer1_feature_records` to defend against
input misconfiguration.

The orchestrator iterates per ticker:
    1. Load OHLCV + fundamentals + macro from R2.
    2. Compute market features (M2.2) using SPY (or another benchmark) as
       cross-asset context when available.
    3. Compute context features (M2.3 + M2.4) — fundamentals merged with
       macro/rates broadcast across every trading day.
    4. Wrap each branch's output in a `Layer1FeatureInput` and assemble into
       per-(date, ticker) FeatureRecords with leakage validation.
    5. Persist each record as `features/layer1/{date}/{ticker}.parquet`.

NLP and regime features have their own dedicated runners
(`run_text_topics.py`, `run_finbert_sentiment.py`, regime training); the
production validator (`validate_layer1_archive.py`) checks shard presence
across the universe.
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
from zoneinfo import ZoneInfo

from loguru import logger

_REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(_REPO_ROOT))

from core.contracts.schemas import FeatureRecord, PipelineManifestRecord, RunStatus  # noqa: E402
from core.features.assembly import Layer1FeatureInput, assemble_layer1_feature_records  # noqa: E402
from core.features.context_features import (  # noqa: E402
    compute_context_features,
    context_features_to_records,
)
from core.features.io import write_feature_record  # noqa: E402
from core.features.loaders import (  # noqa: E402
    load_fundamentals_frame,
    load_macro_frame,
    load_ohlcv_frame,
)
from core.features.market_features import (  # noqa: E402
    compute_market_features,
    market_features_to_records,
)
from services.r2.paths import pipeline_manifest_path  # noqa: E402
from services.r2.writer import R2Writer  # noqa: E402

LAYER1_BACKFILL_STAGE = "layer1_backfill"
SENTINEL_ASSEMBLY_AS_OF = datetime(1900, 1, 1, 0, 0, 0, tzinfo=ZoneInfo("America/New_York"))


class ObjectStore(Protocol):
    """Object-store operations required by the Layer 1 backfill runner."""

    def put_object(self, key: str, data: bytes | str) -> None:
        """Write an object to storage."""

    def get_object(self, key: str) -> bytes:
        """Read an object from storage."""


@dataclass(frozen=True)
class Layer1BackfillConfig:
    """Configuration for one Layer 1 backfill run."""

    run_id: str
    tickers: tuple[str, ...]
    benchmark_ticker: str = "SPY"

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
class Layer1BackfillResult:
    """Summary of one Layer 1 backfill run."""

    run_id: str
    tickers_processed: int
    shards_written: int
    started_at: datetime
    finished_at: datetime
    manifest_key: str


def backfill_layer1(
    config: Layer1BackfillConfig,
    *,
    writer: ObjectStore | None = None,
    now: datetime | None = None,
) -> Layer1BackfillResult:
    """Compute aligned Layer 1 feature shards for every requested ticker."""
    started = (now or datetime.now(UTC)).replace(microsecond=0)
    active_writer = writer or R2Writer()

    macro_frame = load_macro_frame(writer=active_writer)
    benchmark_bars = _try_load_benchmark(active_writer, config.benchmark_ticker)

    shards_written = 0
    try:
        for ticker in config.tickers:
            logger.info("Backfilling Layer 1 features for ticker={}", ticker)
            try:
                ohlcv = load_ohlcv_frame(ticker, writer=active_writer)
            except FileNotFoundError:
                logger.warning("Skipping ticker={} (no OHLCV archive)", ticker)
                continue
            try:
                fundamentals = load_fundamentals_frame(ticker, writer=active_writer)
            except FileNotFoundError:
                fundamentals = _empty_fundamentals_frame(ohlcv)

            market_records = _compute_market_records(
                ticker=ticker,
                ohlcv=ohlcv,
                benchmark_bars=benchmark_bars,
            )
            context_records = _compute_context_records(
                ticker=ticker,
                ohlcv=ohlcv,
                fundamentals=fundamentals,
                macro=macro_frame,
            )

            assembled = assemble_layer1_feature_records(
                [
                    Layer1FeatureInput(
                        name="market",
                        records=market_records,
                        as_of_timestamp=SENTINEL_ASSEMBLY_AS_OF,
                    ),
                    Layer1FeatureInput(
                        name="context",
                        records=context_records,
                        as_of_timestamp=SENTINEL_ASSEMBLY_AS_OF,
                    ),
                ]
            )
            for record in assembled:
                write_feature_record(record, writer=active_writer)
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
                "benchmark_ticker": config.benchmark_ticker,
            },
        )
        logger.info(
            "Layer 1 backfill finished run_id={} shards={}",
            config.run_id,
            shards_written,
        )
        return Layer1BackfillResult(
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
                "benchmark_ticker": config.benchmark_ticker,
            },
        )
        raise


def _compute_market_records(
    *,
    ticker: str,
    ohlcv,
    benchmark_bars,
) -> list[FeatureRecord]:
    """Return market FeatureRecords stripped of the all-NaN warm-up rows."""
    features = compute_market_features(ohlcv, ticker, benchmark_bars=benchmark_bars)
    records = market_features_to_records(features)
    return [record for record in records if any(value is not None for value in record.features.values())]


def _compute_context_records(
    *,
    ticker: str,
    ohlcv,
    fundamentals,
    macro,
) -> list[FeatureRecord]:
    """Return context FeatureRecords for the given ticker."""
    features = compute_context_features(
        fundamentals=fundamentals,
        ohlcv=ohlcv,
        macro=macro,
        ticker=ticker,
    )
    return context_features_to_records(features)


def _try_load_benchmark(writer: ObjectStore, ticker: str):
    """Return the benchmark OHLCV frame when available, else None."""
    try:
        return load_ohlcv_frame(ticker, writer=writer)
    except FileNotFoundError:
        logger.warning("Benchmark OHLCV missing for ticker={}; cross-asset features will be NaN", ticker)
        return None


def _empty_fundamentals_frame(ohlcv):
    """Return an empty fundamentals frame matching the columns expected by features."""
    import pandas as pd

    return pd.DataFrame(
        columns=[
            "report_date",
            "availability_date",
            "fiscal_year",
            "fiscal_period",
            "raw_json",
            "earnings_date",
        ]
    )


def _write_manifest(
    writer: ObjectStore,
    *,
    run_id: str,
    status: RunStatus,
    started_at: datetime,
    finished_at: datetime,
    metadata: dict,
) -> str:
    """Persist a pipeline manifest entry for the backfill run."""
    manifest = PipelineManifestRecord(
        run_id=run_id,
        stage=LAYER1_BACKFILL_STAGE,
        status=status,
        started_at=started_at,
        finished_at=finished_at,
        metadata=metadata,
    )
    key = pipeline_manifest_path(LAYER1_BACKFILL_STAGE, run_id)
    payload = manifest.model_dump_json(indent=2).encode("utf-8")
    writer.put_object(key, payload)
    return key


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """Parse CLI arguments for the Layer 1 backfill runner."""
    parser = argparse.ArgumentParser(description="Run the Layer 1 production backfill.")
    parser.add_argument("--run-id", required=True, help="Run identifier for the backfill batch.")
    parser.add_argument(
        "--tickers",
        required=True,
        help="Comma-separated tickers, or @path/to/tickers.json for a JSON array.",
    )
    parser.add_argument(
        "--benchmark-ticker",
        default="SPY",
        help="Benchmark ticker used for cross-asset features (default: SPY).",
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
    """CLI entry point for `python -m app.lab.data_pipelines.backfill_layer1`."""
    args = _parse_args(argv)
    tickers = _resolve_tickers(args.tickers)
    config = Layer1BackfillConfig(
        run_id=args.run_id.strip(),
        tickers=tickers,
        benchmark_ticker=args.benchmark_ticker.strip().upper(),
    )
    backfill_layer1(config)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
