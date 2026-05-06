"""Diagnose and repair Layer 0 SimFin fundamentals coverage gaps."""
from __future__ import annotations

import argparse
import importlib
import json
import sys
from collections.abc import Callable, Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime
from io import BytesIO
from pathlib import Path
from typing import Protocol

from loguru import logger

_REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(_REPO_ROOT))

from app.lab.data_pipelines.backfill_simfin import (  # noqa: E402
    BackfillResult,
    FundamentalsFetcher,
    FundamentalsSerializer,
    ObjectWriter,
    backfill_simfin_archive,
)
from services.r2.paths import raw_fundamentals_path  # noqa: E402
from services.r2.writer import R2Writer  # noqa: E402
from services.simfin.fundamentals_fetcher import (  # noqa: E402
    SimFinClientConfig,
    SimFinFundamentalsFetcher,
)
from services.wikipedia.sp500_universe import (  # noqa: E402
    fetch_html,
    get_all_historical_tickers,
    parse_change_log,
    parse_current_tickers,
    reconstruct_at_date,
)

DEFAULT_MIN_FUNDAMENTALS_ROWS = 10
DEFAULT_REPORT_DIR = Path("artifacts/reports/integration")
RowCounter = Callable[[object, str], int]


class FundamentalsArchiveReader(Protocol):
    """Object-reader subset used by SimFin coverage diagnostics."""

    def exists(self, key: str) -> bool:
        """Return True when the object key exists."""

    def get_object(self, key: str) -> bytes:
        """Read an object payload by key."""


@dataclass(frozen=True)
class FundamentalsCoverageRecord:
    """One ticker with fundamentals row coverage below the configured threshold."""

    ticker: str
    row_count: int
    active: bool
    reason: str


@dataclass(frozen=True)
class FundamentalsCoverageReport:
    """Diagnostic report for Layer 0 SimFin fundamentals coverage."""

    generated_at: str
    min_rows: int
    historical_ticker_count: int
    active_ticker_count: int
    records: list[FundamentalsCoverageRecord]


def diagnose_fundamentals_coverage(
    *,
    reader: FundamentalsArchiveReader,
    historical_tickers: Sequence[str],
    active_tickers: Sequence[str],
    min_rows: int = DEFAULT_MIN_FUNDAMENTALS_ROWS,
    row_counter: RowCounter | None = None,
    generated_at: datetime | None = None,
) -> FundamentalsCoverageReport:
    """List every historical ticker whose SimFin archive has fewer than ``min_rows`` rows."""
    if min_rows < 0:
        raise ValueError("min_rows must be non-negative")
    historical = sorted({_normalize_archive_ticker(ticker) for ticker in historical_tickers})
    active = {_normalize_archive_ticker(ticker) for ticker in active_tickers}
    if not historical:
        raise ValueError("historical_tickers must contain at least one ticker")
    counter = row_counter or count_fundamentals_rows
    records: list[FundamentalsCoverageRecord] = []
    for ticker in historical:
        key = raw_fundamentals_path(ticker)
        if not reader.exists(key):
            row_count = 0
            reason = "missing_archive"
        else:
            row_count = counter(reader, key)
            reason = "below_min_rows"
        if row_count < min_rows:
            records.append(
                FundamentalsCoverageRecord(
                    ticker=ticker,
                    row_count=row_count,
                    active=ticker in active,
                    reason=reason,
                )
            )
    return FundamentalsCoverageReport(
        generated_at=(generated_at or datetime.now(UTC)).isoformat(),
        min_rows=min_rows,
        historical_ticker_count=len(historical),
        active_ticker_count=len(active),
        records=records,
    )


def affected_active_tickers(report: FundamentalsCoverageReport) -> list[str]:
    """Return active tickers that need a targeted SimFin refetch."""
    return [record.ticker for record in report.records if record.active]


def refetch_active_fundamentals_gaps(
    from_date: date,
    to_date: date,
    *,
    fetcher: FundamentalsFetcher,
    writer: ObjectWriter,
    tickers: Sequence[str],
    retrieved_at: datetime | None = None,
    serializer: FundamentalsSerializer | None = None,
) -> BackfillResult:
    """Refetch only affected active tickers and rewrite their per-ticker shards."""
    scoped_tickers = sorted({_normalize_archive_ticker(ticker) for ticker in tickers})
    if not scoped_tickers:
        return BackfillResult(
            requested_tickers=0,
            written=0,
            skipped=0,
            empty=0,
            total_rows=0,
            output_keys=(),
        )
    return backfill_simfin_archive(
        from_date=from_date,
        to_date=to_date,
        fetcher=fetcher,
        writer=writer,
        tickers=scoped_tickers,
        overwrite=True,
        retrieved_at=retrieved_at,
        serializer=serializer,
    )


def count_fundamentals_rows(reader: FundamentalsArchiveReader, key: str) -> int:
    """Return the number of rows in one raw fundamentals Parquet archive object."""
    payload = reader.get_object(key)
    if not payload:
        return 0
    try:
        import pandas as pd
        importlib.import_module("pyarrow")
    except ModuleNotFoundError as exc:
        raise ModuleNotFoundError(
            "pandas and pyarrow are required to inspect SimFin fundamentals coverage. "
            "Install the Pi, Modal, or dev requirements before running diagnostics."
        ) from exc
    frame = pd.read_parquet(BytesIO(payload))
    return int(len(frame.index))


def write_coverage_report(report: FundamentalsCoverageReport, output_dir: Path) -> Path:
    """Write a deterministic JSON coverage report for audit and recovery records."""
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / f"simfin_coverage_gaps_min{report.min_rows}.json"
    payload = asdict(report)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _current_constituents(as_of_date: date) -> list[str]:
    """Return S&P 500 constituents at ``as_of_date`` using the Wikipedia change log."""
    html = fetch_html()
    current_tickers = parse_current_tickers(html)
    events = parse_change_log(html)
    return reconstruct_at_date(current_tickers, events, as_of_date.isoformat())


def _normalize_archive_ticker(ticker: str) -> str:
    """Normalize provider/archive ticker text to the canonical archive symbol form."""
    if not isinstance(ticker, str):
        raise TypeError("ticker must be a string")
    cleaned = ticker.strip().upper().replace(".", "-")
    if not cleaned:
        raise ValueError("ticker cannot be empty")
    return cleaned


def _parse_args() -> argparse.Namespace:
    """Parse CLI arguments for SimFin coverage diagnostics and repair."""
    parser = argparse.ArgumentParser(description="Diagnose/refetch Layer 0 SimFin coverage gaps.")
    parser.add_argument("--from-date", required=True, metavar="YYYY-MM-DD")
    parser.add_argument("--to-date", required=True, metavar="YYYY-MM-DD")
    parser.add_argument("--min-rows", type=int, default=DEFAULT_MIN_FUNDAMENTALS_ROWS)
    parser.add_argument("--tickers", nargs="*", default=None, help="Optional historical ticker scope")
    parser.add_argument("--active-tickers", nargs="*", default=None, help="Optional active ticker scope")
    parser.add_argument("--output-dir", default=str(DEFAULT_REPORT_DIR))
    parser.add_argument("--refetch", action="store_true", help="Refetch affected active tickers")
    args = parser.parse_args()
    if args.tickers == []:
        parser.error("--tickers requires at least one ticker when provided")
    if args.active_tickers == []:
        parser.error("--active-tickers requires at least one ticker when provided")
    return args


def main() -> int:
    """Run SimFin coverage diagnosis, optionally followed by targeted recovery."""
    args = _parse_args()
    try:
        from_date = date.fromisoformat(args.from_date)
        to_date = date.fromisoformat(args.to_date)
    except ValueError as exc:
        logger.error("Invalid date argument: {}", exc)
        return 1
    if from_date > to_date:
        logger.error("--from-date must be <= --to-date")
        return 1

    historical_tickers = args.tickers or sorted(
        get_all_historical_tickers(from_date.isoformat(), to_date.isoformat())
    )
    active_tickers = args.active_tickers or _current_constituents(to_date)
    writer = R2Writer()
    report = diagnose_fundamentals_coverage(
        reader=writer,
        historical_tickers=historical_tickers,
        active_tickers=active_tickers,
        min_rows=args.min_rows,
    )
    path = write_coverage_report(report, Path(args.output_dir))
    logger.info("SimFin coverage report written to {}", path)
    logger.info(
        "SimFin coverage gaps: {} total, {} active",
        len(report.records),
        len(affected_active_tickers(report)),
    )

    if args.refetch:
        result = refetch_active_fundamentals_gaps(
            from_date=from_date,
            to_date=to_date,
            fetcher=SimFinFundamentalsFetcher(SimFinClientConfig.from_env()),
            writer=writer,
            tickers=affected_active_tickers(report),
        )
        logger.info("SimFin coverage refetch complete: {}", result)
    return 0


if __name__ == "__main__":
    sys.exit(main())
