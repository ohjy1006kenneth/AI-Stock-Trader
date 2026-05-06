"""Validate Layer 0 R2 archive coverage for the Alpaca SIP backfill window."""

from __future__ import annotations

import argparse
import importlib
import json
import sys
from collections.abc import Callable, Sequence
from dataclasses import asdict, dataclass
from datetime import date, timedelta
from io import BytesIO
from pathlib import Path
from typing import Protocol

from loguru import logger

_REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(_REPO_ROOT))

from services.r2.paths import (  # noqa: E402
    pipeline_manifest_path,
    raw_fundamentals_path,
    raw_news_path,
    raw_universe_path,
)
from services.r2.writer import R2Writer  # noqa: E402
from services.wikipedia.sp500_universe import (  # noqa: E402
    fetch_html,
    parse_change_log,
    parse_current_tickers,
    reconstruct_at_date,
)


class ArchiveReader(Protocol):
    """Subset of R2Writer required to inspect an existing Layer 0 archive."""

    def exists(self, key: str) -> bool:
        """Return True when an object key already exists."""

    def list_keys(self, prefix: str) -> list[str]:
        """List object keys beneath the given prefix."""

    def get_object(self, key: str) -> bytes:
        """Read an object payload by key."""

DEFAULT_VALIDATION_FROM_DATE = "2017-01-01"
DEFAULT_REPORT_DIR = Path("artifacts/reports/integration")
DEFAULT_FUNDAMENTALS_MIN_ROWS = 1
FundamentalsRowCounter = Callable[[ArchiveReader, str], int]


@dataclass(frozen=True)
class Layer0ArchiveValidationReport:
    """Summary of Layer 0 R2 archive coverage for one validation window."""

    from_date: str
    to_date: str
    price_archive_count: int
    news_days_expected: int
    news_days_present: int
    universe_days_expected: int
    universe_days_present: int
    fundamentals_ticker_count: int
    fundamentals_tickers_expected: int
    fundamentals_tickers_present: int
    fundamentals_min_rows: int
    fundamentals_tickers_below_min_rows: list[str]
    macro_day_count: int
    manifest_present: bool
    missing_news_dates: list[str]
    missing_universe_dates: list[str]
    ready_for_layer1: bool


def validate_layer0_archive(
    *,
    from_date: date,
    to_date: date,
    run_id: str,
    reader: ArchiveReader,
    active_fundamentals_tickers: Sequence[str] | None = None,
    fundamentals_min_rows: int = DEFAULT_FUNDAMENTALS_MIN_ROWS,
    fundamentals_row_counter: FundamentalsRowCounter | None = None,
) -> Layer0ArchiveValidationReport:
    """Validate expected Layer 0 archive keys for the Alpaca SIP historical window."""
    if from_date > to_date:
        raise ValueError("from_date must be <= to_date")
    if fundamentals_min_rows < 0:
        raise ValueError("fundamentals_min_rows must be non-negative")

    price_keys = reader.list_keys("raw/prices/")
    fundamentals_keys = reader.list_keys("raw/fundamentals/")
    macro_keys = reader.list_keys("raw/macro/")
    calendar_days = _date_range(from_date, to_date)
    business_days = [day for day in calendar_days if day.weekday() < 5]
    missing_news = [day.isoformat() for day in calendar_days if not reader.exists(raw_news_path(day))]
    missing_universe = [
        day.isoformat() for day in business_days if not reader.exists(raw_universe_path(day))
    ]
    manifest_present = reader.exists(pipeline_manifest_path("layer0", run_id))
    fundamentals_expected = 0
    fundamentals_present = len(fundamentals_keys)
    fundamentals_below_min: list[str] = []
    if active_fundamentals_tickers is not None:
        active_tickers = sorted({_normalize_ticker(ticker) for ticker in active_fundamentals_tickers})
        fundamentals_expected = len(active_tickers)
        fundamentals_present = 0
        counter = fundamentals_row_counter or _count_parquet_rows
        for ticker in active_tickers:
            key = raw_fundamentals_path(ticker)
            if not reader.exists(key):
                fundamentals_below_min.append(ticker)
                continue
            fundamentals_present += 1
            if counter(reader, key) < fundamentals_min_rows:
                fundamentals_below_min.append(ticker)

    ready = bool(price_keys) and not missing_news and not missing_universe
    ready = ready and bool(fundamentals_keys) and bool(macro_keys) and manifest_present
    ready = ready and not fundamentals_below_min

    return Layer0ArchiveValidationReport(
        from_date=from_date.isoformat(),
        to_date=to_date.isoformat(),
        price_archive_count=len(price_keys),
        news_days_expected=len(calendar_days),
        news_days_present=len(calendar_days) - len(missing_news),
        universe_days_expected=len(business_days),
        universe_days_present=len(business_days) - len(missing_universe),
        fundamentals_ticker_count=len(fundamentals_keys),
        fundamentals_tickers_expected=fundamentals_expected,
        fundamentals_tickers_present=fundamentals_present,
        fundamentals_min_rows=fundamentals_min_rows,
        fundamentals_tickers_below_min_rows=fundamentals_below_min,
        macro_day_count=len(macro_keys),
        manifest_present=manifest_present,
        missing_news_dates=missing_news,
        missing_universe_dates=missing_universe,
        ready_for_layer1=ready,
    )


def write_validation_report(report: Layer0ArchiveValidationReport, output_dir: Path) -> Path:
    """Write one validation report JSON file under the integration reports directory."""
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / f"layer0_archive_validation_{report.from_date}_to_{report.to_date}.json"
    payload = json.dumps(asdict(report), indent=2, sort_keys=True) + "\n"
    path.write_text(payload, encoding="utf-8")
    return path


def _date_range(start: date, end: date) -> list[date]:
    days: list[date] = []
    current = start
    while current <= end:
        days.append(current)
        current += timedelta(days=1)
    return days


def _count_parquet_rows(reader: ArchiveReader, key: str) -> int:
    """Return the number of rows in a Parquet object-store payload."""
    payload = reader.get_object(key)
    if not payload:
        return 0
    try:
        import pandas as pd
        importlib.import_module("pyarrow")
    except ModuleNotFoundError as exc:
        raise ModuleNotFoundError(
            "pandas and pyarrow are required to validate fundamentals row coverage."
        ) from exc
    frame = pd.read_parquet(BytesIO(payload))
    return int(len(frame.index))


def _normalize_ticker(ticker: str) -> str:
    """Normalize ticker text to the canonical raw fundamentals archive symbol."""
    if not isinstance(ticker, str):
        raise TypeError("ticker must be a string")
    cleaned = ticker.strip().upper().replace(".", "-")
    if not cleaned:
        raise ValueError("ticker cannot be empty")
    return cleaned


def _sp500_constituents(as_of_date: date) -> list[str]:
    """Return S&P 500 constituents at the validation end date."""
    html = fetch_html()
    current_tickers = parse_current_tickers(html)
    events = parse_change_log(html)
    return reconstruct_at_date(current_tickers, events, as_of_date.isoformat())


def _parse_args() -> argparse.Namespace:
    """Parse Layer 0 validation CLI arguments."""
    parser = argparse.ArgumentParser(description="Validate Layer 0 R2 archive coverage.")
    parser.add_argument("--from-date", default=DEFAULT_VALIDATION_FROM_DATE, metavar="YYYY-MM-DD")
    parser.add_argument("--to-date", required=True, metavar="YYYY-MM-DD")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--output-dir", default=str(DEFAULT_REPORT_DIR))
    parser.add_argument(
        "--fundamentals-min-rows",
        type=int,
        default=DEFAULT_FUNDAMENTALS_MIN_ROWS,
        help="Minimum required rows per active constituent fundamentals archive.",
    )
    parser.add_argument(
        "--active-fundamentals-tickers",
        nargs="*",
        default=None,
        help="Optional active ticker scope; defaults to S&P 500 constituents at --to-date.",
    )
    args = parser.parse_args()
    if args.active_fundamentals_tickers == []:
        parser.error("--active-fundamentals-tickers requires at least one ticker when provided")
    return args


def main() -> int:
    """Run Layer 0 archive validation from the command line."""
    args = _parse_args()
    try:
        from_date = date.fromisoformat(args.from_date)
        to_date = date.fromisoformat(args.to_date)
    except ValueError as exc:
        logger.error("Invalid date argument: {}", exc)
        return 1

    active_tickers = args.active_fundamentals_tickers or _sp500_constituents(to_date)
    report = validate_layer0_archive(
        from_date=from_date,
        to_date=to_date,
        run_id=args.run_id,
        reader=R2Writer(),
        active_fundamentals_tickers=active_tickers,
        fundamentals_min_rows=args.fundamentals_min_rows,
    )
    path = write_validation_report(report, Path(args.output_dir))
    logger.info("Layer 0 archive validation report written to {}", path)
    logger.info("ready_for_layer1={}", report.ready_for_layer1)
    return 0 if report.ready_for_layer1 else 2


if __name__ == "__main__":
    sys.exit(main())
