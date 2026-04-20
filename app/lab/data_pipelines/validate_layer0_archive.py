"""Validate Layer 0 R2 archive coverage for the Alpaca SIP backfill window."""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict, dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Protocol

from loguru import logger

_REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(_REPO_ROOT))

from services.r2.paths import (  # noqa: E402
    pipeline_manifest_path,
    raw_news_path,
    raw_universe_path,
)
from services.r2.writer import R2Writer  # noqa: E402


class ArchiveReader(Protocol):
    """Subset of R2Writer required to inspect an existing Layer 0 archive."""

    def exists(self, key: str) -> bool:
        """Return True when an object key already exists."""

    def list_keys(self, prefix: str) -> list[str]:
        """List object keys beneath the given prefix."""

DEFAULT_VALIDATION_FROM_DATE = "2017-01-01"
DEFAULT_REPORT_DIR = Path("artifacts/reports/integration")


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
) -> Layer0ArchiveValidationReport:
    """Validate expected Layer 0 archive keys for the Alpaca SIP historical window."""
    if from_date > to_date:
        raise ValueError("from_date must be <= to_date")

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
    ready = bool(price_keys) and not missing_news and not missing_universe
    ready = ready and bool(fundamentals_keys) and bool(macro_keys) and manifest_present

    return Layer0ArchiveValidationReport(
        from_date=from_date.isoformat(),
        to_date=to_date.isoformat(),
        price_archive_count=len(price_keys),
        news_days_expected=len(calendar_days),
        news_days_present=len(calendar_days) - len(missing_news),
        universe_days_expected=len(business_days),
        universe_days_present=len(business_days) - len(missing_universe),
        fundamentals_ticker_count=len(fundamentals_keys),
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


def _parse_args() -> argparse.Namespace:
    """Parse Layer 0 validation CLI arguments."""
    parser = argparse.ArgumentParser(description="Validate Layer 0 R2 archive coverage.")
    parser.add_argument("--from-date", default=DEFAULT_VALIDATION_FROM_DATE, metavar="YYYY-MM-DD")
    parser.add_argument("--to-date", required=True, metavar="YYYY-MM-DD")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--output-dir", default=str(DEFAULT_REPORT_DIR))
    return parser.parse_args()


def main() -> int:
    """Run Layer 0 archive validation from the command line."""
    args = _parse_args()
    try:
        from_date = date.fromisoformat(args.from_date)
        to_date = date.fromisoformat(args.to_date)
    except ValueError as exc:
        logger.error("Invalid date argument: {}", exc)
        return 1

    report = validate_layer0_archive(
        from_date=from_date,
        to_date=to_date,
        run_id=args.run_id,
        reader=R2Writer(),
    )
    path = write_validation_report(report, Path(args.output_dir))
    logger.info("Layer 0 archive validation report written to {}", path)
    logger.info("ready_for_layer1={}", report.ready_for_layer1)
    return 0 if report.ready_for_layer1 else 2


if __name__ == "__main__":
    sys.exit(main())
