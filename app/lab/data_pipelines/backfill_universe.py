"""Historical S&P 500 universe membership backfill.

Builds a per-date membership database from Wikipedia's change log, covering
every business day in the requested date range. Output is one CSV per date
written to data/processed/universe/membership/YYYY-MM-DD.csv.

Each CSV contains the list of tickers that were in the S&P 500 on that date,
reconstructed point-in-time (no survivorship bias).

Usage:
    python app/lab/data_pipelines/backfill_universe.py \\
        --from-date 2014-01-01 \\
        --to-date 2024-12-31

    # Force overwrite of already-written dates:
    python app/lab/data_pipelines/backfill_universe.py \\
        --from-date 2014-01-01 \\
        --to-date 2024-12-31 \\
        --overwrite

Note:
    Uses business days (Mon–Fri) as a proxy for trading days. US market
    holidays (e.g. Christmas, July 4) are not filtered — those dates will
    have a valid membership file even though the market was closed. This is
    intentional: feature generation can skip non-trading days independently.

    R2 writes are not implemented yet — that is wired in M1.7 (issue #51).
    For now, output goes to data/processed/universe/membership/ on local disk.
"""
from __future__ import annotations

import argparse
import csv
import sys
from datetime import date, timedelta
from pathlib import Path

from loguru import logger

# Repository root so this script works regardless of cwd
_REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(_REPO_ROOT))

from services.wikipedia.sp500_universe import _fetch_html, get_constituents  # noqa: E402

OUTPUT_DIR = _REPO_ROOT / "data" / "processed" / "universe" / "membership"


def _business_days(from_date: date, to_date: date) -> list[date]:
    """Return all Mon–Fri dates in [from_date, to_date] inclusive."""
    days: list[date] = []
    current = from_date
    while current <= to_date:
        if current.weekday() < 5:  # 0=Mon … 4=Fri
            days.append(current)
        current += timedelta(days=1)
    return days


def _output_path(d: date) -> Path:
    return OUTPUT_DIR / f"{d.isoformat()}.csv"


def _write_membership(d: date, tickers: list[str]) -> None:
    path = _output_path(d)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["ticker"])
        for ticker in tickers:
            writer.writerow([ticker])


def backfill(from_date: date, to_date: date, overwrite: bool = False) -> None:
    """Write one membership CSV per business day in [from_date, to_date].

    Args:
        from_date: First date to cover (inclusive).
        to_date: Last date to cover (inclusive).
        overwrite: If True, re-write dates that already have output files.
    """
    days = _business_days(from_date, to_date)
    logger.info(
        "Backfilling universe membership for {} business days ({} to {})",
        len(days),
        from_date,
        to_date,
    )

    # Fetch and cache Wikipedia HTML once — all per-date calls reuse it
    html = _fetch_html()

    skipped = 0
    written = 0

    for i, d in enumerate(days, start=1):
        path = _output_path(d)
        if path.exists() and not overwrite:
            skipped += 1
            continue

        tickers = get_constituents(d.isoformat(), _html=html)
        _write_membership(d, tickers)
        written += 1

        if i % 250 == 0 or i == len(days):
            logger.info(
                "Progress: {}/{} dates processed (written={}, skipped={})",
                i,
                len(days),
                written,
                skipped,
            )

    logger.info(
        "Backfill complete. written={} skipped={} output_dir={}",
        written,
        skipped,
        OUTPUT_DIR,
    )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Backfill S&P 500 point-in-time membership database from Wikipedia."
    )
    parser.add_argument(
        "--from-date",
        required=True,
        metavar="YYYY-MM-DD",
        help="Start date (inclusive).",
    )
    parser.add_argument(
        "--to-date",
        required=True,
        metavar="YYYY-MM-DD",
        help="End date (inclusive).",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Re-write dates that already have output files.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    try:
        from_date = date.fromisoformat(args.from_date)
        to_date = date.fromisoformat(args.to_date)
    except ValueError as exc:
        logger.error("Invalid date: {}", exc)
        sys.exit(1)

    if from_date > to_date:
        logger.error("--from-date must be <= --to-date")
        sys.exit(1)

    backfill(from_date, to_date, overwrite=args.overwrite)
