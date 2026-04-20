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

    # Tune parallelism (default: 16 workers):
    python app/lab/data_pipelines/backfill_universe.py \\
        --from-date 2014-01-01 \\
        --to-date 2024-12-31 \\
        --workers 32

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
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta
from pathlib import Path
from threading import Lock

from loguru import logger

# Repository root so this script works regardless of cwd
_REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(_REPO_ROOT))

from services.wikipedia.sp500_universe import (  # noqa: E402
    ChangeEvent,
    fetch_html,
    parse_change_log,
    parse_current_tickers,
    reconstruct_at_date,
)

OUTPUT_DIR = _REPO_ROOT / "data" / "processed" / "universe" / "membership"
DEFAULT_WORKERS = 16

# Guard against concurrent directory creation across threads
_mkdir_lock = Lock()


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
    with _mkdir_lock:
        path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["ticker"])
        for ticker in tickers:
            writer.writerow([ticker])


def _process_date(
    d: date,
    current_tickers: set[str],
    events: list[ChangeEvent],
    overwrite: bool,
) -> bool:
    """Reconstruct membership for one date and write CSV.

    Returns True if a file was written, False if skipped.
    """
    if _output_path(d).exists() and not overwrite:
        return False
    tickers = reconstruct_at_date(current_tickers, events, d.isoformat())
    _write_membership(d, tickers)
    return True


def backfill(
    from_date: date,
    to_date: date,
    overwrite: bool = False,
    max_workers: int = DEFAULT_WORKERS,
) -> None:
    """Write one membership CSV per business day in [from_date, to_date].

    Parses Wikipedia HTML once, then fans out date reconstruction and file
    writes across a thread pool. File I/O is the bottleneck; threading is
    appropriate here.

    Args:
        from_date: First date to cover (inclusive).
        to_date: Last date to cover (inclusive).
        overwrite: If True, re-write dates that already have output files.
        max_workers: Thread pool size (default 16).
    """
    days = _business_days(from_date, to_date)
    logger.info(
        "Backfilling universe membership for {} business days ({} to {}) "
        "using {} workers",
        len(days),
        from_date,
        to_date,
        max_workers,
    )

    # Parse Wikipedia HTML exactly once — shared read-only across all threads
    html = fetch_html()
    current_tickers = parse_current_tickers(html)
    events = parse_change_log(html)

    written = 0
    skipped = 0
    completed = 0

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(_process_date, d, current_tickers, events, overwrite): d
            for d in days
        }

        for future in as_completed(futures):
            was_written = future.result()
            completed += 1
            if was_written:
                written += 1
            else:
                skipped += 1

            if completed % 250 == 0 or completed == len(days):
                logger.info(
                    "Progress: {}/{} (written={}, skipped={})",
                    completed,
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
    parser.add_argument(
        "--workers",
        type=int,
        default=DEFAULT_WORKERS,
        metavar="N",
        help=f"Thread pool size (default: {DEFAULT_WORKERS}).",
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

    backfill(from_date, to_date, overwrite=args.overwrite, max_workers=args.workers)
