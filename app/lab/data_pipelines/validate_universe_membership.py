"""Validate point-in-time universe membership files against Wikipedia change events.

This tool audits daily membership CSV outputs and enforces a quality gate on
change-event boundary correctness.

Checks performed:
- Structural checks (header, duplicates, empty files)
- Event-boundary checks for additions/removals

Quality gate:
- Fails with non-zero exit code when violation rate > max threshold

Usage:
    python app/lab/data_pipelines/validate_universe_membership.py

    python app/lab/data_pipelines/validate_universe_membership.py \
        --from-date 2019-01-01 \
        --to-date 2024-12-31 \
        --max-violation-rate 0.01
"""
from __future__ import annotations

import argparse
import csv
import sys
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

from loguru import logger

# Repository root so this script works regardless of cwd
_REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(_REPO_ROOT))

from services.wikipedia.sp500_universe import (  # noqa: E402
    fetch_html,
    parse_change_log,
)

MEMBERSHIP_DIR = _REPO_ROOT / "data" / "processed" / "universe" / "membership"


@dataclass(frozen=True)
class AuditResult:
    """Summary metrics for membership quality audit."""

    structural_issues: int
    events_checked: int
    additions_checked: int
    removals_checked: int
    violations: int

    @property
    def checks_total(self) -> int:
        return self.additions_checked + self.removals_checked

    @property
    def violation_rate(self) -> float:
        if self.checks_total == 0:
            return 0.0
        return self.violations / self.checks_total


def _read_membership_set(file_path: Path) -> set[str]:
    """Read membership CSV and return the ticker set for a date."""
    with file_path.open(newline="") as fh:
        rows = list(csv.reader(fh))

    if not rows or rows[0] != ["ticker"]:
        raise ValueError(f"Invalid header in {file_path}")

    tickers = [row[0].strip() for row in rows[1:] if row]
    if len(tickers) != len(set(tickers)):
        raise ValueError(f"Duplicate tickers found in {file_path}")

    return set(tickers)


def _previous_business_day(day: date) -> date:
    """Return previous Mon-Fri date before day."""
    prev = day
    while True:
        prev -= timedelta(days=1)
        if prev.weekday() < 5:
            return prev


def _collect_files(
    membership_dir: Path,
    from_date: date | None,
    to_date: date | None,
) -> dict[date, Path]:
    """Load membership CSV files within optional date range."""
    files: dict[date, Path] = {}
    for path in sorted(membership_dir.glob("*.csv")):
        try:
            d = date.fromisoformat(path.stem)
        except ValueError:
            continue
        if from_date and d < from_date:
            continue
        if to_date and d > to_date:
            continue
        files[d] = path
    return files


def _audit_structure(files: dict[date, Path]) -> int:
    """Return count of structural issues in membership files."""
    issues = 0
    for d, path in files.items():
        try:
            tickers = _read_membership_set(path)
            if not tickers:
                logger.warning("Empty membership on {}: {}", d, path)
                issues += 1
        except ValueError as exc:
            logger.error("Structural issue: {}", exc)
            issues += 1
    return issues


def audit_membership(
    membership_dir: Path,
    from_date: date | None,
    to_date: date | None,
) -> AuditResult:
    """Audit saved membership files against Wikipedia add/remove boundary events."""
    files = _collect_files(membership_dir, from_date, to_date)
    if not files:
        raise ValueError(f"No membership files found in range under {membership_dir}")

    structural_issues = _audit_structure(files)

    html = fetch_html()
    events = parse_change_log(html)

    events_checked = 0
    additions_checked = 0
    removals_checked = 0
    violations = 0

    for event in events:
        event_date = date.fromisoformat(event.date)
        if from_date and event_date < from_date:
            continue
        if to_date and event_date > to_date:
            continue

        on_path = files.get(event_date)
        prev_path = files.get(_previous_business_day(event_date))
        if on_path is None or prev_path is None:
            continue

        events_checked += 1
        on = _read_membership_set(on_path)
        prev = _read_membership_set(prev_path)

        self_canceling = set(event.added) & set(event.removed)
        if self_canceling:
            logger.debug(
                "Skipping self-canceling symbols on {}: {}",
                event.date,
                sorted(self_canceling),
            )

        for ticker in event.added:
            if ticker in self_canceling:
                continue
            additions_checked += 1
            if ticker in prev or ticker not in on:
                violations += 1
                logger.warning(
                    "Add violation on {} ticker={} before_in={} on_in={}",
                    event.date,
                    ticker,
                    ticker in prev,
                    ticker in on,
                )

        for ticker in event.removed:
            if ticker in self_canceling:
                continue
            removals_checked += 1
            if ticker not in prev or ticker in on:
                violations += 1
                logger.warning(
                    "Remove violation on {} ticker={} before_in={} on_in={}",
                    event.date,
                    ticker,
                    ticker in prev,
                    ticker in on,
                )

    return AuditResult(
        structural_issues=structural_issues,
        events_checked=events_checked,
        additions_checked=additions_checked,
        removals_checked=removals_checked,
        violations=violations,
    )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate universe membership quality against Wikipedia change events."
    )
    parser.add_argument(
        "--membership-dir",
        type=Path,
        default=MEMBERSHIP_DIR,
        help=f"Membership directory (default: {MEMBERSHIP_DIR}).",
    )
    parser.add_argument(
        "--from-date",
        type=date.fromisoformat,
        default=None,
        metavar="YYYY-MM-DD",
        help="Start date for audit range (inclusive).",
    )
    parser.add_argument(
        "--to-date",
        type=date.fromisoformat,
        default=None,
        metavar="YYYY-MM-DD",
        help="End date for audit range (inclusive).",
    )
    parser.add_argument(
        "--max-violation-rate",
        type=float,
        default=0.01,
        help="Maximum allowed violation rate before failing (default: 0.01).",
    )
    return parser.parse_args()


def main() -> int:
    """Run membership audit and enforce violation-rate quality gate."""
    args = _parse_args()

    if args.from_date and args.to_date and args.from_date > args.to_date:
        logger.error("--from-date must be <= --to-date")
        return 2

    if args.max_violation_rate < 0 or args.max_violation_rate > 1:
        logger.error("--max-violation-rate must be between 0 and 1")
        return 2

    try:
        result = audit_membership(
            membership_dir=args.membership_dir,
            from_date=args.from_date,
            to_date=args.to_date,
        )
    except ValueError as exc:
        logger.error("Audit failed: {}", exc)
        return 2

    logger.info(
        "Audit summary: structural_issues={} events_checked={} checks_total={} "
        "violations={} violation_rate={:.4%}",
        result.structural_issues,
        result.events_checked,
        result.checks_total,
        result.violations,
        result.violation_rate,
    )

    if result.structural_issues > 0:
        logger.error("Quality gate failed: structural issues detected")
        return 1

    if result.violation_rate > args.max_violation_rate:
        logger.error(
            "Quality gate failed: violation_rate {:.4%} exceeds threshold {:.4%}",
            result.violation_rate,
            args.max_violation_rate,
        )
        return 1

    logger.info("Quality gate passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
