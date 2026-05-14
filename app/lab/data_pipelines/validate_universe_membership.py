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
    SymbolIdentityResolution,
    fetch_html,
    parse_change_log,
)

MEMBERSHIP_DIR = _REPO_ROOT / "data" / "processed" / "universe" / "membership"


@dataclass(frozen=True)
class SymbolAuditFinding:
    """One symbol-level event-boundary audit outcome."""

    date: str
    action: str
    raw_ticker: str
    resolved_ticker: str
    status: str
    reason_code: str
    before_contains_raw: bool | None = None
    on_contains_raw: bool | None = None
    before_contains_resolved: bool | None = None
    on_contains_resolved: bool | None = None


@dataclass(frozen=True)
class AuditResult:
    """Summary metrics for membership quality audit."""

    structural_issues: int
    events_checked: int
    additions_checked: int
    removals_checked: int
    violations: int
    resolved_symbols: int = 0
    skipped_symbols: int = 0
    findings: tuple[SymbolAuditFinding, ...] = ()

    @property
    def checks_total(self) -> int:
        return self.additions_checked + self.removals_checked

    @property
    def violation_rate(self) -> float:
        if self.checks_total == 0:
            return 0.0
        return self.violations / self.checks_total

    @property
    def mismatched_symbols(self) -> int:
        """Return the number of symbol checks that still violate the boundary rules."""
        return self.violations


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


def _event_details(
    raw_symbols: frozenset[str],
    details: tuple[SymbolIdentityResolution, ...],
) -> tuple[SymbolIdentityResolution, ...]:
    """Return detailed symbol resolutions, synthesizing identity-preserved rows when absent."""
    if details:
        by_resolved = {detail.resolved_ticker: detail for detail in details}
        return tuple(
            by_resolved.get(
                symbol,
                SymbolIdentityResolution(
                    raw_ticker=symbol,
                    resolved_ticker=symbol,
                    reason_code="identity_preserved",
                ),
            )
            for symbol in sorted(raw_symbols)
        )
    return tuple(
        SymbolIdentityResolution(
            raw_ticker=symbol,
            resolved_ticker=symbol,
            reason_code="identity_preserved",
        )
        for symbol in sorted(raw_symbols)
    )


def _skip_finding(
    *,
    event_date: str,
    action: str,
    detail: SymbolIdentityResolution,
    reason_code: str,
) -> SymbolAuditFinding:
    """Build one skipped audit finding."""
    return SymbolAuditFinding(
        date=event_date,
        action=action,
        raw_ticker=detail.raw_ticker,
        resolved_ticker=detail.resolved_ticker,
        status="skipped",
        reason_code=reason_code,
    )


def _evaluate_addition(
    *,
    event_date: str,
    detail: SymbolIdentityResolution,
    previous_membership: set[str],
    on_membership: set[str],
) -> SymbolAuditFinding | None:
    """Evaluate one addition event against the previous-day and event-day memberships."""
    before_contains_raw = detail.raw_ticker in previous_membership
    on_contains_raw = detail.raw_ticker in on_membership
    before_contains_resolved = detail.resolved_ticker in previous_membership
    on_contains_resolved = detail.resolved_ticker in on_membership
    if not before_contains_resolved and on_contains_resolved:
        if detail.raw_ticker != detail.resolved_ticker:
            return SymbolAuditFinding(
                date=event_date,
                action="add",
                raw_ticker=detail.raw_ticker,
                resolved_ticker=detail.resolved_ticker,
                status="resolved",
                reason_code=detail.reason_code,
                before_contains_raw=before_contains_raw,
                on_contains_raw=on_contains_raw,
                before_contains_resolved=before_contains_resolved,
                on_contains_resolved=on_contains_resolved,
            )
        return None

    if detail.raw_ticker != detail.resolved_ticker and on_contains_raw and not on_contains_resolved:
        reason_code = "raw_symbol_present_without_resolved_identity_on_event_date"
    elif detail.raw_ticker != detail.resolved_ticker and before_contains_raw and not before_contains_resolved:
        reason_code = "raw_symbol_present_without_resolved_identity_before_event"
    elif before_contains_resolved and on_contains_resolved:
        reason_code = "addition_already_present_before_event"
    elif not before_contains_resolved and not on_contains_resolved:
        reason_code = "addition_missing_on_event_date"
    elif before_contains_resolved and not on_contains_resolved:
        reason_code = "addition_present_before_event_but_missing_on_event_date"
    else:
        reason_code = "addition_membership_state_mismatch"
    return SymbolAuditFinding(
        date=event_date,
        action="add",
        raw_ticker=detail.raw_ticker,
        resolved_ticker=detail.resolved_ticker,
        status="mismatched",
        reason_code=reason_code,
        before_contains_raw=before_contains_raw,
        on_contains_raw=on_contains_raw,
        before_contains_resolved=before_contains_resolved,
        on_contains_resolved=on_contains_resolved,
    )


def _evaluate_removal(
    *,
    event_date: str,
    detail: SymbolIdentityResolution,
    previous_membership: set[str],
    on_membership: set[str],
) -> SymbolAuditFinding | None:
    """Evaluate one removal event against the previous-day and event-day memberships."""
    before_contains_raw = detail.raw_ticker in previous_membership
    on_contains_raw = detail.raw_ticker in on_membership
    before_contains_resolved = detail.resolved_ticker in previous_membership
    on_contains_resolved = detail.resolved_ticker in on_membership
    if before_contains_resolved and not on_contains_resolved:
        if detail.raw_ticker != detail.resolved_ticker:
            return SymbolAuditFinding(
                date=event_date,
                action="remove",
                raw_ticker=detail.raw_ticker,
                resolved_ticker=detail.resolved_ticker,
                status="resolved",
                reason_code=detail.reason_code,
                before_contains_raw=before_contains_raw,
                on_contains_raw=on_contains_raw,
                before_contains_resolved=before_contains_resolved,
                on_contains_resolved=on_contains_resolved,
            )
        return None

    if detail.raw_ticker != detail.resolved_ticker and before_contains_raw and not before_contains_resolved:
        reason_code = "raw_symbol_present_without_resolved_identity_before_event"
    elif detail.raw_ticker != detail.resolved_ticker and on_contains_raw and not on_contains_resolved:
        reason_code = "raw_symbol_present_without_resolved_identity_on_event_date"
    elif not before_contains_resolved and on_contains_resolved:
        reason_code = "removal_present_only_on_event_date"
    elif not before_contains_resolved and not on_contains_resolved:
        reason_code = "removal_missing_before_event"
    elif before_contains_resolved and on_contains_resolved:
        reason_code = "removal_still_present_on_event_date"
    else:
        reason_code = "removal_membership_state_mismatch"
    return SymbolAuditFinding(
        date=event_date,
        action="remove",
        raw_ticker=detail.raw_ticker,
        resolved_ticker=detail.resolved_ticker,
        status="mismatched",
        reason_code=reason_code,
        before_contains_raw=before_contains_raw,
        on_contains_raw=on_contains_raw,
        before_contains_resolved=before_contains_resolved,
        on_contains_resolved=on_contains_resolved,
    )


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
    resolved_symbols = 0
    skipped_symbols = 0
    findings: list[SymbolAuditFinding] = []

    for event in events:
        event_date = date.fromisoformat(event.date)
        if from_date and event_date < from_date:
            continue
        if to_date and event_date > to_date:
            continue

        on_path = files.get(event_date)
        prev_path = files.get(_previous_business_day(event_date))
        added_details = _event_details(event.added, event.added_details)
        removed_details = _event_details(event.removed, event.removed_details)
        if on_path is None or prev_path is None:
            for detail in added_details:
                findings.append(
                    _skip_finding(
                        event_date=event.date,
                        action="add",
                        detail=detail,
                        reason_code="missing_boundary_membership_file",
                    )
                )
                skipped_symbols += 1
            for detail in removed_details:
                findings.append(
                    _skip_finding(
                        event_date=event.date,
                        action="remove",
                        detail=detail,
                        reason_code="missing_boundary_membership_file",
                    )
                )
                skipped_symbols += 1
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

        for detail in added_details:
            if detail.resolved_ticker in self_canceling:
                findings.append(
                    _skip_finding(
                        event_date=event.date,
                        action="add",
                        detail=detail,
                        reason_code="self_canceling_event",
                    )
                )
                skipped_symbols += 1
                continue
            additions_checked += 1
            finding = _evaluate_addition(
                event_date=event.date,
                detail=detail,
                previous_membership=prev,
                on_membership=on,
            )
            if finding is None:
                continue
            findings.append(finding)
            if finding.status == "resolved":
                resolved_symbols += 1
                logger.info(
                    "Resolved add identity on {} raw={} resolved={} reason={}",
                    finding.date,
                    finding.raw_ticker,
                    finding.resolved_ticker,
                    finding.reason_code,
                )
                continue
            violations += 1
            logger.warning(
                "Add violation on {} raw={} resolved={} reason={} before_resolved={} on_resolved={}",
                finding.date,
                finding.raw_ticker,
                finding.resolved_ticker,
                finding.reason_code,
                finding.before_contains_resolved,
                finding.on_contains_resolved,
            )

        for detail in removed_details:
            if detail.resolved_ticker in self_canceling:
                findings.append(
                    _skip_finding(
                        event_date=event.date,
                        action="remove",
                        detail=detail,
                        reason_code="self_canceling_event",
                    )
                )
                skipped_symbols += 1
                continue
            removals_checked += 1
            finding = _evaluate_removal(
                event_date=event.date,
                detail=detail,
                previous_membership=prev,
                on_membership=on,
            )
            if finding is None:
                continue
            findings.append(finding)
            if finding.status == "resolved":
                resolved_symbols += 1
                logger.info(
                    "Resolved remove identity on {} raw={} resolved={} reason={}",
                    finding.date,
                    finding.raw_ticker,
                    finding.resolved_ticker,
                    finding.reason_code,
                )
                continue
            violations += 1
            logger.warning(
                "Remove violation on {} raw={} resolved={} reason={} before_resolved={} on_resolved={}",
                finding.date,
                finding.raw_ticker,
                finding.resolved_ticker,
                finding.reason_code,
                finding.before_contains_resolved,
                finding.on_contains_resolved,
            )

    return AuditResult(
        structural_issues=structural_issues,
        events_checked=events_checked,
        additions_checked=additions_checked,
        removals_checked=removals_checked,
        violations=violations,
        resolved_symbols=resolved_symbols,
        skipped_symbols=skipped_symbols,
        findings=tuple(findings),
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
        "violations={} resolved_symbols={} skipped_symbols={} violation_rate={:.4%}",
        result.structural_issues,
        result.events_checked,
        result.checks_total,
        result.violations,
        result.resolved_symbols,
        result.skipped_symbols,
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
