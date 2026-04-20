"""Unit tests for app/lab/data_pipelines/validate_universe_membership.py."""
from __future__ import annotations

import csv
import json
from argparse import Namespace
from datetime import date, timedelta
from itertools import zip_longest
from pathlib import Path
from unittest.mock import patch

from app.lab.data_pipelines import validate_universe_membership as validator
from services.wikipedia.sp500_universe import ChangeEvent, get_constituents

FIXTURE_PATH = Path("data/sample/sp500_changes_fixture.json")


def _build_html(fixture: dict) -> str:
    ticker_rows = "\n".join(
        f"<tr><td>{t}</td><td>Company Name</td></tr>" for t in fixture["current_tickers"]
    )
    constituents_table = (
        '<table id="constituents">'
        "<tr><th>Symbol</th><th>Security</th></tr>"
        f"{ticker_rows}"
        "</table>"
    )

    change_rows: list[str] = []
    for event in fixture["changes"]:
        added_list = event.get("added", [])
        removed_list = event.get("removed", [])
        for added, removed in zip_longest(added_list, removed_list, fillvalue=""):
            change_rows.append(
                f"<tr><td>{event['date']}</td><td>{added}</td><td>Added Name</td>"
                f"<td>{removed}</td><td>Removed Name</td></tr>"
            )

    changes_table = (
        '<table id="changes">'
        "<tr><th>Date</th><th>Added</th><th>Name</th><th>Removed</th><th>Name</th></tr>"
        + "".join(change_rows)
        + "</table>"
    )

    return f"<html><body>{constituents_table}{changes_table}</body></html>"


def _write_membership_csv(output_dir: Path, day: date, tickers: list[str]) -> None:
    path = output_dir / f"{day.isoformat()}.csv"
    with path.open("w", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(["ticker"])
        for ticker in tickers:
            writer.writerow([ticker])


def _business_days(start: date, end: date) -> list[date]:
    days: list[date] = []
    current = start
    while current <= end:
        if current.weekday() < 5:
            days.append(current)
        current += timedelta(days=1)
    return days


def _build_consistent_membership(output_dir: Path, html: str, start: date, end: date) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    for day in _business_days(start, end):
        tickers = get_constituents(day.isoformat(), _html=html)
        _write_membership_csv(output_dir, day, tickers)


def test_audit_membership_has_zero_violations_for_consistent_dataset(tmp_path: Path) -> None:
    fixture = json.loads(FIXTURE_PATH.read_text())
    html = _build_html(fixture)
    start = date(2020, 9, 1)
    end = date(2021, 1, 29)
    _build_consistent_membership(tmp_path, html, start, end)

    with patch("app.lab.data_pipelines.validate_universe_membership.fetch_html", return_value=html):
        result = validator.audit_membership(
            membership_dir=tmp_path,
            from_date=start,
            to_date=end,
        )

    assert result.structural_issues == 0
    assert result.checks_total > 0
    assert result.violations == 0
    assert result.violation_rate == 0.0


def test_main_fails_when_violation_rate_exceeds_threshold() -> None:
    args = Namespace(
        membership_dir=Path("unused"),
        from_date=None,
        to_date=None,
        max_violation_rate=0.01,
    )

    with (
        patch("app.lab.data_pipelines.validate_universe_membership._parse_args", return_value=args),
        patch(
            "app.lab.data_pipelines.validate_universe_membership.audit_membership",
            return_value=validator.AuditResult(
                structural_issues=0,
                events_checked=10,
                additions_checked=50,
                removals_checked=50,
                violations=2,
            ),
        ),
    ):
        assert validator.main() == 1


def test_main_passes_when_violation_rate_is_within_threshold() -> None:
    args = Namespace(
        membership_dir=Path("unused"),
        from_date=None,
        to_date=None,
        max_violation_rate=0.01,
    )

    with (
        patch("app.lab.data_pipelines.validate_universe_membership._parse_args", return_value=args),
        patch(
            "app.lab.data_pipelines.validate_universe_membership.audit_membership",
            return_value=validator.AuditResult(
                structural_issues=0,
                events_checked=10,
                additions_checked=90,
                removals_checked=10,
                violations=1,
            ),
        ),
    ):
        assert validator.main() == 0


def test_audit_ignores_self_canceling_event_symbols(tmp_path: Path) -> None:
    _write_membership_csv(tmp_path, date(2020, 9, 18), ["AAPL", "FOX", "FOXA"])
    _write_membership_csv(tmp_path, date(2020, 9, 21), ["AAPL", "FOX", "FOXA"])

    event = ChangeEvent(
        date="2020-09-21",
        added=frozenset(["FOX", "FOXA"]),
        removed=frozenset(["FOX", "FOXA"]),
    )

    with (
        patch("app.lab.data_pipelines.validate_universe_membership.fetch_html", return_value=""),
        patch("app.lab.data_pipelines.validate_universe_membership.parse_change_log", return_value=[event]),
    ):
        result = validator.audit_membership(
            membership_dir=tmp_path,
            from_date=date(2020, 9, 1),
            to_date=date(2020, 9, 30),
        )

    assert result.events_checked == 1
    assert result.checks_total == 0
    assert result.violations == 0
