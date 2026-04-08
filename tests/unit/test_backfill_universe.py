"""Unit tests for app/lab/data_pipelines/backfill_universe.py."""
from __future__ import annotations

import csv
import json
from datetime import date
from pathlib import Path
from unittest.mock import patch

import pytest

from app.lab.data_pipelines.backfill_universe import (
    _business_days,
    _output_path,
    _write_membership,
    backfill,
)

FIXTURE_PATH = Path("data/sample/sp500_changes_fixture.json")


def _load_fixture_html() -> str:
    """Build fixture HTML from the shared fixture JSON (same helper logic as universe tests)."""
    from itertools import zip_longest

    fixture = json.loads(FIXTURE_PATH.read_text())

    ticker_rows = "\n".join(
        f"<tr><td>{t}</td><td>Name</td></tr>" for t in fixture["current_tickers"]
    )
    constituents_table = (
        '<table id="constituents"><tr><th>Symbol</th><th>Security</th></tr>'
        f"{ticker_rows}</table>"
    )

    change_rows: list[str] = []
    for event in fixture["changes"]:
        for added, removed in zip_longest(
            event.get("added", []), event.get("removed", []), fillvalue=""
        ):
            change_rows.append(
                f"<tr><td>{event['date']}</td><td>{added}</td>"
                f"<td>Name</td><td>{removed}</td><td>Name</td></tr>"
            )

    changes_table = (
        '<table id="changes"><tr><th>Date</th><th>Added</th><th>Name</th>'
        "<th>Removed</th><th>Name</th></tr>"
        + "".join(change_rows)
        + "</table>"
    )
    return f"<html><body>{constituents_table}{changes_table}</body></html>"


# ---------------------------------------------------------------------------
# _business_days
# ---------------------------------------------------------------------------

class TestBusinessDays:
    def test_skips_saturday_and_sunday(self) -> None:
        # 2024-01-06 is Saturday, 2024-01-07 is Sunday
        days = _business_days(date(2024, 1, 5), date(2024, 1, 8))
        assert date(2024, 1, 6) not in days
        assert date(2024, 1, 7) not in days

    def test_includes_friday_and_monday(self) -> None:
        days = _business_days(date(2024, 1, 5), date(2024, 1, 8))
        assert date(2024, 1, 5) in days  # Friday
        assert date(2024, 1, 8) in days  # Monday

    def test_single_weekday(self) -> None:
        days = _business_days(date(2024, 1, 2), date(2024, 1, 2))
        assert days == [date(2024, 1, 2)]

    def test_single_weekend_day_returns_empty(self) -> None:
        days = _business_days(date(2024, 1, 6), date(2024, 1, 6))
        assert days == []

    def test_full_week_has_five_days(self) -> None:
        days = _business_days(date(2024, 1, 1), date(2024, 1, 7))
        assert len(days) == 5

    def test_from_after_to_returns_empty(self) -> None:
        days = _business_days(date(2024, 1, 5), date(2024, 1, 4))
        assert days == []


# ---------------------------------------------------------------------------
# _write_membership
# ---------------------------------------------------------------------------

class TestWriteMembership:
    def test_creates_csv_with_header_and_tickers(self, tmp_path: Path) -> None:
        with patch(
            "app.lab.data_pipelines.backfill_universe.OUTPUT_DIR", tmp_path
        ):
            d = date(2020, 1, 2)
            _write_membership(d, ["AAPL", "MSFT", "AMZN"])
            path = tmp_path / "2020-01-02.csv"
            assert path.exists()
            rows = list(csv.DictReader(path.open()))
            assert [r["ticker"] for r in rows] == ["AAPL", "MSFT", "AMZN"]

    def test_creates_parent_dirs(self, tmp_path: Path) -> None:
        nested = tmp_path / "a" / "b"
        with patch("app.lab.data_pipelines.backfill_universe.OUTPUT_DIR", nested):
            _write_membership(date(2020, 1, 2), ["AAPL"])
            assert (nested / "2020-01-02.csv").exists()

    def test_empty_ticker_list_writes_header_only(self, tmp_path: Path) -> None:
        with patch("app.lab.data_pipelines.backfill_universe.OUTPUT_DIR", tmp_path):
            _write_membership(date(2020, 1, 2), [])
            rows = list(csv.DictReader((tmp_path / "2020-01-02.csv").open()))
            assert rows == []


# ---------------------------------------------------------------------------
# backfill (integration of the pieces)
# ---------------------------------------------------------------------------

class TestBackfill:
    def _run_backfill(
        self, tmp_path: Path, from_date: date, to_date: date, overwrite: bool = False
    ) -> None:
        html = _load_fixture_html()
        with (
            patch("app.lab.data_pipelines.backfill_universe.OUTPUT_DIR", tmp_path),
            patch(
                "app.lab.data_pipelines.backfill_universe._fetch_html",
                return_value=html,
            ),
        ):
            backfill(from_date, to_date, overwrite=overwrite)

    def test_creates_one_file_per_business_day(self, tmp_path: Path) -> None:
        # 2024-01-08 Mon to 2024-01-12 Fri = 5 business days
        self._run_backfill(tmp_path, date(2024, 1, 8), date(2024, 1, 12))
        files = sorted(tmp_path.glob("*.csv"))
        assert len(files) == 5

    def test_skips_weekends(self, tmp_path: Path) -> None:
        # 2024-01-05 Fri to 2024-01-08 Mon — only Fri and Mon written
        self._run_backfill(tmp_path, date(2024, 1, 5), date(2024, 1, 8))
        names = {f.name for f in tmp_path.glob("*.csv")}
        assert "2024-01-06.csv" not in names
        assert "2024-01-07.csv" not in names
        assert "2024-01-05.csv" in names
        assert "2024-01-08.csv" in names

    def test_output_files_contain_tickers(self, tmp_path: Path) -> None:
        self._run_backfill(tmp_path, date(2021, 1, 4), date(2021, 1, 4))
        path = tmp_path / "2021-01-04.csv"
        assert path.exists()
        rows = list(csv.DictReader(path.open()))
        tickers = [r["ticker"] for r in rows]
        assert len(tickers) > 0
        assert "AAPL" in tickers

    def test_idempotent_skips_existing_files(self, tmp_path: Path) -> None:
        self._run_backfill(tmp_path, date(2021, 1, 4), date(2021, 1, 8))
        first_mtime = (tmp_path / "2021-01-04.csv").stat().st_mtime

        self._run_backfill(tmp_path, date(2021, 1, 4), date(2021, 1, 8))
        second_mtime = (tmp_path / "2021-01-04.csv").stat().st_mtime

        assert first_mtime == second_mtime  # file was not re-written

    def test_overwrite_rewrites_existing_files(self, tmp_path: Path) -> None:
        self._run_backfill(tmp_path, date(2021, 1, 4), date(2021, 1, 4))
        first_mtime = (tmp_path / "2021-01-04.csv").stat().st_mtime

        import time
        time.sleep(0.01)  # ensure mtime changes

        self._run_backfill(tmp_path, date(2021, 1, 4), date(2021, 1, 4), overwrite=True)
        second_mtime = (tmp_path / "2021-01-04.csv").stat().st_mtime

        assert second_mtime > first_mtime
