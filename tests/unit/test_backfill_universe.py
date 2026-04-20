"""Unit tests for app/lab/data_pipelines/backfill_universe.py."""
from __future__ import annotations

import csv
import json
from datetime import date
from itertools import zip_longest
from pathlib import Path
from unittest.mock import patch

from app.lab.data_pipelines.backfill_universe import (
    _business_days,
    _process_date,
    _write_membership,
    backfill,
)
from services.wikipedia.sp500_universe import ChangeEvent

FIXTURE_PATH = Path("data/sample/sp500_changes_fixture.json")


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _load_fixture() -> dict:
    return json.loads(FIXTURE_PATH.read_text())


def _build_html(fixture: dict) -> str:
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


def _make_events() -> list[ChangeEvent]:
    return [
        ChangeEvent(date="2019-06-24", added=frozenset(["BIO"]), removed=frozenset(["CELG"])),
        ChangeEvent(date="2020-09-21", added=frozenset(["ETSY"]), removed=frozenset(["FOX"])),
        ChangeEvent(date="2020-12-21", added=frozenset(["TSLA"]), removed=frozenset(["XRX"])),
    ]


def _make_current() -> set[str]:
    return {"AAPL", "MSFT", "TSLA", "ETSY", "BIO"}


# ---------------------------------------------------------------------------
# _business_days
# ---------------------------------------------------------------------------

class TestBusinessDays:
    def test_skips_saturday_and_sunday(self) -> None:
        days = _business_days(date(2024, 1, 5), date(2024, 1, 8))
        assert date(2024, 1, 6) not in days
        assert date(2024, 1, 7) not in days

    def test_includes_friday_and_monday(self) -> None:
        days = _business_days(date(2024, 1, 5), date(2024, 1, 8))
        assert date(2024, 1, 5) in days
        assert date(2024, 1, 8) in days

    def test_single_weekday(self) -> None:
        assert _business_days(date(2024, 1, 2), date(2024, 1, 2)) == [date(2024, 1, 2)]

    def test_single_weekend_day_returns_empty(self) -> None:
        assert _business_days(date(2024, 1, 6), date(2024, 1, 6)) == []

    def test_full_week_has_five_days(self) -> None:
        assert len(_business_days(date(2024, 1, 1), date(2024, 1, 7))) == 5

    def test_from_after_to_returns_empty(self) -> None:
        assert _business_days(date(2024, 1, 5), date(2024, 1, 4)) == []


# ---------------------------------------------------------------------------
# _write_membership
# ---------------------------------------------------------------------------

class TestWriteMembership:
    def test_creates_csv_with_header_and_tickers(self, tmp_path: Path) -> None:
        with patch("app.lab.data_pipelines.backfill_universe.OUTPUT_DIR", tmp_path):
            _write_membership(date(2020, 1, 2), ["AAPL", "MSFT", "AMZN"])
            rows = list(csv.DictReader((tmp_path / "2020-01-02.csv").open()))
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
# _process_date
# ---------------------------------------------------------------------------

class TestProcessDate:
    def test_writes_file_and_returns_true(self, tmp_path: Path) -> None:
        with patch("app.lab.data_pipelines.backfill_universe.OUTPUT_DIR", tmp_path):
            result = _process_date(date(2021, 1, 4), _make_current(), _make_events(), overwrite=False)
            assert result is True
            assert (tmp_path / "2021-01-04.csv").exists()

    def test_skips_existing_file_without_overwrite(self, tmp_path: Path) -> None:
        with patch("app.lab.data_pipelines.backfill_universe.OUTPUT_DIR", tmp_path):
            _process_date(date(2021, 1, 4), _make_current(), _make_events(), overwrite=False)
            mtime = (tmp_path / "2021-01-04.csv").stat().st_mtime

            result = _process_date(date(2021, 1, 4), _make_current(), _make_events(), overwrite=False)
            assert result is False
            assert (tmp_path / "2021-01-04.csv").stat().st_mtime == mtime

    def test_overwrites_existing_file_when_flag_set(self, tmp_path: Path) -> None:
        import time
        with patch("app.lab.data_pipelines.backfill_universe.OUTPUT_DIR", tmp_path):
            _process_date(date(2021, 1, 4), _make_current(), _make_events(), overwrite=False)
            mtime1 = (tmp_path / "2021-01-04.csv").stat().st_mtime
            time.sleep(0.01)

            result = _process_date(date(2021, 1, 4), _make_current(), _make_events(), overwrite=True)
            assert result is True
            assert (tmp_path / "2021-01-04.csv").stat().st_mtime > mtime1

    def test_tsla_absent_before_addition(self, tmp_path: Path) -> None:
        with patch("app.lab.data_pipelines.backfill_universe.OUTPUT_DIR", tmp_path):
            _process_date(date(2020, 12, 20), _make_current(), _make_events(), overwrite=False)
            rows = list(csv.DictReader((tmp_path / "2020-12-20.csv").open()))
            tickers = [r["ticker"] for r in rows]
            assert "TSLA" not in tickers

    def test_tsla_present_on_addition_date(self, tmp_path: Path) -> None:
        with patch("app.lab.data_pipelines.backfill_universe.OUTPUT_DIR", tmp_path):
            _process_date(date(2020, 12, 21), _make_current(), _make_events(), overwrite=False)
            rows = list(csv.DictReader((tmp_path / "2020-12-21.csv").open()))
            tickers = [r["ticker"] for r in rows]
            assert "TSLA" in tickers


# ---------------------------------------------------------------------------
# backfill (end-to-end with mocked HTML fetch)
# ---------------------------------------------------------------------------

class TestBackfill:
    def _run(
        self,
        tmp_path: Path,
        from_date: date,
        to_date: date,
        overwrite: bool = False,
        max_workers: int = 4,
    ) -> None:
        html = _build_html(_load_fixture())
        with (
            patch("app.lab.data_pipelines.backfill_universe.OUTPUT_DIR", tmp_path),
            patch(
                "app.lab.data_pipelines.backfill_universe.fetch_html",
                return_value=html,
            ),
        ):
            backfill(from_date, to_date, overwrite=overwrite, max_workers=max_workers)

    def test_creates_one_file_per_business_day(self, tmp_path: Path) -> None:
        self._run(tmp_path, date(2024, 1, 8), date(2024, 1, 12))
        assert len(list(tmp_path.glob("*.csv"))) == 5

    def test_skips_weekends(self, tmp_path: Path) -> None:
        self._run(tmp_path, date(2024, 1, 5), date(2024, 1, 8))
        names = {f.name for f in tmp_path.glob("*.csv")}
        assert "2024-01-06.csv" not in names
        assert "2024-01-07.csv" not in names
        assert "2024-01-05.csv" in names
        assert "2024-01-08.csv" in names

    def test_output_contains_tickers(self, tmp_path: Path) -> None:
        self._run(tmp_path, date(2021, 1, 4), date(2021, 1, 4))
        rows = list(csv.DictReader((tmp_path / "2021-01-04.csv").open()))
        assert len(rows) > 0
        assert any(r["ticker"] == "AAPL" for r in rows)

    def test_idempotent_skips_existing(self, tmp_path: Path) -> None:
        self._run(tmp_path, date(2021, 1, 4), date(2021, 1, 4))
        mtime1 = (tmp_path / "2021-01-04.csv").stat().st_mtime
        self._run(tmp_path, date(2021, 1, 4), date(2021, 1, 4))
        assert (tmp_path / "2021-01-04.csv").stat().st_mtime == mtime1

    def test_overwrite_rewrites(self, tmp_path: Path) -> None:
        import time
        self._run(tmp_path, date(2021, 1, 4), date(2021, 1, 4))
        mtime1 = (tmp_path / "2021-01-04.csv").stat().st_mtime
        time.sleep(0.01)
        self._run(tmp_path, date(2021, 1, 4), date(2021, 1, 4), overwrite=True)
        assert (tmp_path / "2021-01-04.csv").stat().st_mtime > mtime1

    def test_parallel_produces_same_count_as_sequential(self, tmp_path: Path) -> None:
        tmp1 = tmp_path / "parallel"
        tmp2 = tmp_path / "sequential"
        html = _build_html(_load_fixture())

        for out, workers in [(tmp1, 8), (tmp2, 1)]:
            with (
                patch("app.lab.data_pipelines.backfill_universe.OUTPUT_DIR", out),
                patch(
                    "app.lab.data_pipelines.backfill_universe.fetch_html",
                    return_value=html,
                ),
            ):
                backfill(date(2024, 1, 8), date(2024, 1, 12), max_workers=workers)

        assert len(list(tmp1.glob("*.csv"))) == len(list(tmp2.glob("*.csv")))
