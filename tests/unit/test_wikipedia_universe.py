"""Unit tests for services/wikipedia/sp500_universe.py.

All tests use data/sample/sp500_changes_fixture.json — no live HTTP requests.
"""
from __future__ import annotations

import json
from itertools import zip_longest
from pathlib import Path

import pytest

from services.wikipedia.sp500_universe import (
    _ChangeEvent,
    _normalize_date,
    _parse_change_log,
    _parse_current_tickers,
    _reconstruct_at_date,
    get_all_historical_tickers,
    get_constituents,
)

FIXTURE_PATH = Path("data/sample/sp500_changes_fixture.json")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _load_fixture() -> dict:
    return json.loads(FIXTURE_PATH.read_text())


def _build_html(fixture: dict) -> str:
    """Build minimal Wikipedia-style HTML from fixture dict.

    Produces a <table id="constituents"> and a <table id="changes"> that
    match what the real parsers expect, without any network calls.
    """
    # Current constituents table
    ticker_rows = "\n".join(
        f"<tr><td>{t}</td><td>Company Name</td></tr>"
        for t in fixture["current_tickers"]
    )
    constituents_table = (
        '<table id="constituents">'
        "<tr><th>Symbol</th><th>Security</th></tr>"
        f"{ticker_rows}"
        "</table>"
    )

    # Changes table — one row per (added_ticker, removed_ticker) pair per event.
    # If an event has more adds than removes (or vice versa), pad with empty strings.
    change_rows: list[str] = []
    for event in fixture["changes"]:
        added_list = event.get("added", [])
        removed_list = event.get("removed", [])
        for added, removed in zip_longest(added_list, removed_list, fillvalue=""):
            change_rows.append(
                f"<tr>"
                f"<td>{event['date']}</td>"
                f"<td>{added}</td>"
                f"<td>Added Security Name</td>"
                f"<td>{removed}</td>"
                f"<td>Removed Security Name</td>"
                f"</tr>"
            )

    changes_table = (
        '<table id="changes">'
        "<tr><th>Date</th><th>Added</th><th>Name</th><th>Removed</th><th>Name</th></tr>"
        + "".join(change_rows)
        + "</table>"
    )

    return f"<html><body>{constituents_table}{changes_table}</body></html>"


@pytest.fixture()
def fixture() -> dict:
    return _load_fixture()


@pytest.fixture()
def html(fixture: dict) -> str:
    return _build_html(fixture)


# ---------------------------------------------------------------------------
# _normalize_date
# ---------------------------------------------------------------------------

class TestNormalizeDate:
    def test_iso_format(self) -> None:
        assert _normalize_date("2020-12-21") == "2020-12-21"

    def test_long_us_format(self) -> None:
        assert _normalize_date("December 21, 2020") == "2020-12-21"

    def test_slash_format(self) -> None:
        assert _normalize_date("12/21/2020") == "2020-12-21"

    def test_day_month_year(self) -> None:
        assert _normalize_date("21 December 2020") == "2020-12-21"

    def test_unparseable_returns_none(self) -> None:
        assert _normalize_date("not a date") is None

    def test_empty_string_returns_none(self) -> None:
        assert _normalize_date("") is None


# ---------------------------------------------------------------------------
# _parse_current_tickers
# ---------------------------------------------------------------------------

class TestParseCurrentTickers:
    def test_returns_all_current_tickers(self, html: str, fixture: dict) -> None:
        result = _parse_current_tickers(html)
        assert result == set(fixture["current_tickers"])

    def test_missing_table_raises(self) -> None:
        with pytest.raises(ValueError, match="constituents"):
            _parse_current_tickers("<html><body></body></html>")


# ---------------------------------------------------------------------------
# _parse_change_log
# ---------------------------------------------------------------------------

class TestParseChangeLog:
    def test_returns_correct_event_count(self, html: str, fixture: dict) -> None:
        events = _parse_change_log(html)
        # One _ChangeEvent per unique date in the fixture
        unique_dates = {e["date"] for e in fixture["changes"]}
        assert len(events) == len(unique_dates)

    def test_events_sorted_ascending(self, html: str) -> None:
        events = _parse_change_log(html)
        dates = [e.date for e in events]
        assert dates == sorted(dates)

    def test_tsla_added_on_correct_date(self, html: str) -> None:
        events = _parse_change_log(html)
        event_map = {e.date: e for e in events}
        assert "TSLA" in event_map["2020-12-21"].added

    def test_xrx_removed_on_correct_date(self, html: str) -> None:
        events = _parse_change_log(html)
        event_map = {e.date: e for e in events}
        assert "XRX" in event_map["2020-12-21"].removed

    def test_multi_ticker_event(self, html: str) -> None:
        events = _parse_change_log(html)
        event_map = {e.date: e for e in events}
        sep_21 = event_map["2020-09-21"]
        assert {"ETSY", "PAYC", "POOL"} == set(sep_21.added)
        assert {"FOX", "FOXA", "HPE"} == set(sep_21.removed)

    def test_missing_table_raises(self) -> None:
        with pytest.raises(ValueError, match="changes"):
            _parse_change_log("<html><body></body></html>")


# ---------------------------------------------------------------------------
# _reconstruct_at_date (pure logic, no HTML)
# ---------------------------------------------------------------------------

class TestReconstructAtDate:
    """Test the core reconstruction algorithm directly with controlled data."""

    def _events(self) -> list[_ChangeEvent]:
        return [
            _ChangeEvent(date="2019-06-24", added=frozenset(["BIO", "KEYS"]), removed=frozenset(["CELG", "JEF"])),
            _ChangeEvent(date="2020-09-21", added=frozenset(["ETSY", "POOL"]), removed=frozenset(["FOX", "HPE"])),
            _ChangeEvent(date="2020-12-21", added=frozenset(["TSLA"]), removed=frozenset(["XRX"])),
        ]

    def _current(self) -> set[str]:
        # Current = after all events applied
        return {"AAPL", "MSFT", "TSLA", "ETSY", "POOL", "BIO", "KEYS"}

    def test_date_before_any_event(self) -> None:
        result = set(_reconstruct_at_date(self._current(), self._events(), "2018-01-01"))
        # All additions must be reversed; all removals must be restored
        assert "TSLA" not in result
        assert "XRX" in result
        assert "ETSY" not in result
        assert "FOX" in result
        assert "BIO" not in result
        assert "CELG" in result

    def test_date_exactly_on_event(self) -> None:
        # On the date of the event, the change has taken effect
        result = set(_reconstruct_at_date(self._current(), self._events(), "2020-12-21"))
        assert "TSLA" in result
        assert "XRX" not in result

    def test_date_one_day_before_event(self) -> None:
        result = set(_reconstruct_at_date(self._current(), self._events(), "2020-12-20"))
        assert "TSLA" not in result
        assert "XRX" in result

    def test_date_between_events(self) -> None:
        result = set(_reconstruct_at_date(self._current(), self._events(), "2020-10-01"))
        # After 2020-09-21 but before 2020-12-21
        assert "ETSY" in result
        assert "FOX" not in result
        assert "TSLA" not in result
        assert "XRX" in result

    def test_future_date_returns_current(self) -> None:
        result = set(_reconstruct_at_date(self._current(), self._events(), "2099-01-01"))
        assert result == self._current()


# ---------------------------------------------------------------------------
# get_constituents (public API, uses _html param to skip network)
# ---------------------------------------------------------------------------

class TestGetConstituents:
    def test_tsla_not_present_before_addition(self, html: str) -> None:
        result = get_constituents("2020-12-20", _html=html)
        assert "TSLA" not in result

    def test_tsla_present_on_addition_date(self, html: str) -> None:
        result = get_constituents("2020-12-21", _html=html)
        assert "TSLA" in result

    def test_xrx_present_before_removal(self, html: str) -> None:
        result = get_constituents("2020-12-20", _html=html)
        assert "XRX" in result

    def test_xrx_absent_after_removal(self, html: str) -> None:
        result = get_constituents("2020-12-21", _html=html)
        assert "XRX" not in result

    def test_returns_sorted_list(self, html: str) -> None:
        result = get_constituents("2021-01-01", _html=html)
        assert result == sorted(result)

    def test_invalid_date_format_raises(self, html: str) -> None:
        with pytest.raises(ValueError, match="YYYY-MM-DD"):
            get_constituents("21/12/2020", _html=html)

    def test_multi_ticker_event_all_added(self, html: str) -> None:
        result = get_constituents("2020-09-21", _html=html)
        assert "ETSY" in result
        assert "PAYC" in result
        assert "POOL" in result

    def test_multi_ticker_event_all_removed(self, html: str) -> None:
        result = get_constituents("2020-09-21", _html=html)
        assert "FOX" not in result
        assert "FOXA" not in result
        assert "HPE" not in result

    def test_multi_ticker_event_before_date(self, html: str) -> None:
        result = get_constituents("2020-09-20", _html=html)
        assert "ETSY" not in result
        assert "FOX" in result


# ---------------------------------------------------------------------------
# get_all_historical_tickers (public API, uses _html param to skip network)
# ---------------------------------------------------------------------------

class TestGetAllHistoricalTickers:
    def test_includes_ticker_added_during_range(self, html: str) -> None:
        result = get_all_historical_tickers("2020-01-01", "2021-01-01", _html=html)
        assert "TSLA" in result   # added 2020-12-21 (within range)
        assert "ETSY" in result   # added 2020-09-21 (within range)

    def test_includes_ticker_removed_during_range(self, html: str) -> None:
        result = get_all_historical_tickers("2020-01-01", "2021-01-01", _html=html)
        assert "XRX" in result    # removed 2020-12-21 (was present before removal)
        assert "FOX" in result    # removed 2020-09-21

    def test_excludes_ticker_never_in_range(self, html: str) -> None:
        # CELG was removed 2019-06-24, before the range 2020-2021
        result = get_all_historical_tickers("2020-01-01", "2021-01-01", _html=html)
        assert "CELG" not in result

    def test_includes_ticker_present_throughout(self, html: str) -> None:
        result = get_all_historical_tickers("2020-01-01", "2021-01-01", _html=html)
        assert "AAPL" in result
        assert "MSFT" in result

    def test_returns_set(self, html: str) -> None:
        result = get_all_historical_tickers("2020-01-01", "2021-01-01", _html=html)
        assert isinstance(result, set)

    def test_from_date_after_to_date_raises(self, html: str) -> None:
        with pytest.raises(ValueError, match="from_date"):
            get_all_historical_tickers("2021-01-01", "2020-01-01", _html=html)

    def test_invalid_from_date_raises(self, html: str) -> None:
        with pytest.raises(ValueError, match="from_date"):
            get_all_historical_tickers("not-a-date", "2021-01-01", _html=html)

    def test_invalid_to_date_raises(self, html: str) -> None:
        with pytest.raises(ValueError, match="to_date"):
            get_all_historical_tickers("2020-01-01", "bad", _html=html)

    def test_ticker_added_and_removed_within_range(self, html: str) -> None:
        # Ticker added and removed in the same range window must appear
        result = get_all_historical_tickers("2019-01-01", "2021-01-01", _html=html)
        # CELG removed 2019-06-24 (within range start-to-removal), was present at start
        assert "CELG" in result
