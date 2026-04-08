"""Point-in-time S&P 500 universe builder using Wikipedia's change log.

Parses the addition/removal history from the Wikipedia S&P 500 article to
reconstruct which tickers were constituents on any historical date.

Never use the current constituent table alone — it causes survivorship bias.
"""
from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import requests
from bs4 import BeautifulSoup
from loguru import logger

WIKIPEDIA_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
DEFAULT_CACHE_PATH = Path("data/cache/sp500_wikipedia.html")
CACHE_MAX_AGE_HOURS = 24

# Historical/alternate symbols mapped to canonical symbols used by downstream
# fetch pipelines. Keep this focused on observed S&P 500 history edge cases.
_TICKER_CANONICAL_MAP: dict[str, str] = {
    "BRK.B": "BRK-B",
    "BF.B": "BF-B",
    "FB": "META",
    "UA": "UAA",
    "WLTW": "WTW",
    "RE": "EG",
    "Q": "IQV",
    "FLT": "CPAY",
    "CDAY": "DAY",
}


@dataclass(frozen=True)
class _ChangeEvent:
    """One addition/removal event from the Wikipedia change log."""

    date: str          # YYYY-MM-DD
    added: frozenset[str]
    removed: frozenset[str]


def _canonicalize_ticker(ticker: str) -> str:
    """Normalize ticker formatting and map historical aliases to canonical symbols."""
    normalized = ticker.strip().upper().replace(".", "-")
    return _TICKER_CANONICAL_MAP.get(normalized, normalized)


def _fetch_html(cache_path: Path = DEFAULT_CACHE_PATH) -> str:
    """Return Wikipedia HTML from cache if fresh, otherwise fetch and cache it."""
    if cache_path.exists():
        age_hours = (time.time() - cache_path.stat().st_mtime) / 3600
        if age_hours < CACHE_MAX_AGE_HOURS:
            logger.debug("Using cached Wikipedia HTML (age={:.1f}h)", age_hours)
            return cache_path.read_text(encoding="utf-8")

    logger.info("Fetching Wikipedia S&P 500 page from {}", WIKIPEDIA_URL)
    resp = requests.get(WIKIPEDIA_URL, timeout=30, headers={"User-Agent": "sp500-universe-builder/1.0"})
    resp.raise_for_status()
    html = resp.text

    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_text(html, encoding="utf-8")
    logger.debug("Cached Wikipedia HTML to {}", cache_path)
    return html


def _parse_current_tickers(html: str) -> set[str]:
    """Parse the current S&P 500 constituent table.

    Returns the set of ticker symbols currently in the index.
    """
    soup = BeautifulSoup(html, "lxml")
    table = soup.find("table", {"id": "constituents"})
    if table is None:
        raise ValueError("Could not find 'constituents' table on Wikipedia S&P 500 page")

    tickers: set[str] = set()
    for row in table.find_all("tr")[1:]:  # skip header
        cells = row.find_all("td")
        if cells:
            ticker = _canonicalize_ticker(cells[0].get_text(strip=True))
            if ticker:
                tickers.add(ticker)

    logger.debug("Parsed {} current S&P 500 tickers from Wikipedia", len(tickers))
    return tickers


def _parse_change_log(html: str) -> list[_ChangeEvent]:
    """Parse the historical additions/removals table.

    Returns events sorted by date ascending. Each event captures all tickers
    added and removed on a given date.
    """
    soup = BeautifulSoup(html, "lxml")
    table = soup.find("table", {"id": "changes"})
    if table is None:
        raise ValueError("Could not find 'changes' table on Wikipedia S&P 500 page")

    raw_events: dict[str, dict[str, set[str]]] = {}

    for row in table.find_all("tr")[1:]:
        cells = row.find_all("td")
        if len(cells) < 4:
            continue

        raw_date = cells[0].get_text(strip=True)
        added_ticker = _canonicalize_ticker(cells[1].get_text(strip=True))
        removed_ticker = _canonicalize_ticker(cells[3].get_text(strip=True))

        # Normalize date to YYYY-MM-DD
        date = _normalize_date(raw_date)
        if date is None:
            logger.warning("Skipping unparseable date: {!r}", raw_date)
            continue

        if date not in raw_events:
            raw_events[date] = {"added": set(), "removed": set()}

        if added_ticker:
            raw_events[date]["added"].add(added_ticker)
        if removed_ticker:
            raw_events[date]["removed"].add(removed_ticker)

    events = [
        _ChangeEvent(
            date=date,
            added=frozenset(v["added"]),
            removed=frozenset(v["removed"]),
        )
        for date, v in sorted(raw_events.items())
    ]
    logger.debug("Parsed {} change events from Wikipedia", len(events))
    return events


def _normalize_date(raw: str) -> str | None:
    """Attempt to parse a date string into YYYY-MM-DD format.

    Returns None if the string cannot be parsed.
    """
    for fmt in ("%B %d, %Y", "%Y-%m-%d", "%m/%d/%Y", "%d %B %Y"):
        try:
            return datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def _reconstruct_at_date(
    current_tickers: set[str],
    events: list[_ChangeEvent],
    date: str,
) -> list[str]:
    """Reconstruct S&P 500 constituents as of `date` by reversing later changes.

    Algorithm:
    - Start with the current constituent set.
    - Walk change events in reverse chronological order.
    - For every event that occurred AFTER `date`, reverse it:
        additions become removals (ticker wasn't there yet)
        removals become additions (ticker was still there)
    """
    constituents = set(current_tickers)

    for event in sorted(events, key=lambda e: e.date, reverse=True):
        if event.date > date:
            # This change happened after our query date — undo it
            constituents -= event.added
            constituents |= event.removed

    return sorted(constituents)


def get_constituents(date: str, _html: str | None = None) -> list[str]:
    """Return S&P 500 constituent tickers as of the given date.

    Args:
        date: Target date in YYYY-MM-DD format.
        _html: Optional HTML string for testing (skips network and cache).

    Returns:
        Sorted list of ticker symbols that were in the S&P 500 on `date`.

    Raises:
        ValueError: If the date format is invalid or precedes Wikipedia records.
        requests.HTTPError: If the Wikipedia fetch fails.
    """
    try:
        datetime.strptime(date, "%Y-%m-%d")
    except ValueError as exc:
        raise ValueError(f"date must be YYYY-MM-DD, got {date!r}") from exc

    html = _html if _html is not None else _fetch_html()
    current_tickers = _parse_current_tickers(html)
    events = _parse_change_log(html)

    if events and date < events[0].date:
        logger.warning(
            "Query date {} precedes earliest Wikipedia change record {}; "
            "result may be incomplete",
            date,
            events[0].date,
        )

    result = _reconstruct_at_date(current_tickers, events, date)
    logger.info("get_constituents({}): {} tickers", date, len(result))
    return result


def get_all_historical_tickers(
    from_date: str,
    to_date: str,
    _html: str | None = None,
) -> set[str]:
    """Return all tickers that were in the S&P 500 at any point in [from_date, to_date].

    Used by the OHLCV and news backfill scripts to determine the full fetch scope.
    Includes tickers that entered the index, left the index, or were present
    throughout the period.

    Args:
        from_date: Start of range, YYYY-MM-DD (inclusive).
        to_date: End of range, YYYY-MM-DD (inclusive).
        _html: Optional HTML string for testing (skips network and cache).

    Returns:
        Set of ticker symbols ever present in the S&P 500 during the range.

    Raises:
        ValueError: If date format is invalid or from_date > to_date.
    """
    for label, d in [("from_date", from_date), ("to_date", to_date)]:
        try:
            datetime.strptime(d, "%Y-%m-%d")
        except ValueError as exc:
            raise ValueError(f"{label} must be YYYY-MM-DD, got {d!r}") from exc

    if from_date > to_date:
        raise ValueError(f"from_date {from_date} must be <= to_date {to_date}")

    html = _html if _html is not None else _fetch_html()
    current_tickers = _parse_current_tickers(html)
    events = _parse_change_log(html)

    # Tickers present at from_date = the starting universe
    universe_at_start = set(_reconstruct_at_date(current_tickers, events, from_date))

    # Any ticker added during the range was also in the index for some of the period
    added_during = set()
    removed_during = set()
    for event in events:
        if from_date <= event.date <= to_date:
            added_during |= set(event.added)
            removed_during |= set(event.removed)

    all_tickers = universe_at_start | added_during | removed_during
    logger.info(
        "get_all_historical_tickers({}, {}): {} unique tickers "
        "(start={}, added={}, removed={})",
        from_date,
        to_date,
        len(all_tickers),
        len(universe_at_start),
        len(added_during),
        len(removed_during),
    )
    return all_tickers
