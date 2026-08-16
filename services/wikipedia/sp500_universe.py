"""Point-in-time S&P 500 universe builder using Wikipedia's change log.

Parses the addition/removal history from the Wikipedia S&P 500 article to
reconstruct which tickers were constituents on any historical date.

Never use the current constituent table alone — it causes survivorship bias.
"""
from __future__ import annotations

import re
import tempfile
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import requests
from bs4 import BeautifulSoup
from loguru import logger

WIKIPEDIA_CURRENT_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
WIKIPEDIA_HISTORICAL_URL = "https://en.wikipedia.org/wiki/Historical_components_of_the_S%26P_500"
# Backward-compatible name for callers that only need the canonical current page.
WIKIPEDIA_URL = WIKIPEDIA_CURRENT_URL
DEFAULT_CACHE_PATH = Path("data/cache/sp500_wikipedia.html")
CACHE_MAX_AGE_HOURS = 24

# Conservative source-contract floors protect the cache boundary from publishing a
# syntactically valid but historically truncated Wikipedia generation. These are
# coverage floors, not assertions about the current live event count or latest date.
MIN_HISTORICAL_EVENT_COUNT = 100
MIN_HISTORICAL_EVENT_DATE = "1980-01-01"
MAX_HISTORICAL_EVENT_DATE = "2017-01-01"

# Conflict-free historical/current symbol aliases. Ambiguous symbols that map to
# different securities across time are handled separately with date-bounded
# resolution rules.
_TICKER_CANONICAL_MAP: dict[str, str] = {
    "BRK.B": "BRK-B",
    "BF.B": "BF-B",
    "FB": "META",
    "WLTW": "WTW",
    "RE": "EG",
    "FLT": "CPAY",
    "CDAY": "DAY",
}
_UNDER_ARMOUR_CLASS_C_SPLIT_DATE = "2016-04-08"
_IQVIA_SNP_ENTRY_DATE = "2017-08-29"


@dataclass(frozen=True)
class SymbolIdentityResolution:
    """Resolved security identity for one raw Wikipedia ticker symbol."""

    raw_ticker: str
    resolved_ticker: str
    reason_code: str


@dataclass(frozen=True)
class ChangeEvent:
    """One addition/removal event from the Wikipedia change log."""

    date: str          # YYYY-MM-DD
    added: frozenset[str]
    removed: frozenset[str]
    added_details: tuple[SymbolIdentityResolution, ...] = ()
    removed_details: tuple[SymbolIdentityResolution, ...] = ()


def validate_supported_start_date(query_date: str, earliest_event_date: str, label: str) -> None:
    """Fail fast when a query date precedes available change-log history."""
    if query_date < earliest_event_date:
        raise ValueError(
            f"{label} {query_date} precedes earliest Wikipedia change record "
            f"{earliest_event_date}; point-in-time reconstruction is unsupported"
        )


def canonicalize_ticker(ticker: str) -> str:
    """Normalize ticker formatting and known historical aliases."""
    return _canonicalize_ticker(ticker)


def _canonicalize_ticker(ticker: str) -> str:
    """Normalize ticker formatting and conflict-free historical aliases."""
    normalized = _normalize_ticker(ticker)
    return _TICKER_CANONICAL_MAP.get(normalized, normalized)


def _normalize_ticker(ticker: str) -> str:
    """Normalize ticker formatting without applying identity-alias logic."""
    normalized = re.sub(r"\s*\|\s*$", "", ticker.strip())
    return normalized.upper().replace(".", "-")


def _resolve_change_event_ticker(
    ticker: str,
    *,
    event_date: str,
) -> SymbolIdentityResolution | None:
    """Resolve one raw change-log symbol to the Layer 0 security identity."""
    normalized = _normalize_ticker(ticker)
    if not normalized:
        return None
    if normalized == "UA" and event_date < _UNDER_ARMOUR_CLASS_C_SPLIT_DATE:
        return SymbolIdentityResolution(
            raw_ticker=normalized,
            resolved_ticker="UAA",
            reason_code="pre_2016_under_armour_class_a_alias",
        )
    if normalized == "Q" and event_date >= _IQVIA_SNP_ENTRY_DATE:
        return SymbolIdentityResolution(
            raw_ticker=normalized,
            resolved_ticker="IQV",
            reason_code="post_2017_iqvia_alias",
        )
    resolved = _canonicalize_ticker(normalized)
    if resolved != normalized:
        return SymbolIdentityResolution(
            raw_ticker=normalized,
            resolved_ticker=resolved,
            reason_code="conflict_free_current_symbol_alias",
        )
    return SymbolIdentityResolution(
        raw_ticker=normalized,
        resolved_ticker=normalized,
        reason_code="identity_preserved",
    )


def _resolve_current_table_ticker(ticker: str) -> str:
    """Resolve one current-constituents-table symbol to the live security identity."""
    normalized = _normalize_ticker(ticker)
    if not normalized:
        return ""
    if normalized == "Q":
        return "IQV"
    return _canonicalize_ticker(normalized)


def _extract_table(html: str, table_id: str) -> str:
    """Extract one required table from a provider response."""
    soup = BeautifulSoup(html, "lxml")
    table = soup.find("table", {"id": table_id})
    if table is None:
        raise ValueError(f"Wikipedia response is missing table#{table_id}")
    return str(table)


def _validate_combined_html(html: str) -> None:
    """Validate required tables and parsed content before cache use/publication."""
    current = parse_current_tickers(html)
    events = parse_change_log(html)
    if not current or not events:
        raise ValueError("Wikipedia source contains an empty current or historical set")
    if not 400 <= len(current) <= 600 or len(events) > 10000:
        raise ValueError("Wikipedia source contains implausibly large sets")
    event_dates = {event.date for event in events}
    if (
        len(event_dates) < MIN_HISTORICAL_EVENT_COUNT
        or min(event_dates) > MIN_HISTORICAL_EVENT_DATE
        or max(event_dates) < MAX_HISTORICAL_EVENT_DATE
    ):
        raise ValueError("Wikipedia source fails historical coverage plausibility floors")
    ticker_pattern = re.compile(r"^[A-Z]{1,5}(?:-[A-Z])?$")
    if any(not ticker_pattern.fullmatch(ticker) for ticker in current):
        raise ValueError("Wikipedia source contains an invalid current ticker identity")
    dates = [event.date for event in events]
    if dates != sorted(set(dates)):
        raise ValueError("Wikipedia historical events are not unique and chronological")
    for event in events:
        if not event.added and not event.removed:
            raise ValueError("Wikipedia historical event has no ticker identities")
        if any(not ticker_pattern.fullmatch(ticker) for ticker in event.added | event.removed):
            raise ValueError("Wikipedia source contains an invalid historical ticker identity")


def _combine_source_pages(current_html: str, historical_html: str) -> str:
    """Build the deterministic cache representation from one remote generation."""
    combined = "<html><body>{}{}</body></html>".format(
        _extract_table(current_html, "constituents"),
        _extract_table(historical_html, "changes"),
    )
    _validate_combined_html(combined)
    return combined


def _fetch_page(url: str) -> str:
    """Fetch one Wikipedia page for a source generation."""
    response = requests.get(url, timeout=30, headers={"User-Agent": "sp500-universe-builder/1.0"})
    response.raise_for_status()
    return response.text


def fetch_html(cache_path: Path = DEFAULT_CACHE_PATH) -> str:
    """Return a validated split-page generation, retaining valid stale cache on failure."""
    stale_html: str | None = None
    stale_state = "no cache"
    if cache_path.exists():
        try:
            candidate = cache_path.read_text(encoding="utf-8")
            _validate_combined_html(candidate)
            age_hours = (time.time() - cache_path.stat().st_mtime) / 3600
            if age_hours < CACHE_MAX_AGE_HOURS:
                logger.debug("Using validated cached Wikipedia HTML (age={:.1f}h)", age_hours)
                return candidate
            stale_html = candidate
            stale_state = "valid stale cache"
        except (OSError, ValueError) as exc:
            stale_state = f"invalid cache ({type(exc).__name__})"

    temporary_path: Path | None = None
    try:
        logger.info("Fetching Wikipedia S&P 500 source generation")
        combined = _combine_source_pages(
            _fetch_page(WIKIPEDIA_CURRENT_URL),
            _fetch_page(WIKIPEDIA_HISTORICAL_URL),
        )
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", dir=cache_path.parent, prefix=f".{cache_path.name}.", delete=False
        ) as temporary:
            temporary_path = Path(temporary.name)
            temporary.write(combined)
        temporary_path.replace(cache_path)
        logger.debug("Atomically cached validated Wikipedia generation to {}", cache_path)
        return combined
    except (OSError, ValueError, requests.RequestException) as exc:
        if stale_html is not None:
            logger.warning(
                "Wikipedia refresh failed; using {} ({})", stale_state, type(exc).__name__
            )
            return stale_html
        logger.error("Wikipedia refresh failed with no valid cache ({})", type(exc).__name__)
        if isinstance(exc, requests.RequestException):
            raise
        raise RuntimeError("No valid Wikipedia universe source generation is available") from exc
    finally:
        if temporary_path is not None and temporary_path.exists():
            try:
                temporary_path.unlink()
            except OSError as cleanup_exc:
                logger.warning(
                    "Wikipedia temporary cache cleanup failed ({}; exists={})",
                    type(cleanup_exc).__name__,
                    temporary_path.exists(),
                )


def parse_current_tickers(html: str) -> set[str]:
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
            ticker = _resolve_current_table_ticker(cells[0].get_text(strip=True))
            if ticker:
                tickers.add(ticker)

    logger.debug("Parsed {} current S&P 500 tickers from Wikipedia", len(tickers))
    return tickers


def parse_change_log(html: str) -> list[ChangeEvent]:
    """Parse the historical additions/removals table.

    Returns events sorted by date ascending. Each event captures all tickers
    added and removed on a given date.
    """
    soup = BeautifulSoup(html, "lxml")
    table = soup.find("table", {"id": "changes"})
    if table is None:
        raise ValueError("Could not find 'changes' table on Wikipedia S&P 500 page")

    raw_events: dict[str, dict[str, dict[str, SymbolIdentityResolution]]] = {}

    for row in table.find_all("tr")[1:]:
        cells = row.find_all("td")
        if len(cells) < 4:
            continue

        raw_date = cells[0].get_text(strip=True)
        added_ticker = _normalize_ticker(cells[1].get_text(strip=True))
        removed_ticker = _normalize_ticker(cells[3].get_text(strip=True))

        # Normalize date to YYYY-MM-DD
        date = _normalize_date(raw_date)
        if date is None:
            raise ValueError(f"Wikipedia historical event has an unparseable date: {raw_date!r}")

        if date not in raw_events:
            raw_events[date] = {"added": {}, "removed": {}}

        added_resolution = _resolve_change_event_ticker(added_ticker, event_date=date)
        removed_resolution = _resolve_change_event_ticker(removed_ticker, event_date=date)
        if added_resolution is not None:
            raw_events[date]["added"][added_resolution.resolved_ticker] = added_resolution
        if removed_resolution is not None:
            raw_events[date]["removed"][removed_resolution.resolved_ticker] = removed_resolution

    events = [
        ChangeEvent(
            date=date,
            added=frozenset(v["added"]),
            removed=frozenset(v["removed"]),
            added_details=tuple(
                sorted(v["added"].values(), key=lambda detail: detail.resolved_ticker)
            ),
            removed_details=tuple(
                sorted(v["removed"].values(), key=lambda detail: detail.resolved_ticker)
            ),
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


def reconstruct_at_date(
    current_tickers: set[str],
    events: list[ChangeEvent],
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

    for event in reversed(events):
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

    html = _html if _html is not None else fetch_html()
    current_tickers = parse_current_tickers(html)
    events = parse_change_log(html)

    if events and date < events[0].date:
        validate_supported_start_date(date, events[0].date, label="date")

    result = reconstruct_at_date(current_tickers, events, date)
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

    html = _html if _html is not None else fetch_html()
    current_tickers = parse_current_tickers(html)
    events = parse_change_log(html)

    if events and from_date < events[0].date:
        validate_supported_start_date(from_date, events[0].date, label="from_date")

    # Tickers present at from_date = the starting universe
    universe_at_start = set(reconstruct_at_date(current_tickers, events, from_date))

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
