from __future__ import annotations

import time
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date as Date
from typing import Any, Protocol

import requests

from core.contracts.schemas import OHLCVRecord
from services.alpaca.market_data import (
    ALPACA_STOCK_BARS_ENDPOINT,
    DEFAULT_ALPACA_PAGE_LIMIT,
    AlpacaMarketDataConfig,
    normalize_alpaca_bar_response,
)

DEFAULT_ALPACA_HISTORICAL_FEED = "sip"
DEFAULT_ALPACA_HISTORICAL_ADJUSTMENT = "all"
DEFAULT_ALPACA_REQUESTS_PER_MINUTE = 200


class HistoricalSecurity(Protocol):
    """Security identity fields needed by the Alpaca historical fetcher."""

    ticker: str


@dataclass(frozen=True)
class AlpacaTickerSecurity:
    """Ticker-keyed security identity for Alpaca SIP historical archives."""

    ticker: str
    start_date: str | None = None
    end_date: str | None = None

    @property
    def security_id(self) -> str:
        """Return the canonical raw price archive key for this Alpaca ticker."""
        return _normalize_ticker(self.ticker)

    def to_reference_row(self) -> dict[str, str | None]:
        """Serialize this identity for the raw reference archive."""
        return {
            "ticker": self.security_id,
            "security_id": self.security_id,
            "source": "alpaca_sip",
            "start_date": self.start_date,
            "end_date": self.end_date,
        }


class AlpacaTickerSecurityMaster:
    """Resolve historical tickers directly to Alpaca SIP ticker-keyed identities."""

    def resolve_all(self, ticker: str) -> list[AlpacaTickerSecurity]:
        """Resolve one ticker to the Alpaca identity used for raw price storage."""
        return [AlpacaTickerSecurity(ticker=_normalize_ticker(ticker))]


class AlpacaHistoricalOHLCVFetcher:
    """Fetch split/dividend-adjusted historical daily bars from Alpaca delayed SIP."""

    def __init__(
        self,
        config: AlpacaMarketDataConfig,
        session: requests.Session | None = None,
        *,
        feed: str = DEFAULT_ALPACA_HISTORICAL_FEED,
        adjustment: str = DEFAULT_ALPACA_HISTORICAL_ADJUSTMENT,
        min_request_interval_seconds: float | None = None,
    ) -> None:
        """Store Alpaca historical market-data settings and HTTP session."""
        self.config = config
        self.session = session or requests.Session()
        self.feed = _normalize_feed(feed)
        self.adjustment = _normalize_adjustment(adjustment)
        self.min_request_interval_seconds = (
            60.0 / DEFAULT_ALPACA_REQUESTS_PER_MINUTE
            if min_request_interval_seconds is None
            else min_request_interval_seconds
        )
        if self.min_request_interval_seconds < 0:
            raise ValueError("min_request_interval_seconds must be non-negative")
        self._last_request_at: float | None = None

    def fetch_price_page(
        self,
        *,
        ticker: str,
        from_date: str,
        to_date: str,
        page_token: str | None = None,
        limit: int = DEFAULT_ALPACA_PAGE_LIMIT,
    ) -> tuple[list[OHLCVRecord], str | None]:
        """Fetch one page of Alpaca adjusted 1Day bars for one ticker/date range."""
        normalized_ticker = _normalize_ticker(ticker)
        normalized_from = _validate_date(from_date, "from_date")
        normalized_to = _validate_date(to_date, "to_date")
        if normalized_from > normalized_to:
            raise ValueError("from_date must be <= to_date")
        if limit <= 0:
            raise ValueError("limit must be positive")

        params: dict[str, Any] = {
            "symbols": _alpaca_api_symbol(normalized_ticker),
            "timeframe": "1Day",
            "start": normalized_from,
            "end": normalized_to,
            "adjustment": self.adjustment,
            "feed": self.feed,
            "currency": "USD",
            "limit": limit,
            "sort": "asc",
        }
        if page_token:
            params["page_token"] = page_token

        payload = self._request_json(params=params)
        records = normalize_alpaca_bar_response(payload, requested_tickers=[normalized_ticker])
        return records, _optional_page_token(payload)

    def fetch_records(self, ticker: str, from_date: str, to_date: str) -> list[OHLCVRecord]:
        """Fetch all adjusted Alpaca daily bars for one ticker/date range."""
        _normalize_ticker(ticker)
        _validate_date(from_date, "from_date")
        _validate_date(to_date, "to_date")

        page_token: str | None = None
        records: list[OHLCVRecord] = []
        seen_keys: set[tuple[str, str]] = set()
        while True:
            page_records, page_token = self.fetch_price_page(
                ticker=ticker,
                from_date=from_date,
                to_date=to_date,
                page_token=page_token,
            )
            for record in page_records:
                key = (record.date, record.ticker)
                if key in seen_keys:
                    raise ValueError(
                        f"Duplicate Alpaca historical bar for {record.ticker} {record.date}"
                    )
                seen_keys.add(key)
                records.append(record)
            if not page_token:
                break
        return records

    def fetch_security_records(
        self,
        security: HistoricalSecurity,
        from_date: str,
        to_date: str,
    ) -> list[OHLCVRecord]:
        """Fetch records for a resolved Alpaca ticker identity."""
        return self.fetch_records(ticker=security.ticker, from_date=from_date, to_date=to_date)

    def _request_json(self, params: Mapping[str, Any]) -> Any:
        """Request one Alpaca bars payload with bounded retries and provider throttling."""
        url = f"{self.config.base_url.rstrip('/')}{ALPACA_STOCK_BARS_ENDPOINT}"
        headers = {
            "APCA-API-KEY-ID": self.config.api_key_id,
            "APCA-API-SECRET-KEY": self.config.api_secret_key,
        }
        last_error: requests.RequestException | None = None
        for attempt in range(self.config.max_retries + 1):
            self._throttle()
            try:
                response = self.session.get(
                    url,
                    headers=headers,
                    params=dict(params),
                    timeout=self.config.timeout_seconds,
                )
                response.raise_for_status()
                return response.json()
            except requests.RequestException as exc:
                last_error = exc
                if attempt >= self.config.max_retries or not _is_retryable_error(exc):
                    raise
                time.sleep(self.config.retry_sleep_seconds)
        if last_error is not None:
            raise last_error
        raise RuntimeError("Alpaca historical OHLCV request failed without an exception")

    def _throttle(self) -> None:
        """Sleep long enough to respect Alpaca's 200 request/minute market-data limit."""
        if self.min_request_interval_seconds == 0:
            self._last_request_at = time.monotonic()
            return
        now = time.monotonic()
        if self._last_request_at is not None:
            elapsed = now - self._last_request_at
            remaining = self.min_request_interval_seconds - elapsed
            if remaining > 0:
                time.sleep(remaining)
                now = time.monotonic()
        self._last_request_at = now


def _optional_page_token(payload: Mapping[str, Any]) -> str | None:
    """Return Alpaca's optional pagination token from a bars payload."""
    if not isinstance(payload, Mapping):
        raise ValueError("Alpaca bars response must be a JSON object")
    token = payload.get("next_page_token")
    if token is None:
        return None
    if not isinstance(token, str):
        raise TypeError("Alpaca next_page_token must be a string when present")
    stripped = token.strip()
    return stripped or None


def _normalize_ticker(ticker: str) -> str:
    """Normalize one Alpaca ticker symbol for requests and archive keys."""
    if not isinstance(ticker, str):
        raise TypeError("ticker must be a string")
    cleaned = ticker.strip().upper().replace(".", "-")
    if not cleaned:
        raise ValueError("ticker cannot be empty")
    return cleaned


def _alpaca_api_symbol(ticker: str) -> str:
    """Convert one canonical ticker to Alpaca's request symbol syntax."""
    return _normalize_ticker(ticker).replace("-", ".")


def _normalize_feed(feed: str) -> str:
    """Normalize and validate one Alpaca feed value."""
    if not isinstance(feed, str):
        raise TypeError("feed must be a string")
    cleaned = feed.strip().lower()
    if not cleaned:
        raise ValueError("feed cannot be empty")
    return cleaned


def _normalize_adjustment(adjustment: str) -> str:
    """Normalize and validate one Alpaca adjustment value."""
    if not isinstance(adjustment, str):
        raise TypeError("adjustment must be a string")
    cleaned = adjustment.strip().lower()
    if not cleaned:
        raise ValueError("adjustment cannot be empty")
    return cleaned


def _validate_date(value: str, field_name: str) -> str:
    """Validate and normalize a YYYY-MM-DD date string."""
    if not isinstance(value, str):
        raise TypeError(f"{field_name} must be a YYYY-MM-DD string")
    try:
        return Date.fromisoformat(value.strip()).isoformat()
    except ValueError as exc:
        raise ValueError(f"{field_name} must be YYYY-MM-DD: {value}") from exc


def _is_retryable_error(error: requests.RequestException) -> bool:
    """Return True when an Alpaca request error is likely transient."""
    response = getattr(error, "response", None)
    status_code = getattr(response, "status_code", None)
    return status_code in {429, 500, 502, 503, 504} or isinstance(
        error,
        (requests.ConnectionError, requests.Timeout),
    )
