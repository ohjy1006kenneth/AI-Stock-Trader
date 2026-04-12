from __future__ import annotations

import os
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date as Date
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv

from core.contracts.schemas import OHLCVRecord
from core.data.ohlcv import build_ohlcv_record

ALPACA_API_KEY_ID_ENV = "ALPACA_API_KEY_ID"
ALPACA_API_SECRET_KEY_ENV = "ALPACA_API_SECRET_KEY"
ALPACA_LEGACY_API_KEY_ID_ENV = "APCA_API_KEY_ID"
ALPACA_LEGACY_API_SECRET_KEY_ENV = "APCA_API_SECRET_KEY"
ALPACA_DATA_BASE_URL_ENV = "ALPACA_DATA_BASE_URL"
ALPACA_DATA_FEED_ENV = "ALPACA_DATA_FEED"
DEFAULT_ALPACA_DATA_BASE_URL = "https://data.alpaca.markets"
DEFAULT_ALPACA_DATA_FEED = "iex"
ALPACA_STOCK_BARS_ENDPOINT = "/v2/stocks/bars"
DEFAULT_ALPACA_PAGE_LIMIT = 10000
ALPACA_ENV_FILE = Path(__file__).resolve().parents[2] / "config" / "alpaca.env"


@dataclass(frozen=True)
class AlpacaMarketDataConfig:
    """Configuration for Alpaca market-data HTTP clients."""

    api_key_id: str
    api_secret_key: str
    base_url: str = DEFAULT_ALPACA_DATA_BASE_URL
    feed: str = DEFAULT_ALPACA_DATA_FEED
    timeout_seconds: int = 30
    max_retries: int = 2
    retry_sleep_seconds: float = 1.0

    @classmethod
    def from_env(cls) -> AlpacaMarketDataConfig:
        """Build Alpaca market-data config from environment or config/alpaca.env."""
        _load_local_alpaca_env_file()
        api_key_id = os.getenv(ALPACA_API_KEY_ID_ENV) or os.getenv(
            ALPACA_LEGACY_API_KEY_ID_ENV
        )
        api_secret_key = os.getenv(ALPACA_API_SECRET_KEY_ENV) or os.getenv(
            ALPACA_LEGACY_API_SECRET_KEY_ENV
        )
        if not api_key_id:
            raise ValueError(
                "Missing required Alpaca environment variable: "
                f"{ALPACA_API_KEY_ID_ENV} or {ALPACA_LEGACY_API_KEY_ID_ENV}"
            )
        if not api_secret_key:
            raise ValueError(
                "Missing required Alpaca environment variable: "
                f"{ALPACA_API_SECRET_KEY_ENV} or {ALPACA_LEGACY_API_SECRET_KEY_ENV}"
            )
        return cls(
            api_key_id=api_key_id,
            api_secret_key=api_secret_key,
            base_url=os.getenv(ALPACA_DATA_BASE_URL_ENV) or DEFAULT_ALPACA_DATA_BASE_URL,
            feed=os.getenv(ALPACA_DATA_FEED_ENV) or DEFAULT_ALPACA_DATA_FEED,
        )


@dataclass(frozen=True)
class AlpacaDailyBarPage:
    """One page of Alpaca daily-bar records normalized to OHLCVRecord."""

    records: list[OHLCVRecord]
    next_page_token: str | None


class AlpacaMarketDataClient:
    """Fetch and normalize Alpaca live daily market bars."""

    def __init__(
        self,
        config: AlpacaMarketDataConfig,
        session: requests.Session | None = None,
    ) -> None:
        """Store Alpaca market-data configuration and HTTP session."""
        self.config = config
        self.session = session or requests.Session()

    def fetch_daily_bar_page(
        self,
        *,
        tickers: Sequence[str],
        as_of_date: str,
        page_token: str | None = None,
        limit: int = DEFAULT_ALPACA_PAGE_LIMIT,
    ) -> AlpacaDailyBarPage:
        """Fetch one live 1-day bar page for a ticker slice on one trading date."""
        normalized_tickers = _normalize_tickers(tickers)
        normalized_date = _validate_date(as_of_date, "as_of_date")
        if limit <= 0:
            raise ValueError("limit must be positive")

        params: dict[str, Any] = {
            "symbols": ",".join(normalized_tickers),
            "timeframe": "1Day",
            "start": normalized_date,
            "end": normalized_date,
            "adjustment": "raw",
            "asof": normalized_date,
            "feed": _normalize_feed(self.config.feed),
            "currency": "USD",
            "limit": limit,
            "sort": "asc",
        }
        if page_token:
            params["page_token"] = page_token

        payload = self._request_json(params=params)
        return AlpacaDailyBarPage(
            records=normalize_alpaca_bar_response(
                payload,
                requested_tickers=normalized_tickers,
                as_of_date=normalized_date,
            ),
            next_page_token=_optional_page_token(payload),
        )

    def fetch_live_daily_bars(
        self,
        *,
        tickers: Sequence[str],
        as_of_date: str,
        limit: int = DEFAULT_ALPACA_PAGE_LIMIT,
        max_pages: int | None = None,
    ) -> list[OHLCVRecord]:
        """Fetch all Alpaca live 1-day bars for one date without historical backfill."""
        _normalize_tickers(tickers)
        _validate_date(as_of_date, "as_of_date")
        if max_pages is not None and max_pages <= 0:
            raise ValueError("max_pages must be positive")

        page_token: str | None = None
        pages = 0
        records: list[OHLCVRecord] = []
        seen_keys: set[tuple[str, str]] = set()

        while True:
            page = self.fetch_daily_bar_page(
                tickers=tickers,
                as_of_date=as_of_date,
                page_token=page_token,
                limit=limit,
            )
            for record in page.records:
                key = (record.date, record.ticker)
                if key in seen_keys:
                    raise ValueError(
                        f"Duplicate Alpaca daily bar for {record.ticker} {record.date}"
                    )
                seen_keys.add(key)
                records.append(record)

            pages += 1
            if not page.next_page_token:
                break
            if max_pages is not None and pages >= max_pages:
                break
            page_token = page.next_page_token

        return records

    def _request_json(self, params: Mapping[str, Any]) -> Any:
        """Request one Alpaca market-data payload with bounded transient retries."""
        url = f"{self.config.base_url.rstrip('/')}{ALPACA_STOCK_BARS_ENDPOINT}"
        headers = {
            "APCA-API-KEY-ID": self.config.api_key_id,
            "APCA-API-SECRET-KEY": self.config.api_secret_key,
        }
        last_error: requests.RequestException | None = None
        for attempt in range(self.config.max_retries + 1):
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
        raise RuntimeError("Alpaca market-data request failed without an exception")


def normalize_alpaca_bar_response(
    payload: Mapping[str, Any],
    *,
    requested_tickers: Sequence[str] | None = None,
    as_of_date: str | None = None,
) -> list[OHLCVRecord]:
    """Normalize an Alpaca multi-symbol bars response into OHLCVRecord rows."""
    if not isinstance(payload, Mapping):
        raise ValueError("Alpaca bars response must be a JSON object")
    bars_payload = payload.get("bars")
    if bars_payload is None:
        return []
    if not isinstance(bars_payload, Mapping):
        raise ValueError("Alpaca bars response field bars must be an object")

    normalized_date = _validate_date(as_of_date, "as_of_date") if as_of_date else None
    symbols = (
        _normalize_tickers(requested_tickers)
        if requested_tickers is not None
        else tuple(_normalize_ticker(str(ticker)) for ticker in bars_payload.keys())
    )
    records: list[OHLCVRecord] = []
    seen_keys: set[tuple[str, str]] = set()

    for ticker in symbols:
        rows = bars_payload.get(ticker)
        if rows is None:
            continue
        if not isinstance(rows, list):
            raise ValueError(f"Alpaca bars for {ticker} must be a list")
        for row in rows:
            if not isinstance(row, Mapping):
                raise ValueError(f"Alpaca bar for {ticker} must be an object")
            record = _normalize_alpaca_daily_bar(
                ticker=ticker,
                row=row,
                as_of_date=normalized_date,
            )
            key = (record.date, record.ticker)
            if key in seen_keys:
                raise ValueError(f"Duplicate Alpaca daily bar for {record.ticker} {record.date}")
            seen_keys.add(key)
            records.append(record)

    return records


def _normalize_alpaca_daily_bar(
    *,
    ticker: str,
    row: Mapping[str, Any],
    as_of_date: str | None,
) -> OHLCVRecord:
    """Normalize one Alpaca daily-bar row to the canonical OHLCV contract."""
    close = _required_numeric(row, "c")
    volume = _required_numeric(row, "v")
    date = _normalize_alpaca_timestamp(row.get("t"))
    if as_of_date is not None and date != as_of_date:
        raise ValueError(f"Alpaca bar date {date} does not match requested date {as_of_date}")

    vwap = _optional_numeric(row, "vw")
    dollar_price = close if vwap is None else vwap
    return build_ohlcv_record(
        {
            "date": date,
            "ticker": ticker,
            "open": _required_numeric(row, "o"),
            "high": _required_numeric(row, "h"),
            "low": _required_numeric(row, "l"),
            "close": close,
            "volume": volume,
            "adj_close": close,
            "dollar_volume": dollar_price * volume,
        }
    )


def _optional_page_token(payload: Mapping[str, Any]) -> str | None:
    """Return an optional non-empty Alpaca pagination token."""
    token = payload.get("next_page_token")
    if token is None:
        return None
    if not isinstance(token, str):
        raise TypeError("Alpaca next_page_token must be a string when present")
    stripped = token.strip()
    return stripped or None


def _normalize_alpaca_timestamp(value: Any) -> str:
    """Normalize Alpaca RFC-3339 timestamps to YYYY-MM-DD trading-date strings."""
    if not isinstance(value, str):
        raise TypeError("Alpaca bar timestamp t must be a string")
    date_part = value.strip().split("T", maxsplit=1)[0]
    return _validate_date(date_part, "t")


def _required_numeric(row: Mapping[str, Any], field_name: str) -> int | float:
    """Return a required Alpaca numeric field without string coercion."""
    if field_name not in row or row[field_name] is None:
        raise ValueError(f"Missing required Alpaca bar field: {field_name}")
    value = row[field_name]
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"Alpaca bar field {field_name} must be numeric")
    return value


def _optional_numeric(row: Mapping[str, Any], field_name: str) -> int | float | None:
    """Return an optional Alpaca numeric field without string coercion."""
    if field_name not in row or row[field_name] is None:
        return None
    value = row[field_name]
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"Alpaca bar field {field_name} must be numeric")
    return value


def _normalize_tickers(tickers: Sequence[str]) -> tuple[str, ...]:
    """Validate and normalize a non-empty sequence of Alpaca ticker symbols."""
    if not tickers:
        raise ValueError("tickers must contain at least one ticker")
    normalized = tuple(_normalize_ticker(ticker) for ticker in tickers)
    if len(set(normalized)) != len(normalized):
        raise ValueError("tickers must not contain duplicates after normalization")
    return normalized


def _normalize_ticker(ticker: str) -> str:
    """Normalize one Alpaca ticker symbol."""
    if not isinstance(ticker, str):
        raise TypeError("ticker must be a string")
    cleaned = ticker.strip().upper()
    if not cleaned:
        raise ValueError("ticker cannot be empty")
    return cleaned


def _normalize_feed(feed: str) -> str:
    """Normalize one Alpaca market-data feed token."""
    if not isinstance(feed, str):
        raise TypeError("feed must be a string")
    cleaned = feed.strip().lower()
    if not cleaned:
        raise ValueError("feed cannot be empty")
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
    """Return True when a request error is likely transient."""
    response = getattr(error, "response", None)
    status_code = getattr(response, "status_code", None)
    return status_code in {429, 500, 502, 503, 504} or isinstance(
        error,
        (requests.ConnectionError, requests.Timeout),
    )


def _load_local_alpaca_env_file() -> None:
    """Load local Alpaca settings from config/alpaca.env when the file exists."""
    load_dotenv(dotenv_path=ALPACA_ENV_FILE, override=False)
