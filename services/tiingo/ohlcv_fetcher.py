from __future__ import annotations

import os
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date as Date
from typing import Any
from urllib.parse import quote

import requests

from core.contracts.schemas import OHLCVRecord
from core.data.ohlcv import build_ohlcv_record
from services.tiingo.security_master import TiingoSecurity

TIINGO_API_TOKEN_ENV = "TIINGO_API_TOKEN"
DEFAULT_TIINGO_BASE_URL = "https://api.tiingo.com"


@dataclass(frozen=True)
class TiingoClientConfig:
    """Configuration for Tiingo HTTP clients."""

    api_token: str
    base_url: str = DEFAULT_TIINGO_BASE_URL
    timeout_seconds: int = 30

    @classmethod
    def from_env(cls) -> TiingoClientConfig:
        """Build Tiingo config from environment variables."""
        token = os.getenv(TIINGO_API_TOKEN_ENV)
        if not token:
            raise ValueError(f"Missing required Tiingo environment variable: {TIINGO_API_TOKEN_ENV}")
        return cls(api_token=token)


class TiingoOHLCVFetcher:
    """Fetch and normalize Tiingo historical EOD OHLCV rows."""

    def __init__(
        self,
        config: TiingoClientConfig,
        session: requests.Session | None = None,
    ) -> None:
        """Store client configuration and HTTP session."""
        self.config = config
        self.session = session or requests.Session()

    def fetch_price_rows(self, ticker: str, from_date: str, to_date: str) -> list[dict[str, Any]]:
        """Fetch raw Tiingo EOD price rows for one ticker and date range."""
        normalized_ticker = _normalize_ticker(ticker)
        _validate_date(from_date, "from_date")
        _validate_date(to_date, "to_date")
        if from_date > to_date:
            raise ValueError("from_date must be <= to_date")

        encoded_ticker = quote(normalized_ticker, safe="")
        url = f"{self.config.base_url.rstrip('/')}/tiingo/daily/{encoded_ticker}/prices"
        response = self.session.get(
            url,
            params={
                "startDate": from_date,
                "endDate": to_date,
                "token": self.config.api_token,
            },
            timeout=self.config.timeout_seconds,
        )
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, list):
            raise ValueError("Tiingo OHLCV response must be a JSON list")
        return [dict(item) for item in payload]

    def fetch_records(self, ticker: str, from_date: str, to_date: str) -> list[OHLCVRecord]:
        """Fetch Tiingo EOD rows and normalize them to OHLCVRecord objects."""
        normalized_ticker = _normalize_ticker(ticker)
        rows = self.fetch_price_rows(
            ticker=normalized_ticker,
            from_date=from_date,
            to_date=to_date,
        )
        return normalize_tiingo_price_rows(ticker=normalized_ticker, rows=rows)

    def fetch_security_records(
        self,
        security: TiingoSecurity,
        from_date: str,
        to_date: str,
    ) -> list[OHLCVRecord]:
        """Fetch records for a resolved security using its Tiingo ticker symbol."""
        return self.fetch_records(ticker=security.ticker, from_date=from_date, to_date=to_date)


def normalize_tiingo_price_rows(ticker: str, rows: Sequence[Mapping[str, Any]]) -> list[OHLCVRecord]:
    """Normalize raw Tiingo price rows into schema-valid OHLCV records."""
    normalized_ticker = _normalize_ticker(ticker)
    records: list[OHLCVRecord] = []
    for row in rows:
        adj_close = _required_numeric(row, "adjClose")
        volume = _required_numeric(row, "volume")
        record = build_ohlcv_record(
            {
                "date": _normalize_tiingo_date(row.get("date")),
                "ticker": normalized_ticker,
                "open": _preferred_numeric(row, "adjOpen", "open"),
                "high": _preferred_numeric(row, "adjHigh", "high"),
                "low": _preferred_numeric(row, "adjLow", "low"),
                "close": adj_close,
                "volume": volume,
                "adj_close": adj_close,
                "dollar_volume": adj_close * volume,
            }
        )
        records.append(record)
    return records


def _normalize_tiingo_date(value: Any) -> str:
    """Normalize Tiingo timestamp strings to YYYY-MM-DD."""
    if not isinstance(value, str):
        raise TypeError("Tiingo price row date must be a string")
    date_part = value.strip().split("T", maxsplit=1)[0]
    return _validate_date(date_part, "date")


def _preferred_numeric(row: Mapping[str, Any], preferred: str, fallback: str) -> int | float:
    """Return a preferred numeric value, falling back to another field."""
    if preferred in row and row[preferred] is not None:
        return _required_numeric(row, preferred)
    return _required_numeric(row, fallback)


def _required_numeric(row: Mapping[str, Any], field_name: str) -> int | float:
    """Return a required numeric field without string coercion."""
    if field_name not in row or row[field_name] is None:
        raise ValueError(f"Missing required Tiingo price field: {field_name}")
    value = row[field_name]
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"Tiingo price field {field_name} must be numeric")
    return value


def _validate_date(value: str, field_name: str) -> str:
    """Validate and normalize a YYYY-MM-DD date string."""
    try:
        return Date.fromisoformat(value).isoformat()
    except ValueError as exc:
        raise ValueError(f"{field_name} must be YYYY-MM-DD: {value}") from exc


def _normalize_ticker(ticker: str) -> str:
    """Normalize ticker text for Tiingo requests and records."""
    normalized = ticker.strip().upper().replace(".", "-")
    if not normalized:
        raise ValueError("ticker cannot be empty")
    return normalized
