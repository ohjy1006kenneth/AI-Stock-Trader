from __future__ import annotations

import json
import os
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from datetime import date as Date
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv

SIMFIN_API_KEY_ENV = "SIMFIN_API_KEY"
SIMFIN_BASE_URL_ENV = "SIMFIN_BASE_URL"
DEFAULT_SIMFIN_BASE_URL = "https://simfin.com/api/v3"
SIMFIN_STATEMENTS_ENDPOINT = "/companies/statements/compact"
DEFAULT_SIMFIN_PAGE_LIMIT = 1000
DEFAULT_SIMFIN_STATEMENTS = ("pl", "bs", "cf", "derived")
DEFAULT_SIMFIN_PERIODS = ("q1", "q2", "q3", "q4", "fy")
SIMFIN_ENV_FILE = Path(__file__).resolve().parents[2] / "config" / "simfin.env"


@dataclass(frozen=True)
class SimFinClientConfig:
    """Configuration for SimFin HTTP clients."""

    api_key: str
    base_url: str = DEFAULT_SIMFIN_BASE_URL
    timeout_seconds: int = 30
    max_retries: int = 2
    retry_sleep_seconds: float = 1.0

    @classmethod
    def from_env(cls) -> SimFinClientConfig:
        """Build SimFin config from environment variables or config/simfin.env."""
        _load_local_simfin_env_file()
        api_key = os.getenv(SIMFIN_API_KEY_ENV)
        if not api_key:
            raise ValueError(f"Missing required SimFin environment variable: {SIMFIN_API_KEY_ENV}")
        return cls(
            api_key=api_key,
            base_url=os.getenv(SIMFIN_BASE_URL_ENV) or DEFAULT_SIMFIN_BASE_URL,
        )


@dataclass(frozen=True)
class SimFinPage:
    """One page of normalized SimFin statement rows."""

    rows: list[dict[str, Any]]
    offset: int
    limit: int


class SimFinFundamentalsFetcher:
    """Fetch and normalize SimFin as-reported fundamentals and earnings metadata."""

    def __init__(
        self,
        config: SimFinClientConfig,
        session: requests.Session | None = None,
    ) -> None:
        """Store client configuration and HTTP session."""
        self.config = config
        self.session = session or requests.Session()

    def fetch_statement_rows(
        self,
        *,
        tickers: Sequence[str],
        start_date: str,
        end_date: str,
        statements: Sequence[str] = DEFAULT_SIMFIN_STATEMENTS,
        periods: Sequence[str] = DEFAULT_SIMFIN_PERIODS,
        limit: int = DEFAULT_SIMFIN_PAGE_LIMIT,
        offset: int = 0,
    ) -> SimFinPage:
        """Fetch one page of raw SimFin statement rows for a ticker/date range."""
        normalized_tickers = _normalize_tickers(tickers)
        normalized_statements = _normalize_tokens(statements, "statements")
        normalized_periods = _normalize_tokens(periods, "periods")
        start = _validate_date(start_date, "start_date")
        end = _validate_date(end_date, "end_date")
        if start > end:
            raise ValueError("start_date must be <= end_date")
        if limit <= 0:
            raise ValueError("limit must be positive")
        if offset < 0:
            raise ValueError("offset must be non-negative")

        payload = self._request_json(
            params={
                "ticker": ",".join(normalized_tickers),
                "statements": ",".join(normalized_statements),
                "period": ",".join(normalized_periods),
                "start": start,
                "end": end,
                "asreported": "true",
                "limit": limit,
                "offset": offset,
                "api-key": self.config.api_key,
            }
        )
        raw_rows = _extract_payload_rows(payload)
        rows = normalize_simfin_fundamental_rows(raw_rows)
        return SimFinPage(rows=rows, offset=offset, limit=limit)

    def fetch_all_fundamentals(
        self,
        *,
        tickers: Sequence[str],
        start_date: str,
        end_date: str,
        statements: Sequence[str] = DEFAULT_SIMFIN_STATEMENTS,
        periods: Sequence[str] = DEFAULT_SIMFIN_PERIODS,
        limit: int = DEFAULT_SIMFIN_PAGE_LIMIT,
        max_pages: int | None = None,
    ) -> list[dict[str, Any]]:
        """Fetch all SimFin fundamental rows for a date range, deduplicated."""
        offset = 0
        pages = 0
        seen: set[str] = set()
        rows: list[dict[str, Any]] = []

        while True:
            page = self.fetch_statement_rows(
                tickers=tickers,
                start_date=start_date,
                end_date=end_date,
                statements=statements,
                periods=periods,
                limit=limit,
                offset=offset,
            )
            if not page.rows:
                break

            for row in page.rows:
                key = _fundamental_key(row)
                if key in seen:
                    continue
                seen.add(key)
                rows.append(row)

            if len(page.rows) < page.limit:
                break

            offset += page.limit
            pages += 1
            if max_pages is not None and pages >= max_pages:
                break

        return rows

    def _request_json(self, params: Mapping[str, Any]) -> Any:
        """Request one SimFin payload with bounded retries for transient failures."""
        url = f"{self.config.base_url.rstrip('/')}{SIMFIN_STATEMENTS_ENDPOINT}"
        last_error: requests.RequestException | None = None
        for attempt in range(self.config.max_retries + 1):
            try:
                response = self.session.get(
                    url,
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
        raise RuntimeError("SimFin request failed without an exception")


def normalize_simfin_fundamental_rows(
    rows: Sequence[Mapping[str, Any]],
    *,
    retrieved_at: datetime | None = None,
) -> list[dict[str, Any]]:
    """Normalize raw SimFin rows while preserving the original vendor payload."""
    retrieval_time = (retrieved_at or datetime.now(UTC)).isoformat()
    normalized_rows: list[dict[str, Any]] = []
    for index, row in enumerate(rows):
        if not isinstance(row, Mapping):
            raise ValueError(
                f"SimFin row {index} must be an object, got {type(row).__name__}"
            )
        raw = dict(row)
        ticker = _required_text(raw, ("ticker", "Ticker", "symbol", "Symbol"), "ticker")
        report_date = _required_date(
            raw,
            ("reportDate", "Report Date", "periodEndDate", "period_end_date"),
            "report_date",
        )
        availability_date = _required_date(
            raw,
            (
                "publishDate",
                "Publish Date",
                "filingDate",
                "filing_date",
                "asOfDate",
                "as_of_date",
            ),
            "availability_date",
        )

        normalized: dict[str, Any] = {
            "source": "simfin",
            "ticker": _normalize_ticker(ticker),
            "report_date": report_date,
            "availability_date": availability_date,
            "retrieved_at": retrieval_time,
            "raw": raw,
        }
        _add_optional_text(normalized, raw, "statement", ("statement", "Statement"))
        _add_optional_text(normalized, raw, "fiscal_year", ("fiscalYear", "Fiscal Year"))
        _add_optional_text(normalized, raw, "fiscal_period", ("fiscalPeriod", "Fiscal Period"))
        _add_optional_text(normalized, raw, "currency", ("currency", "Currency"))
        _add_optional_date(normalized, raw, "earnings_date", ("earningsDate", "earnings_date"))
        _add_optional_date(
            normalized,
            raw,
            "announcement_date",
            ("announcementDate", "announcement_date"),
        )
        normalized_rows.append(normalized)
    return normalized_rows


def _extract_payload_rows(payload: Any) -> list[Mapping[str, Any]]:
    """Extract row objects from supported SimFin list and compact payload shapes."""
    if isinstance(payload, list):
        return _coerce_payload_rows(payload)
    if not isinstance(payload, Mapping):
        raise ValueError("SimFin response must be a JSON object or list")

    for rows_field in ("data", "results", "rows"):
        rows = payload.get(rows_field)
        if rows is None:
            continue
        if not isinstance(rows, list):
            raise ValueError(f"SimFin response field {rows_field} must be a list")
        columns = payload.get("columns")
        if columns is not None:
            return _rows_from_compact_payload(columns, rows)
        return _coerce_payload_rows(rows)
    raise ValueError("SimFin response must contain data, results, or rows")


def _rows_from_compact_payload(columns: Any, rows: list[Any]) -> list[Mapping[str, Any]]:
    """Convert SimFin compact columns/data arrays into row mappings."""
    if not isinstance(columns, list) or not all(isinstance(column, str) for column in columns):
        raise ValueError("SimFin compact response columns must be a list of strings")
    mapped_rows: list[Mapping[str, Any]] = []
    for index, row in enumerate(rows):
        if isinstance(row, Mapping):
            mapped_rows.append(row)
            continue
        if not isinstance(row, Sequence) or isinstance(row, (str, bytes, bytearray)):
            raise ValueError(
                f"SimFin compact row {index} must be an array or object, "
                f"got {type(row).__name__}"
            )
        if len(row) != len(columns):
            raise ValueError(
                f"SimFin compact row {index} has {len(row)} values for {len(columns)} columns"
            )
        mapped_rows.append(dict(zip(columns, row, strict=True)))
    return mapped_rows


def _coerce_payload_rows(rows: list[Any]) -> list[Mapping[str, Any]]:
    """Validate that payload rows are JSON objects."""
    coerced: list[Mapping[str, Any]] = []
    for index, row in enumerate(rows):
        if not isinstance(row, Mapping):
            raise ValueError(f"SimFin row {index} must be an object, got {type(row).__name__}")
        coerced.append(row)
    return coerced


def _fundamental_key(row: Mapping[str, Any]) -> str:
    """Return a deterministic key for deduplicating normalized SimFin records."""
    return "|".join(
        [
            str(row.get("ticker") or ""),
            str(row.get("report_date") or ""),
            str(row.get("availability_date") or ""),
            str(row.get("statement") or ""),
            json.dumps(row.get("raw") or {}, sort_keys=True, separators=(",", ":")),
        ]
    )


def _normalize_tickers(tickers: Sequence[str]) -> list[str]:
    """Normalize ticker text for SimFin requests."""
    if not tickers:
        raise ValueError("tickers must contain at least one ticker")
    return [_normalize_ticker(ticker) for ticker in tickers]


def _normalize_ticker(ticker: str) -> str:
    """Normalize one ticker symbol."""
    if not isinstance(ticker, str):
        raise TypeError("ticker must be a string")
    cleaned = ticker.strip().upper().replace(".", "-")
    if not cleaned:
        raise ValueError("ticker cannot be empty")
    return cleaned


def _normalize_tokens(values: Sequence[str], field_name: str) -> tuple[str, ...]:
    """Validate and normalize a non-empty sequence of request tokens."""
    if not values:
        raise ValueError(f"{field_name} must contain at least one value")
    normalized: list[str] = []
    for value in values:
        if not isinstance(value, str):
            raise TypeError(f"{field_name} values must be strings")
        cleaned = value.strip().lower()
        if not cleaned:
            raise ValueError(f"{field_name} values cannot be empty")
        normalized.append(cleaned)
    return tuple(normalized)


def _required_text(row: Mapping[str, Any], names: Sequence[str], field_name: str) -> str:
    """Return a required non-empty text field from possible vendor aliases."""
    value = _first_present(row, names)
    if value is None:
        raise ValueError(f"Missing required SimFin field: {field_name}")
    if not isinstance(value, str):
        raise TypeError(f"SimFin field {field_name} must be a string")
    cleaned = value.strip()
    if not cleaned:
        raise ValueError(f"SimFin field {field_name} cannot be empty")
    return cleaned


def _required_date(row: Mapping[str, Any], names: Sequence[str], field_name: str) -> str:
    """Return a required YYYY-MM-DD date from possible vendor aliases."""
    value = _first_present(row, names)
    if value is None:
        raise ValueError(f"Missing required SimFin field: {field_name}")
    if not isinstance(value, str):
        raise TypeError(f"SimFin field {field_name} must be a string date")
    return _validate_date(value, field_name)


def _add_optional_text(
    output: dict[str, Any],
    row: Mapping[str, Any],
    field_name: str,
    names: Sequence[str],
) -> None:
    """Add an optional text field when SimFin provides it."""
    value = _first_present(row, names)
    if value is not None:
        output[field_name] = str(value).strip()


def _add_optional_date(
    output: dict[str, Any],
    row: Mapping[str, Any],
    field_name: str,
    names: Sequence[str],
) -> None:
    """Add an optional date field when SimFin provides it."""
    value = _first_present(row, names)
    if value is not None:
        if not isinstance(value, str):
            raise TypeError(f"SimFin field {field_name} must be a string date")
        output[field_name] = _validate_date(value, field_name)


def _first_present(row: Mapping[str, Any], names: Sequence[str]) -> Any:
    """Return the first non-empty value found under one of the given aliases."""
    for name in names:
        value = row.get(name)
        if value is not None and value != "":
            return value
    return None


def _validate_date(value: str, field_name: str) -> str:
    """Validate and normalize a YYYY-MM-DD date string."""
    try:
        return Date.fromisoformat(value.strip().split("T", maxsplit=1)[0]).isoformat()
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


def _load_local_simfin_env_file() -> None:
    """Load local SimFin settings from config/simfin.env when the file exists."""
    load_dotenv(dotenv_path=SIMFIN_ENV_FILE, override=False)
