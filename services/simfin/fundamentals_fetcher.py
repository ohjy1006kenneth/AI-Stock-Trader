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
from loguru import logger

from core.common.env_files import populate_env_from_file

SIMFIN_API_KEY_ENV = "SIMFIN_API_KEY"
SIMFIN_BASE_URL_ENV = "SIMFIN_BASE_URL"
SEC_USER_AGENT_ENV = "SEC_USER_AGENT"
DEFAULT_SIMFIN_BASE_URL = "https://backend.simfin.com/api/v3"
SIMFIN_STATEMENTS_ENDPOINT = "/companies/statements/compact"
DEFAULT_SIMFIN_PAGE_LIMIT = 1000
DEFAULT_SIMFIN_TICKER_BATCH_SIZE = 50
DEFAULT_SIMFIN_STATEMENTS = ("pl", "bs", "cf", "derived")
DEFAULT_SIMFIN_PERIODS = ("q1", "q2", "q3", "q4", "fy")
SIMFIN_ENV_FILE = Path(__file__).resolve().parents[2] / "config" / "simfin.env"
SIMFIN_ENV_KEYS = (
    SIMFIN_API_KEY_ENV,
    SIMFIN_BASE_URL_ENV,
    SEC_USER_AGENT_ENV,
)
SEC_COMPANY_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"
SEC_COMPANYFACTS_URL_TEMPLATE = "https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"
_SEC_ALLOWED_FORMS = frozenset({"10-K", "10-K/A", "10-Q", "10-Q/A", "20-F", "20-F/A", "40-F"})
_SIMFIN_REQUEST_TICKER_OVERRIDES: dict[str, str] = {
    # Current S&P 500 ticker aliases that SimFin still serves under an older
    # vendor symbol or the economic share-class peer with identical company fundamentals.
    "CPAY": "FLT",
    "FISV": "FI",
    "FOXA": "FOX",
    "GOOGL": "GOOG",
    "MRSH": "MMC",
    "NWSA": "NWS",
    "RVTY": "PKI",
    "XYZ": "SQ",
}


@dataclass(frozen=True)
class SimFinClientConfig:
    """Configuration for SimFin HTTP clients."""

    api_key: str
    base_url: str = DEFAULT_SIMFIN_BASE_URL
    timeout_seconds: int = 30
    max_retries: int = 3
    retry_sleep_seconds: float = 2.0
    min_request_interval_seconds: float = 1.0
    rate_limit_sleep_seconds: float = 30.0
    split_cooldown_seconds: float = 10.0

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


@dataclass(frozen=True)
class _SecConceptSpec:
    """One SEC company-facts concept mapped onto a Layer 0 raw field."""

    raw_field: str
    taxonomy: str
    concept: str
    unit_kind: str


_SEC_CONCEPT_SPECS: tuple[_SecConceptSpec, ...] = (
    _SecConceptSpec("Revenue", "us-gaap", "Revenues", "currency"),
    _SecConceptSpec("Revenue", "us-gaap", "SalesRevenueNet", "currency"),
    _SecConceptSpec(
        "Revenue",
        "us-gaap",
        "RevenueFromContractWithCustomerExcludingAssessedTax",
        "currency",
    ),
    _SecConceptSpec("Net Income", "us-gaap", "NetIncomeLoss", "currency"),
    _SecConceptSpec(
        "Net Income Available to Common Shareholders",
        "us-gaap",
        "NetIncomeLossAvailableToCommonStockholdersBasic",
        "currency",
    ),
    _SecConceptSpec("Gross Profit", "us-gaap", "GrossProfit", "currency"),
    _SecConceptSpec("totalAssets", "us-gaap", "Assets", "currency"),
    _SecConceptSpec("totalLiabilities", "us-gaap", "Liabilities", "currency"),
    _SecConceptSpec("stockholdersEquity", "us-gaap", "StockholdersEquity", "currency"),
    _SecConceptSpec(
        "stockholdersEquity",
        "us-gaap",
        "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest",
        "currency",
    ),
    _SecConceptSpec("operatingIncome", "us-gaap", "OperatingIncomeLoss", "currency"),
    _SecConceptSpec("interestExpense", "us-gaap", "InterestExpense", "currency"),
    _SecConceptSpec("interestExpense", "us-gaap", "InterestExpenseNonoperating", "currency"),
    _SecConceptSpec("sharesBasic", "dei", "EntityCommonStockSharesOutstanding", "shares"),
    _SecConceptSpec("sharesBasic", "us-gaap", "CommonStockSharesOutstanding", "shares"),
    _SecConceptSpec(
        "sharesBasic",
        "us-gaap",
        "WeightedAverageNumberOfSharesOutstandingBasic",
        "shares",
    ),
    _SecConceptSpec("epsBasic", "us-gaap", "EarningsPerShareBasic", "per_share"),
    _SecConceptSpec("epsDiluted", "us-gaap", "EarningsPerShareDiluted", "per_share"),
)


class SimFinFundamentalsFetcher:
    """Fetch SimFin fundamentals and recover zero-row ticker gaps from SEC company facts."""

    def __init__(
        self,
        config: SimFinClientConfig,
        session: requests.Session | None = None,
    ) -> None:
        """Store client configuration and HTTP session."""
        self.config = config
        self.session = session or requests.Session()
        self._last_request_at: float | None = None
        self._sec_cik_by_ticker: dict[str, str] | None = None

    def fetch_statement_rows(
        self,
        *,
        tickers: Sequence[str],
        start_date: str,
        end_date: str,
        statements: Sequence[str] = DEFAULT_SIMFIN_STATEMENTS,
        periods: Sequence[str] = DEFAULT_SIMFIN_PERIODS,
        retrieved_at: datetime | None = None,
        limit: int = DEFAULT_SIMFIN_PAGE_LIMIT,
        offset: int = 0,
    ) -> SimFinPage:
        """Fetch one page of raw SimFin statement rows for a ticker/date range."""
        normalized_tickers = _normalize_tickers(tickers)
        request_groups = _group_requested_tickers_by_vendor_symbol(normalized_tickers)
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
                "ticker": ",".join(_simfin_request_ticker(ticker) for ticker in request_groups),
                "statements": ",".join(normalized_statements),
                "period": ",".join(normalized_periods),
                "start": start,
                "end": end,
                "asreported": "true",
                "limit": limit,
                "offset": offset,
            }
        )
        raw_rows = _extract_payload_rows(payload)
        rows = _expand_requested_ticker_aliases(
            normalize_simfin_fundamental_rows(raw_rows, retrieved_at=retrieved_at),
            request_groups=request_groups,
        )
        return SimFinPage(rows=rows, offset=offset, limit=limit)

    def fetch_all_fundamentals(
        self,
        *,
        tickers: Sequence[str],
        start_date: str,
        end_date: str,
        statements: Sequence[str] = DEFAULT_SIMFIN_STATEMENTS,
        periods: Sequence[str] = DEFAULT_SIMFIN_PERIODS,
        retrieved_at: datetime | None = None,
        limit: int = DEFAULT_SIMFIN_PAGE_LIMIT,
        max_pages: int | None = None,
    ) -> list[dict[str, Any]]:
        """Fetch all SimFin fundamental rows for a date range, deduplicated."""
        normalized_tickers = _normalize_tickers(tickers)
        seen: set[str] = set()
        rows: list[dict[str, Any]] = []
        archive_retrieved_at = retrieved_at or datetime.now(UTC)
        batches = _ticker_batches(normalized_tickers)
        logger.info(
            "SimFin fundamentals: {} tickers, {} batches, range {}..{}",
            len(normalized_tickers),
            len(batches),
            start_date,
            end_date,
        )

        for batch_index, ticker_batch in enumerate(batches, start=1):
            logger.info(
                "SimFin batch {}/{} ({} tickers): {}",
                batch_index,
                len(batches),
                len(ticker_batch),
                ",".join(ticker_batch[:3]) + ("..." if len(ticker_batch) > 3 else ""),
            )
            batch_rows = self._fetch_fundamentals_batch(
                tickers=ticker_batch,
                start_date=start_date,
                end_date=end_date,
                statements=statements,
                periods=periods,
                retrieved_at=archive_retrieved_at,
                limit=limit,
                max_pages=max_pages,
                seen=seen,
            )
            rows.extend(batch_rows)
            logger.info(
                "SimFin batch {}/{} done: {} rows ({} total)",
                batch_index,
                len(batches),
                len(batch_rows),
                len(rows),
            )

        missing_tickers = _tickers_without_rows(rows=rows, tickers=normalized_tickers)
        if missing_tickers:
            fallback_rows = self._fetch_sec_fallback_rows(
                tickers=missing_tickers,
                start_date=start_date,
                end_date=end_date,
                retrieved_at=archive_retrieved_at,
            )
            if fallback_rows:
                logger.info(
                    "SEC company-facts fallback recovered {} rows across {} tickers",
                    len(fallback_rows),
                    len({str(row.get('ticker') or '') for row in fallback_rows}),
                )
                for row in fallback_rows:
                    key = _fundamental_key(row)
                    if key in seen:
                        continue
                    seen.add(key)
                    rows.append(row)

        return rows

    def _fetch_sec_fallback_rows(
        self,
        *,
        tickers: Sequence[str],
        start_date: str,
        end_date: str,
        retrieved_at: datetime,
    ) -> list[dict[str, Any]]:
        """Recover unresolved tickers from the public SEC company-facts API."""
        cik_by_ticker = self._load_sec_cik_by_ticker()
        rows: list[dict[str, Any]] = []
        unresolved: list[str] = []
        for ticker in tickers:
            cik = cik_by_ticker.get(ticker)
            if cik is None:
                unresolved.append(ticker)
                continue
            try:
                payload = self._request_sec_json(SEC_COMPANYFACTS_URL_TEMPLATE.format(cik=cik))
            except requests.RequestException as exc:
                logger.warning(
                    "SEC company-facts fallback failed for {} (CIK {}): {}",
                    ticker,
                    cik,
                    type(exc).__name__,
                )
                unresolved.append(ticker)
                continue
            rows.extend(
                _sec_companyfacts_to_normalized_rows(
                    payload,
                    ticker=ticker,
                    cik=cik,
                    start_date=start_date,
                    end_date=end_date,
                    retrieved_at=retrieved_at,
                )
            )
        if unresolved:
            logger.warning(
                "SEC company-facts fallback could not resolve tickers: {}",
                ",".join(unresolved),
            )
        return rows

    def _fetch_fundamentals_batch(
        self,
        *,
        tickers: Sequence[str],
        start_date: str,
        end_date: str,
        statements: Sequence[str],
        periods: Sequence[str],
        retrieved_at: datetime,
        limit: int,
        max_pages: int | None,
        seen: set[str],
    ) -> list[dict[str, Any]]:
        """Fetch fundamentals for one ticker batch, splitting on transient failures."""
        try:
            return self._fetch_fundamentals_batch_pages(
                tickers=tickers,
                start_date=start_date,
                end_date=end_date,
                statements=statements,
                periods=periods,
                retrieved_at=retrieved_at,
                limit=limit,
                max_pages=max_pages,
                seen=seen,
            )
        except requests.RequestException as exc:
            status_code = getattr(getattr(exc, "response", None), "status_code", None)
            should_split = False
            if status_code in {429, 500, 502, 503, 504}:
                should_split = True
            elif isinstance(exc, (requests.ConnectionError, requests.Timeout)):
                should_split = True
            if should_split and len(tickers) > 1:
                midpoint = len(tickers) // 2
                rows: list[dict[str, Any]] = []
                rows.extend(
                    self._fetch_fundamentals_batch(
                        tickers=tickers[:midpoint],
                        start_date=start_date,
                        end_date=end_date,
                        statements=statements,
                        periods=periods,
                        retrieved_at=retrieved_at,
                        limit=limit,
                        max_pages=max_pages,
                        seen=seen,
                    )
                )
                # Cool down before the second half so a 5xx burst doesn't
                # trip SimFin's per-minute rate limiter on the recursive retry.
                time.sleep(self.config.split_cooldown_seconds)
                rows.extend(
                    self._fetch_fundamentals_batch(
                        tickers=tickers[midpoint:],
                        start_date=start_date,
                        end_date=end_date,
                        statements=statements,
                        periods=periods,
                        retrieved_at=retrieved_at,
                        limit=limit,
                        max_pages=max_pages,
                        seen=seen,
                    )
                )
                return rows
            if should_split and len(tickers) == 1:
                logger.warning(
                    "SimFin ticker {} failed after retries (status={}): skipping",
                    tickers[0],
                    status_code,
                )
                return []
            raise

    def _fetch_fundamentals_batch_pages(
        self,
        *,
        tickers: Sequence[str],
        start_date: str,
        end_date: str,
        statements: Sequence[str],
        periods: Sequence[str],
        retrieved_at: datetime,
        limit: int,
        max_pages: int | None,
        seen: set[str],
    ) -> list[dict[str, Any]]:
        """Fetch and dedupe all pages for one ticker batch."""
        offset = 0
        pages = 0
        rows: list[dict[str, Any]] = []

        while True:
            page = self.fetch_statement_rows(
                tickers=tickers,
                start_date=start_date,
                end_date=end_date,
                statements=statements,
                periods=periods,
                retrieved_at=retrieved_at,
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
        headers = {
            "accept": "application/json",
            "Authorization": f"api-key {self.config.api_key}",
        }
        last_error: requests.RequestException | None = None
        for attempt in range(self.config.max_retries + 1):
            try:
                self._throttle_if_needed()
                response = self.session.get(
                    url,
                    params=dict(params),
                    headers=headers,
                    timeout=self.config.timeout_seconds,
                )
                response.raise_for_status()
                return response.json()
            except requests.RequestException as exc:
                last_error = exc
                if attempt >= self.config.max_retries or not _is_retryable_error(exc):
                    raise
                time.sleep(
                    _retry_backoff_seconds(
                        exc,
                        attempt,
                        self.config.retry_sleep_seconds,
                        self.config.rate_limit_sleep_seconds,
                    )
                )
            finally:
                self._last_request_at = time.monotonic()
        if last_error is not None:
            raise last_error
        raise RuntimeError("SimFin request failed without an exception")

    def _load_sec_cik_by_ticker(self) -> dict[str, str]:
        """Load and cache the SEC ticker-to-CIK map used by the fallback path."""
        if self._sec_cik_by_ticker is not None:
            return self._sec_cik_by_ticker

        payload = self._request_sec_json(SEC_COMPANY_TICKERS_URL)
        if not isinstance(payload, Mapping):
            raise ValueError("SEC company_tickers payload must be a JSON object")

        cik_by_ticker: dict[str, str] = {}
        for value in payload.values():
            if not isinstance(value, Mapping):
                continue
            ticker = value.get("ticker")
            cik_str = value.get("cik_str")
            if not isinstance(ticker, str) or cik_str is None:
                continue
            cik_by_ticker[_normalize_ticker(ticker)] = str(cik_str).strip().zfill(10)
        self._sec_cik_by_ticker = cik_by_ticker
        return cik_by_ticker

    def _request_sec_json(self, url: str) -> Any:
        """Request one SEC JSON payload with the same retry/throttle behavior."""
        headers = {
            "accept": "application/json",
            "accept-encoding": "gzip, deflate",
            "user-agent": _required_sec_user_agent(),
        }
        last_error: requests.RequestException | None = None
        for attempt in range(self.config.max_retries + 1):
            try:
                self._throttle_if_needed()
                response = self.session.get(
                    url,
                    headers=headers,
                    timeout=self.config.timeout_seconds,
                )
                response.raise_for_status()
                return response.json()
            except requests.RequestException as exc:
                last_error = exc
                if attempt >= self.config.max_retries or not _is_retryable_error(exc):
                    raise
                time.sleep(
                    _retry_backoff_seconds(
                        exc,
                        attempt,
                        self.config.retry_sleep_seconds,
                        self.config.rate_limit_sleep_seconds,
                    )
                )
            finally:
                self._last_request_at = time.monotonic()
        if last_error is not None:
            raise last_error
        raise RuntimeError("SEC request failed without an exception")

    def _throttle_if_needed(self) -> None:
        """Throttle requests to respect SimFin rate limits."""
        min_interval = self.config.min_request_interval_seconds
        if min_interval <= 0:
            return
        if self._last_request_at is None:
            return
        elapsed = time.monotonic() - self._last_request_at
        if elapsed < min_interval:
            time.sleep(min_interval - elapsed)


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
            raise ValueError(f"SimFin row {index} must be an object, got {type(row).__name__}")
        raw = dict(row)
        ticker = _required_text(raw, ("ticker", "Ticker", "symbol", "Symbol"), "ticker")
        report_date = _required_date(
            raw,
            ("reportDate", "Report Date", "periodEndDate", "period_end_date"),
            "report_date",
        )
        try:
            availability_date = _required_date(
                raw,
                (
                    "publishDate",
                    "Publish Date",
                    "filingDate",
                    "Filing Date",
                    "filing_date",
                    "asOfDate",
                    "as_of_date",
                ),
                "availability_date",
            )
        except ValueError:
            # Some compact derived rows omit a publish timestamp; use report_date as the
            # latest defensible point-in-time availability fallback.
            availability_date = report_date

        normalized: dict[str, Any] = {
            "source": "simfin",
            "ticker": _normalize_ticker(ticker),
            "report_date": report_date,
            "availability_date": availability_date,
            "retrieved_at": retrieval_time,
            "raw": raw,
        }
        _add_optional_text(normalized, raw, "statement", ("statement", "Statement"))
        _add_optional_int(normalized, raw, "fiscal_year", ("fiscalYear", "Fiscal Year"))
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
        nested_rows = _rows_from_nested_companies_payload(payload)
        if nested_rows is not None:
            return nested_rows
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


def _rows_from_nested_companies_payload(payload: list[Any]) -> list[Mapping[str, Any]] | None:
    """Flatten backend compact payloads shaped as companies -> statements -> columns/data."""
    if not payload:
        return None
    if not all(isinstance(item, Mapping) for item in payload):
        return None

    has_nested_statements = any("statements" in item for item in payload)
    if not has_nested_statements:
        return None

    flattened: list[Mapping[str, Any]] = []
    for company_index, company in enumerate(payload):
        statements = company.get("statements")
        if statements is None:
            continue
        if not isinstance(statements, list):
            raise ValueError(
                f"SimFin company payload item {company_index} field statements must be a list"
            )

        ticker = company.get("ticker")
        currency = company.get("currency")
        for statement_index, statement_payload in enumerate(statements):
            if not isinstance(statement_payload, Mapping):
                raise ValueError(
                    "SimFin statements payload item "
                    f"{company_index}:{statement_index} must be an object"
                )
            columns = statement_payload.get("columns")
            data_rows = statement_payload.get("data")
            if not isinstance(columns, list) or not all(isinstance(col, str) for col in columns):
                raise ValueError(
                    "SimFin statements payload columns must be a list of strings for "
                    f"item {company_index}:{statement_index}"
                )
            if not isinstance(data_rows, list):
                raise ValueError(
                    f"SimFin statements payload data must be a list for item "
                    f"{company_index}:{statement_index}"
                )

            statement_name = statement_payload.get("statement")
            for row_index, row in enumerate(data_rows):
                if not isinstance(row, Sequence) or isinstance(row, (str, bytes, bytearray)):
                    raise ValueError(
                        "SimFin compact nested row "
                        f"{company_index}:{statement_index}:{row_index} must be an array"
                    )
                if len(row) != len(columns):
                    raise ValueError(
                        "SimFin compact nested row "
                        f"{company_index}:{statement_index}:{row_index} has {len(row)} values "
                        f"for {len(columns)} columns"
                    )
                mapped = dict(zip(columns, row, strict=True))
                if ticker is not None and "ticker" not in mapped:
                    mapped["ticker"] = ticker
                if currency is not None and "currency" not in mapped:
                    mapped["currency"] = currency
                if statement_name is not None and "statement" not in mapped:
                    mapped["statement"] = str(statement_name).lower()
                flattened.append(mapped)

    return flattened


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
                f"SimFin compact row {index} must be an array or object, got {type(row).__name__}"
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


def _tickers_without_rows(
    *,
    rows: Sequence[Mapping[str, Any]],
    tickers: Sequence[str],
) -> list[str]:
    """Return requested tickers that still have no normalized fundamentals rows."""
    present = {_normalize_ticker(str(row.get("ticker") or "")) for row in rows if row.get("ticker")}
    return [ticker for ticker in tickers if ticker not in present]


def _required_sec_user_agent() -> str:
    """Return the configured SEC EDGAR user-agent or fail closed when absent."""
    _load_local_simfin_env_file()
    configured = (os.getenv(SEC_USER_AGENT_ENV) or "").strip()
    if not configured:
        raise ValueError(
            f"Missing required SEC environment variable for EDGAR access: {SEC_USER_AGENT_ENV}"
        )
    return configured


def _sec_companyfacts_to_normalized_rows(
    payload: Any,
    *,
    ticker: str,
    cik: str,
    start_date: str,
    end_date: str,
    retrieved_at: datetime,
) -> list[dict[str, Any]]:
    """Map SEC company-facts JSON into the normalized Layer 0 fundamentals shape."""
    if not isinstance(payload, Mapping):
        raise ValueError("SEC companyfacts payload must be a JSON object")
    facts = payload.get("facts")
    if not isinstance(facts, Mapping):
        return []

    grouped: dict[tuple[str, str, str, str, str], dict[str, Any]] = {}
    for spec in _SEC_CONCEPT_SPECS:
        taxonomy_payload = facts.get(spec.taxonomy)
        if not isinstance(taxonomy_payload, Mapping):
            continue
        concept_payload = taxonomy_payload.get(spec.concept)
        if not isinstance(concept_payload, Mapping):
            continue
        units_payload = concept_payload.get("units")
        if not isinstance(units_payload, Mapping):
            continue
        for unit_name, unit_rows in units_payload.items():
            if not _sec_unit_matches(str(unit_name), spec.unit_kind):
                continue
            if not isinstance(unit_rows, Sequence):
                continue
            for unit_row in unit_rows:
                if not isinstance(unit_row, Mapping):
                    continue
                normalized_row = _sec_fact_row_to_archive_row(
                    grouped=grouped,
                    sec_row=unit_row,
                    ticker=ticker,
                    cik=cik,
                    start_date=start_date,
                    end_date=end_date,
                )
                if normalized_row is None:
                    continue
                raw = normalized_row["raw"]
                if spec.raw_field not in raw:
                    raw[spec.raw_field] = unit_row.get("val")

    rows = list(grouped.values())
    rows.sort(
        key=lambda row: (
            str(row.get("ticker") or ""),
            str(row.get("report_date") or ""),
            str(row.get("availability_date") or ""),
            str(row.get("statement") or ""),
        )
    )
    retrieval_time = retrieved_at.isoformat()
    for row in rows:
        row["retrieved_at"] = retrieval_time
    return rows


def _sec_fact_row_to_archive_row(
    *,
    grouped: dict[tuple[str, str, str, str, str], dict[str, Any]],
    sec_row: Mapping[str, Any],
    ticker: str,
    cik: str,
    start_date: str,
    end_date: str,
) -> dict[str, Any] | None:
    """Create or retrieve one grouped archive row from a single SEC fact row."""
    filed = _optional_sec_date(sec_row.get("filed"))
    report_date = _optional_sec_date(sec_row.get("end"))
    if filed is None or report_date is None:
        return None
    if filed < start_date or filed > end_date:
        return None

    form = str(sec_row.get("form") or "").strip().upper()
    if form not in _SEC_ALLOWED_FORMS:
        return None

    fiscal_year = _optional_sec_int(sec_row.get("fy"))
    fiscal_period = _optional_sec_text(sec_row.get("fp"))
    accession = _optional_sec_text(sec_row.get("accn")) or f"{ticker}-{filed}-{report_date}-{form}"
    key = (
        filed,
        report_date,
        accession,
        fiscal_period or "",
        str(fiscal_year) if fiscal_year is not None else "",
    )
    existing = grouped.get(key)
    if existing is not None:
        return existing

    raw: dict[str, Any] = {
        "ticker": ticker.replace("-", "."),
        "cik": cik,
        "accessionNumber": accession,
        "filed": filed,
        "form": form,
        "reportDate": report_date,
    }
    if fiscal_year is not None:
        raw["Fiscal Year"] = fiscal_year
    if fiscal_period is not None:
        raw["Fiscal Period"] = fiscal_period

    normalized: dict[str, Any] = {
        "source": "sec_companyfacts",
        "ticker": ticker,
        "report_date": report_date,
        "availability_date": filed,
        "statement": "sec_companyfacts",
        "raw": raw,
    }
    if fiscal_year is not None:
        normalized["fiscal_year"] = fiscal_year
    if fiscal_period is not None:
        normalized["fiscal_period"] = fiscal_period

    grouped[key] = normalized
    return normalized


def _sec_unit_matches(unit_name: str, expected_kind: str) -> bool:
    """Return True when a SEC company-facts unit string matches the expected field type."""
    normalized = unit_name.strip().lower()
    if expected_kind == "currency":
        return normalized == "usd"
    if expected_kind == "shares":
        return normalized == "shares"
    if expected_kind == "per_share":
        return "usd" in normalized and "share" in normalized
    raise ValueError(f"Unsupported SEC unit kind: {expected_kind}")


def _optional_sec_date(value: Any) -> str | None:
    """Return a validated SEC date string when present."""
    if value is None or value == "":
        return None
    if not isinstance(value, str):
        return None
    return _validate_date(value, "sec_date")


def _optional_sec_int(value: Any) -> int | None:
    """Return an integer-like SEC fact field when present."""
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return None
        try:
            return int(stripped)
        except ValueError:
            return None
    return None


def _optional_sec_text(value: Any) -> str | None:
    """Return trimmed SEC text when present."""
    if value is None:
        return None
    cleaned = str(value).strip()
    return cleaned or None


def _normalize_tickers(tickers: Sequence[str]) -> list[str]:
    """Normalize ticker text for SimFin requests."""
    if not tickers:
        raise ValueError("tickers must contain at least one ticker")
    return [_normalize_ticker(ticker) for ticker in tickers]


def _ticker_batches(tickers: Sequence[str]) -> list[tuple[str, ...]]:
    """Return ticker batches sized for SimFin request stability."""
    return [
        tuple(tickers[index : index + DEFAULT_SIMFIN_TICKER_BATCH_SIZE])
        for index in range(0, len(tickers), DEFAULT_SIMFIN_TICKER_BATCH_SIZE)
    ]


def _group_requested_tickers_by_vendor_symbol(tickers: Sequence[str]) -> dict[str, tuple[str, ...]]:
    """Group canonical archive tickers by the SimFin vendor symbol to request."""
    groups: dict[str, list[str]] = {}
    for ticker in tickers:
        request_symbol = _simfin_vendor_ticker(ticker)
        groups.setdefault(request_symbol, []).append(ticker)
    return {ticker: tuple(canonicals) for ticker, canonicals in groups.items()}


def _expand_requested_ticker_aliases(
    rows: Sequence[Mapping[str, Any]],
    *,
    request_groups: Mapping[str, Sequence[str]],
) -> list[dict[str, Any]]:
    """Duplicate provider rows back onto every requested canonical ticker alias."""
    expanded: list[dict[str, Any]] = []
    for row in rows:
        provider_ticker = _normalize_ticker(str(row.get("ticker") or ""))
        target_tickers = tuple(request_groups.get(provider_ticker, (provider_ticker,)))
        for target_ticker in target_tickers:
            expanded.append({**row, "ticker": target_ticker})
    return expanded


def _normalize_ticker(ticker: str) -> str:
    """Normalize one ticker symbol."""
    if not isinstance(ticker, str):
        raise TypeError("ticker must be a string")
    cleaned = ticker.strip().upper().replace(".", "-")
    if not cleaned:
        raise ValueError("ticker cannot be empty")
    return cleaned


def _simfin_request_ticker(ticker: str) -> str:
    """Translate canonical archive tickers to SimFin's request symbol convention."""
    return _simfin_vendor_ticker(ticker).replace("-", ".")


def _simfin_vendor_ticker(ticker: str) -> str:
    """Return the normalized SimFin vendor symbol for one canonical archive ticker."""
    normalized = _normalize_ticker(ticker)
    return _SIMFIN_REQUEST_TICKER_OVERRIDES.get(normalized, normalized)


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


def _add_optional_int(
    output: dict[str, Any],
    row: Mapping[str, Any],
    field_name: str,
    names: Sequence[str],
) -> None:
    """Add an optional integer field when SimFin provides it."""
    value = _first_present(row, names)
    if value is None:
        return
    if isinstance(value, bool):
        raise TypeError(f"SimFin field {field_name} must be an integer")
    if isinstance(value, int):
        output[field_name] = value
        return
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return
        try:
            output[field_name] = int(stripped)
        except ValueError as exc:
            raise ValueError(f"SimFin field {field_name} must be an integer: {value}") from exc
        return
    raise TypeError(f"SimFin field {field_name} must be an integer")


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


def _retry_backoff_seconds(
    error: requests.RequestException,
    attempt: int,
    base_sleep_seconds: float,
    rate_limit_sleep_seconds: float,
) -> float:
    """Return the retry delay for a transient SimFin error."""
    response = getattr(error, "response", None)
    headers = getattr(response, "headers", None)
    retry_after = None
    status_code = getattr(response, "status_code", None)
    if isinstance(headers, Mapping):
        header_value = headers.get("Retry-After")
        if header_value is not None:
            try:
                retry_after = float(header_value)
            except (TypeError, ValueError):
                retry_after = None
    backoff = base_sleep_seconds * (2**attempt)
    if status_code == 429:
        return max(backoff, retry_after or 0.0, rate_limit_sleep_seconds)
    return max(backoff, retry_after or 0.0)


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
    populate_env_from_file(
        keys=SIMFIN_ENV_KEYS,
        env_file=SIMFIN_ENV_FILE,
        override=False,
        override_blank=True,
    )
