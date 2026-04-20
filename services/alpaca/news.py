from __future__ import annotations

import json
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date as Date
from typing import Any

import requests

from services.alpaca.market_data import AlpacaMarketDataConfig

ALPACA_NEWS_ENDPOINT = "/v1beta1/news"
DEFAULT_ALPACA_NEWS_PAGE_LIMIT = 50
DEFAULT_ALPACA_NEWS_SYMBOL_BATCH_SIZE = 1000


@dataclass(frozen=True)
class AlpacaNewsPage:
    """One page of Alpaca news API results."""

    articles: list[dict[str, Any]]
    next_page_token: str | None
    limit: int


class AlpacaNewsClient:
    """Fetch raw Alpaca historical and latest news articles."""

    def __init__(
        self,
        config: AlpacaMarketDataConfig,
        session: requests.Session | None = None,
    ) -> None:
        """Store Alpaca market-data configuration and HTTP session."""
        self.config = config
        self.session = session or requests.Session()

    def fetch_news_page(
        self,
        *,
        tickers: Sequence[str] | None,
        start_date: str,
        end_date: str,
        limit: int = DEFAULT_ALPACA_NEWS_PAGE_LIMIT,
        page_token: str | None = None,
        include_content: bool = True,
        exclude_contentless: bool = False,
    ) -> AlpacaNewsPage:
        """Fetch one Alpaca news result page for a date range and optional symbols."""
        normalized_start = _normalize_news_datetime(start_date, field_name="start_date", end=False)
        normalized_end = _normalize_news_datetime(end_date, field_name="end_date", end=True)
        if normalized_start > normalized_end:
            raise ValueError("start_date must be <= end_date")
        if limit <= 0 or limit > DEFAULT_ALPACA_NEWS_PAGE_LIMIT:
            raise ValueError(f"limit must be between 1 and {DEFAULT_ALPACA_NEWS_PAGE_LIMIT}")

        params: dict[str, Any] = {
            "start": normalized_start,
            "end": normalized_end,
            "sort": "asc",
            "limit": limit,
            "include_content": str(include_content).lower(),
            "exclude_contentless": str(exclude_contentless).lower(),
        }
        normalized_tickers = _normalize_optional_tickers(tickers)
        if normalized_tickers:
            params["symbols"] = ",".join(_alpaca_api_symbol(ticker) for ticker in normalized_tickers)
        if page_token:
            params["page_token"] = page_token

        payload = self._request_json(params=params)
        return AlpacaNewsPage(
            articles=_extract_news_articles(payload),
            next_page_token=_optional_page_token(payload),
            limit=limit,
        )

    def fetch_all_news(
        self,
        *,
        tickers: Sequence[str] | None,
        start_date: str,
        end_date: str,
        limit: int = DEFAULT_ALPACA_NEWS_PAGE_LIMIT,
        max_pages: int | None = None,
    ) -> list[dict[str, Any]]:
        """Fetch all Alpaca news rows for a date range, deduplicated by article identity."""
        if max_pages is not None and max_pages <= 0:
            raise ValueError("max_pages must be positive")

        articles: list[dict[str, Any]] = []
        seen: set[str] = set()
        for symbol_batch in _ticker_batches(tickers):
            page_token: str | None = None
            pages = 0
            while True:
                page = self.fetch_news_page(
                    tickers=symbol_batch,
                    start_date=start_date,
                    end_date=end_date,
                    limit=limit,
                    page_token=page_token,
                )
                for article in page.articles:
                    key = _article_key(article)
                    if key in seen:
                        continue
                    seen.add(key)
                    articles.append(article)

                pages += 1
                if not page.next_page_token:
                    break
                if max_pages is not None and pages >= max_pages:
                    break
                page_token = page.next_page_token

        return articles

    def fetch_news_day(
        self,
        *,
        tickers: Sequence[str] | None,
        as_of_date: str,
        limit: int = DEFAULT_ALPACA_NEWS_PAGE_LIMIT,
    ) -> list[dict[str, Any]]:
        """Fetch all Alpaca news rows for one calendar date."""
        normalized_date = _validate_date(as_of_date, "as_of_date")
        return self.fetch_all_news(
            tickers=tickers,
            start_date=normalized_date,
            end_date=normalized_date,
            limit=limit,
        )

    def _request_json(self, params: Mapping[str, Any]) -> Any:
        """Request one Alpaca news payload with bounded transient retries."""
        url = f"{self.config.base_url.rstrip('/')}{ALPACA_NEWS_ENDPOINT}"
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
        raise RuntimeError("Alpaca news request failed without an exception")


def _extract_news_articles(payload: Any) -> list[dict[str, Any]]:
    """Extract raw article objects from an Alpaca news response payload."""
    if not isinstance(payload, Mapping):
        raise ValueError("Alpaca news response must be a JSON object")
    raw_articles = payload.get("news")
    if raw_articles is None:
        return []
    if not isinstance(raw_articles, list):
        raise ValueError("Alpaca news response field news must be a list")

    articles: list[dict[str, Any]] = []
    for index, item in enumerate(raw_articles):
        if not isinstance(item, Mapping):
            raise ValueError(
                f"Alpaca news response item {index} must be an object, got {type(item).__name__}"
            )
        articles.append(dict(item))
    return articles


def _ticker_batches(tickers: Sequence[str] | None) -> list[tuple[str, ...] | None]:
    """Return Alpaca symbol batches small enough for request URLs and provider limits."""
    normalized = _normalize_optional_tickers(tickers)
    if not normalized:
        return [None]
    return [
        tuple(normalized[index : index + DEFAULT_ALPACA_NEWS_SYMBOL_BATCH_SIZE])
        for index in range(0, len(normalized), DEFAULT_ALPACA_NEWS_SYMBOL_BATCH_SIZE)
    ]


def _normalize_optional_tickers(tickers: Sequence[str] | None) -> tuple[str, ...]:
    """Normalize an optional sequence of Alpaca news symbols."""
    if not tickers:
        return ()
    normalized = tuple(_normalize_ticker(ticker) for ticker in tickers)
    deduped = tuple(dict.fromkeys(normalized))
    if len(deduped) != len(normalized):
        raise ValueError("tickers must not contain duplicates after normalization")
    return deduped


def _normalize_ticker(ticker: str) -> str:
    """Normalize one Alpaca news symbol to canonical archive syntax."""
    if not isinstance(ticker, str):
        raise TypeError("ticker must be a string")
    cleaned = ticker.strip().upper().replace(".", "-")
    if not cleaned:
        raise ValueError("ticker cannot be empty")
    return cleaned


def _alpaca_api_symbol(ticker: str) -> str:
    """Convert one canonical ticker to Alpaca's request symbol syntax."""
    return _normalize_ticker(ticker).replace("-", ".")


def _normalize_news_datetime(value: str, *, field_name: str, end: bool) -> str:
    """Normalize date-only inputs to full-day RFC-3339 UTC boundaries."""
    if not isinstance(value, str):
        raise TypeError(f"{field_name} must be a string")
    stripped = value.strip()
    if "T" in stripped:
        return stripped
    normalized_date = _validate_date(stripped, field_name)
    time_part = "23:59:59Z" if end else "00:00:00Z"
    return f"{normalized_date}T{time_part}"


def _validate_date(value: str, field_name: str) -> str:
    """Validate and normalize a YYYY-MM-DD date string."""
    try:
        return Date.fromisoformat(value.strip()).isoformat()
    except ValueError as exc:
        raise ValueError(f"{field_name} must be YYYY-MM-DD: {value}") from exc


def _optional_page_token(payload: Mapping[str, Any]) -> str | None:
    """Return an optional non-empty Alpaca pagination token."""
    token = payload.get("next_page_token")
    if token is None:
        return None
    if not isinstance(token, str):
        raise TypeError("Alpaca news next_page_token must be a string when present")
    stripped = token.strip()
    return stripped or None


def _article_key(article: Mapping[str, Any]) -> str:
    """Return a stable identifier for deduplicating Alpaca articles."""
    for field in ("id", "url"):
        value = article.get(field)
        if value is not None:
            return str(value)
    headline = str(article.get("headline") or article.get("title") or "")
    published = str(article.get("created_at") or article.get("updated_at") or "")
    return f"{headline}|{published}|{json.dumps(article, sort_keys=True)}"


def _is_retryable_error(error: requests.RequestException) -> bool:
    """Return True when a request error is likely transient."""
    response = getattr(error, "response", None)
    status_code = getattr(response, "status_code", None)
    return status_code in {429, 500, 502, 503, 504} or isinstance(
        error,
        (requests.ConnectionError, requests.Timeout),
    )
