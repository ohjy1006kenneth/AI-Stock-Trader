from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date as Date
from typing import Any

import requests

from services.tiingo.ohlcv_fetcher import TiingoClientConfig

TIINGO_NEWS_ENDPOINT = "/tiingo/news"
DEFAULT_NEWS_PAGE_LIMIT = 1000


@dataclass(frozen=True)
class NewsPage:
    """One page of Tiingo news results."""

    articles: list[dict[str, Any]]
    offset: int
    limit: int


class TiingoNewsFetcher:
    """Fetch raw Tiingo news articles and handle pagination/deduplication."""

    def __init__(
        self,
        config: TiingoClientConfig,
        session: requests.Session | None = None,
    ) -> None:
        """Store client configuration and HTTP session."""
        self.config = config
        self.session = session or requests.Session()

    def fetch_news_rows(
        self,
        *,
        tickers: Sequence[str] | None,
        start_date: str,
        end_date: str,
        limit: int = DEFAULT_NEWS_PAGE_LIMIT,
        offset: int = 0,
    ) -> NewsPage:
        """Fetch a single page of Tiingo news rows for a date range."""
        _validate_date(start_date, "start_date")
        _validate_date(end_date, "end_date")
        if start_date > end_date:
            raise ValueError("start_date must be <= end_date")
        if limit <= 0:
            raise ValueError("limit must be positive")
        if offset < 0:
            raise ValueError("offset must be non-negative")

        params: dict[str, Any] = {
            "startDate": start_date,
            "endDate": end_date,
            "limit": limit,
            "offset": offset,
            "token": self.config.api_token,
        }
        normalized = _normalize_tickers(tickers)
        if normalized:
            params["tickers"] = ",".join(normalized)

        url = f"{self.config.base_url.rstrip('/')}{TIINGO_NEWS_ENDPOINT}"
        response = self.session.get(url, params=params, timeout=self.config.timeout_seconds)
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, list):
            raise ValueError("Tiingo news response must be a JSON list")
        return NewsPage(articles=[dict(item) for item in payload], offset=offset, limit=limit)

    def fetch_all_news(
        self,
        *,
        tickers: Sequence[str] | None,
        start_date: str,
        end_date: str,
        limit: int = DEFAULT_NEWS_PAGE_LIMIT,
        max_pages: int | None = None,
    ) -> list[dict[str, Any]]:
        """Fetch all Tiingo news rows for a date range, deduplicated."""
        offset = 0
        seen: set[str] = set()
        articles: list[dict[str, Any]] = []
        pages = 0

        while True:
            page = self.fetch_news_rows(
                tickers=tickers,
                start_date=start_date,
                end_date=end_date,
                limit=limit,
                offset=offset,
            )
            if not page.articles:
                break

            for article in page.articles:
                key = _article_key(article)
                if key in seen:
                    continue
                seen.add(key)
                articles.append(article)

            if len(page.articles) < page.limit:
                break

            offset += page.limit
            pages += 1
            if max_pages is not None and pages >= max_pages:
                break

        return articles

    def fetch_news_day(
        self,
        *,
        tickers: Sequence[str] | None,
        as_of_date: str,
        limit: int = DEFAULT_NEWS_PAGE_LIMIT,
    ) -> list[dict[str, Any]]:
        """Fetch all Tiingo news rows for a single date."""
        return self.fetch_all_news(
            tickers=tickers,
            start_date=as_of_date,
            end_date=as_of_date,
            limit=limit,
        )


def _normalize_tickers(tickers: Sequence[str] | None) -> list[str]:
    """Normalize ticker text for Tiingo requests."""
    if not tickers:
        return []
    normalized: list[str] = []
    for ticker in tickers:
        if not isinstance(ticker, str):
            raise TypeError("tickers must be strings")
        cleaned = ticker.strip().upper().replace(".", "-")
        if not cleaned:
            raise ValueError("ticker cannot be empty")
        normalized.append(cleaned)
    return normalized


def _validate_date(value: str, field_name: str) -> str:
    """Validate and normalize a YYYY-MM-DD date string."""
    try:
        return Date.fromisoformat(value).isoformat()
    except ValueError as exc:
        raise ValueError(f"{field_name} must be YYYY-MM-DD: {value}") from exc


def _article_key(article: Mapping[str, Any]) -> str:
    """Return a stable identifier for deduplicating articles."""
    for field in ("id", "articleId", "url"):
        value = article.get(field)
        if value is not None:
            return str(value)
    title = str(article.get("title") or "")
    published = str(article.get("publishedDate") or article.get("published_at") or "")
    try:
        import json
    except ModuleNotFoundError:
        return f"{title}|{published}|{sorted(article.keys())}"
    return f"{title}|{published}|{json.dumps(article, sort_keys=True, default=str)}"
