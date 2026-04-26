"""Layer 1 NLP preprocessing for raw Layer 0 news archives."""
from __future__ import annotations

import hashlib
import math
import re
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date as Date
from datetime import datetime
from typing import Any

from core.contracts.schemas import NewsSentimentRecord
from services.wikipedia.sp500_universe import canonicalize_ticker

_SENTENCE_BOUNDARY = re.compile(r"(?<=[.!?])\s+(?=[\"'A-Z0-9])")
_WHITESPACE = re.compile(r"\s+")


@dataclass(frozen=True)
class NewsPreprocessingConfig:
    """Text and filtering settings for sentence-level news preprocessing."""

    min_sentence_chars: int = 2
    include_headline: bool = True
    include_summary: bool = True
    include_content: bool = True

    def __post_init__(self) -> None:
        """Validate text preprocessing settings."""
        if self.min_sentence_chars <= 0:
            raise ValueError("min_sentence_chars must be positive")
        if not (self.include_headline or self.include_summary or self.include_content):
            raise ValueError("At least one text field must be included")


def preprocess_news_articles(
    articles: Sequence[Mapping[str, Any]],
    *,
    as_of_date: str,
    point_in_time_tickers: Iterable[str] | None,
    config: NewsPreprocessingConfig | None = None,
) -> list[NewsSentimentRecord]:
    """Convert raw Layer 0 news articles into sentence-level sentiment records."""
    normalized_date = _validate_date(as_of_date)
    settings = config or NewsPreprocessingConfig()
    allowed_tickers = _normalize_allowed_tickers(point_in_time_tickers)

    records: list[NewsSentimentRecord] = []
    for article in articles:
        article_tickers = _article_tickers(article)
        if allowed_tickers is not None:
            article_tickers = sorted(ticker for ticker in article_tickers if ticker in allowed_tickers)
        if not article_tickers:
            continue

        sentences = split_article_sentences(article, config=settings)
        if not sentences:
            continue

        article_id = _article_id(article)
        headline = _optional_text(article.get("headline"))
        source = _optional_text(article.get("source") or article.get("author"))
        url = _optional_text(article.get("url"))
        published_at = _published_at(article)

        for sentence_index, sentence in enumerate(sentences):
            for ticker in article_tickers:
                records.append(
                    NewsSentimentRecord(
                        date=normalized_date,
                        ticker=ticker,
                        headline=headline,
                        text=sentence,
                        article_id=article_id,
                        sentence_index=sentence_index,
                        source=source,
                        url=url,
                        published_at=published_at,
                    )
                )

    return sorted(
        records,
        key=lambda record: (
            record.published_at.isoformat() if record.published_at else "",
            record.article_id or "",
            record.sentence_index if record.sentence_index is not None else -1,
            record.ticker,
        ),
    )


def split_article_sentences(
    article: Mapping[str, Any],
    *,
    config: NewsPreprocessingConfig | None = None,
) -> list[str]:
    """Return normalized article sentences from configured raw text fields."""
    settings = config or NewsPreprocessingConfig()
    chunks: list[str] = []
    if settings.include_headline:
        chunks.extend(_sentences_from_text(article.get("headline"), settings=settings))
    if settings.include_summary:
        chunks.extend(_sentences_from_text(article.get("summary"), settings=settings))
    if settings.include_content:
        chunks.extend(_sentences_from_text(article.get("content"), settings=settings))
    return _dedupe_preserving_order(chunks)


def records_to_news_sentiment_frame(records: Sequence[NewsSentimentRecord]) -> Any:
    """Return a pandas DataFrame with Parquet-ready NewsSentimentRecord rows."""
    pd = _require_pandas()
    rows = [
        {
            "date": record.date,
            "ticker": record.ticker,
            "headline": record.headline,
            "text": record.text,
            "article_id": record.article_id,
            "sentence_index": record.sentence_index,
            "source": record.source,
            "url": record.url,
            "published_at": record.published_at.isoformat() if record.published_at else None,
            "sentiment_positive": record.sentiment_positive,
            "sentiment_negative": record.sentiment_negative,
            "sentiment_neutral": record.sentiment_neutral,
            "sentiment_score": record.sentiment_score,
            "relevance_score": record.relevance_score,
        }
        for record in records
    ]
    return pd.DataFrame(rows, columns=list(_NEWS_SENTIMENT_COLUMNS))


def news_sentiment_frame_to_records(frame: Any) -> list[NewsSentimentRecord]:
    """Convert a DataFrame of sentence-level rows into contract records."""
    records: list[NewsSentimentRecord] = []
    for row in frame.to_dict(orient="records"):
        records.append(
            NewsSentimentRecord(
                date=str(row["date"]),
                ticker=str(row["ticker"]),
                headline=_optional_text(row.get("headline")),
                text=_optional_text(row.get("text")),
                article_id=_optional_text(row.get("article_id")),
                sentence_index=_optional_int(row.get("sentence_index")),
                source=_optional_text(row.get("source")),
                url=_optional_text(row.get("url")),
                published_at=_optional_datetime(row.get("published_at")),
                sentiment_positive=_optional_float(row.get("sentiment_positive")),
                sentiment_negative=_optional_float(row.get("sentiment_negative")),
                sentiment_neutral=_optional_float(row.get("sentiment_neutral")),
                sentiment_score=_optional_float(row.get("sentiment_score")),
                relevance_score=_optional_float(row.get("relevance_score")),
            )
        )
    return records


_NEWS_SENTIMENT_COLUMNS: tuple[str, ...] = (
    "date",
    "ticker",
    "headline",
    "text",
    "article_id",
    "sentence_index",
    "source",
    "url",
    "published_at",
    "sentiment_positive",
    "sentiment_negative",
    "sentiment_neutral",
    "sentiment_score",
    "relevance_score",
)


def _article_tickers(article: Mapping[str, Any]) -> list[str]:
    """Return normalized ticker tags from one raw news article."""
    raw_symbols = article.get("symbols") or article.get("tickers") or []
    if isinstance(raw_symbols, str):
        raw_symbols = [raw_symbols]
    if not isinstance(raw_symbols, Iterable):
        raise TypeError("Article symbols must be a sequence or string")

    tickers: set[str] = set()
    for raw_symbol in raw_symbols:
        if raw_symbol is None:
            continue
        ticker = canonicalize_ticker(str(raw_symbol))
        if ticker:
            tickers.add(ticker)
    return sorted(tickers)


def _normalize_allowed_tickers(tickers: Iterable[str] | None) -> set[str] | None:
    """Return normalized point-in-time ticker allow-list, or None when disabled."""
    if tickers is None:
        return None
    return {canonicalize_ticker(ticker) for ticker in tickers if str(ticker).strip()}


def _sentences_from_text(value: Any, *, settings: NewsPreprocessingConfig) -> list[str]:
    """Split and normalize one raw text field into sentence strings."""
    text = _optional_text(value)
    if text is None:
        return []
    normalized = _WHITESPACE.sub(" ", text).strip()
    sentences = [
        sentence.strip(" \t\r\n")
        for sentence in _SENTENCE_BOUNDARY.split(normalized)
        if len(sentence.strip(" \t\r\n")) >= settings.min_sentence_chars
    ]
    return sentences


def _dedupe_preserving_order(values: Sequence[str]) -> list[str]:
    """Remove exact duplicate strings without changing first-seen order."""
    seen: set[str] = set()
    deduped: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        deduped.append(value)
    return deduped


def _published_at(article: Mapping[str, Any]) -> datetime | None:
    """Return the point-in-time article timestamp without changing source precision."""
    return _optional_datetime(
        article.get("published_at")
        or article.get("publishedDate")
        or article.get("created_at")
        or article.get("createdAt")
    )


def _article_id(article: Mapping[str, Any]) -> str:
    """Return a stable article identity suitable for downstream caching."""
    for field in ("id", "article_id", "url"):
        value = _optional_text(article.get(field))
        if value is not None:
            return value
    digest_source = "|".join(
        _optional_text(article.get(field)) or "" for field in ("headline", "summary", "content")
    )
    digest = hashlib.sha256(digest_source.encode("utf-8")).hexdigest()
    return f"news-{digest[:16]}"


def _optional_text(value: Any) -> str | None:
    """Return stripped non-empty text, or None for missing values."""
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    text = str(value).strip()
    return text or None


def _optional_datetime(value: Any) -> datetime | None:
    """Return an ISO timestamp parsed by Pydantic, or None when missing."""
    text = _optional_text(value)
    if text is None:
        return None
    return NewsSentimentRecord(date="2000-01-01", ticker="SPY", published_at=text).published_at


def _optional_float(value: Any) -> float | None:
    """Return a finite float value, or None for missing/non-finite values."""
    if value is None:
        return None
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(numeric) or math.isinf(numeric):
        return None
    return numeric


def _optional_int(value: Any) -> int | None:
    """Return an integer value, or None for missing/non-finite values."""
    numeric = _optional_float(value)
    if numeric is None:
        return None
    return int(numeric)


def _validate_date(value: str) -> str:
    """Validate and normalize a YYYY-MM-DD date string."""
    try:
        return Date.fromisoformat(value).isoformat()
    except ValueError as exc:
        raise ValueError(f"as_of_date must be YYYY-MM-DD: {value}") from exc


def _require_pandas() -> Any:
    """Import pandas lazily with a clear error when unavailable."""
    try:
        import pandas as pd
    except ModuleNotFoundError as exc:
        raise ModuleNotFoundError("pandas is required for news preprocessing frames.") from exc
    return pd
