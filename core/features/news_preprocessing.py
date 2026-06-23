"""Layer 1 NLP preprocessing for raw Layer 0 news archives."""
from __future__ import annotations

import hashlib
import json
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
_TICKER_TEXT_PATTERN = re.compile(r"\b[A-Z][A-Z0-9.-]{0,6}\b")
_ENTITY_PHRASE_PATTERN = re.compile(
    r"\b(?:[A-Z][a-z0-9&]+(?:\s+[A-Z][a-z0-9&]+){0,3}|[A-Z]{2,}(?:\s+[A-Z]{2,})*)\b"
)
_ENTITY_STOPWORDS = frozenset(
    {
        "article",
        "earnings",
        "full",
        "new",
        "product",
        "quarterly",
        "reported",
        "reports",
        "results",
        "shares",
        "text",
    }
)
_ENTITY_ALIAS_RULES: tuple[tuple[str, str], ...] = (
    ("apple inc", "Apple"),
    ("apple", "Apple"),
    ("berkshire hathaway", "Berkshire Hathaway"),
    ("federal reserve", "Federal Reserve"),
    ("meta platforms", "Meta Platforms"),
    ("meta", "Meta Platforms"),
    ("microsoft corporation", "Microsoft"),
    ("microsoft corp", "Microsoft"),
    ("microsoft", "Microsoft"),
    ("s&p 500", "S&P 500"),
    ("spdr s&p 500 etf trust", "SPY"),
    ("sec", "SEC"),
)


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


@dataclass(frozen=True)
class NewsTextChunk:
    """One reviewable sentence/chunk extracted from a raw news article."""

    text: str
    source_field: str
    source_order: int
    chunk_index: int

    def to_dict(self) -> dict[str, object]:
        """Return a JSON-serializable representation."""
        return {
            "text": self.text,
            "source_field": self.source_field,
            "source_order": self.source_order,
            "chunk_index": self.chunk_index,
        }


def split_article_chunks(
    article: Mapping[str, Any],
    *,
    config: NewsPreprocessingConfig | None = None,
) -> list[NewsTextChunk]:
    """Return reviewable sentence/chunk rows with per-field provenance preserved."""
    settings = config or NewsPreprocessingConfig()
    chunks: list[NewsTextChunk] = []
    for source_field, include_field in (
        ("headline", settings.include_headline),
        ("summary", settings.include_summary),
        ("content", settings.include_content),
    ):
        if not include_field:
            continue
        for source_order, sentence in enumerate(_sentences_from_text(article.get(source_field), settings=settings)):
            chunks.append(
                NewsTextChunk(
                    text=sentence,
                    source_field=source_field,
                    source_order=source_order,
                    chunk_index=len(chunks),
                )
            )
    return chunks


def preprocess_news_articles(
    articles: Sequence[Mapping[str, Any]],
    *,
    as_of_date: str,
    point_in_time_tickers: Iterable[str] | None,
    config: NewsPreprocessingConfig | None = None,
) -> list[NewsSentimentRecord]:
    """Convert raw Layer 0 news articles into reviewable sentence/chunk records."""
    normalized_date = _validate_date(as_of_date)
    settings = config or NewsPreprocessingConfig()
    allowed_tickers = _normalize_allowed_tickers(point_in_time_tickers)

    records: list[NewsSentimentRecord] = []
    for article in articles:
        article_tickers = sorted(
            {
                *_article_tickers(article),
                *_article_text_tickers(article, allowed_tickers),
            }
        )
        if allowed_tickers is not None:
            article_tickers = sorted(ticker for ticker in article_tickers if ticker in allowed_tickers)
        if not article_tickers:
            continue

        article_id = _article_id(article)
        headline = _optional_text(article.get("headline"))
        normalized_headline = _normalize_headline(headline)
        source = _optional_text(article.get("source") or article.get("author"))
        url = _optional_text(article.get("url"))
        published_at = _published_at(article)
        chunks = split_article_chunks(article, config=settings)
        if not chunks:
            continue

        for chunk in chunks:
            chunk_tickers = tuple(_ticker_mentions(chunk.text, article_tickers, allowed_tickers))
            entity_mentions = tuple(_entity_mentions(chunk.text, headline=headline))
            provenance = _source_text_provenance(
                article=article,
                article_id=article_id,
                headline=headline,
                normalized_headline=normalized_headline,
                chunk=chunk,
                article_tickers=article_tickers,
                chunk_tickers=chunk_tickers,
                entity_mentions=entity_mentions,
            )
            for ticker in article_tickers:
                record_ticker_mentions = tuple(
                    _dedupe_preserving_order([ticker, *chunk_tickers])
                )
                records.append(
                    NewsSentimentRecord(
                        date=normalized_date,
                        ticker=ticker,
                        headline=headline,
                        normalized_headline=normalized_headline,
                        text=chunk.text,
                        article_id=article_id,
                        sentence_index=chunk.chunk_index,
                        chunk_index=chunk.chunk_index,
                        source=source,
                        url=url,
                        published_at=published_at,
                        source_text_field=chunk.source_field,
                        source_text_order=chunk.source_order,
                        source_text_provenance=provenance,
                        ticker_mentions=record_ticker_mentions,
                        entity_mentions=entity_mentions,
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
    return _dedupe_preserving_order(
        chunk.text for chunk in split_article_chunks(article, config=config)
    )


def records_to_news_sentiment_frame(records: Sequence[NewsSentimentRecord]) -> Any:
    """Return a pandas DataFrame with Parquet-ready NewsSentimentRecord rows."""
    pd = _require_pandas()
    rows = [
        {
            "date": record.date,
            "ticker": record.ticker,
            "headline": record.headline,
            "normalized_headline": record.normalized_headline,
            "text": record.text,
            "article_id": record.article_id,
            "sentence_index": record.sentence_index,
            "chunk_index": record.chunk_index,
            "source": record.source,
            "url": record.url,
            "published_at": record.published_at.isoformat() if record.published_at else None,
            "source_text_field": record.source_text_field,
            "source_text_order": record.source_text_order,
            "source_text_provenance": json.dumps(record.source_text_provenance, sort_keys=True),
            "ticker_mentions": json.dumps(list(record.ticker_mentions)),
            "entity_mentions": json.dumps(list(record.entity_mentions)),
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
                normalized_headline=_optional_text(row.get("normalized_headline")),
                text=_optional_text(row.get("text")),
                article_id=_optional_text(row.get("article_id")),
                sentence_index=_optional_int(row.get("sentence_index")),
                chunk_index=_optional_int(row.get("chunk_index")),
                source=_optional_text(row.get("source")),
                url=_optional_text(row.get("url")),
                published_at=_optional_datetime(row.get("published_at")),
                source_text_field=_optional_text(row.get("source_text_field")),
                source_text_order=_optional_int(row.get("source_text_order")),
                source_text_provenance=_optional_json_object(row.get("source_text_provenance")),
                ticker_mentions=tuple(_optional_json_list(row.get("ticker_mentions"))),
                entity_mentions=tuple(_optional_json_list(row.get("entity_mentions"))),
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
    "normalized_headline",
    "text",
    "article_id",
    "sentence_index",
    "chunk_index",
    "source",
    "url",
    "published_at",
    "source_text_field",
    "source_text_order",
    "source_text_provenance",
    "ticker_mentions",
    "entity_mentions",
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


def _article_text_tickers(
    article: Mapping[str, Any],
    allowed_tickers: set[str] | None,
) -> list[str]:
    """Return tickers detected from raw article text when point-in-time filtering is available."""
    if allowed_tickers is None:
        return []

    combined_text = " ".join(
        _optional_text(article.get(field)) or "" for field in ("headline", "summary", "content")
    ).strip()
    if not combined_text:
        return []

    tickers: list[str] = []
    for candidate in _TICKER_TEXT_PATTERN.findall(combined_text):
        canonical = canonicalize_ticker(candidate)
        if canonical and canonical in allowed_tickers:
            tickers.append(canonical)
    return _dedupe_preserving_order(tickers)


def _normalize_allowed_tickers(tickers: Iterable[str] | None) -> set[str] | None:
    """Return normalized point-in-time ticker allow-list, or None when disabled."""
    if tickers is None:
        return None
    normalized = {canonicalize_ticker(ticker) for ticker in tickers if str(ticker).strip()}
    return {ticker for ticker in normalized if ticker}


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


def _dedupe_preserving_order(values: Iterable[str]) -> list[str]:
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


def _normalize_headline(value: str | None) -> str:
    """Normalize a headline for duplicate detection and auditing."""
    if not value:
        return ""
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9]+", " ", value.lower())).strip()


def _ticker_mentions(
    text: str,
    article_tickers: Sequence[str],
    allowed_tickers: set[str] | None,
) -> list[str]:
    """Return ticker mentions detected from article tags and source text."""
    mentions: list[str] = []
    for ticker in article_tickers:
        mentions.append(ticker)
    if allowed_tickers is not None:
        for candidate in _TICKER_TEXT_PATTERN.findall(text or ""):
            canonical = canonicalize_ticker(candidate)
            if canonical and canonical in allowed_tickers:
                mentions.append(canonical)
    return _dedupe_preserving_order(mentions)


def _entity_mentions(text: str, *, headline: str | None = None) -> list[str]:
    """Return auditable entity mentions detected from the article text."""
    combined_text = " ".join(part for part in (headline, text) if part).strip()
    if not combined_text:
        return []

    normalized = combined_text.lower()
    mentions: list[str] = []
    for alias, entity in _ENTITY_ALIAS_RULES:
        if _text_contains_term(normalized, alias):
            mentions.append(entity)

    if mentions:
        return _dedupe_preserving_order(mentions)

    for phrase in _ENTITY_PHRASE_PATTERN.findall(combined_text):
        cleaned = phrase.strip()
        if not cleaned:
            continue
        if cleaned.lower() in _ENTITY_STOPWORDS:
            continue
        if all(token.lower() in _ENTITY_STOPWORDS for token in cleaned.split()):
            continue
        mentions.append(cleaned)
    return _dedupe_preserving_order(mentions)


def _source_text_provenance(
    *,
    article: Mapping[str, Any],
    article_id: str,
    headline: str | None,
    normalized_headline: str,
    chunk: NewsTextChunk,
    article_tickers: Sequence[str],
    chunk_tickers: Sequence[str],
    entity_mentions: Sequence[str],
) -> dict[str, Any]:
    """Return raw article and chunk provenance for one preprocessed row."""
    published_at = _published_at(article)
    return {
        "article_id": article_id,
        "source_field": chunk.source_field,
        "source_order": chunk.source_order,
        "chunk_index": chunk.chunk_index,
        "headline": headline,
        "normalized_headline": normalized_headline,
        "source": _optional_text(article.get("source") or article.get("author")),
        "published_at": published_at.isoformat() if published_at else None,
        "raw_headline": _optional_text(article.get("headline")),
        "raw_summary": _optional_text(article.get("summary")),
        "raw_content": _optional_text(article.get("content")),
        "source_field_text": _optional_text(article.get(chunk.source_field)),
        "article_tickers": list(article_tickers),
        "chunk_tickers": list(chunk_tickers),
        "entity_mentions": list(entity_mentions),
    }


def _optional_json_object(value: Any) -> dict[str, Any]:
    """Return a JSON object or an empty dictionary for missing values."""
    if value is None:
        return {}
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return {}
        try:
            parsed = json.loads(text)
        except (json.JSONDecodeError, TypeError):
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return {}


def _optional_json_list(value: Any) -> list[str]:
    """Return a JSON list or an empty list for missing values."""
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item) for item in value if _optional_text(item) is not None]
    if isinstance(value, tuple):
        return [str(item) for item in value if _optional_text(item) is not None]
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return []
        try:
            parsed = json.loads(text)
        except (json.JSONDecodeError, TypeError):
            return []
        if not isinstance(parsed, list):
            return []
        return [str(item) for item in parsed if _optional_text(item) is not None]
    return []


def _text_contains_term(text: str, term: str) -> bool:
    """Return True when a normalized term appears in text."""
    if not text or not term:
        return False
    if " " in term:
        return term in text.lower()
    return re.search(rf"\b{re.escape(term.lower())}\b", text.lower()) is not None


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
