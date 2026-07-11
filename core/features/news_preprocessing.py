"""Layer 1 NLP preprocessing for raw Layer 0 news archives."""
from __future__ import annotations

import hashlib
import html
import math
import re
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date as Date
from datetime import datetime
from typing import Any

from pydantic import TypeAdapter

from core.contracts.schemas import NewsSentimentRecord
from services.wikipedia.sp500_universe import canonicalize_ticker

_HTML_BLOCK_BREAK_START = re.compile(
    r"(?is)<\s*(?:p|div|li|tr|td|th|h[1-6]|section|article|header|footer|blockquote|ul|ol)\b[^>]*>"
)
_HTML_BLOCK_BREAK_END = re.compile(
    r"(?is)</\s*(?:p|div|li|tr|td|th|h[1-6]|section|article|header|footer|blockquote|ul|ol)\b[^>]*>"
)
_HTML_LINE_BREAK = re.compile(r"(?is)<\s*(?:br|hr)\b[^>]*>")
_HTML_NOISE_BLOCKS = (
    re.compile(r"(?is)<script\b[^>]*>.*?</script>"),
    re.compile(r"(?is)<style\b[^>]*>.*?</style>"),
    re.compile(r"(?is)<noscript\b[^>]*>.*?</noscript>"),
    re.compile(r"(?is)<iframe\b[^>]*>.*?</iframe>"),
    re.compile(
        r"(?is)<blockquote\b[^>]*class=[\"'][^\"']*"
        r"(?:twitter-tweet|instagram-media|tiktok-embed|reddit-embed|embed|widget)"
        r"[^\"']*[\"'][^>]*>.*?</blockquote>"
    ),
    re.compile(
        r"(?is)<div\b[^>]*class=[\"'][^\"']*"
        r"(?:widget|embed|promo|related|newsletter|ad|advert|sponsored|social-embed)"
        r"[^\"']*[\"'][^>]*>.*?</div>"
    ),
)
_HTML_TAG = re.compile(r"<[^>]+>")
_INLINE_WHITESPACE = re.compile(r"[ \t\f\v]+")
_PARAGRAPH_BREAK = re.compile(r"\n+")
_SENTENCE_BOUNDARY = re.compile(r'([.!?]+(?:["\')\]]+)?)\s+')
_ABBREVIATION_TOKENS = {
    "a.m",
    "co",
    "corp",
    "dr",
    "e.u",
    "fig",
    "inc",
    "jr",
    "ltd",
    "mr",
    "mrs",
    "ms",
    "mt",
    "no",
    "p.m",
    "prof",
    "sr",
    "st",
    "u.k",
    "u.s",
    "vs",
}
_DATETIME_ADAPTER = TypeAdapter(datetime)


@dataclass(frozen=True)
class NewsPreprocessingConfig:
    """Text splitting and filtering settings for sentence-level news preprocessing."""

    min_sentence_chars: int = 2
    target_chunk_chars: int = 240
    max_chunk_chars: int = 360
    fallback_chunk_chars: int = 160
    include_headline: bool = True
    include_summary: bool = True
    include_content: bool = True

    def __post_init__(self) -> None:
        """Validate text preprocessing settings."""
        if self.min_sentence_chars <= 0:
            raise ValueError("min_sentence_chars must be positive")
        if self.target_chunk_chars <= 0:
            raise ValueError("target_chunk_chars must be positive")
        if self.max_chunk_chars <= 0:
            raise ValueError("max_chunk_chars must be positive")
        if self.fallback_chunk_chars <= 0:
            raise ValueError("fallback_chunk_chars must be positive")
        if self.target_chunk_chars > self.max_chunk_chars:
            raise ValueError("target_chunk_chars must be <= max_chunk_chars")
        if self.fallback_chunk_chars > self.target_chunk_chars:
            raise ValueError("fallback_chunk_chars must be <= target_chunk_chars")
        if not (self.include_headline or self.include_summary or self.include_content):
            raise ValueError("At least one text field must be included")


def preprocess_news_articles(
    articles: Sequence[Mapping[str, Any]],
    *,
    as_of_date: str,
    point_in_time_tickers: Iterable[str] | None,
    config: NewsPreprocessingConfig | None = None,
) -> list[NewsSentimentRecord]:
    """Convert raw Layer 0 news articles into sentence/chunk sentiment records."""
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

        chunks = split_article_sentences(article, config=settings)
        if not chunks:
            continue

        article_id = _article_id(article)
        headline = _clean_article_text(article.get("headline"))
        source = _optional_text(article.get("source") or article.get("author"))
        url = _optional_text(article.get("url"))
        published_at = _published_at(article)

        for sentence_index, chunk in enumerate(chunks):
            for ticker in article_tickers:
                records.append(
                    NewsSentimentRecord(
                        date=normalized_date,
                        ticker=ticker,
                        headline=headline,
                        text=chunk,
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
    """Return normalized article sentences/chunks from configured raw text fields."""
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
    """Split and normalize one raw text field into bounded sentence/chunk strings."""
    text = _clean_article_text(value)
    if text is None:
        return []

    chunks: list[str] = []
    for block in _split_text_blocks(text):
        block_chunks = _split_block_into_chunks(block, settings=settings)
        chunks.extend(block_chunks)
    return chunks


def _split_text_blocks(text: str) -> list[str]:
    """Return paragraph- and list-level blocks from cleaned article text."""
    blocks: list[str] = []
    for raw_block in _PARAGRAPH_BREAK.split(text):
        block = _INLINE_WHITESPACE.sub(" ", raw_block).strip()
        if block:
            blocks.append(block)
    return blocks


def _split_block_into_chunks(
    block: str,
    *,
    settings: NewsPreprocessingConfig,
) -> list[str]:
    """Split one paragraph-like block into bounded chunks."""
    sentence_like = _split_sentence_candidates(block, settings=settings)
    chunks: list[str] = []
    for sentence in sentence_like:
        if len(sentence) > settings.max_chunk_chars:
            chunks.extend(_split_oversized_text(sentence, settings=settings))
        else:
            chunks.append(sentence)
    return chunks


def _split_sentence_candidates(
    text: str,
    *,
    settings: NewsPreprocessingConfig,
) -> list[str]:
    """Split a cleaned block into sentence-like units while honoring abbreviations."""
    sentences: list[str] = []
    start = 0
    for match in _SENTENCE_BOUNDARY.finditer(text):
        boundary_end = match.end(1)
        candidate = text[start:boundary_end].strip()
        remainder = text[match.end():]
        if not candidate:
            start = match.end()
            continue
        if _should_split_sentence(candidate, remainder):
            sentences.append(candidate)
            start = match.end()
    tail = text[start:].strip()
    if tail:
        sentences.append(tail)
    return [sentence for sentence in sentences if len(sentence) >= settings.min_sentence_chars]


def _should_split_sentence(candidate: str, remainder: str) -> bool:
    """Return True when a punctuation boundary should end the current sentence."""
    if not remainder.strip():
        return True
    return not _looks_like_abbreviation(candidate)


def _looks_like_abbreviation(text: str) -> bool:
    """Return True when trailing punctuation belongs to a known abbreviation."""
    stripped = text.rstrip('"\'”’)]}')
    if not stripped.endswith((".", "!", "?")):
        return False
    last_token = stripped.split()[-1]
    normalized = re.sub(r"[^A-Za-z0-9]", "", last_token).lower()
    if normalized in _ABBREVIATION_TOKENS:
        return True
    if re.fullmatch(r"(?:[A-Za-z]\.){2,}", last_token):
        return True
    return False


def _split_oversized_text(text: str, *, settings: NewsPreprocessingConfig) -> list[str]:
    """Split an oversized sentence into readable fallback chunks."""
    if len(text) <= settings.max_chunk_chars:
        return [text]

    words = text.split()
    if len(words) <= 1:
        return _split_single_token(text, limit=settings.fallback_chunk_chars)

    chunks: list[str] = []
    current_words: list[str] = []
    current_length = 0
    for word in words:
        candidate_length = len(word) if not current_words else current_length + 1 + len(word)
        if current_words and candidate_length > settings.target_chunk_chars:
            chunks.append(" ".join(current_words))
            current_words = [word]
            current_length = len(word)
            continue
        current_words.append(word)
        current_length = candidate_length

    if current_words:
        chunks.append(" ".join(current_words))

    if all(len(chunk) <= settings.max_chunk_chars for chunk in chunks):
        return chunks
    return [chunk for piece in chunks for chunk in _split_single_token(piece, limit=settings.fallback_chunk_chars)]


def _split_single_token(text: str, *, limit: int) -> list[str]:
    """Split a long token into fixed-size pieces without dropping text."""
    if len(text) <= limit:
        return [text]
    pieces: list[str] = []
    for start in range(0, len(text), limit):
        pieces.append(text[start : start + limit])
    return pieces


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


def _clean_article_text(value: Any) -> str | None:
    """Return readable plain text from raw provider HTML or text content."""
    text = _optional_text(value)
    if text is None:
        return None

    cleaned = html.unescape(text)
    for pattern in _HTML_NOISE_BLOCKS:
        cleaned = pattern.sub(" ", cleaned)
    cleaned = _HTML_LINE_BREAK.sub("\n", cleaned)
    cleaned = _HTML_BLOCK_BREAK_START.sub("\n", cleaned)
    cleaned = _HTML_BLOCK_BREAK_END.sub("\n", cleaned)
    cleaned = _HTML_TAG.sub(" ", cleaned)
    cleaned = cleaned.replace("\xa0", " ")
    cleaned = re.sub(r"[ \t\f\v]*\n[ \t\f\v]*", "\n", cleaned)
    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned)
    cleaned = _INLINE_WHITESPACE.sub(" ", cleaned)
    cleaned = re.sub(r" *\n *", "\n", cleaned)
    cleaned = re.sub(r"\s+([,.;:!?])", r"\1", cleaned)
    cleaned = cleaned.strip(" \n\t\r")
    return cleaned or None


def _optional_datetime(value: Any) -> datetime | None:
    """Return an ISO timestamp parsed by Pydantic, or None when missing."""
    text = _optional_text(value)
    if text is None:
        return None
    return _DATETIME_ADAPTER.validate_python(text)


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
