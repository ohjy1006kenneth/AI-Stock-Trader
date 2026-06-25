"""Pre-FinBERT financial and ticker relevance gate for Layer 1 news rows."""
from __future__ import annotations

import importlib
import json
import math
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from core.contracts.schemas import NewsSentimentRecord

RELEVANCE_GATE_COLUMNS: tuple[str, ...] = (
    "date",
    "ticker",
    "article_id",
    "sentence_index",
    "chunk_index",
    "headline",
    "text",
    "source",
    "published_at",
    "relevance_decision",
    "relevance_score",
    "ticker_relevance_score",
    "financial_relevance_score",
    "topic_relevance_score",
    "reason_codes",
    "ticker_evidence",
    "entity_evidence",
    "topic_id",
    "topic_probability",
    "embedding_cache_key",
    "has_embedding",
)

_TOKEN_RE = re.compile(r"\b[a-z0-9][a-z0-9&.\-]*\b")
_CASHTAG_RE = re.compile(r"\$([A-Z][A-Z0-9.\-]{0,6})\b")
_FINANCIAL_TERMS = frozenset(
    {
        "analyst",
        "analysts",
        "buyback",
        "cash flow",
        "dividend",
        "earnings",
        "ebitda",
        "eps",
        "forecast",
        "guidance",
        "margin",
        "margins",
        "nasdaq",
        "outlook",
        "price target",
        "profit",
        "profits",
        "quarter",
        "quarterly",
        "rating",
        "revenue",
        "revenues",
        "results",
        "sales",
        "shares",
        "stock",
        "upgrade",
        "upgraded",
        "valuation",
    }
)
_BROAD_MARKET_TERMS = frozenset(
    {
        "bond yields",
        "dow",
        "federal reserve",
        "fed",
        "inflation",
        "interest rates",
        "market",
        "markets",
        "nasdaq",
        "s&p 500",
        "stocks",
        "treasury",
        "vix",
        "wall street",
    }
)
_TICKER_ALIASES: Mapping[str, tuple[str, ...]] = {
    "AAPL": ("apple", "apple inc"),
    "MSFT": ("microsoft", "microsoft corp", "microsoft corporation"),
    "GOOGL": ("alphabet", "google"),
    "GOOG": ("alphabet", "google"),
    "AMZN": ("amazon", "amazon.com"),
    "META": ("meta", "meta platforms", "facebook"),
    "NVDA": ("nvidia",),
    "TSLA": ("tesla",),
    "JPM": ("jpmorgan", "jp morgan", "jpmorgan chase"),
    "XOM": ("exxon", "exxon mobil", "exxonmobil"),
    "SPY": ("spy", "s&p 500", "spdr s&p 500 etf trust"),
}


@dataclass(frozen=True)
class NewsRelevanceGateConfig:
    """Thresholds for the pre-FinBERT news relevance gate."""

    accepted_threshold: float = 0.55
    borderline_threshold: float = 0.35
    min_financial_score: float = 0.35
    source_tag_ticker_score: float = 0.45
    min_topic_probability: float = 0.20

    def __post_init__(self) -> None:
        """Validate relevance thresholds."""
        for field_name in (
            "accepted_threshold",
            "borderline_threshold",
            "min_financial_score",
            "source_tag_ticker_score",
            "min_topic_probability",
        ):
            value = float(getattr(self, field_name))
            if math.isnan(value) or math.isinf(value) or value < 0.0 or value > 1.0:
                raise ValueError(f"{field_name} must be a finite probability")
        if self.borderline_threshold > self.accepted_threshold:
            raise ValueError("borderline_threshold must be <= accepted_threshold")


@dataclass(frozen=True)
class NewsRelevanceGateResult:
    """Rows that may be sent to FinBERT plus the full audit decision frame."""

    finbert_records: list[NewsSentimentRecord]
    audit_frame: Any
    input_rows: int
    accepted_rows: int
    borderline_rows: int
    rejected_rows: int


def apply_news_relevance_gate(
    records: Sequence[NewsSentimentRecord],
    *,
    embeddings: Any | None = None,
    topic_labels: Any | None = None,
    config: NewsRelevanceGateConfig | None = None,
) -> NewsRelevanceGateResult:
    """Filter preprocessed news rows to financially relevant ticker candidates."""
    pd = _require_pandas()
    settings = config or NewsRelevanceGateConfig()
    topic_lookup = _topic_lookup(topic_labels)
    embedding_lookup = _embedding_lookup(embeddings)

    passed: list[NewsSentimentRecord] = []
    audit_rows: list[dict[str, Any]] = []
    accepted_rows = 0
    borderline_rows = 0
    rejected_rows = 0

    for record in records:
        decision = _evaluate_record(
            record,
            topic_lookup=topic_lookup,
            embedding_lookup=embedding_lookup,
            config=settings,
        )
        audit_rows.append(decision)
        if decision["relevance_decision"] == "accepted":
            accepted_rows += 1
            passed.append(_record_with_relevance(record, decision["relevance_score"]))
        elif decision["relevance_decision"] == "borderline":
            borderline_rows += 1
            passed.append(_record_with_relevance(record, decision["relevance_score"]))
        else:
            rejected_rows += 1

    return NewsRelevanceGateResult(
        finbert_records=passed,
        audit_frame=pd.DataFrame(audit_rows, columns=list(RELEVANCE_GATE_COLUMNS)),
        input_rows=len(records),
        accepted_rows=accepted_rows,
        borderline_rows=borderline_rows,
        rejected_rows=rejected_rows,
    )


def _evaluate_record(
    record: NewsSentimentRecord,
    *,
    topic_lookup: Mapping[tuple[str, str, str], Mapping[str, Any]],
    embedding_lookup: Mapping[tuple[str, str], Mapping[str, Any]],
    config: NewsRelevanceGateConfig,
) -> dict[str, Any]:
    """Return one auditable gate decision row for a preprocessed news record."""
    ticker = record.ticker.strip().upper()
    provenance = _provenance(record)
    text = " ".join(part for part in (record.headline, record.text) if part)
    normalized_text = _normalize_text(text)
    source_tickers = _json_string_list(provenance.get("article_tickers"))
    chunk_tickers = _json_string_list(provenance.get("chunk_tickers"))
    entity_mentions = _json_string_list(
        provenance.get("entity_mentions") or list(record.entity_mentions)
    )

    ticker_score, ticker_reasons = _ticker_relevance(
        ticker=ticker,
        text=normalized_text,
        source_tickers=source_tickers,
        entity_mentions=entity_mentions,
    )
    financial_score, financial_reasons = _financial_relevance(normalized_text)
    topic_row = topic_lookup.get((record.date, ticker, _stable_article_id(record)))
    topic_score, topic_reasons = _topic_relevance(topic_row, config=config)
    embedding_row = embedding_lookup.get((record.date, _stable_article_id(record)))
    competitor_reasons = _competitor_reasons(
        ticker=ticker,
        normalized_text=normalized_text,
        entity_mentions=entity_mentions,
    )

    relevance_score = min(
        1.0,
        (0.65 * ticker_score) + (0.25 * financial_score) + (0.10 * topic_score),
    )
    reasons = [*ticker_reasons, *financial_reasons, *topic_reasons]
    if embedding_row is None:
        reasons.append("missing_embedding")
    if competitor_reasons and ticker_score < 1.0:
        relevance_score = min(relevance_score, 0.30)
        reasons.extend(competitor_reasons)

    if financial_score < config.min_financial_score:
        reasons.append("low_financial_relevance")
    if ticker_score <= 0.0:
        reasons.append("low_ticker_relevance")

    if (
        relevance_score >= config.accepted_threshold
        and ticker_score >= 0.75
        and financial_score >= config.min_financial_score
    ):
        decision = "accepted"
    elif (
        relevance_score >= config.borderline_threshold
        and ticker_score > 0.0
        and financial_score >= config.min_financial_score
        and not competitor_reasons
    ):
        decision = "borderline"
        reasons.append("borderline_ticker_or_topic_evidence")
    else:
        decision = "rejected"
        reasons.append("rejected_by_relevance_gate")

    topic_probability = _optional_float(topic_row.get("topic_probability")) if topic_row else None
    topic_id = _optional_int(topic_row.get("topic_id")) if topic_row else None
    embedding_cache_key = (
        _optional_text(embedding_row.get("embedding_cache_key")) if embedding_row else None
    )
    return {
        "date": record.date,
        "ticker": ticker,
        "article_id": record.article_id,
        "sentence_index": record.sentence_index,
        "chunk_index": record.chunk_index,
        "headline": record.headline,
        "text": record.text,
        "source": record.source,
        "published_at": record.published_at.isoformat() if record.published_at else None,
        "relevance_decision": decision,
        "relevance_score": relevance_score,
        "ticker_relevance_score": ticker_score,
        "financial_relevance_score": financial_score,
        "topic_relevance_score": topic_score,
        "reason_codes": json.dumps(sorted(set(reasons))),
        "ticker_evidence": json.dumps(
            {
                "source_tickers": source_tickers,
                "chunk_tickers": chunk_tickers,
            },
            sort_keys=True,
        ),
        "entity_evidence": json.dumps(entity_mentions),
        "topic_id": topic_id,
        "topic_probability": topic_probability,
        "embedding_cache_key": embedding_cache_key,
        "has_embedding": embedding_row is not None,
    }


def _ticker_relevance(
    *,
    ticker: str,
    text: str,
    source_tickers: Sequence[str],
    entity_mentions: Sequence[str],
) -> tuple[float, list[str]]:
    """Score ticker/entity evidence separately from financial relevance."""
    reasons: list[str] = []
    upper_source_tickers = {value.strip().upper() for value in source_tickers}
    aliases = _aliases_for_ticker(ticker)
    text_tokens = {token.upper() for token in _TOKEN_RE.findall(text)}

    if ticker in text_tokens or f"${ticker.lower()}" in text:
        reasons.append("direct_ticker_mention")
        return 1.0, reasons
    if ticker in {value.strip().upper() for value in _CASHTAG_RE.findall(text.upper())}:
        reasons.append("direct_cashtag_mention")
        return 1.0, reasons
    if any(_contains_phrase(text, alias) for alias in aliases):
        reasons.append("target_entity_mention")
        return 1.0, reasons
    if _entity_matches_alias(entity_mentions, aliases):
        reasons.append("target_entity_mention")
        return 1.0, reasons
    if ticker in upper_source_tickers:
        reasons.append("source_ticker_tag_only")
        return 0.45, reasons
    return 0.0, reasons


def _financial_relevance(text: str) -> tuple[float, list[str]]:
    """Score financial relevance without considering whether the target ticker appears."""
    reasons: list[str] = []
    matched_specific = sorted(term for term in _FINANCIAL_TERMS if _contains_phrase(text, term))
    matched_broad = sorted(term for term in _BROAD_MARKET_TERMS if _contains_phrase(text, term))
    if matched_specific:
        reasons.append("financial_terms:" + ",".join(matched_specific[:5]))
    if matched_broad:
        reasons.append("broad_market_terms:" + ",".join(matched_broad[:5]))
    score = 0.0
    if matched_specific:
        score = min(1.0, 0.45 + (0.15 * min(len(matched_specific), 4)))
    if matched_broad:
        score = max(score, min(0.85, 0.40 + (0.10 * min(len(matched_broad), 4))))
    return score, reasons


def _topic_relevance(
    topic_row: Mapping[str, Any] | None,
    *,
    config: NewsRelevanceGateConfig,
) -> tuple[float, list[str]]:
    """Score existing topic-label confidence as supporting relevance evidence."""
    if topic_row is None:
        return 0.0, ["missing_topic_label"]
    probability = _optional_float(topic_row.get("topic_probability"))
    topic_id = _optional_int(topic_row.get("topic_id"))
    if topic_id is None or topic_id < 0:
        return 0.0, ["topic_outlier"]
    if probability is None:
        return 0.0, ["missing_topic_probability"]
    if probability < config.min_topic_probability:
        return probability, ["low_topic_probability"]
    return probability, ["topic_probability_supported"]


def _competitor_reasons(
    *,
    ticker: str,
    normalized_text: str,
    entity_mentions: Sequence[str],
) -> list[str]:
    """Return reason codes when another known company appears without target evidence."""
    reasons: list[str] = []
    matched_competitors: list[str] = []
    for candidate_ticker, aliases in _TICKER_ALIASES.items():
        if candidate_ticker == ticker:
            continue
        if candidate_ticker in {"SPY"}:
            continue
        if any(_contains_phrase(normalized_text, alias) for alias in aliases):
            matched_competitors.append(candidate_ticker)
            continue
        if _entity_matches_alias(entity_mentions, aliases):
            matched_competitors.append(candidate_ticker)
    if matched_competitors:
        reasons.append("competitor_entity_without_target:" + ",".join(sorted(set(matched_competitors))))
    return reasons


def _record_with_relevance(
    record: NewsSentimentRecord,
    relevance_score: Any,
) -> NewsSentimentRecord:
    """Return a contract row with an explicit gate relevance score."""
    return NewsSentimentRecord(
        date=record.date,
        ticker=record.ticker,
        headline=record.headline,
        normalized_headline=record.normalized_headline,
        text=record.text,
        article_id=record.article_id,
        sentence_index=record.sentence_index,
        chunk_index=record.chunk_index,
        source=record.source,
        url=record.url,
        published_at=record.published_at,
        source_text_field=record.source_text_field,
        source_text_order=record.source_text_order,
        source_text_provenance=dict(record.source_text_provenance),
        ticker_mentions=tuple(record.ticker_mentions),
        entity_mentions=tuple(record.entity_mentions),
        sentiment_positive=record.sentiment_positive,
        sentiment_negative=record.sentiment_negative,
        sentiment_neutral=record.sentiment_neutral,
        sentiment_score=record.sentiment_score,
        relevance_score=float(relevance_score),
    )


def _topic_lookup(frame: Any | None) -> dict[tuple[str, str, str], Mapping[str, Any]]:
    """Return topic-label rows keyed by date, ticker, and article id."""
    lookup: dict[tuple[str, str, str], Mapping[str, Any]] = {}
    if frame is None or len(frame) == 0:
        return lookup
    for row in frame.to_dict(orient="records"):
        date = _optional_text(row.get("date"))
        ticker = _optional_text(row.get("ticker"))
        article_id = _optional_text(row.get("article_id"))
        if date is None or ticker is None or article_id is None:
            continue
        lookup[(date, ticker.upper(), article_id)] = row
    return lookup


def _embedding_lookup(frame: Any | None) -> dict[tuple[str, str], Mapping[str, Any]]:
    """Return embedding rows keyed by date and article id."""
    lookup: dict[tuple[str, str], Mapping[str, Any]] = {}
    if frame is None or len(frame) == 0:
        return lookup
    for row in frame.to_dict(orient="records"):
        date = _optional_text(row.get("date"))
        article_id = _optional_text(row.get("article_id"))
        if date is None or article_id is None:
            continue
        lookup[(date, article_id)] = row
    return lookup


def _stable_article_id(record: NewsSentimentRecord) -> str:
    """Return the article id shape used by text-topic artifacts."""
    if record.article_id:
        return record.article_id
    payload = "|".join(
        [
            record.date,
            record.normalized_headline or "",
            record.published_at.isoformat() if record.published_at else "",
            record.url or "",
        ]
    )
    import hashlib

    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


def _provenance(record: NewsSentimentRecord) -> Mapping[str, Any]:
    """Return row provenance as a mapping."""
    if isinstance(record.source_text_provenance, Mapping):
        return record.source_text_provenance
    return {}


def _json_string_list(value: Any) -> list[str]:
    """Return a string list from decoded or JSON-encoded provenance values."""
    if value is None:
        return []
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return []
        try:
            value = json.loads(text)
        except json.JSONDecodeError:
            return [text]
    if isinstance(value, Sequence) and not isinstance(value, (bytes, bytearray, str)):
        return [str(item).strip() for item in value if str(item).strip()]
    return []


def _aliases_for_ticker(ticker: str) -> tuple[str, ...]:
    """Return known entity aliases for one ticker."""
    return (ticker.lower(), *_TICKER_ALIASES.get(ticker, ()))


def _entity_matches_alias(entity_mentions: Sequence[str], aliases: Sequence[str]) -> bool:
    """Return True when entity evidence matches a target ticker alias."""
    normalized_entities = {_normalize_text(entity) for entity in entity_mentions}
    for alias in aliases:
        normalized_alias = _normalize_text(alias)
        if normalized_alias in normalized_entities:
            return True
    return False


def _contains_phrase(text: str, phrase: str) -> bool:
    """Return True when the normalized text contains a complete phrase."""
    normalized_phrase = _normalize_text(phrase)
    if not normalized_phrase:
        return False
    if " " in normalized_phrase:
        return f" {normalized_phrase} " in f" {text} "
    return normalized_phrase in set(_TOKEN_RE.findall(text))


def _normalize_text(value: Any) -> str:
    """Return lowercase whitespace-normalized text."""
    return re.sub(r"\s+", " ", str(value or "").strip().lower())


def _optional_text(value: Any) -> str | None:
    """Return stripped non-empty text, or None."""
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _optional_float(value: Any) -> float | None:
    """Return a finite float, or None."""
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
    """Return an int from a finite numeric value, or None."""
    numeric = _optional_float(value)
    if numeric is None:
        return None
    return int(numeric)


def _require_pandas() -> Any:
    """Import pandas/pyarrow lazily with a clear error when unavailable."""
    try:
        import pandas as pd

        importlib.import_module("pyarrow")
    except ModuleNotFoundError as exc:
        raise ModuleNotFoundError(
            "pandas and pyarrow are required for news relevance gating."
        ) from exc
    return pd
