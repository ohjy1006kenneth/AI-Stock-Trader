"""Ticker assignment provenance helpers for Layer 1 semantic review evidence."""
from __future__ import annotations

import html
import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from typing import Any

from services.wikipedia.sp500_universe import canonicalize_ticker

_TICKER_TOKEN_RE_TEMPLATE = r"(?<![A-Z0-9]){ticker}(?![A-Z0-9])"
_PROVIDER_TICKER_FIELDS: tuple[str, ...] = ("symbols", "tickers")
_ALIAS_FIELDS: tuple[str, ...] = (
    "company_name",
    "company_names",
    "company",
    "company_alias",
    "company_aliases",
    "entity",
    "entities",
    "aliases",
    "issuer_name",
    "matched_entities",
    "matched_aliases",
)


@dataclass(frozen=True)
class TickerAssignmentProvenance:
    """Deterministic evidence payload for assigning one article chunk to a ticker."""

    date: str
    ticker: str
    article_id: str | None
    sentence_index: int | None
    headline: str | None
    text: str | None
    source: str | None
    provider_ticker_source: str | None
    provider_tickers: tuple[str, ...]
    matched_symbols: tuple[str, ...]
    matched_aliases: tuple[str, ...]
    matched_entities: tuple[str, ...]
    evidence_kinds: tuple[str, ...]
    classification: str
    reason: str
    semantic_similarity_score: float | None = None
    topic_id: int | None = None
    topic_probability: float | None = None

    def to_dict(self) -> dict[str, object]:
        """Return a JSON-serializable representation."""
        return asdict(self)


def build_ticker_assignment_provenance(
    article: Mapping[str, Any],
    ticker: str,
    *,
    date: str,
    sentence_index: int | None,
    headline: str | None,
    text: str | None,
    semantic_similarity_score: float | None = None,
    topic_id: int | None = None,
    topic_probability: float | None = None,
) -> TickerAssignmentProvenance:
    """Build a deterministic provenance record for one article chunk/ticker assignment."""
    normalized_ticker = _normalize_ticker(ticker)
    article_text = _normalize_text(" ".join(part for part in (headline, text) if part))
    provider_ticker_source, provider_tickers = _provider_tickers(article)
    provider_matches = tuple(value for value in provider_tickers if value == normalized_ticker)
    direct_matches = _ticker_mentions(article_text, normalized_ticker)
    alias_matches, entity_matches = _alias_entity_matches(article, article_text)

    evidence_kinds: list[str] = []
    if provider_matches:
        evidence_kinds.append("provider_ticker_tag")
    if direct_matches:
        evidence_kinds.append("direct_ticker_mention")
    if alias_matches or entity_matches:
        evidence_kinds.append("company_alias_entity_match")
    if semantic_similarity_score is not None:
        evidence_kinds.append("semantic_similarity")
    if topic_id is not None or topic_probability is not None:
        evidence_kinds.append("topic_context")
    if not evidence_kinds:
        evidence_kinds.append("fallback_missing_evidence")

    classification = _classify_assignment(evidence_kinds)
    reason = _build_reason(
        ticker=normalized_ticker,
        provider_matches=provider_matches,
        direct_matches=direct_matches,
        alias_matches=alias_matches,
        entity_matches=entity_matches,
        semantic_similarity_score=semantic_similarity_score,
        topic_id=topic_id,
        topic_probability=topic_probability,
        fallback=classification == "fallback",
    )
    return TickerAssignmentProvenance(
        date=date,
        ticker=normalized_ticker,
        article_id=_optional_text(article.get("id") or article.get("article_id")),
        sentence_index=sentence_index,
        headline=headline,
        text=text,
        source=_optional_text(article.get("source") or article.get("author")),
        provider_ticker_source=provider_ticker_source,
        provider_tickers=provider_tickers,
        matched_symbols=provider_matches or direct_matches,
        matched_aliases=alias_matches,
        matched_entities=entity_matches,
        evidence_kinds=tuple(evidence_kinds),
        classification=classification,
        reason=reason,
        semantic_similarity_score=semantic_similarity_score,
        topic_id=topic_id,
        topic_probability=topic_probability,
    )


def provenance_rows_to_json(rows: Sequence[TickerAssignmentProvenance]) -> str:
    """Serialize provenance rows to a deterministic JSON array."""
    payload = [row.to_dict() for row in rows]
    return json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False)


def _provider_tickers(article: Mapping[str, Any]) -> tuple[str | None, tuple[str, ...]]:
    """Return the provider ticker field name and canonicalized tickers."""
    for field_name in _PROVIDER_TICKER_FIELDS:
        raw_values = article.get(field_name)
        if raw_values is None:
            continue
        values = [canonicalize_ticker(value) for value in _normalize_sequence(raw_values)]
        values = [value for value in values if value]
        if values:
            return field_name, tuple(_dedupe(values))
    return None, ()


def _alias_entity_matches(article: Mapping[str, Any], article_text: str) -> tuple[tuple[str, ...], tuple[str, ...]]:
    """Return company alias and entity matches present in the article text."""
    alias_matches: list[str] = []
    entity_matches: list[str] = []
    lowered_text = article_text.lower()
    for field_name in _ALIAS_FIELDS:
        raw_values = article.get(field_name)
        if raw_values is None:
            continue
        for value in _normalize_sequence(raw_values):
            candidate = _normalize_display_text(value)
            if not candidate:
                continue
            if candidate.lower() not in lowered_text:
                continue
            if field_name in {"entity", "entities", "matched_entities"}:
                entity_matches.append(candidate)
            else:
                alias_matches.append(candidate)
    return tuple(_dedupe(alias_matches)), tuple(_dedupe(entity_matches))


def _ticker_mentions(article_text: str, ticker: str) -> tuple[str, ...]:
    """Return exact ticker-token matches within the article text."""
    if not article_text:
        return ()
    candidates = {ticker}
    if "-" in ticker:
        candidates.add(ticker.replace("-", "."))
    if "." in ticker:
        candidates.add(ticker.replace(".", "-"))
    matches: list[str] = []
    for candidate in sorted(candidates):
        pattern = re.compile(
            _TICKER_TOKEN_RE_TEMPLATE.format(ticker=re.escape(candidate)),
            flags=re.IGNORECASE,
        )
        if pattern.search(article_text):
            matches.append(candidate.upper())
    return tuple(_dedupe(matches))


def _classify_assignment(evidence_kinds: Sequence[str]) -> str:
    """Return a human-readable assignment classification from evidence kinds."""
    kinds = set(evidence_kinds)
    if {"provider_ticker_tag", "direct_ticker_mention"} & kinds:
        return "direct"
    if "company_alias_entity_match" in kinds:
        return "indirect"
    if {"semantic_similarity", "topic_context"} & kinds:
        return "contextual"
    return "fallback"


def _build_reason(
    *,
    ticker: str,
    provider_matches: Sequence[str],
    direct_matches: Sequence[str],
    alias_matches: Sequence[str],
    entity_matches: Sequence[str],
    semantic_similarity_score: float | None,
    topic_id: int | None,
    topic_probability: float | None,
    fallback: bool,
) -> str:
    """Return a compact human-readable explanation for one assignment."""
    parts: list[str] = []
    if provider_matches:
        parts.append(f"provider ticker tag matched {', '.join(provider_matches)}")
    if direct_matches:
        parts.append(f"ticker symbol mention matched {', '.join(direct_matches)}")
    if alias_matches:
        parts.append(f"company alias matched {', '.join(alias_matches)}")
    if entity_matches:
        parts.append(f"entity match found {', '.join(entity_matches)}")
    if semantic_similarity_score is not None:
        parts.append(f"semantic similarity/relevance score={semantic_similarity_score:.3f}")
    if topic_id is not None or topic_probability is not None:
        topic_bits: list[str] = []
        if topic_id is not None:
            topic_bits.append(f"topic_id={topic_id}")
        if topic_probability is not None:
            topic_bits.append(f"topic_probability={topic_probability:.3f}")
        parts.append("topic context " + ", ".join(topic_bits))
    if fallback or not parts:
        return (
            f"No direct ticker, alias/entity, provider-tag, semantic, or topic evidence for {ticker}; "
            "fallback only."
        )
    return "; ".join(parts) + "."


def _normalize_sequence(value: object) -> tuple[str, ...]:
    """Return a tuple of normalized non-empty strings from a scalar or sequence."""
    if isinstance(value, str):
        values = [value]
    elif isinstance(value, Sequence):
        values = [str(item) for item in value]
    else:
        values = [str(value)]
    normalized: list[str] = []
    for item in values:
        cleaned = _normalize_display_text(item)
        if cleaned:
            normalized.append(cleaned)
    return tuple(_dedupe(normalized))


def _normalize_ticker(ticker: str) -> str:
    """Return a normalized uppercase ticker token."""
    return ticker.strip().upper().replace(".", "-")


def _normalize_text(value: str) -> str:
    """Return lower-noise plain text for evidence matching."""
    text = html.unescape(value or "")
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def _normalize_display_text(value: str) -> str:
    """Return a compact display string without control characters."""
    return re.sub(r"\s+", " ", html.unescape(str(value))).strip()


def _optional_text(value: object) -> str | None:
    """Return a stripped string or None when empty."""
    if value is None:
        return None
    text = _normalize_display_text(str(value))
    return text or None


def _dedupe(values: Sequence[str]) -> list[str]:
    """Remove duplicate strings while preserving order."""
    seen: set[str] = set()
    ordered: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        ordered.append(value)
    return ordered
