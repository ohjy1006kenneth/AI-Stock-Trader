"""Ticker assignment provenance helpers for Layer 1 semantic review evidence."""
from __future__ import annotations

import html
import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from enum import StrEnum
from typing import Any

from services.wikipedia.sp500_universe import canonicalize_ticker


class NewsEvidenceClass(StrEnum):
    """High-level provenance classes for Layer 1 ticker assignment."""

    DIRECT = "direct"
    INDIRECT = "indirect"
    BROAD_MARKET = "broad_market"
    CONTAMINATION = "contamination"


NEWS_EVIDENCE_RELEVANCE_WEIGHTS: dict[NewsEvidenceClass, float] = {
    NewsEvidenceClass.DIRECT: 1.0,
    NewsEvidenceClass.INDIRECT: 0.25,
    NewsEvidenceClass.BROAD_MARKET: 0.0,
    NewsEvidenceClass.CONTAMINATION: 0.0,
}

_CLASS_ORDER: tuple[NewsEvidenceClass, ...] = (
    NewsEvidenceClass.DIRECT,
    NewsEvidenceClass.INDIRECT,
    NewsEvidenceClass.BROAD_MARKET,
    NewsEvidenceClass.CONTAMINATION,
)

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

_TARGET_DIRECT_ALIASES: dict[str, tuple[str, ...]] = {
    "AAPL": ("apple", "apple inc", "apple inc.", "apple computer"),
    "AMD": ("amd", "advanced micro devices"),
    "AMZN": ("amazon", "amazon.com", "amazon inc"),
    "BRK-B": ("berkshire hathaway", "berkshire"),
    "GOOGL": ("alphabet", "google", "google parent"),
    "INTC": ("intel", "intel corp", "intel corporation"),
    "META": ("meta", "meta platforms", "facebook"),
    "MSFT": ("microsoft", "microsoft corp", "microsoft corporation"),
    "NVDA": ("nvidia", "nvidia corp", "nvidia corporation"),
    "QQQ": ("nasdaq 100", "invesco qqq", "qqq etf"),
    "SNAP": ("snap", "snap inc", "snapchat"),
    "SPY": ("spdr s&p 500", "s&p 500 etf", "spy etf"),
    "TSLA": ("tesla", "tesla inc"),
}

_TARGET_RELATIONSHIP_ALIASES: dict[str, tuple[str, ...]] = {
    "AAPL": (
        "intel",
        "qualcomm",
        "samsung",
        "tsmc",
        "foxconn",
        "chip supplier",
        "supplier",
        "supply chain",
        "iphone supplier",
    ),
    "AMD": ("intel", "nvidia", "semiconductor", "chipmaker", "supply chain"),
    "AMZN": ("microsoft", "google", "alphabet", "cloud rival", "competitor"),
    "GOOGL": ("microsoft", "amazon", "meta", "openai", "competitor"),
    "META": ("google", "alphabet", "tiktok", "snap", "competitor"),
    "MSFT": ("openai", "google", "alphabet", "amazon", "cloud", "competitor"),
    "NVDA": ("amd", "intel", "tsmc", "chipmaker", "semiconductor", "data center"),
    "SNAP": ("meta", "alphabet", "google", "tiktok", "competitor"),
    "TSLA": ("ford", "gm", "general motors", "rivian", "competitor"),
}

_BROAD_MARKET_TERMS: tuple[str, ...] = (
    "ai",
    "artificial intelligence",
    "broad market",
    "dow",
    "etf",
    "fed",
    "federal reserve",
    "inflation",
    "interest rates",
    "market",
    "markets",
    "nasdaq",
    "nasdaq 100",
    "qqq",
    "rally",
    "rotation",
    "sector",
    "s&p 500",
    "spy",
    "stocks",
    "tech stocks",
    "treasury",
    "vix",
    "wall street",
)

_RELATION_CONTEXT_TERMS: tuple[str, ...] = (
    "competitor",
    "competitors",
    "peer",
    "peers",
    "partner",
    "partners",
    "supplier",
    "suppliers",
    "customer",
    "customers",
    "rival",
    "rivals",
    "relationship",
    "relationships",
    "supply chain",
    "chipmaker",
    "semiconductor",
    "cloud",
    "data center",
)

_OTHER_COMPANY_TERMS: tuple[str, ...] = (
    "amd",
    "alphabet",
    "amazon",
    "facebook",
    "google",
    "intel",
    "meta",
    "microsoft",
    "nvidia",
    "qualcomm",
    "snap",
    "spdr s&p 500",
    "tsmc",
    "qqq",
    "spy",
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


def classify_news_assignment(
    article: Mapping[str, Any],
    ticker: str,
    *,
    headline: str | None,
    text: str | None,
) -> NewsEvidenceClass:
    """Classify one ticker assignment into a reviewable evidence class."""
    normalized_ticker = _normalize_ticker(ticker)
    article_text = _normalize_text(" ".join(part for part in (headline, text) if part))
    provider_ticker_source, provider_tickers = _provider_tickers(article)
    provider_matches = tuple(value for value in provider_tickers if value == normalized_ticker)
    direct_matches = _ticker_mentions(article_text, normalized_ticker)
    alias_matches, entity_matches = _target_alias_matches(article, article_text, normalized_ticker)
    relationship_matches = _relationship_matches(article_text, normalized_ticker)
    broad_market_matches = _broad_market_matches(article_text)
    contamination_matches = _contamination_matches(
        article,
        normalized_ticker,
        provider_tickers=provider_tickers,
        provider_matches=provider_matches,
        direct_matches=direct_matches,
        alias_matches=alias_matches,
        relationship_matches=relationship_matches,
        broad_market_matches=broad_market_matches,
    )

    if direct_matches:
        return NewsEvidenceClass.DIRECT
    if relationship_matches:
        return NewsEvidenceClass.INDIRECT
    if alias_matches and not _negated_alias_match(article_text, alias_matches):
        return NewsEvidenceClass.DIRECT
    if contamination_matches:
        return NewsEvidenceClass.CONTAMINATION
    if broad_market_matches:
        return NewsEvidenceClass.BROAD_MARKET
    return NewsEvidenceClass.CONTAMINATION


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
    alias_matches, entity_matches = _target_alias_matches(article, article_text, normalized_ticker)
    relationship_matches = _relationship_matches(article_text, normalized_ticker)
    broad_market_matches = _broad_market_matches(article_text)
    provider_tag_only = bool(provider_matches) and not _has_textual_target_evidence(
        direct_matches=direct_matches,
        alias_matches=alias_matches,
        entity_matches=entity_matches,
        relationship_matches=relationship_matches,
    )
    contamination_matches = _contamination_matches(
        article,
        normalized_ticker,
        provider_tickers=provider_tickers,
        provider_matches=provider_matches,
        direct_matches=direct_matches,
        alias_matches=alias_matches,
        relationship_matches=relationship_matches,
        broad_market_matches=broad_market_matches,
    )

    classification = classify_news_assignment(
        article,
        normalized_ticker,
        headline=headline,
        text=text,
    )
    evidence_kinds = _evidence_kinds(
        provider_matches=provider_matches,
        provider_tag_only=provider_tag_only,
        direct_matches=direct_matches,
        alias_matches=alias_matches,
        entity_matches=entity_matches,
        relationship_matches=relationship_matches,
        broad_market_matches=broad_market_matches,
        contamination_matches=contamination_matches,
    )
    reason = _build_reason(
        ticker=normalized_ticker,
        classification=classification,
        provider_matches=provider_matches,
        provider_tag_only=provider_tag_only,
        direct_matches=direct_matches,
        alias_matches=alias_matches,
        entity_matches=entity_matches,
        relationship_matches=relationship_matches,
        broad_market_matches=broad_market_matches,
        contamination_matches=contamination_matches,
        semantic_similarity_score=semantic_similarity_score,
        topic_id=topic_id,
        topic_probability=topic_probability,
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
        matched_symbols=provider_matches or direct_matches or _contamination_symbols(article),
        matched_aliases=alias_matches,
        matched_entities=entity_matches,
        evidence_kinds=tuple(evidence_kinds),
        classification=classification.value,
        reason=reason,
        semantic_similarity_score=semantic_similarity_score,
        topic_id=topic_id,
        topic_probability=topic_probability,
    )


def build_ticker_assignment_review_payload(
    rows: Sequence[TickerAssignmentProvenance],
) -> dict[str, object]:
    """Return a grouped review payload for dashboard display."""
    grouped: dict[str, list[dict[str, object]]] = {
        evidence_class.value: [] for evidence_class in _CLASS_ORDER
    }
    for row in rows:
        grouped.setdefault(row.classification, []).append(row.to_dict())
    summary = {key: len(value) for key, value in grouped.items()}
    summary["total_rows"] = len(rows)
    return {
        "summary": summary,
        "rows_by_class": grouped,
        "rows": [row.to_dict() for row in rows],
    }


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


def _target_alias_matches(
    article: Mapping[str, Any],
    article_text: str,
    ticker: str,
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    """Return target company alias and entity matches present in the article text."""
    alias_matches: list[str] = []
    entity_matches: list[str] = []
    lowered_text = article_text.lower()
    target_aliases = {alias.lower() for alias in _TARGET_DIRECT_ALIASES.get(ticker, ())}
    for field_name in _ALIAS_FIELDS:
        raw_values = article.get(field_name)
        if raw_values is None:
            continue
        for value in _normalize_sequence(raw_values):
            candidate = _normalize_display_text(value)
            if not candidate:
                continue
            normalized = candidate.lower()
            if normalized not in target_aliases or normalized not in lowered_text:
                continue
            if field_name in {"entity", "entities", "matched_entities"}:
                entity_matches.append(candidate)
            else:
                alias_matches.append(candidate)
    for alias in _TARGET_DIRECT_ALIASES.get(ticker, ()):
        if _contains_phrase(lowered_text, alias):
            alias_matches.append(alias)
    return tuple(_dedupe(alias_matches)), tuple(_dedupe(entity_matches))


def _negated_alias_match(article_text: str, aliases: Sequence[str]) -> bool:
    """Return True when an alias appears only in a negated or missing-context phrase."""
    lowered_text = article_text.lower()
    for alias in aliases:
        if any(
            phrase in lowered_text
            for phrase in (
                f"no {alias.lower()}",
                f"not {alias.lower()}",
                f"without {alias.lower()}",
                f"no {alias.lower()} context",
                f"without {alias.lower()} context",
            )
        ):
            return True
    return False


def _relationship_matches(article_text: str, ticker: str) -> tuple[str, ...]:
    """Return target-relationship matches that indicate indirect evidence."""
    lowered_text = article_text.lower()
    aliases = _TARGET_RELATIONSHIP_ALIASES.get(ticker, ())
    matches: list[str] = []
    for alias in aliases:
        if _contains_phrase(lowered_text, alias):
            matches.append(alias)
    if matches and any(_contains_phrase(lowered_text, term) for term in _RELATION_CONTEXT_TERMS):
        return tuple(_dedupe(matches))
    if any(_contains_phrase(lowered_text, alias) for alias in _TARGET_DIRECT_ALIASES.get(ticker, ())) and any(
        _contains_phrase(lowered_text, term) for term in _RELATION_CONTEXT_TERMS
    ):
        return ("target_relationship_context",)
    return ()


def _broad_market_matches(article_text: str) -> tuple[str, ...]:
    """Return broad-market context matches."""
    lowered_text = article_text.lower()
    matches = [term for term in _BROAD_MARKET_TERMS if _contains_phrase(lowered_text, term)]
    return tuple(_dedupe(matches))


def _contamination_matches(
    article: Mapping[str, Any],
    ticker: str,
    *,
    provider_tickers: Sequence[str],
    provider_matches: Sequence[str],
    direct_matches: Sequence[str],
    alias_matches: Sequence[str],
    relationship_matches: Sequence[str],
    broad_market_matches: Sequence[str],
) -> tuple[str, ...]:
    """Return other-company evidence that contaminates an assignment."""
    article_text = _normalize_text(
        " ".join(
            _optional_text(article.get(field)) or "" for field in ("headline", "summary", "content")
        )
    ).lower()
    if direct_matches:
        return ()
    if alias_matches and not _negated_alias_match(article_text, alias_matches):
        return ()
    if relationship_matches:
        return ()
    contamination: list[str] = []
    for value in provider_tickers:
        if value and value != ticker:
            contamination.append(value)
    for term in _OTHER_COMPANY_TERMS:
        if _contains_phrase(article_text, term):
            contamination.append(term)
    if contamination:
        return tuple(_dedupe(contamination))
    if broad_market_matches:
        return ()
    return ("no_target_evidence",)


def _contamination_symbols(article: Mapping[str, Any]) -> tuple[str, ...]:
    """Return other provider tickers that may contaminate a target assignment."""
    _, provider_tickers = _provider_tickers(article)
    return provider_tickers


def _evidence_kinds(
    *,
    provider_matches: Sequence[str],
    provider_tag_only: bool,
    direct_matches: Sequence[str],
    alias_matches: Sequence[str],
    entity_matches: Sequence[str],
    relationship_matches: Sequence[str],
    broad_market_matches: Sequence[str],
    contamination_matches: Sequence[str],
) -> tuple[str, ...]:
    """Return granular evidence kinds for a provenance row."""
    kinds: list[str] = []
    if provider_matches:
        kinds.append("provider_ticker_tag_only" if provider_tag_only else "provider_ticker_tag")
    if direct_matches:
        kinds.append("direct_ticker_mention")
    if alias_matches:
        kinds.append("company_alias_entity_match")
    if relationship_matches:
        kinds.append("relationship_context")
    if broad_market_matches:
        kinds.append("broad_market_context")
    if contamination_matches:
        kinds.append("contamination_other_entity")
    if entity_matches and not alias_matches:
        kinds.append("entity_phrase_match")
    if not kinds:
        kinds.append("fallback_no_target_evidence")
    return tuple(_dedupe(kinds))


def _build_reason(
    *,
    ticker: str,
    classification: NewsEvidenceClass,
    provider_matches: Sequence[str],
    provider_tag_only: bool,
    direct_matches: Sequence[str],
    alias_matches: Sequence[str],
    entity_matches: Sequence[str],
    relationship_matches: Sequence[str],
    broad_market_matches: Sequence[str],
    contamination_matches: Sequence[str],
    semantic_similarity_score: float | None,
    topic_id: int | None,
    topic_probability: float | None,
) -> str:
    """Return a compact human-readable explanation for one assignment."""
    parts: list[str] = [f"classification={classification.value}"]
    if provider_matches:
        if provider_tag_only:
            parts.append(
                f"provider ticker tag only for {', '.join(provider_matches)}; no textual target evidence"
            )
        else:
            parts.append(f"provider ticker tag matched {', '.join(provider_matches)}")
    if direct_matches:
        parts.append(f"ticker symbol mention matched {', '.join(direct_matches)}")
    if alias_matches:
        parts.append(f"company alias matched {', '.join(alias_matches)}")
    if entity_matches:
        parts.append(f"entity match found {', '.join(entity_matches)}")
    if relationship_matches:
        parts.append(f"relationship context matched {', '.join(relationship_matches)}")
    if broad_market_matches:
        parts.append(f"broad market context matched {', '.join(broad_market_matches)}")
    if contamination_matches:
        parts.append(f"contamination evidence matched {', '.join(contamination_matches)}")
    if semantic_similarity_score is not None:
        parts.append(f"semantic similarity/relevance score={semantic_similarity_score:.3f}")
    if topic_id is not None or topic_probability is not None:
        topic_bits: list[str] = []
        if topic_id is not None:
            topic_bits.append(f"topic_id={topic_id}")
        if topic_probability is not None:
            topic_bits.append(f"topic_probability={topic_probability:.3f}")
        parts.append("topic context " + ", ".join(topic_bits))
    return "; ".join(parts) + "."


def _contains_phrase(text: str, phrase: str) -> bool:
    """Return True when a phrase appears as a token boundary match."""
    if not text or not phrase:
        return False
    pattern = rf"(?<![a-z0-9]){re.escape(phrase.lower())}(?![a-z0-9])"
    return re.search(pattern, text) is not None


def _has_textual_target_evidence(
    *,
    direct_matches: Sequence[str],
    alias_matches: Sequence[str],
    entity_matches: Sequence[str],
    relationship_matches: Sequence[str],
) -> bool:
    """Return True when the article text explicitly supports the target ticker."""
    return bool(direct_matches or alias_matches or entity_matches or relationship_matches)


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
        pattern = re.compile(rf"(?<![A-Z0-9]){re.escape(candidate)}(?![A-Z0-9])", flags=re.IGNORECASE)
        if pattern.search(article_text):
            matches.append(candidate.upper())
    return tuple(_dedupe(matches))


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
