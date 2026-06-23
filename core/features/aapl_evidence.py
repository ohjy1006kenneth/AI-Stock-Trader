"""Semantic-review evidence assembly for the Layer 1 AAPL pilot dashboard."""
from __future__ import annotations

import json
import re
from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from datetime import date as Date
from io import BytesIO
from typing import Any

import pandas as pd

from services.r2.paths import layer1_regime_path, layer1_sentiment_score_path
from services.r2.writer import R2Writer

DEFAULT_RELEVANCE_THRESHOLD = 0.6


@dataclass(frozen=True)
class SemanticReviewSentenceRow:
    """Sentence-level FinBERT evidence for one scored-news row."""

    sentence_index: int | None
    text: str | None
    sentiment_score: float | None
    positive_probability: float | None
    negative_probability: float | None
    neutral_probability: float | None
    relevance_score: float | None
    row_granularity: str

    def to_dict(self) -> dict[str, object]:
        """Return a JSON-serializable representation."""
        return asdict(self)


@dataclass(frozen=True)
class SemanticReviewArticleGroup:
    """Raw-article group that collapses multiple sentence-level FinBERT rows."""

    article_id: str
    date: str
    ticker: str
    headline: str | None
    normalized_headline: str
    source: str | None
    url: str | None
    published_at: str | None
    article_row_count: int
    sentence_count: int
    unique_sentence_count: int
    duplicate_sentence_count: int
    headline_duplicate_count: int
    relevance_score: float | None
    relevance_state: str
    article_status: str
    contamination_flags: tuple[str, ...]
    requested_ticker_terms: tuple[str, ...]
    requested_ticker_term_hits: tuple[str, ...]
    evidence_snippets: tuple[str, ...]
    sentence_rows: list[dict[str, object]]

    def to_dict(self) -> dict[str, object]:
        """Return a JSON-serializable representation."""
        payload = asdict(self)
        payload["contamination_flags"] = list(self.contamination_flags)
        payload["requested_ticker_terms"] = list(self.requested_ticker_terms)
        payload["requested_ticker_term_hits"] = list(self.requested_ticker_term_hits)
        payload["evidence_snippets"] = list(self.evidence_snippets)
        return payload


@dataclass(frozen=True)
class SemanticReviewRegimeRow:
    """Date-level HMM regime evidence for the review dashboard."""

    date: str
    regime: str | None
    confidence: float | None
    prob_bear: float | None
    prob_sideways: float | None
    prob_bull: float | None

    def to_dict(self) -> dict[str, object]:
        """Return a JSON-serializable representation."""
        payload = asdict(self)
        payload["scope"] = "date-level"
        payload["applies_to"] = "all sentence rows on the trading date"
        return payload


@dataclass(frozen=True)
class SemanticReviewDateGroup:
    """One trading-date bucket that contains the date-level regime and article cards."""

    date: str
    regime: dict[str, object] | None
    article_count: int
    accepted_article_count: int
    flagged_article_count: int
    sentence_count: int
    articles: list[dict[str, object]]
    accepted_articles: list[dict[str, object]]
    flagged_articles: list[dict[str, object]]

    def to_dict(self) -> dict[str, object]:
        """Return a JSON-serializable representation."""
        return asdict(self)


@dataclass(frozen=True)
class Layer1SemanticReviewReport:
    """Read-only payload used by the semantic-review dashboard and API."""

    run_id: str
    ticker: str
    from_date: str
    to_date: str
    generated_at: str
    row_count: int
    article_count: int
    date_count: int
    accepted_article_count: int
    flagged_article_count: int
    duplicate_article_count: int
    repeated_headline_count: int
    weak_article_count: int
    sentence_count: int
    load_warnings: list[dict[str, object]]
    regime_rows: list[dict[str, object]]
    article_groups: list[dict[str, object]]
    date_groups: list[dict[str, object]]
    summary: dict[str, int]

    def to_dict(self) -> dict[str, object]:
        """Return a JSON-serializable representation."""
        return asdict(self)


def build_layer1_aapl_evidence_report(
    *,
    run_id: str,
    from_date: str,
    to_date: str,
    ticker: str = "AAPL",
    writer: R2Writer | None = None,
    relevance_threshold: float = DEFAULT_RELEVANCE_THRESHOLD,
) -> Layer1SemanticReviewReport:
    """Build the semantic-review evidence report for a Layer 1 run."""
    start_date = _parse_date(from_date)
    end_date = _parse_date(to_date)
    if start_date > end_date:
        raise ValueError("from_date must be on or before to_date")
    requested_ticker = ticker.strip().upper()
    if not requested_ticker:
        raise ValueError("ticker must be non-empty")

    active_writer = writer or R2Writer()
    scored_frames: list[pd.DataFrame] = []
    load_warnings: list[dict[str, object]] = []
    for current_date in _inclusive_date_range(start_date, end_date):
        key = layer1_sentiment_score_path(current_date, run_id)
        try:
            scored_frame = _read_parquet_frame(active_writer.get_object(key))
        except FileNotFoundError:
            load_warnings.append(
                {
                    "scope": "sentence_rows",
                    "date": current_date,
                    "key": key,
                    "message": "Missing scored-news parquet for this trading date.",
                }
            )
            continue
        if scored_frame.empty:
            load_warnings.append(
                {
                    "scope": "sentence_rows",
                    "date": current_date,
                    "key": key,
                    "message": "Scored-news parquet is empty.",
                }
            )
            continue
        scored_frames.append(scored_frame)

    try:
        regime_frame = _read_parquet_frame(active_writer.get_object(layer1_regime_path(run_id)))
    except FileNotFoundError:
        regime_frame = pd.DataFrame()
        load_warnings.append(
            {
                "scope": "regime",
                "key": layer1_regime_path(run_id),
                "message": "Missing date-level HMM regime parquet for this run.",
            }
        )

    if scored_frames:
        scored_frame = pd.concat(scored_frames, ignore_index=True)
    else:
        scored_frame = pd.DataFrame()

    article_groups, article_summary = _build_article_groups(
        scored_frame,
        requested_ticker=requested_ticker,
        relevance_threshold=relevance_threshold,
    )
    regime_by_date = _build_regime_map(regime_frame)
    date_groups = _build_date_groups(article_groups, regime_by_date)
    summary = {
        "row_count": int(article_summary["row_count"]),
        "article_count": int(article_summary["article_count"]),
        "date_count": len(date_groups),
        "accepted_article_count": int(article_summary["accepted_article_count"]),
        "flagged_article_count": int(article_summary["flagged_article_count"]),
        "duplicate_article_count": int(article_summary["duplicate_article_count"]),
        "repeated_headline_count": int(article_summary["repeated_headline_count"]),
        "weak_article_count": int(article_summary["weak_article_count"]),
        "sentence_count": int(article_summary["sentence_count"]),
    }
    generated_at = datetime.now(tz=UTC).isoformat()
    return Layer1SemanticReviewReport(
        run_id=run_id,
        ticker=requested_ticker,
        from_date=_format_date(start_date),
        to_date=_format_date(end_date),
        generated_at=generated_at,
        row_count=summary["row_count"],
        article_count=summary["article_count"],
        date_count=summary["date_count"],
        accepted_article_count=summary["accepted_article_count"],
        flagged_article_count=summary["flagged_article_count"],
        duplicate_article_count=summary["duplicate_article_count"],
        repeated_headline_count=summary["repeated_headline_count"],
        weak_article_count=summary["weak_article_count"],
        sentence_count=summary["sentence_count"],
        load_warnings=load_warnings,
        regime_rows=[item.to_dict() for item in _regime_rows_from_map(regime_by_date)],
        article_groups=[group.to_dict() for group in article_groups],
        date_groups=[group.to_dict() for group in date_groups],
        summary=summary,
    )


def _build_article_groups(
    frame: pd.DataFrame,
    *,
    requested_ticker: str,
    relevance_threshold: float,
) -> tuple[list[SemanticReviewArticleGroup], dict[str, int]]:
    if frame.empty:
        return [], {
            "row_count": 0,
            "article_count": 0,
            "accepted_article_count": 0,
            "flagged_article_count": 0,
            "duplicate_article_count": 0,
            "repeated_headline_count": 0,
            "weak_article_count": 0,
            "sentence_count": 0,
        }

    normalized = _normalize_scored_frame(frame)
    normalized["normalized_headline"] = normalized["headline"].map(_normalize_headline)
    normalized["sentence_index_key"] = normalized["sentence_index"].where(
        normalized["sentence_index"].notna(),
        -1,
    )
    normalized["requested_ticker_terms"] = normalized["headline"].map(
        lambda value: _ticker_terms(requested_ticker)
    )
    normalized["requested_ticker_term_hits"] = normalized.apply(
        lambda row: tuple(
            term
            for term in row["requested_ticker_terms"]
            if _text_contains_term(_combined_text(row), term)
        ),
        axis=1,
    )
    normalized["evidence_snippets"] = normalized.apply(
        lambda row: tuple(_evidence_snippets(_combined_text(row), row["requested_ticker_terms"])),
        axis=1,
    )
    normalized["has_requested_ticker_evidence"] = normalized["requested_ticker_term_hits"].map(bool)
    normalized["relevance_state"] = normalized["relevance_score"].map(
        lambda value: _relevance_state(value, threshold=relevance_threshold)
    )

    headline_counts = normalized.groupby("normalized_headline")["article_id"].nunique()
    article_groups: list[SemanticReviewArticleGroup] = []
    accepted_count = 0
    flagged_count = 0
    weak_count = 0
    duplicate_article_count = 0
    repeated_headline_count = 0
    sentence_count = int(len(normalized))

    for article_key, article_frame in normalized.groupby(["date", "article_id"], sort=True):
        date_text, article_id = article_key
        article_frame = article_frame.sort_values(["sentence_index_key", "sentence_index"])
        headline = _first_non_null(article_frame["headline"])
        normalized_headline = _normalize_headline(headline)
        headline_duplicate_count = int(headline_counts.get(normalized_headline, 1))
        row_count = int(len(article_frame))
        unique_sentence_count = int(article_frame["sentence_index_key"].nunique())
        duplicate_sentence_count = max(row_count - unique_sentence_count, 0)
        duplicate_article_count += 1 if row_count > 1 else 0
        repeated_headline_count += 1 if headline_duplicate_count > 1 else 0

        requested_ticker_term_hits = tuple(
            sorted(
                {term for hits in article_frame["requested_ticker_term_hits"] for term in hits}
            )
        )
        evidence_snippets = tuple(
            _dedupe_preserve_order(
                snippet
                for snippets in article_frame["evidence_snippets"]
                for snippet in snippets
            )
        )
        relevance_score = _first_non_null_float(article_frame["relevance_score"])
        ticker_field = str(_first_non_null(article_frame["ticker"]))
        contamination_flags: list[str] = []
        if ticker_field and ticker_field != requested_ticker:
            contamination_flags.append("ticker_mismatch")
        if not requested_ticker_term_hits:
            contamination_flags.append("no_requested_ticker_evidence")
        if relevance_score is not None and relevance_score < relevance_threshold:
            contamination_flags.append("low_relevance_score")
        elif relevance_score is None:
            contamination_flags.append("missing_relevance_score")
        if headline_duplicate_count > 1:
            contamination_flags.append("duplicate_normalized_headline")
        if duplicate_sentence_count > 0:
            contamination_flags.append("duplicate_sentence_rows")
        article_status = "accepted" if not contamination_flags else "flagged"
        if article_status == "accepted":
            accepted_count += 1
        else:
            flagged_count += 1
        if "low_relevance_score" in contamination_flags or "missing_relevance_score" in contamination_flags:
            weak_count += 1

        sentence_rows = [
            SemanticReviewSentenceRow(
                sentence_index=_maybe_int(row["sentence_index"]),
                text=_optional_str(row["text"]),
                sentiment_score=_maybe_float(row["sentiment_score"]),
                positive_probability=_maybe_float(row["positive_probability"]),
                negative_probability=_maybe_float(row["negative_probability"]),
                neutral_probability=_maybe_float(row["neutral_probability"]),
                relevance_score=_maybe_float(row["relevance_score"]),
                row_granularity="sentence-level",
            ).to_dict()
            for _, row in article_frame.iterrows()
        ]
        article_groups.append(
            SemanticReviewArticleGroup(
                article_id=str(article_id),
                date=str(date_text),
                ticker=ticker_field or requested_ticker,
                headline=_optional_str(headline),
                normalized_headline=normalized_headline,
                source=_optional_str(_first_non_null(article_frame["source"])),
                url=_optional_str(_first_non_null(article_frame["url"])),
                published_at=_optional_str(_first_non_null(article_frame["published_at"])),
                article_row_count=row_count,
                sentence_count=row_count,
                unique_sentence_count=unique_sentence_count,
                duplicate_sentence_count=duplicate_sentence_count,
                headline_duplicate_count=headline_duplicate_count,
                relevance_score=relevance_score,
                relevance_state=_relevance_state(relevance_score, threshold=relevance_threshold),
                article_status=article_status,
                contamination_flags=tuple(contamination_flags),
                requested_ticker_terms=_ticker_terms(requested_ticker),
                requested_ticker_term_hits=requested_ticker_term_hits,
                evidence_snippets=evidence_snippets,
                sentence_rows=sentence_rows,
            )
        )

    summary = {
        "row_count": sentence_count,
        "article_count": len(article_groups),
        "accepted_article_count": accepted_count,
        "flagged_article_count": flagged_count,
        "duplicate_article_count": duplicate_article_count,
        "repeated_headline_count": repeated_headline_count,
        "weak_article_count": weak_count,
        "sentence_count": sentence_count,
    }
    return article_groups, summary


def _build_regime_map(frame: pd.DataFrame) -> dict[str, SemanticReviewRegimeRow]:
    if frame.empty:
        return {}
    normalized = frame.copy()
    normalized.columns = [str(column) for column in normalized.columns]
    date_column = _first_existing_column(normalized, ("date", "as_of_date"))
    if date_column is None:
        return {}
    regime_column = _first_existing_column(normalized, ("regime", "state", "label"))
    confidence_column = _first_existing_column(normalized, ("confidence", "regime_confidence"))
    bear_column = _first_existing_column(normalized, ("prob_bear", "bear_prob", "probability_bear"))
    sideways_column = _first_existing_column(
        normalized,
        ("prob_sideways", "sideways_prob", "probability_sideways"),
    )
    bull_column = _first_existing_column(normalized, ("prob_bull", "bull_prob", "probability_bull"))
    regime_map: dict[str, SemanticReviewRegimeRow] = {}
    for _, row in normalized.iterrows():
        date_text = _optional_str(row.get(date_column))
        if not date_text:
            continue
        regime_map[date_text] = SemanticReviewRegimeRow(
            date=date_text,
            regime=_optional_str(row.get(regime_column)) if regime_column else None,
            confidence=_maybe_float(row.get(confidence_column)) if confidence_column else None,
            prob_bear=_maybe_float(row.get(bear_column)) if bear_column else None,
            prob_sideways=_maybe_float(row.get(sideways_column)) if sideways_column else None,
            prob_bull=_maybe_float(row.get(bull_column)) if bull_column else None,
        )
    return regime_map


def _regime_rows_from_map(
    regime_map: Mapping[str, SemanticReviewRegimeRow],
) -> list[SemanticReviewRegimeRow]:
    return [regime_map[key] for key in sorted(regime_map)]


def _build_date_groups(
    article_groups: Sequence[SemanticReviewArticleGroup],
    regime_map: Mapping[str, SemanticReviewRegimeRow],
) -> list[SemanticReviewDateGroup]:
    grouped: dict[str, list[SemanticReviewArticleGroup]] = defaultdict(list)
    for article_group in article_groups:
        grouped[article_group.date].append(article_group)

    date_groups: list[SemanticReviewDateGroup] = []
    for date_text in sorted(grouped):
        articles = sorted(grouped[date_text], key=lambda item: (item.article_status, item.article_id))
        accepted_articles = [item.to_dict() for item in articles if item.article_status == "accepted"]
        flagged_articles = [item.to_dict() for item in articles if item.article_status != "accepted"]
        date_groups.append(
            SemanticReviewDateGroup(
                date=date_text,
                regime=regime_map.get(date_text).to_dict() if date_text in regime_map else None,
                article_count=len(articles),
                accepted_article_count=len(accepted_articles),
                flagged_article_count=len(flagged_articles),
                sentence_count=sum(item.article_row_count for item in articles),
                articles=[item.to_dict() for item in articles],
                accepted_articles=accepted_articles,
                flagged_articles=flagged_articles,
            )
        )
    return date_groups


def _normalize_scored_frame(frame: pd.DataFrame) -> pd.DataFrame:
    normalized = frame.copy()
    normalized.columns = [str(column) for column in normalized.columns]
    for column in (
        "date",
        "ticker",
        "headline",
        "text",
        "article_id",
        "source",
        "url",
        "published_at",
    ):
        if column not in normalized.columns:
            normalized[column] = None
    for column in (
        "sentence_index",
        "sentiment_score",
        "positive_probability",
        "negative_probability",
        "neutral_probability",
        "relevance_score",
    ):
        if column not in normalized.columns:
            normalized[column] = None
    normalized["date"] = normalized["date"].map(lambda value: _optional_str(value) or "")
    normalized["ticker"] = normalized["ticker"].map(lambda value: _optional_str(value) or "")
    normalized["headline"] = normalized["headline"].map(_optional_str)
    normalized["text"] = normalized["text"].map(_optional_str)
    normalized["article_id"] = normalized["article_id"].map(lambda value: _optional_str(value) or _fallback_article_id(value))
    normalized["source"] = normalized["source"].map(_optional_str)
    normalized["url"] = normalized["url"].map(_optional_str)
    normalized["published_at"] = normalized["published_at"].map(_optional_str)
    for column in (
        "sentence_index",
        "sentiment_score",
        "positive_probability",
        "negative_probability",
        "neutral_probability",
        "relevance_score",
    ):
        normalized[column] = normalized[column].map(_maybe_float)
    normalized = normalized[normalized["date"].astype(bool)]
    return normalized


def _read_parquet_frame(data: bytes) -> pd.DataFrame:
    """Load a parquet payload into a DataFrame."""
    return pd.read_parquet(BytesIO(data))


def _build_payload_from_report(report: Layer1SemanticReviewReport | Mapping[str, object]) -> dict[str, object]:
    """Normalize a report into the semantic-review dashboard payload shape."""
    report_dict = report.to_dict() if isinstance(report, Layer1SemanticReviewReport) else dict(report)
    date_groups = [dict(item) for item in report_dict.get("date_groups", [])]
    article_groups = [dict(item) for item in report_dict.get("article_groups", [])]
    flagged_articles = [
        dict(item)
        for item in article_groups
        if str(item.get("article_status", "flagged")) != "accepted"
    ]
    accepted_articles = [
        dict(item)
        for item in article_groups
        if str(item.get("article_status", "flagged")) == "accepted"
    ]
    payload = {
        "title": "Layer 1 semantic review dashboard",
        "description": (
            "Sentence-level FinBERT rows are grouped under raw-article cards. "
            "HMM regime is date-level and is rendered once per trading date."
        ),
        "report": report_dict,
        "summary": dict(report_dict.get("summary", {})),
        "controls": {
            "ticker": report_dict.get("ticker"),
            "run_id": report_dict.get("run_id"),
            "from_date": report_dict.get("from_date"),
            "to_date": report_dict.get("to_date"),
        },
        "date_groups": date_groups,
        "article_groups": article_groups,
        "accepted_articles": accepted_articles,
        "flagged_articles": flagged_articles,
        "warnings": list(report_dict.get("load_warnings", [])),
    }
    return payload


__all__ = [
    "DEFAULT_RELEVANCE_THRESHOLD",
    "Layer1SemanticReviewReport",
    "SemanticReviewArticleGroup",
    "SemanticReviewDateGroup",
    "SemanticReviewRegimeRow",
    "SemanticReviewSentenceRow",
    "build_layer1_aapl_evidence_report",
    "_build_payload_from_report",
]


def _parse_date(value: str) -> Date:
    """Parse a YYYY-MM-DD date string."""
    return Date.fromisoformat(value.strip())


def _inclusive_date_range(start: Date, end: Date) -> list[str]:
    """Return all YYYY-MM-DD values between two dates, inclusive."""
    current = start
    values: list[str] = []
    while current <= end:
        values.append(current.isoformat())
        current = current.fromordinal(current.toordinal() + 1)
    return values


def _format_date(value: Date) -> str:
    """Format a date for JSON payloads."""
    return value.isoformat()


def _maybe_float(value: Any) -> float | None:
    """Return a float when the input is numeric and not missing."""
    if value is None:
        return None
    if isinstance(value, float) and pd.isna(value):
        return None
    try:
        if pd.isna(value):
            return None
    except TypeError:
        pass
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _maybe_int(value: Any) -> int | None:
    """Return an int when the input is numeric and not missing."""
    maybe_value = _maybe_float(value)
    if maybe_value is None:
        return None
    return int(maybe_value)


def _optional_str(value: Any) -> str | None:
    """Return a stripped string when the input is present."""
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except TypeError:
        pass
    text = str(value).strip()
    return text or None


def _first_non_null(values: Sequence[Any]) -> Any:
    """Return the first non-null value from a sequence."""
    for value in values:
        if _optional_str(value) is not None:
            return value
    return None


def _first_non_null_float(values: Sequence[Any]) -> float | None:
    """Return the first non-null float from a sequence."""
    for value in values:
        maybe_value = _maybe_float(value)
        if maybe_value is not None:
            return maybe_value
    return None


def _fallback_article_id(value: Any) -> str:
    """Build a deterministic fallback article identifier when one is missing."""
    return f"article-{abs(hash(str(value)))}"


def _normalize_headline(value: str | None) -> str:
    """Normalize a headline for duplicate detection."""
    if not value:
        return ""
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9]+", " ", value.lower())).strip()


def _ticker_terms(ticker: str) -> tuple[str, ...]:
    """Return ticker and alias terms used for source-text evidence checks."""
    normalized = ticker.strip().upper()
    aliases = {
        "AAPL": ("apple", "apple inc", "aapl"),
    }
    terms = [normalized.lower()]
    for alias in aliases.get(normalized, ()): 
        if alias.lower() not in terms:
            terms.append(alias.lower())
    return tuple(terms)


def _text_contains_term(text: str, term: str) -> bool:
    """Return True when a normalized term appears in text."""
    if not text or not term:
        return False
    if " " in term:
        return term in text.lower()
    return re.search(rf"\b{re.escape(term.lower())}\b", text.lower()) is not None


def _evidence_snippets(text: str, terms: Sequence[str]) -> list[str]:
    """Return short source-text snippets that justify a ticker relevance decision."""
    if not text:
        return []
    sentences = re.split(r"(?<=[.!?])\s+", text)
    snippets: list[str] = []
    for sentence in sentences:
        if any(_text_contains_term(sentence, term) for term in terms):
            snippets.append(sentence.strip())
    return snippets


def _combined_text(row: pd.Series) -> str:
    """Return headline + text for evidence searches."""
    headline = _optional_str(row.get("headline")) or ""
    text = _optional_str(row.get("text")) or ""
    return f"{headline} {text}".strip()


def _relevance_state(value: float | None, *, threshold: float) -> str:
    """Classify a relevance score for dashboard badges."""
    if value is None:
        return "missing"
    if value < threshold:
        return "weak"
    return "strong"


def _first_existing_column(frame: pd.DataFrame, candidates: Sequence[str]) -> str | None:
    """Return the first matching column name from a candidate list."""
    for candidate in candidates:
        if candidate in frame.columns:
            return candidate
    return None


def _dedupe_preserve_order(items: Sequence[str]) -> list[str]:
    """Deduplicate a sequence while preserving first-seen order."""
    seen: set[str] = set()
    values: list[str] = []
    for item in items:
        if item in seen:
            continue
        seen.add(item)
        values.append(item)
    return values


def _build_payload_from_report_and_json(report_json: str) -> dict[str, object]:
    """Convenience helper for callers that already hold a JSON report string."""
    return _build_payload_from_report(json.loads(report_json))
