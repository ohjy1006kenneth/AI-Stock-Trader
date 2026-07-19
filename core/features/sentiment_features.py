"""Layer 1 sentiment aggregation from scored FinBERT news rows."""
from __future__ import annotations

import importlib
import json
import math
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from core.contracts.schemas import FeatureRecord, NewsSentimentRecord
from core.features.news_assignment_provenance import (
    NEWS_EVIDENCE_RELEVANCE_WEIGHTS,
    NewsEvidenceClass,
)
from services.wikipedia.sp500_universe import canonicalize_ticker

if TYPE_CHECKING:
    import pandas as pd

DEFAULT_SOURCE_CREDIBILITY_CONFIG_PATH = (
    Path(__file__).resolve().parents[2] / "config" / "source_credibility.json"
)

SENTIMENT_AGGREGATE_COLUMNS: tuple[str, ...] = (
    "date",
    "ticker",
    "headline",
    "source",
    "published_at",
    "sentiment_positive",
    "sentiment_negative",
    "sentiment_neutral",
    "sentiment_score",
    "relevance_score",
)

REQUIRED_SENTIMENT_COLUMNS: frozenset[str] = frozenset(
    {
        "date",
        "ticker",
        "source",
        "sentiment_positive",
        "sentiment_negative",
        "sentiment_neutral",
        "sentiment_score",
        "relevance_score",
    }
)

TOPIC_EVIDENCE_COLUMNS: frozenset[str] = frozenset(
    {
        "date",
        "ticker",
        "article_id",
        "topic_id",
        "topic_probability",
    }
)

RELEVANCE_EVIDENCE_COLUMNS: frozenset[str] = frozenset(
    {
        "date",
        "ticker",
        "article_id",
        "relevance_decision",
        "relevance_score",
        "ticker_relevance_score",
        "financial_relevance_score",
        "topic_relevance_score",
    }
)

RELEVANCE_EVIDENCE_OPTIONAL_COLUMNS: tuple[str, ...] = (
    "relevance_category",
    "target_context_score",
    "document_sentiment",
    "target_company_impact_direction",
    "target_company_impact_magnitude",
    "impact_horizon",
    "causal_channel",
    "target_impact_confidence",
    "article_contamination_ratio",
    "article_contamination_count",
    "article_signal_count",
    "article_contribution_weight",
)

SENTIMENT_FEATURE_COLUMNS: tuple[str, ...] = (
    "nlp_sentiment_positive",
    "nlp_sentiment_negative",
    "nlp_sentiment_neutral",
    "nlp_sentiment_score",
    "nlp_sentiment_strength",
    "nlp_sentiment_std",
    "nlp_article_count",
    "nlp_sentence_count",
    "nlp_relevance_score",
    "nlp_source_weight_mean",
    "nlp_source_weight_sum",
    "nlp_effective_weight_sum",
    "nlp_source_count",
    "nlp_dominant_source",
    "nlp_dominant_source_article_count",
    "nlp_dominant_source_weight_share",
    "nlp_effective_source_count",
    "nlp_missing_source_count",
    "nlp_sentiment_topic_score",
    "nlp_sentiment_topic_count",
    "nlp_sentiment_dominant_topic_id",
    "nlp_sentiment_dominant_topic_score",
    "nlp_sentiment_dominant_topic_probability",
    "nlp_relevance_accepted_count",
    "nlp_relevance_borderline_count",
    "nlp_missing_topic_count",
    "nlp_missing_relevance_evidence_count",
    "nlp_contributing_article_ids",
    "nlp_topic_sentiment_summary",
    "nlp_source_weight_summary",
    "nlp_relevance_reason_codes",
    "nlp_semantic_warning_codes",
    "nlp_relevance_category",
    "nlp_target_impact_direction",
    "nlp_target_impact_magnitude",
    "nlp_target_impact_horizon",
    "nlp_target_impact_confidence",
    "nlp_causal_channel",
    "nlp_document_sentiment",
    "nlp_article_contamination_ratio",
    "nlp_article_contamination_count",
    "nlp_article_signal_count",
    "nlp_article_contribution_weight",
    "nlp_article_contribution_weight_mean",
    "nlp_article_contribution_weight_min",
)

DIRECT_RELEVANCE_SCORE = 1.0
INDIRECT_RELEVANCE_SCORE = 0.25
BROAD_MARKET_RELEVANCE_SCORE = 0.0
CONTAMINATION_RELEVANCE_SCORE = 0.0

_CONTEXTUAL_EVIDENCE_TERMS: tuple[str, ...] = (
    "ai",
    "analyst",
    "broad market",
    "chip",
    "competitor",
    "etf",
    "equities",
    "guidance",
    "index",
    "investor",
    "market",
    "peer",
    "rally",
    "rival",
    "sector",
    "semiconductor",
    "stocks",
    "tech",
    "technology",
)

_TICKER_ENTITY_ALIASES: dict[str, tuple[str, ...]] = {
    "AAPL": ("apple", "apple inc", "apple inc.", "apple computer"),
    "AMD": ("amd", "advanced micro devices"),
    "AMZN": ("amazon", "amazon.com", "amazon inc"),
    "BRK-B": ("berkshire hathaway", "berkshire"),
    "GOOGL": ("alphabet", "google", "google parent"),
    "INTC": ("intel", "intel corp", "intel corporation"),
    "META": ("meta", "facebook"),
    "MSFT": ("microsoft", "microsoft corp", "microsoft corporation"),
    "NVDA": ("nvidia", "nvidia corp", "nvidia corporation"),
    "QQQ": ("nasdaq 100", "invesco qqq", "qqq etf"),
    "SNAP": ("snap", "snap inc", "snapchat"),
    "SPY": ("spdr s&p 500", "s&p 500 etf", "spy etf"),
    "TSLA": ("tesla", "tesla inc"),
}


@dataclass(frozen=True)
class SourceCredibilityConfig:
    """Configurable source credibility weights for sentiment aggregation."""

    default_source_weight: float
    source_weights: Mapping[str, float]


@dataclass(frozen=True)
class SentimentScore:
    """FinBERT class probabilities for one text chunk."""

    positive: float
    negative: float
    neutral: float

    def __post_init__(self) -> None:
        """Validate model probabilities."""
        for label, value in (
            ("positive", self.positive),
            ("negative", self.negative),
            ("neutral", self.neutral),
        ):
            numeric = _to_float_or_none(value)
            if numeric is None or numeric < 0.0 or numeric > 1.0:
                raise ValueError(f"{label} must be a probability in [0, 1]")


class SentimentScorer(Protocol):
    """Text sentiment model used by the FinBERT scoring pipeline."""

    def score(self, texts: Sequence[str]) -> Sequence[SentimentScore]:
        """Return sentiment probabilities for each input text."""


def load_source_credibility_config(
    path: Path = DEFAULT_SOURCE_CREDIBILITY_CONFIG_PATH,
) -> SourceCredibilityConfig:
    """Load source credibility weights from the repository config file."""
    payload = json.loads(path.read_text())
    return SourceCredibilityConfig(
        default_source_weight=_validate_weight(
            payload.get("default_source_weight"),
            label="default_source_weight",
        ),
        source_weights=_normalize_source_weights(payload.get("source_weights", {})),
    )


def score_news_sentiment(
    records: Sequence[NewsSentimentRecord],
    *,
    scorer: SentimentScorer,
    batch_size: int = 32,
    default_relevance_score: float | None = None,
) -> list[NewsSentimentRecord]:
    """Score preprocessed news rows with an injected FinBERT-compatible scorer."""
    if batch_size <= 0:
        raise ValueError("batch_size must be positive")
    if default_relevance_score is None:
        relevance = None
    else:
        relevance = _to_float_or_none(default_relevance_score)
        if relevance is None or relevance < 0.0:
            raise ValueError("default_relevance_score must be a non-negative finite number")

    scorable_records: list[NewsSentimentRecord] = []
    texts: list[str] = []
    for record in records:
        text = _scoring_text(record)
        if text is None:
            continue
        scorable_records.append(record)
        texts.append(text)

    scores: list[SentimentScore] = []
    for start in range(0, len(texts), batch_size):
        batch = texts[start : start + batch_size]
        batch_scores = list(scorer.score(batch))
        if len(batch_scores) != len(batch):
            raise ValueError("sentiment scorer returned the wrong number of scores")
        scores.extend(batch_scores)

    scored_records: list[NewsSentimentRecord] = []
    for record, score in zip(scorable_records, scores, strict=True):
        active_relevance = _resolve_relevance_score(
            record,
            default_relevance_score=relevance,
        )
        scored_records.append(
            NewsSentimentRecord(
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
                sentiment_positive=score.positive,
                sentiment_negative=score.negative,
                sentiment_neutral=score.neutral,
                sentiment_score=score.positive - score.negative,
                relevance_score=active_relevance,
            )
        )
    return scored_records


def aggregate_sentiment_by_ticker_day(
    scored_news: pd.DataFrame,
    *,
    credibility_config: SourceCredibilityConfig | None = None,
    bucket_timezone: str = "America/New_York",
) -> pd.DataFrame:
    """Return source-weighted ticker-day sentiment aggregates.

    Args:
        scored_news: Article-level rows with FinBERT probabilities and a
            relevance score. Rows must contain the `NewsSentimentRecord`
            sentiment fields plus `source`.
        credibility_config: Optional source-weight mapping. When omitted, the
            repository default config is loaded from `config/source_credibility.json`.

    Returns:
        DataFrame with columns matching `NewsSentimentRecord`. One row is
        emitted per `(date, ticker)` group. The sentiment fields are weighted by
        source credibility and article relevance.
    """
    config = _normalized_config(credibility_config or load_source_credibility_config())
    pd, frame = _prepare_scored_news_frame(
        scored_news,
        config=config,
        bucket_timezone=bucket_timezone,
    )

    if len(frame) == 0:
        return _empty_frame(pd)

    rows: list[dict[str, Any]] = []
    for (date_value, ticker), group in frame.groupby(["date", "ticker"], sort=True, dropna=False):
        if pd.isna(ticker):
            raise ValueError("sentiment rows must contain ticker values")
        rows.append(
            {
                "date": str(date_value),
                "ticker": str(ticker),
                "headline": None,
                "source": None,
                "published_at": None,
                "sentiment_positive": _weighted_average(
                    group["sentiment_positive"], group["_effective_weight"]
                ),
                "sentiment_negative": _weighted_average(
                    group["sentiment_negative"], group["_effective_weight"]
                ),
                "sentiment_neutral": _weighted_average(
                    group["sentiment_neutral"], group["_effective_weight"]
                ),
                "sentiment_score": _weighted_average(
                    group["sentiment_score"], group["_effective_weight"]
                ),
                "relevance_score": _weighted_average(
                    group["relevance_score"], group["_source_weight"]
                ),
            }
        )

    return pd.DataFrame(rows, columns=list(SENTIMENT_AGGREGATE_COLUMNS))


def sentiment_feature_records_from_scored_news(
    scored_news: pd.DataFrame,
    *,
    topic_labels: pd.DataFrame | None = None,
    relevance_gate: pd.DataFrame | None = None,
    credibility_config: SourceCredibilityConfig | None = None,
    bucket_timezone: str = "America/New_York",
) -> list[FeatureRecord]:
    """Aggregate scored news rows into ticker-day semantic sentiment FeatureRecords."""
    config = _normalized_config(credibility_config or load_source_credibility_config())
    pd, frame = _prepare_scored_news_frame(
        scored_news,
        topic_labels=topic_labels,
        relevance_gate=relevance_gate,
        config=config,
        bucket_timezone=bucket_timezone,
    )
    if len(frame) == 0:
        return []

    records: list[FeatureRecord] = []
    for (date_value, ticker), group in frame.groupby(["date", "ticker"], sort=True, dropna=False):
        if pd.isna(ticker):
            raise ValueError("sentiment rows must contain ticker values")

        strength_values = group[["sentiment_positive", "sentiment_negative"]].max(axis=1)
        sentiment_std = group["sentiment_score"].astype(float).std(ddof=0)
        topic_summary = _topic_sentiment_summary(group)
        dominant_topic = topic_summary[0] if topic_summary else {}
        source_concentration = _source_concentration(group)
        records.append(
            FeatureRecord(
                date=str(date_value),
                ticker=str(ticker),
                features={
                    "nlp_sentiment_positive": _weighted_average(
                        group["sentiment_positive"], group["_effective_weight"]
                    ),
                    "nlp_sentiment_negative": _weighted_average(
                        group["sentiment_negative"], group["_effective_weight"]
                    ),
                    "nlp_sentiment_neutral": _weighted_average(
                        group["sentiment_neutral"], group["_effective_weight"]
                    ),
                    "nlp_sentiment_score": _weighted_average(
                        group["sentiment_score"], group["_effective_weight"]
                    ),
                    "nlp_sentiment_strength": _weighted_average(
                        strength_values, group["_effective_weight"]
                    ),
                    "nlp_sentiment_std": 0.0 if pd.isna(sentiment_std) else float(sentiment_std),
                    "nlp_article_count": _article_count(group),
                    "nlp_sentence_count": int(len(group)),
                    "nlp_relevance_score": _weighted_average(
                        group["relevance_score"], group["_source_weight"]
                    ),
                    "nlp_source_weight_mean": _mean(group["_source_weight"]),
                    "nlp_source_weight_sum": _sum_positive(group["_source_weight"]),
                    "nlp_effective_weight_sum": _sum_positive(group["_effective_weight"]),
                    "nlp_source_count": source_concentration["source_count"],
                    "nlp_dominant_source": source_concentration["dominant_source"],
                    "nlp_dominant_source_article_count": source_concentration[
                        "dominant_source_article_count"
                    ],
                    "nlp_dominant_source_weight_share": source_concentration[
                        "dominant_source_weight_share"
                    ],
                    "nlp_effective_source_count": source_concentration[
                        "effective_source_count"
                    ],
                    "nlp_missing_source_count": _missing_source_count(group),
                    "nlp_sentiment_topic_score": _weighted_average(
                        group["sentiment_score"], group["_topic_effective_weight"]
                    ),
                    "nlp_sentiment_topic_count": _topic_count(group),
                    "nlp_sentiment_dominant_topic_id": dominant_topic.get("topic_id"),
                    "nlp_sentiment_dominant_topic_score": dominant_topic.get(
                        "sentiment_score"
                    ),
                    "nlp_sentiment_dominant_topic_probability": dominant_topic.get(
                        "mean_topic_probability"
                    ),
                    "nlp_relevance_accepted_count": _decision_count(group, "accepted"),
                    "nlp_relevance_borderline_count": _decision_count(group, "borderline"),
                    "nlp_missing_topic_count": _missing_topic_count(group),
                    "nlp_missing_relevance_evidence_count": _missing_relevance_count(group),
                    "nlp_contributing_article_ids": json.dumps(
                        _contributing_article_ids(group),
                        sort_keys=True,
                        separators=(",", ":"),
                    ),
                    "nlp_topic_sentiment_summary": json.dumps(
                        topic_summary,
                        sort_keys=True,
                        separators=(",", ":"),
                    ),
                    "nlp_source_weight_summary": json.dumps(
                        _source_weight_summary(group),
                        sort_keys=True,
                        separators=(",", ":"),
                    ),
                    "nlp_relevance_reason_codes": json.dumps(
                        _relevance_reason_codes(group),
                        sort_keys=True,
                        separators=(",", ":"),
                    ),
                    "nlp_semantic_warning_codes": json.dumps(
                        _semantic_warning_codes(group),
                        sort_keys=True,
                        separators=(",", ":"),
                    ),
                    "nlp_relevance_category": _first_non_null(group["_relevance_category"]),
                    "nlp_target_impact_direction": _first_non_null(
                        group["_target_company_impact_direction"]
                    ),
                    "nlp_target_impact_magnitude": _first_non_null(
                        group["_target_company_impact_magnitude"]
                    ),
                    "nlp_target_impact_horizon": _first_non_null(group["_impact_horizon"]),
                    "nlp_target_impact_confidence": _first_non_null(
                        group["_target_impact_confidence"]
                    ),
                    "nlp_causal_channel": _first_non_null(group["_causal_channel"]),
                    "nlp_document_sentiment": _first_non_null(group["_document_sentiment"]),
                    "nlp_article_contamination_ratio": _first_non_null(
                        group["_article_contamination_ratio"]
                    ),
                    "nlp_article_contamination_count": _first_non_null(
                        group["_article_contamination_count"]
                    ),
                    "nlp_article_signal_count": _first_non_null(group["_article_signal_count"]),
                    "nlp_article_contribution_weight": _first_non_null(
                        group["_article_contribution_weight"]
                    ),
                    "nlp_article_contribution_weight_mean": _mean(
                        group["_article_contribution_weight"]
                    ),
                    "nlp_article_contribution_weight_min": _min_finite(
                        group["_article_contribution_weight"]
                    ),
                },
            )
        )
    return records


def sentiment_feature_records_to_frame(records: Sequence[FeatureRecord]) -> pd.DataFrame:
    """Serialize sentiment FeatureRecords into a Parquet-ready DataFrame."""
    pd = _require_pandas()
    rows = [
        {
            "date": record.date,
            "ticker": record.ticker,
            "features": json.dumps(record.features, sort_keys=True, separators=(",", ":")),
        }
        for record in records
    ]
    return pd.DataFrame(rows, columns=["date", "ticker", "features"])


def sentiment_aggregates_to_records(aggregates: pd.DataFrame) -> list[NewsSentimentRecord]:
    """Convert sentiment aggregate rows into `NewsSentimentRecord` instances."""
    _validate_columns(aggregates, required=frozenset(SENTIMENT_AGGREGATE_COLUMNS))

    records: list[NewsSentimentRecord] = []
    for row in aggregates.to_dict(orient="records"):
        records.append(
            NewsSentimentRecord(
                date=str(row["date"]),
                ticker=str(row["ticker"]),
                headline=_normalize_optional_string(row.get("headline")),
                source=_normalize_optional_string(row.get("source")),
                published_at=row.get("published_at") or None,
                sentiment_positive=_normalize_optional_float(row.get("sentiment_positive")),
                sentiment_negative=_normalize_optional_float(row.get("sentiment_negative")),
                sentiment_neutral=_normalize_optional_float(row.get("sentiment_neutral")),
                sentiment_score=_normalize_optional_float(row.get("sentiment_score")),
                relevance_score=_normalize_optional_float(row.get("relevance_score")),
            )
        )
    return records


def _prepare_scored_news_frame(
    scored_news: pd.DataFrame,
    *,
    topic_labels: pd.DataFrame | None = None,
    relevance_gate: pd.DataFrame | None = None,
    config: SourceCredibilityConfig,
    bucket_timezone: str,
) -> tuple[Any, pd.DataFrame]:
    """Validate and normalize scored news rows for sentiment aggregation."""
    pd = _require_pandas()
    _validate_columns(scored_news)
    _validate_timezone(bucket_timezone)

    frame = scored_news.copy()
    if len(frame) == 0:
        return pd, frame

    frame["_artifact_date"] = [
        _to_iso_date(row.get("date"))
        for row in frame.to_dict(orient="records")
    ]
    frame["date"] = [
        _bucket_date(
            row.get("published_at"),
            fallback_date=row.get("date"),
            bucket_timezone=bucket_timezone,
        )
        for row in frame.to_dict(orient="records")
    ]
    if frame["date"].isna().any():
        raise ValueError("sentiment rows must contain date-like values")

    for column in (
        "sentiment_positive",
        "sentiment_negative",
        "sentiment_neutral",
        "sentiment_score",
        "relevance_score",
    ):
        frame[column] = frame[column].map(_to_float_or_none)

    required_numeric = (
        "sentiment_positive",
        "sentiment_negative",
        "sentiment_neutral",
        "sentiment_score",
    )
    if frame[list(required_numeric)].isna().any().any():
        raise ValueError("sentiment probability and score columns must be numeric")
    _validate_probability_columns(frame)

    frame["_source_weight"] = frame["source"].map(
        lambda value: _source_weight(value, config=config)
    )
    frame["_relevance_weight"] = frame["relevance_score"].map(_relevance_weight)
    frame["_effective_weight"] = frame["_source_weight"] * frame["_relevance_weight"]
    frame = _attach_topic_evidence(pd, frame, topic_labels)
    frame = _attach_relevance_evidence(pd, frame, relevance_gate)
    frame["_article_cap_weight"] = frame["_article_contribution_weight"].map(
        _article_cap_weight
    )
    frame["_event_relevance_weight"] = [
        min(float(relevance_weight), float(article_cap_weight))
        for relevance_weight, article_cap_weight in zip(
            frame["_relevance_weight"].tolist(),
            frame["_article_cap_weight"].tolist(),
            strict=True,
        )
    ]
    frame["_effective_weight"] = frame["_source_weight"] * frame["_event_relevance_weight"]
    frame["_effective_weight"] = _apply_article_event_caps(frame)
    frame["_topic_effective_weight"] = [
        _topic_effective_weight(row)
        for row in frame.to_dict(orient="records")
    ]
    return pd, frame


def _attach_topic_evidence(
    pd: Any,
    frame: pd.DataFrame,
    topic_labels: pd.DataFrame | None,
) -> pd.DataFrame:
    """Attach article-level topic metadata to scored sentiment rows."""
    frame = frame.copy()
    frame["_topic_id"] = None
    frame["_topic_probability"] = None
    frame["_topic_embedding_cache_key"] = None
    frame["_missing_topic_evidence"] = True
    frame["_missing_topic_artifact"] = topic_labels is None or len(topic_labels) == 0
    if topic_labels is None or len(topic_labels) == 0:
        return frame

    _validate_optional_evidence_columns(
        topic_labels,
        required=TOPIC_EVIDENCE_COLUMNS,
        label="topic_labels",
    )
    topic_frame = topic_labels.copy()
    topic_frame["_artifact_date"] = topic_frame["date"].map(_to_iso_date)
    topic_frame["_join_ticker"] = topic_frame["ticker"].map(_normalize_join_ticker)
    topic_frame["_join_article_id"] = topic_frame["article_id"].map(_normalize_optional_string)
    topic_frame = topic_frame.dropna(
        subset=["_artifact_date", "_join_ticker", "_join_article_id"]
    )
    if len(topic_frame) == 0:
        return frame
    topic_columns = [
        "_artifact_date",
        "_join_ticker",
        "_join_article_id",
        "topic_id",
        "topic_probability",
    ]
    if "embedding_cache_key" in topic_frame.columns:
        topic_columns.append("embedding_cache_key")
    topic_frame = topic_frame.loc[:, topic_columns].drop_duplicates(
        subset=["_artifact_date", "_join_ticker", "_join_article_id"],
        keep="first",
    )
    rename_columns = {
        "topic_id": "_topic_id",
        "topic_probability": "_topic_probability",
        "embedding_cache_key": "_topic_embedding_cache_key",
    }
    topic_frame = topic_frame.rename(columns=rename_columns)
    frame["_join_ticker"] = frame["ticker"].map(_normalize_join_ticker)
    frame["_join_article_id"] = frame.get("article_id", pd.Series(index=frame.index)).map(
        _normalize_optional_string
    )
    frame = frame.drop(
        columns=[
            "_topic_id",
            "_topic_probability",
            "_topic_embedding_cache_key",
        ],
        errors="ignore",
    ).merge(
        topic_frame,
        on=["_artifact_date", "_join_ticker", "_join_article_id"],
        how="left",
    )
    frame["_missing_topic_evidence"] = frame["_topic_id"].map(_to_float_or_none).isna()
    frame["_missing_topic_artifact"] = False
    return frame


def _attach_relevance_evidence(
    pd: Any,
    frame: pd.DataFrame,
    relevance_gate: pd.DataFrame | None,
) -> pd.DataFrame:
    """Attach relevance-gate audit evidence to scored sentiment rows."""
    frame = frame.copy()
    for column in (
        "_relevance_decision",
        "_ticker_relevance_score",
        "_financial_relevance_score",
        "_topic_relevance_score",
        "_relevance_category",
        "_target_context_score",
        "_document_sentiment",
        "_target_company_impact_direction",
        "_target_company_impact_magnitude",
        "_impact_horizon",
        "_causal_channel",
        "_target_impact_confidence",
        "_article_contamination_ratio",
        "_article_contamination_count",
        "_article_signal_count",
        "_article_contribution_weight",
        "_relevance_reason_codes",
    ):
        frame[column] = None
    frame["_missing_relevance_evidence"] = relevance_gate is None or len(relevance_gate) == 0
    if relevance_gate is None or len(relevance_gate) == 0:
        return frame

    _validate_optional_evidence_columns(
        relevance_gate,
        required=RELEVANCE_EVIDENCE_COLUMNS,
        label="relevance_gate",
    )
    gate_frame = relevance_gate.copy()
    gate_frame["_artifact_date"] = gate_frame["date"].map(_to_iso_date)
    gate_frame["_join_ticker"] = gate_frame["ticker"].map(_normalize_join_ticker)
    gate_frame["_join_article_id"] = gate_frame["article_id"].map(_normalize_optional_string)
    gate_frame["_join_sentence_index"] = gate_frame.get(
        "sentence_index",
        pd.Series(index=gate_frame.index),
    ).map(_normalize_optional_int)
    gate_frame["_join_chunk_index"] = gate_frame.get(
        "chunk_index",
        pd.Series(index=gate_frame.index),
    ).map(_normalize_optional_int)
    gate_frame = gate_frame.dropna(
        subset=["_artifact_date", "_join_ticker", "_join_article_id"]
    )
    if len(gate_frame) == 0:
        return frame

    for column in RELEVANCE_EVIDENCE_OPTIONAL_COLUMNS:
        if column not in gate_frame.columns:
            gate_frame[column] = None
    reason_codes_series = gate_frame.get("reason_codes")
    if reason_codes_series is None:
        gate_frame["reason_codes"] = "[]"
    else:
        gate_frame["reason_codes"] = reason_codes_series.map(
            lambda value: json.dumps(_json_string_list(value), sort_keys=True, separators=(",", ":"))
        )

    selected_columns = [
        "_artifact_date",
        "_join_ticker",
        "_join_article_id",
        "_join_sentence_index",
        "_join_chunk_index",
        "relevance_decision",
        "ticker_relevance_score",
        "financial_relevance_score",
        "topic_relevance_score",
        "reason_codes",
        *RELEVANCE_EVIDENCE_OPTIONAL_COLUMNS,
    ]
    gate_frame = gate_frame.loc[:, selected_columns].drop_duplicates(
        subset=[
            "_artifact_date",
            "_join_ticker",
            "_join_article_id",
            "_join_sentence_index",
            "_join_chunk_index",
        ],
        keep="first",
    )
    gate_frame = gate_frame.rename(
        columns={
            "relevance_decision": "_relevance_decision",
            "ticker_relevance_score": "_ticker_relevance_score",
            "financial_relevance_score": "_financial_relevance_score",
            "topic_relevance_score": "_topic_relevance_score",
            "relevance_category": "_relevance_category",
            "target_context_score": "_target_context_score",
            "document_sentiment": "_document_sentiment",
            "target_company_impact_direction": "_target_company_impact_direction",
            "target_company_impact_magnitude": "_target_company_impact_magnitude",
            "impact_horizon": "_impact_horizon",
            "causal_channel": "_causal_channel",
            "target_impact_confidence": "_target_impact_confidence",
            "article_contamination_ratio": "_article_contamination_ratio",
            "article_contamination_count": "_article_contamination_count",
            "article_signal_count": "_article_signal_count",
            "article_contribution_weight": "_article_contribution_weight",
            "reason_codes": "_relevance_reason_codes",
        }
    )
    frame["_join_ticker"] = frame["ticker"].map(_normalize_join_ticker)
    frame["_join_article_id"] = frame.get("article_id", pd.Series(index=frame.index)).map(
        _normalize_optional_string
    )
    frame["_join_sentence_index"] = frame.get(
        "sentence_index",
        pd.Series(index=frame.index),
    ).map(_normalize_optional_int)
    frame["_join_chunk_index"] = frame.get(
        "chunk_index",
        pd.Series(index=frame.index),
    ).map(_normalize_optional_int)
    frame = frame.drop(
        columns=[
            "_relevance_decision",
            "_ticker_relevance_score",
            "_financial_relevance_score",
            "_topic_relevance_score",
            "_relevance_category",
            "_target_context_score",
            "_document_sentiment",
            "_target_company_impact_direction",
            "_target_company_impact_magnitude",
            "_impact_horizon",
            "_causal_channel",
            "_target_impact_confidence",
            "_article_contamination_ratio",
            "_article_contamination_count",
            "_article_signal_count",
            "_article_contribution_weight",
            "_relevance_reason_codes",
        ],
        errors="ignore",
    ).merge(
        gate_frame,
        on=[
            "_artifact_date",
            "_join_ticker",
            "_join_article_id",
            "_join_sentence_index",
            "_join_chunk_index",
        ],
        how="left",
    )
    frame["_missing_relevance_evidence"] = frame["_relevance_decision"].map(
        _normalize_optional_string
    ).isna()
    return frame


def _validate_optional_evidence_columns(
    frame: pd.DataFrame,
    *,
    required: frozenset[str],
    label: str,
) -> None:
    """Raise when an optional evidence artifact is present but malformed."""
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ValueError(f"{label} frame missing required columns: {missing}")


def _validate_columns(
    frame: pd.DataFrame,
    *,
    required: frozenset[str] = REQUIRED_SENTIMENT_COLUMNS,
) -> None:
    """Raise when sentiment rows are missing required columns."""
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ValueError(f"Sentiment frame missing required columns: {missing}")


def _validate_config(config: SourceCredibilityConfig) -> None:
    """Raise when source credibility config contains invalid weights."""
    _validate_weight(config.default_source_weight, label="default_source_weight")
    _normalize_source_weights(config.source_weights)


def _validate_timezone(timezone_name: str) -> None:
    """Raise when the configured date-bucketing timezone is invalid."""
    try:
        ZoneInfo(timezone_name)
    except ZoneInfoNotFoundError as exc:
        raise ValueError(f"Invalid bucket_timezone: {timezone_name}") from exc


def _normalized_config(config: SourceCredibilityConfig) -> SourceCredibilityConfig:
    """Return a config copy with validated source keys and weights."""
    return SourceCredibilityConfig(
        default_source_weight=_validate_weight(
            config.default_source_weight,
            label="default_source_weight",
        ),
        source_weights=_normalize_source_weights(config.source_weights),
    )


def _normalize_source_weights(raw_weights: Mapping[str, Any]) -> dict[str, float]:
    """Return source weights keyed by normalized source names."""
    if not isinstance(raw_weights, Mapping):
        raise ValueError("source_weights must be a mapping")

    weights: dict[str, float] = {}
    for source, weight in raw_weights.items():
        normalized_source = _normalize_source(source)
        if not normalized_source:
            raise ValueError("source_weights keys must be non-empty source names")
        weights[normalized_source] = _validate_weight(
            weight,
            label=f"source_weights[{source!r}]",
        )
    return weights


def _validate_weight(value: Any, *, label: str) -> float:
    """Return a finite positive weight or raise ValueError."""
    numeric = _to_float_or_none(value)
    if numeric is None or numeric <= 0.0:
        raise ValueError(f"{label} must be a positive finite number")
    return numeric


def _source_weight(source: Any, *, config: SourceCredibilityConfig) -> float:
    """Return configured source weight for one raw source value."""
    normalized_source = _normalize_source(source)
    if not normalized_source:
        return config.default_source_weight
    return config.source_weights.get(normalized_source, config.default_source_weight)


def _topic_effective_weight(row: Mapping[str, Any]) -> float:
    """Return the source/relevance/topic confidence product for topic sentiment."""
    topic_id = _normalize_optional_int(row.get("_topic_id"))
    probability = _to_float_or_none(row.get("_topic_probability"))
    effective_weight = _to_float_or_none(row.get("_effective_weight"))
    if topic_id is None or topic_id < 0 or probability is None or effective_weight is None:
        return 0.0
    return max(probability, 0.0) * max(effective_weight, 0.0)


def _relevance_weight(value: Any) -> float:
    """Return the non-negative article relevance multiplier.

    Missing relevance stays non-direct evidence by contributing zero weight.
    """
    numeric = _to_float_or_none(value)
    if numeric is None:
        return 0.0
    return min(max(numeric, 0.0), 1.0)


def _article_cap_weight(value: Any) -> float:
    """Return the bounded article/event contribution cap multiplier."""
    numeric = _to_float_or_none(value)
    if numeric is None:
        return 1.0
    return min(max(numeric, 0.0), 1.0)


def _apply_article_event_caps(frame: pd.DataFrame) -> pd.Series:
    """Return weights normalized so one multi-chunk article counts once.

    The relevance gate may emit multiple scored chunks for the same article.
    This cap keeps article length from becoming signal strength by dividing
    each row's effective weight by the number of scored rows for its
    date/ticker/article bucket. Rows without article ids are left as their own
    single-row events.
    """
    if len(frame) == 0 or "article_id" not in frame.columns:
        return frame["_effective_weight"]

    working = frame.copy()
    working["_article_event_key"] = [
        _article_event_key(index, row)
        for index, row in zip(working.index.tolist(), working.to_dict(orient="records"), strict=True)
    ]
    bucket_counts = working.groupby("_article_event_key", dropna=False)[
        "_article_event_key"
    ].transform("count")
    return working["_effective_weight"] / bucket_counts


def _resolve_relevance_score(
    record: NewsSentimentRecord,
    *,
    default_relevance_score: float | None,
) -> float | None:
    """Return a conservative relevance score inferred from explicit evidence.

    Rows that already carry a relevance score keep it. When no explicit score is
    present, provenance classification is authoritative if available; otherwise
    we fall back to text heuristics.
    """
    assignment_class = _assignment_classification(record)
    if assignment_class is not None:
        return NEWS_EVIDENCE_RELEVANCE_WEIGHTS[assignment_class]

    existing = _to_float_or_none(record.relevance_score)
    if existing is not None:
        return min(max(existing, 0.0), 1.0)

    text = _scoring_text(record)
    if text is None:
        return None

    evidence = _ticker_relevance_evidence(record.ticker, text)
    if evidence == "direct":
        return DIRECT_RELEVANCE_SCORE
    if evidence == "indirect":
        return INDIRECT_RELEVANCE_SCORE
    if evidence == "broad_market":
        return BROAD_MARKET_RELEVANCE_SCORE
    if evidence == "contamination":
        return CONTAMINATION_RELEVANCE_SCORE
    if evidence is None:
        return None
    return None


def _assignment_classification(record: NewsSentimentRecord) -> NewsEvidenceClass | None:
    """Return the provenance class attached to a preprocessed news record."""
    provenance = record.source_text_provenance or {}
    classification = _normalize_optional_string(provenance.get("assignment_classification"))
    if classification is None:
        return None
    try:
        return NewsEvidenceClass(classification)
    except ValueError:
        return None


def _ticker_relevance_evidence(ticker: str, text: str) -> str | None:
    """Classify ticker evidence as direct, indirect, broad-market, contamination, or absent."""
    normalized_ticker = canonicalize_ticker(ticker)
    if not normalized_ticker:
        return None

    normalized_text = text.lower()
    if _contains_ticker_mention(normalized_ticker, normalized_text):
        return "direct"
    if _contains_alias_mention(normalized_ticker, normalized_text):
        return "direct"
    if _contains_indirect_evidence(normalized_text):
        return "indirect"
    if _contains_broad_market_evidence(normalized_text):
        return "broad_market"
    if _contains_contamination_evidence(normalized_text):
        return "contamination"
    return None


def _contains_ticker_mention(ticker: str, text: str) -> bool:
    """Return True when the article explicitly mentions the ticker symbol."""
    aliases = {ticker}
    if "-" in ticker:
        aliases.add(ticker.replace("-", "."))
        aliases.add(ticker.replace("-", ""))
    for alias in aliases:
        pattern = rf"(?<![A-Z0-9]){re.escape(alias)}(?![A-Z0-9])"
        if re.search(pattern, text, flags=re.IGNORECASE):
            return True
    return False


def _contains_alias_mention(ticker: str, text: str) -> bool:
    """Return True when the article explicitly names the company entity."""
    aliases = _TICKER_ENTITY_ALIASES.get(ticker, ())
    for alias in aliases:
        pattern = rf"(?<![A-Z0-9]){re.escape(alias)}(?![A-Z0-9])"
        if re.search(pattern, text, flags=re.IGNORECASE):
            return True
    return False


def _contains_indirect_evidence(text: str) -> bool:
    """Return True when the article contains peer/competitor/relationship context."""
    return _contains_contextual_evidence(text)


def _contains_broad_market_evidence(text: str) -> bool:
    """Return True when the article contains broad-market context."""
    return _contains_contextual_evidence(text)


def _contains_contamination_evidence(text: str) -> bool:
    """Return True when the article contains unrelated company contamination."""
    return _contains_contextual_evidence(text)


def _contains_contextual_evidence(text: str) -> bool:
    """Return True when the article only offers broad-market or peer context."""
    for term in _CONTEXTUAL_EVIDENCE_TERMS:
        pattern = rf"(?<![a-z0-9]){re.escape(term)}(?![a-z0-9])"
        if re.search(pattern, text):
            return True
    return False


def _weighted_average(values: pd.Series, weights: pd.Series) -> float | None:
    """Return the finite weighted average, or None when no positive weight exists."""
    total_weight = 0.0
    weighted_sum = 0.0
    for raw_value, raw_weight in zip(values.tolist(), weights.tolist(), strict=True):
        value = _to_float_or_none(raw_value)
        weight = _to_float_or_none(raw_weight)
        if value is None or weight is None or weight <= 0.0:
            continue
        weighted_sum += value * weight
        total_weight += weight
    if total_weight <= 0.0:
        return None
    return weighted_sum / total_weight


def _mean(values: pd.Series) -> float | None:
    """Return a finite arithmetic mean, or None for empty inputs."""
    finite = [value for value in values.map(_to_float_or_none).tolist() if value is not None]
    if not finite:
        return None
    return sum(finite) / len(finite)


def _sum_positive(values: pd.Series) -> float:
    """Return the sum of positive finite values."""
    return sum(
        value
        for value in values.map(_to_float_or_none).tolist()
        if value is not None and value > 0.0
    )


def _min_finite(values: pd.Series) -> float | None:
    """Return the minimum finite value, or None when no values are usable."""
    finite = [value for value in values.map(_to_float_or_none).tolist() if value is not None]
    if not finite:
        return None
    return min(finite)


def _topic_count(group: pd.DataFrame) -> int:
    """Return the count of valid topic ids contributing to sentiment."""
    topic_ids = [
        topic_id
        for topic_id in group["_topic_id"].map(_normalize_optional_int).tolist()
        if topic_id is not None and topic_id >= 0
    ]
    return len(set(topic_ids))


def _decision_count(group: pd.DataFrame, decision: str) -> int:
    """Return the count of rows matching one relevance decision."""
    return sum(
        1
        for value in group["_relevance_decision"].map(_normalize_optional_string).tolist()
        if value == decision
    )


def _missing_topic_count(group: pd.DataFrame) -> int:
    """Return the count of scored rows without matching topic evidence."""
    return int(group["_missing_topic_evidence"].map(bool).sum())


def _missing_relevance_count(group: pd.DataFrame) -> int:
    """Return the count of scored rows without matching relevance-gate evidence."""
    return int(group["_missing_relevance_evidence"].map(bool).sum())


def _missing_source_count(group: pd.DataFrame) -> int:
    """Return the count of scored rows that fell back to the default source weight."""
    return sum(
        1 for value in group["source"].map(_normalize_optional_string).tolist() if value is None
    )


def _contributing_article_ids(group: pd.DataFrame) -> list[str]:
    """Return sorted unique article ids contributing to a ticker-day aggregate."""
    if "article_id" not in group.columns:
        return []
    return sorted(
        {
            article_id
            for article_id in group["article_id"].map(_normalize_optional_string).tolist()
            if article_id is not None
        }
    )


def _topic_sentiment_summary(group: pd.DataFrame) -> list[dict[str, Any]]:
    """Return per-topic sentiment summaries ordered by topic evidence weight."""
    valid = group[group["_topic_effective_weight"].map(lambda value: float(value) > 0.0)]
    if len(valid) == 0:
        return []

    summaries: list[dict[str, Any]] = []
    for topic_id, topic_group in valid.groupby("_topic_id", sort=True, dropna=True):
        topic_weight = _sum_positive(topic_group["_topic_effective_weight"])
        summaries.append(
            {
                "topic_id": _normalize_optional_int(topic_id),
                "sentence_count": int(len(topic_group)),
                "article_count": _article_count(topic_group),
                "weight": topic_weight,
                "sentiment_score": _weighted_average(
                    topic_group["sentiment_score"],
                    topic_group["_topic_effective_weight"],
                ),
                "mean_topic_probability": _weighted_average(
                    topic_group["_topic_probability"],
                    topic_group["_effective_weight"],
                ),
            }
        )
    return sorted(
        summaries,
        key=lambda row: (-(row["weight"] or 0.0), row["topic_id"] if row["topic_id"] is not None else 0),
    )


def _source_weight_summary(group: pd.DataFrame) -> list[dict[str, Any]]:
    """Return per-source counts and configured weights used by aggregation."""
    summary_rows: list[dict[str, Any]] = []
    total_effective_weight = _sum_positive(group["_effective_weight"])
    for source, source_group in group.groupby("source", sort=True, dropna=False):
        source_effective_weight = _sum_positive(source_group["_effective_weight"])
        summary_rows.append(
            {
                "source": _normalize_optional_string(source),
                "source_weight": _mean(source_group["_source_weight"]),
                "sentence_count": int(len(source_group)),
                "article_count": _article_count(source_group),
                "effective_weight": source_effective_weight,
                "weight_share": (
                    source_effective_weight / total_effective_weight
                    if total_effective_weight > 0.0
                    else None
                ),
            }
        )
    return summary_rows


def _source_concentration(group: pd.DataFrame) -> dict[str, Any]:
    """Return source diversification diagnostics for one ticker-day group."""
    source_rows = _source_weight_summary(group)
    populated_sources = [
        row for row in source_rows if row["source"] is not None and row["effective_weight"] > 0.0
    ]
    if not populated_sources:
        return {
            "source_count": 0,
            "dominant_source": None,
            "dominant_source_article_count": 0,
            "dominant_source_weight_share": None,
            "effective_source_count": None,
        }

    dominant = max(
        populated_sources,
        key=lambda row: (row["weight_share"] or 0.0, row["article_count"], str(row["source"])),
    )
    weight_shares = [
        float(row["weight_share"])
        for row in populated_sources
        if _to_float_or_none(row["weight_share"]) is not None and row["weight_share"] > 0.0
    ]
    herfindahl = sum(share * share for share in weight_shares)
    return {
        "source_count": len({str(row["source"]) for row in populated_sources}),
        "dominant_source": dominant["source"],
        "dominant_source_article_count": dominant["article_count"],
        "dominant_source_weight_share": dominant["weight_share"],
        "effective_source_count": 1.0 / herfindahl if herfindahl > 0.0 else None,
    }


def _semantic_warning_codes(group: pd.DataFrame) -> list[str]:
    """Return deterministic warning codes for degraded optional semantic inputs."""
    warnings: set[str] = set()
    if _missing_source_count(group) > 0:
        warnings.add("missing_source_default_weight")
    if bool(group["_missing_topic_artifact"].any()):
        warnings.add("missing_topic_artifact")
    elif _missing_topic_count(group) > 0:
        warnings.add("missing_topic_evidence")
    if _missing_relevance_count(group) > 0:
        warnings.add("missing_relevance_evidence")
    source_concentration = _source_concentration(group)
    article_count = _article_count(group)
    dominant_share = _to_float_or_none(source_concentration["dominant_source_weight_share"])
    if article_count >= 2 and source_concentration["source_count"] == 1:
        warnings.add("single_source_concentration")
    elif article_count >= 3 and dominant_share is not None and dominant_share >= 0.80:
        warnings.add("source_concentration_high")
    return sorted(warnings)


def _relevance_reason_codes(group: pd.DataFrame) -> list[str]:
    """Return relevance-gate reason codes that contributed to scored rows."""
    reason_codes: set[str] = set()
    for value in group["_relevance_reason_codes"].map(_json_string_list).tolist():
        reason_codes.update(value)
    return sorted(reason_codes)


def _validate_probability_columns(frame: pd.DataFrame) -> None:
    """Raise when FinBERT probability columns fall outside [0, 1]."""
    for column in ("sentiment_positive", "sentiment_negative", "sentiment_neutral"):
        invalid = frame[column].map(lambda value: value < 0.0 or value > 1.0)
        if invalid.any():
            raise ValueError(f"{column} must contain probabilities in [0, 1]")


def _normalize_source(source: Any) -> str:
    """Return a stable lowercase source key for config lookup."""
    if source is None:
        return ""
    return re.sub(r"\s+", " ", str(source).strip().lower())


def _normalize_join_ticker(value: Any) -> str | None:
    """Return uppercase ticker text for evidence joins."""
    text = _normalize_optional_string(value)
    return text.upper() if text is not None else None


def _to_iso_date(value: Any) -> str | None:
    """Normalize date-like values to YYYY-MM-DD strings."""
    if value is None:
        return None
    try:
        timestamp = _require_pandas().to_datetime(value, utc=True)
    except (TypeError, ValueError):
        return None
    if timestamp is None or getattr(timestamp, "tzinfo", None) is None:
        return None
    return timestamp.date().isoformat()


def _scoring_text(record: NewsSentimentRecord) -> str | None:
    """Return the text chunk used for FinBERT scoring."""
    return _normalize_optional_string(record.text) or _normalize_optional_string(record.headline)


def _bucket_date(
    published_at: Any,
    *,
    fallback_date: Any,
    bucket_timezone: str,
) -> str | None:
    """Return the ticker-day bucket for a row using the configured timezone."""
    pd = _require_pandas()
    if published_at is not None and not pd.isna(published_at):
        try:
            timestamp = pd.to_datetime(published_at, utc=True)
        except (TypeError, ValueError):
            timestamp = None
        if timestamp is not None and not pd.isna(timestamp):
            return timestamp.tz_convert(bucket_timezone).date().isoformat()
    return _to_iso_date(fallback_date)


def _article_count(group: pd.DataFrame) -> int:
    """Return unique article count when article ids exist, otherwise row count."""
    if "article_id" not in group.columns:
        return int(len(group))
    article_ids = [
        text
        for text in group["article_id"].map(_normalize_optional_string).tolist()
        if text is not None
    ]
    if not article_ids:
        return int(len(group))
    return len(set(article_ids))


def _article_event_key(index: Any, row: Mapping[str, Any]) -> str:
    """Return the bucket key used to cap repeated chunks from one article."""
    article_id = _normalize_optional_string(row.get("article_id"))
    if article_id is None:
        return f"row:{index}"
    return "|".join(
        [
            _normalize_optional_string(row.get("date")) or "",
            _normalize_optional_string(row.get("ticker")) or "",
            article_id,
        ]
    )


def _to_float_or_none(value: Any) -> float | None:
    """Return a finite float value, or None for missing/non-finite input."""
    if value is None:
        return None
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(numeric) or math.isinf(numeric):
        return None
    return numeric


def _normalize_optional_int(value: Any) -> int | None:
    """Return an int from a finite numeric value, or None."""
    numeric = _to_float_or_none(value)
    if numeric is None:
        return None
    return int(numeric)


def _json_string_list(value: Any) -> list[str]:
    """Return a string list from decoded or JSON-encoded values."""
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


def _normalize_optional_float(value: Any) -> float | None:
    """Return a finite optional float suitable for Pydantic models."""
    return _to_float_or_none(value)


def _normalize_optional_string(value: Any) -> str | None:
    """Return a non-empty string or None."""
    if value is None:
        return None
    try:
        if value != value:
            return None
    except (TypeError, ValueError):
        pass
    text = str(value).strip()
    return text or None


def _first_non_null(values: pd.Series) -> Any:
    """Return the first non-missing value from a series."""
    pd = _require_pandas()
    for value in values.tolist():
        if value is None:
            continue
        try:
            if pd.isna(value):
                continue
        except (TypeError, ValueError):
            pass
        return value
    return None


def _empty_frame(pd: Any) -> pd.DataFrame:
    """Return an empty sentiment aggregate frame with canonical columns."""
    return pd.DataFrame(columns=list(SENTIMENT_AGGREGATE_COLUMNS))


def _require_pandas() -> Any:
    """Import pandas/pyarrow lazily with a clear error when absent."""
    try:
        import pandas as pd

        importlib.import_module("pyarrow")
    except ModuleNotFoundError as exc:
        raise ModuleNotFoundError(
            "pandas and pyarrow are required for sentiment feature aggregation."
        ) from exc
    return pd
