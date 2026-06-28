"""Semantic-review evidence assembly for the Layer 1 AAPL pilot dashboard."""
from __future__ import annotations

import csv
import json
import re
from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from datetime import date as Date
from io import BytesIO, StringIO
from pathlib import Path
from typing import Any

import pandas as pd

from core.common.trading_calendar import skipped_non_trading_dates, trading_dates
from core.features.regime_training import HMM_TRAINING_FEATURE_COLUMNS
from services.r2.paths import (
    layer1_feature_path,
    layer1_news_preprocessing_path,
    layer1_news_relevance_gate_path,
    layer1_regime_path,
    layer1_sentiment_feature_path,
    layer1_sentiment_score_path,
    layer1_text_embedding_path,
    layer1_topic_feature_path,
    layer1_topic_label_path,
    pipeline_manifest_path,
    raw_price_path,
)
from services.r2.writer import R2Writer

DEFAULT_RELEVANCE_THRESHOLD = 0.6
HUMAN_REVIEW_STATUSES = frozenset({"pending", "accepted", "rejected"})
DEFAULT_AAPL_PILOT_EVIDENCE_OUTPUT_DIR = Path("artifacts/reports/diagnostics")


def default_aapl_pilot_evidence_paths(run_id: str) -> dict[str, Path]:
    """Return the default JSON, Markdown, and CSV output paths for one AAPL pilot run."""
    safe_run_id = run_id.strip()
    return {
        "json": DEFAULT_AAPL_PILOT_EVIDENCE_OUTPUT_DIR / f"aapl_pilot_evidence_{safe_run_id}.json",
        "markdown": DEFAULT_AAPL_PILOT_EVIDENCE_OUTPUT_DIR / f"aapl_pilot_evidence_{safe_run_id}.md",
        "csv": DEFAULT_AAPL_PILOT_EVIDENCE_OUTPUT_DIR / f"aapl_pilot_evidence_{safe_run_id}.csv",
    }


@dataclass(frozen=True)
class SemanticReviewSentenceRow:
    """Sentence-level FinBERT evidence for one scored-news row."""

    sentence_index: int | None
    chunk_index: int | None
    source_text_field: str | None
    source_text_order: int | None
    ticker_mentions: list[str]
    entity_mentions: list[str]
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
    preprocessing_rows: list[dict[str, object]]
    topic_evidence: list[dict[str, object]]
    relevance_gate_rows: list[dict[str, object]]
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
    readiness_status: str | None = None
    readiness_reason: str | None = None
    required_for_layer2: bool | None = None
    missing_features: list[str] = field(default_factory=list)
    probability_sum: float | None = None
    training_rows: int | None = None
    complete_training_rows: int | None = None
    min_training_rows: int | None = None
    artifact_key: str | None = None
    manifest_key: str | None = None

    def to_dict(self) -> dict[str, object]:
        """Return a JSON-serializable representation."""
        payload = asdict(self)
        payload["scope"] = "date-level"
        payload["applies_to"] = "all sentence rows on the trading date"
        return payload


@dataclass(frozen=True)
class SemanticReviewPriceRow:
    """Date-level raw price evidence aligned to semantic and HMM review rows."""

    date: str
    ticker: str
    open: float | None
    high: float | None
    low: float | None
    close: float | None
    adj_close: float | None
    volume: int | None
    dollar_volume: float | None
    return_1d: float | None
    drawdown_from_window_high: float | None
    artifact_key: str | None

    def to_dict(self) -> dict[str, object]:
        """Return a JSON-serializable representation."""
        payload = asdict(self)
        payload["scope"] = "ticker-date"
        payload["stage"] = "raw_price_context"
        return payload


@dataclass(frozen=True)
class SemanticReviewDateGroup:
    """One trading-date bucket that contains the date-level regime and article cards."""

    date: str
    regime: dict[str, object] | None
    price: dict[str, object] | None
    market_regime_context: dict[str, object]
    article_count: int
    accepted_article_count: int
    flagged_article_count: int
    sentence_count: int
    semantic_aggregates: list[dict[str, object]]
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
    preprocessing_row_count: int
    embedding_row_count: int
    topic_label_row_count: int
    relevance_gate_row_count: int
    semantic_aggregate_row_count: int
    load_warnings: list[dict[str, object]]
    artifact_keys: dict[str, list[str]]
    preprocessing_rows: list[dict[str, object]]
    embedding_rows: list[dict[str, object]]
    topic_label_rows: list[dict[str, object]]
    relevance_gate_rows: list[dict[str, object]]
    semantic_aggregate_rows: list[dict[str, object]]
    regime_rows: list[dict[str, object]]
    price_rows: list[dict[str, object]]
    market_regime_rows: list[dict[str, object]]
    benchmark_ticker: str | None
    benchmark_price_rows: list[dict[str, object]]
    benchmark_market_regime_rows: list[dict[str, object]]
    hmm_evaluation_context: dict[str, object]
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
    artifact_keys: dict[str, list[str]] = {
        "news_preprocessing": [],
        "text_embeddings": [],
        "topic_labels": [],
        "news_relevance_gate": [],
        "news_sentiment_scored": [],
        "sentiment_features": [],
        "regime": [],
        "regime_manifests": [],
        "raw_prices": [],
    }
    trading_date_list = trading_dates(from_date, to_date)
    price_key = raw_price_path(requested_ticker)
    price_rows = _load_price_rows(
        writer=active_writer,
        key=price_key,
        ticker=requested_ticker,
        dates=trading_date_list,
        artifact_keys=artifact_keys,
        load_warnings=load_warnings,
    )
    for current_date in trading_date_list:
        primary_key = layer1_sentiment_score_path(current_date, run_id)
        fallback_key = layer1_sentiment_score_path(current_date, f"{run_id}-{current_date}")
        scored_frame, resolved_key = _read_first_available_parquet_frame(
            active_writer,
            (primary_key, fallback_key),
        )
        if scored_frame is None:
            load_warnings.append(
                {
                    "scope": "sentence_rows",
                    "date": current_date,
                    "key": primary_key,
                    "fallback_key": fallback_key,
                    "tried_keys": [primary_key, fallback_key],
                    "message": "Missing scored-news parquet for this trading date.",
                }
            )
            continue
        if scored_frame.empty:
            load_warnings.append(
                {
                    "scope": "sentence_rows",
                    "date": current_date,
                    "key": resolved_key,
                    "fallback_key": fallback_key if resolved_key == primary_key else primary_key,
                    "tried_keys": [primary_key, fallback_key],
                    "message": "Scored-news parquet is empty.",
                }
            )
            continue
        if resolved_key is not None:
            artifact_keys["news_sentiment_scored"].append(resolved_key)
        scored_frames.append(scored_frame)

    regime_frames: list[pd.DataFrame] = []
    for current_date in trading_date_list:
        primary_key = layer1_regime_path(current_date, run_id)
        fallback_key = layer1_regime_path(current_date, f"{run_id}-{current_date}")
        regime_frame, resolved_key = _read_first_available_parquet_frame(
            active_writer,
            (primary_key, fallback_key),
        )
        if regime_frame is None:
            load_warnings.append(
                {
                    "scope": "regime",
                    "date": current_date,
                    "key": primary_key,
                    "fallback_key": fallback_key,
                    "tried_keys": [primary_key, fallback_key],
                    "message": "Missing date-level HMM regime parquet for this trading date.",
                }
            )
            continue
        if regime_frame.empty:
            load_warnings.append(
                {
                    "scope": "regime",
                    "date": current_date,
                    "key": resolved_key,
                    "fallback_key": fallback_key if resolved_key == primary_key else primary_key,
                    "tried_keys": [primary_key, fallback_key],
                    "message": "Regime parquet is empty.",
                }
            )
            continue
        if resolved_key is not None:
            artifact_keys["regime"].append(resolved_key)
            regime_frame = regime_frame.copy()
            regime_frame["_artifact_key"] = resolved_key
        regime_frames.append(regime_frame)

    regime_frame = pd.concat(regime_frames, ignore_index=True) if regime_frames else pd.DataFrame()
    regime_manifest_context = _load_regime_manifest_context(
        writer=active_writer,
        dates=trading_date_list,
        run_id=run_id,
        artifact_keys=artifact_keys,
        load_warnings=load_warnings,
    )
    benchmark_ticker = _benchmark_ticker_from_manifest_context(regime_manifest_context)
    benchmark_price_rows = _load_price_rows(
        writer=active_writer,
        key=raw_price_path(benchmark_ticker),
        ticker=benchmark_ticker,
        dates=trading_date_list,
        artifact_keys=artifact_keys,
        load_warnings=load_warnings,
    )

    preprocessing_frame = _load_date_partitioned_artifacts(
        writer=active_writer,
        dates=trading_date_list,
        run_id=run_id,
        path_builder=layer1_news_preprocessing_path,
        artifact_name="news_preprocessing",
        artifact_keys=artifact_keys,
        load_warnings=load_warnings,
    )
    embedding_frame = _load_date_partitioned_artifacts(
        writer=active_writer,
        dates=trading_date_list,
        run_id=run_id,
        path_builder=layer1_text_embedding_path,
        artifact_name="text_embeddings",
        artifact_keys=artifact_keys,
        load_warnings=load_warnings,
    )
    topic_label_frame = _load_date_partitioned_artifacts(
        writer=active_writer,
        dates=trading_date_list,
        run_id=run_id,
        path_builder=layer1_topic_label_path,
        artifact_name="topic_labels",
        artifact_keys=artifact_keys,
        load_warnings=load_warnings,
    )
    relevance_gate_frame = _load_date_partitioned_artifacts(
        writer=active_writer,
        dates=trading_date_list,
        run_id=run_id,
        path_builder=layer1_news_relevance_gate_path,
        artifact_name="news_relevance_gate",
        artifact_keys=artifact_keys,
        load_warnings=load_warnings,
    )
    semantic_aggregate_frame = _load_date_partitioned_artifacts(
        writer=active_writer,
        dates=trading_date_list,
        run_id=run_id,
        path_builder=layer1_sentiment_feature_path,
        artifact_name="sentiment_features",
        artifact_keys=artifact_keys,
        load_warnings=load_warnings,
    )

    if (not scored_frames or not regime_frames) and (cached_frames := _load_cached_aapl_pilot_evidence_frames(
        run_id=run_id,
        from_date=from_date,
        to_date=to_date,
        ticker=requested_ticker,
    )) is not None:
        raw_lookup_warnings = list(load_warnings)
        scored_frame, regime_frame = cached_frames
        price_rows = []
        regime_manifest_context = _empty_regime_manifest_context(trading_date_list)
        scored_frames = [scored_frame]
        regime_frames = [regime_frame]
        load_warnings = [
            {
                "scope": "cached_bundle",
                "message": (
                    "Loaded cached AAPL pilot evidence bundle after raw stage-artifact lookups "
                    "returned no rows."
                ),
                "run_id": run_id,
                "source": str(_find_cached_aapl_pilot_evidence_bundle_path(run_id)),
                "raw_lookup_warnings": raw_lookup_warnings,
            }
        ] + raw_lookup_warnings

    if scored_frames:
        scored_frame = pd.concat(scored_frames, ignore_index=True)
    else:
        scored_frame = pd.DataFrame()

    preprocessing_rows = _preprocessing_rows(preprocessing_frame, requested_ticker=requested_ticker)
    embedding_rows = _embedding_rows(embedding_frame)
    topic_label_rows = _topic_label_rows(topic_label_frame, requested_ticker=requested_ticker)
    relevance_gate_rows = _relevance_gate_rows(relevance_gate_frame, requested_ticker=requested_ticker)
    semantic_aggregate_rows = _semantic_aggregate_rows(
        semantic_aggregate_frame,
        requested_ticker=requested_ticker,
    )
    article_groups, article_summary = _build_article_groups(
        scored_frame,
        requested_ticker=requested_ticker,
        relevance_threshold=relevance_threshold,
        preprocessing_rows=preprocessing_rows,
        topic_label_rows=topic_label_rows,
        relevance_gate_rows=relevance_gate_rows,
    )
    regime_by_date = _build_regime_map(regime_frame)
    _enrich_regime_rows_with_manifests(regime_by_date, regime_manifest_context)
    _append_market_context_warnings(
        dates=trading_date_list,
        price_rows=price_rows,
        regime_map=regime_by_date,
        load_warnings=load_warnings,
    )
    market_regime_rows = _build_market_regime_rows(
        dates=trading_date_list,
        price_rows=price_rows,
        regime_map=regime_by_date,
    )
    benchmark_market_regime_rows = _build_market_regime_rows(
        dates=trading_date_list,
        price_rows=benchmark_price_rows,
        regime_map=regime_by_date,
    )
    hmm_evaluation_context = _build_hmm_evaluation_context(
        dates=trading_date_list,
        regime_map=regime_by_date,
        manifest_context=regime_manifest_context,
        artifact_keys=artifact_keys,
    )
    date_groups = _build_date_groups(
        article_groups,
        regime_by_date,
        semantic_aggregate_rows,
        price_rows=price_rows,
        market_regime_rows=market_regime_rows,
    )
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
        "preprocessing_row_count": len(preprocessing_rows),
        "embedding_row_count": len(embedding_rows),
        "topic_label_row_count": len(topic_label_rows),
        "relevance_gate_row_count": len(relevance_gate_rows),
        "semantic_aggregate_row_count": len(semantic_aggregate_rows),
        "price_row_count": len(price_rows),
        "hmm_regime_row_count": len(regime_by_date),
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
        preprocessing_row_count=summary["preprocessing_row_count"],
        embedding_row_count=summary["embedding_row_count"],
        topic_label_row_count=summary["topic_label_row_count"],
        relevance_gate_row_count=summary["relevance_gate_row_count"],
        semantic_aggregate_row_count=summary["semantic_aggregate_row_count"],
        load_warnings=load_warnings,
        artifact_keys=artifact_keys,
        preprocessing_rows=preprocessing_rows,
        embedding_rows=embedding_rows,
        topic_label_rows=topic_label_rows,
        relevance_gate_rows=relevance_gate_rows,
        semantic_aggregate_rows=semantic_aggregate_rows,
        regime_rows=[item.to_dict() for item in _regime_rows_from_map(regime_by_date)],
        price_rows=[item.to_dict() for item in price_rows],
        market_regime_rows=market_regime_rows,
        benchmark_ticker=benchmark_ticker,
        benchmark_price_rows=[item.to_dict() for item in benchmark_price_rows],
        benchmark_market_regime_rows=benchmark_market_regime_rows,
        hmm_evaluation_context=hmm_evaluation_context,
        article_groups=[group.to_dict() for group in article_groups],
        date_groups=[group.to_dict() for group in date_groups],
        summary=summary,
    )


def _build_article_groups(
    frame: pd.DataFrame,
    *,
    requested_ticker: str,
    relevance_threshold: float,
    preprocessing_rows: Sequence[Mapping[str, object]] = (),
    topic_label_rows: Sequence[Mapping[str, object]] = (),
    relevance_gate_rows: Sequence[Mapping[str, object]] = (),
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
    preprocessing_by_article = _rows_by_article(preprocessing_rows)
    topic_by_article = _rows_by_article(topic_label_rows)
    relevance_by_article = _rows_by_article(relevance_gate_rows)
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
                chunk_index=_maybe_int(row.get("chunk_index")),
                source_text_field=_optional_str(row.get("source_text_field")),
                source_text_order=_maybe_int(row.get("source_text_order")),
                ticker_mentions=_json_string_list(row.get("ticker_mentions")),
                entity_mentions=_json_string_list(row.get("entity_mentions")),
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
                preprocessing_rows=preprocessing_by_article.get((str(date_text), str(article_id)), []),
                topic_evidence=topic_by_article.get((str(date_text), str(article_id)), []),
                relevance_gate_rows=relevance_by_article.get((str(date_text), str(article_id)), []),
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
    regime_column = _first_existing_column(normalized, ("regime", "state", "label", "regime_label"))
    confidence_column = _first_existing_column(normalized, ("confidence", "regime_confidence"))
    bear_column = _first_existing_column(
        normalized,
        ("prob_bear", "bear_prob", "probability_bear", "regime_prob_bear"),
    )
    sideways_column = _first_existing_column(
        normalized,
        ("prob_sideways", "sideways_prob", "probability_sideways", "regime_prob_sideways"),
    )
    bull_column = _first_existing_column(
        normalized,
        ("prob_bull", "bull_prob", "probability_bull", "regime_prob_bull"),
    )
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
            readiness_status=_optional_str(row.get("regime_readiness_status")),
            readiness_reason=_optional_str(row.get("regime_readiness_reason")),
            required_for_layer2=_maybe_bool(row.get("regime_required_for_layer2")),
            missing_features=_comma_string_list(row.get("regime_missing_features")),
            probability_sum=_maybe_float(row.get("regime_probability_sum")),
            training_rows=_maybe_int(row.get("training_rows")),
            complete_training_rows=_maybe_int(row.get("complete_training_rows")),
            min_training_rows=_maybe_int(row.get("min_training_rows")),
            artifact_key=_optional_str(row.get("_artifact_key")),
        )
    return regime_map


def _regime_rows_from_map(
    regime_map: Mapping[str, SemanticReviewRegimeRow],
) -> list[SemanticReviewRegimeRow]:
    return [regime_map[key] for key in sorted(regime_map)]


def _build_date_groups(
    article_groups: Sequence[SemanticReviewArticleGroup],
    regime_map: Mapping[str, SemanticReviewRegimeRow],
    semantic_aggregate_rows: Sequence[Mapping[str, object]] = (),
    *,
    price_rows: Sequence[SemanticReviewPriceRow] = (),
    market_regime_rows: Sequence[Mapping[str, object]] = (),
) -> list[SemanticReviewDateGroup]:
    grouped: dict[str, list[SemanticReviewArticleGroup]] = defaultdict(list)
    for article_group in article_groups:
        grouped[article_group.date].append(article_group)
    semantic_by_date: dict[str, list[dict[str, object]]] = defaultdict(list)
    for row in semantic_aggregate_rows:
        date_text = _optional_str(row.get("date"))
        if date_text:
            semantic_by_date[date_text].append(dict(row))
    price_by_date = {row.date: row.to_dict() for row in price_rows}
    context_by_date = {
        str(row["date"]): dict(row)
        for row in market_regime_rows
        if _optional_str(row.get("date")) is not None
    }

    date_groups: list[SemanticReviewDateGroup] = []
    for date_text in sorted(set(grouped) | set(semantic_by_date) | set(price_by_date) | set(regime_map)):
        articles = sorted(grouped[date_text], key=lambda item: (item.article_status, item.article_id))
        accepted_articles = [item.to_dict() for item in articles if item.article_status == "accepted"]
        flagged_articles = [item.to_dict() for item in articles if item.article_status != "accepted"]
        date_groups.append(
            SemanticReviewDateGroup(
                date=date_text,
                regime=regime_map.get(date_text).to_dict() if date_text in regime_map else None,
                price=price_by_date.get(date_text),
                market_regime_context=context_by_date.get(
                    date_text,
                    {
                        "date": date_text,
                        "ticker": None,
                        "price": None,
                        "hmm_regime": None,
                        "warnings": ["missing_aligned_market_regime_context"],
                    },
                ),
                article_count=len(articles),
                accepted_article_count=len(accepted_articles),
                flagged_article_count=len(flagged_articles),
                sentence_count=sum(item.article_row_count for item in articles),
                semantic_aggregates=semantic_by_date.get(date_text, []),
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
        "source_text_field",
        "ticker_mentions",
        "entity_mentions",
    ):
        if column not in normalized.columns:
            normalized[column] = None
    for column in (
        "sentence_index",
        "chunk_index",
        "source_text_order",
        "sentiment_score",
        "positive_probability",
        "negative_probability",
        "neutral_probability",
        "relevance_score",
    ):
        if column not in normalized.columns:
            normalized[column] = None
    _copy_first_existing_column(normalized, "positive_probability", ("sentiment_positive",))
    _copy_first_existing_column(normalized, "negative_probability", ("sentiment_negative",))
    _copy_first_existing_column(normalized, "neutral_probability", ("sentiment_neutral",))
    normalized["date"] = normalized["date"].map(lambda value: _optional_str(value) or "")
    normalized["ticker"] = normalized["ticker"].map(lambda value: _optional_str(value) or "")
    normalized["headline"] = normalized["headline"].map(_optional_str)
    normalized["text"] = normalized["text"].map(_optional_str)
    normalized["article_id"] = normalized["article_id"].map(lambda value: _optional_str(value) or _fallback_article_id(value))
    normalized["source"] = normalized["source"].map(_optional_str)
    normalized["url"] = normalized["url"].map(_optional_str)
    normalized["published_at"] = normalized["published_at"].map(_optional_str)
    normalized["source_text_field"] = normalized["source_text_field"].map(_optional_str)
    for column in (
        "sentence_index",
        "chunk_index",
        "source_text_order",
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


def _read_first_available_parquet_frame(
    writer: R2Writer,
    keys: Sequence[str],
) -> tuple[pd.DataFrame | None, str | None]:
    """Return the first readable parquet frame for one of the provided keys."""
    for key in keys:
        try:
            return _read_parquet_frame(writer.get_object(key)), key
        except FileNotFoundError:
            continue
    return None, None


def _load_price_rows(
    *,
    writer: R2Writer,
    key: str,
    ticker: str,
    dates: Sequence[str],
    artifact_keys: dict[str, list[str]],
    load_warnings: list[dict[str, object]],
) -> list[SemanticReviewPriceRow]:
    """Load raw price rows for the selected ticker/window."""
    try:
        frame = _read_parquet_frame(writer.get_object(key))
    except FileNotFoundError:
        load_warnings.append(
            {
                "scope": "price_series",
                "ticker": ticker,
                "key": key,
                "message": "Missing raw OHLCV price parquet for the selected ticker.",
            }
        )
        return []
    if frame.empty:
        load_warnings.append(
            {
                "scope": "price_series",
                "ticker": ticker,
                "key": key,
                "message": "Raw OHLCV price parquet is empty for the selected ticker.",
            }
        )
        return []

    artifact_keys["raw_prices"].append(key)
    normalized = frame.copy()
    normalized.columns = [str(column) for column in normalized.columns]
    if "date" not in normalized.columns:
        load_warnings.append(
            {
                "scope": "price_series",
                "ticker": ticker,
                "key": key,
                "message": "Raw OHLCV price parquet has no date column.",
            }
        )
        return []

    normalized["date"] = normalized["date"].map(lambda value: _optional_str(value) or "")
    if "ticker" in normalized.columns:
        normalized["ticker"] = normalized["ticker"].map(lambda value: _optional_str(value) or ticker)
        normalized = normalized[normalized["ticker"].str.upper() == ticker]
    expected_dates = set(dates)
    normalized = normalized[normalized["date"].isin(expected_dates)]
    if normalized.empty:
        load_warnings.append(
            {
                "scope": "price_series",
                "ticker": ticker,
                "key": key,
                "message": "Raw OHLCV price parquet has no rows in the review window.",
            }
        )
        return []

    normalized = normalized.sort_values("date").drop_duplicates("date").reset_index(drop=True)
    if "adj_close" not in normalized.columns and "close" not in normalized.columns:
        load_warnings.append(
            {
                "scope": "price_series",
                "ticker": ticker,
                "key": key,
                "message": "Raw OHLCV price parquet has neither adj_close nor close.",
            }
        )
        return []
    close_column = "adj_close" if "adj_close" in normalized.columns else "close"
    normalized["_adj_close_for_context"] = normalized[close_column].map(_maybe_float)
    normalized["_return_1d"] = normalized["_adj_close_for_context"].pct_change()
    running_high = normalized["_adj_close_for_context"].cummax()
    normalized["_drawdown_from_window_high"] = normalized["_adj_close_for_context"] / running_high - 1.0
    rows: list[SemanticReviewPriceRow] = []
    for _, row in normalized.iterrows():
        rows.append(
            SemanticReviewPriceRow(
                date=str(row["date"]),
                ticker=ticker,
                open=_maybe_float(row.get("open")),
                high=_maybe_float(row.get("high")),
                low=_maybe_float(row.get("low")),
                close=_maybe_float(row.get("close")),
                adj_close=_maybe_float(row.get("adj_close")),
                volume=_maybe_int(row.get("volume")),
                dollar_volume=_maybe_float(row.get("dollar_volume")),
                return_1d=_maybe_float(row.get("_return_1d")),
                drawdown_from_window_high=_maybe_float(row.get("_drawdown_from_window_high")),
                artifact_key=key,
            )
        )
    return rows


def _load_regime_manifest_context(
    *,
    writer: R2Writer,
    dates: Sequence[str],
    run_id: str,
    artifact_keys: dict[str, list[str]],
    load_warnings: list[dict[str, object]],
) -> dict[str, object]:
    """Load HMM regime manifest metadata for the selected date window."""
    by_date: dict[str, dict[str, object]] = {}
    manifests: list[dict[str, object]] = []
    seen_keys: set[str] = set()
    for date_text in dates:
        primary_key = pipeline_manifest_path("layer1_5_regime", run_id)
        fallback_key = pipeline_manifest_path("layer1_5_regime", f"{run_id}-{date_text}")
        payload, resolved_key = _read_first_available_json_object(
            writer,
            (primary_key, fallback_key),
        )
        if payload is None:
            load_warnings.append(
                {
                    "scope": "hmm_evaluation_context",
                    "date": date_text,
                    "key": primary_key,
                    "fallback_key": fallback_key,
                    "tried_keys": [primary_key, fallback_key],
                    "message": "Missing Layer 1.5 HMM regime manifest for this date.",
                }
            )
            by_date[date_text] = {"manifest_key": None, "metadata": {}}
            continue
        if resolved_key is not None and resolved_key not in artifact_keys["regime_manifests"]:
            artifact_keys["regime_manifests"].append(resolved_key)
        if resolved_key is not None and resolved_key not in seen_keys:
            manifests.append(_manifest_summary(payload, resolved_key))
            seen_keys.add(resolved_key)
        by_date[date_text] = {
            "manifest_key": resolved_key,
            "metadata": _json_mapping(payload.get("metadata")),
            "status": _optional_str(payload.get("status")),
            "output_path": _optional_str(payload.get("output_path")),
            "input_path": _optional_str(payload.get("input_path")),
            "run_id": _optional_str(payload.get("run_id")),
        }
    return {
        "by_date": by_date,
        "manifests": manifests,
        "source_manifest_keys": sorted(seen_keys),
    }


def _empty_regime_manifest_context(dates: Sequence[str]) -> dict[str, object]:
    """Return an empty manifest-context shape for cached report fallbacks."""
    return {
        "by_date": {date_text: {"manifest_key": None, "metadata": {}} for date_text in dates},
        "manifests": [],
        "source_manifest_keys": [],
    }


def _read_first_available_json_object(
    writer: R2Writer,
    keys: Sequence[str],
) -> tuple[dict[str, object] | None, str | None]:
    """Return the first readable JSON object for one of the provided keys."""
    for key in keys:
        try:
            payload = json.loads(writer.get_object(key).decode("utf-8"))
        except FileNotFoundError:
            continue
        if isinstance(payload, Mapping):
            return {str(item_key): _json_safe(item) for item_key, item in payload.items()}, key
        return {}, key
    return None, None


def _manifest_summary(payload: Mapping[str, object], key: str) -> dict[str, object]:
    """Return manifest fields relevant to HMM review evidence."""
    metadata = _json_mapping(payload.get("metadata"))
    return {
        "manifest_key": key,
        "run_id": _optional_str(payload.get("run_id")),
        "stage": _optional_str(payload.get("stage")),
        "status": _optional_str(payload.get("status")),
        "input_path": _optional_str(payload.get("input_path")),
        "output_path": _optional_str(payload.get("output_path")),
        "train_start_date": _optional_str(metadata.get("train_start_date")),
        "train_end_date": _optional_str(metadata.get("train_end_date")),
        "macro_load_start_date": _optional_str(metadata.get("macro_load_start_date")),
        "macro_load_end_date": _optional_str(metadata.get("macro_load_end_date")),
        "inference_dates": _json_string_list(metadata.get("inference_dates")),
        "ready_inference_dates": _json_string_list(metadata.get("ready_inference_dates")),
        "warning_inference_dates": _json_string_list(metadata.get("warning_inference_dates")),
        "training_rows": _maybe_int(metadata.get("training_rows")),
        "complete_training_rows": _maybe_int(metadata.get("complete_training_rows")),
        "dropped_feature_columns": _json_string_list(metadata.get("dropped_feature_columns")),
        "regime_layer2_ready": _maybe_bool(metadata.get("regime_layer2_ready")),
    }


def _enrich_regime_rows_with_manifests(
    regime_map: Mapping[str, SemanticReviewRegimeRow],
    manifest_context: Mapping[str, object],
) -> None:
    """Attach source manifest keys and missing-feature metadata to regime rows."""
    by_date = manifest_context.get("by_date")
    if not isinstance(by_date, Mapping):
        return
    for date_text, row in regime_map.items():
        context = by_date.get(date_text)
        if not isinstance(context, Mapping):
            continue
        object.__setattr__(row, "manifest_key", _optional_str(context.get("manifest_key")))
        metadata = _json_mapping(context.get("metadata"))
        readiness_by_date = _json_mapping(metadata.get("regime_readiness_by_date"))
        readiness = _json_mapping(readiness_by_date.get(date_text))
        if readiness and row.readiness_status is None:
            object.__setattr__(row, "readiness_status", _optional_str(readiness.get("status")))
            object.__setattr__(row, "readiness_reason", _optional_str(readiness.get("reason")))
            object.__setattr__(
                row,
                "required_for_layer2",
                _maybe_bool(readiness.get("required_for_layer2")),
            )
            object.__setattr__(
                row,
                "missing_features",
                _json_string_list(readiness.get("missing_features")),
            )
            object.__setattr__(row, "probability_sum", _maybe_float(readiness.get("probability_sum")))


def _append_market_context_warnings(
    *,
    dates: Sequence[str],
    price_rows: Sequence[SemanticReviewPriceRow],
    regime_map: Mapping[str, SemanticReviewRegimeRow],
    load_warnings: list[dict[str, object]],
) -> None:
    """Add explicit warnings for missing or all-null price/HMM review evidence."""
    price_dates = {row.date for row in price_rows}
    for date_text in dates:
        if date_text not in price_dates:
            load_warnings.append(
                {
                    "scope": "price_series",
                    "date": date_text,
                    "message": "No raw price row is available for this review date.",
                }
            )
        regime = regime_map.get(date_text)
        if regime is None:
            load_warnings.append(
                {
                    "scope": "hmm_regime",
                    "date": date_text,
                    "message": "No HMM regime row is available for this review date.",
                }
            )
            continue
        if all(
            value is None
            for value in (
                regime.regime,
                regime.confidence,
                regime.prob_bear,
                regime.prob_sideways,
                regime.prob_bull,
            )
        ):
            load_warnings.append(
                {
                    "scope": "hmm_regime",
                    "date": date_text,
                    "artifact_key": regime.artifact_key,
                    "manifest_key": regime.manifest_key,
                    "message": "HMM regime row is present but all label/probability fields are null.",
                }
            )


def _build_market_regime_rows(
    *,
    dates: Sequence[str],
    price_rows: Sequence[SemanticReviewPriceRow],
    regime_map: Mapping[str, SemanticReviewRegimeRow],
) -> list[dict[str, object]]:
    """Return date-aligned price and HMM regime rows for chart/API consumers."""
    price_by_date = {row.date: row.to_dict() for row in price_rows}
    rows: list[dict[str, object]] = []
    for date_text in dates:
        price = price_by_date.get(date_text)
        regime = regime_map.get(date_text)
        warnings: list[str] = []
        if price is None:
            warnings.append("missing_price")
        if regime is None:
            warnings.append("missing_hmm_regime")
            regime_payload = None
        else:
            regime_payload = regime.to_dict()
            if all(
                regime_payload.get(field) is None
                for field in ("regime", "confidence", "prob_bear", "prob_sideways", "prob_bull")
            ):
                warnings.append("all_null_hmm_regime")
            if regime.manifest_key is None:
                warnings.append("missing_hmm_manifest")
            if regime.missing_features:
                warnings.append("incomplete_hmm_feature_set")
        rows.append(
            {
                "date": date_text,
                "ticker": price.get("ticker") if price else None,
                "price": price,
                "hmm_regime": regime_payload,
                "warnings": warnings,
                "scope": "date-aligned-price-and-hmm",
            }
        )
    return rows


def _build_hmm_evaluation_context(
    *,
    dates: Sequence[str],
    regime_map: Mapping[str, SemanticReviewRegimeRow],
    manifest_context: Mapping[str, object],
    artifact_keys: Mapping[str, Sequence[str]],
) -> dict[str, object]:
    """Return explicit HMM evaluation scope and source metadata for review."""
    manifests = [
        dict(item)
        for item in manifest_context.get("manifests", [])
        if isinstance(item, Mapping)
    ]
    observed_dates = sorted(regime_map)
    source_manifest_keys = [
        str(item)
        for item in manifest_context.get("source_manifest_keys", [])
        if _optional_str(item) is not None
    ]
    by_date = manifest_context.get("by_date")
    not_evaluated_dates: list[str] = []
    stale_manifest_dates: list[str] = []
    if isinstance(by_date, Mapping):
        for date_text in dates:
            context = by_date.get(date_text)
            if not isinstance(context, Mapping):
                continue
            status = (_optional_str(context.get("status")) or "").lower()
            if status and status != "completed":
                stale_manifest_dates.append(date_text)
            metadata = _json_mapping(context.get("metadata"))
            inference_dates = _json_string_list(metadata.get("inference_dates"))
            if inference_dates and date_text not in set(inference_dates):
                not_evaluated_dates.append(date_text)
    dropped_columns = sorted(
        {
            column
            for manifest in manifests
            for column in _json_string_list(manifest.get("dropped_feature_columns"))
        }
    )
    expected_columns = list(HMM_TRAINING_FEATURE_COLUMNS)
    active_columns = [column for column in expected_columns if column not in set(dropped_columns)]
    warnings: list[str] = []
    if not source_manifest_keys:
        warnings.append("missing_hmm_manifest")
    missing_dates = sorted(set(dates) - set(observed_dates))
    if missing_dates:
        warnings.append("missing_hmm_inference_dates")
    unexpected_dates = sorted(set(observed_dates) - set(dates))
    if unexpected_dates:
        warnings.append("unexpected_hmm_inference_dates")
    if not_evaluated_dates:
        warnings.append("hmm_not_evaluated_for_date")
    if stale_manifest_dates:
        warnings.append("stale_hmm_manifest")
    if dropped_columns:
        warnings.append("incomplete_hmm_feature_set")
    if not any(_optional_str(manifest.get("train_end_date")) for manifest in manifests):
        warnings.append("missing_training_window_metadata")
    return {
        "scope": "market-wide-date-level-hmm",
        "applies_to": "all tickers and sentence rows for each inference date",
        "expected_input_feature_columns": expected_columns,
        "input_feature_columns_used": active_columns,
        "dropped_feature_columns": dropped_columns,
        "requested_inference_dates": list(dates),
        "observed_inference_dates": observed_dates,
        "missing_inference_dates": missing_dates,
        "unexpected_inference_dates": unexpected_dates,
        "not_evaluated_dates": not_evaluated_dates,
        "stale_manifest_dates": stale_manifest_dates,
        "training_windows": _training_windows_from_manifests(manifests),
        "source_artifact_keys": list(artifact_keys.get("regime", [])),
        "source_manifest_keys": source_manifest_keys,
        "manifest_summaries": manifests,
        "warnings": warnings,
    }


def _benchmark_ticker_from_manifest_context(manifest_context: Mapping[str, object]) -> str:
    """Return the benchmark ticker to use for the market-level HMM chart."""
    manifests = manifest_context.get("manifests")
    if isinstance(manifests, Sequence):
        for manifest in manifests:
            if not isinstance(manifest, Mapping):
                continue
            benchmark_ticker = _optional_str(manifest.get("benchmark_ticker"))
            if benchmark_ticker:
                return benchmark_ticker.upper()
    return "SPY"


def _training_windows_from_manifests(
    manifests: Sequence[Mapping[str, object]],
) -> list[dict[str, object]]:
    """Return de-duplicated HMM training/lookback windows from manifest summaries."""
    windows: list[dict[str, object]] = []
    seen: set[tuple[object, ...]] = set()
    for manifest in manifests:
        window = {
            "train_start_date": _optional_str(manifest.get("train_start_date")),
            "train_end_date": _optional_str(manifest.get("train_end_date")),
            "macro_load_start_date": _optional_str(manifest.get("macro_load_start_date")),
            "macro_load_end_date": _optional_str(manifest.get("macro_load_end_date")),
            "training_rows": _maybe_int(manifest.get("training_rows")),
            "complete_training_rows": _maybe_int(manifest.get("complete_training_rows")),
        }
        key = tuple(window.values())
        if key in seen:
            continue
        seen.add(key)
        windows.append(window)
    return windows


def _load_date_partitioned_artifacts(
    *,
    writer: R2Writer,
    dates: Sequence[str],
    run_id: str,
    path_builder: Any,
    artifact_name: str,
    artifact_keys: dict[str, list[str]],
    load_warnings: list[dict[str, object]],
) -> pd.DataFrame:
    """Load date-partitioned Layer 1 parquet artifacts with dated-run fallback."""
    frames: list[pd.DataFrame] = []
    for date_text in dates:
        primary_key = path_builder(date_text, run_id)
        fallback_key = path_builder(date_text, f"{run_id}-{date_text}")
        frame, resolved_key = _read_first_available_parquet_frame(
            writer,
            (primary_key, fallback_key),
        )
        if frame is None:
            load_warnings.append(
                {
                    "scope": artifact_name,
                    "date": date_text,
                    "key": primary_key,
                    "fallback_key": fallback_key,
                    "tried_keys": [primary_key, fallback_key],
                    "message": f"Missing {artifact_name} parquet for this trading date.",
                }
            )
            continue
        if frame.empty:
            load_warnings.append(
                {
                    "scope": artifact_name,
                    "date": date_text,
                    "key": resolved_key,
                    "fallback_key": fallback_key if resolved_key == primary_key else primary_key,
                    "tried_keys": [primary_key, fallback_key],
                    "message": f"{artifact_name} parquet is empty.",
                }
            )
            continue
        if resolved_key is not None:
            artifact_keys[artifact_name].append(resolved_key)
            frame = frame.copy()
            frame["_artifact_key"] = resolved_key
        frames.append(frame)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def _preprocessing_rows(
    frame: pd.DataFrame,
    *,
    requested_ticker: str,
) -> list[dict[str, object]]:
    """Return normalized pre-FinBERT preprocessing rows for dashboard review."""
    if frame.empty:
        return []
    normalized = frame.copy()
    normalized.columns = [str(column) for column in normalized.columns]
    rows: list[dict[str, object]] = []
    for _, row in normalized.iterrows():
        ticker_mentions = _json_string_list(row.get("ticker_mentions"))
        entity_mentions = _json_string_list(row.get("entity_mentions"))
        provenance = _json_mapping(row.get("source_text_provenance"))
        flags: list[str] = []
        if requested_ticker not in {value.upper() for value in ticker_mentions}:
            flags.append("missing_requested_ticker_mention")
        if not entity_mentions:
            flags.append("missing_entity_mentions")
        if not provenance:
            flags.append("missing_source_text_provenance")
        rows.append(
            {
                "date": _optional_str(row.get("date")),
                "ticker": _optional_str(row.get("ticker")),
                "article_id": _optional_str(row.get("article_id")),
                "headline": _optional_str(row.get("headline")),
                "normalized_headline": _optional_str(row.get("normalized_headline")),
                "text": _optional_str(row.get("text")),
                "sentence_index": _maybe_int(row.get("sentence_index")),
                "chunk_index": _maybe_int(row.get("chunk_index")),
                "source": _optional_str(row.get("source")),
                "url": _optional_str(row.get("url")),
                "published_at": _optional_str(row.get("published_at")),
                "source_text_field": _optional_str(row.get("source_text_field")),
                "source_text_order": _maybe_int(row.get("source_text_order")),
                "source_text_provenance": provenance,
                "ticker_mentions": ticker_mentions,
                "entity_mentions": entity_mentions,
                "missing_evidence_flags": flags,
                "artifact_key": _optional_str(row.get("_artifact_key")),
                "stage": "ticker_entity_preprocessing",
                "row_granularity": "sentence-or-chunk",
            }
        )
    return _sort_evidence_rows(rows)


def _embedding_rows(frame: pd.DataFrame) -> list[dict[str, object]]:
    """Return normalized article embedding cache rows for dashboard review."""
    if frame.empty:
        return []
    normalized = frame.copy()
    normalized.columns = [str(column) for column in normalized.columns]
    rows: list[dict[str, object]] = []
    for _, row in normalized.iterrows():
        embedding_vector = _json_list(row.get("embedding_json"))
        rows.append(
            {
                "date": _optional_str(row.get("date")),
                "article_id": _optional_str(row.get("article_id")),
                "normalized_headline": _optional_str(row.get("normalized_headline")),
                "article_sentence_count": _maybe_int(row.get("article_sentence_count")),
                "embedding_model": _optional_str(row.get("embedding_model")),
                "embedding_revision": _optional_str(row.get("embedding_revision")),
                "embedding_cache_key": _optional_str(row.get("embedding_cache_key")),
                "embedding_dimension": len(embedding_vector),
                "artifact_key": _optional_str(row.get("_artifact_key")),
                "stage": "article_embeddings",
            }
        )
    return _sort_evidence_rows(rows)


def _topic_label_rows(
    frame: pd.DataFrame,
    *,
    requested_ticker: str,
) -> list[dict[str, object]]:
    """Return normalized topic-label evidence rows for dashboard review."""
    if frame.empty:
        return []
    normalized = frame.copy()
    normalized.columns = [str(column) for column in normalized.columns]
    rows: list[dict[str, object]] = []
    for _, row in normalized.iterrows():
        ticker = (_optional_str(row.get("ticker")) or "").upper()
        if ticker and ticker != requested_ticker:
            continue
        rows.append(
            {
                "date": _optional_str(row.get("date")),
                "ticker": ticker or requested_ticker,
                "article_id": _optional_str(row.get("article_id")),
                "normalized_headline": _optional_str(row.get("normalized_headline")),
                "article_sentence_count": _maybe_int(row.get("article_sentence_count")),
                "embedding_cache_key": _optional_str(row.get("embedding_cache_key")),
                "topic_model": _optional_str(row.get("topic_model")),
                "topic_model_version": _optional_str(row.get("topic_model_version")),
                "topic_id": _maybe_int(row.get("topic_id")),
                "topic_probability": _maybe_float(row.get("topic_probability")),
                "topic_label": _optional_str(row.get("topic_label")),
                "topic_keywords": _json_string_list(row.get("topic_keywords")),
                "artifact_key": _optional_str(row.get("_artifact_key")),
                "stage": "article_topic_labels",
            }
        )
    return _sort_evidence_rows(rows)


def _relevance_gate_rows(
    frame: pd.DataFrame,
    *,
    requested_ticker: str,
) -> list[dict[str, object]]:
    """Return normalized relevance-gate audit rows for dashboard review."""
    if frame.empty:
        return []
    normalized = frame.copy()
    normalized.columns = [str(column) for column in normalized.columns]
    rows: list[dict[str, object]] = []
    for _, row in normalized.iterrows():
        ticker = (_optional_str(row.get("ticker")) or "").upper()
        if ticker and ticker != requested_ticker:
            continue
        rows.append(
            {
                "date": _optional_str(row.get("date")),
                "ticker": ticker or requested_ticker,
                "article_id": _optional_str(row.get("article_id")),
                "sentence_index": _maybe_int(row.get("sentence_index")),
                "chunk_index": _maybe_int(row.get("chunk_index")),
                "headline": _optional_str(row.get("headline")),
                "text": _optional_str(row.get("text")),
                "source": _optional_str(row.get("source")),
                "published_at": _optional_str(row.get("published_at")),
                "relevance_decision": _optional_str(row.get("relevance_decision")),
                "relevance_score": _maybe_float(row.get("relevance_score")),
                "ticker_relevance_score": _maybe_float(row.get("ticker_relevance_score")),
                "financial_relevance_score": _maybe_float(row.get("financial_relevance_score")),
                "topic_relevance_score": _maybe_float(row.get("topic_relevance_score")),
                "reason_codes": _json_string_list(row.get("reason_codes")),
                "ticker_evidence": _json_mapping(row.get("ticker_evidence")),
                "entity_evidence": _json_string_list(row.get("entity_evidence")),
                "topic_id": _maybe_int(row.get("topic_id")),
                "topic_probability": _maybe_float(row.get("topic_probability")),
                "embedding_cache_key": _optional_str(row.get("embedding_cache_key")),
                "has_embedding": _maybe_bool(row.get("has_embedding")),
                "artifact_key": _optional_str(row.get("_artifact_key")),
                "stage": "pre_finbert_relevance_gate",
            }
        )
    return _sort_evidence_rows(rows)


def _semantic_aggregate_rows(
    frame: pd.DataFrame,
    *,
    requested_ticker: str,
) -> list[dict[str, object]]:
    """Return normalized ticker-date semantic aggregate FeatureRecord rows."""
    if frame.empty:
        return []
    normalized = frame.copy()
    normalized.columns = [str(column) for column in normalized.columns]
    rows: list[dict[str, object]] = []
    for _, row in normalized.iterrows():
        ticker = (_optional_str(row.get("ticker")) or "").upper()
        if ticker and ticker != requested_ticker:
            continue
        features = _json_mapping(row.get("features"))
        rows.append(
            {
                "date": _optional_str(row.get("date")),
                "ticker": ticker or requested_ticker,
                "features": features,
                "source_weight_summary": _json_list(
                    features.get("nlp_source_weight_summary")
                ),
                "topic_sentiment_summary": _json_list(
                    features.get("nlp_topic_sentiment_summary")
                ),
                "relevance_reason_codes": _json_string_list(
                    features.get("nlp_relevance_reason_codes")
                ),
                "semantic_warning_codes": _json_string_list(
                    features.get("nlp_semantic_warning_codes")
                ),
                "contributing_article_ids": _json_string_list(
                    features.get("nlp_contributing_article_ids")
                ),
                "artifact_key": _optional_str(row.get("_artifact_key")),
                "stage": "source_weighted_semantic_aggregation",
                "row_granularity": "ticker-date",
            }
        )
    return _sort_evidence_rows(rows)


def _rows_by_article(
    rows: Sequence[Mapping[str, object]],
) -> dict[tuple[str, str], list[dict[str, object]]]:
    """Group normalized evidence rows by date and article id."""
    grouped: dict[tuple[str, str], list[dict[str, object]]] = defaultdict(list)
    for row in rows:
        date_text = _optional_str(row.get("date"))
        article_id = _optional_str(row.get("article_id"))
        if date_text and article_id:
            grouped[(date_text, article_id)].append(dict(row))
    return grouped


def _sort_evidence_rows(rows: Sequence[Mapping[str, object]]) -> list[dict[str, object]]:
    """Return evidence rows in deterministic review order."""
    return sorted(
        (dict(row) for row in rows),
        key=lambda row: (
            str(row.get("date") or ""),
            str(row.get("article_id") or ""),
            _maybe_int(row.get("sentence_index")) if row.get("sentence_index") is not None else -1,
            _maybe_int(row.get("chunk_index")) if row.get("chunk_index") is not None else -1,
        ),
    )


def _load_cached_aapl_pilot_evidence_frames(
    *,
    run_id: str,
    from_date: str,
    to_date: str,
    ticker: str,
) -> tuple[pd.DataFrame, pd.DataFrame] | None:
    """Load a cached AAPL pilot evidence bundle when raw stage artifacts are unavailable."""
    bundle_path = _find_cached_aapl_pilot_evidence_bundle_path(run_id)
    if bundle_path is None:
        return None
    try:
        bundle = json.loads(bundle_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None

    raw_rows = bundle.get("human_review_rows")
    if not isinstance(raw_rows, list) or not raw_rows:
        return None

    raw_frame = pd.DataFrame(raw_rows)
    if raw_frame.empty or "date" not in raw_frame.columns:
        return None

    requested_ticker = ticker.strip().upper()
    expected_dates = set(trading_dates(from_date, to_date))
    filtered = raw_frame.copy()
    filtered["date"] = filtered["date"].map(lambda value: _optional_str(value) or "")
    filtered = filtered[filtered["date"].isin(expected_dates)]
    if "ticker" in filtered.columns:
        filtered["ticker"] = filtered["ticker"].map(lambda value: _optional_str(value) or "")
        filtered = filtered[filtered["ticker"].str.upper() == requested_ticker]
    if filtered.empty:
        return None

    filtered = filtered.reset_index(drop=True)
    sentence_index = filtered.groupby(["date", "raw_article_id"], sort=False).cumcount()
    scored_frame = pd.DataFrame(
        {
            "date": filtered["date"],
            "ticker": filtered.get("ticker", requested_ticker),
            "headline": filtered.get("raw_headline"),
            "text": filtered.get("raw_snippet"),
            "article_id": filtered.get("raw_article_id"),
            "source": filtered.get("raw_source"),
            "url": None,
            "published_at": filtered.get("raw_published_at"),
            "sentence_index": sentence_index,
            "sentiment_score": filtered.get("finbert_score"),
            "positive_probability": filtered.get("finbert_positive"),
            "negative_probability": filtered.get("finbert_negative"),
            "neutral_probability": filtered.get("finbert_neutral"),
            "relevance_score": filtered.get("finbert_relevance"),
        }
    )
    regime_columns = {
        "date": filtered["date"],
        "regime": filtered["regime_label"] if "regime_label" in filtered.columns else filtered.get("regime"),
        "confidence": (
            filtered["regime_confidence"] if "regime_confidence" in filtered.columns else filtered.get("confidence")
        ),
        "prob_bear": (
            filtered["regime_prob_bear"] if "regime_prob_bear" in filtered.columns else filtered.get("prob_bear")
        ),
        "prob_sideways": (
            filtered["regime_prob_sideways"]
            if "regime_prob_sideways" in filtered.columns
            else filtered.get("prob_sideways")
        ),
        "prob_bull": (
            filtered["regime_prob_bull"] if "regime_prob_bull" in filtered.columns else filtered.get("prob_bull")
        ),
    }
    regime_frame = pd.DataFrame(regime_columns).drop_duplicates(subset=["date"])
    return scored_frame, regime_frame


def _find_cached_aapl_pilot_evidence_bundle_path(run_id: str) -> Path | None:
    """Return the most recent cached AAPL pilot evidence bundle for a run, if one exists."""
    filename = f"aapl_pilot_evidence_{run_id}.json"
    direct_path = DEFAULT_AAPL_PILOT_EVIDENCE_OUTPUT_DIR / filename
    if direct_path.exists():
        return direct_path

    profiles_root = Path.home() / ".hermes" / "profiles"
    if profiles_root.exists():
        candidates = [path for path in profiles_root.rglob(filename) if path.is_file()]
        if candidates:
            return max(candidates, key=lambda path: path.stat().st_mtime)
    return None


def build_layer1_semantic_review_dashboard_smoke_result(
    payload: Mapping[str, object],
) -> dict[str, object]:
    """Return machine-checkable readiness for the rendered dashboard smoke."""
    summary = _json_mapping(payload.get("summary"))
    warnings = [dict(item) for item in payload.get("warnings", []) if isinstance(item, Mapping)]
    artifact_keys = _json_mapping(payload.get("artifact_keys"))
    pipeline_sections = _json_mapping(payload.get("pipeline_sections"))
    hmm_context = _json_mapping(payload.get("hmm_evaluation_context"))
    benchmark_ticker = _optional_str(payload.get("benchmark_ticker")) or "SPY"
    failures: list[dict[str, object]] = []

    required_sections = (
        ("news_preprocessing", "raw_preprocessing_rows", "news_preprocessing"),
        ("text_embeddings", "article_embedding_rows", "text_embeddings"),
        ("topic_labels", "topic_label_rows", "topic_labels"),
        ("news_relevance_gate", "relevance_gate_rows", "news_relevance_gate"),
        ("news_sentiment_scored", "finbert_sentence_rows", "news_sentiment_scored"),
        ("sentiment_features", "semantic_aggregate_rows", "sentiment_features"),
        ("hmm_regime", "date_level_regime_rows", "regime"),
        ("raw_price_context", "stock_price_rows", "raw_prices"),
    )
    for stage_name, section_key, artifact_key_name in required_sections:
        rows = pipeline_sections.get(section_key)
        if not isinstance(rows, list) or not rows:
            failures.append(
                _dashboard_smoke_failure(
                    stage=stage_name,
                    reason="empty_rows",
                    warnings=warnings,
                    artifact_key_name=artifact_key_name,
                    artifact_keys=artifact_keys,
                )
            )
        elif _warning_keys_for_scopes(warnings, _warning_scopes_for_stage(stage_name)):
            failures.append(
                _dashboard_smoke_failure(
                    stage=stage_name,
                    reason="missing_or_incomplete_artifacts",
                    warnings=warnings,
                    artifact_key_name=artifact_key_name,
                    artifact_keys=artifact_keys,
                )
            )

    if any(str(item.get("scope")) == "cached_bundle" for item in warnings):
        failures.append(
            {
                "stage": "cached_bundle",
                "reason": "cached_bundle_fallback",
                "message": (
                    "Dashboard loaded cached AAPL evidence; final acceptance requires raw "
                    "stage artifacts."
                ),
                "missing_or_tried_keys": _warning_keys_for_scopes(
                    warnings,
                    {
                        "sentence_rows",
                        "regime",
                        "hmm_evaluation_context",
                        "news_preprocessing",
                        "text_embeddings",
                        "topic_labels",
                        "news_relevance_gate",
                        "sentiment_features",
                        "price_series",
                    },
                ),
            }
        )

    benchmark_prices = [
        dict(item)
        for item in payload.get("benchmark_price_series", [])
        if isinstance(item, Mapping)
    ]
    benchmark_rows = [
        dict(item)
        for item in payload.get("benchmark_market_regime_series", [])
        if isinstance(item, Mapping)
    ]
    if not benchmark_prices:
        failures.append(
            _dashboard_smoke_failure(
                stage="benchmark_price_context",
                reason="empty_benchmark_price_rows",
                warnings=warnings,
                artifact_key_name="raw_prices",
                artifact_keys=artifact_keys,
            )
        )
    if not benchmark_rows:
        failures.append(
            {
                "stage": "benchmark_hmm_chart",
                "reason": "empty_benchmark_hmm_rows",
                "message": "No date-aligned benchmark/HMM rows are available for the chart.",
                "missing_or_tried_keys": _warning_keys_for_scopes(
                    warnings,
                    {"hmm_regime", "hmm_evaluation_context", "price_series"},
                ),
            }
        )
    if benchmark_rows and not any(_row_has_renderable_price(row) for row in benchmark_rows):
        failures.append(
            {
                "stage": "benchmark_hmm_chart",
                "reason": "no_renderable_benchmark_prices",
                "message": f"{benchmark_ticker} benchmark rows have no numeric close/adj_close.",
                "missing_or_tried_keys": _warning_keys_for_scopes(warnings, {"price_series"}),
            }
        )
    if benchmark_rows and not any(_row_has_renderable_probabilities(row) for row in benchmark_rows):
        failures.append(
            {
                "stage": "benchmark_hmm_chart",
                "reason": "no_renderable_hmm_probabilities",
                "message": "HMM chart rows have no numeric bear/sideways/bull probabilities.",
                "missing_or_tried_keys": _warning_keys_for_scopes(
                    warnings,
                    {"hmm_regime", "hmm_evaluation_context"},
                ),
            }
        )

    if not _json_string_list(hmm_context.get("source_manifest_keys")):
        failures.append(
            {
                "stage": "hmm_manifest",
                "reason": "missing_hmm_manifest",
                "message": "HMM manifest key is missing from the dashboard payload.",
                "missing_or_tried_keys": _warning_keys_for_scopes(
                    warnings,
                    {"hmm_evaluation_context"},
                ),
            }
        )
    training_windows = hmm_context.get("training_windows")
    if not isinstance(training_windows, list) or not training_windows:
        failures.append(
            {
                "stage": "hmm_manifest",
                "reason": "missing_training_window_metadata",
                "message": "HMM training-window metadata is missing.",
                "missing_or_tried_keys": _warning_keys_for_scopes(
                    warnings,
                    {"hmm_evaluation_context"},
                ),
            }
        )
    hmm_warnings = set(_json_string_list(hmm_context.get("warnings")))
    blocker_warnings = {
        "missing_hmm_manifest",
        "missing_hmm_inference_dates",
        "hmm_not_evaluated_for_date",
        "stale_hmm_manifest",
        "incomplete_hmm_feature_set",
        "missing_training_window_metadata",
    }
    if hmm_warnings & blocker_warnings:
        failures.append(
            {
                "stage": "hmm_evaluation_context",
                "reason": "hmm_context_blocker_warnings",
                "message": "HMM context has blocker warnings.",
                "warning_codes": sorted(hmm_warnings & blocker_warnings),
                "missing_or_tried_keys": _warning_keys_for_scopes(
                    warnings,
                    {"hmm_evaluation_context", "hmm_regime"},
                ),
            }
        )

    status = "pass" if not failures else "fail"
    return {
        "status": status,
        "ready_for_final_human_acceptance": status == "pass",
        "required_stage_row_counts": {
            "news_preprocessing": int(summary.get("preprocessing_row_count") or 0),
            "text_embeddings": int(summary.get("embedding_row_count") or 0),
            "topic_labels": int(summary.get("topic_label_row_count") or 0),
            "news_relevance_gate": int(summary.get("relevance_gate_row_count") or 0),
            "news_sentiment_scored": int(summary.get("row_count") or 0),
            "sentiment_features": int(summary.get("semantic_aggregate_row_count") or 0),
            "hmm_regime": int(summary.get("hmm_regime_row_count") or 0),
            "stock_price_context": int(summary.get("price_row_count") or 0),
            "benchmark_price_context": len(benchmark_prices),
            "benchmark_price_hmm_context": len(benchmark_rows),
        },
        "benchmark_ticker": benchmark_ticker,
        "visual_browser_qa_required": True,
        "visual_browser_qa_assertions": [
            "Rendered page contains an SVG benchmark chart rather than the blocker card.",
            f"{benchmark_ticker} benchmark close/adj_close values are numeric.",
            "HMM bear, sideways, and bull probabilities are numeric and visible in the chart.",
            "HMM manifest and training-window metadata are present.",
        ],
        "failures": failures,
    }


def _dashboard_smoke_failure(
    *,
    stage: str,
    reason: str,
    warnings: Sequence[Mapping[str, object]],
    artifact_key_name: str,
    artifact_keys: Mapping[str, object],
) -> dict[str, object]:
    """Return a normalized dashboard smoke failure with repair keys."""
    warning_scopes = _warning_scopes_for_stage(stage)
    if reason == "missing_or_incomplete_artifacts":
        message = f"{stage} has missing or incomplete raw artifacts for the review window."
    else:
        message = f"{stage} did not provide nonzero rows for dashboard smoke."
    return {
        "stage": stage,
        "reason": reason,
        "message": message,
        "resolved_artifact_keys": _json_string_list(artifact_keys.get(artifact_key_name)),
        "missing_or_tried_keys": _warning_keys_for_scopes(warnings, warning_scopes),
    }


def _warning_scopes_for_stage(stage: str) -> set[str]:
    """Return load-warning scopes that can explain a dashboard smoke stage failure."""
    scopes_by_stage = {
        "news_preprocessing": {"news_preprocessing"},
        "text_embeddings": {"text_embeddings"},
        "topic_labels": {"topic_labels"},
        "news_relevance_gate": {"news_relevance_gate"},
        "news_sentiment_scored": {"sentence_rows"},
        "sentiment_features": {"sentiment_features"},
        "hmm_regime": {"regime", "hmm_regime", "hmm_evaluation_context"},
        "raw_price_context": {"price_series"},
        "benchmark_price_context": {"price_series"},
    }
    return scopes_by_stage.get(stage, {stage})


def _warning_keys_for_scopes(
    warnings: Sequence[Mapping[str, object]],
    scopes: set[str],
) -> list[str]:
    """Return exact missing or attempted keys reported for warning scopes."""
    keys: list[str] = []
    for warning in warnings:
        if str(warning.get("scope")) not in scopes:
            continue
        for field_name in ("key", "fallback_key", "artifact_key", "manifest_key"):
            value = _optional_str(warning.get(field_name))
            if value:
                keys.append(value)
        tried_keys = warning.get("tried_keys")
        if isinstance(tried_keys, Sequence) and not isinstance(tried_keys, str):
            keys.extend(str(item) for item in tried_keys if _optional_str(item) is not None)
        raw_warnings = warning.get("raw_lookup_warnings")
        if isinstance(raw_warnings, Sequence) and not isinstance(raw_warnings, str):
            keys.extend(
                _warning_keys_for_scopes(
                    [item for item in raw_warnings if isinstance(item, Mapping)],
                    scopes,
                )
            )
    return _dedupe_preserve_order(keys)


def _row_has_renderable_price(row: Mapping[str, object]) -> bool:
    """Return whether a date-aligned chart row has a numeric price."""
    price = row.get("price")
    if not isinstance(price, Mapping):
        return False
    return _maybe_float(price.get("adj_close")) is not None or _maybe_float(price.get("close")) is not None


def _row_has_renderable_probabilities(row: Mapping[str, object]) -> bool:
    """Return whether a date-aligned chart row has numeric HMM probabilities."""
    regime = row.get("hmm_regime")
    if not isinstance(regime, Mapping):
        return False
    probabilities = (
        _maybe_float(regime.get("prob_bear")),
        _maybe_float(regime.get("prob_sideways")),
        _maybe_float(regime.get("prob_bull")),
    )
    return any(value is not None for value in probabilities)


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
    benchmark_price_rows = report_dict.get("benchmark_price_rows")
    benchmark_market_regime_rows = report_dict.get("benchmark_market_regime_rows")
    payload = {
        "title": "Layer 1 semantic review dashboard",
        "description": (
            "Beginner-friendly review of whether the Layer 1 Apple news signal, "
            "the market benchmark, and the market-regime evidence look trustworthy."
        ),
        "human_semantic_review_status": "needs_human_review",
        "recommendation_for_issue_202": "needs_human_review",
        "report": report_dict,
        "summary": dict(report_dict.get("summary", {})),
        "controls": {
            "ticker": report_dict.get("ticker"),
            "run_id": report_dict.get("run_id"),
            "from_date": report_dict.get("from_date"),
            "to_date": report_dict.get("to_date"),
        },
        "date_groups": date_groups,
        "price_series": list(report_dict.get("price_rows", [])),
        "market_regime_series": list(report_dict.get("market_regime_rows", [])),
        "benchmark_ticker": report_dict.get("benchmark_ticker"),
        "benchmark_price_series": benchmark_price_rows if isinstance(benchmark_price_rows, list) else [],
        "benchmark_market_regime_series": (
            benchmark_market_regime_rows if isinstance(benchmark_market_regime_rows, list) else []
        ),
        "hmm_evaluation_context": dict(report_dict.get("hmm_evaluation_context", {})),
        "article_groups": article_groups,
        "accepted_articles": accepted_articles,
        "flagged_articles": flagged_articles,
        "pipeline_sections": {
            "raw_preprocessing_rows": list(report_dict.get("preprocessing_rows", [])),
            "article_embedding_rows": list(report_dict.get("embedding_rows", [])),
            "topic_label_rows": list(report_dict.get("topic_label_rows", [])),
            "relevance_gate_rows": list(report_dict.get("relevance_gate_rows", [])),
            "finbert_sentence_rows": [
                row
                for article in article_groups
                for row in list(article.get("sentence_rows", []))
            ],
            "semantic_aggregate_rows": list(
                report_dict.get("semantic_aggregate_rows", [])
            ),
            "date_level_regime_rows": list(report_dict.get("regime_rows", [])),
            "stock_price_rows": list(report_dict.get("price_rows", [])),
            "date_aligned_price_hmm_rows": list(report_dict.get("market_regime_rows", [])),
        },
        "artifact_keys": dict(report_dict.get("artifact_keys", {})),
        "warnings": list(report_dict.get("load_warnings", [])),
    }
    payload["smoke"] = build_layer1_semantic_review_dashboard_smoke_result(payload)
    return payload


__all__ = [
    "DEFAULT_RELEVANCE_THRESHOLD",
    "Layer1SemanticReviewReport",
    "SemanticReviewArticleGroup",
    "SemanticReviewDateGroup",
    "SemanticReviewPriceRow",
    "SemanticReviewRegimeRow",
    "SemanticReviewSentenceRow",
    "build_layer1_aapl_evidence_report",
    "build_layer1_semantic_review_dashboard_smoke_result",
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


def _copy_first_existing_column(
    frame: pd.DataFrame,
    target_column: str,
    source_columns: Sequence[str],
) -> None:
    """Fill an empty normalized column from the first existing source column."""
    if target_column not in frame.columns:
        frame[target_column] = None
    if frame[target_column].notna().any():
        return
    for source_column in source_columns:
        if source_column in frame.columns:
            frame[target_column] = frame[source_column]
            return


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


def _json_mapping(value: Any) -> dict[str, object]:
    """Return a JSON object from an encoded or native mapping value."""
    decoded = _json_value(value)
    if isinstance(decoded, Mapping):
        return {str(key): _json_safe(item) for key, item in decoded.items()}
    return {}


def _json_list(value: Any) -> list[object]:
    """Return a JSON list from an encoded or native sequence value."""
    decoded = _json_value(value)
    if isinstance(decoded, Sequence) and not isinstance(decoded, (bytes, bytearray, str)):
        return [_json_safe(item) for item in decoded]
    return []


def _json_string_list(value: Any) -> list[str]:
    """Return a string list from an encoded or native sequence value."""
    decoded = _json_value(value)
    if decoded is None:
        return []
    if isinstance(decoded, str):
        text = decoded.strip()
        return [text] if text else []
    if isinstance(decoded, Sequence) and not isinstance(decoded, (bytes, bytearray, str)):
        return [str(item).strip() for item in decoded if str(item).strip()]
    return [str(decoded).strip()] if str(decoded).strip() else []


def _comma_string_list(value: Any) -> list[str]:
    """Return a clean string list from comma-delimited or JSON-list input."""
    items = _json_string_list(value)
    if len(items) != 1 or "," not in items[0]:
        return items
    return [item.strip() for item in items[0].split(",") if item.strip()]


def _json_value(value: Any) -> object:
    """Decode JSON-looking strings while preserving native objects."""
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            return text
    return value


def _json_safe(value: Any) -> object:
    """Return a JSON-safe scalar, list, or dict."""
    decoded = _json_value(value)
    if isinstance(decoded, Mapping):
        return {str(key): _json_safe(item) for key, item in decoded.items()}
    if isinstance(decoded, Sequence) and not isinstance(decoded, (bytes, bytearray, str)):
        return [_json_safe(item) for item in decoded]
    if isinstance(decoded, bool):
        return decoded
    maybe_float = _maybe_float(decoded)
    if maybe_float is not None:
        return maybe_float
    maybe_text = _optional_str(decoded)
    return maybe_text


def _maybe_bool(value: Any) -> bool | None:
    """Return a bool when the input is clearly boolean-like."""
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    text = _optional_str(value)
    if text is None:
        return None
    lowered = text.lower()
    if lowered in {"true", "1", "yes"}:
        return True
    if lowered in {"false", "0", "no"}:
        return False
    return None


def _build_payload_from_report_and_json(report_json: str) -> dict[str, object]:
    """Convenience helper for callers that already hold a JSON report string."""
    return _build_payload_from_report(json.loads(report_json))


@dataclass(frozen=True)
class IntegrityGate:
    """One machine-integrity gate result for the AAPL pilot bundle."""

    name: str
    passed: bool
    details: dict[str, object] = field(default_factory=dict)

    def to_dict(self) -> dict[str, object]:
        """Return a JSON-serializable gate payload."""
        return asdict(self)


@dataclass(frozen=True)
class HumanReviewRow:
    """One human-review row summarizing a trading date."""

    date: str
    ticker: str
    review_status: str
    raw_article_id: str | None
    raw_headline: str | None
    raw_snippet: str | None
    raw_source: str | None
    raw_published_at: str | None
    raw_news_key: str
    preprocessed_news_key: str
    finbert_scored_news_key: str
    finbert_positive: float | None
    finbert_negative: float | None
    finbert_neutral: float | None
    finbert_score: float | None
    relevance_score: float | None
    regime: str | None
    notes: str | None

    def to_dict(self) -> dict[str, object]:
        """Return a JSON-serializable row payload."""
        return asdict(self)


@dataclass(frozen=True)
class AAPLPilotEvidenceBundle:
    """Compact AAPL pilot evidence bundle used by the CLI and tests."""

    run_id: str
    ticker: str
    from_date: str
    to_date: str
    layer0_run_id: str
    layer1_run_id: str | None
    generated_at: str
    gates: list[IntegrityGate]
    machine_integrity_status: str
    human_semantic_review_status: str
    recommendation_for_issue_202: str
    human_review_rows: list[HumanReviewRow]
    artifact_keys: dict[str, object]
    report: Layer1SemanticReviewReport

    def to_dict(self) -> dict[str, object]:
        """Return a JSON-serializable bundle payload."""
        return {
            "run_id": self.run_id,
            "ticker": self.ticker,
            "from_date": self.from_date,
            "to_date": self.to_date,
            "layer0_run_id": self.layer0_run_id,
            "layer1_run_id": self.layer1_run_id,
            "generated_at": self.generated_at,
            "gates": [gate.to_dict() for gate in self.gates],
            "machine_integrity_status": self.machine_integrity_status,
            "human_semantic_review_status": self.human_semantic_review_status,
            "recommendation_for_issue_202": self.recommendation_for_issue_202,
            "human_review_rows": [row.to_dict() for row in self.human_review_rows],
            "artifact_keys": self.artifact_keys,
            "report": self.report.to_dict(),
        }


def build_aapl_pilot_evidence_bundle(
    *,
    run_id: str,
    from_date: str,
    to_date: str,
    layer0_run_id: str,
    layer1_run_id: str | None = None,
    ticker: str = "AAPL",
    human_semantic_review_status: str = "pending",
    writer: R2Writer | None = None,
    now: datetime | None = None,
) -> AAPLPilotEvidenceBundle:
    """Build the compact AAPL pilot evidence bundle for the dashboard CLI."""
    if human_semantic_review_status not in HUMAN_REVIEW_STATUSES:
        raise ValueError("human_semantic_review_status must be one of the supported statuses")

    active_writer = writer or R2Writer()
    active_ticker = ticker.strip().upper()
    report = build_layer1_aapl_evidence_report(
        run_id=run_id,
        from_date=from_date,
        to_date=to_date,
        ticker=active_ticker,
        writer=active_writer,
    )
    trading_date_list = trading_dates(from_date, to_date)
    skipped_date_list = list(skipped_non_trading_dates(from_date, to_date))
    layer1_stage_run_id = (layer1_run_id or run_id).strip()
    expected_trading_dates = list(trading_date_list)
    expected_layer1_keys = [layer1_feature_path(date_text, active_ticker) for date_text in expected_trading_dates]
    expected_score_keys: list[str] = []
    expected_topic_keys: list[str] = []
    expected_sentiment_feature_keys: list[str] = []
    expected_regime_keys: list[str] = []
    expected_news_preprocessing_keys: list[str] = []
    missing_keys: list[str] = []

    for date_text in expected_trading_dates:
        stage_run_id = f"{run_id}-{date_text}"
        expected_news_preprocessing_keys.append(
            layer1_news_preprocessing_path(date_text, stage_run_id)
        )
        expected_score_keys.append(layer1_sentiment_score_path(date_text, stage_run_id))
        expected_topic_keys.append(layer1_topic_feature_path(date_text, stage_run_id))
        expected_sentiment_feature_keys.append(layer1_sentiment_feature_path(date_text, stage_run_id))
        expected_regime_keys.append(layer1_regime_path(date_text, stage_run_id))
        for key in (
            layer1_feature_path(date_text, active_ticker),
            expected_news_preprocessing_keys[-1],
            expected_score_keys[-1],
            expected_topic_keys[-1],
            expected_sentiment_feature_keys[-1],
            expected_regime_keys[-1],
        ):
            if not active_writer.exists(key):
                missing_keys.append(key)

    raw_price_key = raw_price_path(active_ticker)
    if not active_writer.exists(raw_price_key):
        missing_keys.append(raw_price_key)

    layer0_manifest_key = pipeline_manifest_path("layer0", layer0_run_id)
    if not active_writer.exists(layer0_manifest_key):
        missing_keys.append(layer0_manifest_key)

    layer1_manifest_key = pipeline_manifest_path("layer1", layer1_stage_run_id)
    if not active_writer.exists(layer1_manifest_key):
        missing_keys.append(layer1_manifest_key)

    date_gate = IntegrityGate(
        name="date_first_feature_coverage",
        passed=True,
        details={
            "expected_trading_dates": expected_trading_dates,
            "skipped_non_trading_dates": skipped_date_list,
            "expected_layer1_feature_keys": expected_layer1_keys,
        },
    )
    artifacts_gate = IntegrityGate(
        name="expected_artifacts_exist",
        passed=not missing_keys,
        details={
            "missing_keys": missing_keys,
            "expected_trading_dates": expected_trading_dates,
        },
    )
    machine_passed = date_gate.passed and artifacts_gate.passed
    machine_status = "pass" if machine_passed else "fail"
    recommendation = (
        "proceed"
        if machine_passed and human_semantic_review_status == "accepted"
        else ("do_not_proceed" if not machine_passed else "needs_human_review")
    )
    human_rows = _build_human_review_rows(
        active_writer,
        active_ticker,
        expected_trading_dates,
        run_id,
    )
    return AAPLPilotEvidenceBundle(
        run_id=run_id,
        ticker=active_ticker,
        from_date=from_date,
        to_date=to_date,
        layer0_run_id=layer0_run_id,
        layer1_run_id=layer1_run_id,
        generated_at=(now or datetime.now(tz=UTC)).isoformat(),
        gates=[date_gate, artifacts_gate],
        machine_integrity_status=machine_status,
        human_semantic_review_status=human_semantic_review_status,
        recommendation_for_issue_202=recommendation,
        human_review_rows=human_rows,
        artifact_keys={
            "raw_price": raw_price_key,
            "layer0_manifest": layer0_manifest_key,
            "layer1_manifest": layer1_manifest_key,
            "expected_trading_dates": expected_trading_dates,
            "skipped_non_trading_dates": skipped_date_list,
            "expected_layer1_feature_keys": expected_layer1_keys,
            "expected_news_preprocessing_keys": expected_news_preprocessing_keys,
            "expected_finbert_score_keys": expected_score_keys,
            "expected_topic_keys": expected_topic_keys,
            "expected_sentiment_feature_keys": expected_sentiment_feature_keys,
            "expected_regime_keys": expected_regime_keys,
        },
        report=report,
    )


def render_aapl_pilot_human_review_markdown(bundle: AAPLPilotEvidenceBundle) -> str:
    """Render the compact human-review markdown summary."""
    lines = [
        f"# AAPL Layer 1 pilot evidence for {bundle.run_id}",
        "FinBERT, topic-model, and HMM semantic correctness is a human decision.",
        f"Machine integrity: {bundle.machine_integrity_status}",
        f"Human semantic review: {bundle.human_semantic_review_status}",
        f"Recommendation for #202: {bundle.recommendation_for_issue_202}",
        "",
        "## Human review rows",
    ]
    for row in bundle.human_review_rows:
        headline = row.raw_headline or "(missing headline)"
        snippet = row.raw_snippet or "(missing snippet)"
        lines.extend(
            [
                f"- {row.date} | {headline} | {snippet}",
            ]
        )
    return "\n".join(lines) + "\n"


def render_aapl_pilot_human_review_csv(bundle: AAPLPilotEvidenceBundle) -> str:
    """Render the compact human-review CSV summary."""
    buffer = StringIO()
    fieldnames = [
        "date",
        "ticker",
        "review_status",
        "raw_article_id",
        "raw_headline",
        "raw_snippet",
        "raw_source",
        "raw_published_at",
        "raw_news_key",
        "preprocessed_news_key",
        "finbert_scored_news_key",
        "finbert_positive",
        "finbert_negative",
        "finbert_neutral",
        "finbert_score",
        "relevance_score",
        "regime",
        "notes",
    ]
    writer = csv.DictWriter(buffer, fieldnames=fieldnames)
    writer.writeheader()
    for row in bundle.human_review_rows:
        writer.writerow(row.to_dict())
    return buffer.getvalue()


def write_aapl_pilot_evidence_outputs(
    bundle: AAPLPilotEvidenceBundle,
    *,
    json_path: Path,
    markdown_path: Path,
    csv_path: Path,
) -> dict[str, Path]:
    """Write the AAPL pilot evidence bundle to JSON, Markdown, and CSV."""
    json_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_path.parent.mkdir(parents=True, exist_ok=True)
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(json.dumps(bundle.to_dict(), indent=2, sort_keys=True), encoding="utf-8")
    markdown_path.write_text(render_aapl_pilot_human_review_markdown(bundle), encoding="utf-8")
    csv_path.write_text(render_aapl_pilot_human_review_csv(bundle), encoding="utf-8")
    return {"json": json_path, "markdown": markdown_path, "csv": csv_path}


def _build_human_review_rows(
    writer: R2Writer,
    ticker: str,
    trading_date_list: Sequence[str],
    run_id: str,
) -> list[HumanReviewRow]:
    """Build one human-review row per trading date from scored-news artifacts."""
    rows: list[HumanReviewRow] = []
    for date_text in trading_date_list:
        stage_run_id = f"{run_id}-{date_text}"
        score_key = layer1_sentiment_score_path(date_text, stage_run_id)
        try:
            frame = _read_parquet_frame(writer.get_object(score_key))
        except FileNotFoundError:
            frame = pd.DataFrame()
        first_row = frame.iloc[0] if not frame.empty else {}
        row_source = first_row.to_dict() if hasattr(first_row, "to_dict") else {}
        rows.append(
            HumanReviewRow(
                date=date_text,
                ticker=ticker,
                review_status="pending",
                raw_article_id=_optional_str(row_source.get("article_id")),
                raw_headline=_optional_str(row_source.get("headline")),
                raw_snippet=_optional_str(row_source.get("text")),
                raw_source=_optional_str(row_source.get("source")),
                raw_published_at=_optional_str(row_source.get("published_at")),
                raw_news_key=score_key,
                preprocessed_news_key=layer1_news_preprocessing_path(date_text, stage_run_id),
                finbert_scored_news_key=score_key,
                finbert_positive=_maybe_float(row_source.get("sentiment_positive")),
                finbert_negative=_maybe_float(row_source.get("sentiment_negative")),
                finbert_neutral=_maybe_float(row_source.get("sentiment_neutral")),
                finbert_score=_maybe_float(row_source.get("sentiment_score")),
                relevance_score=_maybe_float(row_source.get("relevance_score")),
                regime=None,
                notes="FinBERT, topic-model, and HMM semantic correctness is a human decision.",
            )
        )
    return rows
