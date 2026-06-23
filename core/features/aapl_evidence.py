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
from services.r2.paths import (
    layer1_feature_path,
    layer1_news_preprocessing_path,
    layer1_regime_path,
    layer1_sentiment_feature_path,
    layer1_sentiment_score_path,
    layer1_topic_feature_path,
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
    trading_date_list = trading_dates(from_date, to_date)
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
        regime_frames.append(regime_frame)

    regime_frame = pd.concat(regime_frames, ignore_index=True) if regime_frames else pd.DataFrame()

    if (not scored_frames or not regime_frames) and (cached_frames := _load_cached_aapl_pilot_evidence_frames(
        run_id=run_id,
        from_date=from_date,
        to_date=to_date,
        ticker=requested_ticker,
    )) is not None:
        scored_frame, regime_frame = cached_frames
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
            }
        ]

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
