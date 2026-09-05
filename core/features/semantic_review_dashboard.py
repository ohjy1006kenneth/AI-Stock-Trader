"""UI payload helpers for the Layer 1 semantic-review dashboard."""
from __future__ import annotations

import json
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, cast

from core.features.aapl_evidence import (
    Layer1SemanticReviewReport,
    _build_payload_from_report,
    build_layer1_semantic_review_dashboard_smoke_result,
)


@dataclass(frozen=True)
class _GateDefinition:
    """Static metadata for one dashboard readiness gate."""

    key: str
    label: str
    section_key: str
    artifact_key: str
    failure_stages: tuple[str, ...]
    required: bool = True


_GATE_DEFINITIONS = (
    _GateDefinition(
        key="news_preprocessing",
        label="Ticker/entity preprocessing",
        section_key="raw_preprocessing_rows",
        artifact_key="news_preprocessing",
        failure_stages=("news_preprocessing",),
    ),
    _GateDefinition(
        key="text_embeddings",
        label="Article embeddings",
        section_key="article_embedding_rows",
        artifact_key="text_embeddings",
        failure_stages=("text_embeddings",),
    ),
    _GateDefinition(
        key="topic_labels",
        label="BERTopic labels",
        section_key="topic_label_rows",
        artifact_key="topic_labels",
        failure_stages=("topic_labels",),
    ),
    _GateDefinition(
        key="news_relevance_gate",
        label="Pre-FinBERT relevance gate",
        section_key="relevance_gate_rows",
        artifact_key="news_relevance_gate",
        failure_stages=("news_relevance_gate",),
    ),
    _GateDefinition(
        key="news_sentiment_scored",
        label="Sentence/chunk FinBERT rows",
        section_key="finbert_sentence_rows",
        artifact_key="news_sentiment_scored",
        failure_stages=("news_sentiment_scored",),
    ),
    _GateDefinition(
        key="sentiment_features",
        label="Ticker-Date Semantic Aggregates",
        section_key="semantic_aggregate_rows",
        artifact_key="sentiment_features",
        failure_stages=("sentiment_features",),
    ),
    _GateDefinition(
        key="hmm_regime",
        label="Date-level HMM regime",
        section_key="date_level_regime_rows",
        artifact_key="regime",
        failure_stages=("hmm_regime", "hmm_manifest", "hmm_evaluation_context"),
    ),
    _GateDefinition(
        key="stock_price_context",
        label="Selected-ticker price rows",
        section_key="stock_price_rows",
        artifact_key="raw_prices",
        failure_stages=("raw_price_context",),
    ),
    _GateDefinition(
        key="benchmark_price_context",
        label="Benchmark price rows",
        section_key="benchmark_price_series",
        artifact_key="raw_prices",
        failure_stages=("benchmark_price_context",),
    ),
    _GateDefinition(
        key="benchmark_hmm_context",
        label="Benchmark/HMM chart rows",
        section_key="benchmark_market_regime_series",
        artifact_key="regime",
        failure_stages=("benchmark_hmm_chart",),
    ),
)


_DIAGNOSTIC_STATES = ("PASS", "WARN", "FAIL", "NOT_RUN", "NO_DATA")


def _diagnostic_record(state: str, reason: str, *, reviewable: bool) -> dict[str, object]:
    normalized_state = state.upper()
    if normalized_state not in _DIAGNOSTIC_STATES:
        normalized_state = "NOT_RUN"
    return {"state": normalized_state, "reason": reason, "reviewable": reviewable}


def _topic_label_looks_like_model_metadata(label: object) -> bool:
    text = _optional_str(label)
    if not text:
        return False
    lowered = text.lower()
    if lowered.startswith("bertopic") and "bertopic-" in lowered:
        return True
    if lowered.startswith("bertopic") and any(token in lowered for token in ("v0.", "v1.", "version", "model")):
        return True
    return lowered in {"bertopic", "bertopic labels", "topic model metadata"}


def _has_human_readable_topic_label(label: object) -> bool:
    text = _optional_str(label)
    if not text:
        return False
    lowered = text.lower()
    return not _topic_label_looks_like_model_metadata(lowered) and any(char.isalpha() for char in text)


def build_layer1_semantic_review_dashboard_payload(
    report: Layer1SemanticReviewReport | Mapping[str, object],
) -> dict[str, object]:
    """Return the JSON payload rendered by the semantic-review dashboard UI."""
    payload = _build_payload_from_report(report)
    payload["topic_relevance_review"] = build_layer1_topic_relevance_review(payload)
    payload["semantic_aggregate_review"] = build_layer1_semantic_aggregate_review(payload)
    payload.update(build_layer1_semantic_review_readiness_summary(payload))
    return _compact_layer1_semantic_review_dashboard_payload(payload)


def build_layer1_semantic_review_dashboard_smoke_payload(
    report: Layer1SemanticReviewReport | Mapping[str, object],
) -> dict[str, object]:
    """Return a compact payload for dashboard smoke rendering.

    The full dashboard payload is intentionally rich and highly duplicated so the
    human-facing UI can render article, sentence, and topic detail tabs. That is
    appropriate for the API, but it is too expensive to materialize in the smoke
    path on memory-constrained hardware. This helper keeps the smoke payload
    small by sampling representative rows while preserving the data the smoke
    gate and chart renderer need.
    """
    if isinstance(report, Layer1SemanticReviewReport):
        summary = dict(report.summary)
        controls = {
            "ticker": report.ticker,
            "run_id": report.run_id,
            "from_date": report.from_date,
            "to_date": report.to_date,
        }
        benchmark_price_rows_source = report.benchmark_price_rows
        benchmark_market_regime_rows_source = report.benchmark_market_regime_rows
        preprocessing_rows_source = report.preprocessing_rows
        embedding_rows_source = report.embedding_rows
        topic_label_rows_source = report.topic_label_rows
        relevance_gate_rows_source = report.relevance_gate_rows
        semantic_aggregate_rows_source = report.semantic_aggregate_rows
        regime_rows_source = report.regime_rows
        price_rows_source = report.price_rows
        market_regime_rows_source = report.market_regime_rows
        article_groups_source = report.article_groups
        warnings_source = report.load_warnings
        artifact_keys = {key: list(value) for key, value in report.artifact_keys.items()}
        hmm_context = dict(report.hmm_evaluation_context)
        benchmark_ticker = report.benchmark_ticker
    else:
        report_dict = dict(report)
        summary = _json_mapping(report_dict.get("summary"))
        controls = {
            "ticker": report_dict.get("ticker"),
            "run_id": report_dict.get("run_id"),
            "from_date": report_dict.get("from_date"),
            "to_date": report_dict.get("to_date"),
        }
        benchmark_price_rows_source = report_dict.get("benchmark_price_rows", [])
        benchmark_market_regime_rows_source = report_dict.get("benchmark_market_regime_rows", [])
        preprocessing_rows_source = report_dict.get("preprocessing_rows", [])
        embedding_rows_source = report_dict.get("embedding_rows", [])
        topic_label_rows_source = report_dict.get("topic_label_rows", [])
        relevance_gate_rows_source = report_dict.get("relevance_gate_rows", [])
        semantic_aggregate_rows_source = report_dict.get("semantic_aggregate_rows", [])
        regime_rows_source = report_dict.get("regime_rows", [])
        price_rows_source = report_dict.get("price_rows", [])
        market_regime_rows_source = report_dict.get("market_regime_rows", [])
        article_groups_source = report_dict.get("article_groups", [])
        warnings_source = report_dict.get("load_warnings", [])
        artifact_keys = {
            str(key): _json_string_list(value)
            for key, value in _json_mapping(report_dict.get("artifact_keys")).items()
        }
        hmm_context = _json_mapping(report_dict.get("hmm_evaluation_context"))
        benchmark_ticker = report_dict.get("benchmark_ticker")

    payload: dict[str, object] = {
        "title": "Layer 1 semantic review dashboard",
        "description": (
            "Beginner-friendly review of whether the Layer 1 Apple news signal, "
            "the market benchmark, and the market-regime evidence look trustworthy."
        ),
        "human_semantic_review_status": "needs_human_review",
        "recommendation_for_issue_202": "needs_human_review",
        "summary": summary,
        "controls": controls,
        "benchmark_ticker": benchmark_ticker,
        "benchmark_price_series": [
            dict(item) for item in _smoke_sample_rows(benchmark_price_rows_source, limit=3)
        ],
        "benchmark_market_regime_series": [
            dict(item) for item in _smoke_sample_rows(benchmark_market_regime_rows_source, limit=3)
        ],
        "hmm_evaluation_context": hmm_context,
        "artifact_keys": artifact_keys,
        "warnings": [dict(item) for item in _smoke_sample_rows(warnings_source, limit=12)],
        "pipeline_sections": {
            "raw_preprocessing_rows": [
                dict(item) for item in _smoke_sample_rows(preprocessing_rows_source, limit=1)
            ],
            "article_embedding_rows": [
                dict(item) for item in _smoke_sample_rows(embedding_rows_source, limit=1)
            ],
            "topic_label_rows": [
                dict(item) for item in _smoke_sample_rows(topic_label_rows_source, limit=1)
            ],
            "relevance_gate_rows": [
                dict(item) for item in _smoke_sample_rows(relevance_gate_rows_source, limit=1)
            ],
            "finbert_sentence_rows": _smoke_sentence_rows(article_groups_source),
            "semantic_aggregate_rows": [
                dict(item) for item in _smoke_sample_rows(semantic_aggregate_rows_source, limit=1)
            ],
            "date_level_regime_rows": [
                dict(item) for item in _smoke_sample_rows(regime_rows_source, limit=1)
            ],
            "stock_price_rows": [dict(item) for item in _smoke_sample_rows(price_rows_source, limit=3)],
            "date_aligned_price_hmm_rows": [
                dict(item) for item in _smoke_sample_rows(market_regime_rows_source, limit=3)
            ],
        },
    }
    payload["smoke"] = build_layer1_semantic_review_dashboard_smoke_result(payload)
    payload.update(build_layer1_semantic_review_readiness_summary(payload))
    return payload


def build_layer1_topic_relevance_review(
    payload: Mapping[str, object],
) -> dict[str, object]:
    """Return article-level topic, embedding, and relevance-gate review evidence."""
    sections = _json_mapping(payload.get("pipeline_sections"))
    summary = _json_mapping(payload.get("summary"))
    articles = [
        dict(item)
        for item in _json_list(payload.get("article_groups"))
        if isinstance(item, Mapping)
    ]
    topic_review = _json_mapping(payload.get("topic_review"))
    preprocessing_by_article = _rows_by_article(sections.get("raw_preprocessing_rows"))
    embedding_by_article = _rows_by_article(sections.get("article_embedding_rows"))
    topic_by_article = _rows_by_article(sections.get("topic_label_rows"))
    relevance_by_article = _rows_by_article(sections.get("relevance_gate_rows"))
    preprocessing_rows = _json_list(sections.get("raw_preprocessing_rows"))
    embedding_rows = _json_list(sections.get("article_embedding_rows"))
    topic_rows = _json_list(sections.get("topic_label_rows"))
    relevance_rows = _json_list(sections.get("relevance_gate_rows"))
    diversity_status = str(topic_review.get("diversity_status") or "unknown")
    diversity_reason = _optional_str(topic_review.get("diversity_reason"))
    topic_review_topics = _json_list(topic_review.get("topics"))
    topic_review_rows = _json_list(topic_review.get("rows"))
    topic_review_warning = (
        None
        if diversity_status == "diverse"
        else diversity_reason or "Topic diversity is insufficient for a varied review sample."
    )

    rows = [
        _topic_relevance_article_row(
            article=article,
            preprocessing_rows=preprocessing_by_article.get(_article_key(article), []),
            embedding_rows=embedding_by_article.get(_article_key(article), []),
            topic_rows=topic_by_article.get(_article_key(article), []),
            relevance_rows=relevance_by_article.get(_article_key(article), []),
        )
        for article in articles
    ]
    date_groups: list[dict[str, object]] = []
    for date_text in _dedupe_preserve_order(
        str(row.get("date")) for row in rows if row.get("date") is not None
    ):
        grouped_rows = [row for row in rows if row.get("date") == date_text]
        date_groups.append(
            {
                "date": date_text,
                "article_count": len(grouped_rows),
                "accepted_count": sum(
                    1 for row in grouped_rows if row.get("evidence_status") == "accepted"
                ),
                "borderline_count": sum(
                    1 for row in grouped_rows if row.get("evidence_status") == "borderline"
                ),
                "rejected_count": sum(
                    1 for row in grouped_rows if row.get("evidence_status") == "rejected"
                ),
                "missing_or_default_count": sum(
                    1
                    for row in grouped_rows
                    if row.get("evidence_status") == "missing_or_default"
                ),
                "articles": grouped_rows,
            }
        )

    missing_blockers = [
        {
            "date": row.get("date"),
            "article_id": row.get("article_id"),
            "headline": row.get("headline"),
            "missing_evidence_flags": row.get("missing_evidence_flags"),
            "evidence_status": row.get("evidence_status"),
            "relevance_score_interpretation": row.get("relevance_score_interpretation"),
        }
        for row in rows
        if _json_string_list(row.get("missing_evidence_flags"))
    ]
    reviewability = _topic_relevance_reviewability_summary(
        article_count=_effective_row_count(len(rows), summary.get("article_count")),
        relevance_gate_row_count=_effective_row_count(
            len(relevance_rows),
            summary.get("relevance_gate_row_count"),
        ),
        embedding_row_count=_effective_row_count(
            len(embedding_rows),
            summary.get("embedding_row_count"),
        ),
        topic_label_row_count=_effective_row_count(
            len(topic_rows),
            summary.get("topic_label_row_count"),
        ),
        topic_review_rows=topic_review_rows,
        topic_review_topics=topic_review_topics,
        topic_review_warning=topic_review_warning,
        relevance_rows=relevance_rows,
    )
    return {
        "summary": {
            "article_count": len(rows),
            "preprocessing_row_count": _effective_row_count(
                len(preprocessing_rows),
                summary.get("preprocessing_row_count"),
            ),
            "embedding_row_count": _effective_row_count(
                len(embedding_rows),
                summary.get("embedding_row_count"),
            ),
            "topic_label_row_count": _effective_row_count(
                len(topic_rows),
                summary.get("topic_label_row_count"),
            ),
            "relevance_gate_row_count": _effective_row_count(
                len(relevance_rows),
                summary.get("relevance_gate_row_count"),
            ),
            "relevance_gate_available": bool(relevance_rows),
            "topic_review_row_count": len(topic_review_rows),
            "topic_review_topic_count": len(topic_review_topics),
            "topic_review_diversity_status": diversity_status,
            "topic_review_warning": topic_review_warning,
            "accepted_count": sum(
                1 for row in rows if row.get("evidence_status") == "accepted"
            ),
            "borderline_count": sum(
                1 for row in rows if row.get("evidence_status") == "borderline"
            ),
            "rejected_count": sum(
                1 for row in rows if row.get("evidence_status") == "rejected"
            ),
            "missing_or_default_count": sum(
                1 for row in rows if row.get("evidence_status") == "missing_or_default"
            ),
            "missing_embedding_count": sum(
                1
                for row in rows
                if "missing_embedding" in _json_string_list(row.get("missing_evidence_flags"))
            ),
            "missing_topic_count": sum(
                1
                for row in rows
                if "missing_topic_label" in _json_string_list(row.get("missing_evidence_flags"))
            ),
            "default_relevance_count": sum(
                1
                for row in rows
                if "default_relevance_without_supporting_evidence"
                in _json_string_list(row.get("missing_evidence_flags"))
            ),
            "target_impact_included_count": sum(1 for row in rows if row.get("included_in_signal") is True),
            "target_impact_excluded_count": sum(1 for row in rows if row.get("included_in_signal") is False),
            "target_impact_missing_count": sum(
                1 for row in rows if _json_string_list(row.get("target_impact_missing_flags"))
            ),
            **reviewability,
        },
        "date_groups": date_groups,
        "articles": rows,
        "topic_review": {
            "summary": {
                "row_count": len(topic_review_rows),
                "topic_count": len(topic_review_topics),
                "diversity_status": diversity_status,
                "diversity_reason": diversity_reason,
                "warning": topic_review_warning,
                "dominant_topic_id": topic_review.get("dominant_topic_id"),
                "dominant_topic_share": topic_review.get("dominant_topic_share"),
                "diagnostic_state": reviewability["topic_review_state"]["state"],
                "reviewable": reviewability["topic_review_state"]["reviewable"],
                "review_status": reviewability["topic_review_state"]["state"].lower(),
                "review_explanation": reviewability["topic_review_state"]["reason"],
            },
            "topics": topic_review_topics,
            "rows": topic_review_rows,
            "diagnostic_state": reviewability["topic_review_state"]["state"],
        },
        "missing_evidence_blockers": missing_blockers,
    }



def _hmm_chart_auditability_state(payload: Mapping[str, object]) -> dict[str, object]:
    benchmark_prices = [
        dict(item)
        for item in _json_list(payload.get("benchmark_price_series"))
        if isinstance(item, Mapping)
    ]
    benchmark_rows = [
        dict(item)
        for item in _json_list(payload.get("benchmark_market_regime_series"))
        if isinstance(item, Mapping)
    ]
    point_count = min(len(benchmark_prices), len(benchmark_rows))
    if point_count == 0:
        return {
            **_diagnostic_record("NO_DATA", "No benchmark/HMM chart rows are available.", reviewable=False),
            "point_count": point_count,
            "price_row_count": len(benchmark_prices),
            "market_regime_row_count": len(benchmark_rows),
        }
    if point_count < 2:
        return {
            **_diagnostic_record(
                "WARN",
                "Only one benchmark/HMM point is available, so the chart is limited for auditability.",
                reviewable=False,
            ),
            "point_count": point_count,
            "price_row_count": len(benchmark_prices),
            "market_regime_row_count": len(benchmark_rows),
        }
    return {
        **_diagnostic_record(
            "PASS",
            "The benchmark/HMM chart has enough points for meaningful auditability.",
            reviewable=True,
        ),
        "point_count": point_count,
        "price_row_count": len(benchmark_prices),
        "market_regime_row_count": len(benchmark_rows),
    }


def build_layer1_semantic_aggregate_review(
    payload: Mapping[str, object],
) -> dict[str, object]:
    """Return human-focused ticker-date NLP aggregate review rows."""
    sections = _json_mapping(payload.get("pipeline_sections"))
    rows = [
        _semantic_aggregate_review_row(row)
        for row in _json_list(sections.get("semantic_aggregate_rows"))
        if isinstance(row, Mapping)
    ]
    direction_counts: dict[str, int] = {}
    warning_codes: list[str] = []
    single_source_concentration_count = 0
    dominant_source_weight_share = None
    for row in rows:
        direction = _optional_str(row.get("target_company_impact_direction"))
        if direction:
            direction_counts[direction] = direction_counts.get(direction, 0) + 1
        warning_codes.extend(_json_string_list(row.get("semantic_warning_codes")))
        if bool(row.get("single_source_concentration")):
            single_source_concentration_count += 1
        share = _maybe_float(row.get("dominant_source_weight_share"))
        if share is not None:
            dominant_source_weight_share = (
                share
                if dominant_source_weight_share is None
                else max(dominant_source_weight_share, share)
            )
    summary = {
        "row_count": len(rows),
        "date_count": len({str(row.get("date")) for row in rows if row.get("date") is not None}),
        "reviewable": bool(rows),
        "review_focus": "ticker-date NLP summary consumed by later model layers",
        "target_impact_direction": _first_text(
            row.get("target_company_impact_direction") for row in rows
        ),
        "target_impact_magnitude": _first_text(
            row.get("target_company_impact_magnitude") for row in rows
        ),
        "target_impact_confidence": _first_float(
            row.get("target_impact_confidence") for row in rows
        ),
        "causal_channel": _first_text(row.get("causal_channel") for row in rows),
        "impact_horizon": _first_text(row.get("impact_horizon") for row in rows),
        "single_source_concentration": single_source_concentration_count > 0,
        "single_source_concentration_count": single_source_concentration_count,
        "dominant_source_weight_share": dominant_source_weight_share,
        "semantic_warning_codes": _dedupe_preserve_order(warning_codes),
        "target_impact_review_status": (
            "derived"
            if any(
                _optional_str(row.get("target_company_impact_direction"))
                or _optional_str(row.get("target_company_impact_magnitude"))
                or _optional_str(row.get("causal_channel"))
                or _optional_str(row.get("impact_horizon"))
                or _maybe_float(row.get("target_impact_confidence")) is not None
                for row in rows
            )
            else "unclear"
        ),
        "target_impact_warning": (
            None
            if any(
                _optional_str(row.get("target_company_impact_direction"))
                or _optional_str(row.get("target_company_impact_magnitude"))
                or _optional_str(row.get("causal_channel"))
                or _optional_str(row.get("impact_horizon"))
                or _maybe_float(row.get("target_impact_confidence")) is not None
                for row in rows
            )
            else "No row-level target-impact evidence was available, so the aggregate review remains unclear."
        ),
        "target_impact_direction_counts": direction_counts,
    }
    return {"summary": summary, "rows": rows}


def build_layer1_semantic_review_readiness_summary(
    payload: Mapping[str, object],
) -> dict[str, object]:
    """Return stable run-readiness and gate-card fields for dashboard consumers."""
    summary = _json_mapping(payload.get("summary"))
    smoke = _json_mapping(payload.get("smoke"))
    semantic_aggregate_review = _json_mapping(payload.get("semantic_aggregate_review"))
    semantic_aggregate_summary = _json_mapping(semantic_aggregate_review.get("summary"))
    topic_relevance_review = _json_mapping(payload.get("topic_relevance_review"))
    topic_relevance_summary = _json_mapping(topic_relevance_review.get("summary"))
    topic_review_state = _json_mapping(topic_relevance_summary.get("topic_review_state"))
    relevance_state = _json_mapping(topic_relevance_summary.get("relevance_informativeness_state"))
    embedding_state = _json_mapping(topic_relevance_summary.get("embedding_coverage_state"))
    hmm_chart_state = _hmm_chart_auditability_state(payload)
    hmm_feature_state = _hmm_feature_set_summary(payload)
    failures = [dict(item) for item in _json_list(smoke.get("failures")) if isinstance(item, Mapping)]
    failure_map = _failures_by_stage(failures)
    gate_cards = [
        _gate_card_payload(
            definition=definition,
            payload=payload,
            failure_map=failure_map,
        )
        for definition in _GATE_DEFINITIONS
    ]
    blocked_gates = [card for card in gate_cards if card["status"] == "blocked"]
    smoke_passed = str(smoke.get("status")) == "pass"
    diagnostic_states = {
        "topic_review": str(topic_review_state.get("state") or topic_relevance_summary.get("diagnostic_state") or "NOT_RUN"),
        "relevance_informativeness": str(relevance_state.get("state") or "NOT_RUN"),
        "embedding_coverage": str(embedding_state.get("state") or "NOT_RUN"),
        "hmm_chart_auditability": str(hmm_chart_state.get("state") or "NOT_RUN"),
    }
    diagnostics_reviewable = all(
        state and bool(state.get("reviewable"))
        for state in (topic_review_state, relevance_state, embedding_state, hmm_chart_state)
    )
    ready_for_final_acceptance = smoke_passed and not blocked_gates and diagnostics_reviewable
    readiness_status = (
        "ready_for_final_human_acceptance"
        if ready_for_final_acceptance
        else "not_ready_for_final_human_acceptance"
    )
    recommendation = (
        "ready for final human acceptance"
        if ready_for_final_acceptance
        else "not ready for final human acceptance"
    )
    if ready_for_final_acceptance:
        human_review_status = "can_start"
    elif blocked_gates:
        human_review_status = "blocked_by_missing_pipeline_evidence"
    else:
        human_review_status = "blocked_by_unreviewable_diagnostics"
    missing_pipeline_sections = [
        {
            "key": str(card["key"]),
            "label": str(card["label"]),
            "reason": str(card["message"]),
            "missing_or_tried_keys": list(_json_string_list(card.get("missing_or_tried_keys"))),
        }
        for card in blocked_gates
    ]
    diagnostic_blocker_reasons = [
        str(state.get("reason") or "")
        for state in (topic_review_state, relevance_state, embedding_state, hmm_chart_state)
        if state and not bool(state.get("reviewable"))
    ]
    diagnostic_summary = {
        "topic_review": topic_review_state,
        "relevance_informativeness": relevance_state,
        "embedding_coverage": embedding_state,
        "hmm_chart_auditability": hmm_chart_state,
        "hmm_feature_set": hmm_feature_state,
        "overall_state": _diagnostic_worst_state(
            diagnostic_states["topic_review"],
            diagnostic_states["relevance_informativeness"],
            diagnostic_states["embedding_coverage"],
            diagnostic_states["hmm_chart_auditability"],
        ),
        "reviewable": diagnostics_reviewable,
        "blocker_reasons": [reason for reason in diagnostic_blocker_reasons if reason],
    }
    run_readiness = {
        "run_id": payload.get("run_id") or _json_mapping(payload.get("controls")).get("run_id"),
        "ticker": payload.get("ticker") or _json_mapping(payload.get("controls")).get("ticker"),
        "from_date": payload.get("from_date") or _json_mapping(payload.get("controls")).get("from_date"),
        "to_date": payload.get("to_date") or _json_mapping(payload.get("controls")).get("to_date"),
        "readiness_status": readiness_status,
        "recommendation": recommendation,
        "human_review_status": human_review_status,
        "human_review_can_start": ready_for_final_acceptance,
        "ready_for_final_human_acceptance": ready_for_final_acceptance,
        "single_source_concentration": bool(semantic_aggregate_summary.get("single_source_concentration")),
        "single_source_concentration_count": _first_int(
            [semantic_aggregate_summary.get("single_source_concentration_count")]
        )
        or 0,
        "target_impact_review_status": semantic_aggregate_summary.get("target_impact_review_status") or "unclear",
        "target_impact_warning": semantic_aggregate_summary.get("target_impact_warning"),
        "target_impact_direction": semantic_aggregate_summary.get("target_impact_direction"),
        "target_impact_magnitude": semantic_aggregate_summary.get("target_impact_magnitude"),
        "target_impact_confidence": semantic_aggregate_summary.get("target_impact_confidence"),
        "causal_channel": semantic_aggregate_summary.get("causal_channel"),
        "impact_horizon": semantic_aggregate_summary.get("impact_horizon"),
        "hmm_feature_set_state": hmm_feature_state["state"],
        "hmm_feature_set_warning": hmm_feature_state["reason"],
        "hmm_feature_set_reviewable": hmm_feature_state["reviewable"],
        "smoke_status": smoke.get("status") or "unknown",
        "sentence_row_count": int(summary.get("row_count") or 0),
        "article_count": int(summary.get("article_count") or 0),
        "date_count": int(summary.get("date_count") or 0),
        "accepted_article_count": int(summary.get("accepted_article_count") or 0),
        "flagged_article_count": int(summary.get("flagged_article_count") or 0),
        "blocked_gate_count": len(blocked_gates),
        "missing_pipeline_section_count": len(missing_pipeline_sections),
        "diagnostic_states": diagnostic_states,
        "diagnostic_summary": diagnostic_summary,
        "topic_relevance_review_status": topic_relevance_summary.get("review_status") or "unknown",
        "topic_review_state": topic_review_state,
        "relevance_informativeness_state": relevance_state,
        "embedding_coverage_state": embedding_state,
        "hmm_chart_auditability_state": hmm_chart_state,
        "status_reason": _readiness_status_reason(ready_for_final_acceptance, diagnostic_blocker_reasons),
    }
    if hmm_feature_state.get("state") == "WARN":
        hmm_reason = str(hmm_feature_state.get("reason") or "HMM feature set is degraded but non-blocking.")
        base_reason = str(run_readiness["status_reason"])
        if hmm_reason not in base_reason:
            run_readiness["status_reason"] = _normalize_reason_text(
                f"{base_reason} {hmm_reason}"
            )
        else:
            run_readiness["status_reason"] = _normalize_reason_text(base_reason)
    return {
        "run_readiness": run_readiness,
        "summary_cards": _summary_cards(run_readiness),
        "gate_cards": gate_cards,
        "missing_pipeline_sections": missing_pipeline_sections,
    }




def _smoke_sample_rows(value: object, *, limit: int) -> list[Mapping[str, object]]:
    """Return a small, ordered sample of mapping rows for smoke payloads."""
    rows = [item for item in _json_list(value) if isinstance(item, Mapping)]
    return rows[: max(limit, 0)]


def _smoke_sentence_rows(article_groups_value: object) -> list[dict[str, object]]:
    """Return a compact sample of scored sentence rows for the smoke payload."""
    def _text(value: object) -> str | None:
        if value is None:
            return None
        text = str(value).strip()
        return text or None

    def _int(value: object) -> int | None:
        try:
            if value is None:
                return None
            number = float(value)
        except (TypeError, ValueError):
            return None
        return int(number)

    def _float(value: object) -> float | None:
        try:
            if value is None:
                return None
            return float(cast(Any, value))
        except (TypeError, ValueError):
            return None

    article_groups = [item for item in _json_list(article_groups_value) if isinstance(item, Mapping)]
    for article in article_groups:
        sentence_rows = [item for item in _json_list(article.get("sentence_rows")) if isinstance(item, Mapping)]
        if sentence_rows:
            row = sentence_rows[0]
            return [
                {
                    "date": _text(row.get("date")) or _text(article.get("date")),
                    "article_id": _text(row.get("article_id")) or _text(article.get("article_id")),
                    "sentence_index": _int(row.get("sentence_index")),
                    "chunk_index": _int(row.get("chunk_index")),
                    "source_text_field": _text(row.get("source_text_field")),
                    "source_text_order": _int(row.get("source_text_order")),
                    "text": _text(row.get("text")),
                    "positive_probability": _float(row.get("positive_probability")),
                    "negative_probability": _float(row.get("negative_probability")),
                    "neutral_probability": _float(row.get("neutral_probability")),
                    "relevance_score": _float(row.get("relevance_score")),
                    "assignment_classification": _text(row.get("assignment_classification")),
                }
            ]
    return []



def validate_layer1_semantic_review_dashboard_payload(
    payload: Mapping[str, object],
) -> dict[str, object]:
    """Return the smoke gate result for a dashboard payload."""
    return build_layer1_semantic_review_dashboard_smoke_result(payload)


def _gate_card_payload(
    *,
    definition: _GateDefinition,
    payload: Mapping[str, object],
    failure_map: Mapping[str, Sequence[Mapping[str, object]]],
) -> dict[str, object]:
    rows = _section_rows(payload, definition.section_key)
    matching_failures = [
        dict(failure)
        for stage in definition.failure_stages
        for failure in failure_map.get(stage, [])
    ]
    missing_keys = _dedupe_preserve_order(
        key
        for failure in matching_failures
        for key in _json_string_list(failure.get("missing_or_tried_keys"))
    )
    resolved_keys = _dedupe_preserve_order(
        [
            *(
                key
                for failure in matching_failures
                for key in _json_string_list(failure.get("resolved_artifact_keys"))
            ),
            *_json_string_list(_json_mapping(payload.get("artifact_keys")).get(definition.artifact_key)),
        ]
    )
    if matching_failures or (definition.required and not rows):
        status = "blocked"
    else:
        status = "ready"
    return {
        "key": definition.key,
        "label": definition.label,
        "status": status,
        "required": definition.required,
        "row_count": len(rows),
        "artifact_keys": resolved_keys,
        "missing_or_tried_keys": missing_keys,
        "failure_reasons": _dedupe_preserve_order(
            str(failure.get("reason"))
            for failure in matching_failures
            if failure.get("reason") is not None
        ),
        "message": _gate_message(definition.label, status, len(rows), matching_failures),
    }


def _topic_relevance_article_row(
    *,
    article: Mapping[str, object],
    preprocessing_rows: Sequence[Mapping[str, object]],
    embedding_rows: Sequence[Mapping[str, object]],
    topic_rows: Sequence[Mapping[str, object]],
    relevance_rows: Sequence[Mapping[str, object]],
) -> dict[str, object]:
    relevance_decisions = _dedupe_preserve_order(
        str(row.get("relevance_decision"))
        for row in relevance_rows
        if row.get("relevance_decision") is not None
    )
    reason_codes = _dedupe_preserve_order(
        code
        for row in relevance_rows
        for code in _json_string_list(row.get("reason_codes"))
    )
    source_tickers = _dedupe_preserve_order(
        ticker
        for row in relevance_rows
        for ticker in _ticker_evidence_values(row.get("ticker_evidence"))
    )
    ticker_mentions = _dedupe_preserve_order(
        ticker
        for row in preprocessing_rows
        for ticker in _json_string_list(row.get("ticker_mentions"))
    )
    entity_mentions = _dedupe_preserve_order(
        entity
        for row in preprocessing_rows
        for entity in _json_string_list(row.get("entity_mentions"))
    )
    relevance_score = _first_float(
        [row.get("relevance_score") for row in relevance_rows]
        + [article.get("relevance_score")]
    )
    ticker_score = _first_float([row.get("ticker_relevance_score") for row in relevance_rows])
    financial_score = _first_float(
        [row.get("financial_relevance_score") for row in relevance_rows]
    )
    topic_score = _first_float([row.get("topic_relevance_score") for row in relevance_rows])
    assignment_fields = _first_assignment_provenance_fields(preprocessing_rows)
    target_impact = _target_impact_review_summary(article, relevance_rows)
    has_embedding = bool(embedding_rows)
    has_topic = bool(topic_rows)
    has_relevance_gate = bool(relevance_rows)
    has_ticker_evidence = bool(ticker_mentions) or (
        ticker_score is not None and ticker_score > 0.0
    )
    has_entity_evidence = bool(entity_mentions)
    has_semantic_evidence = has_embedding and has_topic and (
        topic_score is None or topic_score > 0.0
    )
    missing_flags = _topic_relevance_missing_flags(
        has_embedding=has_embedding,
        has_topic=has_topic,
        has_relevance_gate=has_relevance_gate,
        has_ticker_evidence=has_ticker_evidence,
        has_entity_evidence=has_entity_evidence,
        has_semantic_evidence=has_semantic_evidence,
        relevance_score=relevance_score,
        relevance_decisions=relevance_decisions,
    )
    evidence_status = _topic_relevance_status(
        relevance_decisions=relevance_decisions,
        missing_flags=missing_flags,
    )
    return {
        "date": article.get("date"),
        "ticker": article.get("ticker"),
        "article_id": article.get("article_id"),
        "headline": article.get("headline"),
        "normalized_headline": article.get("normalized_headline"),
        "article_status": article.get("article_status"),
        "evidence_status": evidence_status,
        "relevance_score": relevance_score,
        "relevance_score_interpretation": _relevance_score_interpretation(
            relevance_score=relevance_score,
            missing_flags=missing_flags,
            relevance_decisions=relevance_decisions,
        ),
        "relevance_decision": relevance_decisions[0] if relevance_decisions else "missing",
        "relevance_decisions": relevance_decisions,
        "ticker_relevance_score": ticker_score,
        "financial_relevance_score": financial_score,
        "topic_relevance_score": topic_score,
        **target_impact,
        "reason_codes": reason_codes,
        "assignment_classification": assignment_fields["assignment_classification"],
        "assignment_reason": assignment_fields["assignment_reason"],
        "assignment_weight": assignment_fields["assignment_weight"],
        "assignment_evidence_kinds": assignment_fields["assignment_evidence_kinds"],
        "missing_evidence_flags": missing_flags,
        "ticker_evidence": {
            "requested_ticker_term_hits": _json_string_list(
                article.get("requested_ticker_term_hits")
            ),
            "preprocessing_ticker_mentions": ticker_mentions,
            "source_tickers": source_tickers,
        },
        "entity_evidence": {
            "preprocessing_entity_mentions": entity_mentions,
            "relevance_gate_entity_mentions": _dedupe_preserve_order(
                entity
                for row in relevance_rows
                for entity in _json_string_list(row.get("entity_evidence"))
            ),
        },
        "embedding_evidence": [
            {
                "embedding_cache_key": row.get("embedding_cache_key"),
                "embedding_model": row.get("embedding_model"),
                "embedding_revision": row.get("embedding_revision"),
                "embedding_dimension": row.get("embedding_dimension"),
                "artifact_key": row.get("artifact_key"),
            }
            for row in embedding_rows
        ],
        "topic_evidence": [
            {
                "topic_id": row.get("topic_id"),
                "topic_probability": row.get("topic_probability"),
                "topic_label": row.get("topic_label"),
                "topic_keywords": row.get("topic_keywords"),
                "topic_example_text": row.get("topic_example_text"),
                "topic_example_texts": row.get("topic_example_texts"),
                "topic_model": row.get("topic_model"),
                "topic_model_version": row.get("topic_model_version"),
                "embedding_cache_key": row.get("embedding_cache_key"),
                "artifact_key": row.get("artifact_key"),
            }
            for row in topic_rows
        ],
        "relevance_gate_rows": _compact_topic_relevance_gate_rows(relevance_rows),
        "preprocessing_rows": [dict(row) for row in preprocessing_rows],
    }


def _compact_topic_relevance_gate_rows(rows: Sequence[Mapping[str, object]]) -> list[dict[str, object]]:
    """Return compact row evidence for the article tab; full rows remain in pipeline_sections."""
    compact_rows: list[dict[str, object]] = []
    for row in rows:
        compact_rows.append(
            {
                "date": row.get("date"),
                "ticker": row.get("ticker"),
                "article_id": row.get("article_id"),
                "sentence_index": row.get("sentence_index"),
                "relevance_decision": row.get("relevance_decision"),
                "relevance_score": row.get("relevance_score"),
                "target_context_score": row.get("target_context_score"),
                "relevance_category": row.get("relevance_category"),
                "target_company_impact_direction": row.get("target_company_impact_direction"),
                "article_contribution_weight": row.get("article_contribution_weight"),
                "included_in_signal": _target_row_included_in_signal(row),
                "final_contribution": row.get("final_contribution"),
                "reason_codes": _json_string_list(row.get("reason_codes")),
            }
        )
    return compact_rows


_TARGET_SIGNAL_CATEGORIES = frozenset(
    {
        "direct_target_event",
        "supplier_or_input_cost_exposure",
        "competitor_read_through",
        "industry_or_macro_exposure",
    }
)


def _target_impact_review_summary(
    article: Mapping[str, object],
    relevance_rows: Sequence[Mapping[str, object]],
) -> dict[str, object]:
    """Return article-level target-impact and final contribution review fields."""
    category = _first_text([row.get("relevance_category") for row in relevance_rows] + [article.get("relationship_to_target")])
    target_context_score = _first_float([row.get("target_context_score") for row in relevance_rows] + [article.get("target_context_score")])
    contribution_weight = _first_float([row.get("article_contribution_weight") for row in relevance_rows] + [article.get("article_contribution_weight")])
    included = any(_target_row_included_in_signal(row) for row in relevance_rows)
    final_contribution = contribution_weight if included else (0.0 if relevance_rows else None)
    missing_flags = sorted({flag for row in relevance_rows for flag in _target_row_missing_flags(row)})
    status = "missing" if not relevance_rows else ("included" if included else "excluded")
    return {
        "relationship_to_target": category,
        "target_relationship": category,
        "relevance_category": category,
        "target_context_score": target_context_score,
        "direct_target_relevance": target_context_score,
        "document_sentiment": _first_text(row.get("document_sentiment") for row in relevance_rows),
        "target_company_impact_direction": _first_text(row.get("target_company_impact_direction") for row in relevance_rows),
        "target_company_impact_magnitude": _first_text(row.get("target_company_impact_magnitude") for row in relevance_rows),
        "impact_horizon": _first_text(row.get("impact_horizon") for row in relevance_rows),
        "causal_channel": _first_text(row.get("causal_channel") for row in relevance_rows),
        "target_impact_confidence": _first_float(row.get("target_impact_confidence") for row in relevance_rows),
        "article_contamination_ratio": _first_float(row.get("article_contamination_ratio") for row in relevance_rows),
        "article_contamination_count": _first_int(row.get("article_contamination_count") for row in relevance_rows),
        "article_signal_count": _first_int(row.get("article_signal_count") for row in relevance_rows),
        "article_contribution_weight": contribution_weight,
        "included_in_signal": included,
        "final_contribution": final_contribution,
        "final_signal_contribution": final_contribution,
        "target_impact_evidence_status": status,
        "target_impact_missing_flags": missing_flags,
    }


def _target_row_included_in_signal(row: Mapping[str, object]) -> bool:
    """Return True only when a relevance-gate row contributes target-conditioned signal."""
    category = _optional_str(row.get("relevance_category"))
    decision = (_optional_str(row.get("relevance_decision")) or "").lower()
    signal_count = _coerce_int(row.get("article_signal_count"))
    contribution_weight = _maybe_float(row.get("article_contribution_weight"))
    return bool(
        category in _TARGET_SIGNAL_CATEGORIES
        and decision in {"accepted", "borderline"}
        and (signal_count is None or signal_count > 0)
        and (contribution_weight is None or contribution_weight > 0.0)
    )


def _target_row_missing_flags(row: Mapping[str, object]) -> list[str]:
    """Return missing target-impact fields for dashboard blockers."""
    flags: list[str] = []
    for field_name in (
        "relevance_category",
        "target_context_score",
        "target_company_impact_direction",
        "target_company_impact_magnitude",
        "impact_horizon",
        "causal_channel",
        "target_impact_confidence",
        "article_contribution_weight",
    ):
        if _optional_str(row.get(field_name)) is None:
            flags.append(f"missing_{field_name}")
    return flags


def _first_text(values: object) -> str | None:
    """Return first non-empty text in an iterable."""
    iterable: Iterable[object]
    if isinstance(values, Iterable) and not isinstance(values, (str, bytes, Mapping)):
        iterable = values
    else:
        iterable = [values]
    for value in iterable:
        text = _optional_str(value)
        if text is not None:
            return text
    return None


def _first_int(values: object) -> int | None:
    """Return first integer in an iterable."""
    iterable: Iterable[object]
    if isinstance(values, Iterable) and not isinstance(values, (str, bytes, Mapping)):
        iterable = values
    else:
        iterable = [values]
    for value in iterable:
        number = _maybe_float(value)
        if number is not None:
            return int(number)
    return None


def _coerce_int(value: object) -> int | None:
    """Return an int when a single value is numeric."""
    number = _maybe_float(value)
    if number is None:
        return None
    return int(number)


def _topic_relevance_missing_flags(
    *,
    has_embedding: bool,
    has_topic: bool,
    has_relevance_gate: bool,
    has_ticker_evidence: bool,
    has_entity_evidence: bool,
    has_semantic_evidence: bool,
    relevance_score: float | None,
    relevance_decisions: Sequence[str],
) -> list[str]:
    flags: list[str] = []
    lowered_decisions = {decision.lower() for decision in relevance_decisions}
    if not has_relevance_gate:
        flags.append("missing_relevance_gate")
    if not has_ticker_evidence:
        flags.append("missing_ticker_evidence")
    if not has_entity_evidence:
        flags.append("missing_entity_evidence")
    if not has_embedding:
        flags.append("missing_embedding")
    if not has_topic:
        flags.append("missing_topic_label")
    if lowered_decisions & {"rejected", "reject"}:
        flags.append("rejected_by_relevance_gate")
    if lowered_decisions & {"borderline", "review", "needs_review"}:
        flags.append("borderline_relevance_gate")
    if relevance_score == 1.0 and (
        not has_ticker_evidence
        or not has_entity_evidence
        or not has_semantic_evidence
        or lowered_decisions & {"rejected", "reject"}
    ):
        flags.append("default_relevance_without_supporting_evidence")
    return _dedupe_preserve_order(flags)


def _topic_relevance_status(
    *,
    relevance_decisions: Sequence[str],
    missing_flags: Sequence[str],
) -> str:
    lowered_flags = set(missing_flags)
    lowered_decisions = {decision.lower() for decision in relevance_decisions}
    if "default_relevance_without_supporting_evidence" in lowered_flags:
        return "missing_or_default"
    if "missing_embedding" in lowered_flags or "missing_topic_label" in lowered_flags:
        return "missing_or_default"
    if lowered_decisions & {"rejected", "reject"}:
        return "rejected"
    if lowered_decisions & {"borderline", "review", "needs_review"}:
        return "borderline"
    if lowered_decisions & {"accepted", "accept"}:
        return "accepted"
    if "missing_relevance_gate" in lowered_flags:
        return "missing_or_default"
    return "borderline"


def _relevance_score_interpretation(
    *,
    relevance_score: float | None,
    missing_flags: Sequence[str],
    relevance_decisions: Sequence[str],
) -> str:
    if relevance_score is None:
        return "missing"
    if "default_relevance_without_supporting_evidence" in set(missing_flags):
        return "default_or_unknown_not_strong_evidence"
    lowered_decisions = {decision.lower() for decision in relevance_decisions}
    if lowered_decisions & {"rejected", "reject"}:
        return "computed_rejected"
    if lowered_decisions & {"borderline", "review", "needs_review"}:
        return "computed_borderline"
    return "computed"


_DIAGNOSTIC_STATE_PRIORITY = {"PASS": 0, "WARN": 1, "NOT_RUN": 2, "NO_DATA": 2, "FAIL": 3}


def _diagnostic_worst_state(*states: object) -> str:
    worst_state = "PASS"
    worst_priority = _DIAGNOSTIC_STATE_PRIORITY[worst_state]
    for state in states:
        normalized = _diagnostic_record(str(state or "NOT_RUN"), "", reviewable=False)["state"]
        priority = _DIAGNOSTIC_STATE_PRIORITY.get(str(normalized), _DIAGNOSTIC_STATE_PRIORITY["NOT_RUN"])
        if priority > worst_priority:
            worst_state = str(normalized)
            worst_priority = priority
    return worst_state


def _topic_review_diagnostic_state(
    *,
    topic_review_rows: Sequence[Mapping[str, object]],
    topic_review_topics: Sequence[object],
    topic_review_warning: str | None,
) -> dict[str, object]:
    row_count = len(topic_review_rows)
    topic_count = len(topic_review_topics)
    labels = [_optional_str(row.get("topic_label")) for row in topic_review_rows]
    keyword_rows = sum(1 for row in topic_review_rows if _json_string_list(row.get("topic_keywords")))
    example_rows = sum(
        1
        for row in topic_review_rows
        if _optional_str(row.get("topic_example_text")) or _json_string_list(row.get("topic_example_texts"))
    )
    human_labels = sum(1 for label in labels if _has_human_readable_topic_label(label))
    metadata_labels = sum(1 for label in labels if _topic_label_looks_like_model_metadata(label))
    topic_ids = {
        int(topic_id)
        for row in topic_review_rows
        for topic_id in [_maybe_float(row.get("topic_id"))]
        if topic_id is not None
    }
    collapsed = topic_count <= 1 or len(topic_ids) <= 1
    readable = bool(topic_review_rows) and human_labels > 0 and keyword_rows > 0 and example_rows > 0
    if row_count == 0 or topic_count == 0:
        return {
            **_diagnostic_record("NO_DATA", "No topic review rows were produced.", reviewable=False),
            "topic_count": topic_count,
            "row_count": row_count,
            "topic_ids": sorted(topic_ids),
            "readable_topic_count": human_labels,
            "keyword_row_count": keyword_rows,
            "example_row_count": example_rows,
            "metadata_label_count": metadata_labels,
        }
    if not readable:
        reason = topic_review_warning or (
            "BERTopic output is collapsed or unreviewable: it needs multiple readable topics, "
            "human-friendly labels, keywords, and examples."
        )
        return {
            **_diagnostic_record("WARN", reason, reviewable=False),
            "topic_count": topic_count,
            "row_count": row_count,
            "topic_ids": sorted(topic_ids),
            "readable_topic_count": human_labels,
            "keyword_row_count": keyword_rows,
            "example_row_count": example_rows,
            "metadata_label_count": metadata_labels,
        }
    if collapsed or metadata_labels == row_count:
        reason = topic_review_warning or (
            "BERTopic output is collapsed but still readable: it needs multiple readable topics, "
            "human-friendly labels, keywords, and examples."
        )
        return {
            **_diagnostic_record("WARN", reason, reviewable=True),
            "topic_count": topic_count,
            "row_count": row_count,
            "topic_ids": sorted(topic_ids),
            "readable_topic_count": human_labels,
            "keyword_row_count": keyword_rows,
            "example_row_count": example_rows,
            "metadata_label_count": metadata_labels,
        }
    return {
        **_diagnostic_record("PASS", "Topic review has readable, varied topic evidence.", reviewable=True),
        "topic_count": topic_count,
        "row_count": row_count,
        "topic_ids": sorted(topic_ids),
        "readable_topic_count": human_labels,
        "keyword_row_count": keyword_rows,
        "example_row_count": example_rows,
        "metadata_label_count": metadata_labels,
    }
def _relevance_informativeness_diagnostic_state(
    *,
    article_count: int,
    relevance_gate_row_count: int,
    relevance_rows: Sequence[Mapping[str, object]],
) -> dict[str, object]:
    scores = [
        _maybe_float(row.get("relevance_score"))
        for row in relevance_rows
        if _maybe_float(row.get("relevance_score")) is not None
    ]
    accepted_count = sum(1 for row in relevance_rows if str(row.get("relevance_decision") or "").lower() == "accepted")
    borderline_count = sum(
        1 for row in relevance_rows if str(row.get("relevance_decision") or "").lower() in {"borderline", "review", "needs_review"}
    )
    rejected_count = sum(1 for row in relevance_rows if str(row.get("relevance_decision") or "").lower() in {"rejected", "reject"})
    default_scores = bool(scores) and all(score is not None and score >= 0.99 for score in scores)
    diverse_decisions = sum(1 for count in (accepted_count, borderline_count, rejected_count) if count > 0)
    if article_count == 0 or relevance_gate_row_count == 0:
        return {
            **_diagnostic_record("NO_DATA", "No pre-FinBERT relevance rows were provided.", reviewable=False),
            "coverage": "absent",
            "article_count": article_count,
            "relevance_gate_row_count": relevance_gate_row_count,
            "accepted_count": accepted_count,
            "borderline_count": borderline_count,
            "rejected_count": rejected_count,
            "default_score_count": sum(1 for score in scores if score is not None and score >= 0.99),
        }
    if relevance_gate_row_count < article_count:
        return {
            **_diagnostic_record(
                "WARN",
                "Relevance-gate rows are missing for part of the selected ticker/date slice.",
                reviewable=False,
            ),
            "coverage": "partial",
            "article_count": article_count,
            "relevance_gate_row_count": relevance_gate_row_count,
            "accepted_count": accepted_count,
            "borderline_count": borderline_count,
            "rejected_count": rejected_count,
            "default_score_count": sum(1 for score in scores if score is not None and score >= 0.99),
        }
    if default_scores or diverse_decisions <= 1:
        return {
            **_diagnostic_record(
                "WARN",
                "Relevance scores are all default-like or do not show accepted/borderline/rejected diversity.",
                reviewable=False,
            ),
            "coverage": "complete",
            "article_count": article_count,
            "relevance_gate_row_count": relevance_gate_row_count,
            "accepted_count": accepted_count,
            "borderline_count": borderline_count,
            "rejected_count": rejected_count,
            "default_score_count": sum(1 for score in scores if score is not None and score >= 0.99),
        }
    return {
        **_diagnostic_record("PASS", "Relevance rows show informative, non-default evidence.", reviewable=True),
        "coverage": "complete",
        "article_count": article_count,
        "relevance_gate_row_count": relevance_gate_row_count,
        "accepted_count": accepted_count,
        "borderline_count": borderline_count,
        "rejected_count": rejected_count,
        "default_score_count": sum(1 for score in scores if score >= 0.99),
    }


def _embedding_coverage_diagnostic_state(*, article_count: int, embedding_row_count: int) -> dict[str, object]:
    if article_count == 0 or embedding_row_count == 0:
        return {
            **_diagnostic_record("NO_DATA", "No embedding rows were provided for the selected slice.", reviewable=False),
            "scope": "no_data",
            "article_count": article_count,
            "embedding_row_count": embedding_row_count,
        }
    if embedding_row_count == article_count:
        return {
            **_diagnostic_record(
                "PASS",
                "Embedding rows match the selected ticker/day slice.",
                reviewable=True,
            ),
            "scope": "selected_ticker_day",
            "article_count": article_count,
            "embedding_row_count": embedding_row_count,
        }
    scope = "packet_or_full_corpus" if embedding_row_count > article_count else "partial_selected_ticker_day"
    if embedding_row_count > article_count:
        return {
            **_diagnostic_record(
                "WARN",
                "Embedding rows cover a broader packet/full corpus than the selected article slice.",
                reviewable=True,
            ),
            "scope": scope,
            "article_count": article_count,
            "embedding_row_count": embedding_row_count,
        }
    return {
        **_diagnostic_record(
            "WARN",
            "Embedding rows are missing for part of the selected ticker/date slice.",
            reviewable=False,
        ),
        "scope": scope,
        "article_count": article_count,
        "embedding_row_count": embedding_row_count,
    }


def _topic_relevance_reviewability_summary(
    *,
    article_count: int,
    relevance_gate_row_count: int,
    embedding_row_count: int,
    topic_label_row_count: int,
    topic_review_rows: Sequence[Mapping[str, object]],
    topic_review_topics: Sequence[object],
    topic_review_warning: str | None,
    relevance_rows: Sequence[Mapping[str, object]],
) -> dict[str, object]:
    topic_review_state = _topic_review_diagnostic_state(
        topic_review_rows=topic_review_rows,
        topic_review_topics=topic_review_topics,
        topic_review_warning=topic_review_warning,
    )
    relevance_state = _relevance_informativeness_diagnostic_state(
        article_count=article_count,
        relevance_gate_row_count=relevance_gate_row_count,
        relevance_rows=relevance_rows,
    )
    embedding_state = _embedding_coverage_diagnostic_state(
        article_count=article_count,
        embedding_row_count=embedding_row_count,
    )
    topic_label_scope = "complete" if topic_label_row_count else "absent"
    reviewable = bool(
        topic_review_state["reviewable"]
        and relevance_state["reviewable"]
        and embedding_state["reviewable"]
        and article_count > 0
    )
    overall_state = _diagnostic_worst_state(
        topic_review_state["state"],
        relevance_state["state"],
        embedding_state["state"],
    )
    if article_count == 0:
        overall_state = "NO_DATA"
    if reviewable:
        review_status = "reviewable"
        review_explanation = (
            "Topic labels are readable, relevance scores are informative, and embedding coverage "
            "matches the selected ticker/date slice."
        )
    elif topic_review_state["state"] == "NO_DATA":
        review_status = "not_run_topic_review"
        review_explanation = str(topic_review_state["reason"])
    elif relevance_state["state"] == "NO_DATA":
        review_status = "not_run_relevance_gate"
        review_explanation = str(relevance_state["reason"])
    elif embedding_state["state"] == "NO_DATA":
        review_status = "not_run_embedding_coverage"
        review_explanation = str(embedding_state["reason"])
    elif topic_review_state["state"] == "WARN":
        review_status = "not_reviewable_collapsed_topic_output"
        review_explanation = str(topic_review_state["reason"])
    elif relevance_state["state"] == "WARN":
        review_status = "not_reviewable_uninformative_relevance"
        review_explanation = str(relevance_state["reason"])
    else:
        review_status = "not_reviewable_unclear_embedding_scope"
        review_explanation = str(embedding_state["reason"])
    return {
        "reviewable": reviewable,
        "review_status": review_status,
        "review_explanation": review_explanation,
        "diagnostic_state": overall_state,
        "topic_review_state": topic_review_state,
        "relevance_informativeness_state": relevance_state,
        "embedding_coverage_state": embedding_state,
        "topic_label_scope": topic_label_scope,
    }


def _semantic_aggregate_review_row(row: Mapping[str, object]) -> dict[str, object]:
    features = _json_mapping(row.get("features"))
    target_summary = _semantic_aggregate_target_impact_summary(features)
    sentiment_score = _maybe_float(features.get("nlp_sentiment_score"))
    sentiment_label = _sentiment_label_from_score(sentiment_score)
    cards = _semantic_aggregate_review_cards(features)
    return {
        **dict(row),
        **target_summary,
        "sentiment_label": sentiment_label,
        "human_review_summary": _semantic_aggregate_human_summary(
            sentiment_label=sentiment_label,
            sentiment_score=sentiment_score,
            article_count=_maybe_float(features.get("nlp_article_count")),
            sentence_count=_maybe_float(features.get("nlp_sentence_count")),
        ),
        "review_value_cards": cards,
    }


def _semantic_aggregate_target_impact_summary(features: Mapping[str, object]) -> dict[str, object]:
    """Return target-impact fields and warnings for one semantic aggregate row."""
    relationship_to_target = _first_text(
        [
            features.get("nlp_relevance_category"),
            features.get("relevance_category"),
        ]
    )
    target_context_score = _first_float(
        [features.get("nlp_target_context_score"), features.get("target_context_score")]
    )
    document_sentiment = _first_text(
        [features.get("nlp_document_sentiment"), features.get("document_sentiment")]
    )
    target_company_impact_direction = _first_text(
        [features.get("nlp_target_impact_direction"), features.get("target_company_impact_direction")]
    )
    target_company_impact_magnitude = _first_text(
        [features.get("nlp_target_impact_magnitude"), features.get("target_company_impact_magnitude")]
    )
    impact_horizon = _first_text([features.get("nlp_target_impact_horizon"), features.get("impact_horizon")])
    causal_channel = _first_text([features.get("nlp_causal_channel"), features.get("causal_channel")])
    target_impact_confidence = _first_float(
        [features.get("nlp_target_impact_confidence"), features.get("target_impact_confidence")]
    )
    included_in_signal = _maybe_bool(features.get("included_in_signal"))
    final_contribution = _first_float([features.get("final_contribution"), features.get("final_signal_contribution")])
    final_signal_contribution = _first_float([features.get("final_signal_contribution"), features.get("final_contribution")])
    target_impact_missing_flags: list[str] = []
    for field_name, value in (
        ("relationship_to_target", relationship_to_target),
        ("target_context_score", target_context_score),
        ("document_sentiment", document_sentiment),
        ("target_company_impact_direction", target_company_impact_direction),
        ("target_company_impact_magnitude", target_company_impact_magnitude),
        ("impact_horizon", impact_horizon),
        ("causal_channel", causal_channel),
        ("target_impact_confidence", target_impact_confidence),
    ):
        if value is None:
            target_impact_missing_flags.append(f"missing_{field_name}")
    source_count = _first_int([features.get("nlp_source_count"), features.get("source_count")])
    dominant_source_weight_share = _first_float(
        [features.get("nlp_dominant_source_weight_share"), features.get("dominant_source_weight_share")]
    )
    single_source_concentration = bool(
        source_count == 1 or (dominant_source_weight_share is not None and dominant_source_weight_share >= 0.99)
    )
    semantic_warning_codes = _dedupe_preserve_order(
        _json_string_list(features.get("nlp_semantic_warning_codes"))
    )
    if single_source_concentration and "single_source_concentration" not in semantic_warning_codes:
        semantic_warning_codes.append("single_source_concentration")
    target_impact_status = "derived" if target_company_impact_direction or target_context_score is not None else "unclear"
    target_impact_warning = (
        None
        if target_impact_status == "derived"
        else "All row-level target-impact evidence is unclear or missing for this aggregate row."
    )
    if target_impact_warning and "unclear_target_company_impact_direction" not in semantic_warning_codes:
        semantic_warning_codes.append("unclear_target_company_impact_direction")
    if target_impact_confidence is None and "missing_target_impact_confidence" not in semantic_warning_codes:
        semantic_warning_codes.append("missing_target_impact_confidence")
    return {
        "relationship_to_target": relationship_to_target,
        "target_context_score": target_context_score,
        "document_sentiment": document_sentiment,
        "target_company_impact_direction": target_company_impact_direction,
        "target_company_impact_magnitude": target_company_impact_magnitude,
        "impact_horizon": impact_horizon,
        "causal_channel": causal_channel,
        "target_impact_confidence": target_impact_confidence,
        "included_in_signal": included_in_signal,
        "final_contribution": final_contribution,
        "final_signal_contribution": final_signal_contribution,
        "target_impact_missing_flags": target_impact_missing_flags,
        "source_count": source_count,
        "dominant_source_weight_share": dominant_source_weight_share,
        "single_source_concentration": single_source_concentration,
        "semantic_warning_codes": semantic_warning_codes,
        "target_impact_status": target_impact_status,
        "target_impact_warning": target_impact_warning,
    }


def _hmm_feature_set_summary(payload: Mapping[str, object]) -> dict[str, object]:
    """Return a human-readable HMM feature-set degradation summary."""
    context = _json_mapping(payload.get("hmm_evaluation_context"))
    warning_codes = _dedupe_preserve_order(_json_string_list(context.get("warnings")))
    feature_set_blocking = _maybe_bool(context.get("feature_set_blocking"))
    if feature_set_blocking is None:
        feature_set_blocking = "incomplete_hmm_feature_set" in warning_codes
    degraded = bool(warning_codes)
    if not degraded:
        return {
            "state": "PASS",
            "reason": "HMM feature set is ready.",
            "reviewable": True,
            "warning_codes": [],
            "blocking": bool(feature_set_blocking),
        }
    if feature_set_blocking:
        return {
            "state": "WARN",
            "reason": "HMM feature set is degraded and blocks readiness.",
            "reviewable": False,
            "warning_codes": warning_codes,
            "blocking": True,
        }
    return {
        "state": "WARN",
        "reason": "HMM feature set is degraded but non-blocking.",
        "reviewable": True,
        "warning_codes": warning_codes,
        "blocking": False,
    }


def _sentiment_label_from_score(score: float | None) -> str:
    if score is None:
        return "unknown"
    if score > 0.05:
        return "positive"
    if score < -0.05:
        return "negative"
    return "neutral"


def _semantic_aggregate_human_summary(
    *,
    sentiment_label: str,
    sentiment_score: float | None,
    article_count: float | None,
    sentence_count: float | None,
) -> str:
    return (
        f"Overall NLP sentiment is {sentiment_label} "
        f"(score={_display_number(sentiment_score, 3)}) from "
        f"{_display_number(article_count, 0)} article(s) and "
        f"{_display_number(sentence_count, 0)} sentence/chunk row(s)."
    )


def _semantic_aggregate_review_cards(features: Mapping[str, object]) -> list[dict[str, object]]:
    cards: list[dict[str, object]] = []
    target_direction = _first_text(
        [features.get("nlp_target_impact_direction"), features.get("target_company_impact_direction")]
    )
    if target_direction is not None:
        cards.append(
            {
                "label": "Target impact direction",
                "value": target_direction,
                "field": "features.nlp_target_impact_direction",
            }
        )
    target_magnitude = _first_text(
        [features.get("nlp_target_impact_magnitude"), features.get("target_company_impact_magnitude")]
    )
    if target_magnitude is not None:
        cards.append(
            {
                "label": "Target impact magnitude",
                "value": target_magnitude,
                "field": "features.nlp_target_impact_magnitude",
            }
        )
    target_confidence = _first_float([features.get("nlp_target_impact_confidence"), features.get("target_impact_confidence")])
    if target_confidence is not None:
        cards.append(
            {
                "label": "Target impact confidence",
                "value": _display_number(target_confidence, 3),
                "field": "features.nlp_target_impact_confidence",
            }
        )
    target_channel = _first_text([features.get("nlp_causal_channel"), features.get("causal_channel")])
    if target_channel is not None:
        cards.append(
            {
                "label": "Causal channel",
                "value": target_channel,
                "field": "features.nlp_causal_channel",
            }
        )
    target_horizon = _first_text([features.get("nlp_target_impact_horizon"), features.get("impact_horizon")])
    if target_horizon is not None:
        cards.append(
            {
                "label": "Impact horizon",
                "value": target_horizon,
                "field": "features.nlp_target_impact_horizon",
            }
        )
    sentiment_score = _maybe_float(features.get("nlp_sentiment_score"))
    if sentiment_score is not None:
        cards.append(
            {
                "label": "Overall sentiment",
                "value": _display_number(sentiment_score, 3),
                "field": "features.nlp_sentiment_score",
            }
        )
    mix_values = [
        _maybe_float(features.get("nlp_sentiment_positive")),
        _maybe_float(features.get("nlp_sentiment_negative")),
        _maybe_float(features.get("nlp_sentiment_neutral")),
    ]
    if all(value is not None for value in mix_values):
        cards.append(
            {
                "label": "Positive / negative / neutral mix",
                "value": " / ".join(_display_number(value, 3) for value in mix_values),
                "field": "features.nlp_sentiment_positive / negative / neutral",
            }
        )
    article_count = _maybe_float(features.get("nlp_article_count"))
    sentence_count = _maybe_float(features.get("nlp_sentence_count"))
    if article_count is not None or sentence_count is not None:
        cards.append(
            {
                "label": "Articles / sentences",
                "value": (
                    f"{_display_number(article_count, 0)} / "
                    f"{_display_number(sentence_count, 0)}"
                ),
                "field": "features.nlp_article_count / features.nlp_sentence_count",
            }
        )
    relevance_score = _maybe_float(features.get("nlp_relevance_score"))
    if relevance_score is not None:
        cards.append(
            {
                "label": "Relevance score",
                "value": _display_number(relevance_score, 3),
                "field": "features.nlp_relevance_score",
            }
        )
    source_count = _first_int([features.get("nlp_source_count"), features.get("source_count")])
    dominant_share = _first_float(
        [features.get("nlp_dominant_source_weight_share"), features.get("dominant_source_weight_share")]
    )
    if source_count is not None or dominant_share is not None:
        cards.append(
            {
                "label": "Source concentration",
                "value": (
                    f"{source_count if source_count is not None else 'n/a'} source(s) / "
                    f"{_display_number(dominant_share, 3)} share"
                ),
                "field": "features.nlp_source_count / features.nlp_dominant_source_weight_share",
            }
        )
    return cards


def _display_number(value: float | None, decimals: int) -> str:
    if value is None:
        return "n/a"
    if decimals == 0:
        return str(int(round(value)))
    return f"{value:.{decimals}f}"


def _rows_by_article(value: object) -> dict[tuple[str, str], list[dict[str, object]]]:
    rows_by_article: dict[tuple[str, str], list[dict[str, object]]] = {}
    for row in _json_list(value):
        if not isinstance(row, Mapping):
            continue
        row_dict = dict(row)
        key = _article_key(row_dict)
        if key == ("", ""):
            continue
        rows_by_article.setdefault(key, []).append(row_dict)
    return rows_by_article


def _article_key(row: Mapping[str, object]) -> tuple[str, str]:
    date_text = str(row.get("date") or "")
    article_id = str(row.get("article_id") or "")
    return date_text, article_id


def _ticker_evidence_values(value: object) -> list[str]:
    evidence = _json_mapping(value)
    values: list[str] = []
    for key in ("source_tickers", "article_tickers", "ticker_mentions", "chunk_tickers"):
        values.extend(_json_string_list(evidence.get(key)))
    return values


def _first_assignment_provenance_fields(
    rows: Sequence[Mapping[str, object]],
) -> dict[str, object]:
    """Return the first available assignment provenance fields from related rows."""
    for row in rows:
        assignment_classification = _optional_str(row.get("assignment_classification"))
        assignment_reason = _optional_str(row.get("assignment_reason"))
        assignment_weight = _maybe_float(row.get("assignment_weight"))
        assignment_evidence_kinds = _dedupe_preserve_order(
            _json_string_list(row.get("assignment_evidence_kinds"))
        )
        if (
            assignment_classification is None
            and assignment_reason is None
            and assignment_weight is None
            and not assignment_evidence_kinds
        ):
            continue
        return {
            "assignment_classification": assignment_classification,
            "assignment_reason": assignment_reason,
            "assignment_weight": assignment_weight,
            "assignment_evidence_kinds": assignment_evidence_kinds,
        }
    return {
        "assignment_classification": None,
        "assignment_reason": None,
        "assignment_weight": None,
        "assignment_evidence_kinds": [],
    }


def _first_float(values: object) -> float | None:
    iterable: Iterable[object]
    if isinstance(values, Iterable) and not isinstance(values, (str, bytes, Mapping)):
        iterable = values
    else:
        iterable = [values]
    for value in iterable:
        number = _maybe_float(value)
        if number is not None:
            return number
    return None


def _maybe_float(value: object) -> float | None:
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if number != number:
        return None
    return number


def _summary_cards(run_readiness: Mapping[str, object]) -> list[dict[str, object]]:
    return [
        {"label": "Run ID", "value": run_readiness.get("run_id") or "n/a", "field": "run_id"},
        {"label": "Ticker", "value": run_readiness.get("ticker") or "n/a", "field": "ticker"},
        {
            "label": "Date range",
            "value": (
                f"{run_readiness.get('from_date') or 'n/a'} to "
                f"{run_readiness.get('to_date') or 'n/a'}"
            ),
            "field": "from_date,to_date",
        },
        {
            "label": "Recommendation",
            "value": run_readiness.get("recommendation") or "n/a",
            "field": "recommendation",
        },
        {
            "label": "Human review",
            "value": run_readiness.get("human_review_status") or "n/a",
            "field": "human_review_status",
        },
        {
            "label": "Sentence rows",
            "value": int(run_readiness.get("sentence_row_count") or 0),
            "field": "sentence_row_count",
        },
        {
            "label": "Articles",
            "value": int(run_readiness.get("article_count") or 0),
            "field": "article_count",
        },
        {
            "label": "Dates",
            "value": int(run_readiness.get("date_count") or 0),
            "field": "date_count",
        },
        {
            "label": "Accepted",
            "value": int(run_readiness.get("accepted_article_count") or 0),
            "field": "accepted_article_count",
        },
        {
            "label": "Flagged",
            "value": int(run_readiness.get("flagged_article_count") or 0),
            "field": "flagged_article_count",
        },
    ]


def _gate_message(
    label: str,
    status: str,
    row_count: int,
    failures: Sequence[Mapping[str, object]],
) -> str:
    if status == "ready":
        return f"{label} is present with {row_count} row(s)."
    if failures:
        reasons = ", ".join(
            _dedupe_preserve_order(
                str(failure.get("reason"))
                for failure in failures
                if failure.get("reason") is not None
            )
        )
        if reasons:
            return f"{label} is blocked: {reasons}."
    return f"{label} is blocked because required rows are missing."


def _readiness_status_reason(ready_for_final_acceptance: bool, blocker_reasons: Sequence[str] | None = None) -> str:
    if ready_for_final_acceptance:
        return "Required Layer 1 NLP, HMM, and price evidence is present for review."
    reasons = [str(reason) for reason in blocker_reasons or [] if str(reason)]
    if reasons:
        return "Readiness is blocked: " + "; ".join(reasons)
    return (
        "Required Layer 1 NLP, HMM, or price evidence is missing, so human semantic "
        "review remains blocked."
    )


def _section_rows(payload: Mapping[str, object], section_key: str) -> list[object]:
    top_level = payload.get(section_key)
    if isinstance(top_level, Sequence) and not isinstance(top_level, (bytes, bytearray, str)):
        return list(top_level)
    sections = _json_mapping(payload.get("pipeline_sections"))
    rows = sections.get(section_key)
    if isinstance(rows, Sequence) and not isinstance(rows, (bytes, bytearray, str)):
        return list(rows)
    return []


def _failures_by_stage(
    failures: Sequence[Mapping[str, object]],
) -> dict[str, list[dict[str, object]]]:
    grouped: dict[str, list[dict[str, object]]] = {}
    for failure in failures:
        stage = failure.get("stage")
        if stage is None:
            continue
        grouped.setdefault(str(stage), []).append(dict(failure))
    return grouped


def _json_mapping(value: Any) -> dict[str, object]:
    if isinstance(value, Mapping):
        return {str(key): item for key, item in value.items()}
    return {}


def _json_list(value: Any) -> list[object]:
    if isinstance(value, Sequence) and not isinstance(value, (bytes, bytearray, str)):
        return list(value)
    return []


def _json_string_list(value: Any) -> list[str]:
    if isinstance(value, str):
        return [value] if value else []
    if isinstance(value, Sequence) and not isinstance(value, (bytes, bytearray, str)):
        return [str(item) for item in value if str(item)]
    return []


def _optional_str(value: Any) -> str | None:
    """Return a stripped string when a value is present."""
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _effective_row_count(sample_count: int, full_count: object) -> int:
    """Return the best available row count, preferring explicit full-count metadata."""
    if sample_count <= 0:
        return 0
    try:
        parsed_full_count = int(full_count)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        parsed_full_count = None
    if parsed_full_count is None:
        return sample_count
    return max(sample_count, parsed_full_count)


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


def _dedupe_preserve_order(items: Sequence[str]) -> list[str]:
    seen: set[str] = set()
    values: list[str] = []
    for item in items:
        if item in seen:
            continue
        seen.add(item)
        values.append(item)
    return values


def _normalize_reason_text(value: str) -> str:
    """Collapse duplicated reason text that may appear after formatting."""
    text = " ".join(value.split())
    if ": " in text:
        left, right = text.split(": ", 1)
        if left.rstrip(" .") == right.rstrip(" ."):
            return f"{left.rstrip(' .')}."
    return text


_MISSING = object()

# B02-R2-001: the strict API contract is that the final pretty-encoded payload stays
# under 200,000 UTF-8 bytes for arbitrary valid Mapping input. Generic per-node
# bounds plus the final pretty-byte budget below make that contract hold on every
# retained scalar/key route instead of only the expected fixture shapes.
_PAYLOAD_PRETTY_BYTE_BUDGET = 200_000
_PAYLOAD_BUDGET_METADATA_RESERVE = 1_024
_PAYLOAD_TARGET_PRETTY_BYTES = _PAYLOAD_PRETTY_BYTE_BUDGET - _PAYLOAD_BUDGET_METADATA_RESERVE
_PAYLOAD_STRING_CHARACTER_LIMIT = 256
_PAYLOAD_MAPPING_KEY_LIMIT = 96
_PAYLOAD_TOP_LEVEL_KEY_LIMIT = 256
_PAYLOAD_KEY_NAME_LIMIT = 96
_PAYLOAD_MAX_DEPTH = 5
_PAYLOAD_SEQUENCE_ITEM_LIMIT = 32
_PAYLOAD_NUMBER_REPR_LIMIT = 64
_PAYLOAD_COMPACTION_PASSES = 12
_PAYLOAD_SHRINK_FLOOR = 24
_PAYLOAD_PRESERVED_STATUS_LIMIT = 256
_PAYLOAD_NODE_FLOOR = 2
_PAYLOAD_COMPACTABLE_ITEM_FLOOR = 2
_PAYLOAD_SACRIFICED_SECTION_LIST_LIMIT = 8
_HMM_CONTEXT_SCALAR_SAMPLE_LIMIT = 96
_PAYLOAD_IMMUTABLE_TOP_LEVEL_KEYS = frozenset(
    {
        "ticker", "requested_ticker", "controls", "hmm_evaluation_context",
        "smoke", "run_readiness", "pipeline_section_counts",
        "article_group_counts", "accepted_article_counts", "flagged_article_counts",
        "date_group_counts", "warnings_counts", "human_review_queue",
    }
)


def _payload_compaction_marker(field: str, value: int) -> dict[str, object]:
    """Return an explicit deterministic marker for bounded dropped content."""
    return {field: value, "truncated": True, "payload_compaction_marker": True}


def _pretty_payload_size(candidate: object) -> int:
    """Return the strict contract metric: pretty sorted UTF-8 JSON byte length."""
    return len(json.dumps(candidate, indent=2, sort_keys=True, default=str).encode("utf-8"))


def _shrink_payload_node(node: object, depth: int = 0, key: str = "") -> tuple[object, int, int]:
    """Halve oversized keys, items, and strings; return (node, dropped, truncated)."""
    dropped = 0
    truncated = 0
    if isinstance(node, Mapping):
        raw_keys = sorted(node, key=str)
        total_keys = len(raw_keys)
        if total_keys > _PAYLOAD_NODE_FLOOR and (depth or total_keys > _PAYLOAD_MAPPING_KEY_LIMIT):
            keep = max(_PAYLOAD_NODE_FLOOR, -(-total_keys // 2))
            # Preserve identity fields on diagnostic rows while compacting their
            # lower-value detail.  In particular, ``key`` is the stable route
            # used by the readiness UI and must not disappear from a row merely
            # because the final byte-budget pass is active.
            priority_keys = [
                candidate
                for candidate in (
                    "key", "label", "scope", "status", "ready_for_final_human_acceptance",
                    "recommendation", "human_review_status", "overall_state",
                )
                if candidate in node
            ]
            keep = max(keep, len(priority_keys))
            kept_keys = list(dict.fromkeys(priority_keys + raw_keys[:keep]))[:keep]
            dropped += total_keys - len(kept_keys)
            raw_keys = kept_keys
        result: dict[str, object] = {}
        for raw_key in raw_keys:
            name = str(raw_key)
            if len(name) > _PAYLOAD_KEY_NAME_LIMIT:
                result[f"{name[:_PAYLOAD_KEY_NAME_LIMIT]}..."] = None
                result[f"key_truncated_{name[:_PAYLOAD_KEY_NAME_LIMIT]}"] = len(name)
                truncated += 1
                continue
            child, child_dropped, child_truncated = _shrink_payload_node(
                node[raw_key], depth + 1, name
            )
            result[name] = child
            dropped += child_dropped
            truncated += child_truncated
        if dropped:
            result["payload_compaction_omitted_node_count"] = dropped
        return result, dropped, truncated
    if isinstance(node, Sequence) and not isinstance(node, (str, bytes, bytearray)):
        items = list(node)
        # Readiness section identities are small but semantically complete; do
        # not replace missing-section rows with an anonymous compaction marker.
        preserve_rows = key == "missing_pipeline_sections"
        if len(items) > _PAYLOAD_COMPACTABLE_ITEM_FLOOR and not preserve_rows:
            keep = max(_PAYLOAD_COMPACTABLE_ITEM_FLOOR, -(-len(items) // 2))
            dropped += len(items) - keep
            items = items[:keep]
        shrunk: list[object] = []
        for item in items:
            child, child_dropped, child_truncated = _shrink_payload_node(item, depth + 1, key)
            shrunk.append(child)
            dropped += child_dropped
            truncated += child_truncated
        if dropped and not preserve_rows:
            shrunk.append(_payload_compaction_marker("payload_compaction_omitted_item_count", dropped))
        return shrunk, dropped, truncated
    if isinstance(node, str) and key in {
        "recommendation", "human_review_status", "status", "state", "reason",
    } and len(node) <= _PAYLOAD_PRESERVED_STATUS_LIMIT:
        return node, 0, 0
    if isinstance(node, str) and len(node) > _PAYLOAD_SHRINK_FLOOR:
        keep = max(_PAYLOAD_SHRINK_FLOOR, len(node) // 2)
        return f"{node[:keep]}...", 0, 1
    return node, 0, 0


def _enforce_payload_pretty_byte_budget(payload: Mapping[str, object]) -> dict[str, object]:
    """Enforce the strict pretty-JSON byte budget at the final public-builder boundary.

    The generic per-node bounds keep legitimate payloads inside the budget, so the
    compaction loop below never fires for normal reports. For arbitrary Mapping
    input it repeatedly halves oversized branches deterministically (sorted keys,
    canonical list order) until the pretty payload fits; because every pass strictly
    reduces encoded volume toward a constant skeleton, the loop converges. Dropped
    content is reported through ``payload_budget`` metadata and inline
    ``payload_compaction_*`` markers instead of being silently hidden.
    """
    import copy as _copy

    result = _copy.deepcopy(dict(payload))
    initial_size = _pretty_payload_size(result)
    compacted = False
    passes = 0
    dropped_nodes = 0
    truncated_scalars = 0
    sacrificed_sections: list[str] = []
    while _pretty_payload_size(result) >= _PAYLOAD_TARGET_PRETTY_BYTES and passes < _PAYLOAD_COMPACTION_PASSES:
        compacted = True
        passes += 1
        if passes == _PAYLOAD_COMPACTION_PASSES // 2:
            # Mid-loop accelerator: drop whole lowest-value sample sections first;
            # authoritative *_counts metadata and readiness blocks remain.
            section_keys = sorted(
                key for key in result
                if isinstance(result.get(key), list)
                and key not in _PAYLOAD_IMMUTABLE_TOP_LEVEL_KEYS
                and not str(key).endswith("_counts")
            )
            sacrificed_sections = []
            for key in reversed(section_keys):
                result.pop(key, None)
                if len(sacrificed_sections) < _PAYLOAD_SACRIFICED_SECTION_LIST_LIMIT:
                    sacrificed_sections.append(str(key))
                elif sacrificed_sections[-1] != "payload_compaction_more_sections":
                    sacrificed_sections.append("payload_compaction_more_sections")
                if _pretty_payload_size(result) < _PAYLOAD_TARGET_PRETTY_BYTES:
                    break
            continue
        shrunk, dropped, truncated = _shrink_payload_node(result)
        result = cast(dict[str, object], shrunk)
        dropped_nodes += dropped
        truncated_scalars += truncated

    final_size = _pretty_payload_size(result)
    result["payload_budget"] = {
        "pretty_utf8_byte_budget": _PAYLOAD_PRETTY_BYTE_BUDGET,
        "initial_pretty_utf8_bytes": initial_size,
        "final_pretty_utf8_bytes": final_size,
        "within_budget": final_size < _PAYLOAD_PRETTY_BYTE_BUDGET,
        "compacted": compacted,
        "compaction_passes": passes,
        "omitted_node_count": dropped_nodes,
        "truncated_scalar_count": truncated_scalars,
        "sacrificed_sections": sacrificed_sections,
    }
    return result

_PIPELINE_SECTION_SAMPLE_LIMITS: dict[str, int] = {
    "raw_preprocessing_rows": 1,
    "article_embedding_rows": 1,
    "topic_label_rows": 1,
    "relevance_gate_rows": 1,
    "finbert_sentence_rows": 1,
    "semantic_aggregate_rows": 2,
    "date_level_regime_rows": 2,
    "stock_price_rows": 2,
    "date_aligned_price_hmm_rows": 2,
}
_CHART_SAMPLE_LIMIT = 32
_WARNING_SAMPLE_LIMIT = 12
_ARTICLE_DETAIL_SAMPLE_LIMIT = 1
_FINBERT_SENTENCE_SAMPLE_LIMIT = 3
_FULL_TEXT_PREVIEW_LIMIT = 280
_TOP_LEVEL_ARTICLE_SAMPLE_LIMIT = 25
_TOP_LEVEL_DATE_SAMPLE_LIMIT = 50
_UNKNOWN_PIPELINE_SECTION_LIMIT = 4
_MISSING = object()
_PROSE_CHARACTER_LIMIT = 240
_HMM_CONTEXT_LIST_LIMIT = 8
_HMM_CONTEXT_SCALAR_KEYS = frozenset(
    {
        "requested_inference_dates", "observed_inference_dates", "missing_inference_dates",
        "stale_manifest_dates", "warnings", "warning_codes", "degradation_state",
        "feature_set_blocking", "feature_set_name", "source_manifest_keys",
        "requested_from_date", "requested_to_date", "observed_from_date", "observed_to_date",
    }
)
_HMM_CONTEXT_LIST_KEYS = frozenset(
    {"manifest_summaries", "training_windows", "requested_inference_dates", "observed_inference_dates",
     "missing_inference_dates", "stale_manifest_dates", "warnings", "warning_codes", "source_manifest_keys"}
)
_HMM_ROW_KEYS = frozenset(
    {
        "date", "from_date", "to_date", "train_start_date", "train_end_date", "inference_date",
        "requested_date", "observed_date", "run_id", "stage", "status", "state", "warning",
        "warnings", "warning_codes", "degradation_state", "feature_set_blocking", "feature_set_name",
        "artifact_key", "manifest_key", "source_manifest_key", "model_version", "model", "ticker",
    }
)


def _compact_hmm_evaluation_context(value: object) -> dict[str, object]:
    """Keep auditable HMM dates, warnings, manifests, and windows within fixed bounds."""
    context = _json_mapping(value)
    compact: dict[str, object] = {}
    for key in sorted(context):
        if key not in _HMM_CONTEXT_SCALAR_KEYS and key not in _HMM_CONTEXT_LIST_KEYS:
            continue
        raw = context[key]
        if key in {"manifest_summaries", "training_windows"}:
            rows = [dict(item) for item in _json_list(raw) if isinstance(item, Mapping)]
            rows.sort(key=_stable_mapping_key)
            sample = [
                {
                    str(row_key): _bound_json_value(row_value, key=str(row_key), depth=1)
                    for row_key, row_value in sorted(row.items(), key=lambda item: str(item[0]))
                    if str(row_key) in _HMM_ROW_KEYS
                }
                for row in rows[:_HMM_CONTEXT_LIST_LIMIT]
            ]
            compact[key] = sample
            compact[f"{key}_counts"] = _sampling_counts(len(rows), len(sample))
        elif key in _HMM_CONTEXT_LIST_KEYS:
            raw_items = _json_string_list(raw)
            items = sorted(raw_items)[:_HMM_CONTEXT_LIST_LIMIT]
            compact[key] = [_bound_json_value(item, key=key) for item in items]
            counts = _sampling_counts(len(raw_items), len(items))
            truncated_item_count = sum(
                1 for item in items if len(item) > _PAYLOAD_STRING_CHARACTER_LIMIT
            )
            counts["truncated_item_count"] = truncated_item_count
            counts["item_truncated"] = truncated_item_count > 0
            compact[f"{key}_counts"] = counts
        else:
            compact[key] = _bound_json_value(raw, key=key)
    return compact


def _compact_layer1_semantic_review_dashboard_payload(payload: Mapping[str, object]) -> dict[str, object]:
    """Return a bounded dashboard payload with representative detail samples."""
    compact: dict[str, object] = dict(payload)
    report = _json_mapping(compact.pop("report", None))
    if report:
        compact["report_summary"] = {
            "run_id": report.get("run_id"),
            "ticker": report.get("ticker"),
            "from_date": report.get("from_date"),
            "to_date": report.get("to_date"),
            "row_count": report.get("row_count"),
            "article_count": report.get("article_count"),
            "date_count": report.get("date_count"),
            "summary": _json_mapping(report.get("summary")),
            "artifact_keys": _json_mapping(report.get("artifact_keys")),
        }
    if "artifact_keys" in compact:
        compact["artifact_keys"] = _bounded_artifact_keys(compact.get("artifact_keys"))
    if "hmm_evaluation_context" in compact:
        compact["hmm_evaluation_context"] = _compact_hmm_evaluation_context(
            compact.get("hmm_evaluation_context")
        )

    article_groups = [dict(item) for item in _json_list(compact.get("article_groups")) if isinstance(item, Mapping)]
    accepted_articles = [
        dict(item) for item in _json_list(compact.get("accepted_articles")) if isinstance(item, Mapping)
    ]
    flagged_articles = [
        dict(item) for item in _json_list(compact.get("flagged_articles")) if isinstance(item, Mapping)
    ]
    compact["article_group_counts"] = {
        "full_count": len(article_groups),
        "sample_count": min(len(article_groups), _TOP_LEVEL_ARTICLE_SAMPLE_LIMIT),
        "omitted_count": max(0, len(article_groups) - _TOP_LEVEL_ARTICLE_SAMPLE_LIMIT),
        "truncated": len(article_groups) > _TOP_LEVEL_ARTICLE_SAMPLE_LIMIT,
    }
    compact["article_groups"] = [
        _compact_article_summary_row(article)
        for article in article_groups[:_TOP_LEVEL_ARTICLE_SAMPLE_LIMIT]
    ]
    compact["accepted_article_counts"] = {
        "full_count": len(accepted_articles),
        "sample_count": min(len(accepted_articles), _TOP_LEVEL_ARTICLE_SAMPLE_LIMIT),
        "omitted_count": max(0, len(accepted_articles) - _TOP_LEVEL_ARTICLE_SAMPLE_LIMIT),
        "truncated": len(accepted_articles) > _TOP_LEVEL_ARTICLE_SAMPLE_LIMIT,
    }
    compact["accepted_articles"] = [
        _compact_article_summary_row(article)
        for article in accepted_articles[:_TOP_LEVEL_ARTICLE_SAMPLE_LIMIT]
    ]
    compact["flagged_article_counts"] = {
        "full_count": len(flagged_articles),
        "sample_count": min(len(flagged_articles), _TOP_LEVEL_ARTICLE_SAMPLE_LIMIT),
        "omitted_count": max(0, len(flagged_articles) - _TOP_LEVEL_ARTICLE_SAMPLE_LIMIT),
        "truncated": len(flagged_articles) > _TOP_LEVEL_ARTICLE_SAMPLE_LIMIT,
    }
    compact["flagged_articles"] = [
        _compact_article_summary_row(article)
        for article in flagged_articles[:_TOP_LEVEL_ARTICLE_SAMPLE_LIMIT]
    ]
    date_groups = [
        dict(item) for item in _json_list(compact.get("date_groups")) if isinstance(item, Mapping)
    ]
    compact["date_group_counts"] = {
        "full_count": len(date_groups),
        "sample_count": min(len(date_groups), _TOP_LEVEL_DATE_SAMPLE_LIMIT),
        "omitted_count": max(0, len(date_groups) - _TOP_LEVEL_DATE_SAMPLE_LIMIT),
        "truncated": len(date_groups) > _TOP_LEVEL_DATE_SAMPLE_LIMIT,
    }
    compact["date_groups"] = [
        _compact_date_summary_row(dict(item))
        for item in date_groups[:_TOP_LEVEL_DATE_SAMPLE_LIMIT]
    ]

    article_review = _json_mapping(compact.get("article_review"))
    if article_review:
        compact["article_review"] = _compact_article_review(article_review)

    topic_relevance_review = _json_mapping(compact.get("topic_relevance_review"))
    if topic_relevance_review:
        compact["topic_relevance_review"] = _compact_topic_relevance_review(topic_relevance_review)

    finbert_sentence_review = _json_mapping(compact.get("finbert_sentence_review"))
    if finbert_sentence_review:
        compact["finbert_sentence_review"] = _compact_finbert_sentence_review(finbert_sentence_review)

    pipeline_sections = _json_mapping(compact.get("pipeline_sections"))
    known_sections = tuple(_PIPELINE_SECTION_SAMPLE_LIMITS)
    section_items = {str(key): value for key, value in pipeline_sections.items()}
    unknown_keys = sorted(set(section_items) - set(known_sections))
    kept_unknown = unknown_keys[:4]
    section_keys = set(known_sections) | set(kept_unknown)
    for key in known_sections:
        section_items.setdefault(key, _MISSING)
    compact["pipeline_section_counts"] = {
        str(key): {
            "row_count": len(rows),
            "full_count": len(rows),
            "sample_count": min(len(rows), _PIPELINE_SECTION_SAMPLE_LIMITS.get(str(key), 1)),
            "omitted_count": max(0, len(rows) - _PIPELINE_SECTION_SAMPLE_LIMITS.get(str(key), 1)),
            "truncated": len(rows) > _PIPELINE_SECTION_SAMPLE_LIMITS.get(str(key), 1),
        }
        for key, rows in ((str(key), _json_list(value)) for key, value in section_items.items() if key in section_keys)
    }
    compact["pipeline_sections"] = {
        key: _compact_fixed_section_rows(
            value,
            _PIPELINE_SECTION_SAMPLE_LIMITS.get(key, 1) if key in known_sections else _UNKNOWN_PIPELINE_SECTION_LIMIT,
        )
        for key, value in section_items.items()
        if key in section_keys
    }
    if len(unknown_keys) > len(kept_unknown):
        compact.setdefault("warnings", []).append({
            "code": "unknown_pipeline_sections_omitted",
            "message": "Unknown pipeline section detail was omitted from the bounded review payload.",
        })
    for key in (
        "price_series",
        "benchmark_price_series",
        "market_regime_series",
        "benchmark_market_regime_series",
        "warnings",
    ):
        limit = _WARNING_SAMPLE_LIMIT if key == "warnings" else _CHART_SAMPLE_LIMIT
        rows, counts = _bounded_extremes(compact.get(key), limit)
        compact[key] = rows
        compact[f"{key}_counts"] = counts
    bounded = cast(dict[str, object], _bound_json_value(compact))
    return _enforce_payload_pretty_byte_budget(bounded)


def _compact_article_summary_row(article: Mapping[str, object]) -> dict[str, object]:
    """Return a minimal article summary for top-level payload indexes."""
    article_dict = dict(article)
    preprocessing_rows = _json_list(article_dict.get("preprocessing_rows"))
    topic_evidence = _json_list(article_dict.get("topic_evidence"))
    relevance_gate_rows = _json_list(article_dict.get("relevance_gate_rows"))
    sentence_rows = _json_list(article_dict.get("sentence_rows"))
    compact = {
        "date": article_dict.get("date"),
        "ticker": article_dict.get("ticker"),
        "article_id": article_dict.get("article_id"),
        "headline": article_dict.get("headline"),
        "normalized_headline": article_dict.get("normalized_headline"),
        "article_status": article_dict.get("article_status"),
        "relevance_state": article_dict.get("relevance_state"),
        "relevance_score": article_dict.get("relevance_score"),
        "article_row_count": article_dict.get("article_row_count"),
        "sentence_count": article_dict.get("sentence_count"),
        "unique_sentence_count": article_dict.get("unique_sentence_count"),
        "duplicate_sentence_count": article_dict.get("duplicate_sentence_count"),
        "headline_duplicate_count": article_dict.get("headline_duplicate_count"),
        "contamination_flags": article_dict.get("contamination_flags"),
        "assignment_classifications": article_dict.get("assignment_classifications"),
        "assignment_reasons": article_dict.get("assignment_reasons"),
        "requested_ticker_terms": article_dict.get("requested_ticker_terms"),
        "requested_ticker_term_hits": article_dict.get("requested_ticker_term_hits"),
        "evidence_snippets": article_dict.get("evidence_snippets"),
        "preprocessing_row_count": len(preprocessing_rows),
        "topic_evidence_row_count": len(topic_evidence),
        "relevance_gate_row_count": len(relevance_gate_rows),
        "sentence_row_count": len(sentence_rows),
        "sentence_rows_sample_count": min(len(sentence_rows), _FINBERT_SENTENCE_SAMPLE_LIMIT),
        "sentence_rows_truncated": len(sentence_rows) > _FINBERT_SENTENCE_SAMPLE_LIMIT,
    }
    return compact


def _compact_date_summary_row(date_group: Mapping[str, object]) -> dict[str, object]:
    """Return a minimal date summary for top-level payload indexes."""
    articles = [dict(item) for item in _json_list(date_group.get("articles")) if isinstance(item, Mapping)]
    return {
        "date": date_group.get("date"),
        "article_count": date_group.get("article_count", len(articles)),
        "accepted_count": date_group.get("accepted_count"),
        "flagged_count": date_group.get("flagged_count"),
        "sentence_count": date_group.get("sentence_count"),
        "article_ids": [str(article.get("article_id") or "") for article in articles if article.get("article_id")],
        "regime": _json_mapping(date_group.get("regime")),
        "price": _json_mapping(date_group.get("price")),
        "semantic_aggregate_count": len(_json_list(date_group.get("semantic_aggregates"))),
    }


def _compact_article_group_row(article: Mapping[str, object]) -> dict[str, object]:
    """Return a representative article card with bounded nested evidence rows."""
    compact = dict(article)
    preprocessing_rows = _sample_mapping_rows(compact.get("preprocessing_rows"), limit=_ARTICLE_DETAIL_SAMPLE_LIMIT)
    topic_evidence = _sample_mapping_rows(compact.get("topic_evidence"), limit=_ARTICLE_DETAIL_SAMPLE_LIMIT)
    relevance_gate_rows = _sample_mapping_rows(compact.get("relevance_gate_rows"), limit=_ARTICLE_DETAIL_SAMPLE_LIMIT)
    sentence_rows = _sample_mapping_rows(compact.get("sentence_rows"), limit=_FINBERT_SENTENCE_SAMPLE_LIMIT)
    full_scored_text = _optional_str(compact.get("full_scored_text"))
    if full_scored_text is not None:
        compact["full_scored_text_preview"] = full_scored_text[:_FULL_TEXT_PREVIEW_LIMIT]
        compact["full_scored_text_truncated"] = len(full_scored_text) > _FULL_TEXT_PREVIEW_LIMIT
        compact.pop("full_scored_text", None)
    compact["preprocessing_rows"] = preprocessing_rows
    compact["preprocessing_row_count"] = len(_json_list(article.get("preprocessing_rows")))
    compact["preprocessing_full_count"] = compact["preprocessing_row_count"]
    compact["preprocessing_sample_count"] = len(preprocessing_rows)
    compact["preprocessing_omitted_count"] = compact["preprocessing_row_count"] - len(preprocessing_rows)
    compact["preprocessing_rows_truncated"] = compact["preprocessing_row_count"] > len(preprocessing_rows)
    compact["topic_evidence"] = topic_evidence
    compact["topic_evidence_row_count"] = len(_json_list(article.get("topic_evidence")))
    compact["topic_evidence_truncated"] = compact["topic_evidence_row_count"] > len(topic_evidence)
    compact["relevance_gate_rows"] = relevance_gate_rows
    compact["relevance_gate_row_count"] = len(_json_list(article.get("relevance_gate_rows")))
    compact["relevance_gate_rows_truncated"] = compact["relevance_gate_row_count"] > len(relevance_gate_rows)
    compact["sentence_rows"] = sentence_rows
    compact["sentence_row_count"] = len(_json_list(article.get("sentence_rows")))
    compact["sentence_rows_sample_count"] = len(sentence_rows)
    compact["sentence_rows_truncated"] = compact["sentence_row_count"] > len(sentence_rows)
    return compact


def _compact_date_group_row(date_group: Mapping[str, object]) -> dict[str, object]:
    """Return a date-group card with bounded article evidence rows."""
    compact = dict(date_group)
    articles = [
        _compact_article_group_row(article)
        for article in _json_list(compact.get("articles"))
        if isinstance(article, Mapping)
    ]
    compact["articles"] = articles
    compact["article_count"] = len(articles)
    compact["accepted_count"] = sum(1 for article in articles if article.get("article_status") == "accepted")
    compact["flagged_count"] = sum(1 for article in articles if article.get("article_status") != "accepted")
    return compact


def _compact_article_review(review: Mapping[str, object]) -> dict[str, object]:
    """Return the article-review tab with bounded accepted and contamination evidence."""
    compact = dict(review)
    compact["accepted_date_groups"] = [
        _compact_date_group_row(group)
        for group in _json_list(compact.get("accepted_date_groups"))
        if isinstance(group, Mapping)
    ]
    compact["contamination_date_groups"] = [
        _compact_date_group_row(group)
        for group in _json_list(compact.get("contamination_date_groups"))
        if isinstance(group, Mapping)
    ]
    compact["accepted_articles"] = [
        _compact_article_group_row(article)
        for article in _json_list(compact.get("accepted_articles"))[:_TOP_LEVEL_ARTICLE_SAMPLE_LIMIT]
        if isinstance(article, Mapping)
    ]
    compact["contamination_articles"] = [
        _compact_article_group_row(article)
        for article in _json_list(compact.get("contamination_articles"))[:_TOP_LEVEL_ARTICLE_SAMPLE_LIMIT]
        if isinstance(article, Mapping)
    ]
    compact["accepted_articles_truncated"] = len(_json_list(review.get("accepted_articles"))) > len(
        compact["accepted_articles"]
    )
    compact["contamination_articles_truncated"] = len(_json_list(review.get("contamination_articles"))) > len(
        compact["contamination_articles"]
    )
    return compact


def _compact_finbert_sentence_review(review: Mapping[str, object]) -> dict[str, object]:
    """Return the FinBERT review tab with bounded article and sentence detail."""
    compact = dict(review)
    compact["articles"] = [
        _compact_finbert_article(article)
        for article in _json_list(compact.get("articles"))[:_TOP_LEVEL_ARTICLE_SAMPLE_LIMIT]
        if isinstance(article, Mapping)
    ]
    compact["article_full_count"] = len(_json_list(review.get("articles")))
    compact["article_sample_count"] = len(compact["articles"])
    compact["article_omitted_count"] = compact["article_full_count"] - compact["article_sample_count"]
    compact["articles_truncated"] = compact["article_omitted_count"] > 0
    compact["missing_text_warnings"] = [
        dict(item)
        for item in _json_list(compact.get("missing_text_warnings"))[:25]
        if isinstance(item, Mapping)
    ]
    compact["source_artifact_gaps"] = [
        dict(item)
        for item in _json_list(compact.get("source_artifact_gaps"))[:25]
        if isinstance(item, Mapping)
    ]
    compact["missing_text_warning_count"] = len(_json_list(review.get("missing_text_warnings")))
    compact["source_artifact_gap_count"] = len(_json_list(review.get("source_artifact_gaps")))
    return compact


def _compact_finbert_article(article: Mapping[str, object]) -> dict[str, object]:
    """Return a FinBERT article card with representative sentence rows."""
    compact = dict(article)
    preprocessing_rows = _sample_mapping_rows(
        compact.get("preprocessing_rows"), limit=_ARTICLE_DETAIL_SAMPLE_LIMIT
    )
    topic_evidence = _sample_mapping_rows(compact.get("topic_evidence"), limit=_ARTICLE_DETAIL_SAMPLE_LIMIT)
    relevance_gate_rows = _sample_mapping_rows(
        compact.get("relevance_gate_rows"), limit=_ARTICLE_DETAIL_SAMPLE_LIMIT
    )
    sentence_rows = _sample_mapping_rows(compact.get("sentence_rows"), limit=_FINBERT_SENTENCE_SAMPLE_LIMIT)
    full_scored_text = _optional_str(compact.get("full_scored_text"))
    if full_scored_text is not None:
        compact["full_scored_text_preview"] = full_scored_text[:_FULL_TEXT_PREVIEW_LIMIT]
        compact["full_scored_text_truncated"] = len(full_scored_text) > _FULL_TEXT_PREVIEW_LIMIT
        compact.pop("full_scored_text", None)
    compact["preprocessing_rows"] = preprocessing_rows
    compact["preprocessing_row_count"] = len(_json_list(article.get("preprocessing_rows")))
    compact["preprocessing_full_count"] = compact["preprocessing_row_count"]
    compact["preprocessing_sample_count"] = len(preprocessing_rows)
    compact["preprocessing_omitted_count"] = compact["preprocessing_row_count"] - len(preprocessing_rows)
    compact["preprocessing_rows_truncated"] = compact["preprocessing_row_count"] > len(preprocessing_rows)
    compact["topic_evidence"] = topic_evidence
    compact["topic_evidence_row_count"] = len(_json_list(article.get("topic_evidence")))
    compact["topic_evidence_truncated"] = compact["topic_evidence_row_count"] > len(topic_evidence)
    compact["relevance_gate_rows"] = relevance_gate_rows
    compact["relevance_gate_row_count"] = len(_json_list(article.get("relevance_gate_rows")))
    compact["relevance_gate_rows_truncated"] = compact["relevance_gate_row_count"] > len(relevance_gate_rows)
    compact["sentence_rows"] = sentence_rows
    compact["sentence_rows_sample_count"] = len(sentence_rows)
    compact["sentence_rows_truncated"] = len(_json_list(article.get("sentence_rows"))) > len(sentence_rows)
    return compact


def _compact_topic_relevance_review(review: Mapping[str, object]) -> dict[str, object]:
    """Return the topic/relevance tab with bounded article and blocker evidence."""
    compact = dict(review)
    compact["date_groups"] = [
        _compact_topic_relevance_date_group(group)
        for group in _json_list(compact.get("date_groups"))
        if isinstance(group, Mapping)
    ]
    compact["articles"] = [
        _compact_topic_relevance_article_row(article)
        for article in _json_list(compact.get("articles"))[:_TOP_LEVEL_ARTICLE_SAMPLE_LIMIT]
        if isinstance(article, Mapping)
    ]
    compact["article_full_count"] = len(_json_list(review.get("articles")))
    compact["article_sample_count"] = len(compact["articles"])
    compact["article_omitted_count"] = compact["article_full_count"] - compact["article_sample_count"]
    compact["articles_truncated"] = compact["article_omitted_count"] > 0
    compact["missing_evidence_blockers"] = [
        dict(item)
        for item in _json_list(compact.get("missing_evidence_blockers"))[:25]
        if isinstance(item, Mapping)
    ]
    compact["missing_evidence_blocker_count"] = len(_json_list(review.get("missing_evidence_blockers")))
    topic_review = _json_mapping(compact.get("topic_review"))
    if topic_review:
        topic_review = dict(topic_review)
        topic_review["rows"] = [
            dict(item)
            for item in _json_list(topic_review.get("rows"))[:12]
            if isinstance(item, Mapping)
        ]
        compact["topic_review"] = topic_review
    return compact


def _compact_topic_relevance_date_group(date_group: Mapping[str, object]) -> dict[str, object]:
    """Return a topic/relevance date bucket with bounded article evidence."""
    compact = dict(date_group)
    compact["articles"] = [
        _compact_topic_relevance_article_row(article)
        for article in _json_list(compact.get("articles"))
        if isinstance(article, Mapping)
    ]
    compact["article_count"] = len(_json_list(date_group.get("articles")))
    return compact


def _compact_topic_relevance_article_row(article: Mapping[str, object]) -> dict[str, object]:
    """Return an article row with representative topic/relevance evidence only."""
    compact = dict(article)
    preprocessing_rows = _sample_mapping_rows(compact.get("preprocessing_rows"), limit=_ARTICLE_DETAIL_SAMPLE_LIMIT)
    embedding_evidence = _sample_mapping_rows(compact.get("embedding_evidence"), limit=_ARTICLE_DETAIL_SAMPLE_LIMIT)
    topic_evidence = _sample_mapping_rows(compact.get("topic_evidence"), limit=_ARTICLE_DETAIL_SAMPLE_LIMIT)
    relevance_gate_rows = _sample_mapping_rows(compact.get("relevance_gate_rows"), limit=_ARTICLE_DETAIL_SAMPLE_LIMIT)
    sentence_rows = _sample_mapping_rows(compact.get("sentence_rows"), limit=_FINBERT_SENTENCE_SAMPLE_LIMIT)
    compact["preprocessing_rows"] = preprocessing_rows
    compact["preprocessing_row_count"] = len(_json_list(article.get("preprocessing_rows")))
    compact["preprocessing_full_count"] = compact["preprocessing_row_count"]
    compact["preprocessing_sample_count"] = len(preprocessing_rows)
    compact["preprocessing_omitted_count"] = compact["preprocessing_row_count"] - len(preprocessing_rows)
    compact["preprocessing_rows_truncated"] = compact["preprocessing_row_count"] > len(preprocessing_rows)
    compact["embedding_evidence"] = embedding_evidence
    compact["embedding_evidence_row_count"] = len(_json_list(article.get("embedding_evidence")))
    compact["embedding_evidence_truncated"] = compact["embedding_evidence_row_count"] > len(embedding_evidence)
    compact["topic_evidence"] = topic_evidence
    compact["topic_evidence_row_count"] = len(_json_list(article.get("topic_evidence")))
    compact["topic_evidence_truncated"] = compact["topic_evidence_row_count"] > len(topic_evidence)
    compact["relevance_gate_rows"] = relevance_gate_rows
    compact["relevance_gate_row_count"] = len(_json_list(article.get("relevance_gate_rows")))
    compact["relevance_gate_rows_truncated"] = compact["relevance_gate_row_count"] > len(relevance_gate_rows)
    compact["sentence_rows"] = sentence_rows
    compact["sentence_row_count"] = len(_json_list(article.get("sentence_rows")))
    compact["sentence_rows_sample_count"] = len(sentence_rows)
    compact["sentence_rows_truncated"] = compact["sentence_row_count"] > len(sentence_rows)
    compact["evidence_sampling_note"] = (
        "Representative rows only" if any(
            compact[key]
            for key in (
                "preprocessing_rows_truncated",
                "embedding_evidence_truncated",
                "topic_evidence_truncated",
                "relevance_gate_rows_truncated",
                "sentence_rows_truncated",
            )
        ) else None
    )
    return compact


_FIXED_ROW_SCALAR_KEYS = frozenset(
    {
        "date", "ticker", "article_id", "sentence_index", "chunk_index", "chunk_id", "topic_id",
        "run_id", "stage", "status", "state", "label", "headline", "normalized_headline",
        "article_status", "relevance_decision", "relevance_score", "relevance_category",
        "target_context_score", "target_company_impact_direction", "target_company_impact_magnitude",
        "article_contribution_weight", "final_contribution", "included_in_signal", "regime",
        "close", "open", "high", "low", "volume", "adj_close", "price", "probability",
        "positive", "negative", "neutral", "sentiment_score", "reason", "warning", "message",
        "source_text_field", "source_text_order", "source_character_count", "missing_text_state",
        "extraction_status", "selection_status", "artifact_key", "model", "model_version",
    }
)


_FIXED_ROW_LIST_KEYS = frozenset(
    {
        "article_ids", "ticker_mentions", "entity_mentions", "source_tickers", "topic_keywords",
        "reason_codes", "warning_codes", "assignment_evidence_kinds", "requested_ticker_terms",
        "requested_ticker_term_hits", "missing_evidence_flags", "resolved_artifact_keys",
    }
)


def _compact_fixed_section_rows(value: object, limit: int) -> list[dict[str, object]]:
    """Project known and unknown pipeline rows to bounded approved fields."""
    rows, _ = _bounded_mappings(value, limit)
    projected: list[dict[str, object]] = []
    for row in rows:
        item: dict[str, object] = {}
        for key, raw_value in row.items():
            name = str(key)
            if name == "source_text_provenance":
                continue
            if name not in _FIXED_ROW_SCALAR_KEYS and name not in _FIXED_ROW_LIST_KEYS:
                continue
            if isinstance(raw_value, Mapping):
                continue
            if isinstance(raw_value, Sequence) and not isinstance(raw_value, (str, bytes, bytearray)):
                if name not in _FIXED_ROW_LIST_KEYS:
                    continue
                item[name] = [_bound_json_value(member, key=name) for member in list(raw_value)[:8] if not isinstance(member, Mapping)]
                continue
            item[name] = _bound_json_value(raw_value, key=name)
        projected.append(item)
    return projected


def _sample_mapping_rows(value: object, *, limit: int) -> list[dict[str, object]]:
    """Return a bounded list of JSON-mapping rows."""
    rows, _ = _bounded_mappings(value, limit)
    return rows


def _collection_counts(value: object, limit: int) -> dict[str, object]:
    """Return complete metadata for a possibly malformed dynamic row list."""
    if value is _MISSING:
        return {
            "row_count": 0, "sample_count": 0, "omitted_row_count": 0,
            "invalid_row_count": 0, "truncated": False, "input_state": "missing",
            "full_count": 0, "omitted_count": 0,
        }
    if value is None or not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        return {
            "row_count": 0, "sample_count": 0, "omitted_row_count": 0,
            "invalid_row_count": 1, "truncated": False, "input_state": "invalid",
            "full_count": 0, "omitted_count": 0,
        }
    invalid = sum(1 for item in value if not isinstance(item, Mapping))
    # A valid sequence's authoritative count includes malformed members.
    row_count = len(value)
    sample_count = min(row_count - invalid, max(limit, 0))
    omitted = row_count - sample_count
    return {
        "row_count": row_count, "sample_count": sample_count,
        "omitted_row_count": omitted, "invalid_row_count": invalid,
        "truncated": omitted > 0, "input_state": "present",
        "full_count": row_count, "omitted_count": omitted,
    }


def _bounded_artifact_keys(value: object) -> dict[str, object]:
    """Keep a deterministic stage/path artifact index without raw provenance."""
    mapping = _json_mapping(value)
    keys = sorted(mapping)[:16]
    result: dict[str, object] = {}
    counts: dict[str, object] = {}
    for key in keys:
        raw = mapping[key]
        if isinstance(raw, Sequence) and not isinstance(raw, (str, bytes, bytearray)):
            values = sorted((str(item) for item in raw), key=lambda item: item)[:8]
            result[key] = values
            counts[key] = {
                "entry_count": len(raw), "sample_count": len(values),
                "omitted_entry_count": max(0, len(raw) - len(values)),
                "invalid_entry_count": 0, "truncated": len(raw) > len(values),
                "input_state": "present", "full_count": len(raw),
                "omitted_count": max(0, len(raw) - len(values)),
            }
        else:
            result[key] = []
            counts[key] = _collection_counts(raw, 8)
    result["artifact_key_counts"] = counts
    result["artifact_key_entry_count"] = len(mapping)
    result["artifact_key_omitted_entry_count"] = max(0, len(mapping) - len(keys))
    result["artifact_key_truncated"] = len(mapping) > len(keys)
    return result


def _bound_json_value(value: object, *, key: str = "", depth: int = 0) -> object:
    """Project arbitrary report values into deterministic, shallow JSON-safe values."""
    import json

    if depth > _PAYLOAD_MAX_DEPTH and isinstance(value, (Mapping, Sequence)) and not isinstance(value, (str, bytes, bytearray)):
        return None
    if isinstance(value, Mapping):
        result: dict[str, object] = {}
        raw_keys = sorted(value, key=str)
        if depth >= 5 and key not in {"summary", "controls", "features"}:
            raw_keys = raw_keys[:32]
        if key == "features":
            raw_keys = raw_keys[:32]
        key_limit = _PAYLOAD_TOP_LEVEL_KEY_LIMIT if depth == 0 else _PAYLOAD_MAPPING_KEY_LIMIT
        if len(raw_keys) > key_limit:
            result["key_count"] = len(raw_keys)
            result["keys_truncated"] = True
            raw_keys = raw_keys[:key_limit]
        for raw_key in raw_keys:
            name = str(raw_key)
            if name == "source_text_provenance":
                continue
            if len(name) > _PAYLOAD_KEY_NAME_LIMIT:
                truncated_name = f"{name[:_PAYLOAD_KEY_NAME_LIMIT]}..."
                result[f"key_character_count_{truncated_name}"] = len(name)
                result[truncated_name] = _bound_json_value(value[raw_key], key=truncated_name, depth=depth + 1)
                continue
            item = value[raw_key]
            result[name] = _bound_json_value(item, key=name, depth=depth + 1)
            if isinstance(item, str) and name in {
                "text", "full_scored_text", "headline", "message", "reason",
                "summary", "topic_example_text", "snippet",
            } and len(item) > _PROSE_CHARACTER_LIMIT:
                result[f"{name}_character_count"] = len(item)
                result[f"{name}_truncated"] = True
            elif isinstance(item, str) and len(item) > _PAYLOAD_STRING_CHARACTER_LIMIT:
                result[f"{name}_character_count"] = len(item)
                result[f"{name}_truncated"] = True
        return result
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        items = list(value)
        if key in {
            "article_ids", "topic_keywords", "reason_codes", "warning_codes",
            "entity_evidence", "ticker_evidence", "assignment_evidence_kinds",
            "assignment_classifications", "assignment_reasons", "ticker_terms", "ticker_hits",
        }:
            limit = 8
        elif key == "sentence_rows":
            limit = 3
        elif key in {"preprocessing_rows", "topic_evidence", "relevance_gate_rows", "embedding_evidence"}:
            limit = 2
        elif key in {"examples", "evidence_snippets", "snippets"}:
            limit = 3
        elif key in {"warnings", "summary_cards", "gate_cards", "failures"}:
            limit = 12
        else:
            limit = _PAYLOAD_SEQUENCE_ITEM_LIMIT
        if all(isinstance(item, Mapping) for item in items):
            items.sort(key=lambda item: _stable_mapping_key(item))  # type: ignore[arg-type]
        else:
            items.sort(key=lambda item: (type(item).__name__, json.dumps(item, sort_keys=True, default=str)))
        return [_bound_json_value(item, key=key, depth=depth + 1) for item in items[:limit]]
    if isinstance(value, str):
        if key in {
            "text", "full_scored_text", "headline", "message", "reason", "summary",
            "topic_example_text", "snippet",
        }:
            return value[:_PROSE_CHARACTER_LIMIT]
        if len(value) > _PAYLOAD_STRING_CHARACTER_LIMIT:
            return f"{value[:_PAYLOAD_STRING_CHARACTER_LIMIT]}..."
        return value
    if isinstance(value, int) and not isinstance(value, bool):
        digits = str(value)
        if len(digits) > _PAYLOAD_NUMBER_REPR_LIMIT:
            return f"{digits[:_PAYLOAD_NUMBER_REPR_LIMIT]}..."
    return value


def _sampling_counts(full_count: int, sample_count: int) -> dict[str, object]:
    """Return the canonical metadata for one bounded collection."""
    omitted = max(0, full_count - sample_count)
    return {
        "row_count": full_count,
        "sample_count": sample_count,
        "omitted_row_count": omitted,
        "invalid_row_count": 0,
        "truncated": omitted > 0,
        "input_state": "present",
        "full_count": full_count,
        "omitted_count": omitted,
    }


def _bounded_mappings(value: object, limit: int) -> tuple[list[dict[str, object]], dict[str, object]]:
    """Sort JSON mapping rows by stable content and return a bounded sample plus counts."""
    rows = [dict(item) for item in _json_list(value) if isinstance(item, Mapping)]
    rows.sort(key=lambda item: _stable_mapping_key(item))
    sample = rows[: max(limit, 0)]
    counts = _collection_counts(value, limit)
    counts["sample_count"] = len(sample)
    counts["omitted_row_count"] = max(0, int(counts["row_count"]) - len(sample))
    counts["omitted_count"] = counts["omitted_row_count"]
    counts["truncated"] = bool(counts["omitted_row_count"] > 0)
    return sample, counts


def _stratified_bounded_mappings(value: object, limit: int) -> tuple[list[dict[str, object]], dict[str, object]]:
    """Select deterministic representatives without discarding higher-priority strata."""
    rows = [dict(item) for item in _json_list(value) if isinstance(item, Mapping)]
    rows.sort(key=_stable_mapping_key)
    selected: list[dict[str, object]] = []

    def status(row: Mapping[str, object]) -> str:
        return str(row.get("evidence_status", "")).lower()

    def categories(row: Mapping[str, object]) -> set[str]:
        values = [row.get("relevance_category"), row.get("assignment_classification")]
        values.extend(_json_string_list(row.get("assignment_classifications")))
        return {str(item).lower().replace("-", "_") for item in values if item is not None}

    def has_category(row: Mapping[str, object], *names: str) -> bool:
        row_categories = categories(row)
        return bool(row_categories.intersection(names))

    predicates = (
        lambda row: has_category(row, "direct") or status(row) == "direct",
        lambda row: has_category(row, "indirect") or status(row) == "indirect",
        lambda row: has_category(row, "broad_market", "broad") or status(row) == "broad_market",
        lambda row: has_category(row, "contamination", "competitor")
        or status(row) in {"contamination", "flagged", "weak"}
        or bool(_json_string_list(row.get("contamination_flags"))),
        lambda row: status(row) == "accepted",
        lambda row: status(row) == "rejected" or str(row.get("relevance_decision", "")).lower() == "reject",
        lambda row: bool(_json_string_list(row.get("missing_evidence_flags")))
        or status(row) in {"missing", "missing_text", "missing_or_default"},
    )
    for predicate in predicates:
        candidates = [row for row in rows if predicate(row)]
        candidates.sort(key=lambda row: (
            _maybe_float(row.get("relevance_score")) is None,
            _maybe_float(row.get("relevance_score")) or 0.0,
            _stable_mapping_key(row),
        ))
        if candidates and candidates[0] not in selected:
            selected.append(candidates[0])
    for row in rows:
        if row not in selected:
            selected.append(row)
        if len(selected) >= max(limit, 0):
            break
    # Priority order above is authoritative; stable keys only break ties within a stratum.
    sample = selected[: max(limit, 0)]
    counts = _collection_counts(value, limit)
    counts["sample_count"] = len(sample)
    counts["omitted_row_count"] = max(0, int(cast(int, counts["row_count"])) - len(sample))
    counts["omitted_count"] = counts["omitted_row_count"]
    counts["truncated"] = bool(counts["omitted_row_count"] > 0)
    return sample, counts


def _bounded_extremes(value: object, limit: int) -> tuple[list[dict[str, object]], dict[str, object]]:
    """Return deterministic evenly spaced, endpoint-preserving date-series samples."""
    rows, _ = _bounded_mappings(value, len(_json_list(value)))
    rows.sort(key=lambda row: (_date_sort_key(row.get("date")), _stable_mapping_key(row)))
    if len(rows) <= limit:
        sample = rows
    elif limit <= 1:
        sample = rows[:1]
    else:
        indices = [round(index * (len(rows) - 1) / (limit - 1)) for index in range(limit)]
        sample = [rows[index] for index in indices]
    counts = _collection_counts(value, limit)
    counts["sample_count"] = len(sample)
    counts["omitted_row_count"] = max(0, int(cast(int, counts["row_count"])) - len(sample))
    counts["omitted_count"] = counts["omitted_row_count"]
    counts["truncated"] = bool(counts["omitted_row_count"] > 0)
    return sample, counts


def _date_sort_key(value: object) -> tuple[int, str]:
    """Sort ISO dates chronologically while keeping malformed values deterministic."""
    text = "" if value is None else str(value)
    try:
        from datetime import date
        return (0, date.fromisoformat(text).isoformat())
    except ValueError:
        return (1, text)


def _stable_mapping_key(value: Mapping[str, object]) -> tuple[str, ...]:
    """Return the approved deterministic semantic ordering key for a JSON mapping."""
    import json

    def field(name: str) -> str:
        item = value.get(name)
        return "" if item is None else str(item)

    return (
        field("date"), field("ticker"), field("article_id"), field("sentence_index"),
        field("chunk_index"), field("chunk_id"), field("topic_id"),
        json.dumps(value, sort_keys=True, default=str, separators=(",", ":")),
    )
