"""UI payload helpers for the Layer 1 semantic-review dashboard."""
from __future__ import annotations

from collections.abc import Mapping, Sequence
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


def build_layer1_semantic_review_dashboard_payload(
    report: Layer1SemanticReviewReport | Mapping[str, object],
) -> dict[str, object]:
    """Return the JSON payload rendered by the semantic-review dashboard UI."""
    payload = _build_payload_from_report(report)
    payload["topic_relevance_review"] = build_layer1_topic_relevance_review(payload)
    payload["semantic_aggregate_review"] = build_layer1_semantic_aggregate_review(payload)
    payload.update(build_layer1_semantic_review_readiness_summary(payload))
    return payload


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
    return {
        "summary": {
            "article_count": len(rows),
            "preprocessing_row_count": len(preprocessing_rows),
            "embedding_row_count": len(embedding_rows),
            "topic_label_row_count": len(topic_rows),
            "relevance_gate_row_count": len(relevance_rows),
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
            **_topic_relevance_reviewability_summary(
                article_count=len(rows),
                relevance_gate_row_count=len(relevance_rows),
                embedding_row_count=len(embedding_rows),
                topic_label_row_count=len(topic_rows),
            ),
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
            },
            "topics": topic_review_topics,
            "rows": topic_review_rows,
        },
        "missing_evidence_blockers": missing_blockers,
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
    return {
        "summary": {
            "row_count": len(rows),
            "date_count": len({str(row.get("date")) for row in rows if row.get("date") is not None}),
            "reviewable": bool(rows),
            "review_focus": "ticker-date NLP summary consumed by later model layers",
        },
        "rows": rows,
    }


def build_layer1_semantic_review_readiness_summary(
    payload: Mapping[str, object],
) -> dict[str, object]:
    """Return stable run-readiness and gate-card fields for dashboard consumers."""
    summary = _json_mapping(payload.get("summary"))
    smoke = _json_mapping(payload.get("smoke"))
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
    ready_for_final_acceptance = smoke_passed and not blocked_gates
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
    human_review_status = (
        "can_start"
        if ready_for_final_acceptance
        else "blocked_by_missing_pipeline_evidence"
    )
    missing_pipeline_sections = [
        {
            "key": str(card["key"]),
            "label": str(card["label"]),
            "reason": str(card["message"]),
            "missing_or_tried_keys": list(_json_string_list(card.get("missing_or_tried_keys"))),
        }
        for card in blocked_gates
    ]
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
        "smoke_status": smoke.get("status") or "unknown",
        "sentence_row_count": int(summary.get("row_count") or 0),
        "article_count": int(summary.get("article_count") or 0),
        "date_count": int(summary.get("date_count") or 0),
        "accepted_article_count": int(summary.get("accepted_article_count") or 0),
        "flagged_article_count": int(summary.get("flagged_article_count") or 0),
        "blocked_gate_count": len(blocked_gates),
        "missing_pipeline_section_count": len(missing_pipeline_sections),
        "status_reason": _readiness_status_reason(ready_for_final_acceptance),
    }
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
        "relevance_gate_rows": [dict(row) for row in relevance_rows],
        "preprocessing_rows": [dict(row) for row in preprocessing_rows],
    }


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


def _topic_relevance_reviewability_summary(
    *,
    article_count: int,
    relevance_gate_row_count: int,
    embedding_row_count: int,
    topic_label_row_count: int,
) -> dict[str, object]:
    if article_count and relevance_gate_row_count == 0:
        return {
            "reviewable": False,
            "review_status": "not_reviewable_missing_relevance_gate",
            "review_explanation": (
                "Pre-FinBERT relevance gate artifact is missing. Embeddings and BERTopic "
                "topic rows are present, but the dashboard cannot prove accept/reject "
                "relevance decisions or sub-scores for human acceptance."
            ),
        }
    if article_count and (embedding_row_count == 0 or topic_label_row_count == 0):
        return {
            "reviewable": False,
            "review_status": "not_reviewable_missing_topic_or_embedding_evidence",
            "review_explanation": (
                "Embedding or BERTopic evidence is missing, so topic relevance cannot be reviewed."
            ),
        }
    return {
        "reviewable": bool(article_count),
        "review_status": "reviewable" if article_count else "no_topic_relevance_rows",
        "review_explanation": (
            "Review ticker/entity evidence, topic assignment, and pre-FinBERT relevance "
            "gate decisions before trusting FinBERT sentiment."
        ),
    }


def _semantic_aggregate_review_row(row: Mapping[str, object]) -> dict[str, object]:
    features = _json_mapping(row.get("features"))
    sentiment_score = _maybe_float(features.get("nlp_sentiment_score"))
    sentiment_label = _sentiment_label_from_score(sentiment_score)
    cards = _semantic_aggregate_review_cards(features)
    return {
        **dict(row),
        "sentiment_label": sentiment_label,
        "human_review_summary": _semantic_aggregate_human_summary(
            sentiment_label=sentiment_label,
            sentiment_score=sentiment_score,
            article_count=_maybe_float(features.get("nlp_article_count")),
            sentence_count=_maybe_float(features.get("nlp_sentence_count")),
        ),
        "review_value_cards": cards,
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
    for value in _json_list(values):
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


def _readiness_status_reason(ready_for_final_acceptance: bool) -> str:
    if ready_for_final_acceptance:
        return "Required Layer 1 NLP, HMM, and price evidence is present for review."
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


def _dedupe_preserve_order(items: Sequence[str]) -> list[str]:
    seen: set[str] = set()
    values: list[str] = []
    for item in items:
        if item in seen:
            continue
        seen.add(item)
        values.append(item)
    return values
