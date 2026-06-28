"""UI payload helpers for the Layer 1 semantic-review dashboard."""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

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
        label="Ticker-date semantic aggregates",
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
    payload.update(build_layer1_semantic_review_readiness_summary(payload))
    return payload


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


def _dedupe_preserve_order(items: Sequence[str]) -> list[str]:
    seen: set[str] = set()
    values: list[str] = []
    for item in items:
        if item in seen:
            continue
        seen.add(item)
        values.append(item)
    return values
