"""Unit tests for the Layer 1 semantic-review dashboard."""

from __future__ import annotations

import copy
import json
from datetime import date, timedelta
from pathlib import Path
from typing import Any, cast

import pandas as pd
import pytest

from app.lab.semantic_review_dashboard import _DashboardDefaults, _render_dashboard_html
from core.features.aapl_evidence import (
    _benchmark_context_dates,
    _load_training_regime_rows,
    _training_rows_sufficient,
    build_layer1_aapl_evidence_report,
)
from core.features.regime_training import HMM_OPTIONAL_FEATURE_COLUMNS
from core.features.semantic_qa import (
    PILOT_TICKERS,
    SEMANTIC_QA_SCHEMA_ID,
    build_semantic_qa_payload,
)
from core.features.semantic_review_dashboard import (
    _compact_layer1_semantic_review_dashboard_payload,
    _enforce_payload_pretty_byte_budget,
    _stratified_bounded_mappings,
    build_layer1_semantic_review_dashboard_payload,
    build_layer1_semantic_review_dashboard_smoke_payload,
    build_layer1_semantic_review_readiness_summary,
    validate_layer1_semantic_review_dashboard_payload,
)
from services.r2.paths import (
    layer1_news_preprocessing_path,
    layer1_news_relevance_gate_path,
    layer1_regime_path,
    layer1_sentiment_feature_path,
    layer1_sentiment_score_path,
    layer1_text_embedding_path,
    layer1_topic_label_path,
    pipeline_manifest_path,
    raw_price_path,
)
from services.r2.writer import R2Writer
from tests.fixtures.semantic_review_support import seed_semantic_review_fixture


def _evidence_row_count(rows: object) -> int:
    """Count delivered evidence rows, excluding typed compaction markers."""
    if not isinstance(rows, list):
        raise AssertionError(f"expected list of rows, got {type(rows).__name__}")
    return sum(
        1
        for row in rows
        if not (isinstance(row, dict) and row.get("payload_compaction_marker") is True)
    )


def test_smoke_preserves_canonical_counts_and_exact_controls() -> None:
    """A one-row smoke sample must retain producer counts and immutable identity."""
    run_id = "layer1-daily-2026-06-18-2026-06-18-post-pr312-modal-t4-v1"
    report = {
        "ticker": "AAPL",
        "run_id": run_id,
        "from_date": "2026-06-18",
        "to_date": "2026-06-18",
        "summary": {
            "preprocessing_row_count": 650,
            "embedding_row_count": 157,
            "topic_label_row_count": 16,
            "relevance_gate_row_count": 650,
            "row_count": 157,
            "semantic_aggregate_row_count": 2,
            "hmm_regime_row_count": 1,
            "price_row_count": 25,
        },
        "preprocessing_rows": [{"ticker": "AAPL", "article_id": "a"}],
        "embedding_rows": [{"ticker": "AAPL", "article_id": "a"}],
        "topic_label_rows": [{"ticker": "AAPL", "article_id": "a"}],
        "relevance_gate_rows": [{"ticker": "AAPL", "article_id": "a"}],
        "article_groups": [{"ticker": "AAPL", "article_id": "a"}],
        "semantic_aggregate_rows": [{"ticker": "AAPL", "date": "2026-06-18"}],
        "regime_rows": [{"ticker": "AAPL", "date": "2026-06-18"}],
        "price_rows": [{"ticker": "AAPL", "date": "2026-06-18"}],
    }
    payload = cast(dict[str, Any], build_layer1_semantic_review_dashboard_smoke_payload(report))
    assert payload["controls"]["run_id"] == run_id
    assert payload["run_readiness"]["run_id"] == run_id
    assert payload["smoke"]["required_stage_row_counts"]["news_preprocessing"] == 650
    assert payload["smoke"]["required_stage_row_counts"]["news_sentiment_scored"] == 157


def test_payload_budget_records_final_serialized_size_and_preserves_ids() -> None:
    """The budget metadata describes the final pretty serialization, including itself."""
    run_id = "r" * 72
    payload = cast(
        dict[str, Any],
        build_layer1_semantic_review_dashboard_payload(
            {"ticker": "AAPL", "run_id": run_id, "from_date": "2026-01-01", "to_date": "2026-01-02"}
        ),
    )
    encoded = json.dumps(payload, indent=2, sort_keys=True).encode("utf-8")
    assert payload["report_summary"]["run_id"] == run_id
    assert payload["run_readiness"]["run_id"] == run_id
    assert payload["payload_budget"]["final_pretty_utf8_bytes"] == len(encoded)
    assert payload["payload_budget"]["within_budget"] is True


def test_public_builder_preserves_long_ids_and_audits_oversized_ids() -> None:
    """Exact identifiers bypass prose bounds and use an auditable oversized-ID preview."""
    run_id = "run-" + "x" * 300
    payload = cast(
        dict[str, Any],
        build_layer1_semantic_review_dashboard_payload(
            {
                "ticker": "AAPL",
                "run_id": run_id,
                "from_date": "2026-01-01",
                "to_date": "2026-01-02",
                "artifact_keys": {"manifest": ["artifact-" + "a" * 300]},
            }
        ),
    )
    assert payload["controls"]["run_id"] == run_id
    assert payload["report_summary"]["run_id"] == run_id
    artifact_id = "artifact-" + "a" * 300
    assert payload["artifact_keys"]["manifest"][0] == artifact_id
    assert payload["report_summary"]["artifact_keys"]["manifest"][0] == artifact_id

    oversized = "r" * 4_097
    oversized_payload = cast(
        dict[str, Any],
        build_layer1_semantic_review_dashboard_payload({"ticker": "AAPL", "run_id": oversized}),
    )
    bounded_id = oversized_payload["controls"]["run_id"]
    assert bounded_id["exact_value_omitted"] is True
    assert bounded_id["exact_character_count"] == len(oversized)
    assert len(bounded_id["exact_sha256"]) == 64

    oversized_artifact = "artifact-" + "b" * 4_097
    oversized_artifact_payload = cast(
        dict[str, Any],
        build_layer1_semantic_review_dashboard_payload(
            {"ticker": "AAPL", "artifact_keys": {"manifest": [oversized_artifact]}}
        ),
    )
    bounded_artifact = oversized_artifact_payload["artifact_keys"]["manifest"][0]
    assert bounded_artifact["exact_value_omitted"] is True
    assert bounded_artifact["exact_character_count"] == len(oversized_artifact)
    assert len(bounded_artifact["exact_sha256"]) == 64


def test_compaction_preserves_canonical_smoke_counts_and_exact_identity_indexes() -> None:
    """A byte-budget-compacted payload keeps smoke counts, IDs, and preview hashes exact."""
    run_id = "layer1-daily-2026-06-18-2026-06-18-post-pr312-modal-t4-v1"
    artifact_id = "artifact-" + "a" * 300
    oversized_artifact = "s" * 5_000
    rows = [
        {
            "article_id": f"aapl-{index:04d}",
            "article_status": "accepted",
            "headline": "h" * 2_000,
            "date": f"2026-05-{(index % 28) + 1:02d}",
            "ticker": "AAPL",
            "summary": {f"metric_{metric}": metric for metric in range(30)},
        }
        for index in range(400)
    ]
    report: dict[str, object] = {
        "ticker": "AAPL",
        "run_id": run_id,
        "from_date": "2026-06-18",
        "to_date": "2026-06-18",
        "summary": {
            "preprocessing_row_count": 650,
            "relevance_gate_row_count": 650,
            "embedding_row_count": 157,
            "topic_label_row_count": 16,
            "semantic_aggregate_row_count": 2,
            "hmm_regime_row_count": 1,
            "price_row_count": 25,
        },
        "article_groups": rows,
        "artifact_keys": {
            "manifest": [artifact_id, "short-key"],
            "news_sentiment_scored": [oversized_artifact],
        },
    }
    payload = cast(dict[str, Any], build_layer1_semantic_review_dashboard_payload(report))
    assert payload["payload_budget"]["compacted"] is True

    # Exact control/provenance identity survives the final byte-budget pass.
    assert payload["controls"]["run_id"] == run_id
    assert payload["report_summary"]["run_id"] == run_id
    assert payload["run_readiness"]["run_id"] == run_id

    # Canonical readiness/smoke counts stay authoritative under compaction.
    stage_counts = cast(dict[str, Any], payload["smoke"]["required_stage_row_counts"])
    assert stage_counts["news_preprocessing"] == 650
    assert stage_counts["news_sentiment_scored"] == 0  # no scored rows in this fixture
    assert stage_counts["text_embeddings"] == 157
    assert stage_counts["topic_labels"] == 16
    assert stage_counts["sentiment_features"] == 2
    assert stage_counts["stock_price_context"] == 25

    # Artifact index: ordinary ID byte-exact, oversized ID typed preview+hash.
    artifact_index = cast(dict[str, Any], payload["artifact_keys"])
    assert artifact_index["manifest"][0] == artifact_id
    preview = cast(dict[str, Any], artifact_index["news_sentiment_scored"][0])
    assert preview["exact_value_omitted"] is True
    assert preview["exact_character_count"] == 5_000
    assert len(preview["exact_sha256"]) == 64

    encoded = json.dumps(payload, indent=2, sort_keys=True).encode("utf-8")
    assert len(encoded) < 200_000
    assert payload["payload_budget"]["final_pretty_utf8_bytes"] == len(encoded)
    reversed_payload = build_layer1_semantic_review_dashboard_payload(
        dict(reversed(list(report.items())))
    )
    assert encoded == json.dumps(reversed_payload, indent=2, sort_keys=True).encode("utf-8")

    # Count metadata is internally consistent for every delivered collection.
    for collection_key, counts_key in (
        ("article_groups", "article_group_counts"),
        ("date_groups", "date_group_counts"),
        ("warnings", "warnings_counts"),
    ):
        counts = cast(dict[str, Any], payload[counts_key])
        if collection_key not in payload:
            assert counts["sample_count"] == 0
            continue
        delivered = _evidence_row_count(payload[collection_key])
        assert counts["sample_count"] == delivered
        assert counts["omitted_count"] == counts["full_count"] - delivered


def _artifact_index_consistency(index: object) -> list[tuple[object, object, int]]:
    """Return (stage, metadata, delivered) triples that disagree with the index."""
    assert isinstance(index, dict)
    counts = index.get("artifact_key_counts")
    assert isinstance(counts, dict)
    problems: list[tuple[object, object, int]] = []
    for stage, values in index.items():
        if stage in {
            "artifact_key_counts",
            "artifact_key_entry_count",
            "artifact_key_omitted_entry_count",
            "artifact_key_truncated",
        } or not isinstance(values, list):
            continue
        meta = counts.get(stage)
        delivered = _evidence_row_count(values)
        if not isinstance(meta, dict):
            problems.append((stage, meta, delivered))
            continue
        if (
            meta.get("sample_count") != delivered
            or meta.get("omitted_count") != meta.get("full_count", -1) - delivered
            or meta.get("full_count", 0) < delivered
        ):
            problems.append((stage, meta, delivered))
    return problems


def test_high_cardinality_artifact_index_counts_reconcile_after_final_compaction() -> None:
    """Every final artifact-index sample count matches delivered rows in both indexes."""
    long_id = "artifact-" + "a" * 300
    oversized_id = "s" * 5_000
    stages = [f"stage_{index:02d}" for index in range(20)]
    artifact_keys = {
        stage: [f"{stage}-artifact-id-value-number-{item:03d}-padding" for item in range(40)]
        for stage in stages
    }
    artifact_keys["manifest"] = [long_id, oversized_id] + [f"m-{item}" for item in range(38)]
    report: dict[str, object] = {
        "ticker": "AAPL",
        "run_id": "layer1-daily-2026-06-18-2026-06-18-post-pr312-modal-t4-v1",
        "summary": {"preprocessing_row_count": 650, "embedding_row_count": 157},
        "article_groups": [
            {
                "article_id": f"a-{index:04d}",
                "article_status": "accepted",
                "headline": "h" * 2_000,
                "date": f"2026-05-{(index % 28) + 1:02d}",
                "ticker": "AAPL",
            }
            for index in range(400)
        ],
        "artifact_keys": artifact_keys,
        "report": {
            "run_id": "layer1-daily-2026-06-18-2026-06-18-post-pr312-modal-t4-v1",
            "ticker": "AAPL",
            "from_date": "2026-06-18",
            "to_date": "2026-06-18",
            "summary": {"preprocessing_row_count": 650},
            "artifact_keys": artifact_keys,
        },
    }
    payload = cast(dict[str, Any], build_layer1_semantic_review_dashboard_payload(report))
    assert payload["payload_budget"]["compacted"] is True

    for index in (
        payload["artifact_keys"],
        cast(dict[str, Any], payload["report_summary"])["artifact_keys"],
    ):
        assert _artifact_index_consistency(index) == []
        # Exact-ID policy for every delivered manifest value: ordinary IDs are
        # byte-exact (never ellipsized); dicts are typed compaction markers or
        # complete oversized-ID previews; the long ID itself survived halving.
        manifest_values = index["manifest"]
        ordinary = [value for value in manifest_values if isinstance(value, str)]
        assert long_id in ordinary
        assert all(not value.endswith("...") for value in ordinary)
        for value in manifest_values:
            if isinstance(value, dict):
                assert value.get("payload_compaction_marker") is True or (
                    value.get("exact_value_omitted") is True
                    and value.get("exact_character_count") == 5_000
                    and len(value["exact_sha256"]) == 64
                )

    encoded = json.dumps(payload, indent=2, sort_keys=True).encode("utf-8")
    assert len(encoded) < 200_000
    assert payload["payload_budget"]["final_pretty_utf8_bytes"] == len(encoded)
    reversed_payload = build_layer1_semantic_review_dashboard_payload(
        dict(reversed(list(report.items())))
    )
    assert encoded == json.dumps(reversed_payload, indent=2, sort_keys=True).encode("utf-8")


def test_smoke_finbert_sample_is_strictly_ticker_isolated() -> None:
    """Foreign and missing-ticker article/sentence rows never enter evidence samples."""
    payload = cast(
        dict[str, Any],
        build_layer1_semantic_review_dashboard_smoke_payload(
            {
                "ticker": "AAPL",
                "article_groups": [
                    {
                        "ticker": "MSFT",
                        "article_id": "foreign",
                        "sentence_rows": [{"ticker": "MSFT", "text": "foreign"}],
                    },
                    {
                        "ticker": None,
                        "article_id": "missing-row-ticker",
                        "sentence_rows": [{"ticker": "AAPL", "text": "untyped"}],
                    },
                    {
                        "ticker": "AAPL",
                        "article_id": "aapl-article",
                        "sentence_rows": [
                            {"ticker": "MSFT", "text": "foreign nested"},
                            {"ticker": "AAPL", "text": "target", "sentence_index": 2},
                        ],
                    },
                ],
            }
        ),
    )
    rows = cast(list[dict[str, Any]], payload["pipeline_sections"]["finbert_sentence_rows"])
    assert len(rows) == 1
    assert rows[0]["ticker"] == "AAPL"
    assert rows[0]["article_id"] == "aapl-article"
    assert rows[0]["text"] == "target"


def test_semantic_review_payload_bounds_retained_hmm_context_and_preserves_evidence() -> None:
    """Retained HMM metadata stays useful and bounded even with adversarial mappings."""
    context: dict[str, object] = {
        "requested_inference_dates": ["2026-05-21"],
        "observed_inference_dates": ["2026-05-21"],
        "manifest_summaries": [
            {
                "date": f"2026-05-{(index % 28) + 1:02d}",
                "artifact_key": f"regime-{index}",
                "source_text_provenance": "must not leak",
                "arbitrary_payload": "x" * 10_000,
            }
            for index in range(1_000)
        ],
        "training_windows": [
            {
                "train_end_date": "2026-05-20",
                "inference_date": "2026-05-21",
                "arbitrary_payload": "y" * 10_000,
            }
            for _ in range(1_000)
        ],
        "arbitrary_context": {str(index): "z" * 10_000 for index in range(100)},
    }
    report = {"ticker": "AAPL", "hmm_evaluation_context": context}
    payload = cast(dict[str, Any], build_layer1_semantic_review_dashboard_payload(report))
    encoded = json.dumps(payload, indent=2, sort_keys=True).encode("utf-8")
    bounded_context = cast(dict[str, Any], payload["hmm_evaluation_context"])

    assert len(encoded) < 200_000
    assert bounded_context["manifest_summaries"][0]["artifact_key"]
    assert bounded_context["training_windows"][0]["train_end_date"] == "2026-05-20"
    assert "arbitrary_context" not in bounded_context
    assert "source_text_provenance" not in encoded.decode("utf-8")
    assert bounded_context["manifest_summaries_counts"]["full_count"] == 1_000

    reversed_payload = cast(
        dict[str, Any],
        build_layer1_semantic_review_dashboard_payload(
            {"ticker": "AAPL", "hmm_evaluation_context": dict(reversed(list(context.items())))}
        ),
    )
    assert encoded == json.dumps(reversed_payload, indent=2, sort_keys=True).encode("utf-8")


def test_semantic_review_payload_forwards_and_bounds_training_regime_rows() -> None:
    rows = [{"date": f"2025-01-{index:03d}", "regime": "sideways"} for index in range(1, 301)]
    payload = cast(
        dict[str, Any],
        build_layer1_semantic_review_dashboard_payload(
            {"ticker": "AAPL", "training_regime_rows": rows}
        ),
    )
    returned = cast(list[dict[str, Any]], payload["training_regime_rows"])
    assert len(returned) <= 250
    assert returned[0]["date"] == rows[0]["date"]
    assert returned[-1]["date"] == rows[-1]["date"]
    counts = cast(dict[str, Any], payload["training_regime_row_counts"])
    assert counts["full_count"] == 300
    assert counts["sample_count"] == len(returned)
    assert counts["omitted_row_count"] == 300 - len(returned)
    assert counts["truncated"] is True


@pytest.mark.parametrize(
    "report",
    [
        {"ticker": "AAPL", "summary": {"unbounded": "x" * 250_000}},
        {"ticker": "AAPL", "warnings": [{"scope": "news", "detail": "w" * 250_000}]},
        {
            "ticker": "AAPL",
            "hmm_evaluation_context": {
                "requested_inference_dates": ["d" * 250_000],
                "warnings": ["h" * 250_000],
                "manifest_summaries": [{"artifact_key": "m" * 250_000}],
                "training_windows": [{"train_end_date": "t" * 250_000}],
            },
        },
    ],
)
def test_semantic_review_public_builder_bounds_arbitrary_scalar_routes(
    report: dict[str, object],
) -> None:
    """Arbitrary retained scalars obey the strict pretty-byte budget with audit metadata."""
    payload = cast(dict[str, Any], build_layer1_semantic_review_dashboard_payload(report))
    encoded = json.dumps(payload, indent=2, sort_keys=True).encode("utf-8")
    reversed_report = dict(reversed(list(report.items())))
    reversed_payload = cast(
        dict[str, Any], build_layer1_semantic_review_dashboard_payload(reversed_report)
    )

    assert len(encoded) < 200_000
    assert payload["payload_budget"]["within_budget"] is True
    assert "character_count" in encoded.decode("utf-8")
    assert encoded == json.dumps(reversed_payload, indent=2, sort_keys=True).encode("utf-8")
    assert "source_text_provenance" not in encoded.decode("utf-8")


def test_stratified_dashboard_sampling_preserves_relevance_strata_under_budget() -> None:
    """Direct, indirect, broad-market, and contamination rows survive adverse IDs and truncation."""
    rows = [
        {"article_id": "z-direct", "relevance_category": "direct"},
        {"article_id": "a-indirect", "relevance_category": "indirect"},
        {"article_id": "b-broad", "relevance_category": "broad_market"},
        {"article_id": "c-contamination", "relevance_category": "contamination"},
        {"article_id": "d-accepted", "evidence_status": "accepted"},
        {"article_id": "e-rejected", "evidence_status": "rejected"},
    ]
    sample, counts = _stratified_bounded_mappings(rows, 6)

    assert [row["article_id"] for row in sample] == [
        "z-direct",
        "a-indirect",
        "b-broad",
        "c-contamination",
        "d-accepted",
        "e-rejected",
    ]
    assert counts["full_count"] == 6
    assert counts["omitted_count"] == 0

    truncated, truncated_counts = _stratified_bounded_mappings(list(reversed(rows)), 4)
    assert [row["article_id"] for row in truncated] == [
        "z-direct",
        "a-indirect",
        "b-broad",
        "c-contamination",
    ]
    assert truncated_counts["full_count"] == 6
    assert truncated_counts["omitted_count"] == 2


def test_semantic_review_payload_bounds_high_cardinality_article_indexes() -> None:
    """High-cardinality article detail is sampled while authoritative counts remain explicit."""
    report_dict: dict[str, object] = {
        "ticker": "AAPL",
        "article_groups": [
            {
                "article_id": f"aapl-{index:04d}",
                "article_status": "accepted" if index < 250 else "flagged",
            }
            for index in range(500)
        ],
    }
    payload = cast(dict[str, Any], build_layer1_semantic_review_dashboard_payload(report_dict))
    assert len(json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")) < 200_000
    assert payload["article_group_counts"]["full_count"] == 500
    for collection_key, counts_key, expected_sample in (
        ("article_groups", "article_group_counts", 500),
        ("accepted_articles", "accepted_article_counts", 250),
        ("flagged_articles", "flagged_article_counts", 250),
    ):
        assert payload[counts_key]["full_count"] == expected_sample
        delivered = _evidence_row_count(payload[collection_key])
        assert delivered == payload[counts_key]["sample_count"]
        assert payload[counts_key]["omitted_count"] == (
            payload[counts_key]["full_count"] - delivered
        )
    reversed_payload = build_layer1_semantic_review_dashboard_payload(
        dict(reversed(list(report_dict.items())))
    )
    assert json.dumps(payload, indent=2, sort_keys=True) == json.dumps(
        reversed_payload, indent=2, sort_keys=True
    )


def test_semantic_review_payload_projects_high_cardinality_pipeline_rows() -> None:
    """Known and unknown pipeline rows use fixed projections under the strict API byte budget."""
    rows = [
        {
            "article_id": f"article-{index:03d}",
            "date": "2026-01-01",
            "ticker": "AAPL",
            "headline": "headline " + "x" * 3000,
            "article_ids": [f"nested-{item}" for item in range(500)],
            "sentence_rows": [{"sentence_index": item, "text": "z" * 1000} for item in range(20)],
            "source_text_provenance": {"raw": "y" * 20_000},
        }
        for index in range(50)
    ]
    payload = cast(
        dict[str, Any],
        build_layer1_semantic_review_dashboard_payload(
            {
                "ticker": "AAPL",
                "article_groups": rows,
                "pipeline_sections": {
                    "raw_preprocessing_rows": rows,
                    "unknown_rows": rows,
                },
                "unknown": {"blob": "u" * 10_000, "nested": rows},
            }
        ),
    )
    encoded = json.dumps(payload, indent=2, sort_keys=True).encode("utf-8")

    assert len(encoded) < 200_000
    unknown_sample = cast(list[dict[str, Any]], payload["pipeline_sections"]["unknown_rows"])
    assert len(cast(list[str], unknown_sample[0]["article_ids"])) <= 8
    assert "sentence_rows" not in unknown_sample[0]
    assert "source_text_provenance" not in encoded.decode("utf-8")


def test_final_byte_compaction_preserves_protected_readiness_and_diagnostic_shapes() -> None:
    """The final pretty-byte pass may bound values, but cannot drop contract keys."""
    diagnostic_keys = {
        "embedding_coverage",
        "hmm_chart_auditability",
        "relevance_informativeness",
        "topic_review",
        "hmm_feature_set",
    }
    readiness_keys = {
        "readiness_status",
        "status_reason",
        "run_id",
        "ticker",
        "from_date",
        "to_date",
        "topic_review_state",
        "topic_relevance_review_status",
        "relevance_informativeness_state",
        "diagnostic_states",
        "diagnostic_summary",
    }
    payload = {
        "run_readiness": {key: {"value": "r" * 30_000} for key in readiness_keys},
        "diagnostic_states": {key: {"value": "d" * 30_000} for key in diagnostic_keys},
        "gate_cards": [
            {"key": f"gate-{index}", "label": "gate", "status": "WARN", "reason": "reason"}
            for index in range(10)
        ],
        "missing_pipeline_sections": [
            {"key": f"missing-{index}", "label": "missing", "reason": "not produced"}
            for index in range(3)
        ],
    }

    compacted = _enforce_payload_pretty_byte_budget(payload)
    compacted_readiness = cast(dict[str, Any], compacted["run_readiness"])
    compacted_diagnostics = cast(dict[str, Any], compacted["diagnostic_states"])
    compacted_gates = cast(list[dict[str, Any]], compacted["gate_cards"])
    compacted_missing = cast(list[dict[str, Any]], compacted["missing_pipeline_sections"])

    assert compacted["payload_budget"]["compacted"] is True
    assert compacted["payload_budget"]["truncated"] is True
    assert set(compacted_readiness) == readiness_keys
    assert set(compacted_diagnostics) == diagnostic_keys
    assert len(compacted_gates) == 10
    assert all("reason" in row for row in compacted_missing)


def test_semantic_review_payload_compacts_nested_dynamic_branches_deterministically() -> None:
    """Nested evidence, prose, and unknown sections stay bounded and order-independent."""
    rows = [
        {
            "date": f"2026-05-{index + 1:02d}",
            "ticker": "AAPL",
            "article_id": f"article-{index:03d}",
            "headline": "headline " + "x" * 2000,
            "article_ids": [f"nested-{item}" for item in range(500)],
            "source_text_provenance": {"raw": "y" * 20_000},
            "sentence_rows": [{"sentence_index": item, "text": "z" * 1000} for item in range(500)],
        }
        for index in range(50)
    ]
    report = {
        "ticker": "AAPL",
        "article_groups": rows,
        "date_groups": [{"date": "2026-05-01", "articles": rows}],
        "price_series": [
            {"date": f"2026-01-{index + 1:02d}", "close": index} for index in range(100)
        ],
        "pipeline_sections": {
            "unknown-b": rows,
            "unknown-a": rows,
            "raw_preprocessing_rows": rows,
        },
    }
    first = cast(dict[str, Any], build_layer1_semantic_review_dashboard_payload(report))
    second = cast(
        dict[str, Any],
        build_layer1_semantic_review_dashboard_payload(
            {
                **report,
                "article_groups": list(reversed(rows)),
            }
        ),
    )
    encoded = json.dumps(first, indent=2, sort_keys=True).encode("utf-8")
    assert len(encoded) < 200_000
    assert encoded == json.dumps(second, indent=2, sort_keys=True).encode("utf-8")
    delivered_articles = _evidence_row_count(first["article_groups"])
    assert delivered_articles == first["article_group_counts"]["sample_count"]
    assert first["article_group_counts"]["omitted_count"] == (
        first["article_group_counts"]["full_count"] - delivered_articles
    )
    assert all(
        _evidence_row_count(section) == counts["sample_count"]
        for section, counts in (
            (first["accepted_articles"], first["accepted_article_counts"]),
            (first["flagged_articles"], first["flagged_article_counts"]),
            (first["date_groups"], first["date_group_counts"]),
            (first["price_series"], first["price_series_counts"]),
            (first["warnings"], first["warnings_counts"]),
        )
    )
    for section_name, section_rows in cast(dict[str, Any], first["pipeline_sections"]).items():
        if section_name.startswith("payload_compaction_"):
            continue
        counts = cast(dict[str, Any], first["pipeline_section_counts"])[section_name]
        assert _evidence_row_count(section_rows) == counts["sample_count"]
    assert len(cast(list[Any], first["price_series"])) <= 32
    assert len(cast(list[Any], first["date_groups"])[0]["article_ids"]) <= 8
    assert "source_text_provenance" not in json.dumps(first)
    assert len(cast(dict[str, Any], first["pipeline_sections"])) == 12


def test_semantic_review_payload_preserves_collection_input_states_and_invalid_members() -> None:
    """Dynamic list metadata distinguishes missing, invalid, empty, and malformed members."""
    report = {
        "article_groups": [{"article_id": "a"}, "bad-member", {"article_id": "b"}],
        "pipeline_sections": {
            "raw_preprocessing_rows": None,
            "article_embedding_rows": [],
        },
    }
    payload = cast(dict[str, Any], build_layer1_semantic_review_dashboard_payload(report))
    assert payload["article_group_counts"]["row_count"] == 3
    assert payload["article_group_counts"]["sample_count"] == 2
    assert payload["article_group_counts"]["invalid_row_count"] == 1
    assert payload["article_group_counts"]["omitted_row_count"] == 1
    counts = cast(dict[str, Any], payload["pipeline_section_counts"])
    assert counts["raw_preprocessing_rows"]["input_state"] == "invalid"
    assert counts["article_embedding_rows"]["input_state"] == "present"
    assert counts["topic_label_rows"]["input_state"] == "missing"


def test_semantic_review_payload_evenly_samples_sorted_time_series() -> None:
    """Chart samples sort by date, preserve endpoints, and include spaced interior points."""
    dates = [(date(2026, 1, 1) + timedelta(days=index)).isoformat() for index in range(100)]
    report = {
        "price_series": [
            {"date": value, "close": index} for index, value in enumerate(reversed(dates), 1)
        ]
    }
    payload = cast(dict[str, Any], build_layer1_semantic_review_dashboard_payload(report))
    sampled = cast(list[dict[str, Any]], payload["price_series"])
    assert len(sampled) == 32
    assert sampled[0]["date"] == dates[0]
    assert sampled[-1]["date"] == dates[-1]
    assert sampled[1]["date"] != dates[1]


def test_semantic_review_report_includes_benchmark_rows(tmp_path: Path) -> None:
    """The report should load the SPY benchmark alongside the selected ticker."""
    fixture = seed_semantic_review_fixture(local_root=tmp_path / "r2")
    report = build_layer1_aapl_evidence_report(
        run_id=str(fixture["run_id"]),
        from_date="2026-05-21",
        to_date="2026-05-22",
        ticker="AAPL",
        writer=fixture["writer"],
    )
    report_dict = cast(dict[str, Any], report.to_dict())

    assert report_dict["row_count"] == 8
    assert report_dict["article_count"] == 4
    assert report_dict["date_count"] == 2
    assert report_dict["benchmark_ticker"] == "SPY"
    assert len(cast(list[dict[str, Any]], report_dict["benchmark_price_rows"])) == 2
    assert len(cast(list[dict[str, Any]], report_dict["benchmark_market_regime_rows"])) == 2
    assert report_dict["summary"]["price_row_count"] == 2
    assert report_dict["summary"]["hmm_regime_row_count"] == 2

    price_rows = cast(list[dict[str, Any]], report_dict["price_rows"])
    benchmark_rows = cast(list[dict[str, Any]], report_dict["benchmark_price_rows"])
    assert [row["date"] for row in price_rows] == ["2026-05-21", "2026-05-22"]
    assert [row["ticker"] for row in benchmark_rows] == ["SPY", "SPY"]
    assert price_rows[0]["adj_close"] == 192.4
    assert benchmark_rows[0]["adj_close"] == 590.8

    article_groups = {
        str(item["article_id"]): cast(dict[str, Any], item)
        for item in cast(list[dict[str, Any]], report_dict["article_groups"])
    }
    aapl_one = article_groups["aapl-001"]
    assert [
        row["sentence_index"] for row in cast(list[dict[str, Any]], aapl_one["sentence_rows"])
    ] == [0, 1, 2]
    assert aapl_one["sentence_rows"][0]["text"] != aapl_one["sentence_rows"][1]["text"]
    assert aapl_one["sentence_rows"][0]["row_granularity"] == "sentence-level"
    assert aapl_one["sentence_rows"][0]["assignment_classification"] == "direct"
    assert aapl_one["sentence_rows"][0]["assignment_weight"] == 1.0
    assert "provider_ticker_tag" in aapl_one["sentence_rows"][0]["assignment_evidence_kinds"]
    assert aapl_one["preprocessing_rows"][0]["ticker_mentions"] == ["AAPL"]
    assert aapl_one["preprocessing_rows"][0]["assignment_classification"] == "direct"
    assert aapl_one["preprocessing_rows"][0]["assignment_reason"]
    assert aapl_one["topic_evidence"][0]["topic_label"] == "earnings and demand"
    assert aapl_one["topic_evidence"][0]["topic_keywords"] == ["earnings", "demand", "iphone"]
    assert aapl_one["topic_evidence"][0]["topic_example_text"]
    assert aapl_one["relevance_gate_rows"][0]["relevance_decision"] == "accepted"

    topic_review = cast(dict[str, Any], report_dict["topic_review"])
    assert topic_review["topic_count"] == 2
    assert topic_review["diversity_status"] == "diverse"
    assert len(cast(list[dict[str, Any]], topic_review["topics"])) == 2

    date_groups = {
        str(item["date"]): cast(dict[str, Any], item)
        for item in cast(list[dict[str, Any]], report_dict["date_groups"])
    }
    regime = cast(dict[str, Any], date_groups["2026-05-21"]["regime"])
    assert regime["scope"] == "date-level"
    assert regime["applies_to"] == "all sentence rows on the trading date"
    assert regime["regime"] == "sideways"
    assert regime["readiness_status"] == "ready"
    assert regime["manifest_key"] == pipeline_manifest_path(
        "layer1_5_regime",
        str(fixture["run_id"]),
    )
    assert date_groups["2026-05-21"]["price"]["close"] == 192.4
    assert date_groups["2026-05-21"]["market_regime_context"]["warnings"] == []
    assert date_groups["2026-05-21"]["sentence_count"] == 5
    assert date_groups["2026-05-21"]["semantic_aggregates"][0]["source_weight_summary"]

    semantic_rows = cast(list[dict[str, Any]], report_dict["semantic_aggregate_rows"])
    assert len(semantic_rows) == 2
    assert semantic_rows[0]["date"] == "2026-05-21"
    assert semantic_rows[0]["ticker"] == "AAPL"
    assert semantic_rows[0]["row_granularity"] == "ticker-date"
    assert semantic_rows[0]["stage"] == "source_weighted_semantic_aggregation"
    assert semantic_rows[0]["artifact_key"].endswith(
        "sentiment_features/layer1-semantic-review-fixture.parquet"
    )
    assert semantic_rows[0]["features"]["nlp_article_count"] == 2.0
    assert semantic_rows[0]["features"]["nlp_contributing_article_ids"] == [
        "aapl-001",
        "ferrari-001",
    ]

    semantic_group = cast(list[dict[str, Any]], date_groups["2026-05-21"]["semantic_aggregates"])
    assert semantic_group[0]["row_granularity"] == "ticker-date"
    assert semantic_group[0]["contributing_article_ids"] == ["aapl-001", "ferrari-001"]

    context = cast(dict[str, Any], report_dict["hmm_evaluation_context"])
    assert context["requested_inference_dates"] == ["2026-05-21", "2026-05-22"]
    assert context["observed_inference_dates"] == ["2026-05-21", "2026-05-22"]
    assert context["warnings"] == []
    assert context["training_windows"][0]["train_end_date"] == "2026-05-20"
    assert context["complete_training_rows_sufficient"] is None
    assert "min_training_rows is absent" in context["complete_training_rows_sufficient_derivation"]


def test_benchmark_context_is_bounded_to_trailing_25_trading_days() -> None:
    """A one-day inference request still asks the benchmark loader for 25 days."""
    dates = _benchmark_context_dates(["2026-05-22"])
    assert len(dates) == 25
    assert dates[-1] == "2026-05-22"


def test_hmm_training_sufficiency_requires_explicit_threshold() -> None:
    """Absent thresholds stay unknown; explicit 98 >= 30 derives true."""
    assert _training_rows_sufficient([{"complete_training_rows": 98}]) is None
    assert (
        _training_rows_sufficient([{"complete_training_rows": 98, "min_training_rows": 30}]) is True
    )


def test_training_regime_rows_are_bounded_to_250_points(monkeypatch: pytest.MonkeyPatch) -> None:
    """Training-window chart context preserves endpoints while bounding payload size."""

    def fake_read(_writer: object, keys: tuple[str, str]) -> tuple[pd.DataFrame, str]:
        date_text = keys[0].split("/")[1]
        return pd.DataFrame([{"date": date_text, "regime": "sideways", "confidence": 0.8}]), keys[0]

    monkeypatch.setattr(
        "core.features.aapl_evidence._read_first_available_parquet_frame", fake_read
    )
    rows = _load_training_regime_rows(
        writer=cast(Any, object()),
        manifests=[{"train_start_date": "2025-01-01", "train_end_date": "2026-03-31"}],
        run_id="run-1",
        artifact_keys={"regime": []},
    )
    assert len(rows) == 250
    assert rows[0]["date"] == "2025-01-02"
    assert rows[-1]["date"] == "2026-03-31"


def test_semantic_review_report_loads_dated_stage_artifacts_for_parent_run_id(
    tmp_path: Path,
) -> None:
    """Parent run ids should resolve dated scored-news and regime stage artifacts."""
    fixture = seed_semantic_review_fixture(local_root=tmp_path / "r2")
    writer = fixture["writer"]
    parent_run_id = "layer1-aapl-accuracy-2026-05-06-to-2026-05-28-v4-after-pr221"
    stage_run_ids = {
        "2026-05-21": f"{parent_run_id}-2026-05-21",
        "2026-05-22": f"{parent_run_id}-2026-05-22",
    }

    for date_text, stage_run_id in stage_run_ids.items():
        for path_builder in (
            layer1_news_preprocessing_path,
            layer1_text_embedding_path,
            layer1_topic_label_path,
            layer1_news_relevance_gate_path,
            layer1_sentiment_score_path,
            layer1_sentiment_feature_path,
        ):
            writer.put_object(
                path_builder(date_text, stage_run_id),
                writer.get_object(path_builder(date_text, fixture["run_id"])),
            )
        writer.put_object(
            layer1_regime_path(date_text, stage_run_id),
            writer.get_object(layer1_regime_path(date_text, fixture["run_id"])),
        )
    writer.put_object(
        pipeline_manifest_path("layer1_5_regime", parent_run_id),
        writer.get_object(pipeline_manifest_path("layer1_5_regime", fixture["run_id"])),
    )
    writer.put_object(
        raw_price_path("AAPL"),
        writer.get_object(raw_price_path("AAPL")),
    )

    report = build_layer1_aapl_evidence_report(
        run_id=parent_run_id,
        from_date="2026-05-21",
        to_date="2026-05-25",
        ticker="AAPL",
        writer=writer,
    )
    report_dict = cast(dict[str, Any], report.to_dict())

    assert report_dict["row_count"] == 8
    assert report_dict["article_count"] == 4
    assert report_dict["date_count"] == 2
    assert report_dict["regime_rows"][0]["date"] == "2026-05-21"
    assert report_dict["load_warnings"] == []


def test_semantic_review_payload_flags_weak_and_duplicate_articles(tmp_path: Path) -> None:
    """The payload should flag duplicate headlines and weak/non-AAPL article contamination."""
    fixture = seed_semantic_review_fixture(local_root=tmp_path / "r2")
    report = build_layer1_aapl_evidence_report(
        run_id=str(fixture["run_id"]),
        from_date="2026-05-21",
        to_date="2026-05-22",
        ticker="AAPL",
        writer=fixture["writer"],
    )
    payload = cast(dict[str, Any], build_layer1_semantic_review_dashboard_payload(report))

    flagged_ids = {
        str(item["article_id"]) for item in cast(list[dict[str, Any]], payload["flagged_articles"])
    }
    accepted_ids = {
        str(item["article_id"]) for item in cast(list[dict[str, Any]], payload["accepted_articles"])
    }
    sections = cast(dict[str, Any], payload["pipeline_sections"])

    assert flagged_ids == {"aapl-001", "aapl-002", "ferrari-001"}
    assert accepted_ids == {"aapl-003"}
    assert payload["human_semantic_review_status"] == "needs_human_review"
    assert payload["benchmark_ticker"] == "SPY"
    assert len(cast(list[dict[str, Any]], payload["benchmark_price_series"])) == 2
    assert len(cast(list[dict[str, Any]], payload["benchmark_market_regime_series"])) == 2
    assert (
        cast(dict[str, Any], payload["pipeline_section_counts"])["raw_preprocessing_rows"][
            "row_count"
        ]
        == 8
    )
    assert (
        cast(dict[str, Any], payload["pipeline_section_counts"])["topic_label_rows"]["row_count"]
        == 4
    )
    assert (
        cast(dict[str, Any], payload["pipeline_section_counts"])["relevance_gate_rows"]["row_count"]
        == 8
    )
    assert (
        cast(dict[str, Any], payload["pipeline_section_counts"])["semantic_aggregate_rows"][
            "row_count"
        ]
        == 2
    )
    assert len(cast(list[dict[str, Any]], sections["raw_preprocessing_rows"])) == 1
    assert len(cast(list[dict[str, Any]], sections["topic_label_rows"])) == 1
    assert len(cast(list[dict[str, Any]], sections["relevance_gate_rows"])) == 1
    assert len(cast(list[dict[str, Any]], sections["semantic_aggregate_rows"])) == 2
    assert len(cast(list[dict[str, Any]], payload["price_series"])) == 2
    assert len(cast(list[dict[str, Any]], payload["market_regime_series"])) == 2
    assert len(cast(list[dict[str, Any]], sections["stock_price_rows"])) == 2
    assert len(cast(list[dict[str, Any]], sections["date_aligned_price_hmm_rows"])) == 2
    assert payload["hmm_evaluation_context"]["warnings"] == []
    assert payload["smoke"]["status"] == "pass"
    assert payload["smoke"]["ready_for_final_human_acceptance"] is True
    assert payload["smoke"]["visual_browser_qa_required"] is True
    assert payload["smoke"]["required_stage_row_counts"]["benchmark_price_context"] == 2

    article_groups = cast(list[dict[str, Any]], payload["article_groups"])
    ferrari = next(item for item in article_groups if item["article_id"] == "ferrari-001")
    assert "no_requested_ticker_evidence" in ferrari["contamination_flags"]
    assert ferrari["requested_ticker_term_hits"] == []
    finbert_articles = {
        str(item["article_id"]): cast(dict[str, Any], item)
        for item in cast(list[dict[str, Any]], payload["finbert_sentence_review"]["articles"])
    }
    assert finbert_articles["ferrari-001"]["sentence_rows"][0]["sentence_index"] == 0
    assert finbert_articles["ferrari-001"]["sentence_rows"][0]["text"].startswith(
        "Ferrari shares fell sharply"
    )

    duplicate = next(item for item in article_groups if item["article_id"] == "aapl-001")
    assert "duplicate_normalized_headline" in duplicate["contamination_flags"]
    assert duplicate["requested_ticker_term_hits"] == ["apple"]
    assert payload["article_review"]["accepted_article_count"] == 1
    assert payload["article_review"]["contamination_article_count"] == 3


def test_semantic_review_payload_separates_tabs_and_reports_missing_sentence_text(
    tmp_path: Path,
) -> None:
    """The payload should keep article review separate from sentence review and warn on missing text."""
    fixture = seed_semantic_review_fixture(local_root=tmp_path / "r2")
    report = build_layer1_aapl_evidence_report(
        run_id=str(fixture["run_id"]),
        from_date="2026-05-21",
        to_date="2026-05-22",
        ticker="AAPL",
        writer=fixture["writer"],
    )
    report_dict = cast(dict[str, Any], report.to_dict())
    article_groups = cast(list[dict[str, Any]], report_dict["article_groups"])
    date_groups = cast(list[dict[str, Any]], report_dict["date_groups"])
    for article in article_groups:
        if article.get("article_id") == "aapl-001":
            cast(list[dict[str, Any]], article["sentence_rows"])[1]["text"] = None
            break
    for date_group in date_groups:
        articles = cast(list[dict[str, Any]], date_group.get("articles", []))
        for article in articles:
            if article.get("article_id") == "aapl-001":
                cast(list[dict[str, Any]], article["sentence_rows"])[1]["text"] = None
                break

    payload = cast(dict[str, Any], build_layer1_semantic_review_dashboard_payload(report_dict))
    article_review = cast(dict[str, Any], payload["article_review"])
    finbert_review = cast(dict[str, Any], payload["finbert_sentence_review"])
    finbert_articles = cast(list[dict[str, Any]], finbert_review["articles"])
    missing_article = next(item for item in finbert_articles if item["article_id"] == "aapl-001")

    assert article_review["accepted_date_groups"][0]["articles"][0]["article_id"] == "aapl-003"
    assert article_review["accepted_article_count"] == 1
    assert article_review["contamination_article_count"] == 3
    assert finbert_review["row_count"] == 8
    assert finbert_review["has_missing_text"] is True
    assert finbert_review["missing_text_warning_count"] == 1
    assert finbert_review["source_artifact_gaps"][0]["gap"] == "missing_full_scored_sentence_text"
    assert finbert_review["sentiment_label_counts"]["positive"] == 6
    assert finbert_review["sentiment_label_counts"]["negative"] == 2
    assert missing_article["full_scored_text_available"] is False
    assert missing_article["full_scored_text_warning"]
    sentence_rows = cast(list[dict[str, Any]], missing_article["sentence_rows"])
    assert sentence_rows[0]["sentiment_label"] == "positive"
    assert sentence_rows[0]["sentiment_label_confidence"] == 0.78
    assert sentence_rows[0]["assignment_classification"] == "direct"
    assert sentence_rows[0]["assignment_reason"]
    assert any(row["missing_text_warning"] for row in sentence_rows)


def test_semantic_review_payload_warns_on_collapsed_single_topic_output(
    tmp_path: Path,
) -> None:
    """Collapsed BERTopic output should stay a warning, not a green pass."""
    fixture = seed_semantic_review_fixture(local_root=tmp_path / "r2")
    report = build_layer1_aapl_evidence_report(
        run_id=str(fixture["run_id"]),
        from_date="2026-05-21",
        to_date="2026-05-22",
        ticker="AAPL",
        writer=fixture["writer"],
    )
    report_dict = cast(dict[str, Any], report.to_dict())
    topic_review = cast(dict[str, Any], report_dict["topic_review"])
    topic_rows = cast(list[dict[str, Any]], topic_review["rows"])
    topic_topics = cast(list[dict[str, Any]], topic_review["topics"])

    collapsed_row = copy.deepcopy(topic_rows[0])
    collapsed_row["topic_label"] = "BERTopic · bertopic-0.16.x"
    collapsed_row["topic_keywords"] = []
    collapsed_row["topic_example_text"] = None
    collapsed_row["topic_example_texts"] = []
    topic_review["rows"] = [collapsed_row]
    topic_review["topics"] = topic_topics[:1]
    topic_review["diversity_status"] = "insufficient_diversity"
    topic_review["diversity_reason"] = "Only one topic was detected across the review rows."

    payload = cast(dict[str, Any], build_layer1_semantic_review_dashboard_payload(report_dict))
    review = cast(dict[str, Any], payload["topic_relevance_review"])
    summary = cast(dict[str, Any], review["summary"])

    assert summary["topic_review_state"]["state"] == "WARN"
    assert summary["topic_review_state"]["reviewable"] is False
    assert summary["reviewable"] is False
    assert summary["review_status"] == "not_reviewable_collapsed_topic_output"
    assert "Only one topic" in summary["review_explanation"]
    assert payload["run_readiness"]["ready_for_final_human_acceptance"] is False


def test_semantic_review_payload_warns_on_default_relevance_only(
    tmp_path: Path,
) -> None:
    """A universal default relevance score should not be treated as informative evidence."""
    fixture = seed_semantic_review_fixture(local_root=tmp_path / "r2")
    report = build_layer1_aapl_evidence_report(
        run_id=str(fixture["run_id"]),
        from_date="2026-05-21",
        to_date="2026-05-22",
        ticker="AAPL",
        writer=fixture["writer"],
    )
    report_dict = cast(dict[str, Any], report.to_dict())
    for row in cast(list[dict[str, Any]], report_dict["relevance_gate_rows"]):
        row["relevance_score"] = 1.0
        row["relevance_decision"] = "accepted"

    payload = cast(dict[str, Any], build_layer1_semantic_review_dashboard_payload(report_dict))
    review = cast(dict[str, Any], payload["topic_relevance_review"])
    summary = cast(dict[str, Any], review["summary"])

    assert summary["relevance_informativeness_state"]["state"] == "WARN"
    assert summary["relevance_informativeness_state"]["reviewable"] is False
    assert summary["relevance_informativeness_state"]["coverage"] == "complete"
    assert summary["reviewable"] is False
    assert summary["review_status"] == "not_reviewable_uninformative_relevance"
    assert "default-like" in summary["review_explanation"]
    assert payload["run_readiness"]["ready_for_final_human_acceptance"] is False


def test_semantic_review_payload_warns_on_one_point_hmm_chart(
    tmp_path: Path,
) -> None:
    """A one-point HMM chart should be limited and fail readiness."""
    fixture = seed_semantic_review_fixture(local_root=tmp_path / "r2")
    report = build_layer1_aapl_evidence_report(
        run_id=str(fixture["run_id"]),
        from_date="2026-05-21",
        to_date="2026-05-22",
        ticker="AAPL",
        writer=fixture["writer"],
    )
    report_dict = cast(dict[str, Any], report.to_dict())
    report_dict["benchmark_price_rows"] = cast(
        list[dict[str, Any]], report_dict["benchmark_price_rows"]
    )[:1]
    report_dict["benchmark_market_regime_rows"] = cast(
        list[dict[str, Any]], report_dict["benchmark_market_regime_rows"]
    )[:1]

    payload = cast(dict[str, Any], build_layer1_semantic_review_dashboard_payload(report_dict))
    readiness = cast(dict[str, Any], payload["run_readiness"])
    smoke = cast(dict[str, Any], payload["smoke"])

    assert smoke["status"] == "fail"
    assert any(
        item["reason"] == "insufficient_hmm_chart_points"
        for item in cast(list[dict[str, Any]], smoke["failures"])
    )
    assert readiness["hmm_chart_auditability_state"]["state"] == "WARN"
    assert readiness["hmm_chart_auditability_state"]["reviewable"] is False
    assert readiness["ready_for_final_human_acceptance"] is False
    assert readiness["human_review_status"] == "blocked_by_missing_pipeline_evidence"


def test_semantic_review_payload_explains_unreviewable_missing_relevance_gate(
    tmp_path: Path,
) -> None:
    """The topic/relevance tab should explain missing gate artifacts once, not as row spam."""
    fixture = seed_semantic_review_fixture(local_root=tmp_path / "r2")
    report = build_layer1_aapl_evidence_report(
        run_id=str(fixture["run_id"]),
        from_date="2026-05-21",
        to_date="2026-05-22",
        ticker="AAPL",
        writer=fixture["writer"],
    )
    report_dict = cast(dict[str, Any], report.to_dict())
    report_dict["relevance_gate_rows"] = []

    payload = cast(dict[str, Any], build_layer1_semantic_review_dashboard_payload(report_dict))
    review = cast(dict[str, Any], payload["topic_relevance_review"])
    summary = cast(dict[str, Any], review["summary"])

    assert summary["relevance_gate_row_count"] == 0
    assert summary["embedding_row_count"] > 0
    assert summary["topic_label_row_count"] > 0
    assert summary["reviewable"] is False
    assert summary["review_status"] == "not_run_relevance_gate"
    assert summary["diagnostic_state"] == "NO_DATA"
    assert summary["relevance_informativeness_state"]["state"] == "NO_DATA"
    assert summary["topic_review_state"]["state"] == "PASS"
    assert summary["embedding_coverage_state"]["state"] == "PASS"
    assert "No pre-FinBERT relevance rows were provided." in summary["review_explanation"]
    assert review["missing_evidence_blockers"]
    assert payload["run_readiness"]["ready_for_final_human_acceptance"] is False


def test_semantic_review_payload_adds_human_focused_aggregate_review(
    tmp_path: Path,
) -> None:
    """Ticker-date aggregates should expose useful NLP review cards instead of mostly n/a fields."""
    fixture = seed_semantic_review_fixture(local_root=tmp_path / "r2")
    report = build_layer1_aapl_evidence_report(
        run_id=str(fixture["run_id"]),
        from_date="2026-05-21",
        to_date="2026-05-22",
        ticker="AAPL",
        writer=fixture["writer"],
    )

    report_dict = cast(dict[str, Any], report.to_dict())
    semantic_rows = cast(list[dict[str, Any]], report_dict["semantic_aggregate_rows"])
    first_row = semantic_rows[0]
    first_features = cast(dict[str, Any], first_row["features"])
    first_features["nlp_target_impact_direction"] = "positive"
    first_features["nlp_target_impact_magnitude"] = "high"
    first_features["nlp_target_impact_confidence"] = 0.91
    first_features["nlp_causal_channel"] = "product_device"
    first_features["nlp_target_impact_horizon"] = "short_term"
    first_features["nlp_source_count"] = 1
    first_features["nlp_dominant_source_weight_share"] = 1.0
    first_row["source_weight_summary"] = [{"source": "Benzinga", "weight_share": 1.0}]

    payload = cast(dict[str, Any], build_layer1_semantic_review_dashboard_payload(report_dict))
    review = cast(dict[str, Any], payload["semantic_aggregate_review"])
    rows = cast(list[dict[str, Any]], review["rows"])

    assert review["summary"]["row_count"] == 2
    assert rows[0]["target_company_impact_direction"] == "positive"
    assert rows[0]["target_company_impact_magnitude"] == "high"
    assert rows[0]["target_impact_confidence"] == 0.91
    assert rows[0]["causal_channel"] == "product_device"
    assert rows[0]["impact_horizon"] == "short_term"
    assert rows[0]["single_source_concentration"] is True
    assert rows[0]["source_count"] == 1
    assert rows[0]["dominant_source_weight_share"] == 1.0
    assert "single_source_concentration" in rows[0]["semantic_warning_codes"]
    assert rows[0]["sentiment_label"] in {"positive", "negative", "neutral"}
    assert rows[0]["human_review_summary"].startswith("Overall NLP sentiment is")
    card_labels = {
        str(card["label"]) for card in cast(list[dict[str, Any]], rows[0]["review_value_cards"])
    }
    assert {
        "Target impact direction",
        "Target impact magnitude",
        "Target impact confidence",
        "Causal channel",
        "Impact horizon",
        "Source concentration",
        "Overall sentiment",
        "Positive / negative / neutral mix",
        "Articles / sentences",
        "Relevance score",
    }.issubset(card_labels)
    assert review["summary"]["target_impact_direction"] == "positive"
    assert review["summary"]["single_source_concentration"] is True
    assert payload["run_readiness"]["single_source_concentration"] is True
    assert payload["run_readiness"]["target_impact_direction"] == "positive"


def test_semantic_review_payload_marks_degraded_hmm_feature_set_without_blocking(
    tmp_path: Path,
) -> None:
    """A degraded HMM feature set should be explained even if it does not block readiness."""
    fixture = seed_semantic_review_fixture(local_root=tmp_path / "r2")
    report = build_layer1_aapl_evidence_report(
        run_id=str(fixture["run_id"]),
        from_date="2026-05-21",
        to_date="2026-05-22",
        ticker="AAPL",
        writer=fixture["writer"],
    )
    report_dict = cast(dict[str, Any], report.to_dict())
    hmm_context = cast(dict[str, Any], report_dict["hmm_evaluation_context"])
    hmm_context["warnings"] = ["incomplete_hmm_feature_set"]
    hmm_context["feature_set_blocking"] = False

    payload = cast(dict[str, Any], build_layer1_semantic_review_dashboard_payload(report_dict))
    readiness = cast(dict[str, Any], payload["run_readiness"])

    assert readiness["hmm_feature_set_state"] == "WARN"
    assert readiness["hmm_feature_set_reviewable"] is True
    assert "degraded but non-blocking" in readiness["hmm_feature_set_warning"]
    assert "degraded but non-blocking" in readiness["status_reason"]


def test_semantic_review_topic_relevance_tab_flags_default_relevance(
    tmp_path: Path,
) -> None:
    """Topic/relevance review should expose supporting evidence and default relevance flags."""
    fixture = seed_semantic_review_fixture(local_root=tmp_path / "r2")
    report = build_layer1_aapl_evidence_report(
        run_id=str(fixture["run_id"]),
        from_date="2026-05-21",
        to_date="2026-05-22",
        ticker="AAPL",
        writer=fixture["writer"],
    )
    payload = cast(dict[str, Any], build_layer1_semantic_review_dashboard_payload(report))
    review = cast(dict[str, Any], payload["topic_relevance_review"])
    articles = {
        str(item["article_id"]): cast(dict[str, Any], item)
        for item in cast(list[dict[str, Any]], review["articles"])
    }

    accepted = articles["aapl-003"]
    defaulted = articles["ferrari-001"]

    assert cast(dict[str, Any], review["summary"])["accepted_count"] == 3
    assert accepted["evidence_status"] == "accepted"
    assert accepted["ticker_relevance_score"] == 1.0
    assert accepted["financial_relevance_score"] == 0.8
    assert accepted["topic_relevance_score"] == 0.82
    assert accepted["reason_codes"] == ["target_entity_mention"]
    assert accepted["embedding_evidence"][0]["embedding_model"] == "sentence-transformers/test"
    assert accepted["topic_evidence"][0]["topic_id"] == 0
    assert accepted["topic_evidence"][0]["topic_probability"] == 0.82
    assert accepted["topic_evidence"][0]["topic_example_text"]
    assert len(cast(list[dict[str, Any]], accepted["sentence_rows"])) <= 3
    assert len(cast(list[dict[str, Any]], accepted["preprocessing_rows"])) <= 1
    assert accepted["assignment_classification"] == "direct"
    assert accepted["assignment_weight"] == 1.0
    assert accepted["assignment_evidence_kinds"]
    assert accepted["missing_evidence_flags"] == []

    assert defaulted["relevance_score"] == 1.0
    assert defaulted["evidence_status"] == "missing_or_default"
    assert defaulted["relevance_score_interpretation"] == "default_or_unknown_not_strong_evidence"
    assert "missing_ticker_evidence" in defaulted["missing_evidence_flags"]
    assert "rejected_by_relevance_gate" in defaulted["missing_evidence_flags"]
    assert "default_relevance_without_supporting_evidence" in defaulted["missing_evidence_flags"]
    assert defaulted["ticker_evidence"]["source_tickers"] == ["AAPL"]
    assert defaulted["entity_evidence"]["preprocessing_entity_mentions"] == ["Ferrari"]
    assert review["missing_evidence_blockers"]


def test_semantic_review_topic_relevance_tab_covers_borderline_rejected_and_missing_rows(
    tmp_path: Path,
) -> None:
    """Topic/relevance review should classify borderline, rejected, and missing-evidence rows."""
    fixture = seed_semantic_review_fixture(local_root=tmp_path / "r2")
    report = build_layer1_aapl_evidence_report(
        run_id=str(fixture["run_id"]),
        from_date="2026-05-21",
        to_date="2026-05-22",
        ticker="AAPL",
        writer=fixture["writer"],
    )
    report_dict = cast(dict[str, Any], report.to_dict())
    report_dict["embedding_rows"] = [
        row
        for row in cast(list[dict[str, Any]], report_dict["embedding_rows"])
        if row["article_id"] != "aapl-001"
    ]
    report_dict["topic_label_rows"] = [
        row
        for row in cast(list[dict[str, Any]], report_dict["topic_label_rows"])
        if row["article_id"] != "aapl-001"
    ]
    for row in cast(list[dict[str, Any]], report_dict["relevance_gate_rows"]):
        if row["article_id"] == "aapl-002":
            row["relevance_decision"] = "borderline"
            row["relevance_score"] = 0.64
            row["reason_codes"] = ["borderline_topic_evidence"]
        if row["article_id"] == "aapl-003":
            row["relevance_decision"] = "rejected"
            row["relevance_score"] = 0.2
            row["ticker_relevance_score"] = 0.0
            row["reason_codes"] = ["low_ticker_relevance"]

    payload = cast(dict[str, Any], build_layer1_semantic_review_dashboard_payload(report_dict))
    review = cast(dict[str, Any], payload["topic_relevance_review"])
    articles = {
        str(item["article_id"]): cast(dict[str, Any], item)
        for item in cast(list[dict[str, Any]], review["articles"])
    }
    summary = cast(dict[str, Any], review["summary"])

    assert articles["aapl-001"]["evidence_status"] == "missing_or_default"
    assert "missing_embedding" in articles["aapl-001"]["missing_evidence_flags"]
    assert "missing_topic_label" in articles["aapl-001"]["missing_evidence_flags"]
    assert articles["aapl-002"]["evidence_status"] == "borderline"
    assert articles["aapl-002"]["relevance_score_interpretation"] == "computed_borderline"
    assert "borderline_relevance_gate" in articles["aapl-002"]["missing_evidence_flags"]
    assert articles["aapl-003"]["evidence_status"] == "rejected"
    assert articles["aapl-003"]["relevance_score_interpretation"] == "computed_rejected"
    assert "rejected_by_relevance_gate" in articles["aapl-003"]["missing_evidence_flags"]
    assert summary["missing_embedding_count"] == 1
    assert summary["missing_topic_count"] == 1
    assert summary["borderline_count"] == 1
    assert summary["rejected_count"] == 1


def test_semantic_review_summary_gate_status_allows_complete_evidence(tmp_path: Path) -> None:
    """Complete raw evidence should produce ready summary and gate-card fields."""
    fixture = seed_semantic_review_fixture(local_root=tmp_path / "r2")
    report = build_layer1_aapl_evidence_report(
        run_id=str(fixture["run_id"]),
        from_date="2026-05-21",
        to_date="2026-05-22",
        ticker="AAPL",
        writer=fixture["writer"],
    )
    payload = cast(dict[str, Any], build_layer1_semantic_review_dashboard_payload(report))
    readiness = cast(dict[str, Any], payload["run_readiness"])
    gates = {
        str(item["key"]): cast(dict[str, Any], item)
        for item in cast(list[dict[str, Any]], payload["gate_cards"])
    }

    assert readiness["ready_for_final_human_acceptance"] is True
    assert readiness["recommendation"] == "ready for final human acceptance"
    assert readiness["human_review_status"] == "can_start"
    assert readiness["diagnostic_states"]["topic_review"] == "PASS"
    assert readiness["diagnostic_states"]["relevance_informativeness"] == "PASS"
    assert readiness["diagnostic_states"]["embedding_coverage"] == "PASS"
    assert readiness["diagnostic_states"]["hmm_chart_auditability"] == "PASS"
    assert readiness["hmm_chart_auditability_state"]["state"] == "PASS"
    assert readiness["article_count"] == 4
    assert readiness["sentence_row_count"] == 8
    assert payload["missing_pipeline_sections"] == []
    assert gates["news_preprocessing"]["status"] == "ready"
    assert gates["text_embeddings"]["row_count"] == 4
    assert gates["topic_labels"]["status"] == "ready"
    assert gates["news_relevance_gate"]["status"] == "ready"
    assert gates["sentiment_features"]["status"] == "ready"
    assert gates["hmm_regime"]["status"] == "ready"
    assert gates["stock_price_context"]["status"] == "ready"
    assert gates["benchmark_price_context"]["status"] == "ready"


def test_semantic_review_summary_gate_status_blocks_missing_evidence(tmp_path: Path) -> None:
    """Missing stage and benchmark artifacts should be surfaced as blocked gate cards."""
    fixture = seed_semantic_review_fixture(
        local_root=tmp_path / "r2",
        include_benchmark_price_rows=False,
    )
    writer = fixture["writer"]
    run_id = str(fixture["run_id"])
    writer.delete_object(layer1_topic_label_path("2026-05-21", run_id))
    report = build_layer1_aapl_evidence_report(
        run_id=run_id,
        from_date="2026-05-21",
        to_date="2026-05-22",
        ticker="AAPL",
        writer=writer,
    )
    payload = cast(dict[str, Any], build_layer1_semantic_review_dashboard_payload(report))
    readiness = cast(dict[str, Any], payload["run_readiness"])
    gates = {
        str(item["key"]): cast(dict[str, Any], item)
        for item in cast(list[dict[str, Any]], payload["gate_cards"])
    }
    missing_labels = {
        str(item["label"])
        for item in cast(list[dict[str, Any]], payload["missing_pipeline_sections"])
    }

    assert readiness["ready_for_final_human_acceptance"] is False
    assert readiness["recommendation"] == "not ready for final human acceptance"
    assert readiness["human_review_status"] == "blocked_by_missing_pipeline_evidence"
    assert gates["topic_labels"]["status"] == "blocked"
    assert gates["benchmark_price_context"]["status"] == "blocked"
    assert "BERTopic labels" in missing_labels
    assert "Benchmark price rows" in missing_labels
    assert (
        layer1_topic_label_path("2026-05-21", run_id)
        in gates["topic_labels"]["missing_or_tried_keys"]
    )


def test_semantic_review_summary_gate_status_blocks_missing_semantic_aggregate_rows(
    tmp_path: Path,
) -> None:
    """Missing ticker-date aggregate rows should block readiness and name the aggregate gate."""
    fixture = seed_semantic_review_fixture(local_root=tmp_path / "r2")
    writer = fixture["writer"]
    run_id = str(fixture["run_id"])
    writer.delete_object(layer1_sentiment_feature_path("2026-05-22", run_id))

    report = build_layer1_aapl_evidence_report(
        run_id=run_id,
        from_date="2026-05-21",
        to_date="2026-05-22",
        ticker="AAPL",
        writer=writer,
    )
    payload = cast(dict[str, Any], build_layer1_semantic_review_dashboard_payload(report))
    readiness = cast(dict[str, Any], payload["run_readiness"])
    gates = {
        str(item["key"]): cast(dict[str, Any], item)
        for item in cast(list[dict[str, Any]], payload["gate_cards"])
    }
    missing_labels = {
        str(item["label"])
        for item in cast(list[dict[str, Any]], payload["missing_pipeline_sections"])
    }

    assert payload["warnings"]
    assert any(
        item["scope"] == "sentiment_features"
        for item in cast(list[dict[str, Any]], payload["warnings"])
    )
    assert readiness["ready_for_final_human_acceptance"] is False
    assert readiness["recommendation"] == "not ready for final human acceptance"
    assert readiness["human_review_status"] == "blocked_by_missing_pipeline_evidence"
    assert gates["sentiment_features"]["status"] == "blocked"
    assert "Ticker-Date Semantic Aggregates" in missing_labels
    assert (
        layer1_sentiment_feature_path("2026-05-22", run_id)
        in gates["sentiment_features"]["missing_or_tried_keys"]
    )


def test_semantic_review_summary_gate_status_blocks_cached_bundle_fallback(
    tmp_path: Path,
) -> None:
    """Cached bundles should be prominent and remain blocked for final acceptance."""
    run_id = "layer1-aapl-accuracy-2026-05-06-to-2026-05-28-v4-after-pr221"
    fixture = seed_semantic_review_fixture(local_root=tmp_path / "empty-r2", run_id="unused")
    report = build_layer1_aapl_evidence_report(
        run_id=run_id,
        from_date="2026-05-06",
        to_date="2026-05-28",
        ticker="AAPL",
        writer=fixture["writer"],
    )
    payload = cast(dict[str, Any], build_layer1_semantic_review_dashboard_payload(report))
    readiness = cast(dict[str, Any], payload["run_readiness"])
    sections = {
        str(item["key"]): cast(dict[str, Any], item)
        for item in cast(list[dict[str, Any]], payload["missing_pipeline_sections"])
    }

    assert any(
        item["scope"] == "cached_bundle" for item in cast(list[dict[str, Any]], payload["warnings"])
    )
    assert readiness["ready_for_final_human_acceptance"] is False
    assert readiness["recommendation"] == "not ready for final human acceptance"
    assert "news_preprocessing" in sections
    assert "sentiment_features" in sections
    assert "stock_price_context" in sections


def test_semantic_review_summary_gate_status_handles_no_row_runs(tmp_path: Path) -> None:
    """A run with no loaded rows should return stable blocked readiness fields."""
    writer = R2Writer(local_root=tmp_path / "empty-r2")
    report = build_layer1_aapl_evidence_report(
        run_id="semantic-review-no-row-run",
        from_date="2026-05-21",
        to_date="2026-05-22",
        ticker="AAPL",
        writer=writer,
    )
    payload = cast(dict[str, Any], build_layer1_semantic_review_dashboard_payload(report))
    readiness = cast(dict[str, Any], payload["run_readiness"])
    gate_keys = {
        str(item["key"])
        for item in cast(list[dict[str, Any]], payload["missing_pipeline_sections"])
    }

    assert readiness["sentence_row_count"] == 0
    assert readiness["article_count"] == 0
    assert readiness["date_count"] == 0
    assert readiness["ready_for_final_human_acceptance"] is False
    assert readiness["diagnostic_states"]["topic_review"] == "NO_DATA"
    assert readiness["diagnostic_states"]["relevance_informativeness"] == "NO_DATA"
    assert readiness["diagnostic_states"]["embedding_coverage"] == "NO_DATA"
    assert readiness["diagnostic_summary"]["overall_state"] == "NO_DATA"
    assert "news_sentiment_scored" in gate_keys
    assert "hmm_regime" in gate_keys
    assert "stock_price_context" in gate_keys
    readiness_summary = cast(
        dict[str, Any], build_layer1_semantic_review_readiness_summary(payload)
    )
    assert readiness_summary["run_readiness"]["recommendation"] == (
        "not ready for final human acceptance"
    )


def test_semantic_review_payload_suppresses_benchmark_chart_when_benchmark_missing(
    tmp_path: Path,
) -> None:
    """Missing benchmark rows should leave the chart with no SPY data to render."""
    fixture = seed_semantic_review_fixture(
        local_root=tmp_path / "r2",
        include_benchmark_price_rows=False,
    )
    report = build_layer1_aapl_evidence_report(
        run_id=str(fixture["run_id"]),
        from_date="2026-05-21",
        to_date="2026-05-22",
        ticker="AAPL",
        writer=fixture["writer"],
    )
    payload = cast(dict[str, Any], build_layer1_semantic_review_dashboard_payload(report))

    assert payload["benchmark_ticker"] == "SPY"
    assert payload["benchmark_price_series"] == []
    assert len(cast(list[dict[str, Any]], payload["benchmark_market_regime_series"])) == 2
    assert any(
        item["scope"] == "price_series" for item in cast(list[dict[str, Any]], payload["warnings"])
    )
    smoke = cast(dict[str, Any], payload["smoke"])
    assert smoke["status"] == "fail"
    failure_reasons = {item["reason"] for item in cast(list[dict[str, Any]], smoke["failures"])}
    assert "empty_benchmark_price_rows" in failure_reasons
    assert "no_renderable_benchmark_prices" in failure_reasons


def test_semantic_review_smoke_reports_missing_hmm_manifest_metadata(tmp_path: Path) -> None:
    """Missing HMM manifest or training-window metadata should block final acceptance."""
    fixture = seed_semantic_review_fixture(local_root=tmp_path / "r2")
    report = build_layer1_aapl_evidence_report(
        run_id=str(fixture["run_id"]),
        from_date="2026-05-21",
        to_date="2026-05-22",
        ticker="AAPL",
        writer=fixture["writer"],
    )
    payload = copy.deepcopy(
        cast(dict[str, Any], build_layer1_semantic_review_dashboard_payload(report))
    )
    hmm_context = cast(dict[str, Any], payload["hmm_evaluation_context"])
    hmm_context["source_manifest_keys"] = []
    hmm_context["training_windows"] = []

    smoke = validate_layer1_semantic_review_dashboard_payload(payload)
    failures = cast(list[dict[str, Any]], smoke["failures"])
    failure_reasons = {
        item["reason"]
        for item in failures
        if item["stage"] in {"hmm_manifest", "hmm_evaluation_context"}
    }

    assert smoke["status"] == "fail"
    assert "missing_hmm_manifest" in failure_reasons
    assert "missing_training_window_metadata" in failure_reasons


def test_semantic_review_smoke_allows_degraded_hmm_feature_set_when_layer2_ready(
    tmp_path: Path,
) -> None:
    """Dropped HMM input columns should degrade, not block, when the manifest says layer 2 is ready."""
    fixture = seed_semantic_review_fixture(local_root=tmp_path / "r2")
    report = build_layer1_aapl_evidence_report(
        run_id=str(fixture["run_id"]),
        from_date="2026-05-21",
        to_date="2026-05-22",
        ticker="AAPL",
        writer=fixture["writer"],
    )
    payload = copy.deepcopy(
        cast(dict[str, Any], build_layer1_semantic_review_dashboard_payload(report))
    )
    hmm_context = cast(dict[str, Any], payload["hmm_evaluation_context"])
    hmm_context["warnings"] = ["incomplete_hmm_feature_set"]
    hmm_context["dropped_feature_columns"] = list(HMM_OPTIONAL_FEATURE_COLUMNS)
    hmm_context["expected_input_feature_columns"] = [
        "spy_log_return_1d",
        "spy_return_5d",
        "spy_realized_vol_21d",
        "spy_realized_vol_63d",
        "spy_vol_ratio_21_63",
        "spy_drawdown_63d",
        "vix_level",
        "vix_change_5d",
        "yield_curve_slope_10y_2y",
        "yield_curve_slope_10y_3m",
        "high_yield_spread",
    ]
    hmm_context["input_feature_columns_used"] = [
        "spy_log_return_1d",
        "spy_return_5d",
        "spy_realized_vol_21d",
        "spy_realized_vol_63d",
        "spy_vol_ratio_21_63",
        "spy_drawdown_63d",
        "vix_level",
        "vix_change_5d",
        "yield_curve_slope_10y_2y",
        "yield_curve_slope_10y_3m",
    ]
    hmm_context["feature_set_optional_columns"] = list(HMM_OPTIONAL_FEATURE_COLUMNS)
    hmm_context["training_windows"] = [
        {
            "train_start_date": "2026-02-02",
            "train_end_date": "2026-05-20",
            "macro_load_start_date": "2026-02-02",
            "macro_load_end_date": "2026-05-22",
            "training_rows": 770,
            "complete_training_rows": 770,
        }
    ]
    hmm_context["regime_layer2_ready"] = True
    hmm_context["complete_training_rows_sufficient"] = True
    hmm_context["feature_set_status"] = "degraded"
    hmm_context["feature_set_blocking"] = None

    smoke = validate_layer1_semantic_review_dashboard_payload(payload)
    failures = cast(list[dict[str, Any]], smoke["failures"])

    assert smoke["status"] == "pass"
    assert smoke["ready_for_final_human_acceptance"] is True
    assert hmm_context["dropped_feature_columns"] == list(HMM_OPTIONAL_FEATURE_COLUMNS)
    assert hmm_context["feature_set_optional_columns"] == list(HMM_OPTIONAL_FEATURE_COLUMNS)
    assert hmm_context["feature_set_status"] == "degraded"
    assert hmm_context["warnings"] == ["incomplete_hmm_feature_set"]
    assert not any(item["stage"] == "hmm_evaluation_context" for item in failures)


@pytest.mark.parametrize(
    "dropped_columns",
    [
        ["vix_level"],
        ["spy_log_return_1d"],
        ["high_yield_spread", "vix_level"],
    ],
)
def test_semantic_review_smoke_blocks_non_allowlisted_hmm_feature_drops(
    tmp_path: Path,
    dropped_columns: list[str],
) -> None:
    """Only the allowlisted HMM drop may degrade; other missing inputs must block smoke."""
    fixture = seed_semantic_review_fixture(local_root=tmp_path / "r2")
    report = build_layer1_aapl_evidence_report(
        run_id=str(fixture["run_id"]),
        from_date="2026-05-21",
        to_date="2026-05-22",
        ticker="AAPL",
        writer=fixture["writer"],
    )
    payload = copy.deepcopy(
        cast(dict[str, Any], build_layer1_semantic_review_dashboard_payload(report))
    )
    hmm_context = cast(dict[str, Any], payload["hmm_evaluation_context"])
    hmm_context["warnings"] = ["incomplete_hmm_feature_set"]
    hmm_context["dropped_feature_columns"] = dropped_columns
    hmm_context["feature_set_optional_columns"] = list(HMM_OPTIONAL_FEATURE_COLUMNS)
    hmm_context["training_windows"] = [
        {
            "train_start_date": "2026-02-02",
            "train_end_date": "2026-05-20",
            "macro_load_start_date": "2026-02-02",
            "macro_load_end_date": "2026-05-22",
            "training_rows": 770,
            "complete_training_rows": 770,
        }
    ]
    hmm_context["regime_layer2_ready"] = True
    hmm_context["complete_training_rows_sufficient"] = True
    hmm_context["feature_set_status"] = "degraded"
    hmm_context["feature_set_blocking"] = None

    smoke = validate_layer1_semantic_review_dashboard_payload(payload)
    failures = cast(list[dict[str, Any]], smoke["failures"])
    hmm_failure = next(item for item in failures if item["stage"] == "hmm_evaluation_context")

    assert smoke["status"] == "fail"
    assert hmm_context["feature_set_optional_columns"] == list(HMM_OPTIONAL_FEATURE_COLUMNS)
    assert hmm_context["warnings"] == ["incomplete_hmm_feature_set"]
    assert hmm_failure["reason"] == "hmm_context_blocker_warnings"
    assert "incomplete_hmm_feature_set" in cast(list[str], hmm_failure["warning_codes"])


def test_semantic_review_smoke_reports_missing_stage_keys(tmp_path: Path) -> None:
    """The smoke result should name missing raw stage keys needed to repair the pilot."""
    fixture = seed_semantic_review_fixture(local_root=tmp_path / "r2")
    writer = fixture["writer"]
    run_id = str(fixture["run_id"])
    missing_key = layer1_topic_label_path("2026-05-21", run_id)
    writer.delete_object(missing_key)

    report = build_layer1_aapl_evidence_report(
        run_id=run_id,
        from_date="2026-05-21",
        to_date="2026-05-22",
        ticker="AAPL",
        writer=writer,
    )
    payload = cast(dict[str, Any], build_layer1_semantic_review_dashboard_payload(report))
    smoke = validate_layer1_semantic_review_dashboard_payload(payload)
    failures = cast(list[dict[str, Any]], smoke["failures"])
    topic_failure = next(item for item in failures if item["stage"] == "topic_labels")

    assert smoke["status"] == "fail"
    assert topic_failure["reason"] == "missing_or_incomplete_artifacts"
    assert missing_key in topic_failure["missing_or_tried_keys"]
    assert (
        layer1_topic_label_path("2026-05-21", f"{run_id}-2026-05-21")
        in topic_failure["missing_or_tried_keys"]
    )


def test_semantic_review_dashboard_html_is_beginner_friendly_and_collapsed() -> None:
    """The dashboard shell should stay clean, explain itself, and keep advanced sections collapsed."""
    html = _render_dashboard_html(
        _DashboardDefaults(
            run_id="run-123",
            from_date="2026-05-21",
            to_date="2026-05-22",
            ticker="AAPL",
            host="127.0.0.1",
            port=8766,
        )
    )
    assert "Layer 1 semantic-review dashboard" in html
    assert "Summary / Gate Status" in html
    assert "Article Review" in html
    assert "FinBERT Sentence Review" in html
    assert "Topic / Relevance Pipeline" in html
    assert "topic-relevance-tab" in html
    assert "topic-relevance-content" in html
    assert "default score shown without support" in html
    assert "Topic / relevance is not reviewable yet" in html
    assert "Topic review cards" in html
    assert "topic_label" in html
    assert "topic_keywords" in html
    assert "topic_example_text" in html
    assert "topic_example_texts" in html
    assert "topic_row_count" in html
    assert "topic_row_share" in html
    assert "topic_probability_mean" in html
    assert "topic_probability_max" in html
    assert "Ticker-Date Semantic Aggregates" in html
    assert "semantic-aggregate-tab" in html
    assert "Repeated context / aggregate value" in html
    assert "one record per <strong>(date, ticker)</strong>" in html
    assert "HMM Regime" in html
    assert "hmm-regime-tab" in html
    assert "hmm-summary-cards" in html
    assert "hmm-context-cards" in html
    assert "hmm-date-rows" in html
    assert "not ready for final human acceptance" in html
    assert "What am I looking at?" in html
    assert "Why does it matter?" in html
    assert "What would make this good or bad?" in html
    assert "Benchmark chart blocked" in html
    assert "SPY" in html
    assert "Advanced evidence and raw rows" in html
    assert "Advanced HMM evidence and raw rows" in html
    assert "data-smoke-status" in html
    assert "<details open" not in html
    assert '<table class="qa-table"' in html
    assert "date_aligned_price_hmm_rows" in html
    assert "/api/review" in html


def test_semantic_qa_tab_exposes_ordered_human_review_surface() -> None:
    """The seventh tab should expose every semantic QA section in the approved order."""
    html = _render_dashboard_html(
        _DashboardDefaults(
            run_id="exact/run...id",
            from_date="2026-05-21",
            to_date="2026-05-22",
            ticker="AAPL",
            host="127.0.0.1",
            port=8766,
        )
    )

    assert "Semantic QA / Human Review" in html
    assert 'id="semantic-qa-tab"' in html
    section_ids = [
        "semantic-qa-summary",
        "semantic-qa-funnel",
        "semantic-qa-matrix",
        "semantic-qa-queues",
        "semantic-qa-inspector",
        "semantic-qa-integrity",
        "semantic-qa-controls",
    ]
    positions = [html.index(f'id="{section_id}"') for section_id in section_ids]
    assert positions == sorted(positions)
    for filter_id in (
        "qa-run-id",
        "qa-from-date",
        "qa-to-date",
        "qa-filter-ticker",
        "qa-filter-decision",
        "qa-filter-included",
        "qa-filter-relevance",
        "qa-filter-reason",
        "qa-filter-source",
        "qa-filter-anomaly",
        "qa-filter-subject",
    ):
        assert f'id="{filter_id}"' in html
    assert "Loading four-ticker semantic QA" in html
    assert "No semantic QA rows match" in html
    assert "Partial pilot data" in html
    assert "Stale advisory" in html
    assert "Payload integrity failed" in html


def test_semantic_qa_tab_uses_bounded_api_and_wires_local_interactions() -> None:
    """The semantic QA UI should use the bounded API and browser-local shared selection state."""
    html = _render_dashboard_html(
        _DashboardDefaults(
            run_id="exact/run...id",
            from_date="2026-05-21",
            to_date="2026-05-22",
            ticker="AAPL",
            host="127.0.0.1",
            port=8766,
        )
    )

    assert "fetch(`/api/semantic-qa?${params.toString()}`)" in html
    assert "params.set('tickers', QA_PILOT_TICKERS.join(','))" in html
    assert "params.set('sample_limit', '25')" in html
    assert "loadSemanticQa" in html
    assert "applySemanticQaFilters" in html
    assert "selectQaRow" in html
    assert "data-qa-summary-filter" in html
    assert "data-qa-funnel-stage" in html
    assert "data-qa-matrix-cell" in html
    assert "data-qa-row-id" in html
    assert "setTickerDisposition" in html
    assert "setOverallDisposition" in html
    assert "sample_count + omitted_count = canonical_count" in html
    assert "Unknown" in html
    assert "No data" in html
    assert "min-height: 44px" in html
    assert "@media (max-width: 600px)" in html
    assert "localStorage" not in html
    assert "R2_ACCESS_KEY" not in html
    assert "R2_SECRET" not in html


def test_semantic_review_dashboard_hmm_tab_puts_chart_before_diagnostics() -> None:
    """The HMM tab should show the SPY chart before raw model/date diagnostics."""
    html = _render_dashboard_html(
        _DashboardDefaults(
            run_id="run-123",
            from_date="2026-05-21",
            to_date="2026-05-22",
            ticker="AAPL",
            host="127.0.0.1",
            port=8766,
        )
    )

    chart_index = html.index('id="chart-section"')
    context_details_index = html.index('id="hmm-context-section"')
    date_rows_index = html.index('id="hmm-date-rows"')

    assert chart_index < context_details_index
    assert context_details_index < date_rows_index
    assert "Model inputs and date-by-date regime rows" in html
    assert "Evidence blocker" not in html


def test_semantic_review_dashboard_html_names_human_review_outputs() -> None:
    """Visible dashboard copy should describe exactly what a human can review."""
    html = _render_dashboard_html(
        _DashboardDefaults(
            run_id="run-123",
            from_date="2026-05-21",
            to_date="2026-05-22",
            ticker="AAPL",
            host="127.0.0.1",
            port=8766,
        )
    )

    assert "Topic review diversity warning" in html
    assert "Topic / relevance is not reviewable yet" in html
    assert "Topic review cards" in html
    assert "Topic review rows" in html
    assert "Sentence sentiment label" in html
    assert "Pre-FinBERT relevance gate artifact is missing" in html
    assert "Human-review digest" in html
    assert "Only AI/ML/NLP evidence belongs here" in html


def test_semantic_review_payload_exposes_target_impact_signal_inclusion(tmp_path: Path) -> None:
    """Article review rows should show relationship, target impact, and final inclusion status."""
    fixture = seed_semantic_review_fixture(local_root=tmp_path / "r2")
    report = build_layer1_aapl_evidence_report(
        run_id=str(fixture["run_id"]),
        from_date="2026-05-21",
        to_date="2026-05-22",
        ticker="AAPL",
        writer=fixture["writer"],
    )
    report_dict = cast(dict[str, Any], report.to_dict())
    for row in cast(list[dict[str, Any]], report_dict["relevance_gate_rows"]):
        if row["article_id"] == "aapl-001":
            row.update(
                {
                    "relevance_category": "incidental_comparison",
                    "relationship_to_target": "incidental_comparison",
                    "target_context_score": 0.08,
                    "target_company_impact_direction": "none",
                    "target_company_impact_magnitude": "low",
                    "impact_horizon": "same_day",
                    "causal_channel": "comparison_context",
                    "target_impact_confidence": 0.15,
                    "article_signal_count": 0,
                    "article_contribution_weight": 0.35,
                    "relevance_decision": "rejected",
                }
            )

    payload = cast(dict[str, Any], build_layer1_semantic_review_dashboard_payload(report_dict))
    articles = cast(list[dict[str, Any]], payload["topic_relevance_review"]["articles"])
    article = next(row for row in articles if row["article_id"] == "aapl-001")

    assert article["relationship_to_target"] == "incidental_comparison"
    assert article["target_context_score"] == 0.08
    assert article["target_company_impact_direction"] == "none"
    assert article["included_in_signal"] is False
    assert article["final_contribution"] == 0.0
    assert article["target_impact_evidence_status"] == "excluded"
    assert article["target_impact_missing_flags"] == []


def test_semantic_review_dashboard_payload_is_bounded_and_valid(tmp_path: Path) -> None:
    """The normal dashboard payload should stay bounded and still validate."""
    fixture = seed_semantic_review_fixture(local_root=tmp_path / "r2")
    report = build_layer1_aapl_evidence_report(
        run_id=str(fixture["run_id"]),
        from_date="2026-05-21",
        to_date="2026-05-22",
        ticker="AAPL",
        writer=fixture["writer"],
    )

    payload = cast(dict[str, Any], build_layer1_semantic_review_dashboard_payload(report))
    payload_json = json.dumps(payload)

    assert payload["smoke"]["status"] == "pass"
    assert payload["report_summary"]["row_count"] == 8
    assert payload["pipeline_section_counts"]["raw_preprocessing_rows"]["row_count"] == 8
    assert payload["pipeline_section_counts"]["raw_preprocessing_rows"]["sample_count"] == 1
    assert payload["pipeline_section_counts"]["raw_preprocessing_rows"]["truncated"] is True
    assert len(payload_json.encode("utf-8")) < 200_000
    assert validate_layer1_semantic_review_dashboard_payload(payload)["status"] == "pass"
    assert "report" not in payload
    assert "report_summary" in payload
    assert len(cast(list[dict[str, Any]], payload["article_groups"])) == 4
    assert (
        cast(list[dict[str, Any]], payload["article_groups"])[0]["sentence_rows_sample_count"] <= 3
    )
    finbert_articles = cast(list[dict[str, Any]], payload["finbert_sentence_review"]["articles"])
    first_finbert_article = finbert_articles[0]
    assert first_finbert_article["preprocessing_row_count"] == 3
    assert len(cast(list[dict[str, Any]], first_finbert_article["preprocessing_rows"])) == 1
    assert first_finbert_article["preprocessing_rows_truncated"] is True
    assert first_finbert_article["sentence_rows_sample_count"] <= 3


def test_semantic_review_dashboard_smoke_payload_is_compact_and_valid(tmp_path: Path) -> None:
    """Smoke payloads should stay compact enough for Pi browser QA."""
    fixture = seed_semantic_review_fixture(local_root=tmp_path / "r2")
    report = build_layer1_aapl_evidence_report(
        run_id=str(fixture["run_id"]),
        from_date="2026-05-21",
        to_date="2026-05-22",
        ticker="AAPL",
        writer=fixture["writer"],
    )

    payload = cast(dict[str, Any], build_layer1_semantic_review_dashboard_smoke_payload(report))

    assert payload["smoke"]["status"] == "pass"
    assert payload["summary"]["article_count"] == 4
    assert payload["summary"]["date_count"] == 2
    assert "report" not in payload
    assert "article_groups" not in payload
    assert "date_groups" not in payload
    assert (
        len(cast(list[dict[str, Any]], payload["pipeline_sections"]["raw_preprocessing_rows"])) == 1
    )
    assert (
        len(cast(list[dict[str, Any]], payload["pipeline_sections"]["finbert_sentence_rows"])) == 1
    )
    assert len(json.dumps(payload)) < 50_000


def test_semantic_review_compaction_preserves_stable_readiness_schema() -> None:
    """A packet-sized payload keeps readiness, diagnostics, gates, and reasons intact."""
    readiness_keys = {
        "readiness_status",
        "status_reason",
        "run_id",
        "ticker",
        "from_date",
        "to_date",
        "topic_review_state",
        "topic_relevance_review_status",
        "relevance_informativeness_state",
        "diagnostic_states",
        "diagnostic_summary",
    }
    diagnostic_keys = {
        "embedding_coverage",
        "hmm_chart_auditability",
        "relevance_informativeness",
        "topic_review",
        "hmm_feature_set",
    }
    payload = _compact_layer1_semantic_review_dashboard_payload(
        {
            "ticker": "AAPL",
            "run_readiness": {
                **{
                    key: f"value-{key}"
                    for key in readiness_keys
                    if key not in {"diagnostic_states", "diagnostic_summary"}
                },
                "diagnostic_states": {key: "WARN" for key in diagnostic_keys},
                "diagnostic_summary": {"overall_state": "WARN"},
            },
            "gate_cards": [
                {
                    "key": f"gate-{index}",
                    "label": f"Gate {index}",
                    "status": "ready",
                    "reason": "ok",
                }
                for index in range(10)
            ],
            "missing_pipeline_sections": [
                {
                    "key": "topic_labels",
                    "label": "Topics",
                    "reason": "not present",
                    "scope": "packet",
                }
            ],
            "article_groups": [
                {"article_id": f"article-{index}", "headline": "x" * 2_000} for index in range(32)
            ],
            "oversized_detail": [{"value": "y" * 10_000} for _ in range(32)],
        }
    )

    assert set(payload["run_readiness"]) == readiness_keys
    assert set(payload["run_readiness"]["diagnostic_states"]) == diagnostic_keys
    assert len(payload["gate_cards"]) == 10
    assert payload["missing_pipeline_sections"][0]["reason"] == "not present"
    assert payload["payload_budget"]["truncated"] is True


def test_semantic_review_final_compaction_preserves_control_plane_contract() -> None:
    """The final pretty-byte pass may compact evidence, not public control-plane shape."""
    readiness_keys = {
        "readiness_status",
        "status_reason",
        "run_id",
        "ticker",
        "from_date",
        "to_date",
        "topic_review_state",
        "topic_relevance_review_status",
        "relevance_informativeness_state",
        "diagnostic_states",
        "diagnostic_summary",
    }
    diagnostic_keys = {
        "embedding_coverage",
        "hmm_chart_auditability",
        "relevance_informativeness",
        "topic_review",
        "hmm_feature_set",
    }
    summary_cards = [
        {"label": f"Card {index}", "value": f"value-{index}", "field": f"field-{index}"}
        for index in range(10)
    ]
    payload = cast(
        dict[str, Any],
        _enforce_payload_pretty_byte_budget(
            {
                "run_readiness": {
                    **{
                        key: f"value-{key}"
                        for key in readiness_keys
                        if key not in {"diagnostic_states", "diagnostic_summary"}
                    },
                    "diagnostic_states": {key: "WARN" for key in diagnostic_keys},
                    "diagnostic_summary": {"overall_state": "WARN"},
                },
                "summary_cards": summary_cards,
                "gate_cards": [
                    {
                        "key": f"gate-{index}",
                        "label": f"Gate {index}",
                        "status": "ready",
                        "reason": "ok",
                    }
                    for index in range(10)
                ],
                "missing_pipeline_sections": [
                    {
                        "key": "topic_labels",
                        "label": "Topics",
                        "reason": "not present",
                        "scope": "packet",
                    }
                ],
                "article_group_counts": {
                    "full_count": 32,
                    "sample_count": 6,
                    "omitted_count": 26,
                    "truncated": True,
                    "sampling_method": "extremes",
                },
                "oversized_detail": [{"value": "x" * 20_000} for _ in range(32)],
            }
        ),
    )

    assert payload["payload_budget"]["compacted"] is True
    assert payload["payload_budget"]["truncated"] is True
    assert len(json.dumps(payload, indent=2, sort_keys=True).encode("utf-8")) < 200_000
    assert set(payload["run_readiness"]) == readiness_keys
    assert set(payload["run_readiness"]["diagnostic_states"]) == diagnostic_keys
    assert len(payload["summary_cards"]) == 10
    assert {frozenset(card) for card in payload["summary_cards"]} == {
        frozenset({"label", "value", "field"})
    }
    assert len(payload["gate_cards"]) == 10
    assert payload["missing_pipeline_sections"][0]["reason"] == "not present"
    assert set(payload["article_group_counts"]) >= {
        "full_count",
        "sample_count",
        "omitted_count",
        "truncated",
        "sampling_method",
    }


# ---------------------------------------------------------------------------
# B3/S1/S4/S5 — explicit independent evidence-gate fields
# ---------------------------------------------------------------------------

_EXPLICIT_REVIEW_STATE_KEYS = {
    "topic_review_state",
    "topic_review_reason",
    "target_impact_review_status",
    "hmm_chart_point_count",
    "hmm_chart_required_minimum",
}


def _report_dict_for_ticker(
    tmp_path: Path,
    ticker: str,
) -> dict[str, Any]:
    """Return the semantic fixture report dict re-targeted at one ticker."""
    fixture = seed_semantic_review_fixture(local_root=tmp_path / "r2")
    report = build_layer1_aapl_evidence_report(
        run_id=str(fixture["run_id"]),
        from_date="2026-05-21",
        to_date="2026-05-22",
        ticker="AAPL",
        writer=fixture["writer"],
    )
    report_dict = cast(dict[str, Any], report.to_dict())
    report_dict["ticker"] = ticker
    return report_dict


def test_semantic_review_feature_diagnostics_default_to_not_run(tmp_path: Path) -> None:
    """B3: all six feature-diagnostic slots exist and default to explicit NOT_RUN."""
    fixture = seed_semantic_review_fixture(local_root=tmp_path / "r2")
    report = build_layer1_aapl_evidence_report(
        run_id=str(fixture["run_id"]),
        from_date="2026-05-21",
        to_date="2026-05-22",
        ticker="AAPL",
        writer=fixture["writer"],
    )
    payload = cast(dict[str, Any], build_layer1_semantic_review_dashboard_payload(report))

    section = cast(dict[str, Any], payload["feature_diagnostics"])
    readiness_section = cast(dict[str, Any], payload["run_readiness"]["feature_diagnostics"])
    for checks in (section, readiness_section):
        for check in ("heatmap", "null_rate", "recomputation", "formula", "leakage", "outlier"):
            record = cast(dict[str, Any], checks[check])
            assert record["state"] == "NOT_RUN"
            assert "not been executed" in record["reason"]
            assert record["reviewable"] is False
        assert checks["overall_state"] == "NOT_RUN"
        assert checks["reviewable"] is False
    # Slot presence must not silently change the existing human-review gate.
    assert payload["run_readiness"]["ready_for_final_human_acceptance"] is True


def test_semantic_review_feature_diagnostics_adopt_loaded_records(tmp_path: Path) -> None:
    """B3: when a future producer loads real diagnostics, their states are adopted."""
    report_dict = _report_dict_for_ticker(tmp_path, "AAPL")
    report_dict["feature_diagnostics"] = {
        "heatmap": {"state": "PASS", "reason": "Heatmap regenerated cleanly.", "reviewable": True},
        "null_rate": {
            "state": "FAIL",
            "reason": "Null rate exceeds threshold.",
            "reviewable": False,
        },
    }

    payload = cast(dict[str, Any], build_layer1_semantic_review_dashboard_payload(report_dict))
    section = cast(dict[str, Any], payload["feature_diagnostics"])

    assert section["heatmap"]["state"] == "PASS"
    assert section["null_rate"]["state"] == "FAIL"
    assert section["null_rate"]["reviewable"] is False
    assert section["formula"]["state"] == "NOT_RUN"
    assert section["overall_state"] == "FAIL"


def test_semantic_review_diagnostic_states_expose_explicit_review_fields(
    tmp_path: Path,
) -> None:
    """S1/S4/S5: all four ticker responses carry the explicit review-state fields."""
    for ticker in ("AAPL", "AMD", "NVDA", "MSFT"):
        report_dict = _report_dict_for_ticker(tmp_path / ticker, ticker)
        payload = cast(dict[str, Any], build_layer1_semantic_review_dashboard_payload(report_dict))
        states = cast(dict[str, Any], payload["run_readiness"]["diagnostic_states"])

        assert _EXPLICIT_REVIEW_STATE_KEYS.issubset(states), f"{ticker}: {sorted(states)}"
        assert states["topic_review_state"] in {"PASS", "WARN", "FAIL", "NOT_RUN", "NO_DATA"}
        assert isinstance(states["topic_review_reason"], str) and states["topic_review_reason"]
        assert states["target_impact_review_status"] in {"PASS", "WARN", "NO_DATA"}
        assert states["hmm_chart_point_count"] == 2
        assert states["hmm_chart_required_minimum"] == 2
        assert "feature_diagnostics" in payload["run_readiness"]


def test_semantic_review_topic_review_state_is_no_data_for_outlier_only_topics(
    tmp_path: Path,
) -> None:
    """S1: all-outlier topic rows produce explicit NO_DATA with the exact reason."""
    report_dict = _report_dict_for_ticker(tmp_path, "AAPL")
    topic_review = cast(dict[str, Any], report_dict["topic_review"])
    rows = [dict(row) for row in cast(list[dict[str, Any]], topic_review["rows"])]
    assert rows
    for row in rows:
        row["topic_id"] = -1
    topic_review["rows"] = rows
    topic_review["topics"] = []
    topic_review["topic_count"] = 0

    payload = cast(dict[str, Any], build_layer1_semantic_review_dashboard_payload(report_dict))
    states = cast(dict[str, Any], payload["run_readiness"]["diagnostic_states"])

    assert states["topic_review_state"] == "NO_DATA"
    assert states["topic_review_reason"] == (
        "No non-outlier topic clusters were found. All rows are outlier topic -1."
    )
    nested = cast(dict[str, Any], payload["run_readiness"]["topic_review_state"])
    assert nested["state"] == "NO_DATA"
    assert "outlier topic -1" in str(nested["reason"])
    assert payload["run_readiness"]["ready_for_final_human_acceptance"] is False


def test_semantic_review_target_impact_review_status_warns_on_mixed_directions(
    tmp_path: Path,
) -> None:
    """S4: partial concrete directions yield WARN; full concrete coverage yields PASS."""
    report_dict = _report_dict_for_ticker(tmp_path, "AAPL")
    semantic_rows = cast(list[dict[str, Any]], report_dict["semantic_aggregate_rows"])
    cast(dict[str, Any], semantic_rows[0]["features"])["nlp_target_impact_direction"] = "positive"

    payload = cast(dict[str, Any], build_layer1_semantic_review_dashboard_payload(report_dict))
    states = cast(dict[str, Any], payload["run_readiness"]["diagnostic_states"])
    assert states["target_impact_review_status"] == "WARN"
    reason = cast(str, payload["run_readiness"]["target_impact_review_reason"])
    assert "part of the aggregate rows" in reason

    report_dict = _report_dict_for_ticker(tmp_path / "pass", "AAPL")
    semantic_rows = cast(list[dict[str, Any]], report_dict["semantic_aggregate_rows"])
    for row in semantic_rows:
        features = cast(dict[str, Any], row["features"])
        features["nlp_target_impact_direction"] = "positive"
        features["nlp_target_impact_magnitude"] = "medium"

    payload = cast(dict[str, Any], build_layer1_semantic_review_dashboard_payload(report_dict))
    states = cast(dict[str, Any], payload["run_readiness"]["diagnostic_states"])
    assert states["target_impact_review_status"] == "PASS"


def test_semantic_review_hmm_chart_point_count_survives_one_point_compaction(
    tmp_path: Path,
) -> None:
    """S5: a one-point chart keeps WARN state plus numeric point/minimum facts."""
    report_dict = _report_dict_for_ticker(tmp_path, "AAPL")
    report_dict["benchmark_price_rows"] = cast(
        list[dict[str, Any]], report_dict["benchmark_price_rows"]
    )[:1]
    report_dict["benchmark_market_regime_rows"] = cast(
        list[dict[str, Any]], report_dict["benchmark_market_regime_rows"]
    )[:1]

    payload = cast(dict[str, Any], build_layer1_semantic_review_dashboard_payload(report_dict))
    states = cast(dict[str, Any], payload["run_readiness"]["diagnostic_states"])

    assert states["hmm_chart_auditability"] == "WARN"
    assert states["hmm_chart_point_count"] == 1
    assert states["hmm_chart_required_minimum"] == 2
    hmm_state = cast(dict[str, Any], payload["run_readiness"]["hmm_chart_auditability_state"])
    assert hmm_state["point_count"] == 1
    assert hmm_state["required_minimum"] == 2


def test_semantic_review_compaction_preserves_explicit_review_fields(tmp_path: Path) -> None:
    """B1-style compaction must keep the new review fields on AAPL, NVDA, MSFT."""
    for ticker in ("AAPL", "NVDA", "MSFT"):
        report_dict = _report_dict_for_ticker(tmp_path / ticker, ticker)
        payload = cast(dict[str, Any], build_layer1_semantic_review_dashboard_payload(report_dict))
        # Double compaction with oversized synthetic evidence branches attached.
        payload["oversized_detail"] = [{"value": "x" * 20_000} for _ in range(32)]
        compacted = cast(dict[str, Any], _compact_layer1_semantic_review_dashboard_payload(payload))
        assert compacted["payload_budget"]["compacted"] is True
        assert compacted["payload_budget"]["truncated"] is True

        states = cast(dict[str, Any], compacted["run_readiness"]["diagnostic_states"])
        assert _EXPLICIT_REVIEW_STATE_KEYS.issubset(states), f"{ticker}: {sorted(states)}"
        assert states["hmm_chart_point_count"] == 2
        assert states["hmm_chart_required_minimum"] == 2
        assert isinstance(states["topic_review_reason"], str) and states["topic_review_reason"]
        fd = cast(dict[str, Any], compacted["feature_diagnostics"])
        for check in ("heatmap", "null_rate", "recomputation", "formula", "leakage", "outlier"):
            assert cast(dict[str, Any], fd[check])["state"] == "NOT_RUN"
        assert compacted["payload_budget"]["within_budget"] is True


# -- Missing artifact fallback tests ------------------------------------------------


def test_semantic_qa_payload_all_tickers_missing_returns_empty_status() -> None:
    """When no review reports exist for any pilot ticker, the payload should
    return status 'empty' with missing_review_artifacts warnings for all four
    pilot tickers."""
    payload = build_semantic_qa_payload(
        reports={},
        run_id="test-run-id",
        from_date="2026-01-01",
        to_date="2026-01-01",
        tickers=list(PILOT_TICKERS),
    )

    assert payload["ok"] is True
    assert payload["status"] == "empty"
    assert payload["schema_id"] == SEMANTIC_QA_SCHEMA_ID
    assert payload["run"]["tickers"] == list(PILOT_TICKERS)

    # All four pilot tickers must be present as empty placeholders
    for ticker in PILOT_TICKERS:
        ticker_data = payload["tickers"][ticker]
        assert ticker_data["summary"]["signal_rows"] is None
        assert ticker_data["queues"]["potential_false_positives"]["sample_count"] == 0

    # Each ticker must have a missing_review_artifacts warning
    warning_codes = [w["code"] for w in payload["warnings"]]
    assert warning_codes.count("missing_review_artifacts") == len(PILOT_TICKERS)

    # Artifact IDs must be empty lists for all missing tickers
    for ticker in PILOT_TICKERS:
        assert payload["run"]["artifact_ids"][ticker] == []


def test_semantic_qa_payload_partial_missing_returns_warning_status(tmp_path: Path) -> None:
    """When only two of four pilot tickers have reports, the payload should
    return status 'warning' with the two missing tickers rendered as empty
    placeholders and missing_review_artifacts warnings."""
    run_id = "test-run-id"
    from_date = "2026-05-20"
    to_date = "2026-05-22"
    # Seed AAPL and AMD with full reports (fixture dates are 2026-05-21)
    for ticker in ("AAPL", "AMD"):
        seed_semantic_review_fixture(local_root=tmp_path / ticker)

    writer = R2Writer(local_root=tmp_path)

    # Build reports only for AAPL and AMD
    reports = {}
    for ticker in ("AAPL", "AMD"):
        try:
            reports[ticker] = build_layer1_aapl_evidence_report(
                run_id=run_id,
                from_date=from_date,
                to_date=to_date,
                ticker=ticker,
                writer=writer,
            )
        except FileNotFoundError:
            pass

    # NVDA and MSFT are missing (reports dict has no entry for them)
    payload = build_semantic_qa_payload(
        reports=reports,
        run_id="test-run-id",
        from_date="2026-01-01",
        to_date="2026-01-01",
        tickers=list(PILOT_TICKERS),
    )

    assert payload["ok"] is True
    assert payload["status"] == "warning"

    # Present tickers (AAPL, AMD) must have real data
    for ticker in ("AAPL", "AMD"):
        ticker_data = payload["tickers"][ticker]
        # summary.signal_rows is either an int or a summary-count dict — never None for real reports
        # The key distinction: empty tickers have ALL summary fields as None
        assert ticker_data["summary"]["signal_rows"] is not None or \
               ticker_data["summary"]["rejected_rows"] is not None

    # Missing tickers (NVDA, MSFT) must be empty placeholders
    for ticker in ("NVDA", "MSFT"):
        ticker_data = payload["tickers"][ticker]
        assert ticker_data["summary"]["signal_rows"] is None
        assert ticker_data["summary"]["rejected_rows"] is None

    # Exactly two missing_review_artifacts warnings (one per missing ticker)
    missing_warnings = [
        w for w in payload["warnings"] if w["code"] == "missing_review_artifacts"
    ]
    assert len(missing_warnings) == 2
    missing_tickers = {w["ticker"] for w in missing_warnings}
    assert missing_tickers == {"NVDA", "MSFT"}


def test_semantic_qa_payload_non_pilot_tickers_excluded_from_response() -> None:
    """Requesting only non-pilot tickers should produce an empty payload with
    no tickers and no warnings — the request normalizes to zero pilot tickers."""
    payload = build_semantic_qa_payload(
        reports={},
        run_id="test-run-id",
        from_date="2026-01-01",
        to_date="2026-01-01",
        tickers=["TSLA", "GOOG", "MSFT"],
    )

    assert payload["ok"] is True
    # MSFT is a pilot ticker so it should be in the normalized list
    assert payload["run"]["tickers"] == ["MSFT"]
    # MSFT should be an empty placeholder since reports is empty
    assert payload["tickers"]["MSFT"]["summary"]["signal_rows"] is None
    missing_warnings = [
        w for w in payload["warnings"] if w["code"] == "missing_review_artifacts"
    ]
    assert len(missing_warnings) == 1
    assert missing_warnings[0]["ticker"] == "MSFT"


def test_semantic_qa_payload_non_pilot_only_no_pilot_match(tmp_path: Path) -> None:
    """When requested tickers share no overlap with PILOT_TICKERS, the
    normalized ticker list is empty and no warnings are emitted."""
    payload = build_semantic_qa_payload(
        reports={},
        run_id="test-run-id",
        from_date="2026-01-01",
        to_date="2026-01-01",
        tickers=["TSLA", "GOOG"],
    )

    assert payload["ok"] is True
    assert payload["run"]["tickers"] == []
    assert len(payload["tickers"]) == 0
    assert len(payload["warnings"]) == 0
