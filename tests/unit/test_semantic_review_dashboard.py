"""Unit tests for the Layer 1 semantic-review dashboard."""
from __future__ import annotations

import copy
from pathlib import Path
from typing import Any, cast

from app.lab.semantic_review_dashboard import _DashboardDefaults, _render_dashboard_html
from core.features.aapl_evidence import build_layer1_aapl_evidence_report
from core.features.semantic_review_dashboard import (
    build_layer1_semantic_review_dashboard_payload,
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
    assert [row["sentence_index"] for row in cast(list[dict[str, Any]], aapl_one["sentence_rows"])] == [0, 1, 2]
    assert aapl_one["sentence_rows"][0]["text"] != aapl_one["sentence_rows"][1]["text"]
    assert aapl_one["sentence_rows"][0]["row_granularity"] == "sentence-level"
    assert aapl_one["preprocessing_rows"][0]["ticker_mentions"] == ["AAPL"]
    assert aapl_one["topic_evidence"][0]["topic_label"] == "earnings and demand"
    assert aapl_one["relevance_gate_rows"][0]["relevance_decision"] == "accepted"

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
    assert semantic_rows[0]["artifact_key"].endswith("sentiment_features/layer1-semantic-review-fixture.parquet")
    assert semantic_rows[0]["features"]["nlp_article_count"] == 2.0
    assert semantic_rows[0]["features"]["nlp_contributing_article_ids"] == ["aapl-001", "ferrari-001"]

    semantic_group = cast(list[dict[str, Any]], date_groups["2026-05-21"]["semantic_aggregates"])
    assert semantic_group[0]["row_granularity"] == "ticker-date"
    assert semantic_group[0]["contributing_article_ids"] == ["aapl-001", "ferrari-001"]

    context = cast(dict[str, Any], report_dict["hmm_evaluation_context"])
    assert context["requested_inference_dates"] == ["2026-05-21", "2026-05-22"]
    assert context["observed_inference_dates"] == ["2026-05-21", "2026-05-22"]
    assert context["warnings"] == []
    assert context["training_windows"][0]["train_end_date"] == "2026-05-20"


def test_semantic_review_report_loads_dated_stage_artifacts_for_parent_run_id(tmp_path: Path) -> None:
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

    flagged_ids = {str(item["article_id"]) for item in cast(list[dict[str, Any]], payload["flagged_articles"])}
    accepted_ids = {str(item["article_id"]) for item in cast(list[dict[str, Any]], payload["accepted_articles"])}
    sections = cast(dict[str, Any], payload["pipeline_sections"])

    assert flagged_ids == {"aapl-001", "aapl-002", "ferrari-001"}
    assert accepted_ids == {"aapl-003"}
    assert payload["human_semantic_review_status"] == "needs_human_review"
    assert payload["benchmark_ticker"] == "SPY"
    assert len(cast(list[dict[str, Any]], payload["benchmark_price_series"])) == 2
    assert len(cast(list[dict[str, Any]], payload["benchmark_market_regime_series"])) == 2
    assert len(cast(list[dict[str, Any]], sections["raw_preprocessing_rows"])) == 8
    assert len(cast(list[dict[str, Any]], sections["topic_label_rows"])) == 4
    assert len(cast(list[dict[str, Any]], sections["relevance_gate_rows"])) == 8
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
    assert ferrari["sentence_rows"][0]["sentence_index"] == 0
    assert ferrari["sentence_rows"][0]["text"].startswith("Ferrari shares fell sharply")

    duplicate = next(item for item in article_groups if item["article_id"] == "aapl-001")
    assert "duplicate_normalized_headline" in duplicate["contamination_flags"]
    assert duplicate["sentence_rows"][0]["text"].startswith("Apple shares climbed")
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
    assert missing_article["full_scored_text_available"] is False
    assert missing_article["full_scored_text_warning"]
    assert any(row["missing_text_warning"] for row in missing_article["sentence_rows"])


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
    assert layer1_topic_label_path("2026-05-21", run_id) in gates["topic_labels"][
        "missing_or_tried_keys"
    ]


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
    assert any(item["scope"] == "sentiment_features" for item in cast(list[dict[str, Any]], payload["warnings"]))
    assert readiness["ready_for_final_human_acceptance"] is False
    assert readiness["recommendation"] == "not ready for final human acceptance"
    assert readiness["human_review_status"] == "blocked_by_missing_pipeline_evidence"
    assert gates["sentiment_features"]["status"] == "blocked"
    assert "Ticker-Date Semantic Aggregates" in missing_labels
    assert layer1_sentiment_feature_path("2026-05-22", run_id) in gates["sentiment_features"]["missing_or_tried_keys"]

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

    assert any(item["scope"] == "cached_bundle" for item in cast(list[dict[str, Any]], payload["warnings"]))
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
    assert any(item["scope"] == "price_series" for item in cast(list[dict[str, Any]], payload["warnings"]))
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
    payload = copy.deepcopy(cast(dict[str, Any], build_layer1_semantic_review_dashboard_payload(report)))
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
    assert layer1_topic_label_path("2026-05-21", f"{run_id}-2026-05-21") in topic_failure[
        "missing_or_tried_keys"
    ]


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
    assert "<table" not in html
    assert "date_aligned_price_hmm_rows" in html
    assert "/api/review" in html
