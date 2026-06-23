"""Unit tests for the Layer 1 semantic-review dashboard."""
from __future__ import annotations

from pathlib import Path
from typing import Any, cast

from app.lab.semantic_review_dashboard import _DashboardDefaults, _render_dashboard_html
from core.features.aapl_evidence import build_layer1_aapl_evidence_report
from core.features.semantic_review_dashboard import build_layer1_semantic_review_dashboard_payload
from tests.fixtures.semantic_review_support import seed_semantic_review_fixture


def test_semantic_review_report_groups_sentence_rows_and_date_regime(tmp_path: Path) -> None:
    """The report should collapse sentence rows beneath each raw article and each date regime."""
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

    article_groups = {
        str(item["article_id"]): cast(dict[str, Any], item)
        for item in cast(list[dict[str, Any]], report_dict["article_groups"])
    }
    aapl_one = article_groups["aapl-001"]
    assert [row["sentence_index"] for row in cast(list[dict[str, Any]], aapl_one["sentence_rows"])] == [0, 1, 2]
    assert aapl_one["sentence_rows"][0]["text"] != aapl_one["sentence_rows"][1]["text"]
    assert aapl_one["sentence_rows"][0]["row_granularity"] == "sentence-level"

    date_groups = {
        str(item["date"]): cast(dict[str, Any], item)
        for item in cast(list[dict[str, Any]], report_dict["date_groups"])
    }
    regime = cast(dict[str, Any], date_groups["2026-05-21"]["regime"])
    assert regime["scope"] == "date-level"
    assert regime["applies_to"] == "all sentence rows on the trading date"
    assert regime["regime"] == "sideways"
    assert date_groups["2026-05-21"]["sentence_count"] == 5


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

    assert flagged_ids == {"aapl-001", "aapl-002", "ferrari-001"}
    assert accepted_ids == {"aapl-003"}

    article_groups = cast(list[dict[str, Any]], payload["article_groups"])
    ferrari = next(item for item in article_groups if item["article_id"] == "ferrari-001")
    assert "no_requested_ticker_evidence" in ferrari["contamination_flags"]
    assert ferrari["requested_ticker_term_hits"] == []
    assert ferrari["sentence_rows"][0]["sentence_index"] == 0
    assert ferrari["sentence_rows"][0]["text"].startswith("Ferrari shares fell sharply")

    duplicate = next(item for item in article_groups if item["article_id"] == "aapl-001")
    assert "duplicate_normalized_headline" in duplicate["contamination_flags"]
    assert duplicate["sentence_rows"][0]["text"].startswith("Apple shares climbed")


def test_semantic_review_dashboard_html_labels_date_level_regime_and_sentence_rows() -> None:
    """The dashboard shell should explain the different granularities to human reviewers."""
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
    assert "Sentence-level FinBERT rows are grouped beneath raw articles" in html
    assert "Date-level HMM regime" in html
    assert "/api/review" in html
