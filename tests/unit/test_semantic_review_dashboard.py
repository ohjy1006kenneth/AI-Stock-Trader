from __future__ import annotations

import json
import threading
from pathlib import Path
from typing import Any
from urllib.request import urlopen

from app.lab.semantic_review_dashboard import create_semantic_review_server
from core.features.semantic_review_dashboard import (
    SemanticReviewDashboardConfig,
    SemanticReviewFilters,
    build_semantic_review_payload,
)

FIXTURE_DIR = Path("tests/fixtures/semantic_review")
CURRENT_PILOT_DIR = Path("artifacts/reports/diagnostics")


def test_build_semantic_review_payload_loads_local_artifacts() -> None:
    """The dashboard payload exposes status, gates, rows, and source artifact keys."""
    payload = build_semantic_review_payload(_fixture_config())

    assert payload["load_status"] == "ok"
    assert payload["run"]["machine_integrity_status"] == "pass"
    assert payload["run"]["human_semantic_review_status"] == "pending"
    assert len(payload["gates"]) == 2
    assert len(payload["rows"]) == 2
    assert payload["rows"][0]["finbert_polarity"] == "positive"
    assert payload["rows"][0]["duplicate_count"] == 2
    assert payload["groups"][0]["count"] == 2
    assert (
        payload["rows"][0]["source_artifact_keys"]["finbert_scored_news_key"]
        == "features/2024-01-03/news_sentiment_scored/"
        "layer1-semantic-fixture-2024-01-03.parquet"
    )
    assert payload["accuracy_report"]["accepted"] is True


def test_build_semantic_review_payload_loads_current_aapl_pilot_bundle(
    monkeypatch: Any,
) -> None:
    """The checked-in current AAPL pilot bundle should load non-empty review rows."""

    def _fail_if_called(*args: object, **kwargs: object) -> None:
        raise AssertionError("R2Writer should not be constructed for local artifacts")

    monkeypatch.setattr("core.features.semantic_review_dashboard.R2Writer", _fail_if_called)

    payload: dict[str, Any] = build_semantic_review_payload(
        SemanticReviewDashboardConfig(
            run_id="layer1-aapl-accuracy-2026-05-06-to-2026-05-28-v4-after-pr221",
            from_date="2026-05-06",
            to_date="2026-05-28",
            ticker="AAPL",
            artifact_dir=CURRENT_PILOT_DIR,
            review_csv_path=CURRENT_PILOT_DIR / "missing-review-csv.csv",
        )
    )

    assert payload["load_status"] == "ok"
    assert payload["run"]["review_row_count"] == 1571
    assert payload["run"]["machine_integrity_status"] == "pass"
    assert payload["run"]["human_semantic_review_status"] == "pending"
    assert payload["run"]["recommendation_for_issue_202"] == "needs_human_review"
    assert len(payload["rows"]) == 1571
    assert payload["source_files"]["evidence_json"] == (
        "artifacts/reports/diagnostics/"
        "aapl_pilot_evidence_layer1-aapl-accuracy-2026-05-06-to-2026-05-28-v4-after-pr221.json"
    )
    assert payload["source_files"]["accuracy_report"] == (
        "artifacts/reports/diagnostics/"
        "layer1_aapl_feature_accuracy_layer1-aapl-accuracy-2026-05-06-to-2026-05-28-v4-after-pr221_"
        "2026-05-06_to_2026-05-28.json"
    )
    assert "review_csv" not in payload["source_files"]


def test_build_semantic_review_payload_applies_filters() -> None:
    """Date, ticker, search, and relevance filters narrow review rows."""
    payload: dict[str, Any] = build_semantic_review_payload(
        _fixture_config(),
        filters=SemanticReviewFilters(
            date="2024-01-03",
            ticker="AAPL",
            search="supplier",
            min_relevance=0.9,
        ),
    )

    assert len(payload["rows"]) == 1
    assert payload["rows"][0]["raw_article_id"] == "article-1"
    assert payload["rows"][0]["finbert_relevance"] == 0.92


def test_build_semantic_review_payload_reports_missing_artifacts(tmp_path: Path) -> None:
    """Missing local artifacts produce a fail-closed dashboard payload."""
    payload = build_semantic_review_payload(
        SemanticReviewDashboardConfig(
            run_id="missing-run",
            artifact_dir=tmp_path,
            use_r2=False,
        )
    )

    assert payload["load_status"] == "missing_artifacts"
    assert payload["rows"] == []
    assert payload["gates"] == []
    assert any(
        "aapl_pilot_evidence_missing-run.json" in item
        for item in payload["missing_artifacts"]
    )


def test_semantic_review_http_server_serves_health_and_api() -> None:
    """The live dashboard exposes health and JSON review endpoints."""
    server = create_semantic_review_server("127.0.0.1", 0, _fixture_config())
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        base_url = f"http://127.0.0.1:{server.server_port}"
        health = _fetch_json(f"{base_url}/health")
        payload = _fetch_json(f"{base_url}/api/review?min_relevance=0.9")
        html = urlopen(f"{base_url}/", timeout=5).read().decode("utf-8")
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)

    assert health["status"] == "ok"
    assert health["run_id"] == "layer1-semantic-fixture"
    assert len(payload["rows"]) == 1
    assert payload["rows"][0]["raw_article_id"] == "article-1"
    assert "Layer 1 Semantic Review" in html


def _fixture_config() -> SemanticReviewDashboardConfig:
    """Return the local fixture dashboard configuration."""
    return SemanticReviewDashboardConfig(
        run_id="layer1-semantic-fixture",
        from_date="2024-01-03",
        to_date="2024-01-03",
        ticker="AAPL",
        artifact_dir=FIXTURE_DIR,
        use_r2=False,
    )


def _fetch_json(url: str) -> dict[str, object]:
    """Fetch and parse one JSON endpoint."""
    with urlopen(url, timeout=5) as response:
        return json.loads(response.read().decode("utf-8"))
