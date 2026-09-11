from __future__ import annotations

from http.client import HTTPConnection
from threading import Thread

import pytest

from app.lab import semantic_review_dashboard as dashboard
from core.features.semantic_qa import (
    SEMANTIC_QA_SCHEMA_ID,
    build_semantic_qa_payload,
    normalize_semantic_qa_query,
)


def _report() -> dict[str, object]:
    return {
        "run_id": "exact-run/01",
        "from_date": "2026-05-21",
        "to_date": "2026-05-22",
        "generated_at": "2026-05-22T20:00:00+00:00",
        "artifact_keys": {"news_sentiment_scored": ["exact/artifact.parquet"]},
        "summary": {"preprocessing_row_count": 3, "sentence_count": 3},
        "preprocessing_rows": [
            {
                "date": "2026-05-21",
                "ticker": "AAPL",
                "article_id": "a1",
                "sentence_index": 0,
                "chunk_index": 0,
                "ticker_mentions": ["AAPL"],
                "has_requested_ticker_evidence": True,
            },
            {
                "date": "2026-05-21",
                "ticker": "AAPL",
                "article_id": "a2",
                "sentence_index": 0,
                "chunk_index": 0,
                "ticker_mentions": [],
                "has_requested_ticker_evidence": False,
            },
            {
                "date": "2026-05-21",
                "ticker": "AAPL",
                "article_id": "a3",
                "sentence_index": 0,
                "chunk_index": 0,
                "ticker_mentions": ["AAPL"],
                "has_requested_ticker_evidence": True,
            },
            {
                "date": "2026-05-21",
                "ticker": "AAPL",
                "article_id": "a4",
                "sentence_index": 0,
                "chunk_index": 0,
                "ticker_mentions": [],
                "has_requested_ticker_evidence": False,
            },
        ],
        "article_groups": [
            {
                "date": "2026-05-21",
                "ticker": "AAPL",
                "article_id": "a1",
                "headline": "Apple product event",
                "source": "source-a",
                "sentence_rows": [
                    {
                        "sentence_index": 0,
                        "chunk_index": 0,
                        "included_in_signal": True,
                        "effective_contribution": 0.5,
                        "sentiment_score": 0.8,
                    }
                ],
            },
            {
                "date": "2026-05-21",
                "ticker": "AAPL",
                "article_id": "a2",
                "headline": "Ferrari comparison",
                "source": "source-b",
                "sentence_rows": [
                    {
                        "sentence_index": 0,
                        "chunk_index": 0,
                        "rejected": True,
                        "relevance_decision": "rejected",
                        "effective_contribution": 0.0,
                    }
                ],
            },
            {
                "date": "2026-05-21",
                "ticker": "AAPL",
                "article_id": "a3",
                "headline": "Apple legal filing",
                "source": "source-c",
                "sentence_rows": [
                    {
                        "sentence_index": 0,
                        "chunk_index": 0,
                        "rejected": True,
                        "relevance_decision": "rejected",
                        "effective_contribution": 0.0,
                        "text": "Apple legal regulatory filing",
                    }
                ],
            },
            {
                "date": "2026-05-21",
                "ticker": "AAPL",
                "article_id": "a4",
                "headline": "Market context",
                "source": "source-d",
                "sentence_rows": [
                    {
                        "sentence_index": 0,
                        "chunk_index": 0,
                        "included_in_signal": True,
                        "effective_contribution": 0.1,
                    }
                ],
            },
        ],
    }


def test_normalize_query_preserves_opaque_run_and_pilot_order() -> None:
    query = normalize_semantic_qa_query(
        run_id="opaque/run...exact",
        from_date="2026-05-21",
        to_date="2026-05-22",
        tickers="msft,aapl,MSFT",
        sample_limit="7",
    )
    assert query["run_id"] == "opaque/run...exact"
    assert query["tickers"] == ("AAPL", "MSFT")
    assert query["sample_limit"] == 7


def test_payload_queues_distinguish_leakage_context_rejection_and_false_negative() -> None:
    payload = build_semantic_qa_payload(
        reports={"AAPL": _report()},
        run_id="exact-run/01",
        from_date="2026-05-21",
        to_date="2026-05-22",
        tickers=("AAPL",),
        sample_limit=10,
        generated_at="2026-05-22T20:00:00+00:00",
    )
    ticker = payload["tickers"]["AAPL"]
    assert payload["schema_id"] == SEMANTIC_QA_SCHEMA_ID
    assert [row["article_id"] for row in ticker["queues"]["potential_false_positives"]["rows"]] == [
        "a4"
    ]
    assert ticker["queues"]["potential_false_negatives"]["rows"][0]["article_id"] == "a3"
    assert ticker["queues"]["top_contributors"]["rows"][0]["article_id"] == "a1"


def test_payload_preserves_canonical_counts_and_exact_identity() -> None:
    report = _report()
    report["preprocessing_row_count"] = 650
    report["summary"] = {"preprocessing_row_count": 650}
    payload = build_semantic_qa_payload(
        reports={"AAPL": report},
        run_id="exact-run/01",
        from_date="2026-05-21",
        to_date="2026-05-22",
        tickers=("AAPL",),
        sample_limit=2,
        generated_at="2026-05-22T20:00:00+00:00",
    )
    funnel = payload["tickers"]["AAPL"]["funnel"]["total_preprocessed_chunks"]
    assert funnel["canonical_count"] == 650
    assert funnel["sample_count"] == 4
    assert funnel["omitted_count"] == 646
    assert payload["run"]["run_id"] == "exact-run/01"
    assert payload["run"]["artifact_ids"]["AAPL"] == ["exact/artifact.parquet"]


def test_empty_and_missing_tickers_are_explicit() -> None:
    payload = build_semantic_qa_payload(
        reports={},
        run_id="run",
        from_date="2026-05-21",
        to_date="2026-05-22",
        tickers=("AAPL", "AMD"),
        generated_at="2026-05-22T20:00:00+00:00",
    )
    assert payload["status"] == "empty"
    assert payload["tickers"]["AAPL"]["summary"]["signal_rows"] is None
    assert any(item["code"] == "missing_review_artifacts" for item in payload["warnings"])


def test_source_final_contribution_is_totalled_and_ranked_deterministically() -> None:
    report = _report()
    report["article_groups"][0]["sentence_rows"][0].pop("effective_contribution")
    report["article_groups"][0]["sentence_rows"][0]["final_contribution"] = 0.75
    report["article_groups"][0]["sentence_rows"][0]["final_signal_contribution"] = 0.75
    report["article_groups"][3]["sentence_rows"][0].pop("effective_contribution")
    report["article_groups"][3]["sentence_rows"][0]["final_signal_contribution"] = 0.25
    payload = build_semantic_qa_payload(
        reports={"AAPL": report}, run_id="exact-run/01", from_date="2026-05-21", to_date="2026-05-22"
    )
    ticker = payload["tickers"]["AAPL"]
    assert ticker["summary"]["total_contribution"] == 1.0
    assert ticker["queues"]["top_contributors"]["rows"][0]["final_contribution"] == 0.75


def test_leakage_matrix_uses_full_owner_rows_before_sampling() -> None:
    report = _report()
    rows = []
    for index in range(40):
        rows.append(
            {
                "date": "2026-05-21",
                "ticker": "AMD",
                "article_id": f"cross-{index}",
                "sentence_index": 0,
                "chunk_index": 0,
                "included_in_signal": True,
                "final_contribution": 0.1,
                "evidence_owner": "NVDA",
                "has_requested_ticker_evidence": False,
            }
        )
    report["article_groups"] = [{"ticker": "AMD", "sentence_rows": rows}]
    payload = build_semantic_qa_payload(
        reports={"AMD": report}, run_id="run", from_date="2026-05-21", to_date="2026-05-22", tickers=("AMD",), sample_limit=10
    )
    cell = payload["leakage_matrix"]["NVDA"]["AMD"]
    assert cell["canonical_count"] == 40
    assert len(cell["row_ids"]) == 40
    assert payload["tickers"]["AMD"]["queues"]["cross_ticker_anomalies"]["sample_count"] == 10


def test_integrity_reports_stage_reconciliation_and_identity_mismatch() -> None:
    report = _report()
    report["preprocessing_row_count"] = 650
    report["run_id"] = "wrong-run"
    payload = build_semantic_qa_payload(
        reports={"AAPL": report}, run_id="exact-run/01", from_date="2026-05-21", to_date="2026-05-22", sample_limit=30
    )
    stage = payload["integrity"]["stages"]["AAPL:total_preprocessed_chunks"]
    assert stage == {"canonical_count": 650, "sample_count": 4, "omitted_count": 646, "status": "measured"}
    assert "AAPL:run_id_mismatch" in payload["integrity"]["issue_codes"]


def test_established_empty_report_warns_without_fabricating_counts() -> None:
    report = _report()
    report["row_count"] = 0
    report["article_groups"] = []
    report["preprocessing_rows"] = []
    report["summary"] = {"preprocessing_row_count": 0, "sentence_count": 0}
    payload = build_semantic_qa_payload(
        reports={"AAPL": report}, run_id="exact-run/01", from_date="2026-05-21", to_date="2026-05-22"
    )
    assert payload["status"] == "empty"
    assert any(item["code"] == "no_review_rows" for item in payload["warnings"])
    assert payload["tickers"]["AAPL"]["summary"]["signal_rows"] == 0


def test_endpoint_validation_headers_and_missing_artifact_envelope(monkeypatch: pytest.MonkeyPatch) -> None:
    defaults = dashboard._DashboardDefaults(
        run_id="run", from_date="2026-05-21", to_date="2026-05-22", ticker="AAPL", host="127.0.0.1", port=0
    )
    server = dashboard._DashboardHTTPServer((defaults.host, defaults.port), defaults)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    connection = HTTPConnection(str(server.server_address[0]), int(server.server_address[1]), timeout=5)
    try:
        connection.request("GET", "/api/semantic-qa?run_id=run&from_date=bad&to_date=2026-05-22")
        response = connection.getresponse()
        assert response.status == 400
        assert response.getheader("Cache-Control") == "no-store"
        assert '"code": "invalid_request"' in response.read().decode()

        def missing_report(**_: object) -> None:
            raise FileNotFoundError("missing")

        monkeypatch.setattr(dashboard, "build_layer1_aapl_evidence_report", missing_report)
        connection.request(
            "GET", "/api/semantic-qa?run_id=run&from_date=2026-05-21&to_date=2026-05-22&sample_limit=51"
        )
        response = connection.getresponse()
        assert response.status == 400
        assert '"code": "invalid_request"' in response.read().decode()

        connection.request("GET", "/api/semantic-qa?run_id=run&from_date=2026-05-21&to_date=2026-05-22")
        response = connection.getresponse()
        assert response.status == 404
        assert '"code": "review_artifacts_not_found"' in response.read().decode()
    finally:
        connection.close()
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
