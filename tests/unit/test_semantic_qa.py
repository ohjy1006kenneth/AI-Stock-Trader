from __future__ import annotations

import json
from http.client import HTTPConnection
from threading import Thread
from typing import Any

import pytest

from app.lab import semantic_review_dashboard as dashboard
from core.features.semantic_qa import (
    SEMANTIC_QA_SCHEMA_ID,
    build_semantic_qa_payload,
    normalize_semantic_qa_query,
)

_FIXED_NOW = "2026-06-18T20:00:00+00:00"
_WINDOW = {"from_date": "2026-05-21", "to_date": "2026-05-22"}


# --------------------------------------------------------------------------- #
# Source-shaped report builders (canonical producer fields, no invented keys)  #
# --------------------------------------------------------------------------- #
def _row(
    *,
    si: int = 0,
    ci: int = 0,
    included: bool | None = None,
    decision: str | None = None,
    contribution: float | None = None,
    text: str | None = None,
    reason_codes: list[str] | None = None,
    rel: bool | None = None,
) -> dict[str, Any]:
    row: dict[str, object] = {"sentence_index": si, "chunk_index": ci}
    if included is not None:
        row["included_in_signal"] = included
    if decision is not None:
        row["relevance_decision"] = decision
    if contribution is not None:
        row["final_contribution"] = contribution
        row["final_signal_contribution"] = contribution
    if text is not None:
        row["text"] = text
    if reason_codes is not None:
        row["reason_codes"] = reason_codes
    if rel is not None:
        row["explicit_local_material_relationship"] = rel
    return row


def _preproc(ticker: str, article_id: str, mentions: list[str], *, si: int = 0, ci: int = 0) -> dict[str, Any]:
    return {
        "date": "2026-05-21",
        "ticker": ticker,
        "article_id": article_id,
        "sentence_index": si,
        "chunk_index": ci,
        "ticker_mentions": list(mentions),
    }


def _group(ticker: str, article_id: str, headline: str, rows: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "date": "2026-05-21",
        "ticker": ticker,
        "article_id": article_id,
        "headline": headline,
        "source": "src",
        "sentence_rows": rows,
    }


def _report(
    ticker: str = "AAPL",
    groups: list[dict[str, Any]] | None = None,
    preproc: list[dict[str, Any]] | None = None,
    *,
    summary: dict[str, int] | None = None,
    run_id: str = "exact-run/01",
) -> dict[str, Any]:
    return {
        "run_id": run_id,
        "ticker": ticker,
        "from_date": _WINDOW["from_date"],
        "to_date": _WINDOW["to_date"],
        "generated_at": _FIXED_NOW,
        "artifact_keys": {"news_sentiment_scored": [f"exact/{ticker}.parquet"]},
        "summary": summary or {},
        "preprocessing_rows": list(preproc or []),
        "article_groups": list(groups or []),
    }


def _mixed_report(ticker: str = "AAPL") -> dict[str, Any]:
    """AAPL product/legal report exercising leakage, context rejection, and false negative."""
    groups = [
        _group(ticker, "a1", "Apple product event", [_row(included=True, decision="accepted", contribution=0.75)]),
        _group(ticker, "a2", "Apple legal filing", [_row(included=False, decision="rejected", text="Apple legal regulatory filing")]),
        _group(ticker, "a3", "Apple services", [_row(included=True, decision="borderline", contribution=0.25)]),
        _group(ticker, "a4", "Market context", [_row(included=True, decision="accepted", contribution=0.1)]),
        _group(ticker, "a5", "Ferrari comparison", [_row(included=False, decision="rejected")]),
    ]
    preproc = [
        _preproc(ticker, "a1", [ticker]),
        _preproc(ticker, "a2", [ticker]),
        _preproc(ticker, "a3", [ticker]),
        _preproc(ticker, "a4", []),  # included but no target-local chunk evidence -> leakage
        _preproc(ticker, "a5", []),  # article-context-only reject -> successful decision
    ]
    summary = {
        "preprocessing_row_count": 5,
        "sentence_count": 5,
        "article_context_only_rejected_count": 1,
    }
    return _report(ticker, groups, preproc, summary=summary)


def _build(reports: dict[str, object], *, tickers: tuple[str, ...], sample_limit: int = 25, run_id: str = "exact-run/01") -> dict[str, Any]:
    return build_semantic_qa_payload(
        reports=reports,
        run_id=run_id,
        from_date=_WINDOW["from_date"],
        to_date=_WINDOW["to_date"],
        tickers=tickers,
        sample_limit=sample_limit,
        generated_at=_FIXED_NOW,
    )


# --------------------------------------------------------------------------- #
# Query normalization                                                          #
# --------------------------------------------------------------------------- #
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


def test_normalize_query_defaults_and_validation_bounds() -> None:
    default = normalize_semantic_qa_query(
        run_id="r", from_date="2026-05-21", to_date="2026-05-22"
    )
    assert default["tickers"] == ("AAPL", "AMD", "NVDA", "MSFT")
    assert default["sample_limit"] == 25
    assert normalize_semantic_qa_query(
        run_id="r", from_date="2026-05-21", to_date="2026-05-22", sample_limit="1"
    )["sample_limit"] == 1
    for bad in ("0", "51", "-3", "abc"):
        with pytest.raises(ValueError):
            normalize_semantic_qa_query(
                run_id="r", from_date="2026-05-21", to_date="2026-05-22", sample_limit=bad
            )
    with pytest.raises(ValueError):
        normalize_semantic_qa_query(run_id="", from_date="2026-05-21", to_date="2026-05-22")
    with pytest.raises(ValueError):
        normalize_semantic_qa_query(run_id="r", from_date="2026-05-22", to_date="2026-05-21")
    with pytest.raises(ValueError):
        normalize_semantic_qa_query(
            run_id="r", from_date="2026-05-21", to_date="2026-05-22", tickers="TSLA"
        )


# --------------------------------------------------------------------------- #
# Aggregation: leakage, context rejection, false negative, contributors        #
# --------------------------------------------------------------------------- #
def test_queues_distinguish_leakage_context_rejection_and_false_negative() -> None:
    payload = _build({"AAPL": _mixed_report()}, tickers=("AAPL",), sample_limit=25)
    ticker = payload["tickers"]["AAPL"]
    assert payload["schema_id"] == SEMANTIC_QA_SCHEMA_ID
    assert [r["article_id"] for r in ticker["queues"]["potential_false_positives"]["rows"]] == ["a4"]
    assert [r["article_id"] for r in ticker["queues"]["potential_false_negatives"]["rows"]] == ["a2"]
    assert [r["article_id"] for r in ticker["queues"]["top_contributors"]["rows"]] == ["a1", "a3", "a4"]


def test_article_context_only_rejection_is_success_not_leakage() -> None:
    payload = _build({"AAPL": _mixed_report()}, tickers=("AAPL",))
    ticker = payload["tickers"]["AAPL"]
    fp_ids = {r["article_id"] for r in ticker["queues"]["potential_false_positives"]["rows"]}
    fn_ids = {r["article_id"] for r in ticker["queues"]["potential_false_negatives"]["rows"]}
    assert "a5" not in fp_ids
    assert "a5" not in fn_ids
    stage = ticker["funnel"]["article_context_only_rejected"]
    assert stage["canonical_count"] == 1
    assert stage["sample_count"] == 1


def test_summary_uses_canonical_producer_decisions_not_absent_boolean_keys() -> None:
    payload = _build({"AAPL": _mixed_report()}, tickers=("AAPL",))
    summary = payload["tickers"]["AAPL"]["summary"]
    assert summary["signal_rows"] == 3
    assert summary["rejected_rows"] == 2
    assert summary["direct_accepted"] == 2
    assert summary["borderline"] == 1
    assert summary["total_contribution"] == 1.1
    assert summary["human_status"] is None


def test_top_contributors_use_canonical_final_contribution_and_rank_absolutely() -> None:
    groups = [
        _group("AAPL", "a1", "Apple product event", [_row(included=True, decision="accepted", contribution=-0.9)]),
        _group("AAPL", "a2", "Apple services", [_row(included=True, decision="accepted", contribution=0.4)]),
    ]
    report = _report("AAPL", groups, [_preproc("AAPL", "a1", ["AAPL"]), _preproc("AAPL", "a2", ["AAPL"])])
    payload = _build({"AAPL": report}, tickers=("AAPL",))
    summary = payload["tickers"]["AAPL"]["summary"]
    assert summary["total_contribution"] == pytest.approx(-0.5)
    assert [r["article_id"] for r in payload["tickers"]["AAPL"]["queues"]["top_contributors"]["rows"]] == ["a1", "a2"]


def test_cross_ticker_owner_is_derived_from_chunk_local_mentions() -> None:
    report = _report(
        "AMD",
        [_group("AMD", "x1", "NVDA datacenter GPU", [_row(included=True, decision="accepted", contribution=0.2)])],
        [_preproc("AMD", "x1", ["NVDA"])],
    )
    payload = _build({"AMD": report}, tickers=("AMD",), sample_limit=10)
    anomaly = payload["tickers"]["AMD"]["queues"]["cross_ticker_anomalies"]
    assert anomaly["canonical_count"] == 1
    assert anomaly["rows"][0]["evidence_owner"] == "NVDA"
    cell = payload["leakage_matrix"]["NVDA"]["AMD"]
    assert cell["canonical_count"] == 1
    assert cell["suspicious"] is True


# --------------------------------------------------------------------------- #
# Leakage matrix canonical counts + bounded IDs                                #
# --------------------------------------------------------------------------- #
def test_leakage_matrix_uses_full_owner_rows_before_sampling() -> None:
    groups = [
        _group("AMD", f"cross-{i}", "NVDA datacenter GPU", [_row(included=True, decision="accepted", contribution=0.1)])
        for i in range(40)
    ]
    preproc = [_preproc("AMD", f"cross-{i}", ["NVDA"]) for i in range(40)]
    report = _report("AMD", groups, preproc)
    payload = _build({"AMD": report}, tickers=("AMD",), sample_limit=10)
    cell = payload["leakage_matrix"]["NVDA"]["AMD"]
    assert cell["canonical_count"] == 40
    assert len(cell["row_ids"]) <= 10
    assert payload["tickers"]["AMD"]["queues"]["cross_ticker_anomalies"]["canonical_count"] == 40
    assert payload["tickers"]["AMD"]["queues"]["cross_ticker_anomalies"]["sample_count"] == 10


def test_leakage_matrix_records_unknown_generic_for_unattributable_contributions() -> None:
    report = _report(
        "AAPL",
        [_group("AAPL", "a4", "Market context", [_row(included=True, decision="accepted", contribution=0.1)])],
        [_preproc("AAPL", "a4", [])],
    )
    payload = _build({"AAPL": report}, tickers=("AAPL",))
    assert payload["leakage_matrix"]["unknown_generic"]["AAPL"]["canonical_count"] == 1
    assert payload["leakage_matrix"]["unknown_generic"]["AAPL"]["suspicious"] is True


def test_four_ticker_queue_isolation_keeps_cross_ticker_rows_off_own_queues() -> None:
    reports: dict[str, object] = {}
    for ticker in ("AAPL", "NVDA", "MSFT"):
        reports[ticker] = _report(
            ticker,
            [_group(ticker, f"{ticker}-fp", "Market context", [_row(included=True, decision="accepted", contribution=0.1)])],
            [_preproc(ticker, f"{ticker}-fp", [])],
        )
    reports["AMD"] = _report(
        "AMD",
        [_group("AMD", "AMD-anom", "NVDA datacenter GPU", [_row(included=True, decision="accepted", contribution=0.2)])],
        [_preproc("AMD", "AMD-anom", ["NVDA"])],
    )
    payload = _build(reports, tickers=("AAPL", "AMD", "NVDA", "MSFT"), sample_limit=5)
    assert [r["row_id"] for r in payload["tickers"]["AAPL"]["queues"]["potential_false_positives"]["rows"]] == [
        "2026-05-21:AAPL:AAPL-fp:0:0"
    ]
    amd_fp = payload["tickers"]["AMD"]["queues"]["potential_false_positives"]
    assert [r["article_id"] for r in amd_fp["rows"]] == ["AMD-anom"]
    assert payload["tickers"]["AMD"]["queues"]["cross_ticker_anomalies"]["rows"][0]["article_id"] == "AMD-anom"
    for ticker in ("AAPL", "AMD", "NVDA", "MSFT"):
        own_fp = payload["tickers"][ticker]["queues"]["potential_false_positives"]["rows"]
        assert all(row["ticker"] == ticker for row in own_fp)
    for ticker in ("AAPL", "NVDA", "MSFT"):
        assert payload["tickers"][ticker]["queues"]["cross_ticker_anomalies"]["canonical_count"] == 0
    assert payload["leakage_matrix"]["NVDA"]["AMD"]["canonical_count"] == 1


def test_payload_is_deterministic_under_reversed_input() -> None:
    def build(order: tuple[str, ...], reverse_groups: bool) -> dict[str, Any]:
        reports: dict[str, object] = {}
        for ticker in order:
            groups = [
                _group(ticker, f"{ticker}-1", "Market context", [_row(included=True, decision="accepted", contribution=0.1)]),
                _group(ticker, f"{ticker}-2", "Market context", [_row(included=True, decision="accepted", contribution=0.2)]),
            ]
            preproc = [_preproc(ticker, f"{ticker}-1", []), _preproc(ticker, f"{ticker}-2", [])]
            if reverse_groups:
                groups.reverse()
                preproc.reverse()
            reports[ticker] = _report(ticker, groups, preproc)
        return _build(reports, tickers=order if not reverse_groups else tuple(reversed(order)), sample_limit=5)

    forward = build(("AAPL", "AMD"), reverse_groups=False)
    reversed_ = build(("AAPL", "AMD"), reverse_groups=True)
    assert json.dumps(forward, sort_keys=True) == json.dumps(reversed_, sort_keys=True)


# --------------------------------------------------------------------------- #
# Integrity                                                                    #
# --------------------------------------------------------------------------- #
def test_integrity_stage_records_reconcile_and_surface_mismatch() -> None:
    groups = [
        _group("AAPL", f"a{i}", "Apple product event", [_row(included=True, decision="accepted", contribution=0.1)])
        for i in range(30)
    ]
    preproc = [_preproc("AAPL", f"a{i}", ["AAPL"]) for i in range(30)]
    report = _report("AAPL", groups, preproc, summary={"preprocessing_row_count": 650, "sentence_count": 30})
    payload = _build({"AAPL": report}, tickers=("AAPL",), sample_limit=30)
    stage = payload["integrity"]["stages"]["AAPL:total_preprocessed_chunks"]
    assert set(stage) == {"canonical_count", "sample_count", "omitted_count", "reconciles"}
    assert stage == {"canonical_count": 650, "sample_count": 30, "omitted_count": 620, "reconciles": True}

    mismatched = _report(
        "AAPL",
        groups[:3],
        preproc[:3],
        summary={"sentence_count": 1},
    )
    for group in mismatched["article_groups"]:
        group["sentence_rows"][0]["sentiment_score"] = 0.5
    payload_mismatch = _build({"AAPL": mismatched}, tickers=("AAPL",), sample_limit=30)
    stage_mismatch = payload_mismatch["integrity"]["stages"]["AAPL:sentiment_scored"]
    assert stage_mismatch["reconciles"] is False
    assert "AAPL:sentiment_scored_not_reconciled" in payload_mismatch["integrity"]["issue_codes"]


def test_integrity_reports_unavailable_counts_as_null_not_zero() -> None:
    report = _report(
        "AAPL",
        [_group("AAPL", "a1", "Apple product event", [_row(included=True, decision="accepted", contribution=0.5)])],
        [_preproc("AAPL", "a1", ["AAPL"])],
    )
    payload = _build({"AAPL": report}, tickers=("AAPL",))
    stage = payload["integrity"]["stages"]["AAPL:local_target_evidence"]
    assert stage["canonical_count"] is None
    assert stage["reconciles"] is None
    assert payload["integrity"]["status"] in {"pass", "warn"}


def test_integrity_checks_exact_identity_without_truncation() -> None:
    opaque = "layer1-daily-2026-06-18...post-pr313_pr314-modal-t4-20260911_153730"
    report = _report(
        "AAPL",
        [_group("AAPL", "a1", "Apple product event", [_row(included=True, decision="accepted", contribution=0.5)])],
        [_preproc("AAPL", "a1", ["AAPL"])],
        summary={"preprocessing_row_count": 1},
        run_id=opaque,
    )
    payload = _build({"AAPL": report}, tickers=("AAPL",), run_id=opaque)
    assert payload["run"]["run_id"] == opaque
    assert payload["run"]["requested_start"] == "2026-05-21"
    assert payload["run"]["requested_end"] == "2026-05-22"
    assert payload["run"]["artifact_ids"]["AAPL"] == ["exact/AAPL.parquet"]
    assert payload["schema_id"] == SEMANTIC_QA_SCHEMA_ID
    assert payload["integrity"]["issue_codes"] == []

    mismatched = _build({"AAPL": _report("AAPL", run_id="other-run")}, tickers=("AAPL",), run_id=opaque)
    assert "AAPL:run_id_mismatch" in mismatched["integrity"]["issue_codes"]


# --------------------------------------------------------------------------- #
# Empty / partial / missing / null semantics                                   #
# --------------------------------------------------------------------------- #
def test_empty_and_missing_tickers_are_explicit_null_not_zero() -> None:
    payload = _build({}, tickers=("AAPL", "AMD"), run_id="r")
    assert payload["status"] == "empty"
    assert payload["tickers"]["AAPL"]["summary"]["signal_rows"] is None
    assert payload["tickers"]["AAPL"]["funnel"]["total_preprocessed_chunks"]["canonical_count"] is None
    codes = {w["code"] for w in payload["warnings"]}
    assert "missing_review_artifacts" in codes
    assert payload["human_gate"]["can_accept_four_ticker_pilot"] is False


def test_partial_missing_returns_present_metrics_and_ticker_warning() -> None:
    reports = {"AAPL": _mixed_report()}
    payload = _build(reports, tickers=("AAPL", "AMD"))
    assert payload["tickers"]["AAPL"]["summary"]["signal_rows"] == 3
    assert payload["tickers"]["AMD"]["summary"]["signal_rows"] is None
    missing = [w for w in payload["warnings"] if w["code"] == "missing_review_artifacts"]
    assert [w["ticker"] for w in missing] == ["AMD"]


def test_established_empty_emits_zero_and_no_review_rows_warning() -> None:
    report = _report("AAPL", summary={"preprocessing_row_count": 0, "sentence_count": 0})
    report["row_count"] = 0
    payload = _build({"AAPL": report}, tickers=("AAPL",))
    assert payload["status"] == "empty"
    assert payload["tickers"]["AAPL"]["summary"]["signal_rows"] == 0
    assert payload["tickers"]["AAPL"]["summary"]["rejected_rows"] == 0
    assert any(w["code"] == "no_review_rows" for w in payload["warnings"])


def test_unobservable_decisions_return_null_with_warning() -> None:
    report = _report(
        "AAPL",
        [_group("AAPL", "a1", "Apple product event", [_row(included=True, contribution=0.5)])],
        [_preproc("AAPL", "a1", ["AAPL"])],
    )
    payload = _build({"AAPL": report}, tickers=("AAPL",))
    summary = payload["tickers"]["AAPL"]["summary"]
    assert summary["signal_rows"] == 1
    assert summary["rejected_rows"] is None
    assert summary["direct_accepted"] is None
    assert summary["borderline"] is None
    unavailable = [w for w in payload["warnings"] if w["code"] == "canonical_count_unavailable"]
    assert {w["stage"] for w in unavailable} >= {"rejected_rows", "direct_accepted", "borderline"}


def test_stale_freshness_is_advisory_not_integrity_failure() -> None:
    report = _report(
        "AAPL",
        [_group("AAPL", "a1", "Apple product event", [_row(included=True, decision="accepted", contribution=0.5)])],
        [_preproc("AAPL", "a1", ["AAPL"])],
        summary={"preprocessing_row_count": 1},
    )
    report["generated_at"] = "2026-06-10T20:00:00+00:00"
    payload = _build({"AAPL": report}, tickers=("AAPL",))
    assert payload["freshness"]["state"] == "stale"
    assert payload["freshness"]["age_seconds"] > 86400
    assert payload["status"] == "warning"
    assert any(w["code"] == "stale_review_artifacts" for w in payload["warnings"])
    assert payload["integrity"]["status"] != "fail"


# --------------------------------------------------------------------------- #
# Endpoint behavior                                                            #
# --------------------------------------------------------------------------- #
@pytest.fixture
def qa_server() -> object:
    defaults = dashboard._DashboardDefaults(
        run_id="run",
        from_date="2026-05-21",
        to_date="2026-05-22",
        ticker="AAPL",
        host="127.0.0.1",
        port=0,
    )
    server = dashboard._DashboardHTTPServer((defaults.host, defaults.port), defaults)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def _get(server: object, path: str) -> tuple[int, str | None, str]:
    connection = HTTPConnection(str(server.server_address[0]), int(server.server_address[1]), timeout=5)  # type: ignore[attr-defined]
    try:
        connection.request("GET", path)
        response = connection.getresponse()
        return response.status, response.getheader("Cache-Control"), response.read().decode("utf-8")
    finally:
        connection.close()


def _fp_report(ticker: str, count: int) -> dict[str, Any]:
    groups = [
        _group(ticker, f"{ticker}-fp-{i}", "Market context", [_row(included=True, decision="accepted", contribution=0.1, si=i, ci=i)])
        for i in range(count)
    ]
    preproc = [_preproc(ticker, f"{ticker}-fp-{i}", [], si=i, ci=i) for i in range(count)]
    return _report(ticker, groups, preproc, summary={"preprocessing_row_count": count})


def test_endpoint_rejects_invalid_requests_with_envelope(monkeypatch: pytest.MonkeyPatch, qa_server: object) -> None:
    status, cache, body = _get(qa_server, "/api/semantic-qa?run_id=run&from_date=bad&to_date=2026-05-22")
    assert status == 400
    assert cache == "no-store"
    payload = json.loads(body)
    assert payload["status"] == "error"
    assert payload["schema_id"] == SEMANTIC_QA_SCHEMA_ID
    assert payload["error"]["code"] == "invalid_request"


def test_endpoint_enforces_sample_limit_bounds(monkeypatch: pytest.MonkeyPatch, qa_server: object) -> None:
    monkeypatch.setattr(dashboard, "build_layer1_aapl_evidence_report", lambda **_: _fp_report("AAPL", 3))
    for limit in ("0", "51"):
        status, _, body = _get(
            qa_server,
            f"/api/semantic-qa?run_id=run&from_date=2026-05-21&to_date=2026-05-22&sample_limit={limit}",
        )
        assert status == 400
        assert json.loads(body)["error"]["code"] == "invalid_request"
    status, cache, body = _get(
        qa_server,
        "/api/semantic-qa?run_id=run&from_date=2026-05-21&to_date=2026-05-22&tickers=AAPL&sample_limit=1",
    )
    assert status == 200
    assert cache == "no-store"
    assert json.loads(body)["schema_id"] == SEMANTIC_QA_SCHEMA_ID


def test_endpoint_success_sets_no_store_and_exact_identity(monkeypatch: pytest.MonkeyPatch, qa_server: object) -> None:
    monkeypatch.setattr(dashboard, "build_layer1_aapl_evidence_report", lambda **_: _fp_report("AAPL", 3))
    status, cache, body = _get(
        qa_server, "/api/semantic-qa?run_id=run&from_date=2026-05-21&to_date=2026-05-22&tickers=AAPL"
    )
    assert status == 200
    assert cache == "no-store"
    payload = json.loads(body)
    assert payload["run"]["run_id"] == "run"
    assert payload["run"]["requested_start"] == "2026-05-21"
    assert payload["run"]["requested_end"] == "2026-05-22"


def test_endpoint_missing_artifacts_returns_404_envelope(monkeypatch: pytest.MonkeyPatch, qa_server: object) -> None:
    def missing(**_: object) -> dict[str, Any]:
        raise FileNotFoundError("missing")

    monkeypatch.setattr(dashboard, "build_layer1_aapl_evidence_report", missing)
    status, cache, body = _get(
        qa_server, "/api/semantic-qa?run_id=run&from_date=2026-05-21&to_date=2026-05-22"
    )
    assert status == 404
    assert cache == "no-store"
    assert json.loads(body)["error"]["code"] == "review_artifacts_not_found"


def test_endpoint_partial_missing_returns_200_with_warning(monkeypatch: pytest.MonkeyPatch, qa_server: object) -> None:
    def partial(*, ticker: str, **_: object) -> dict[str, Any]:
        if ticker != "AAPL":
            raise FileNotFoundError("missing")
        return _fp_report("AAPL", 3)

    monkeypatch.setattr(dashboard, "build_layer1_aapl_evidence_report", partial)
    status, cache, body = _get(
        qa_server, "/api/semantic-qa?run_id=run&from_date=2026-05-21&to_date=2026-05-22"
    )
    assert status == 200
    assert cache == "no-store"
    payload = json.loads(body)
    assert payload["tickers"]["AAPL"]["summary"]["signal_rows"] == 3
    assert payload["tickers"]["AMD"]["summary"]["signal_rows"] is None
    assert any(w["code"] == "missing_review_artifacts" and w["ticker"] == "AMD" for w in payload["warnings"])


def test_endpoint_producer_failure_is_sanitized_500(monkeypatch: pytest.MonkeyPatch, qa_server: object) -> None:
    monkeypatch.setattr(dashboard, "build_layer1_aapl_evidence_report", lambda **_: _fp_report("AAPL", 1))

    def explode(**_: object) -> dict[str, Any]:
        raise RuntimeError("boom secret-token /var/secrets/creds.json")

    monkeypatch.setattr(dashboard, "build_semantic_qa_payload", explode)
    status, cache, body = _get(
        qa_server, "/api/semantic-qa?run_id=run&from_date=2026-05-21&to_date=2026-05-22"
    )
    assert status == 500
    assert cache == "no-store"
    payload = json.loads(body)
    assert payload["error"]["code"] == "semantic_qa_build_failed"
    assert "boom" not in body
    assert "secret-token" not in body


def test_endpoint_cache_key_isolates_limit_and_ticker_identities(
    monkeypatch: pytest.MonkeyPatch, qa_server: object
) -> None:
    calls: list[str] = []

    def counting(*, ticker: str, **_: object) -> dict[str, Any]:
        calls.append(ticker)
        return _fp_report(ticker, 3)

    monkeypatch.setattr(dashboard, "build_layer1_aapl_evidence_report", counting)
    base = "/api/semantic-qa?run_id=run&from_date=2026-05-21&to_date=2026-05-22"

    _, _, body_limit1 = _get(qa_server, f"{base}&sample_limit=1")
    after_first = len(calls)
    assert after_first == 4  # four pilot tickers built
    assert json.loads(body_limit1)["tickers"]["AAPL"]["queues"]["potential_false_positives"]["sample_count"] == 1

    _get(qa_server, f"{base}&sample_limit=1")
    assert len(calls) == after_first  # identical request served from cache

    _, _, body_limit2 = _get(qa_server, f"{base}&sample_limit=2")
    assert len(calls) == after_first + 4  # different limit is a distinct cache key
    assert json.loads(body_limit2)["tickers"]["AAPL"]["queues"]["potential_false_positives"]["sample_count"] == 2

    _, _, body_ticker = _get(qa_server, f"{base}&tickers=AAPL&sample_limit=2")
    assert len(calls) == after_first + 5  # different ticker set is a distinct cache key
    assert set(json.loads(body_ticker)["tickers"]) == {"AAPL"}
