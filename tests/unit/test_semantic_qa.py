from __future__ import annotations

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
