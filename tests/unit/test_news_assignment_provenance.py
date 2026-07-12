from __future__ import annotations

import json
from typing import cast

import pytest

from core.features.news_assignment_provenance import (
    NEWS_EVIDENCE_RELEVANCE_WEIGHTS,
    NewsEvidenceClass,
    build_ticker_assignment_provenance,
    build_ticker_assignment_review_payload,
)
from core.features.news_preprocessing import preprocess_news_articles_with_provenance


def test_build_ticker_assignment_provenance_classifies_direct_aapl_article() -> None:
    """Explicit Apple evidence is classified as direct."""
    article = {
        "id": "direct-aapl",
        "headline": "Apple reports record iPhone sales",
        "content": "AAPL and Apple Inc. both beat estimates on strong demand.",
        "symbols": ["AAPL"],
    }

    provenance = build_ticker_assignment_provenance(
        article,
        "AAPL",
        date="2024-01-02",
        sentence_index=0,
        headline="Apple reports record iPhone sales",
        text="AAPL and Apple Inc. both beat estimates on strong demand.",
    )

    assert provenance.classification == NewsEvidenceClass.DIRECT.value
    assert provenance.evidence_kinds == (
        "provider_ticker_tag",
        "direct_ticker_mention",
        "company_alias_entity_match",
    )
    assert NEWS_EVIDENCE_RELEVANCE_WEIGHTS[NewsEvidenceClass.DIRECT] == pytest.approx(1.0)
    assert "classification=direct" in provenance.reason


def test_build_ticker_assignment_provenance_classifies_indirect_supply_chain_context() -> None:
    """Supply-chain and relationship articles are classified as indirect."""
    article = {
        "id": "indirect-aapl",
        "headline": "Intel and TSMC discuss chip supply chain updates",
        "content": "The Apple supplier chain was the focus as peers discussed capacity.",
        "entities": ["Intel", "TSMC"],
    }

    provenance = build_ticker_assignment_provenance(
        article,
        "AAPL",
        date="2024-01-02",
        sentence_index=1,
        headline="Intel and TSMC discuss chip supply chain updates",
        text="The Apple supplier chain was the focus as peers discussed capacity.",
    )

    assert provenance.classification == NewsEvidenceClass.INDIRECT.value
    assert "relationship context matched" in provenance.reason
    assert NEWS_EVIDENCE_RELEVANCE_WEIGHTS[NewsEvidenceClass.INDIRECT] == pytest.approx(0.25)


def test_build_ticker_assignment_provenance_classifies_broad_market_context() -> None:
    """Provider tags without target text stay review-only broad-market evidence."""
    article = {
        "id": "broad-msft",
        "headline": "AI and tech stocks rally on market optimism",
        "content": "The broader Nasdaq and S&P 500 moved higher as investors rotated into stocks.",
        "symbols": ["MSFT"],
    }

    provenance = build_ticker_assignment_provenance(
        article,
        "MSFT",
        date="2024-01-02",
        sentence_index=2,
        headline="AI and tech stocks rally on market optimism",
        text="The broader Nasdaq and S&P 500 moved higher as investors rotated into stocks.",
    )

    assert provenance.classification == NewsEvidenceClass.BROAD_MARKET.value
    assert provenance.evidence_kinds == (
        "provider_ticker_tag_only",
        "broad_market_context",
    )
    assert NEWS_EVIDENCE_RELEVANCE_WEIGHTS[NewsEvidenceClass.BROAD_MARKET] == pytest.approx(0.0)
    assert "provider ticker tag only for MSFT; no textual target evidence" in provenance.reason
    assert "broad market context matched" in provenance.reason


def test_build_ticker_assignment_provenance_classifies_contamination() -> None:
    """Provider-tag-only rows with another company in the text are contamination."""
    article = {
        "id": "contamination-aapl",
        "headline": "Microsoft shares rise after cloud revenue beat",
        "content": "Microsoft reported Azure growth and stronger enterprise demand.",
        "symbols": ["AAPL"],
    }

    provenance = build_ticker_assignment_provenance(
        article,
        "AAPL",
        date="2024-01-02",
        sentence_index=3,
        headline="Microsoft shares rise after cloud revenue beat",
        text="Microsoft reported Azure growth and stronger enterprise demand.",
    )

    assert provenance.classification == NewsEvidenceClass.CONTAMINATION.value
    assert provenance.evidence_kinds == (
        "provider_ticker_tag_only",
        "contamination_other_entity",
    )
    assert (
        "provider ticker tag only for AAPL; no textual target evidence" in provenance.reason
    )
    assert "contamination evidence matched microsoft" in provenance.reason
    assert provenance.matched_symbols == ("AAPL",)


def test_build_ticker_assignment_review_payload_groups_rows_by_class() -> None:
    """Review payloads expose grouped counts for dashboard rendering."""
    rows = [
        build_ticker_assignment_provenance(
            {"id": "a1", "headline": "Apple earnings", "content": "AAPL beats", "symbols": ["AAPL"]},
            "AAPL",
            date="2024-01-02",
            sentence_index=0,
            headline="Apple earnings",
            text="AAPL beats",
        ),
        build_ticker_assignment_provenance(
            {
                "id": "a2",
                "headline": "Intel and TSMC discuss chips",
                "content": "Apple supplier chain focus.",
            },
            "AAPL",
            date="2024-01-02",
            sentence_index=1,
            headline="Intel and TSMC discuss chips",
            text="Apple supplier chain focus.",
        ),
        build_ticker_assignment_provenance(
            {
                "id": "a3",
                "headline": "AI stocks rally",
                "content": "Nasdaq and S&P 500 higher.",
            },
            "MSFT",
            date="2024-01-02",
            sentence_index=2,
            headline="AI stocks rally",
            text="Nasdaq and S&P 500 higher.",
        ),
        build_ticker_assignment_provenance(
            {
                "id": "a4",
                "headline": "Snap and Intel rise",
                "content": "ETF inflows lift tech.",
                "symbols": ["SNAP", "INTC"],
            },
            "AAPL",
            date="2024-01-02",
            sentence_index=3,
            headline="Snap and Intel rise",
            text="ETF inflows lift tech.",
        ),
    ]

    payload = build_ticker_assignment_review_payload(rows)

    rows_by_class = cast(dict[str, list[dict[str, object]]], payload["rows_by_class"])
    rows = cast(list[dict[str, object]], payload["rows"])

    assert payload["summary"] == {
        "direct": 1,
        "indirect": 1,
        "broad_market": 1,
        "contamination": 1,
        "total_rows": 4,
    }
    assert [row["classification"] for row in rows] == [
        "direct",
        "indirect",
        "broad_market",
        "contamination",
    ]
    assert list(rows_by_class.keys()) == ["direct", "indirect", "broad_market", "contamination"]


def test_preprocess_news_articles_with_provenance_emits_classified_rows() -> None:
    """Preprocessing preserves assignment provenance alongside sentence rows."""
    articles = [
        {
            "id": "article-1",
            "headline": "Apple shares rise",
            "content": "Apple shares rose after results.",
            "symbols": ["AAPL"],
        }
    ]

    records, provenance_rows = preprocess_news_articles_with_provenance(
        articles,
        as_of_date="2024-01-02",
        point_in_time_tickers=["AAPL"],
    )

    assert len(records) == len(provenance_rows) == 2
    assert all(row.ticker == "AAPL" for row in provenance_rows)
    assert all(row.classification == NewsEvidenceClass.DIRECT.value for row in provenance_rows)
    assert all(
        row.source_text_provenance["assignment_classification"] == NewsEvidenceClass.DIRECT.value
        for row in records
    )
    assert json.loads(json.dumps([row.to_dict() for row in provenance_rows]))
