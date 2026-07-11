from __future__ import annotations

import json

import pytest

from core.features.news_assignment_provenance import build_ticker_assignment_provenance
from core.features.news_preprocessing import preprocess_news_articles_with_provenance


def test_build_ticker_assignment_provenance_prefers_direct_ticker_mention() -> None:
    """Exact ticker mentions are classified as direct evidence."""
    article = {
        "id": "direct-1",
        "headline": "AAPL rises after earnings",
        "content": "AAPL beats estimates and shares jump.",
        "symbols": [],
    }

    provenance = build_ticker_assignment_provenance(
        article,
        "AAPL",
        date="2024-01-02",
        sentence_index=0,
        headline="AAPL rises after earnings",
        text="AAPL beats estimates and shares jump.",
    )

    assert provenance.classification == "direct"
    assert provenance.evidence_kinds == ("direct_ticker_mention",)
    assert provenance.provider_ticker_source is None
    assert provenance.reason == "ticker symbol mention matched AAPL."


def test_build_ticker_assignment_provenance_detects_provider_ticker_tag_without_text_mention() -> None:
    """Raw provider ticker tags are exposed even when the text omits the symbol."""
    article = {
        "id": "provider-1",
        "headline": "Quarterly results beat estimates",
        "content": "The company reported strong growth.",
        "symbols": ["AAPL"],
    }

    provenance = build_ticker_assignment_provenance(
        article,
        "AAPL",
        date="2024-01-02",
        sentence_index=1,
        headline="Quarterly results beat estimates",
        text="The company reported strong growth.",
    )

    assert provenance.classification == "direct"
    assert provenance.evidence_kinds == ("provider_ticker_tag",)
    assert provenance.provider_ticker_source == "symbols"
    assert provenance.provider_tickers == ("AAPL",)
    assert provenance.reason == "provider ticker tag matched AAPL."


def test_build_ticker_assignment_provenance_detects_company_alias_entity_match() -> None:
    """Company aliases and entities are surfaced as indirect evidence."""
    article = {
        "id": "alias-1",
        "headline": "Apple expands services",
        "content": "Apple Inc. said the new bundle will grow subscriptions.",
        "entities": ["Apple Inc."],
    }

    provenance = build_ticker_assignment_provenance(
        article,
        "AAPL",
        date="2024-01-02",
        sentence_index=0,
        headline="Apple expands services",
        text="Apple Inc. said the new bundle will grow subscriptions.",
    )

    assert provenance.classification == "indirect"
    assert provenance.evidence_kinds == ("company_alias_entity_match",)
    assert provenance.matched_entities == ("Apple Inc.",)
    assert "entity match found Apple Inc." in provenance.reason


def test_build_ticker_assignment_provenance_marks_contextual_only_signals() -> None:
    """Semantic similarity and topic context are exposed when no direct evidence exists."""
    article = {
        "id": "context-1",
        "headline": "Broad market optimism lifts technology names",
        "content": "Traders rotated into the sector on improved sentiment.",
    }

    provenance = build_ticker_assignment_provenance(
        article,
        "MSFT",
        date="2024-01-02",
        sentence_index=2,
        headline="Broad market optimism lifts technology names",
        text="Traders rotated into the sector on improved sentiment.",
        semantic_similarity_score=0.8421,
        topic_id=7,
        topic_probability=0.91,
    )

    assert provenance.classification == "contextual"
    assert provenance.evidence_kinds == ("semantic_similarity", "topic_context")
    assert provenance.semantic_similarity_score == pytest.approx(0.8421)
    assert provenance.topic_id == 7
    assert provenance.topic_probability == pytest.approx(0.91)
    assert "semantic similarity/relevance score=0.842" in provenance.reason
    assert "topic context topic_id=7, topic_probability=0.910" in provenance.reason


def test_build_ticker_assignment_provenance_falls_back_without_evidence() -> None:
    """Rows with no provenance signals are labeled as fallback-only."""
    article = {
        "id": "fallback-1",
        "headline": "Market roundup",
        "content": "The session was mixed across major indices.",
    }

    provenance = build_ticker_assignment_provenance(
        article,
        "AAPL",
        date="2024-01-02",
        sentence_index=None,
        headline="Market roundup",
        text="The session was mixed across major indices.",
    )

    assert provenance.classification == "fallback"
    assert provenance.evidence_kinds == ("fallback_missing_evidence",)
    assert "fallback only" in provenance.reason


def test_preprocess_news_articles_with_provenance_emits_matching_rows() -> None:
    """Preprocessing returns aligned records and provenance rows for each assignment."""
    articles = [
        {
            "id": "article-1",
            "headline": "Apple shares rise",
            "content": "Apple shares rose after results.",
            "symbols": ["AAPL"],
            "entities": ["Apple Inc."],
        }
    ]

    records, provenance_rows = preprocess_news_articles_with_provenance(
        articles,
        as_of_date="2024-01-02",
        point_in_time_tickers=["AAPL"],
    )

    assert len(records) == len(provenance_rows) == 2
    assert all(row.ticker == "AAPL" for row in provenance_rows)
    assert all(row.classification == "direct" for row in provenance_rows)
    assert all(row.provider_ticker_source == "symbols" for row in provenance_rows)
    assert {row.sentence_index for row in provenance_rows} == {0, 1}
    assert records[0].article_id == "article-1"
    assert json.loads(json.dumps([row.to_dict() for row in provenance_rows]))
