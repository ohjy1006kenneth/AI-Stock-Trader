from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from core.features.news_preprocessing import preprocess_news_articles
from core.features.news_relevance import (
    RELEVANCE_GATE_COLUMNS,
    apply_news_relevance_gate,
)


def test_news_relevance_gate_filters_aapl_contamination_pattern() -> None:
    """Weak non-AAPL article tags do not flow into AAPL FinBERT scoring."""
    articles = json.loads(
        Path("tests/fixtures/news_relevance_gate_articles.json").read_text(
            encoding="utf-8"
        )
    )
    records = preprocess_news_articles(
        articles,
        as_of_date="2024-01-02",
        point_in_time_tickers=("AAPL",),
    )

    result = apply_news_relevance_gate(
        records,
        embeddings=_embedding_frame(article["id"] for article in articles),
        topic_labels=_topic_label_frame(article["id"] for article in articles),
    )

    audit = result.audit_frame
    by_article = {
        article_id: set(frame["relevance_decision"])
        for article_id, frame in audit.groupby("article_id")
    }

    assert list(audit.columns) == list(RELEVANCE_GATE_COLUMNS)
    assert by_article["aapl-specific"] == {"accepted"}
    assert by_article["broad-market"] == {"borderline"}
    assert by_article["competitor-only"] == {"rejected"}
    assert by_article["irrelevant"] == {"rejected"}
    assert "competitor_entity_without_target:MSFT" in _reason_codes(
        audit.loc[audit["article_id"] == "competitor-only"].iloc[0]
    )
    assert {record.article_id for record in result.finbert_records} == {
        "aapl-specific",
        "broad-market",
    }
    assert all(record.relevance_score is not None for record in result.finbert_records)


def test_news_relevance_gate_does_not_fully_promote_unknown_rows() -> None:
    """Rows without ticker/entity and financial evidence are rejected with audit reasons."""
    records = preprocess_news_articles(
        [
            {
                "id": "unknown",
                "headline": "Community event opens downtown",
                "summary": "The schedule includes local speakers.",
                "created_at": "2024-01-02T12:00:00+00:00",
                "source": "benzinga",
                "symbols": ["AAPL"],
            }
        ],
        as_of_date="2024-01-02",
        point_in_time_tickers=("AAPL",),
    )

    result = apply_news_relevance_gate(records)

    assert result.finbert_records == []
    assert set(result.audit_frame["relevance_decision"]) == {"rejected"}
    assert result.audit_frame["relevance_score"].max() < 1.0
    assert "low_financial_relevance" in _reason_codes(result.audit_frame.iloc[0])


def _topic_label_frame(article_ids) -> pd.DataFrame:
    """Return topic-label rows for relevance-gate tests."""
    return pd.DataFrame(
        [
            {
                "date": "2024-01-02",
                "ticker": "AAPL",
                "article_id": article_id,
                "normalized_headline": article_id,
                "text": article_id,
                "article_sentence_count": 2,
                "embedding_cache_key": f"embedding-{article_id}",
                "topic_model": "test-topic",
                "topic_model_version": "test",
                "topic_id": 1,
                "topic_probability": 0.70,
            }
            for article_id in article_ids
        ]
    )


def _embedding_frame(article_ids) -> pd.DataFrame:
    """Return embedding rows for relevance-gate tests."""
    return pd.DataFrame(
        [
            {
                "date": "2024-01-02",
                "article_id": article_id,
                "normalized_headline": article_id,
                "text": article_id,
                "article_sentence_count": 2,
                "embedding_model": "test-embedding",
                "embedding_revision": "test",
                "embedding_cache_key": f"embedding-{article_id}",
                "embedding_json": "[0.1,0.2]",
            }
            for article_id in article_ids
        ]
    )


def _reason_codes(row: pd.Series) -> set[str]:
    """Decode relevance reason codes from one audit row."""
    return set(json.loads(str(row["reason_codes"])))
