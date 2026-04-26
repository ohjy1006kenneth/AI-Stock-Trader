from __future__ import annotations

import io
from datetime import UTC

import pandas as pd

from core.contracts.schemas import NewsSentimentRecord
from core.features.news_preprocessing import (
    NewsPreprocessingConfig,
    news_sentiment_frame_to_records,
    preprocess_news_articles,
    records_to_news_sentiment_frame,
    split_article_sentences,
)


def test_preprocess_news_articles_emits_sentence_records_for_multiple_alias_tickers() -> None:
    """Raw Alpaca symbols are normalized and expanded into per-ticker sentence rows."""
    articles = [
        {
            "id": 1001,
            "headline": "Apple and Berkshire rally.",
            "summary": "Apple Inc. reported earnings. Berkshire also gained!",
            "content": "Apple Inc. reported earnings. Berkshire also gained!",
            "source": "benzinga",
            "url": "https://example.test/article",
            "created_at": "2024-01-02T12:00:00+00:00",
            "symbols": ["AAPL", "BRK.B", "FB", "NOTIN"],
        }
    ]

    records = preprocess_news_articles(
        articles,
        as_of_date="2024-01-02",
        point_in_time_tickers=["AAPL", "BRK-B", "META"],
    )

    assert {(record.ticker, record.sentence_index) for record in records} == {
        ("AAPL", 0),
        ("BRK-B", 0),
        ("META", 0),
        ("AAPL", 1),
        ("BRK-B", 1),
        ("META", 1),
        ("AAPL", 2),
        ("BRK-B", 2),
        ("META", 2),
    }
    assert all(isinstance(record, NewsSentimentRecord) for record in records)
    assert all(record.article_id == "1001" for record in records)
    assert all(record.headline == "Apple and Berkshire rally." for record in records)
    assert all(record.published_at is not None for record in records)
    assert records[0].published_at.isoformat() == "2024-01-02T12:00:00+00:00"


def test_split_article_sentences_handles_abbreviations_and_deduplicates() -> None:
    """Sentence splitting avoids common abbreviation splits and removes duplicate text."""
    article = {
        "headline": "U.S. stocks rose.",
        "summary": "Apple Inc. reported earnings. Shares rose!",
        "content": "Apple Inc. reported earnings. Shares rose!",
    }

    sentences = split_article_sentences(article)

    assert sentences == [
        "U.S. stocks rose.",
        "Apple Inc. reported earnings.",
        "Shares rose!",
    ]


def test_preprocess_news_articles_filters_empty_text_and_missing_universe_symbols() -> None:
    """Articles without allowed point-in-time ticker tags are skipped."""
    records = preprocess_news_articles(
        [
            {"id": "empty-text", "headline": " ", "symbols": ["AAPL"]},
            {"id": "not-allowed", "headline": "Microsoft rallied.", "symbols": ["MSFT"]},
        ],
        as_of_date="2024-01-02",
        point_in_time_tickers=["AAPL"],
        config=NewsPreprocessingConfig(min_sentence_chars=3),
    )

    assert records == []


def test_news_sentiment_frame_round_trips_records() -> None:
    """Preprocessed records serialize to a Parquet-ready frame and back."""
    records = preprocess_news_articles(
        [
            {
                "id": "round-trip",
                "headline": "Apple launches product.",
                "created_at": "2024-01-02T12:00:00+00:00",
                "symbols": ["AAPL"],
            }
        ],
        as_of_date="2024-01-02",
        point_in_time_tickers=["AAPL"],
    )

    frame = records_to_news_sentiment_frame(records)
    restored = news_sentiment_frame_to_records(pd.read_parquet(_to_parquet(frame)))

    assert restored == records
    assert restored[0].published_at.tzinfo == UTC


def _to_parquet(frame: pd.DataFrame) -> io.BytesIO:
    """Serialize a frame to an in-memory Parquet buffer."""
    buffer = io.BytesIO()
    frame.to_parquet(buffer, index=False)
    buffer.seek(0)
    return buffer
