from __future__ import annotations

from cloud_training.data_pipelines.finbert_sentiment import (
    KeywordMockSentimentScorer,
    aggregate_ticker_day_sentiment_features,
)


class CountingKeywordMockSentimentScorer(KeywordMockSentimentScorer):
    def __init__(self) -> None:
        self.calls = 0

    def score(self, text: str) -> dict[str, float]:
        self.calls += 1
        return super().score(text)
from cloud_training.model_architecture.hybrid_model import FEATURE_NAMES, extract_feature_row


def test_ticker_day_sentiment_aggregation_and_feature_extraction() -> None:
    articles = [
        {
            "date": "2026-03-27T18:00:00Z",
            "headline": "AAPL beats estimates on profit surge",
            "summary": "Analysts turn bullish after strong quarter.",
            "source": "benzinga",
        },
        {
            "date": "2026-03-27T23:00:00Z",
            "headline": "AAPL faces downgrade risk after fraud probe rumor",
            "summary": "Late article adds negative tone.",
            "source": "benzinga",
        },
    ]
    features = aggregate_ticker_day_sentiment_features(
        articles,
        scorer=KeywordMockSentimentScorer(),
        as_of_date="2026-03-27",
        recency_halflife_hours=12.0,
    )

    assert features["news_count"] == 2
    assert features["news_volume"] == 2.0
    assert 0.0 <= features["finbert_positive_prob_mean"] <= 1.0
    assert 0.0 <= features["finbert_negative_prob_mean"] <= 1.0
    assert 0.0 <= features["finbert_neutral_prob_mean"] <= 1.0
    assert len(features["article_sentiment"]) == 2
    assert features["finbert_article_age_hours_min"] <= features["finbert_article_age_hours_max"]

    sample = {
        "history": [
            {"close": 100, "high": 101, "low": 99, "volume": 1000},
            {"close": 101, "high": 103, "low": 100, "volume": 1200},
            {"close": 102, "high": 104, "low": 101, "volume": 1500},
        ],
        **features,
    }
    row = extract_feature_row(sample)
    assert len(row) == len(FEATURE_NAMES)
    assert row[FEATURE_NAMES.index("news_count")] == 2.0


def test_empty_news_returns_zeroed_sentiment_features() -> None:
    features = aggregate_ticker_day_sentiment_features(
        [],
        scorer=KeywordMockSentimentScorer(),
        as_of_date="2026-03-27",
    )
    assert features["news_count"] == 0
    assert features["finbert_sentiment_score_recency_weighted"] == 0.0
    assert features["article_sentiment"] == []


def test_sentiment_scoring_cache_reuses_duplicate_article_text_across_aggregations() -> None:
    scorer = CountingKeywordMockSentimentScorer()
    articles = [
        {
            "date": "2026-03-27T18:00:00Z",
            "headline": "AAPL beats estimates on profit surge",
            "summary": "Analysts turn bullish after strong quarter.",
            "source": "benzinga",
        },
        {
            "date": "2026-03-27T23:00:00Z",
            "headline": "AAPL faces downgrade risk after fraud probe rumor",
            "summary": "Late article adds negative tone.",
            "source": "benzinga",
        },
    ]

    first = aggregate_ticker_day_sentiment_features(
        articles,
        scorer=scorer,
        as_of_date="2026-03-27",
        recency_halflife_hours=12.0,
    )
    second = aggregate_ticker_day_sentiment_features(
        articles,
        scorer=scorer,
        as_of_date="2026-03-28",
        recency_halflife_hours=12.0,
    )

    assert scorer.calls == 2
    assert first["finbert_sentiment_score_mean"] == second["finbert_sentiment_score_mean"]
    assert len(first["article_sentiment"]) == len(second["article_sentiment"]) == 2
    assert second["finbert_article_age_hours_mean"] > first["finbert_article_age_hours_mean"]
