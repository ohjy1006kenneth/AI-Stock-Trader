from __future__ import annotations

from cloud_training.data_pipelines import build_predictive_dataset as dataset


class CountingKeywordMockSentimentScorer(dataset.KeywordMockSentimentScorer):
    def __init__(self) -> None:
        self.calls = 0

    def score(self, text: str) -> dict[str, float]:
        self.calls += 1
        return super().score(text)


def test_compute_text_features_reuses_cached_window_aggregations() -> None:
    news_by_day = {
        "2026-03-27": [
            {
                "date": "2026-03-27T18:00:00Z",
                "timestamp": "2026-03-27T18:00:00Z",
                "headline": "AAPL beats estimates on profit surge",
                "summary": "Analysts turn bullish after strong quarter.",
                "source": "benzinga",
            },
            {
                "date": "2026-03-27T23:00:00Z",
                "timestamp": "2026-03-27T23:00:00Z",
                "headline": "AAPL faces downgrade risk after fraud probe rumor",
                "summary": "Late article adds negative tone.",
                "source": "benzinga",
            },
        ],
        "2026-03-26": [
            {
                "date": "2026-03-26T18:00:00Z",
                "timestamp": "2026-03-26T18:00:00Z",
                "headline": "AAPL launches new product",
                "summary": "Investors await demand commentary.",
                "source": "reuters",
            },
        ],
    }
    scorer = CountingKeywordMockSentimentScorer()
    cache: dict[tuple[str, int], dict[str, float]] = {}

    first = dataset._compute_text_features(
        news_by_day,
        scorer=scorer,
        as_of_date="2026-03-27",
        recency_halflife_hours=12.0,
        rolling_window_days=7,
        aggregate_cache=cache,
    )
    calls_after_first = scorer.calls

    second = dataset._compute_text_features(
        news_by_day,
        scorer=scorer,
        as_of_date="2026-03-27",
        recency_halflife_hours=12.0,
        rolling_window_days=7,
        aggregate_cache=cache,
    )

    assert calls_after_first == 3
    assert scorer.calls == calls_after_first
    assert first == second
    assert first["news_count"] == first["news_count_7d"]
    assert set(cache) == {("2026-03-27", 3), ("2026-03-27", 7)}
