from __future__ import annotations

from cloud_training.data_pipelines.build_predictive_dataset import (
    _compute_text_features,
    _coverage_sort_key,
    build_sector_date_features,
    compute_context_features,
    compute_macro_features,
    compute_market_features,
    estimate_sample_start_date,
    fetch_ticker_news_map,
    select_tickers,
)
from cloud_training.data_pipelines.finbert_sentiment import KeywordMockSentimentScorer


class CountingKeywordMockSentimentScorer(KeywordMockSentimentScorer):
    def __init__(self) -> None:
        self.calls = 0

    def score(self, text: str) -> dict[str, float]:
        self.calls += 1
        return super().score(text)
from cloud_training.model_architecture.hybrid_model import FEATURE_NAMES, extract_feature_row


def _make_history(closes: list[float], *, start_volume: int = 1000) -> list[dict[str, float | int | str]]:
    rows = []
    for idx, close in enumerate(closes):
        rows.append({
            "date": f"2026-01-{idx + 1:02d}",
            "open": close * 0.99,
            "high": close * 1.01,
            "low": close * 0.98,
            "close": close,
            "volume": start_volume + idx * 10,
        })
    return rows


def test_issue11_market_and_macro_feature_blocks_emit_expected_fields() -> None:
    ticker_history = _make_history([100 + idx for idx in range(80)])
    spy_history = _make_history([200 + (idx * 0.5) for idx in range(80)], start_volume=5000)

    market_features = compute_market_features(ticker_history[-63:], spy_history[-63:])
    macro_features = compute_macro_features(spy_history[-63:])

    assert market_features["return_21d"] > 0
    assert "beta_to_spy_21d" in market_features
    assert "relative_strength_vs_spy_21d" in market_features
    assert "macro_spy_realized_vol_21d" in macro_features
    assert "macro_spy_sma_21_over_63" in macro_features


def test_issue11_context_features_include_fundamental_and_sector_relative_fields() -> None:
    prices = {
        "AAA": _make_history([100 + idx for idx in range(50)]),
        "BBB": _make_history([90 + idx for idx in range(50)]),
    }
    fundamentals = {
        "AAA": {
            "ticker": "AAA",
            "sector": "Tech",
            "industry": "Software",
            "country": "US",
            "quote_type": "EQUITY",
            "market_cap": 1_000_000_000,
            "average_volume": 2_000_000,
            "net_margin": 0.2,
            "debt_to_equity": 0.5,
            "free_cash_flow": 50_000_000,
            "revenue_growth": 0.1,
            "operating_margin": 0.18,
            "return_on_equity": 0.12,
        },
        "BBB": {
            "ticker": "BBB",
            "sector": "Tech",
            "industry": "Hardware",
            "country": "US",
            "quote_type": "EQUITY",
            "market_cap": 2_000_000_000,
            "average_volume": 3_000_000,
            "net_margin": 0.15,
            "debt_to_equity": 0.3,
            "free_cash_flow": 70_000_000,
            "revenue_growth": 0.07,
            "operating_margin": 0.16,
            "return_on_equity": 0.11,
        },
    }
    sector_map = build_sector_date_features(prices, fundamentals)
    as_of_date = prices["AAA"][21]["date"]

    context = compute_context_features("AAA", as_of_date, fundamentals, sector_map)

    assert context["sector"] == "Tech"
    assert context["industry"] == "Software"
    assert "market_cap_log" in context
    assert "free_cash_flow_yield" in context
    assert "sector_relative_return_21d" in context
    assert context["sector_peer_count"] >= 1.0


def test_issue11_text_features_use_rolling_news_window_and_coverage_fields() -> None:
    news_by_day = {
        "2026-01-05": [{"date": "2026-01-05T20:00:00Z", "timestamp": "2026-01-05T20:00:00Z", "headline": "AAA beats on profit surge", "summary": "Bullish quarter", "source": "benzinga"}],
        "2026-01-04": [{"date": "2026-01-04T15:00:00Z", "timestamp": "2026-01-04T15:00:00Z", "headline": "AAA faces fraud probe", "summary": "Risk rises", "source": "reuters"}],
        "2026-01-02": [{"date": "2026-01-02T13:00:00Z", "timestamp": "2026-01-02T13:00:00Z", "headline": "AAA gains after upgrade", "summary": "Momentum improves", "source": "benzinga"}],
    }

    features = _compute_text_features(
        news_by_day,
        scorer=KeywordMockSentimentScorer(),
        as_of_date="2026-01-05",
        recency_halflife_hours=12.0,
        rolling_window_days=7,
    )

    assert features["same_day_news_count"] == 1.0
    assert features["news_count"] == 3
    assert features["news_count_3d"] == 2.0
    assert features["news_count_7d"] == 3.0
    assert features["news_days_with_coverage_7d"] == 3.0
    assert features["news_source_count_7d"] == 2.0
    assert features["days_since_last_news_7d"] == 0.0
    assert "sentiment_acceleration_3d_vs_7d" in features


def test_issue11_text_feature_sentiment_cache_avoids_repeated_rescoring_across_overlapping_windows() -> None:
    news_by_day = {
        "2026-01-05": [{"date": "2026-01-05T20:00:00Z", "timestamp": "2026-01-05T20:00:00Z", "headline": "AAA beats on profit surge", "summary": "Bullish quarter", "source": "benzinga"}],
        "2026-01-04": [{"date": "2026-01-04T15:00:00Z", "timestamp": "2026-01-04T15:00:00Z", "headline": "AAA faces fraud probe", "summary": "Risk rises", "source": "reuters"}],
        "2026-01-02": [{"date": "2026-01-02T13:00:00Z", "timestamp": "2026-01-02T13:00:00Z", "headline": "AAA gains after upgrade", "summary": "Momentum improves", "source": "benzinga"}],
    }
    scorer = CountingKeywordMockSentimentScorer()

    _compute_text_features(
        news_by_day,
        scorer=scorer,
        as_of_date="2026-01-05",
        recency_halflife_hours=12.0,
        rolling_window_days=7,
    )

    assert scorer.calls == 3


def test_issue11_coverage_sort_key_prefers_more_news_days_then_items() -> None:
    ranked = sorted([
        ("AAA", {"news_days": 4.0, "news_items": 20.0}),
        ("BBB", {"news_days": 10.0, "news_items": 5.0}),
        ("CCC", {"news_days": 10.0, "news_items": 12.0}),
    ], key=_coverage_sort_key, reverse=True)

    assert [ticker for ticker, _ in ranked] == ["CCC", "BBB", "AAA"]


def test_issue11_select_tickers_can_rank_by_recent_coverage_and_exclude_market_proxy(monkeypatch) -> None:
    def fake_fetch_recent_news_coverage(tickers, *, lookback_days, end_dt=None, batch_size=50):
        assert lookback_days == 30
        assert end_dt is None
        assert batch_size == 50
        return {
            ticker: {
                "news_items": {"AAPL": 2.0, "ABBV": 18.0, "SPY": 99.0}.get(ticker, 0.0),
                "news_days": {"AAPL": 1.0, "ABBV": 7.0, "SPY": 20.0}.get(ticker, 0.0),
                "sources": 1.0,
            }
            for ticker in tickers
        }

    monkeypatch.setattr(
        "cloud_training.data_pipelines.build_predictive_dataset.fetch_recent_news_coverage",
        fake_fetch_recent_news_coverage,
    )

    selected, coverage = select_tickers(
        ["SPY", "AAPL", "ABBV"],
        2,
        strategy="coverage",
        coverage_lookback_days=30,
        exclude_market_proxy_target=True,
    )

    assert selected == ["ABBV", "AAPL"]
    assert "SPY" not in selected
    assert coverage["ABBV"]["news_days"] == 7.0


def test_issue11_estimate_sample_start_date_limits_news_fetch_to_sample_horizon() -> None:
    history = _make_history([100 + idx for idx in range(120)])

    assert estimate_sample_start_date(history, 21, 0) == history[0]["date"]
    assert estimate_sample_start_date(history, 21, 10) == history[-11]["date"]


def test_issue11_fetch_ticker_news_map_uses_unbounded_pagination(monkeypatch) -> None:
    captured = {}

    def fake_fetch_news(*, symbols, start_iso, end_iso, limit):
        captured.update({
            "symbols": symbols,
            "start_iso": start_iso,
            "end_iso": end_iso,
            "limit": limit,
        })
        return [
            {"date": "2026-03-31T10:00:00Z", "headline": "headline", "summary": "summary", "source": "benzinga"},
        ]

    monkeypatch.setattr(
        "cloud_training.data_pipelines.build_predictive_dataset.fetch_news",
        fake_fetch_news,
    )

    by_day = fetch_ticker_news_map("NVDA", "2026-03-01", "2026-04-01", 7)

    assert captured["symbols"] == ["NVDA"]
    assert captured["limit"] == 0
    assert "2026-03-31" in by_day


def test_issue11_feature_extractor_matches_expanded_feature_schema() -> None:
    sample = {name: float(idx) / 10.0 for idx, name in enumerate(FEATURE_NAMES, start=1)}
    sample["target_positive_return"] = 1
    row = extract_feature_row(sample)
    assert len(row) == len(FEATURE_NAMES)
    assert row[0] == sample[FEATURE_NAMES[0]]
    assert row[-1] == sample[FEATURE_NAMES[-1]]
