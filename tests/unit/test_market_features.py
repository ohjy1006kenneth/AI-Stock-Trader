from __future__ import annotations

import math

import pandas as pd
import pytest

from core.features.market_features import (
    MARKET_FEATURE_COLUMNS,
    compute_market_features,
    market_features_to_records,
)


def _bars(prices: list[float], volumes: list[int] | None = None) -> pd.DataFrame:
    """Build a synthetic OHLCV frame matching the Layer 0 contract."""
    rows = []
    if volumes is None:
        volumes = [1_000_000] * len(prices)
    for index, price in enumerate(prices):
        date = f"2024-01-{index + 1:02d}"
        high = price * 1.01
        low = price * 0.99
        rows.append(
            {
                "date": date,
                "open": price,
                "high": high,
                "low": low,
                "close": price,
                "adj_close": price,
                "volume": volumes[index],
                "dollar_volume": price * volumes[index],
            }
        )
    return pd.DataFrame(rows)


def test_compute_market_features_returns_columns_and_shape() -> None:
    """Feature frame contains canonical columns and one row per input bar."""
    bars = _bars([100.0 + i for i in range(30)])

    features = compute_market_features(bars, "AAPL")

    assert list(features.columns)[:2] == ["date", "ticker"]
    for column in MARKET_FEATURE_COLUMNS:
        assert column in features.columns
    assert len(features) == len(bars)
    assert (features["ticker"] == "AAPL").all()


def test_compute_market_features_empty_frame_returns_empty_canonical_frame() -> None:
    """Empty input yields an empty frame with canonical columns."""
    empty = pd.DataFrame(
        columns=["date", "open", "high", "low", "close", "adj_close", "volume"]
    )

    features = compute_market_features(empty, "AAPL")

    assert len(features) == 0
    assert list(features.columns)[:2] == ["date", "ticker"]
    for column in MARKET_FEATURE_COLUMNS:
        assert column in features.columns


def test_compute_market_features_rejects_missing_columns() -> None:
    """Missing required columns raise ValueError naming the columns."""
    bars = _bars([100.0, 101.0, 102.0]).drop(columns=["adj_close"])

    with pytest.raises(ValueError, match="adj_close"):
        compute_market_features(bars, "AAPL")


def test_returns_1d_respects_leakage_guard() -> None:
    """returns_1d at row T equals close(T-1)/close(T-2) - 1, not T's close."""
    prices = [100.0, 110.0, 121.0, 133.1, 146.41]
    bars = _bars(prices)

    features = compute_market_features(bars, "AAPL")

    # Day 0 and day 1: not enough history for a shifted return
    assert pd.isna(features.loc[0, "returns_1d"])
    assert pd.isna(features.loc[1, "returns_1d"])
    # Day 2 uses close on day 1 / day 0 - 1 = 0.10
    assert features.loc[2, "returns_1d"] == pytest.approx(0.10)
    # Day 3 uses close on day 2 / day 1 - 1 = 0.10
    assert features.loc[3, "returns_1d"] == pytest.approx(0.10)


def test_overnight_gap_uses_prior_close_and_is_shifted() -> None:
    """overnight_gap at T = open(T-1)/close(T-2) - 1 after leakage shift."""
    frame = pd.DataFrame(
        [
            {
                "date": "2024-01-01",
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 100.0,
                "adj_close": 100.0,
                "volume": 1_000_000,
            },
            {
                "date": "2024-01-02",
                "open": 102.0,
                "high": 103.0,
                "low": 101.0,
                "close": 102.5,
                "adj_close": 102.5,
                "volume": 1_000_000,
            },
            {
                "date": "2024-01-03",
                "open": 105.0,
                "high": 106.0,
                "low": 104.0,
                "close": 105.5,
                "adj_close": 105.5,
                "volume": 1_000_000,
            },
        ]
    )

    features = compute_market_features(frame, "AAPL")

    # Row 2's gap uses row 1's open(102.0) / row 0's close(100.0) - 1 = 0.02
    assert features.loc[2, "overnight_gap"] == pytest.approx(0.02)


def test_volume_ratio_and_rsi_populate_after_enough_history() -> None:
    """Rolling features produce finite values once the window fills."""
    prices = [100.0 + (i % 3) * 5 for i in range(30)]
    bars = _bars(prices, volumes=[1_000_000 + i * 10_000 for i in range(30)])

    features = compute_market_features(bars, "AAPL")

    assert not math.isnan(features.loc[25, "volume_ratio_20"])
    assert not math.isnan(features.loc[25, "rsi_14"])
    assert 0.0 <= features.loc[25, "rsi_14"] <= 100.0


def test_compute_market_features_cross_asset_features_populate_with_benchmark() -> None:
    """Providing SPY bars fills in spy_* and beta_60d columns; without them, NaN."""
    prices = [100.0 + i * 0.5 for i in range(120)]
    bars = _bars(prices)
    spy_prices = [400.0 + i * 0.25 for i in range(120)]
    spy_bars = _bars(spy_prices)

    with_benchmark = compute_market_features(bars, "AAPL", benchmark_bars=spy_bars)
    without_benchmark = compute_market_features(bars, "AAPL")

    assert not math.isnan(with_benchmark.loc[100, "spy_return_1d"])
    assert not math.isnan(with_benchmark.loc[100, "beta_60d"])
    assert math.isnan(without_benchmark.loc[100, "spy_return_1d"])
    assert math.isnan(without_benchmark.loc[100, "beta_60d"])


def test_compute_market_features_sorts_input_and_drops_duplicate_dates() -> None:
    """Unsorted/duplicate input does not corrupt the feature sequence."""
    bars = _bars([100.0, 101.0, 102.0, 103.0])
    shuffled = pd.concat([bars.iloc[[2, 0, 1, 3]], bars.iloc[[0]]], ignore_index=True)

    features = compute_market_features(shuffled, "AAPL")

    dates = features["date"].tolist()
    assert dates == sorted(dates)
    assert len(set(dates)) == len(dates)


def test_market_features_to_records_coerces_nan_to_none() -> None:
    """FeatureRecord output replaces NaN/inf with None and preserves booleans."""
    prices = [100.0 + i for i in range(5)]
    bars = _bars(prices)

    features = compute_market_features(bars, "AAPL")
    records = market_features_to_records(features)

    assert len(records) == len(bars)
    assert records[0].features["returns_1d"] is None
    assert records[0].date == "2024-01-01"
    assert records[0].ticker == "AAPL"


def test_market_features_to_records_produces_validated_records() -> None:
    """All returned records satisfy the FeatureRecord contract."""
    prices = [100.0 + i for i in range(10)]
    bars = _bars(prices)

    features = compute_market_features(bars, "MSFT")
    records = market_features_to_records(features)

    for record in records:
        assert record.ticker == "MSFT"
        assert record.date.startswith("2024-01-")
        for column in MARKET_FEATURE_COLUMNS:
            assert column in record.features
