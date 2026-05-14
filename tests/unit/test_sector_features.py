from __future__ import annotations

import math
from collections.abc import Sequence

import pandas as pd
import pytest

from core.features.sector_features import (
    SECTOR_FEATURE_COLUMNS,
    SectorEtfConfig,
    compute_sector_features,
    sector_features_to_records,
)


def _bars(
    prices: Sequence[float | None],
    *,
    start: str = "2024-01-02",
) -> pd.DataFrame:
    """Build a synthetic OHLCV frame with business-day dates."""
    rows: list[dict[str, object]] = []
    for index, day in enumerate(pd.bdate_range(start=start, periods=len(prices))):
        price = prices[index]
        rows.append(
            {
                "date": day.date().isoformat(),
                "open": price,
                "high": price,
                "low": price,
                "close": price,
                "adj_close": price,
                "volume": 1_000_000 + index,
            }
        )
    return pd.DataFrame(rows)


def _fundamentals(sector: str, *, availability_date: str) -> pd.DataFrame:
    """Build a minimal fundamentals archive with one sector label."""
    return pd.DataFrame(
        [
            {
                "report_date": "2023-12-31",
                "availability_date": availability_date,
                "raw_json": f'{{"sector": "{sector}"}}',
            }
        ]
    )


def _config() -> SectorEtfConfig:
    """Return a small deterministic sector ETF config for unit tests."""
    return SectorEtfConfig(
        sector_field_names=("sector",),
        sector_aliases={
            "technology": "information technology",
            "energy": "energy",
        },
        sector_to_etf={
            "information technology": "XLK",
            "energy": "XLE",
        },
    )


def test_compute_sector_features_happy_path_ranks_sector_peers() -> None:
    """Sector ETF, momentum, and peer-relative-strength populate when inputs exist."""
    tickers = {
        "AAPL": _bars([100.0 + index * 1.2 for index in range(90)]),
        "MSFT": _bars([100.0 + index * 0.6 for index in range(90)]),
        "XOM": _bars([100.0 + index * 0.3 for index in range(90)]),
    }
    fundamentals = {
        "AAPL": _fundamentals("Technology", availability_date="2024-01-01"),
        "MSFT": _fundamentals("Technology", availability_date="2024-01-01"),
        "XOM": _fundamentals("Energy", availability_date="2024-01-01"),
    }
    sector_prices = {
        "XLK": _bars([200.0 + index * 0.4 for index in range(90)]),
        "XLE": _bars([150.0 + index * 0.2 for index in range(90)]),
    }

    results = compute_sector_features(
        ohlcv_by_ticker=tickers,
        fundamentals_by_ticker=fundamentals,
        sector_price_frames=sector_prices,
        sector_config=_config(),
    )

    aapl = results["AAPL"].iloc[-1]
    msft = results["MSFT"].iloc[-1]
    xom = results["XOM"].iloc[-1]
    xlk = sector_prices["XLK"]["adj_close"].astype(float)
    expected_sector_return = xlk.iloc[-2] / xlk.iloc[-3] - 1.0
    expected_sector_momentum = xlk.iloc[-2] / xlk.iloc[-23] - 1.0

    assert aapl["sector_etf_ret"] == pytest.approx(expected_sector_return)
    assert aapl["sector_momentum"] == pytest.approx(expected_sector_momentum)
    assert aapl["stock_vs_sector"] > msft["stock_vs_sector"]
    assert aapl["sector_relative_strength"] == pytest.approx(1.0)
    assert msft["sector_relative_strength"] == pytest.approx(0.5)
    assert math.isnan(float(xom["sector_relative_strength"]))


def test_compute_sector_features_missing_mapping_returns_null_features() -> None:
    """Unmapped sectors resolve to null feature values instead of guessed ETF joins."""
    results = compute_sector_features(
        ohlcv_by_ticker={"AAPL": _bars([100.0 + index for index in range(10)])},
        fundamentals_by_ticker={
            "AAPL": _fundamentals("Technology", availability_date="2024-01-01")
        },
        sector_price_frames={"XLK": _bars([200.0 + index for index in range(10)])},
        sector_config=SectorEtfConfig(
            sector_field_names=("sector",),
            sector_aliases={},
            sector_to_etf={"energy": "XLE"},
        ),
    )

    row = results["AAPL"].iloc[-1]
    for column in SECTOR_FEATURE_COLUMNS:
        assert math.isnan(float(row[column]))


def test_compute_sector_features_empty_scope_returns_empty_mapping() -> None:
    """Empty ticker scope produces no sector feature frames."""
    assert compute_sector_features(
        ohlcv_by_ticker={},
        fundamentals_by_ticker={},
        sector_config=_config(),
    ) == {}


def test_sector_features_to_records_coerces_nan_values_to_none() -> None:
    """NaN sector features remain nullable in FeatureRecord output."""
    bars = _bars([100.0, 101.0, None, 103.0, 104.0])
    results = compute_sector_features(
        ohlcv_by_ticker={"AAPL": bars},
        fundamentals_by_ticker={
            "AAPL": _fundamentals("Technology", availability_date="2024-01-01")
        },
        sector_price_frames={"XLK": _bars([200.0, 201.0, 202.0, None, 204.0])},
        sector_config=_config(),
    )

    records = sector_features_to_records(results["AAPL"])

    assert records[-1].features["sector_etf_ret"] is None
    assert records[-1].features["stock_vs_sector"] is None


def test_compute_sector_features_respects_availability_date_and_price_alignment() -> None:
    """Sector assignment starts strictly after availability and uses T-1 ETF returns."""
    dates = _bars([100.0, 110.0, 121.0, 133.1])
    sector_prices = _bars([50.0, 60.0, 72.0, 86.4])
    results = compute_sector_features(
        ohlcv_by_ticker={"AAPL": dates},
        fundamentals_by_ticker={
            "AAPL": _fundamentals("Technology", availability_date="2024-01-04")
        },
        sector_price_frames={"XLK": sector_prices},
        sector_config=_config(),
    )

    third_row = results["AAPL"].iloc[2]
    fourth_row = results["AAPL"].iloc[3]

    assert math.isnan(float(third_row["sector_etf_ret"]))
    assert fourth_row["sector_etf_ret"] == pytest.approx(72.0 / 60.0 - 1.0)
    assert fourth_row["stock_vs_sector"] == pytest.approx((121.0 / 110.0 - 1.0) - 0.2)
