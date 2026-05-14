from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from core.features.order_book_features import (
    ORDER_BOOK_FEATURE_COLUMNS,
    compute_order_book_features,
    order_book_features_to_records,
)
from services.order_book.config import OrderBookFeatureConfig, load_order_book_feature_config


def _order_book_rows() -> pd.DataFrame:
    """Return a minimal normalized order-book archive for one trade date."""
    return pd.DataFrame(
        [
            {
                "date": "2024-01-03",
                "ticker": "AAPL",
                "captured_at": "2024-01-03T14:15:00+00:00",
                "bid_price": 100.00,
                "ask_price": 100.05,
                "bid_size": 600,
                "ask_size": 400,
            },
            {
                "date": "2024-01-03",
                "ticker": "AAPL",
                "captured_at": "2024-01-03T14:25:00+00:00",
                "bid_price": 100.01,
                "ask_price": 100.06,
                "bid_size": 700,
                "ask_size": 300,
            },
            {
                "date": "2024-01-03",
                "ticker": "MSFT",
                "captured_at": "2024-01-03T14:20:00+00:00",
                "bid_price": 200.00,
                "ask_price": 200.10,
                "bid_size": 500,
                "ask_size": 500,
            },
        ]
    )


def test_compute_order_book_features_aggregates_latest_pre_open_snapshot() -> None:
    """Each ticker emits one daily row using the latest valid pre-open snapshot."""
    features = compute_order_book_features(
        _order_book_rows(),
        target_date="2024-01-03",
        tickers=("AAPL", "MSFT"),
    )

    assert list(features.columns) == ["date", "ticker", *ORDER_BOOK_FEATURE_COLUMNS]
    aapl = features.loc[features["ticker"] == "AAPL"].iloc[0]
    assert aapl["l2_bid_ask_spread"] == pytest.approx(0.05)
    assert aapl["l2_quoted_spread_bps"] == pytest.approx(4.99875, rel=1e-4)
    assert aapl["l2_book_imbalance"] == pytest.approx(0.4)
    assert aapl["l2_snapshot_count"] == 2


def test_compute_order_book_features_empty_frame_returns_null_rows() -> None:
    """Missing archives produce explicit null feature rows without failing assembly."""
    empty = pd.DataFrame(
        columns=["date", "ticker", "captured_at", "bid_price", "ask_price", "bid_size", "ask_size"]
    )

    features = compute_order_book_features(
        empty,
        target_date="2024-01-03",
        tickers=("AAPL",),
    )

    row = features.iloc[0]
    assert row["ticker"] == "AAPL"
    assert pd.isna(row["l2_bid_ask_spread"])
    assert pd.isna(row["l2_quoted_spread_bps"])
    assert pd.isna(row["l2_book_imbalance"])
    assert row["l2_snapshot_count"] == 0


def test_compute_order_book_features_rejects_missing_columns() -> None:
    """The normalized archive contract must include the required provider columns."""
    rows = _order_book_rows().drop(columns=["ask_size"])

    with pytest.raises(ValueError, match="ask_size"):
        compute_order_book_features(rows, target_date="2024-01-03", tickers=("AAPL",))


def test_compute_order_book_features_ignores_invalid_and_post_open_rows() -> None:
    """Bad quotes and post-open rows are dropped instead of breaking the branch."""
    rows = pd.DataFrame(
        [
            {
                "date": "2024-01-03",
                "ticker": "AAPL",
                "captured_at": "2024-01-03T14:31:00+00:00",
                "bid_price": 100.00,
                "ask_price": 100.05,
                "bid_size": 600,
                "ask_size": 400,
            },
            {
                "date": "2024-01-03",
                "ticker": "AAPL",
                "captured_at": "2024-01-03T14:20:00+00:00",
                "bid_price": 100.00,
                "ask_price": float("nan"),
                "bid_size": 600,
                "ask_size": 400,
            },
        ]
    )

    features = compute_order_book_features(
        rows,
        target_date="2024-01-03",
        tickers=("AAPL",),
    )

    row = features.iloc[0]
    assert pd.isna(row["l2_bid_ask_spread"])
    assert row["l2_snapshot_count"] == 0


def test_order_book_features_to_records_coerces_nan_to_none() -> None:
    """FeatureRecord serialization keeps missing order-book values contract-safe."""
    features = compute_order_book_features(
        pd.DataFrame(
            columns=[
                "date",
                "ticker",
                "captured_at",
                "bid_price",
                "ask_price",
                "bid_size",
                "ask_size",
            ]
        ),
        target_date="2024-01-03",
        tickers=("AAPL",),
    )

    records = order_book_features_to_records(features)

    assert len(records) == 1
    assert records[0].date == "2024-01-03"
    assert records[0].ticker == "AAPL"
    assert records[0].features["l2_bid_ask_spread"] is None
    assert records[0].features["l2_snapshot_count"] == 0


def test_load_order_book_feature_config_reads_repo_owned_gate(tmp_path: Path) -> None:
    """The optional branch is controlled by repository config rather than code defaults."""
    path = tmp_path / "order_book_features.json"
    path.write_text(
        json.dumps({"enabled": True, "provider": "alpaca"}),
        encoding="utf-8",
    )

    config = load_order_book_feature_config(path)

    assert config == OrderBookFeatureConfig(enabled=True, provider="alpaca")
    assert config.is_active is True
