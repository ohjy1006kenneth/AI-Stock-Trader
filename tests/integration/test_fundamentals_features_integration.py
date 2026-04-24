from __future__ import annotations

import io
import json
from pathlib import Path

import pandas as pd
import pytest

from core.features import (
    compute_fundamentals_features,
    load_fundamentals_frame,
    load_ohlcv_frame,
)
from services.r2.paths import raw_fundamentals_path, raw_price_path
from services.r2.writer import R2Writer


def _write_ohlcv(writer: R2Writer, ticker: str, dates: list[str], prices: list[float]) -> None:
    rows = []
    for date, price in zip(dates, prices, strict=True):
        rows.append(
            {
                "date": date,
                "ticker": ticker,
                "open": price,
                "high": price * 1.01,
                "low": price * 0.99,
                "close": price,
                "adj_close": price,
                "volume": 1_000_000,
                "dollar_volume": price * 1_000_000,
            }
        )
    buffer = io.BytesIO()
    pd.DataFrame(rows).to_parquet(buffer, index=False)
    writer.put_object(raw_price_path(ticker), buffer.getvalue())


def _write_fundamentals(writer: R2Writer, ticker: str, rows: list[dict]) -> None:
    buffer = io.BytesIO()
    pd.DataFrame(rows).to_parquet(buffer, index=False)
    writer.put_object(raw_fundamentals_path(ticker), buffer.getvalue())


def test_fundamentals_features_across_two_earnings_cycles(tmp_path: Path) -> None:
    """Features pick up each new filing at the correct availability date."""
    writer = R2Writer(local_root=tmp_path)
    _write_ohlcv(
        writer,
        "AAPL",
        ["2024-02-05", "2024-05-06", "2024-08-05"],
        [150.0, 175.0, 200.0],
    )
    _write_fundamentals(
        writer,
        "AAPL",
        [
            {
                "source": "simfin",
                "ticker": "AAPL",
                "report_date": "2023-12-31",
                "availability_date": "2024-02-01",
                "retrieved_at": "2024-02-01T00:00:00",
                "fiscal_year": 2023,
                "fiscal_period": "Q4",
                "statement": "pl",
                "earnings_date": "2024-02-01",
                "raw_json": json.dumps({"revenue": 500.0, "netIncome": 50.0}),
            },
            {
                "source": "simfin",
                "ticker": "AAPL",
                "report_date": "2024-03-31",
                "availability_date": "2024-05-03",
                "retrieved_at": "2024-05-03T00:00:00",
                "fiscal_year": 2024,
                "fiscal_period": "Q1",
                "statement": "pl",
                "earnings_date": "2024-05-03",
                "raw_json": json.dumps({"revenue": 1_000.0, "netIncome": 200.0}),
            },
            {
                "source": "simfin",
                "ticker": "AAPL",
                "report_date": "2024-06-30",
                "availability_date": "2024-08-02",
                "retrieved_at": "2024-08-02T00:00:00",
                "fiscal_year": 2024,
                "fiscal_period": "Q2",
                "statement": "pl",
                "earnings_date": "2024-08-01",
                "raw_json": json.dumps({"revenue": 1_200.0, "netIncome": 250.0}),
            },
        ],
    )

    fundamentals = load_fundamentals_frame("AAPL", writer=writer)
    ohlcv = load_ohlcv_frame("AAPL", writer=writer)
    features = compute_fundamentals_features(fundamentals, ohlcv, "AAPL")

    assert len(features) == 3
    assert features.loc[0, "net_profit_margin"] == pytest.approx(50 / 500)
    assert features.loc[1, "net_profit_margin"] == pytest.approx(200 / 1_000)
    assert features.loc[2, "net_profit_margin"] == pytest.approx(250 / 1_200)
    # 2024-08-05 is four days after 2024-08-01 earnings, outside the post window
    assert features.loc[2, "post_earnings_flag"] == 0
