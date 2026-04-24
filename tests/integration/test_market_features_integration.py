from __future__ import annotations

import io
import math
from pathlib import Path

import pandas as pd

from core.features import compute_market_features, load_ohlcv_frame
from services.r2.paths import raw_price_path
from services.r2.writer import R2Writer


def _write_synthetic_archive(writer: R2Writer, ticker: str, num_bars: int) -> None:
    """Persist a synthetic OHLCV parquet shard for one ticker under the mock R2 root."""
    rows = []
    start = pd.Timestamp("2023-01-02")
    for offset in range(num_bars):
        date = (start + pd.tseries.offsets.BDay(offset)).date().isoformat()
        price = 100.0 + offset * 0.25
        rows.append(
            {
                "date": date,
                "ticker": ticker,
                "open": price,
                "high": price * 1.01,
                "low": price * 0.99,
                "close": price,
                "adj_close": price,
                "volume": 1_000_000 + offset * 100,
                "dollar_volume": (price) * (1_000_000 + offset * 100),
            }
        )
    buffer = io.BytesIO()
    pd.DataFrame(rows).to_parquet(buffer, index=False)
    writer.put_object(raw_price_path(ticker), buffer.getvalue())


def test_market_features_round_trip_through_local_r2(tmp_path: Path) -> None:
    """Feature computation works end-to-end against a local R2 mock archive."""
    writer = R2Writer(local_root=tmp_path)
    _write_synthetic_archive(writer, "AAPL", num_bars=220)
    _write_synthetic_archive(writer, "SPY", num_bars=220)

    bars = load_ohlcv_frame("AAPL", writer=writer)
    spy_bars = load_ohlcv_frame("SPY", writer=writer)
    features = compute_market_features(bars, "AAPL", benchmark_bars=spy_bars)

    assert len(features) == 220
    last_row = features.iloc[-1]
    assert last_row["ticker"] == "AAPL"
    assert not math.isnan(last_row["returns_21d"])
    assert not math.isnan(last_row["realized_vol_21d"])
    assert not math.isnan(last_row["spy_return_5d"])
    assert not math.isnan(last_row["beta_60d"])
