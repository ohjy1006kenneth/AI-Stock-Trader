from __future__ import annotations

import io
import json
from pathlib import Path

import pandas as pd
import pytest

from core.features import (
    compute_context_features,
    context_features_to_records,
    load_fundamentals_frame,
    load_macro_frame,
    load_ohlcv_frame,
)
from services.r2 import client as r2_client
from services.r2.paths import raw_fundamentals_path, raw_macro_path, raw_price_path
from services.r2.writer import R2Writer


def _write_parquet(writer: R2Writer, key: str, rows: list[dict[str, object]]) -> None:
    """Persist rows as a Parquet object in the local R2 mock."""
    buffer = io.BytesIO()
    pd.DataFrame(rows).to_parquet(buffer, index=False)
    writer.put_object(key, buffer.getvalue())


def test_context_features_round_trip_through_local_r2(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Context feature composition works from Layer 0 R2 archives to FeatureRecords."""
    for env_name in r2_client.REQUIRED_R2_ENV_VARS:
        monkeypatch.delenv(env_name, raising=False)
    monkeypatch.setattr(r2_client, "R2_ENV_FILE", tmp_path / "missing-r2.env")

    writer = R2Writer(local_root=tmp_path)
    _write_parquet(
        writer,
        raw_price_path("AAPL"),
        [
            {
                "date": "2024-05-03",
                "ticker": "AAPL",
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 100.0,
                "adj_close": 100.0,
                "volume": 1_000_000,
                "dollar_volume": 100_000_000.0,
            },
            {
                "date": "2024-05-06",
                "ticker": "AAPL",
                "open": 101.0,
                "high": 102.0,
                "low": 100.0,
                "close": 101.0,
                "adj_close": 101.0,
                "volume": 1_000_000,
                "dollar_volume": 101_000_000.0,
            },
        ],
    )
    _write_parquet(
        writer,
        raw_fundamentals_path("AAPL"),
        [
            {
                "source": "simfin",
                "ticker": "AAPL",
                "report_date": "2024-03-31",
                "availability_date": "2024-05-03",
                "retrieved_at": "2024-05-03T00:00:00",
                "fiscal_year": 2024,
                "fiscal_period": "Q1",
                "statement": "pl",
                "earnings_date": "2024-05-10",
                "raw_json": json.dumps({"revenue": 1_000.0, "netIncome": 100.0}),
            }
        ],
    )
    _write_parquet(
        writer,
        raw_macro_path("2024-05-03"),
        [
            {
                "source": "fred",
                "series_id": "DGS10",
                "observation_date": "2024-05-03",
                "realtime_start": "2024-05-03",
                "realtime_end": "2024-05-03",
                "retrieved_at": "2024-05-03T00:00:00+00:00",
                "value": 4.5,
                "is_missing": False,
                "raw": {"fixture": True},
            }
        ],
    )

    features = compute_context_features(
        load_fundamentals_frame("AAPL", writer=writer),
        load_ohlcv_frame("AAPL", writer=writer),
        load_macro_frame(writer=writer),
        "AAPL",
    )
    records = context_features_to_records(features)

    assert len(records) == 2
    assert records[1].features["net_profit_margin"] == pytest.approx(0.1)
    assert records[1].features["days_to_next_earnings"] == 4
    assert records[1].features["treasury_10y"] == pytest.approx(4.5)
