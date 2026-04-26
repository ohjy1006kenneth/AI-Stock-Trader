from __future__ import annotations

import io
from pathlib import Path

import pandas as pd
import pytest

from core.features import load_ohlcv_frame
from core.labels import (
    compute_forward_return_labels,
    forward_return_labels_to_records,
    read_label_record,
    write_label_record,
)
from services.r2.client import (
    R2_ACCESS_KEY_ENV,
    R2_BUCKET_ENV,
    R2_ENDPOINT_ENV,
    R2_SECRET_KEY_ENV,
)
from services.r2.paths import raw_price_path
from services.r2.writer import R2Writer


def _local_writer(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> R2Writer:
    """Return a local mock R2 writer regardless of developer env files."""
    for name in (R2_ENDPOINT_ENV, R2_ACCESS_KEY_ENV, R2_SECRET_KEY_ENV, R2_BUCKET_ENV):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setattr("services.r2.client.R2_ENV_FILE", tmp_path / "missing-r2.env")
    return R2Writer(local_root=tmp_path)


def _write_synthetic_archive(writer: R2Writer, ticker: str, num_bars: int) -> None:
    """Persist a synthetic OHLCV parquet for one ticker beneath the local mock R2 root."""
    rows = []
    start = pd.Timestamp("2024-01-02")
    for offset in range(num_bars):
        date = (start + pd.tseries.offsets.BDay(offset)).date().isoformat()
        price = 100.0 * (1.0 + offset * 0.01)
        rows.append(
            {
                "date": date,
                "ticker": ticker,
                "open": price,
                "high": price * 1.01,
                "low": price * 0.99,
                "close": price,
                "adj_close": price,
                "volume": 1_000_000 + offset,
                "dollar_volume": price * (1_000_000 + offset),
            }
        )
    buffer = io.BytesIO()
    pd.DataFrame(rows).to_parquet(buffer, index=False)
    writer.put_object(raw_price_path(ticker), buffer.getvalue())


def test_label_generation_end_to_end_through_local_r2(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Labels round-trip from the OHLCV archive into a label-shard archive."""
    writer = _local_writer(tmp_path, monkeypatch)
    _write_synthetic_archive(writer, "AAPL", num_bars=30)

    ohlcv = load_ohlcv_frame("AAPL", writer=writer)
    labels = compute_forward_return_labels(ohlcv, "AAPL")
    records = forward_return_labels_to_records(labels)

    assert len(records) == 30
    assert records[0].features["survives_to_t20"] == 1
    # The last row has no future bars at all — survives_to_t1/5/20 are 0
    assert records[-1].features["survives_to_t1"] == 0
    assert records[-1].features["survives_to_t20"] == 0

    # Persist a representative shard and read it back
    target = records[0]
    key = write_label_record(target, writer=writer)
    assert key.startswith("labels/layer1/")

    loaded = read_label_record(target.date, target.ticker, writer=writer)
    assert loaded == target
