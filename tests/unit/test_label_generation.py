from __future__ import annotations

import io
import json
import math
from pathlib import Path

import pandas as pd
import pytest

from core.contracts.schemas import FeatureRecord
from core.labels import (
    LABEL_FEATURE_COLUMNS,
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
from services.r2.paths import layer1_label_path
from services.r2.writer import R2Writer


def _local_writer(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> R2Writer:
    """Return a local mock R2 writer regardless of developer env files."""
    for name in (R2_ENDPOINT_ENV, R2_ACCESS_KEY_ENV, R2_SECRET_KEY_ENV, R2_BUCKET_ENV):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setattr("services.r2.client.R2_ENV_FILE", tmp_path / "missing-r2.env")
    return R2Writer(local_root=tmp_path)


def _ohlcv_frame(dates: list[str], prices: list[float]) -> pd.DataFrame:
    """Build a minimal OHLCV frame with date and adj_close (others ignored by labels)."""
    return pd.DataFrame({"date": dates, "adj_close": prices})


def test_compute_forward_return_labels_returns_canonical_columns_and_shape() -> None:
    """The labels frame contains the canonical columns and one row per input bar."""
    ohlcv = _ohlcv_frame(
        [f"2024-01-{i + 1:02d}" for i in range(30)],
        [100.0 + i for i in range(30)],
    )

    labels = compute_forward_return_labels(ohlcv, "AAPL")

    assert list(labels.columns)[:2] == ["date", "ticker"]
    for column in LABEL_FEATURE_COLUMNS:
        assert column in labels.columns
    assert len(labels) == len(ohlcv)
    assert (labels["ticker"] == "AAPL").all()


def test_forward_returns_compute_against_known_prices() -> None:
    """Forward returns equal adj_close(T+H)/adj_close(T) - 1 for each horizon."""
    prices = [100.0, 110.0, 121.0, 133.1, 146.41, 161.05]
    ohlcv = _ohlcv_frame([f"2024-01-{i + 1:02d}" for i in range(len(prices))], prices)

    labels = compute_forward_return_labels(ohlcv, "AAPL")

    assert labels.loc[0, "forward_return_1d"] == pytest.approx(0.10)
    assert labels.loc[0, "forward_return_5d"] == pytest.approx((161.05 / 100.0) - 1.0)
    assert labels.loc[0, "forward_log_return_1d"] == pytest.approx(math.log(110.0 / 100.0))


def test_end_of_history_emits_none_returns_and_zero_survival_flag() -> None:
    """A row whose forward window extends past the archive end is None + survives=0."""
    ohlcv = _ohlcv_frame(
        [f"2024-01-{i + 1:02d}" for i in range(3)],
        [100.0, 101.0, 102.0],
    )

    labels = compute_forward_return_labels(ohlcv, "AAPL")

    # Last row has no T+1 observation
    assert pd.isna(labels.loc[2, "forward_return_1d"])
    assert labels.loc[2, "survives_to_t1"] == 0
    # Every row lacks T+5 because the archive only spans 3 days
    assert (labels["survives_to_t5"] == 0).all()


def test_compute_forward_return_labels_rejects_missing_columns() -> None:
    """OHLCV frames lacking required columns raise ValueError naming them."""
    ohlcv = pd.DataFrame({"adj_close": [100.0, 101.0]})

    with pytest.raises(ValueError, match="date"):
        compute_forward_return_labels(ohlcv, "AAPL")


def test_compute_forward_return_labels_handles_empty_frame() -> None:
    """Empty input yields an empty canonical frame."""
    empty = pd.DataFrame(columns=["date", "adj_close"])

    labels = compute_forward_return_labels(empty, "AAPL")

    assert len(labels) == 0
    assert list(labels.columns)[:2] == ["date", "ticker"]
    for column in LABEL_FEATURE_COLUMNS:
        assert column in labels.columns


def test_compute_forward_return_labels_sorts_input_and_drops_duplicates() -> None:
    """Unsorted/duplicate input does not corrupt the forward-return alignment."""
    ohlcv = _ohlcv_frame(
        ["2024-01-03", "2024-01-01", "2024-01-02", "2024-01-01"],
        [102.0, 100.0, 101.0, 100.0],
    )

    labels = compute_forward_return_labels(ohlcv, "AAPL")

    dates = labels["date"].tolist()
    assert dates == sorted(set(dates))
    assert labels.loc[0, "forward_return_1d"] == pytest.approx(0.01)


def test_forward_return_labels_to_records_coerces_nan_to_none() -> None:
    """Last-row NaN returns convert to None in the FeatureRecord output."""
    ohlcv = _ohlcv_frame(["2024-01-01", "2024-01-02"], [100.0, 105.0])

    labels = compute_forward_return_labels(ohlcv, "AAPL")
    records = forward_return_labels_to_records(labels)

    assert len(records) == 2
    assert records[0].features["forward_return_1d"] == pytest.approx(0.05)
    assert records[1].features["forward_return_1d"] is None
    # Survival flags are integers, not None
    assert records[0].features["survives_to_t1"] == 1
    assert records[1].features["survives_to_t1"] == 0


def test_label_record_round_trips_through_local_r2(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Label shards round-trip through the local R2 mock at the canonical path."""
    writer = _local_writer(tmp_path, monkeypatch)
    record = FeatureRecord(
        date="2024-01-02",
        ticker="AAPL",
        features={
            "forward_return_1d": 0.0125,
            "forward_log_return_1d": 0.0124,
            "survives_to_t1": 1,
            "forward_return_5d": None,
            "survives_to_t5": 0,
        },
    )

    key = write_label_record(record, writer=writer)
    loaded = read_label_record("2024-01-02", "AAPL", writer=writer)

    assert key == "labels/layer1/2024-01-02/AAPL.parquet"
    assert key == layer1_label_path("2024-01-02", "AAPL")
    assert loaded == record


def test_read_label_record_rejects_multi_row_archives(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A label shard containing more than one row raises ValueError on read."""
    writer = _local_writer(tmp_path, monkeypatch)
    frame = pd.DataFrame(
        [
            {"date": "2024-01-02", "ticker": "AAPL", "features": json.dumps({})},
            {"date": "2024-01-02", "ticker": "MSFT", "features": json.dumps({})},
        ]
    )
    buffer = io.BytesIO()
    frame.to_parquet(buffer, index=False)
    writer.put_object(layer1_label_path("2024-01-02", "AAPL"), buffer.getvalue())

    with pytest.raises(ValueError, match="exactly one record"):
        read_label_record("2024-01-02", "AAPL", writer=writer)
