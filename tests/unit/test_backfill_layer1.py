from __future__ import annotations

import io
import json
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
import pytest

from app.lab.data_pipelines.backfill_layer1 import (
    LAYER1_BACKFILL_STAGE,
    Layer1BackfillConfig,
    _resolve_tickers,
    backfill_layer1,
)
from core.features.io import read_feature_records
from services.r2.client import (
    R2_ACCESS_KEY_ENV,
    R2_BUCKET_ENV,
    R2_ENDPOINT_ENV,
    R2_SECRET_KEY_ENV,
)
from services.r2.paths import (
    layer1_ticker_history_path,
    pipeline_manifest_path,
    raw_fundamentals_path,
    raw_macro_path,
    raw_price_path,
)
from services.r2.writer import R2Writer


def _local_writer(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> R2Writer:
    """Return a local mock R2 writer regardless of developer env files."""
    for name in (R2_ENDPOINT_ENV, R2_ACCESS_KEY_ENV, R2_SECRET_KEY_ENV, R2_BUCKET_ENV):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setattr("services.r2.client.R2_ENV_FILE", tmp_path / "missing-r2.env")
    return R2Writer(local_root=tmp_path)


def _write_synthetic_ohlcv(
    writer: R2Writer,
    ticker: str,
    *,
    num_bars: int,
    start: pd.Timestamp = pd.Timestamp("2024-01-02"),
) -> None:
    """Persist synthetic OHLCV bars for one ticker beneath the local mock root."""
    rows = []
    for offset in range(num_bars):
        date = (start + pd.tseries.offsets.BDay(offset)).date().isoformat()
        price = 100.0 + offset
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


def _write_empty_macro_shard(writer: R2Writer) -> None:
    """Persist a single empty macro shard so the macro loader returns a valid frame."""
    frame = pd.DataFrame(
        columns=[
            "source",
            "series_id",
            "observation_date",
            "realtime_start",
            "realtime_end",
            "retrieved_at",
            "value",
            "is_missing",
        ]
    )
    buffer = io.BytesIO()
    frame.to_parquet(buffer, index=False)
    writer.put_object(raw_macro_path("2024-01-02"), buffer.getvalue())


def _write_empty_fundamentals(writer: R2Writer, ticker: str) -> None:
    """Persist an empty fundamentals shard for one ticker."""
    frame = pd.DataFrame(
        columns=[
            "source",
            "ticker",
            "report_date",
            "availability_date",
            "retrieved_at",
            "fiscal_year",
            "fiscal_period",
            "statement",
            "earnings_date",
            "raw_json",
        ]
    )
    buffer = io.BytesIO()
    frame.to_parquet(buffer, index=False)
    writer.put_object(raw_fundamentals_path(ticker), buffer.getvalue())


def test_backfill_layer1_writes_feature_histories_and_manifest(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The backfill writes one feature history per ticker plus a manifest."""
    writer = _local_writer(tmp_path, monkeypatch)
    _write_synthetic_ohlcv(writer, "AAPL", num_bars=30)
    _write_synthetic_ohlcv(writer, "SPY", num_bars=30)
    _write_empty_macro_shard(writer)
    _write_empty_fundamentals(writer, "AAPL")

    config = Layer1BackfillConfig(run_id="layer1-test", tickers=("AAPL",))
    fixed_now = datetime(2024, 2, 15, 12, 0, tzinfo=UTC)
    result = backfill_layer1(config, writer=writer, now=fixed_now)

    assert result.tickers_processed == 1
    assert result.ticker_files_written == 1
    assert result.feature_rows_written > 0
    feature_keys = writer.list_keys("features/layer1/")
    assert feature_keys == [layer1_ticker_history_path("AAPL")]
    loaded_records = read_feature_records("AAPL", writer=writer)
    assert len(loaded_records) == result.feature_rows_written
    manifest_payload = writer.get_object(result.manifest_key)
    payload = json.loads(manifest_payload.decode("utf-8"))
    assert payload["status"] == "completed"
    assert payload["metadata"]["ticker_files_written"] == 1
    assert payload["metadata"]["feature_rows_written"] == result.feature_rows_written
    assert payload["metadata"]["benchmark_ticker"] == "SPY"


def test_backfill_layer1_skips_tickers_with_no_ohlcv(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A ticker without an OHLCV archive is skipped without aborting the run."""
    writer = _local_writer(tmp_path, monkeypatch)
    _write_synthetic_ohlcv(writer, "SPY", num_bars=30)
    _write_empty_macro_shard(writer)

    config = Layer1BackfillConfig(run_id="layer1-skip", tickers=("AAPL",))
    fixed_now = datetime(2024, 2, 15, 12, 0, tzinfo=UTC)
    result = backfill_layer1(config, writer=writer, now=fixed_now)

    assert result.ticker_files_written == 0
    assert result.feature_rows_written == 0
    manifest_payload = writer.get_object(
        pipeline_manifest_path(LAYER1_BACKFILL_STAGE, "layer1-skip"),
    )
    payload = json.loads(manifest_payload.decode("utf-8"))
    assert payload["status"] == "completed"


def test_backfill_layer1_writes_failed_manifest_on_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failure during processing still emits a failed manifest before raising."""
    writer = _local_writer(tmp_path, monkeypatch)
    _write_synthetic_ohlcv(writer, "AAPL", num_bars=5)
    _write_empty_macro_shard(writer)
    _write_empty_fundamentals(writer, "AAPL")

    def _exploding_market_features(*args, **kwargs):
        raise RuntimeError("simulated market-feature failure")

    monkeypatch.setattr(
        "app.lab.data_pipelines.backfill_layer1.compute_market_features",
        _exploding_market_features,
    )

    config = Layer1BackfillConfig(run_id="layer1-fail", tickers=("AAPL",))
    fixed_now = datetime(2024, 2, 15, 12, 0, tzinfo=UTC)

    with pytest.raises(RuntimeError, match="simulated market-feature failure"):
        backfill_layer1(config, writer=writer, now=fixed_now)

    manifest_payload = writer.get_object(
        pipeline_manifest_path(LAYER1_BACKFILL_STAGE, "layer1-fail"),
    )
    payload = json.loads(manifest_payload.decode("utf-8"))
    assert payload["status"] == "failed"


def test_layer1_backfill_config_rejects_invalid_inputs() -> None:
    """Layer1BackfillConfig validates run_id and ticker contents."""
    with pytest.raises(ValueError, match="run_id"):
        Layer1BackfillConfig(run_id="", tickers=("AAPL",))
    with pytest.raises(ValueError, match="at least one ticker"):
        Layer1BackfillConfig(run_id="run", tickers=())
    with pytest.raises(ValueError, match="tickers cannot contain empty"):
        Layer1BackfillConfig(run_id="run", tickers=("AAPL", "  "))


def test_resolve_tickers_supports_inline_and_file_inputs(tmp_path: Path) -> None:
    """The CLI helper accepts inline CSV and @path/to/file.json inputs."""
    inline = _resolve_tickers("aapl, MSFT, googl")
    assert inline == ("AAPL", "MSFT", "GOOGL")

    json_path = tmp_path / "tickers.json"
    json_path.write_text(json.dumps(["spy", "qqq"]), encoding="utf-8")
    file_based = _resolve_tickers(f"@{json_path}")
    assert file_based == ("SPY", "QQQ")
