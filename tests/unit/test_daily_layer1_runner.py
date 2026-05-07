from __future__ import annotations

import csv
import io
import json
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
import pytest

from app.lab.data_pipelines.run_daily_layer1 import (
    LAYER1_STAGE,
    DailyLayer1PipelineConfig,
    load_modal_runtime_config,
    run_daily_layer1,
)
from core.contracts.schemas import PipelineManifestRecord, RunStatus
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
    raw_universe_path,
)
from services.r2.writer import R2Writer


def test_run_daily_layer1_reads_layer0_inputs_and_writes_manifest(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The daily Layer 1 runner validates Layer 0, writes ticker histories, and emits a manifest."""
    writer = _local_writer(tmp_path, monkeypatch)
    _write_layer0_manifest(writer, run_id="layer0-daily-2024-01-02", as_of_date="2024-01-02")
    _write_universe(
        writer,
        raw_universe_path("2024-01-02"),
        [
            {"date": "2024-01-02", "ticker": "AAPL", "in_universe": "True"},
            {"date": "2024-01-02", "ticker": "MSFT", "in_universe": "False"},
        ],
    )
    _write_synthetic_ohlcv(writer, "AAPL", num_bars=30)
    _write_synthetic_ohlcv(writer, "SPY", num_bars=30)
    _write_empty_macro_shard(writer)
    _write_empty_fundamentals(writer, "AAPL")

    result = run_daily_layer1(
        DailyLayer1PipelineConfig(
            run_id="layer1-daily-2024-01-02",
            as_of_date="2024-01-02",
            layer0_run_id="layer0-daily-2024-01-02",
        ),
        writer=writer,
    )

    manifest = json.loads(writer.get_object(result.manifest_key))
    assert result.manifest_key == pipeline_manifest_path(LAYER1_STAGE, "layer1-daily-2024-01-02")
    assert manifest["status"] == RunStatus.COMPLETED
    assert manifest["metadata"]["as_of_date"] == "2024-01-02"
    assert manifest["metadata"]["layer0_run_id"] == "layer0-daily-2024-01-02"
    assert manifest["metadata"]["tickers_requested"] == 1
    assert manifest["metadata"]["tickers_processed"] == 1
    assert layer1_ticker_history_path("AAPL") in writer.list_keys("features/layer1/")


def test_run_daily_layer1_writes_failed_manifest_when_layer0_manifest_is_missing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The runner fails closed and emits a failed manifest when upstream Layer 0 is missing."""
    writer = _local_writer(tmp_path, monkeypatch)

    with pytest.raises(FileNotFoundError):
        run_daily_layer1(
            DailyLayer1PipelineConfig(
                run_id="layer1-daily-2024-01-02",
                as_of_date="2024-01-02",
                layer0_run_id="layer0-daily-2024-01-02",
            ),
            writer=writer,
        )

    manifest = json.loads(
        writer.get_object(pipeline_manifest_path(LAYER1_STAGE, "layer1-daily-2024-01-02"))
    )
    assert manifest["status"] == RunStatus.FAILED
    assert manifest["metadata"]["error"]["type"] == "FileNotFoundError"


def test_run_daily_layer1_writes_failed_manifest_when_universe_has_no_eligible_tickers(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An empty eligible universe fails closed instead of running a stale Layer 1 job."""
    writer = _local_writer(tmp_path, monkeypatch)
    _write_layer0_manifest(writer, run_id="layer0-daily-2024-01-02", as_of_date="2024-01-02")
    _write_universe(
        writer,
        raw_universe_path("2024-01-02"),
        [{"date": "2024-01-02", "ticker": "AAPL", "in_universe": "False"}],
    )

    with pytest.raises(ValueError, match="No eligible universe tickers"):
        run_daily_layer1(
            DailyLayer1PipelineConfig(
                run_id="layer1-daily-2024-01-02",
                as_of_date="2024-01-02",
                layer0_run_id="layer0-daily-2024-01-02",
            ),
            writer=writer,
        )

    manifest = json.loads(
        writer.get_object(pipeline_manifest_path(LAYER1_STAGE, "layer1-daily-2024-01-02"))
    )
    assert manifest["status"] == RunStatus.FAILED
    assert manifest["metadata"]["error"]["type"] == "ValueError"


def test_run_daily_layer1_writes_failed_manifest_when_universe_is_missing_ticker_column(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Malformed universe shards fail closed and leave an error manifest behind."""
    writer = _local_writer(tmp_path, monkeypatch)
    _write_layer0_manifest(writer, run_id="layer0-daily-2024-01-02", as_of_date="2024-01-02")
    writer.put_object(
        raw_universe_path("2024-01-02"),
        "date,in_universe,tradable,liquid,halted,data_quality_ok\n"
        "2024-01-02,True,True,True,False,True\n",
    )

    with pytest.raises(KeyError, match="ticker"):
        run_daily_layer1(
            DailyLayer1PipelineConfig(
                run_id="layer1-daily-2024-01-02",
                as_of_date="2024-01-02",
                layer0_run_id="layer0-daily-2024-01-02",
            ),
            writer=writer,
        )

    manifest = json.loads(
        writer.get_object(pipeline_manifest_path(LAYER1_STAGE, "layer1-daily-2024-01-02"))
    )
    assert manifest["status"] == RunStatus.FAILED
    assert manifest["metadata"]["error"]["type"] == "KeyError"


def test_load_modal_runtime_config_reads_repo_config() -> None:
    """The daily Layer 1 Modal app name lives in repository config."""
    config = load_modal_runtime_config()

    assert config.app_name
    assert config.r2_secret_name
    assert config.timeout_seconds > 0


def _local_writer(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> R2Writer:
    """Return a local mock R2 writer regardless of developer env files."""
    for name in (R2_ENDPOINT_ENV, R2_ACCESS_KEY_ENV, R2_SECRET_KEY_ENV, R2_BUCKET_ENV):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setattr("services.r2.client.R2_ENV_FILE", tmp_path / "missing-r2.env")
    return R2Writer(local_root=tmp_path)


def _write_layer0_manifest(writer: R2Writer, *, run_id: str, as_of_date: str) -> None:
    """Persist a completed upstream Layer 0 manifest for one daily run."""
    manifest = PipelineManifestRecord(
        run_id=run_id,
        stage="layer0",
        status=RunStatus.COMPLETED,
        started_at=datetime(2024, 1, 2, tzinfo=UTC),
        finished_at=datetime(2024, 1, 2, 0, 0, 1, tzinfo=UTC),
        output_path="raw/",
        metadata={
            "mode": "daily_incremental",
            "from_date": as_of_date,
            "to_date": as_of_date,
        },
    )
    writer.put_object(pipeline_manifest_path("layer0", run_id), manifest.model_dump_json())


def _write_universe(writer: R2Writer, key: str, rows: list[dict[str, object]]) -> None:
    """Write a Layer 0 universe CSV shard into the mock object store."""
    fieldnames = [
        "date",
        "ticker",
        "in_universe",
        "tradable",
        "liquid",
        "halted",
        "data_quality_ok",
        "reason",
    ]
    buffer = io.StringIO()
    csv_writer = csv.DictWriter(buffer, fieldnames=fieldnames)
    csv_writer.writeheader()
    for row in rows:
        csv_writer.writerow(
            {
                "tradable": "True",
                "liquid": "True",
                "halted": "False",
                "data_quality_ok": "True",
                "reason": "",
                **row,
            }
        )
    writer.put_object(key, buffer.getvalue())


def _write_synthetic_ohlcv(
    writer: R2Writer,
    ticker: str,
    *,
    num_bars: int,
    start: pd.Timestamp = pd.Timestamp("2024-01-02"),
) -> None:
    """Persist synthetic OHLCV bars for one ticker beneath the local mock root."""
    rows: list[dict[str, object]] = []
    for offset in range(num_bars):
        day = (start + pd.tseries.offsets.BDay(offset)).date().isoformat()
        price = 100.0 + offset
        rows.append(
            {
                "date": day,
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
