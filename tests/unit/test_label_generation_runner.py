from __future__ import annotations

import io
import json
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
import pytest

from app.lab.data_pipelines.run_label_generation import (
    LABEL_STAGE,
    LabelGenerationConfig,
    _resolve_tickers,
    run_label_generation,
)
from services.r2.client import (
    R2_ACCESS_KEY_ENV,
    R2_BUCKET_ENV,
    R2_ENDPOINT_ENV,
    R2_SECRET_KEY_ENV,
)
from services.r2.paths import pipeline_manifest_path, raw_price_path
from services.r2.writer import R2Writer


def _local_writer(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> R2Writer:
    """Return a local mock R2 writer regardless of developer env files."""
    for name in (R2_ENDPOINT_ENV, R2_ACCESS_KEY_ENV, R2_SECRET_KEY_ENV, R2_BUCKET_ENV):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setattr("services.r2.client.R2_ENV_FILE", tmp_path / "missing-r2.env")
    return R2Writer(local_root=tmp_path)


def _write_synthetic_archive(writer: R2Writer, ticker: str, num_bars: int) -> None:
    """Persist a synthetic OHLCV parquet for one ticker."""
    rows = []
    start = pd.Timestamp("2024-02-01")
    for offset in range(num_bars):
        date = (start + pd.tseries.offsets.BDay(offset)).date().isoformat()
        price = 50.0 + offset
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


def test_run_label_generation_writes_shards_and_manifest(tmp_path, monkeypatch) -> None:
    """The runner persists per-(date, ticker) label shards and a completed manifest."""
    writer = _local_writer(tmp_path, monkeypatch)
    _write_synthetic_archive(writer, "AAPL", num_bars=10)
    config = LabelGenerationConfig(run_id="labels-2024-02-01", tickers=("AAPL",))

    fixed_now = datetime(2024, 2, 15, 12, 0, tzinfo=UTC)
    result = run_label_generation(config, writer=writer, now=fixed_now)

    assert result.shards_written == 10
    assert result.manifest_key == pipeline_manifest_path(LABEL_STAGE, "labels-2024-02-01")

    label_keys = writer.list_keys("labels/layer1/")
    assert len(label_keys) == 10
    manifest_payload = writer.get_object(result.manifest_key)
    payload = json.loads(manifest_payload.decode("utf-8"))
    assert payload["status"] == "completed"
    assert payload["metadata"]["shards_written"] == 10


def test_run_label_generation_writes_failed_manifest_on_error(tmp_path, monkeypatch) -> None:
    """When a ticker is missing the runner records a failed manifest before raising."""
    writer = _local_writer(tmp_path, monkeypatch)
    config = LabelGenerationConfig(run_id="missing-tickers", tickers=("AAPL",))
    fixed_now = datetime(2024, 2, 15, 12, 0, tzinfo=UTC)

    with pytest.raises(FileNotFoundError):
        run_label_generation(config, writer=writer, now=fixed_now)

    manifest_payload = writer.get_object(pipeline_manifest_path(LABEL_STAGE, "missing-tickers"))
    payload = json.loads(manifest_payload.decode("utf-8"))
    assert payload["status"] == "failed"


def test_label_generation_config_rejects_invalid_inputs() -> None:
    """LabelGenerationConfig validates run_id and ticker contents."""
    with pytest.raises(ValueError, match="run_id"):
        LabelGenerationConfig(run_id="", tickers=("AAPL",))
    with pytest.raises(ValueError, match="at least one ticker"):
        LabelGenerationConfig(run_id="run", tickers=())
    with pytest.raises(ValueError, match="tickers cannot contain empty"):
        LabelGenerationConfig(run_id="run", tickers=("AAPL", "  "))


def test_resolve_tickers_supports_inline_and_file_inputs(tmp_path) -> None:
    """The CLI helper accepts inline CSV and @path/to/file.json inputs."""
    inline = _resolve_tickers("aapl, MSFT, googl")
    assert inline == ("AAPL", "MSFT", "GOOGL")

    json_path = tmp_path / "tickers.json"
    json_path.write_text(json.dumps(["spy", "qqq"]), encoding="utf-8")
    file_based = _resolve_tickers(f"@{json_path}")
    assert file_based == ("SPY", "QQQ")


def test_resolve_tickers_rejects_non_array_files(tmp_path) -> None:
    """A ticker JSON file with the wrong shape raises a clear error."""
    bad_path = tmp_path / "bad.json"
    bad_path.write_text(json.dumps({"tickers": ["aapl"]}), encoding="utf-8")
    with pytest.raises(ValueError, match="array"):
        _resolve_tickers(f"@{bad_path}")
