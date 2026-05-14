from __future__ import annotations

import argparse
import io
import json
from pathlib import Path

import pandas as pd

from app.lab.data_pipelines.run_hmm_regime_detection import (
    REGIME_STAGE,
    HMMRegimePipelineConfig,
    _config_from_args,
    hmm_regime_output_path,
    load_modal_runtime_config,
    run_hmm_regime_detection,
)
from core.contracts.schemas import RunStatus
from services.r2.client import (
    R2_ACCESS_KEY_ENV,
    R2_BUCKET_ENV,
    R2_ENDPOINT_ENV,
    R2_SECRET_KEY_ENV,
)
from services.r2.paths import pipeline_manifest_path, raw_macro_path, raw_price_path
from services.r2.writer import R2Writer


def test_run_hmm_regime_detection_reads_r2_and_writes_outputs(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """The lab runner reads R2 archives and writes regime parquet plus manifest."""
    writer = _local_writer(tmp_path, monkeypatch)
    bars = _benchmark_bars(130)
    macro = _macro_archive(150)
    _write_parquet(writer, raw_price_path("SPY"), bars)
    _write_parquet(writer, raw_macro_path("2023-01-02"), macro)
    train_end_date = str(bars.loc[95, "date"])
    inference_dates = (str(bars.loc[105, "date"]), str(bars.loc[106, "date"]))

    result = run_hmm_regime_detection(
        HMMRegimePipelineConfig(
            run_id="hmm-test-run",
            train_end_date=train_end_date,
            inference_dates=inference_dates,
            min_training_rows=20,
            max_iterations=20,
        ),
        writer=writer,
    )

    output = pd.read_parquet(io.BytesIO(writer.get_object(result.output_key)))
    manifest = json.loads(writer.get_object(result.manifest_key))

    assert result.output_key == hmm_regime_output_path("hmm-test-run")
    assert result.manifest_key == pipeline_manifest_path(REGIME_STAGE, "hmm-test-run")
    assert output["date"].tolist() == list(inference_dates)
    assert output["regime_confidence"].notna().all()
    assert manifest["status"] == RunStatus.COMPLETED
    assert manifest["stage"] == REGIME_STAGE
    assert manifest["output_path"] == result.output_key
    assert manifest["metadata"]["regime_rows"] == len(inference_dates)


def test_run_hmm_regime_detection_writes_failure_manifest(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """The lab runner writes a failed manifest when required R2 inputs are missing."""
    writer = _local_writer(tmp_path, monkeypatch)

    try:
        run_hmm_regime_detection(
            HMMRegimePipelineConfig(
                run_id="hmm-fail-run",
                train_end_date="2023-05-01",
                inference_dates=("2023-05-02",),
            ),
            writer=writer,
        )
    except FileNotFoundError:
        pass
    else:
        assert False, "Expected FileNotFoundError for missing SPY archive"

    manifest = json.loads(writer.get_object(pipeline_manifest_path(REGIME_STAGE, "hmm-fail-run")))
    assert manifest["status"] == RunStatus.FAILED
    assert manifest["metadata"]["error"]["type"] == "FileNotFoundError"


def test_run_hmm_regime_detection_emits_warning_rows_for_short_history(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """Short history writes explicit null regime rows instead of failing the stage."""
    writer = _local_writer(tmp_path, monkeypatch)
    bars = _benchmark_bars(35)
    macro = _macro_archive(60)
    _write_parquet(writer, raw_price_path("SPY"), bars)
    _write_parquet(writer, raw_macro_path("2023-01-02"), macro)

    result = run_hmm_regime_detection(
        HMMRegimePipelineConfig(
            run_id="hmm-short-history",
            train_end_date=str(bars.loc[20, "date"]),
            inference_dates=(str(bars.loc[25, "date"]),),
            min_training_rows=30,
            max_iterations=20,
        ),
        writer=writer,
    )

    output = pd.read_parquet(io.BytesIO(writer.get_object(result.output_key)))
    manifest = json.loads(writer.get_object(result.manifest_key))

    assert output.loc[0, "regime_label"] is None or pd.isna(output.loc[0, "regime_label"])
    assert pd.isna(output.loc[0, "regime_confidence"])
    assert output.loc[0, "regime_readiness_status"] == "warning"
    assert output.loc[0, "regime_readiness_reason"] == "insufficient_training_history"
    assert manifest["status"] == RunStatus.COMPLETED
    assert manifest["metadata"]["regime_layer2_ready"] is False


def test_load_modal_runtime_config_reads_repo_config() -> None:
    """Modal app and secret names live in config rather than code constants."""
    config = load_modal_runtime_config()

    assert config.hmm_regime_app_name
    assert config.r2_secret_name
    assert config.timeout_seconds > 0
    assert config.python_version == "3.11"
    assert config.requirements_path == "requirements/modal.txt"


def test_config_from_args_normalizes_benchmark_ticker() -> None:
    """CLI parsing normalizes benchmark tickers before building the pipeline config."""
    config = _config_from_args(
        argparse.Namespace(
            run_id="hmm-cli",
            train_start_date="2024-01-02",
            train_end_date="2024-01-31",
            inference_date=["2024-02-01"],
            benchmark_ticker=" spy ",
            max_iterations=10,
            min_training_rows=5,
        )
    )

    assert config.benchmark_ticker == "SPY"


def _local_writer(tmp_path: Path, monkeypatch) -> R2Writer:
    """Return a local mock R2 writer regardless of developer env files."""
    for name in (R2_ENDPOINT_ENV, R2_ACCESS_KEY_ENV, R2_SECRET_KEY_ENV, R2_BUCKET_ENV):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setattr("services.r2.client.R2_ENV_FILE", tmp_path / "missing-r2.env")
    return R2Writer(local_root=tmp_path)


def _write_parquet(writer: R2Writer, key: str, frame: pd.DataFrame) -> None:
    """Write one DataFrame as a parquet object."""
    buffer = io.BytesIO()
    frame.to_parquet(buffer, index=False)
    writer.put_object(key, buffer.getvalue())


def _benchmark_bars(count: int) -> pd.DataFrame:
    """Build synthetic SPY bars with three broad regimes."""
    rows: list[dict[str, object]] = []
    close = 400.0
    for index, date in enumerate(pd.bdate_range("2023-01-02", periods=count)):
        if index < count // 3:
            close *= 0.998
        elif index < (count * 2) // 3:
            close *= 1.0002
        else:
            close *= 1.002
        rows.append(
            {
                "date": date.date().isoformat(),
                "ticker": "SPY",
                "open": close - 0.5,
                "high": close + 1.0,
                "low": close - 1.0,
                "close": close,
                "volume": 2_000_000 + index,
                "adj_close": close,
                "dollar_volume": close * (2_000_000 + index),
            }
        )
    return pd.DataFrame(rows)


def _macro_archive(count: int) -> pd.DataFrame:
    """Build point-in-time macro rows for HMM training features."""
    rows: list[dict[str, object]] = []
    for date in pd.bdate_range("2022-12-01", periods=count):
        date_text = date.date().isoformat()
        rows.extend(
            [
                _macro_row("VIXCLS", date_text, 18.0),
                _macro_row("DGS10", date_text, 4.0),
                _macro_row("DGS2", date_text, 3.8),
                _macro_row("DGS3MO", date_text, 3.5),
                _macro_row("BAMLH0A0HYM2", date_text, 3.2),
            ]
        )
    return pd.DataFrame(rows)


def _macro_row(series_id: str, date_text: str, value: float) -> dict[str, object]:
    """Build one normalized FRED macro archive row."""
    return {
        "source": "fred",
        "series_id": series_id,
        "observation_date": date_text,
        "realtime_start": date_text,
        "realtime_end": date_text,
        "retrieved_at": "2024-01-01T00:00:00+00:00",
        "value": value,
        "is_missing": False,
        "raw": {"series_id": series_id},
    }
