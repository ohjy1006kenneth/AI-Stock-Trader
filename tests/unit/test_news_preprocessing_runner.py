from __future__ import annotations

import csv
import io
import json
from pathlib import Path

import pandas as pd

from app.lab.data_pipelines.run_news_preprocessing import (
    NLP_PREPROCESSING_STAGE,
    NewsPreprocessingPipelineConfig,
    load_modal_runtime_config,
    news_preprocessing_output_path,
    run_news_preprocessing,
)
from core.contracts.schemas import RunStatus
from services.r2.client import (
    R2_ACCESS_KEY_ENV,
    R2_BUCKET_ENV,
    R2_ENDPOINT_ENV,
    R2_SECRET_KEY_ENV,
)
from services.r2.paths import pipeline_manifest_path, raw_news_path, raw_universe_path
from services.r2.writer import R2Writer


def test_run_news_preprocessing_reads_r2_and_writes_outputs(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """The lab runner reads raw news/universe archives and writes sentence rows."""
    writer = _local_writer(tmp_path, monkeypatch)
    _write_jsonl(
        writer,
        raw_news_path("2024-01-02"),
        [
            {
                "id": 1001,
                "headline": "Apple reports earnings.",
                "summary": "Shares rose.",
                "created_at": "2024-01-02T12:00:00+00:00",
                "source": "benzinga",
                "symbols": ["AAPL", "MSFT"],
            }
        ],
    )
    _write_universe(
        writer,
        raw_universe_path("2024-01-02"),
        [
            {"date": "2024-01-02", "ticker": "AAPL", "in_universe": "True"},
            {
                "date": "2024-01-02",
                "ticker": "MSFT",
                "in_universe": "True",
                "data_quality_ok": "False",
            },
        ],
    )

    result = run_news_preprocessing(
        NewsPreprocessingPipelineConfig(run_id="nlp-test-run", as_of_date="2024-01-02"),
        writer=writer,
    )

    output = pd.read_parquet(io.BytesIO(writer.get_object(result.output_key)))
    manifest = json.loads(writer.get_object(result.manifest_key))

    assert result.output_key == news_preprocessing_output_path("nlp-test-run", "2024-01-02")
    assert result.manifest_key == pipeline_manifest_path(NLP_PREPROCESSING_STAGE, "nlp-test-run")
    assert output["ticker"].unique().tolist() == ["AAPL"]
    assert output["text"].tolist() == ["Apple reports earnings.", "Shares rose."]
    assert manifest["status"] == RunStatus.COMPLETED
    assert manifest["metadata"]["article_rows"] == 1
    assert manifest["metadata"]["sentence_rows"] == 2


def test_run_news_preprocessing_writes_failure_manifest(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """The lab runner writes a failed manifest when required R2 inputs are missing."""
    writer = _local_writer(tmp_path, monkeypatch)

    try:
        run_news_preprocessing(
            NewsPreprocessingPipelineConfig(run_id="nlp-fail-run", as_of_date="2024-01-02"),
            writer=writer,
        )
    except FileNotFoundError:
        pass
    else:
        assert False, "Expected FileNotFoundError for missing raw news archive"

    manifest = json.loads(
        writer.get_object(pipeline_manifest_path(NLP_PREPROCESSING_STAGE, "nlp-fail-run"))
    )
    assert manifest["status"] == RunStatus.FAILED
    assert manifest["metadata"]["error"]["type"] == "FileNotFoundError"


def test_load_modal_runtime_config_reads_repo_config() -> None:
    """Modal app and secret names live in config rather than code constants."""
    config = load_modal_runtime_config()

    assert config.app_name
    assert config.r2_secret_name
    assert config.timeout_seconds > 0


def _local_writer(tmp_path: Path, monkeypatch) -> R2Writer:
    """Return a local mock R2 writer regardless of developer env files."""
    for name in (R2_ENDPOINT_ENV, R2_ACCESS_KEY_ENV, R2_SECRET_KEY_ENV, R2_BUCKET_ENV):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setattr("services.r2.client.R2_ENV_FILE", tmp_path / "missing-r2.env")
    return R2Writer(local_root=tmp_path)


def _write_jsonl(writer: R2Writer, key: str, rows: list[dict[str, object]]) -> None:
    """Write raw JSONL rows into the mock object store."""
    payload = "\n".join(json.dumps(row, sort_keys=True) for row in rows) + "\n"
    writer.put_object(key, payload)


def _write_universe(writer: R2Writer, key: str, rows: list[dict[str, object]]) -> None:
    """Write raw universe CSV rows into the mock object store."""
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
