from __future__ import annotations

import io
import json
from pathlib import Path

import pandas as pd
import pytest

from app.lab.training.finbert_finetuning import ClassificationMetrics
from app.lab.training.run_finbert_finetuning import (
    ARTIFACT_MANIFEST_STAGE,
    FINBERT_FINETUNING_STAGE,
    FinBERTFineTuneConfig,
    FinBERTFineTuneRuntimeConfig,
    FineTuneTrainingArtifact,
    load_finbert_finetuning_runtime_config,
    run_finbert_finetuning,
)
from core.contracts.schemas import NewsSentimentRecord, RunStatus
from core.features.news_preprocessing import records_to_news_sentiment_frame
from core.features.sentiment_features import SentimentScore
from core.labels import write_label_record
from services.r2.client import (
    R2_ACCESS_KEY_ENV,
    R2_BUCKET_ENV,
    R2_ENDPOINT_ENV,
    R2_SECRET_KEY_ENV,
)
from services.r2.paths import layer1_news_preprocessing_path
from services.r2.writer import R2Writer


class _FakeScorer:
    """Deterministic scorer used to avoid loading transformers in unit tests."""

    def __init__(self, labels: list[str]) -> None:
        """Return one configured label per evaluated row."""
        self._labels = labels
        self._index = 0

    def score(self, texts: list[str]) -> list[SentimentScore]:
        """Return one sentiment distribution per input text."""
        batch_labels = self._labels[self._index : self._index + len(texts)]
        self._index += len(texts)
        scores: list[SentimentScore] = []
        for label in batch_labels:
            if label == "positive":
                scores.append(SentimentScore(positive=0.8, negative=0.1, neutral=0.1))
            elif label == "negative":
                scores.append(SentimentScore(positive=0.1, negative=0.8, neutral=0.1))
            else:
                scores.append(SentimentScore(positive=0.1, negative=0.1, neutral=0.8))
        return scores


class _FakeTrainer:
    """Lightweight trainer stub for fine-tuning orchestration tests."""

    def train(
        self,
        *,
        run_id: str,
        train_dataset: pd.DataFrame,
        eval_dataset: pd.DataFrame,
        output_dir: Path,
        runtime_config: FinBERTFineTuneRuntimeConfig,
    ) -> FineTuneTrainingArtifact:
        """Write a small placeholder file and return deterministic metrics."""
        del runtime_config
        output_dir.mkdir(parents=True, exist_ok=True)
        (output_dir / "weights.bin").write_text("stub", encoding="utf-8")
        return FineTuneTrainingArtifact(
            model_version=f"finbert-finetuned-{run_id}",
            bundle_subdir=output_dir.name,
            eval_metrics=ClassificationMetrics(
                accuracy=0.75,
                macro_precision=0.75,
                macro_recall=0.75,
                macro_f1=0.75,
                label_support={"negative": 1, "neutral": 0, "positive": 1},
                confusion_matrix={
                    "negative": {"negative": 1, "neutral": 0, "positive": 0},
                    "neutral": {"negative": 0, "neutral": 0, "positive": 0},
                    "positive": {"negative": 0, "neutral": 0, "positive": 1},
                },
            ),
            training_loss=0.25,
            epochs_completed=1,
        )


def test_run_finbert_finetuning_writes_reports_and_manifests(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Offline runner writes local artifacts while leaving production config untouched."""
    writer = _local_writer(tmp_path / "r2", monkeypatch)
    _write_preprocessed_news(
        writer,
        layer1_news_preprocessing_path("2024-04-10", "news-run"),
        [
            _record("2024-04-10", "AAPL", "Apple beats estimates.", "a1"),
            _record("2024-04-10", "MSFT", "Microsoft misses estimates.", "m1"),
        ],
    )
    _write_preprocessed_news(
        writer,
        layer1_news_preprocessing_path("2024-04-11", "news-run"),
        [
            _record("2024-04-11", "AAPL", "Apple guidance steady.", "a2"),
            _record("2024-04-11", "MSFT", "Microsoft demand slows.", "m2"),
        ],
    )
    write_label_record(
        {
            "date": "2024-04-10",
            "ticker": "AAPL",
            "features": {"forward_return_1d": 0.03},
        },
        writer=writer,
    )
    write_label_record(
        {
            "date": "2024-04-10",
            "ticker": "MSFT",
            "features": {"forward_return_1d": -0.02},
        },
        writer=writer,
    )
    write_label_record(
        {
            "date": "2024-04-11",
            "ticker": "AAPL",
            "features": {"forward_return_1d": 0.02},
        },
        writer=writer,
    )
    write_label_record(
        {
            "date": "2024-04-11",
            "ticker": "MSFT",
            "features": {"forward_return_1d": -0.03},
        },
        writer=writer,
    )

    result = run_finbert_finetuning(
        FinBERTFineTuneConfig(
            run_id="finetune-offline-1",
            from_date="2024-04-10",
            to_date="2024-04-11",
            news_run_id="news-run",
            fine_tune=True,
        ),
        writer=writer,
        scorer=_FakeScorer(["positive", "negative"]),
        trainer=_FakeTrainer(),
        runtime_config=_runtime_config(),
        artifact_root=tmp_path / "artifacts-root",
    )

    metrics_report = json.loads((tmp_path / "artifacts-root" / result.baseline_metrics_path).read_text())
    dataset_report = json.loads((tmp_path / "artifacts-root" / result.dataset_report_path).read_text())
    pipeline_manifest = json.loads(
        (tmp_path / "artifacts-root" / result.pipeline_manifest_path).read_text()
    )
    artifact_manifest = json.loads(
        (tmp_path / "artifacts-root" / result.artifact_manifest_path).read_text()
    )

    assert result.dataset_rows == 4
    assert result.train_rows == 2
    assert result.eval_rows == 2
    assert metrics_report["approved_for_production"] is False
    assert metrics_report["baseline_metrics"]["accuracy"] == pytest.approx(1.0)
    assert metrics_report["fine_tuned_metrics"]["accuracy"] == pytest.approx(0.75)
    assert dataset_report["dataset_stats"]["label_counts"] == {
        "negative": 2,
        "neutral": 0,
        "positive": 2,
    }
    assert pipeline_manifest["stage"] == FINBERT_FINETUNING_STAGE
    assert pipeline_manifest["status"] == RunStatus.COMPLETED
    assert artifact_manifest["stage"] == ARTIFACT_MANIFEST_STAGE
    assert artifact_manifest["approved"] is False
    assert artifact_manifest["bundle_path"] == result.bundle_path
    assert (tmp_path / "artifacts-root" / result.bundle_path / "model" / "weights.bin").exists()


def test_load_finbert_finetuning_runtime_config_reads_repo_config() -> None:
    """Offline FinBERT runtime settings come from repository config."""
    config = load_finbert_finetuning_runtime_config()

    assert config.app_name
    assert config.r2_secret_name
    assert config.base_model_name == "ProsusAI/finbert"
    assert config.base_model_revision == "db38d3727cbaed87c9aed72df7b3519e2ba5cca1"
    assert config.gpu_type == "T4"
    assert config.requirements_path == "requirements/modal.txt"


def _local_writer(root: Path, monkeypatch: pytest.MonkeyPatch) -> R2Writer:
    """Return a local mock writer regardless of developer env files."""
    for name in (R2_ENDPOINT_ENV, R2_ACCESS_KEY_ENV, R2_SECRET_KEY_ENV, R2_BUCKET_ENV):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setattr("services.r2.client.R2_ENV_FILE", root / "missing-r2.env")
    return R2Writer(local_root=root)


def _write_preprocessed_news(
    writer: R2Writer,
    key: str,
    records: list[NewsSentimentRecord],
) -> None:
    """Persist preprocessed news rows into the local R2 mock."""
    buffer = io.BytesIO()
    records_to_news_sentiment_frame(records).to_parquet(buffer, index=False)
    writer.put_object(key, buffer.getvalue())


def _record(date: str, ticker: str, text: str, article_id: str) -> NewsSentimentRecord:
    """Build one sentence-level test record."""
    return NewsSentimentRecord(
        date=date,
        ticker=ticker,
        headline=text,
        text=text,
        article_id=article_id,
        sentence_index=0,
        source="Reuters",
        published_at=f"{date}T13:00:00Z",
    )


def _runtime_config() -> FinBERTFineTuneRuntimeConfig:
    """Return small offline settings for unit tests."""
    return FinBERTFineTuneRuntimeConfig(
        app_name="test-finbert-finetuning",
        r2_secret_name="ai-stock-trader-r2",
        timeout_seconds=60,
        python_version="3.11",
        requirements_path="requirements/modal.txt",
        base_model_name="ProsusAI/finbert",
        base_model_revision="test-revision",
        train_batch_size=2,
        eval_batch_size=2,
        learning_rate=2e-5,
        weight_decay=0.01,
        epochs=1,
        max_length=32,
        eval_fraction=0.5,
        neutral_band_return=0.01,
        gpu_type="T4",
    )
