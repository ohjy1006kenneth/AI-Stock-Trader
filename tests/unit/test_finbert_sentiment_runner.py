from __future__ import annotations

import io
import json
from collections.abc import Sequence
from pathlib import Path

import pandas as pd
import pytest

from app.lab.data_pipelines.run_finbert_sentiment import (
    FINBERT_SENTIMENT_STAGE,
    FinBERTModelRuntimeConfig,
    FinBERTPipelineConfig,
    load_finbert_runtime_config,
    run_finbert_sentiment,
)
from app.lab.data_pipelines.run_news_preprocessing import news_preprocessing_output_path
from core.contracts.schemas import NewsSentimentRecord, RunStatus
from core.features.news_preprocessing import records_to_news_sentiment_frame
from core.features.sentiment_features import SentimentScore
from services.r2.client import (
    R2_ACCESS_KEY_ENV,
    R2_BUCKET_ENV,
    R2_ENDPOINT_ENV,
    R2_SECRET_KEY_ENV,
)
from services.r2.paths import (
    layer1_sentiment_feature_path,
    layer1_sentiment_score_path,
    pipeline_manifest_path,
)
from services.r2.writer import R2Writer


class _FakeScorer:
    """Deterministic FinBERT scorer for unit tests."""

    def score(self, texts: Sequence[str]) -> Sequence[SentimentScore]:
        """Return one positive-leaning score per input text."""
        return [SentimentScore(positive=0.8, negative=0.1, neutral=0.1) for _ in texts]


def test_run_finbert_sentiment_reads_preprocessed_news_and_writes_outputs(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """The lab runner writes scored news, sentiment features, and a manifest."""
    writer = _local_writer(tmp_path, monkeypatch)
    input_key = news_preprocessing_output_path("nlp-pre-run", "2024-01-02")
    _write_preprocessed_news(writer, input_key, _records())

    result = run_finbert_sentiment(
        FinBERTPipelineConfig(
            run_id="finbert-run",
            as_of_date="2024-01-02",
            preprocessed_news_key=input_key,
        ),
        writer=writer,
        scorer=_FakeScorer(),
        runtime_config=_runtime_config(tmp_path),
    )

    scored = pd.read_parquet(io.BytesIO(writer.get_object(result.scored_news_key)))
    features = pd.read_parquet(io.BytesIO(writer.get_object(result.sentiment_feature_key)))
    manifest = json.loads(writer.get_object(result.manifest_key))

    assert result.scored_news_key == layer1_sentiment_score_path("2024-01-02", "finbert-run")
    assert result.sentiment_feature_key == layer1_sentiment_feature_path(
        "2024-01-02",
        "finbert-run",
    )
    assert result.manifest_key == pipeline_manifest_path(FINBERT_SENTIMENT_STAGE, "finbert-run")
    assert len(scored) == 3
    assert set(features["ticker"]) == {"AAPL", "MSFT"}
    assert json.loads(features.loc[features["ticker"] == "AAPL", "features"].iloc[0])[
        "nlp_sentence_count"
    ] == 2
    assert manifest["status"] == RunStatus.COMPLETED
    assert manifest["metadata"]["scored_rows"] == 3
    assert manifest["metadata"]["feature_rows"] == 2


def test_run_finbert_sentiment_writes_failure_manifest(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """The lab runner writes a failed manifest when the preprocessing input is missing."""
    writer = _local_writer(tmp_path, monkeypatch)

    with pytest.raises(FileNotFoundError):
        run_finbert_sentiment(
            FinBERTPipelineConfig(
                run_id="finbert-fail",
                as_of_date="2024-01-02",
                preprocessed_news_key="features/layer1/news_sentiment/missing.parquet",
            ),
            writer=writer,
            scorer=_FakeScorer(),
            runtime_config=_runtime_config(tmp_path),
        )

    manifest = json.loads(
        writer.get_object(pipeline_manifest_path(FINBERT_SENTIMENT_STAGE, "finbert-fail"))
    )
    assert manifest["status"] == RunStatus.FAILED
    assert manifest["metadata"]["error"]["type"] == "FileNotFoundError"


def test_load_finbert_runtime_config_reads_repo_config() -> None:
    """Model identity and Modal settings are loaded from repository config."""
    config = load_finbert_runtime_config()

    assert config.app_name
    assert config.r2_secret_name
    assert config.timeout_seconds > 0
    assert config.model_name == "ProsusAI/finbert"
    assert config.model_revision
    assert config.batch_size > 0
    assert config.bucket_timezone == "America/New_York"


def _local_writer(tmp_path: Path, monkeypatch) -> R2Writer:
    """Return a local mock R2 writer regardless of developer env files."""
    for name in (R2_ENDPOINT_ENV, R2_ACCESS_KEY_ENV, R2_SECRET_KEY_ENV, R2_BUCKET_ENV):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setattr("services.r2.client.R2_ENV_FILE", tmp_path / "missing-r2.env")
    return R2Writer(local_root=tmp_path)


def _write_preprocessed_news(
    writer: R2Writer,
    key: str,
    records: list[NewsSentimentRecord],
) -> None:
    """Write sentence-level news records to a mock R2 parquet object."""
    buffer = io.BytesIO()
    records_to_news_sentiment_frame(records).to_parquet(buffer, index=False)
    writer.put_object(key, buffer.getvalue())


def _records() -> list[NewsSentimentRecord]:
    """Return preprocessed sentence-level rows."""
    return [
        _record(ticker="AAPL", text="Apple released results.", sentence_index=0),
        _record(ticker="MSFT", text="Microsoft released results.", sentence_index=0),
        _record(ticker="AAPL", text="Margins improved.", sentence_index=1),
    ]


def _record(ticker: str, text: str, sentence_index: int) -> NewsSentimentRecord:
    """Build one sentence-level news record."""
    return NewsSentimentRecord(
        date="2024-01-02",
        ticker=ticker,
        headline="Company released results.",
        text=text,
        article_id=f"article-{ticker}",
        sentence_index=sentence_index,
        source="benzinga",
        published_at="2024-01-02T12:00:00+00:00",
    )


def _runtime_config(tmp_path: Path) -> FinBERTModelRuntimeConfig:
    """Return small model settings for unit tests."""
    source_config_path = tmp_path / "source_credibility.json"
    source_config_path.write_text('{"default_source_weight": 1.0, "source_weights": {}}')
    return FinBERTModelRuntimeConfig(
        app_name="test-finbert-sentiment",
        r2_secret_name="ai-stock-trader-r2",
        timeout_seconds=60,
        model_name="test-finbert",
        model_revision="test-revision",
        batch_size=2,
        default_relevance_score=1.0,
        bucket_timezone="America/New_York",
        source_credibility_config_path=source_config_path,
        device=-1,
    )
