from __future__ import annotations

import io
import json
from collections.abc import Sequence
from pathlib import Path

import pandas as pd

from app.lab.data_pipelines.run_news_preprocessing import news_preprocessing_output_path
from app.lab.data_pipelines.run_text_topics import (
    TEXT_TOPICS_STAGE,
    TextModelRuntimeConfig,
    TextTopicPipelineConfig,
    load_text_model_runtime_config,
    run_text_topics,
)
from core.contracts.schemas import NewsSentimentRecord, RunStatus
from core.features.news_preprocessing import records_to_news_sentiment_frame
from core.features.text_topics import TextEmbeddingConfig, TopicModelConfig
from services.r2.client import (
    R2_ACCESS_KEY_ENV,
    R2_BUCKET_ENV,
    R2_ENDPOINT_ENV,
    R2_SECRET_KEY_ENV,
)
from services.r2.paths import (
    layer1_text_embedding_path,
    layer1_topic_feature_path,
    layer1_topic_label_path,
    pipeline_manifest_path,
)
from services.r2.writer import R2Writer


class _FakeEmbedder:
    def encode(self, sentences: Sequence[str]) -> Sequence[Sequence[float]]:
        return [[float(index), float(len(sentence))] for index, sentence in enumerate(sentences)]


class _FakeTopicLabeler:
    def fit_transform(
        self,
        documents: Sequence[str],
        embeddings: Sequence[Sequence[float]],
    ) -> tuple[Sequence[int], Sequence[float]]:
        return [index for index, _ in enumerate(documents)], [0.75 for _ in documents]


def test_run_text_topics_reads_preprocessed_news_and_writes_outputs(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """The lab runner writes embeddings, topic labels, topic features, and a manifest."""
    writer = _local_writer(tmp_path, monkeypatch)
    input_key = news_preprocessing_output_path("nlp-pre-run", "2024-01-02")
    _write_preprocessed_news(writer, input_key, _records())

    result = run_text_topics(
        TextTopicPipelineConfig(
            run_id="text-topics-run",
            as_of_date="2024-01-02",
            preprocessed_news_key=input_key,
        ),
        writer=writer,
        embedder=_FakeEmbedder(),
        topic_labeler=_FakeTopicLabeler(),
        runtime_config=_runtime_config(),
    )

    embeddings = pd.read_parquet(io.BytesIO(writer.get_object(result.embedding_key)))
    labels = pd.read_parquet(io.BytesIO(writer.get_object(result.topic_label_key)))
    features = pd.read_parquet(io.BytesIO(writer.get_object(result.topic_feature_key)))
    manifest = json.loads(writer.get_object(result.manifest_key))

    assert result.embedding_key == layer1_text_embedding_path("2024-01-02", "text-topics-run")
    assert result.topic_label_key == layer1_topic_label_path("2024-01-02", "text-topics-run")
    assert result.topic_feature_key == layer1_topic_feature_path("2024-01-02", "text-topics-run")
    assert result.manifest_key == pipeline_manifest_path(TEXT_TOPICS_STAGE, "text-topics-run")
    assert len(embeddings) == 2
    assert len(labels) == 3
    assert set(features["ticker"]) == {"AAPL", "MSFT"}
    assert manifest["status"] == RunStatus.COMPLETED
    assert manifest["metadata"]["embedding_rows"] == 2
    assert manifest["metadata"]["topic_label_rows"] == 3


def test_run_text_topics_writes_failure_manifest(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """The lab runner writes a failed manifest when the preprocessing input is missing."""
    writer = _local_writer(tmp_path, monkeypatch)

    try:
        run_text_topics(
            TextTopicPipelineConfig(
                run_id="text-topics-fail",
                as_of_date="2024-01-02",
                preprocessed_news_key="features/layer1/news_sentiment/missing.parquet",
            ),
            writer=writer,
            embedder=_FakeEmbedder(),
            topic_labeler=_FakeTopicLabeler(),
            runtime_config=_runtime_config(),
        )
    except FileNotFoundError:
        pass
    else:
        assert False, "Expected FileNotFoundError for missing preprocessing input"

    manifest = json.loads(
        writer.get_object(pipeline_manifest_path(TEXT_TOPICS_STAGE, "text-topics-fail"))
    )
    assert manifest["status"] == RunStatus.FAILED
    assert manifest["metadata"]["error"]["type"] == "FileNotFoundError"


def test_load_text_model_runtime_config_reads_repo_config() -> None:
    """Model identities and Modal settings are loaded from repository config."""
    config = load_text_model_runtime_config()

    assert config.app_name
    assert config.r2_secret_name
    assert config.timeout_seconds > 0
    assert config.python_version == "3.11"
    assert config.requirements_path == "requirements/modal.txt"
    assert config.embedding_config.model_name == "sentence-transformers/all-mpnet-base-v2"
    assert config.embedding_config.model_revision
    assert config.embedding_config.embedding_dimension == 768
    assert config.topic_config.model_name == "BERTopic"


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
    """Return preprocessed sentence-level rows with one duplicated sentence."""
    return [
        _record(ticker="AAPL", text="Apple released results.", sentence_index=0),
        _record(ticker="MSFT", text="Apple released results.", sentence_index=0),
        _record(ticker="AAPL", text="Margins improved.", sentence_index=1),
    ]


def _record(ticker: str, text: str, sentence_index: int) -> NewsSentimentRecord:
    """Build one sentence-level news record."""
    return NewsSentimentRecord(
        date="2024-01-02",
        ticker=ticker,
        headline="Apple released results.",
        text=text,
        article_id="article-1",
        sentence_index=sentence_index,
        source="benzinga",
        published_at="2024-01-02T12:00:00+00:00",
    )


def _runtime_config() -> TextModelRuntimeConfig:
    """Return small model settings for unit tests."""
    return TextModelRuntimeConfig(
        app_name="test-text-topics",
        r2_secret_name="ai-stock-trader-r2",
        timeout_seconds=60,
        python_version="3.11",
        requirements_path="requirements/modal.txt",
        embedding_config=TextEmbeddingConfig(
            model_name="test-embedder",
            model_revision="test-revision",
            embedding_dimension=2,
        ),
        topic_config=TopicModelConfig(model_name="test-topic", model_version="1.0"),
        min_topic_size=2,
    )
