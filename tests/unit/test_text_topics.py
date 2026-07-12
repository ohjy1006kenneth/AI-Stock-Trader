from __future__ import annotations

import json
from collections.abc import Sequence
from typing import cast

import pandas as pd
import pytest

from core.contracts.schemas import FeatureRecord, NewsSentimentRecord
from core.features.text_topics import (
    EMBEDDING_COLUMNS,
    TOPIC_LABEL_COLUMNS,
    TextEmbeddingConfig,
    TopicModelConfig,
    compute_sentence_embeddings,
    compute_text_topics,
    compute_topic_labels,
    embedding_cache_key,
    feature_records_to_frame,
    sentence_identity,
    topic_labels_to_feature_records,
)


class _FakeEmbedder:
    def encode(self, sentences: Sequence[str]) -> Sequence[Sequence[float]]:
        return [[float(index), float(len(sentence))] for index, sentence in enumerate(sentences)]


class _NaNEmbedder:
    def encode(self, sentences: Sequence[str]) -> Sequence[Sequence[float]]:
        return [[float("nan"), 1.0] for _ in sentences]


class _FakeTopicLabeler:
    def __init__(self) -> None:
        self._topics = {
            0: [("earnings", 0.5), ("guidance", 0.3), ("revenue", 0.2)],
            1: [("cloud", 0.5), ("ai", 0.3), ("demand", 0.2)],
        }

    def fit_transform(
        self,
        documents: Sequence[str],
        embeddings: Sequence[Sequence[float]],
    ) -> tuple[Sequence[int], Sequence[float]]:
        topics = [index % 2 for index, _ in enumerate(documents)]
        probabilities = [0.8 - (index * 0.05) for index, _ in enumerate(documents)]
        return topics, probabilities

    def get_topic(self, topic_id: int) -> Sequence[tuple[str, float]]:
        return self._topics.get(topic_id, [])

    def get_topic_info(self) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {"Topic": 0, "Name": "earnings_guidance_revenue"},
                {"Topic": 1, "Name": "cloud_ai_demand"},
            ]
        )


class _RecordingEmbedder:
    def __init__(self) -> None:
        self.calls: list[list[str]] = []

    def encode(self, sentences: Sequence[str]) -> Sequence[Sequence[float]]:
        batch = list(sentences)
        self.calls.append(batch)
        return [[float(index), float(len(sentence))] for index, sentence in enumerate(batch)]


class _ResettingTopicLabeler:
    def __init__(self) -> None:
        self.calls: list[list[str]] = []

    def fit_transform(
        self,
        documents: Sequence[str],
        embeddings: Sequence[Sequence[float]],
    ) -> tuple[Sequence[int], Sequence[float]]:
        batch = list(documents)
        self.calls.append(batch)
        return [0 for _ in batch], [0.75 for _ in batch]


def test_compute_text_topics_caches_unique_sentence_embeddings_and_topic_features() -> None:
    """Sentence embeddings are cached once while topic labels remain ticker-specific."""
    records = [
        _record(ticker="AAPL", text="Apple released results.", sentence_index=0),
        _record(ticker="MSFT", text="Apple released results.", sentence_index=0),
        _record(ticker="AAPL", text="Margins improved.", sentence_index=1),
    ]

    result = compute_text_topics(
        records,
        embedder=_FakeEmbedder(),
        topic_labeler=_FakeTopicLabeler(),
        embedding_config=_embedding_config(),
        topic_config=_topic_config(),
    )

    assert list(result.embeddings.columns) == list(EMBEDDING_COLUMNS)
    assert list(result.topic_labels.columns) == list(TOPIC_LABEL_COLUMNS)
    assert len(result.embeddings) == 2
    assert len(result.topic_labels) == 3
    assert {record.ticker for record in result.feature_records} == {"AAPL", "MSFT"}
    assert all(isinstance(record, FeatureRecord) for record in result.feature_records)
    aapl = next(record for record in result.feature_records if record.ticker == "AAPL")
    assert aapl.features["nlp_sentence_count"] == 2
    assert aapl.features["nlp_topic_count"] == 2




def test_compute_text_topics_builds_human_readable_topic_review() -> None:
    """Topic review output should carry labels, keywords, and representative examples."""
    result = compute_text_topics(
        [
            _record(text="Apple raised guidance on iPhone demand.", sentence_index=0),
            _record(text="Apple posted strong margins and services revenue.", sentence_index=1),
            _record(
                ticker="MSFT",
                text="Microsoft cloud revenue accelerated on AI demand.",
                sentence_index=0,
            ),
        ],
        embedder=_FakeEmbedder(),
        topic_labeler=_FakeTopicLabeler(),
        embedding_config=_embedding_config(),
        topic_config=_topic_config(),
    )

    review = cast(dict[str, object], result.topic_review)
    topics = cast(list[dict[str, object]], review["topics"])
    rows = cast(list[dict[str, object]], review["rows"])

    assert review["status"] == "pass"
    assert review["diversity_status"] == "diverse"
    assert review["topic_count"] == 2
    assert len(topics) == 2
    first_topic = topics[0]
    assert "Earnings" in str(first_topic["topic_label"])
    assert cast(list[str], first_topic["topic_keywords"] )[:3] == ["earnings", "guidance", "revenue"]
    assert first_topic["topic_example_text"]
    assert len(cast(list[str], first_topic["topic_example_texts"])) >= 1
    assert all("topic_label" in row for row in rows)


def test_compute_text_topics_marks_insufficient_diversity_for_single_topic() -> None:
    """A single-topic corpus should be called out explicitly in the review payload."""
    result = compute_text_topics(
        [
            _record(text="Apple released results.", sentence_index=0),
            _record(text="Margins improved.", sentence_index=1),
        ],
        embedder=_FakeEmbedder(),
        topic_labeler=_ResettingTopicLabeler(),
        embedding_config=_embedding_config(),
        topic_config=_topic_config(),
    )

    review = cast(dict[str, object], result.topic_review)
    topics = cast(list[dict[str, object]], review["topics"])

    assert review["status"] == "warn"
    assert review["diversity_status"] == "insufficient_diversity"
    assert review["topic_count"] == 1
    assert topics[0]["topic_label"]
    assert topics[0]["topic_example_text"]


    """Embedding cache keys include the pinned model revision."""
    record = _record()
    first = embedding_cache_key(record, config=_embedding_config(model_revision="rev-a"))
    second = embedding_cache_key(record, config=_embedding_config(model_revision="rev-b"))

    assert first != second
    assert sentence_identity(record) == sentence_identity(_record(ticker="MSFT"))


def test_compute_sentence_embeddings_empty_input_returns_canonical_frame() -> None:
    """Empty sentence rows return a canonical empty embedding cache."""
    embeddings = compute_sentence_embeddings(
        [],
        embedder=_FakeEmbedder(),
        config=_embedding_config(),
    )

    assert len(embeddings) == 0
    assert list(embeddings.columns) == list(EMBEDDING_COLUMNS)


def test_compute_sentence_embeddings_rejects_nan_vectors() -> None:
    """Embedding vectors must be finite numeric values."""
    with pytest.raises(ValueError, match="finite"):
        compute_sentence_embeddings(
            [_record()],
            embedder=_NaNEmbedder(),
            config=_embedding_config(),
        )


def test_compute_topic_labels_rejects_missing_embedding_cache_row() -> None:
    """Topic labeling fails closed when embedding cache rows are missing."""
    embeddings = compute_sentence_embeddings(
        [_record(text="Different sentence.")],
        embedder=_FakeEmbedder(),
        config=_embedding_config(),
    )

    with pytest.raises(ValueError, match="Missing embedding"):
        compute_topic_labels(
            [_record(text="Apple released results.")],
            embeddings,
            topic_labeler=_FakeTopicLabeler(),
            config=_topic_config(),
        )


def test_compute_text_topics_batches_and_offsets_topic_ids() -> None:
    """Positive batch-local topic ids are offset so merged features do not collide."""
    labeler = _ResettingTopicLabeler()
    result = compute_text_topics(
        [
            _record(text="Alpha.", sentence_index=0),
            _record(text="Beta.", sentence_index=1),
            _record(text="Gamma.", sentence_index=2),
            _record(text="Delta.", sentence_index=3),
        ],
        embedder=_FakeEmbedder(),
        topic_labeler=labeler,
        embedding_config=_embedding_config(),
        topic_config=_topic_config(),
        topic_batch_size=2,
    )

    assert labeler.calls == [["Alpha.", "Beta."], ["Gamma.", "Delta."]]
    assert result.topic_labels["topic_id"].tolist() == [0, 0, 1, 1]
    assert result.feature_records[0].features["nlp_topic_count"] == 2


def test_compute_text_topics_truncates_documents_before_embedding_and_topic_labeling() -> None:
    """Configured document truncation is applied consistently to embeddings and topic labels."""
    embedder = _RecordingEmbedder()
    labeler = _ResettingTopicLabeler()

    compute_text_topics(
        [_record(text="ABCDEFGHIJ", sentence_index=0)],
        embedder=embedder,
        topic_labeler=labeler,
        embedding_config=_embedding_config(),
        topic_config=_topic_config(),
        max_document_characters=5,
    )

    assert embedder.calls == [["ABCDE"]]
    assert labeler.calls == [["ABCDE"]]


def test_topic_labels_to_feature_records_rejects_missing_columns() -> None:
    """Topic-label aggregation requires the canonical topic label columns."""
    with pytest.raises(ValueError, match="topic_probability"):
        topic_labels_to_feature_records(pd.DataFrame([{"date": "2024-01-02"}]))


def test_feature_records_to_frame_serializes_validated_features() -> None:
    """Topic feature rows are serialized as FeatureRecord-compatible JSON."""
    frame = feature_records_to_frame(
        [
            FeatureRecord(
                date="2024-01-02",
                ticker="AAPL",
                features={"nlp_sentence_count": 2, "nlp_dominant_topic_id": 1},
            )
        ]
    )

    assert json.loads(frame.loc[0, "features"]) == {
        "nlp_dominant_topic_id": 1,
        "nlp_sentence_count": 2,
    }


def _record(
    *,
    date: str = "2024-01-02",
    ticker: str = "AAPL",
    text: str = "Apple released results.",
    sentence_index: int = 0,
) -> NewsSentimentRecord:
    """Build one sentence-level news sentiment record."""
    return NewsSentimentRecord(
        date=date,
        ticker=ticker,
        headline="Apple released results.",
        text=text,
        article_id="article-1",
        sentence_index=sentence_index,
        source="benzinga",
        published_at="2024-01-02T12:00:00+00:00",
    )


def _embedding_config(
    *,
    model_revision: str = "test-revision",
) -> TextEmbeddingConfig:
    """Return a small embedding config for unit tests."""
    return TextEmbeddingConfig(
        model_name="test-embedder",
        model_revision=model_revision,
        embedding_dimension=2,
    )


def _topic_config() -> TopicModelConfig:
    """Return a test topic model config."""
    return TopicModelConfig(model_name="test-topic-model", model_version="1.0")
