from __future__ import annotations

import json
from collections.abc import Sequence
from datetime import UTC, datetime

import pandas as pd
import pytest

from app.lab.data_pipelines.run_text_topics import BERTopicLabeler, TextModelRuntimeConfig
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
    def fit_transform(
        self,
        documents: Sequence[str],
        embeddings: Sequence[Sequence[float]],
    ) -> tuple[Sequence[int], Sequence[float]]:
        return [index % 2 for index, _ in enumerate(documents)], [0.8, 0.6][: len(documents)]


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


class _ReviewTopicLabeler:
    def fit_transform(
        self,
        documents: Sequence[str],
        embeddings: Sequence[Sequence[float]],
    ) -> tuple[Sequence[int], Sequence[float]]:
        return [0 for _ in documents], [0.93 for _ in documents]

    def get_topic_info(self) -> pd.DataFrame:
        return pd.DataFrame(
            [{"Topic": 0, "Name": "earnings_and_guidance", "Representation": "earnings_and_guidance"}]
        )

    def get_topic(self, topic_id: int) -> list[tuple[str, float]]:
        assert topic_id == 0
        return [("earnings", 0.92), ("guidance", 0.86), ("revenue", 0.81)]


def test_compute_text_topics_caches_article_embeddings_and_topic_features() -> None:
    """Article embeddings are cached once while topic labels and article counts stay canonical."""
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
    assert len(result.embeddings) == 1
    assert result.embeddings.loc[0, "article_sentence_count"] == 2
    assert len(result.topic_labels) == 2
    assert result.feature_records[0].features["nlp_article_count"] == 1
    assert result.feature_records[0].features["nlp_sentence_count"] == 2
    assert result.topic_review["row_count"] == 2
    assert result.topic_review["topic_count"] == 1
    assert result.topic_review["diversity_status"] == "insufficient_diversity"
    assert len(result.topic_review["rows"]) == 2


def test_compute_text_topics_builds_human_readable_topic_review() -> None:
    """Topic review payload exposes readable labels, keywords, and example text."""
    result = compute_text_topics(
        [
            _record(text="Apple raised guidance and discussed revenue."),
            _record(text="Management cited stronger earnings and guidance."),
        ],
        embedder=_FakeEmbedder(),
        topic_labeler=_ReviewTopicLabeler(),
        embedding_config=_embedding_config(),
        topic_config=_topic_config(),
    )

    review = result.topic_review
    assert review["diversity_status"] == "insufficient_diversity"
    assert review["topic_count"] == 1
    assert review["dominant_topic_id"] == 0
    assert review["dominant_topic_share"] == 1.0
    assert review["topics"][0]["topic_keywords"] == ["earnings", "guidance", "revenue"]
    assert review["topics"][0]["topic_example_text"]
    assert review["rows"][0]["topic_row_count"] == 1
    assert review["rows"][0]["topic_row_share"] == 1.0


def test_embedding_cache_key_changes_with_model_revision() -> None:
    """Embedding cache keys include the pinned model revision."""
    record = _record()
    first = embedding_cache_key(record, config=_embedding_config(model_revision="rev-a"))
    second = embedding_cache_key(record, config=_embedding_config(model_revision="rev-b"))

    assert first != second
    assert sentence_identity(record) == sentence_identity(_record(ticker="MSFT"))


@pytest.mark.parametrize("document_count", range(2, 7))
def test_compute_text_topics_falls_back_for_tiny_corpora(document_count: int) -> None:
    """Tiny corpora use the deterministic fallback instead of BERTopic/UMAP."""
    records = [
        _record(
            text=f"Apple earnings and iPhone demand headline {index}.",
            article_id=f"article-{index}",
            sentence_index=0,
        )
        for index in range(document_count)
    ]

    result = compute_text_topics(
        records,
        embedder=_FakeEmbedder(),
        topic_labeler=BERTopicLabeler(_runtime_config()),
        embedding_config=_embedding_config(),
        topic_config=_topic_config(),
    )

    assert result.topic_labels["topic_id"].tolist() == [0 for _ in range(document_count)]
    assert result.topic_labels["topic_probability"].tolist() == [1.0 for _ in range(document_count)]
    assert result.topic_review["generation_mode"] == "tiny_corpus_fallback"
    assert "spectral initialization" in str(result.topic_review["generation_reason"])
    assert result.topic_review["dominant_topic_id"] == 0
    assert result.topic_review["dominant_topic_share"] == 1.0


def test_bertopic_labeler_uses_bertopic_path_for_sufficient_corpora(monkeypatch) -> None:
    """Document batches above the tiny-corpus cutoff still use BERTopic."""
    import_count = 0
    calls: dict[str, object] = {}

    class _FakeModel:
        def __init__(self, **kwargs: object) -> None:
            calls["kwargs"] = kwargs

        def fit_transform(
            self,
            documents: Sequence[str],
            embeddings: object,
        ) -> tuple[list[int], None]:
            calls["documents"] = list(documents)
            calls["embeddings"] = embeddings
            return [1 for _ in documents], None

    class _FakeBertopicModule:
        BERTopic = _FakeModel

    class _FakeNumpyModule:
        @staticmethod
        def asarray(values: object) -> object:
            return values

    class _FakeUmapModel:
        def __init__(self, **kwargs: object) -> None:
            calls["umap_kwargs"] = kwargs

    class _FakeHdbscanModel:
        def __init__(self, **kwargs: object) -> None:
            calls["hdbscan_kwargs"] = kwargs

    class _FakeUmapModule:
        UMAP = _FakeUmapModel

    class _FakeHdbscanModule:
        HDBSCAN = _FakeHdbscanModel

    def fake_import_module(name: str) -> object:
        nonlocal import_count
        if name == "bertopic":
            import_count += 1
            return _FakeBertopicModule()
        if name == "numpy":
            return _FakeNumpyModule()
        if name == "umap":
            return _FakeUmapModule()
        if name == "hdbscan":
            return _FakeHdbscanModule()
        raise AssertionError(f"Unexpected import requested: {name}")

    monkeypatch.setattr("app.lab.data_pipelines.run_text_topics.importlib.import_module", fake_import_module)
    labeler = BERTopicLabeler(_runtime_config())
    topics, probabilities = labeler.fit_transform(
        [f"doc {index}" for index in range(7)],
        [[float(index)] for index in range(7)],
    )

    assert topics == [1 for _ in range(7)]
    assert probabilities == [1.0 for _ in range(7)]
    assert labeler.last_generation_mode == "bertopic"
    assert import_count == 1
    assert calls["documents"] == [f"doc {index}" for index in range(7)]


def test_compute_sentence_embeddings_empty_input_returns_canonical_frame() -> None:
    """Empty article rows return a canonical empty embedding cache."""
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
            _record(text="Alpha.", article_id="article-1", sentence_index=0),
            _record(text="Beta.", article_id="article-2", sentence_index=0),
            _record(text="Gamma.", article_id="article-3", sentence_index=0),
            _record(text="Delta.", article_id="article-4", sentence_index=0),
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
    article_id: str = "article-1",
    sentence_index: int = 0,
) -> NewsSentimentRecord:
    """Build one preprocessed news sentiment record."""
    return NewsSentimentRecord(
        date=date,
        ticker=ticker,
        headline="Apple released results.",
        normalized_headline="apple released results.",
        text=text,
        article_id=article_id,
        sentence_index=sentence_index,
        chunk_index=sentence_index,
        source_text_order=sentence_index,
        source="benzinga",
        published_at=datetime(2024, 1, 2, 12, 0, tzinfo=UTC),
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


def _runtime_config() -> TextModelRuntimeConfig:
    """Return a small runtime config that still exercises the real BERTopic labeler path."""
    return TextModelRuntimeConfig(
        app_name="test-text-topics",
        r2_secret_name="ai-stock-trader-r2",
        timeout_seconds=60,
        python_version="3.11",
        requirements_path="requirements/modal.txt",
        embedding_config=_embedding_config(),
        topic_config=_topic_config(),
        min_topic_size=2,
    )
