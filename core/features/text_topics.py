"""Layer 1 sentence embeddings and topic-label feature helpers."""
from __future__ import annotations

import hashlib
import importlib
import json
import math
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, Protocol

from core.contracts.schemas import FeatureRecord, NewsSentimentRecord

EMBEDDING_COLUMNS: tuple[str, ...] = (
    "date",
    "article_id",
    "sentence_index",
    "text",
    "embedding_model",
    "embedding_revision",
    "embedding_cache_key",
    "embedding_json",
)

TOPIC_LABEL_COLUMNS: tuple[str, ...] = (
    "date",
    "ticker",
    "article_id",
    "sentence_index",
    "text",
    "embedding_cache_key",
    "topic_model",
    "topic_model_version",
    "topic_id",
    "topic_probability",
)


class SentenceEmbedder(Protocol):
    """Sentence embedding provider used by Layer 1 topic features."""

    def encode(self, sentences: Sequence[str]) -> Sequence[Sequence[float]]:
        """Return one embedding vector per input sentence."""


class TopicLabeler(Protocol):
    """Topic-label provider used by Layer 1 topic features."""

    def fit_transform(
        self,
        documents: Sequence[str],
        embeddings: Sequence[Sequence[float]],
    ) -> tuple[Sequence[int], Sequence[float]]:
        """Return one topic id and confidence per input document."""


@dataclass(frozen=True)
class TextEmbeddingConfig:
    """Pinned sentence embedding model identity and shape."""

    model_name: str
    model_revision: str
    embedding_dimension: int

    def __post_init__(self) -> None:
        """Validate embedding model identity and vector shape."""
        if not self.model_name.strip():
            raise ValueError("model_name cannot be empty")
        if not self.model_revision.strip():
            raise ValueError("model_revision cannot be empty")
        if self.embedding_dimension <= 0:
            raise ValueError("embedding_dimension must be positive")


@dataclass(frozen=True)
class TopicModelConfig:
    """Pinned topic model identity for reproducible topic labels."""

    model_name: str
    model_version: str

    def __post_init__(self) -> None:
        """Validate topic model identity."""
        if not self.model_name.strip():
            raise ValueError("model_name cannot be empty")
        if not self.model_version.strip():
            raise ValueError("model_version cannot be empty")


@dataclass(frozen=True)
class TextTopicResult:
    """Computed embedding cache, topic labels, and ticker-day topic features."""

    embeddings: Any
    topic_labels: Any
    feature_records: list[FeatureRecord]


def compute_text_topics(
    records: Sequence[NewsSentimentRecord],
    *,
    embedder: SentenceEmbedder,
    topic_labeler: TopicLabeler,
    embedding_config: TextEmbeddingConfig,
    topic_config: TopicModelConfig,
) -> TextTopicResult:
    """Compute sentence embeddings, topic labels, and ticker-day topic features."""
    embeddings = compute_sentence_embeddings(
        records,
        embedder=embedder,
        config=embedding_config,
    )
    topic_labels = compute_topic_labels(
        records,
        embeddings,
        topic_labeler=topic_labeler,
        config=topic_config,
    )
    return TextTopicResult(
        embeddings=embeddings,
        topic_labels=topic_labels,
        feature_records=topic_labels_to_feature_records(topic_labels),
    )


def compute_sentence_embeddings(
    records: Sequence[NewsSentimentRecord],
    *,
    embedder: SentenceEmbedder,
    config: TextEmbeddingConfig,
) -> Any:
    """Return a deterministic embedding-cache DataFrame for unique sentence records."""
    pd = _require_pandas()
    sentence_records = _unique_sentence_records(records)
    if not sentence_records:
        return pd.DataFrame(columns=list(EMBEDDING_COLUMNS))

    texts = [str(record.text) for record in sentence_records]
    vectors = embedder.encode(texts)
    if len(vectors) != len(sentence_records):
        raise ValueError("Embedder returned a different number of vectors than input texts")

    rows: list[dict[str, object]] = []
    for record, vector in zip(sentence_records, vectors, strict=True):
        normalized_vector = _validate_embedding_vector(vector, config=config)
        rows.append(
            {
                "date": record.date,
                "article_id": _stable_article_id(record),
                "sentence_index": record.sentence_index,
                "text": record.text,
                "embedding_model": config.model_name,
                "embedding_revision": config.model_revision,
                "embedding_cache_key": embedding_cache_key(record, config=config),
                "embedding_json": json.dumps(normalized_vector, separators=(",", ":")),
            }
        )
    return pd.DataFrame(rows, columns=list(EMBEDDING_COLUMNS))


def compute_topic_labels(
    records: Sequence[NewsSentimentRecord],
    embeddings: Any,
    *,
    topic_labeler: TopicLabeler,
    config: TopicModelConfig,
) -> Any:
    """Return per-record BERTopic-style topic labels using the embedding cache."""
    pd = _require_pandas()
    if len(records) == 0:
        return pd.DataFrame(columns=list(TOPIC_LABEL_COLUMNS))

    embedding_by_key = {
        row["embedding_cache_key"]: _embedding_from_json(row["embedding_json"])
        for row in embeddings.to_dict(orient="records")
    }
    unique_records = _unique_sentence_records(records)
    if not unique_records:
        return pd.DataFrame(columns=list(TOPIC_LABEL_COLUMNS))

    documents: list[str] = []
    vectors: list[list[float]] = []
    for record in unique_records:
        key = _embedding_cache_key_from_frame(record, embeddings)
        if key not in embedding_by_key:
            raise ValueError(f"Missing embedding cache row for sentence key {key}")
        documents.append(str(record.text))
        vectors.append(embedding_by_key[key])

    topics, probabilities = topic_labeler.fit_transform(documents, vectors)
    if len(topics) != len(unique_records) or len(probabilities) != len(unique_records):
        raise ValueError("Topic labeler returned a different number of labels than input texts")

    topic_by_sentence = {
        sentence_identity(record): (
            int(topic),
            _validate_probability(probability),
            _embedding_cache_key_from_frame(record, embeddings),
        )
        for record, topic, probability in zip(
            unique_records,
            topics,
            probabilities,
            strict=True,
        )
    }

    rows: list[dict[str, object]] = []
    for record in records:
        if not record.text:
            continue
        topic_id, probability, cache_key = topic_by_sentence[sentence_identity(record)]
        rows.append(
            {
                "date": record.date,
                "ticker": record.ticker,
                "article_id": _stable_article_id(record),
                "sentence_index": record.sentence_index,
                "text": record.text,
                "embedding_cache_key": cache_key,
                "topic_model": config.model_name,
                "topic_model_version": config.model_version,
                "topic_id": topic_id,
                "topic_probability": probability,
            }
        )
    return pd.DataFrame(rows, columns=list(TOPIC_LABEL_COLUMNS))


def topic_labels_to_feature_records(topic_labels: Any) -> list[FeatureRecord]:
    """Aggregate sentence topic labels into validated ticker-day FeatureRecords."""
    pd = _require_pandas()
    if len(topic_labels) == 0:
        return []

    _require_columns(topic_labels, TOPIC_LABEL_COLUMNS)
    frame = topic_labels.copy()
    records: list[FeatureRecord] = []
    for (date_value, ticker), group in frame.groupby(["date", "ticker"], sort=True):
        valid_topics = group[group["topic_id"].map(lambda value: int(value) >= 0)]
        if len(valid_topics) == 0:
            dominant_topic_id: int | None = None
            dominant_probability: float | None = None
        else:
            topic_counts = valid_topics.groupby("topic_id").size().sort_values(ascending=False)
            dominant_topic_id = int(topic_counts.index[0])
            dominant_probability = float(
                valid_topics.loc[
                    valid_topics["topic_id"] == dominant_topic_id,
                    "topic_probability",
                ].mean()
            )

        mean_probability = frame.loc[group.index, "topic_probability"].map(float).mean()
        records.append(
            FeatureRecord(
                date=str(date_value),
                ticker=str(ticker),
                features={
                    "nlp_sentence_count": int(len(group)),
                    "nlp_topic_count": int(valid_topics["topic_id"].nunique()),
                    "nlp_dominant_topic_id": dominant_topic_id,
                    "nlp_dominant_topic_probability": dominant_probability,
                    "nlp_mean_topic_probability": (
                        None if pd.isna(mean_probability) else float(mean_probability)
                    ),
                },
            )
        )
    return records


def feature_records_to_frame(records: Sequence[FeatureRecord]) -> Any:
    """Serialize FeatureRecord rows into a Parquet-ready DataFrame."""
    pd = _require_pandas()
    rows = [
        {
            "date": record.date,
            "ticker": record.ticker,
            "features": json.dumps(record.features, sort_keys=True, separators=(",", ":")),
        }
        for record in records
    ]
    return pd.DataFrame(rows, columns=["date", "ticker", "features"])


def embedding_cache_key(record: NewsSentimentRecord, *, config: TextEmbeddingConfig) -> str:
    """Return a reproducible cache key for one sentence/model pair."""
    payload = "|".join(
        [
            sentence_identity(record),
            config.model_name,
            config.model_revision,
            str(config.embedding_dimension),
        ]
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def sentence_identity(record: NewsSentimentRecord) -> str:
    """Return a stable identity for one preprocessed sentence."""
    payload = "|".join(
        [
            record.date,
            _stable_article_id(record),
            str(record.sentence_index if record.sentence_index is not None else ""),
            str(record.text or ""),
        ]
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _unique_sentence_records(records: Sequence[NewsSentimentRecord]) -> list[NewsSentimentRecord]:
    """Return first-seen sentence records with non-empty text."""
    seen: set[str] = set()
    unique: list[NewsSentimentRecord] = []
    for record in records:
        if not record.text:
            continue
        identity = sentence_identity(record)
        if identity in seen:
            continue
        seen.add(identity)
        unique.append(record)
    return unique


def _embedding_cache_key_from_frame(record: NewsSentimentRecord, embeddings: Any) -> str:
    """Find a sentence embedding cache key in an embedding cache DataFrame."""
    identity = sentence_identity(record)
    for row in embeddings.to_dict(orient="records"):
        candidate = NewsSentimentRecord(
            date=str(row["date"]),
            ticker=record.ticker,
            text=str(row["text"]),
            article_id=str(row["article_id"]),
            sentence_index=int(row["sentence_index"]),
        )
        if sentence_identity(candidate) == identity:
            return str(row["embedding_cache_key"])
    raise ValueError(f"Missing embedding cache key for sentence identity {identity}")


def _stable_article_id(record: NewsSentimentRecord) -> str:
    """Return an article id or a deterministic fallback from sentence text."""
    if record.article_id:
        return record.article_id
    payload = f"{record.date}|{record.ticker}|{record.text or ''}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


def _validate_embedding_vector(
    vector: Sequence[float],
    *,
    config: TextEmbeddingConfig,
) -> list[float]:
    """Return a finite embedding vector with the configured dimension."""
    values = [float(value) for value in vector]
    if len(values) != config.embedding_dimension:
        raise ValueError(
            f"Embedding dimension {len(values)} does not match {config.embedding_dimension}"
        )
    if any(math.isnan(value) or math.isinf(value) for value in values):
        raise ValueError("Embedding vectors must contain only finite numeric values")
    return values


def _embedding_from_json(value: object) -> list[float]:
    """Decode an embedding JSON list into finite floats."""
    parsed = json.loads(str(value))
    if not isinstance(parsed, list):
        raise ValueError("embedding_json must decode to a list")
    values = [float(item) for item in parsed]
    if any(math.isnan(item) or math.isinf(item) for item in values):
        raise ValueError("embedding_json contains non-finite values")
    return values


def _validate_probability(value: object) -> float:
    """Return a finite topic confidence probability."""
    probability = float(value)
    if math.isnan(probability) or math.isinf(probability):
        raise ValueError("topic probabilities must be finite")
    if probability < 0.0 or probability > 1.0:
        raise ValueError("topic probabilities must be in [0, 1]")
    return probability


def _require_columns(frame: Any, columns: Sequence[str]) -> None:
    """Raise when a DataFrame is missing required columns."""
    missing = sorted(set(columns) - set(frame.columns))
    if missing:
        raise ValueError(f"Topic label frame missing required columns: {missing}")


def _require_pandas() -> Any:
    """Import pandas/pyarrow lazily with a clear error when unavailable."""
    try:
        import pandas as pd

        importlib.import_module("pyarrow")
    except ModuleNotFoundError as exc:
        raise ModuleNotFoundError(
            "pandas and pyarrow are required for text topic feature processing."
        ) from exc
    return pd
