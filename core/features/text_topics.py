"""Layer 1 article embeddings and topic-label feature helpers."""
from __future__ import annotations

import hashlib
import importlib
import json
import math
import re
from collections import Counter
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, Protocol

from core.contracts.schemas import FeatureRecord, NewsSentimentRecord

EMBEDDING_COLUMNS: tuple[str, ...] = (
    "date",
    "article_id",
    "normalized_headline",
    "text",
    "article_sentence_count",
    "embedding_model",
    "embedding_revision",
    "embedding_cache_key",
    "embedding_json",
)

TOPIC_LABEL_COLUMNS: tuple[str, ...] = (
    "date",
    "ticker",
    "article_id",
    "normalized_headline",
    "text",
    "article_sentence_count",
    "embedding_cache_key",
    "topic_model",
    "topic_model_version",
    "topic_id",
    "topic_probability",
)

TOPIC_FEATURE_COLUMNS: tuple[str, ...] = (
    "nlp_article_count",
    "nlp_sentence_count",
    "nlp_topic_count",
    "nlp_dominant_topic_id",
    "nlp_dominant_topic_probability",
    "nlp_mean_topic_probability",
)


class SentenceEmbedder(Protocol):
    """Article embedding provider used by Layer 1 topic features."""

    def encode(self, documents: Sequence[str]) -> Sequence[Sequence[float]]:
        """Return one embedding vector per input article document."""


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
    """Pinned article embedding model identity and shape."""

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


_TOPIC_FALLBACK_STOPWORDS = {
    "a",
    "about",
    "after",
    "all",
    "also",
    "an",
    "and",
    "are",
    "as",
    "at",
    "be",
    "before",
    "by",
    "for",
    "from",
    "has",
    "have",
    "in",
    "into",
    "is",
    "it",
    "its",
    "new",
    "of",
    "on",
    "or",
    "said",
    "shares",
    "the",
    "their",
    "this",
    "to",
    "with",
    "via",
    "over",
    "up",
}


def _optional_int(value: object) -> int | None:
    """Return an int if the value is numeric, otherwise None."""
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return None if math.isnan(value) else int(value)
    try:
        parsed = float(str(value))
    except (TypeError, ValueError):
        return None
    return None if math.isnan(parsed) else int(parsed)


def _optional_float(value: object) -> float | None:
    """Return a float if the value is numeric, otherwise None."""
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return None if isinstance(value, float) and math.isnan(value) else float(value)
    try:
        parsed = float(str(value))
    except (TypeError, ValueError):
        return None
    return None if math.isnan(parsed) else parsed


def _optional_str(value: object) -> str | None:
    """Return a stripped string if present, otherwise None."""
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _coerce_string_list(value: object) -> list[str]:
    """Return a normalized list of human-readable strings from JSON-ish values."""
    if value is None:
        return []
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return []
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            return [text]
        if isinstance(parsed, list):
            return [str(item).strip() for item in parsed if str(item).strip()]
        return [str(parsed).strip()]
    if isinstance(value, Sequence) and not isinstance(value, (bytes, bytearray, str)):
        return [str(item).strip() for item in value if str(item).strip()]
    text = str(value).strip()
    return [text] if text else []


@dataclass(frozen=True)
class TextTopicResult:
    """Computed article embedding cache, topic labels, and ticker-day topic features."""

    embeddings: Any
    topic_labels: Any
    feature_records: list[FeatureRecord]
    topic_review: dict[str, object]


@dataclass(frozen=True)
class _ArticleDocument:
    """Article-level document derived from one or more preprocessed sentence rows."""

    date: str
    article_id: str
    normalized_headline: str | None
    text: str
    tickers: tuple[str, ...]
    sentence_count: int


def compute_text_topics(
    records: Sequence[NewsSentimentRecord],
    *,
    embedder: SentenceEmbedder,
    topic_labeler: TopicLabeler,
    embedding_config: TextEmbeddingConfig,
    topic_config: TopicModelConfig,
    embedding_batch_size: int | None = None,
    topic_batch_size: int | None = None,
    max_document_characters: int | None = None,
) -> TextTopicResult:
    """Compute article embeddings, topic labels, and ticker-day topic features."""
    articles = _article_documents(records)
    embeddings = compute_sentence_embeddings(
        articles,
        embedder=embedder,
        config=embedding_config,
        batch_size=embedding_batch_size,
        max_document_characters=max_document_characters,
    )
    topic_labels = compute_topic_labels(
        articles,
        embeddings,
        topic_labeler=topic_labeler,
        config=topic_config,
        batch_size=topic_batch_size,
        max_document_characters=max_document_characters,
    )
    return TextTopicResult(
        embeddings=embeddings,
        topic_labels=topic_labels,
        feature_records=topic_labels_to_feature_records(topic_labels),
        topic_review=build_topic_review_payload(topic_labels, topic_labeler=topic_labeler),
    )


def build_topic_review_payload(
    topic_labels: Any,
    *,
    topic_labeler: Any | None = None,
) -> dict[str, object]:
    """Build a human-readable topic review payload from topic label rows."""
    if len(topic_labels) == 0:
        return {
            "status": "warn",
            "diversity_status": "insufficient_diversity",
            "diversity_reason": "No topic rows were produced.",
            "row_count": 0,
            "topic_count": 0,
            "dominant_topic_id": None,
            "dominant_topic_share": None,
            "topics": [],
            "rows": [],
        }

    _require_columns(topic_labels, TOPIC_LABEL_COLUMNS)
    frame = topic_labels.copy()
    frame.columns = [str(column) for column in frame.columns]
    frame["topic_id"] = frame["topic_id"].map(int)
    frame["topic_probability"] = frame["topic_probability"].map(float)

    topic_info = _topic_info_lookup(topic_labeler)
    topic_summaries: list[dict[str, object]] = []
    row_payloads: list[dict[str, object]] = []
    valid_rows = frame[frame["topic_id"] >= 0].copy()
    invalid_rows = frame[frame["topic_id"] < 0].copy()
    topic_count = int(valid_rows["topic_id"].nunique()) if len(valid_rows) else 0

    for topic_id, group in valid_rows.groupby("topic_id", sort=True):
        summary = _summarize_topic_group(
            group,
            topic_id=int(topic_id),
            topic_info=topic_info,
            topic_labeler=topic_labeler,
            total_rows=len(frame),
        )
        topic_summaries.append(summary)
        row_payloads.extend(_topic_rows_with_summary(group, summary))

    if len(invalid_rows):
        invalid_summary = _summarize_topic_group(
            invalid_rows,
            topic_id=-1,
            topic_info=topic_info,
            topic_labeler=topic_labeler,
            total_rows=len(frame),
        )
        invalid_summary["topic_label"] = "Outlier / Unassigned"
        invalid_summary["topic_keywords"] = []
        row_payloads.extend(_topic_rows_with_summary(invalid_rows, invalid_summary))

    dominant_topic_id: int | None = None
    dominant_topic_share: float | None = None
    diversity_reason = "No non-outlier topics were detected."
    diversity_status = "insufficient_diversity"
    if topic_summaries:
        dominant = max(topic_summaries, key=lambda item: float(item["topic_row_share"]))
        dominant_topic_id = int(dominant["topic_id"])
        dominant_topic_share = float(dominant["topic_row_share"])
        if len(topic_summaries) == 1:
            diversity_reason = f"Only one topic was detected across {len(valid_rows)} rows."
            diversity_status = "insufficient_diversity"
        elif dominant_topic_share >= 0.85:
            diversity_reason = (
                f"One topic covers {dominant_topic_share:.0%} of rows, so the corpus is too "
                "concentrated for a varied review sample."
            )
            diversity_status = "concentrated"
        else:
            diversity_reason = f"Detected {len(topic_summaries)} topics across {len(valid_rows)} rows."
            diversity_status = "diverse"

    status = "pass" if diversity_status == "diverse" else "warn"
    row_payloads.sort(key=_topic_row_sort_key)
    topic_summaries.sort(key=_topic_summary_sort_key)
    return {
        "status": status,
        "diversity_status": diversity_status,
        "diversity_reason": diversity_reason,
        "row_count": int(len(frame)),
        "topic_count": topic_count,
        "dominant_topic_id": dominant_topic_id,
        "dominant_topic_share": dominant_topic_share,
        "topics": topic_summaries,
        "rows": row_payloads,
    }


def compute_sentence_embeddings(
    records: Sequence[NewsSentimentRecord] | Sequence[_ArticleDocument],
    *,
    embedder: SentenceEmbedder,
    config: TextEmbeddingConfig,
    batch_size: int | None = None,
    max_document_characters: int | None = None,
) -> Any:
    """Return a deterministic embedding-cache DataFrame for unique article documents."""
    pd = _require_pandas()
    _validate_optional_positive(batch_size, field_name="batch_size")
    _validate_optional_positive(
        max_document_characters,
        field_name="max_document_characters",
    )
    articles = _coerce_article_documents(records)
    if not articles:
        return pd.DataFrame(columns=list(EMBEDDING_COLUMNS))

    rows: list[dict[str, object]] = []
    for batch_records in _batched_articles(articles, batch_size):
        texts = [
            _prepare_document_text(
                article.text,
                max_document_characters=max_document_characters,
            )
            for article in batch_records
        ]
        vectors = embedder.encode(texts)
        if len(vectors) != len(batch_records):
            raise ValueError("Embedder returned a different number of vectors than input texts")

        for article, vector in zip(batch_records, vectors, strict=True):
            normalized_vector = _validate_embedding_vector(vector, config=config)
            rows.append(
                {
                    "date": article.date,
                    "article_id": article.article_id,
                    "normalized_headline": article.normalized_headline,
                    "text": article.text,
                    "article_sentence_count": article.sentence_count,
                    "embedding_model": config.model_name,
                    "embedding_revision": config.model_revision,
                    "embedding_cache_key": embedding_cache_key(article, config=config),
                    "embedding_json": json.dumps(normalized_vector, separators=(",", ":")),
                }
            )
    return pd.DataFrame(rows, columns=list(EMBEDDING_COLUMNS))


def compute_topic_labels(
    records: Sequence[NewsSentimentRecord] | Sequence[_ArticleDocument],
    embeddings: Any,
    *,
    topic_labeler: TopicLabeler,
    config: TopicModelConfig,
    batch_size: int | None = None,
    max_document_characters: int | None = None,
) -> Any:
    """Return per-article/ticker BERTopic-style topic labels using the embedding cache."""
    pd = _require_pandas()
    _validate_optional_positive(batch_size, field_name="batch_size")
    _validate_optional_positive(
        max_document_characters,
        field_name="max_document_characters",
    )
    articles = _coerce_article_documents(records)
    if not articles:
        return pd.DataFrame(columns=list(TOPIC_LABEL_COLUMNS))

    embedding_by_key = {
        row["embedding_cache_key"]: _embedding_from_json(row["embedding_json"])
        for row in embeddings.to_dict(orient="records")
    }

    topic_by_article: dict[str, tuple[int, float, str]] = {}
    next_topic_offset = 0
    for batch_records in _batched_articles(articles, batch_size):
        documents: list[str] = []
        vectors: list[list[float]] = []
        cache_keys: list[str] = []
        for article in batch_records:
            key = _embedding_cache_key_from_frame(article, embeddings)
            if key not in embedding_by_key:
                raise ValueError(f"Missing embedding cache row for article key {key}")
            documents.append(
                _prepare_document_text(
                    article.text,
                    max_document_characters=max_document_characters,
                )
            )
            vectors.append(embedding_by_key[key])
            cache_keys.append(key)

        topics, probabilities = topic_labeler.fit_transform(documents, vectors)
        if len(topics) != len(batch_records) or len(probabilities) != len(batch_records):
            raise ValueError("Topic labeler returned a different number of labels than input texts")
        adjusted_topics, next_topic_offset = _offset_batch_topic_ids(
            topics,
            starting_offset=next_topic_offset,
        )
        for article, topic, probability, cache_key in zip(
            batch_records,
            adjusted_topics,
            probabilities,
            cache_keys,
            strict=True,
        ):
            topic_by_article[article_identity(article)] = (
                int(topic),
                _validate_probability(probability),
                cache_key,
            )

    rows: list[dict[str, object]] = []
    for article in articles:
        topic_id, probability, cache_key = topic_by_article[article_identity(article)]
        for ticker in article.tickers:
            rows.append(
                {
                    "date": article.date,
                    "ticker": ticker,
                    "article_id": article.article_id,
                    "normalized_headline": article.normalized_headline,
                    "text": article.text,
                    "article_sentence_count": article.sentence_count,
                    "embedding_cache_key": cache_key,
                    "topic_model": config.model_name,
                    "topic_model_version": config.model_version,
                    "topic_id": topic_id,
                    "topic_probability": probability,
                }
            )
    return pd.DataFrame(rows, columns=list(TOPIC_LABEL_COLUMNS))


def topic_labels_to_feature_records(topic_labels: Any) -> list[FeatureRecord]:
    """Aggregate article topic labels into validated ticker-day FeatureRecords."""
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
        sentence_count = group["article_sentence_count"].map(int).sum()
        records.append(
            FeatureRecord(
                date=str(date_value),
                ticker=str(ticker),
                features={
                    "nlp_article_count": int(len(group)),
                    "nlp_sentence_count": int(sentence_count),
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


def _topic_info_lookup(topic_labeler: Any | None) -> dict[int, str]:
    """Return optional topic names keyed by topic id."""
    if topic_labeler is None or not hasattr(topic_labeler, "get_topic_info"):
        return {}

    try:
        topic_info = topic_labeler.get_topic_info()
    except Exception:  # pragma: no cover - defensive against optional BERTopic APIs
        return {}

    pd = _require_pandas()
    if not isinstance(topic_info, pd.DataFrame) or "Topic" not in topic_info.columns:
        return {}

    name_column = None
    for candidate in ("Name", "Representation", "TopicName"):
        if candidate in topic_info.columns:
            name_column = candidate
            break
    if name_column is None:
        return {}

    names: dict[int, str] = {}
    for _, row in topic_info.iterrows():
        topic_value = row.get("Topic")
        if topic_value is None:
            continue
        try:
            topic_id = int(topic_value)
        except (TypeError, ValueError):
            continue
        name = str(row.get(name_column, "")).strip()
        if name:
            names[topic_id] = name
    return names


def _summarize_topic_group(
    group: Any,
    *,
    topic_id: int,
    topic_info: dict[int, str],
    topic_labeler: Any | None,
    total_rows: int,
) -> dict[str, object]:
    """Summarize one topic group for review output."""
    texts = [str(text or "") for text in group["text"].tolist() if str(text or "").strip()]
    first_row = group.iloc[0] if len(group) else None
    explicit_label = _optional_str(first_row["topic_label"]) if first_row is not None and "topic_label" in group.columns else None
    explicit_keywords = _coerce_string_list(first_row["topic_keywords"]) if first_row is not None and "topic_keywords" in group.columns else []
    keywords = explicit_keywords or _topic_keywords_for_group(
        topic_id=topic_id,
        texts=texts,
        topic_labeler=topic_labeler,
        topic_name=topic_info.get(topic_id),
    )
    representative_row = group.copy()
    representative_row["_sort_text_length"] = representative_row["text"].map(
        lambda value: len(str(value or ""))
    )
    representative_row = representative_row.sort_values(
        by=["topic_probability", "_sort_text_length", "article_id", "date"],
        ascending=[False, False, True, True],
        kind="mergesort",
    )
    primary_text = str(representative_row.iloc[0]["text"] or "") if len(representative_row) else ""
    example_texts = _representative_texts(group)
    row_count = int(len(group))
    return {
        "topic_id": topic_id,
        "topic_label": explicit_label or _format_topic_label(
            topic_id=topic_id,
            keywords=keywords,
            topic_name=topic_info.get(topic_id),
        ),
        "topic_keywords": keywords,
        "topic_keyword_text": ", ".join(keywords),
        "topic_example_text": primary_text,
        "topic_example_texts": example_texts,
        "topic_row_count": row_count,
        "topic_row_share": row_count / max(1, total_rows),
        "topic_probability_mean": float(group["topic_probability"].mean()),
        "topic_probability_max": float(group["topic_probability"].max()),
    }


def _topic_rows_with_summary(
    group: Any,
    summary: dict[str, object],
) -> list[dict[str, object]]:
    """Return row-level review payloads decorated with topic summary fields."""
    rows: list[dict[str, object]] = []
    keywords = list(summary.get("topic_keywords", []))
    example_text = summary.get("topic_example_text")
    for record in group.to_dict(orient="records"):
        rows.append(
            {
                "date": str(record.get("date", "")),
                "ticker": str(record.get("ticker", "")),
                "article_id": _optional_str(record.get("article_id")),
                "normalized_headline": _optional_str(record.get("normalized_headline")),
                "text": _optional_str(record.get("text")),
                "article_sentence_count": _optional_int(record.get("article_sentence_count")),
                "topic_id": _optional_int(record.get("topic_id")),
                "topic_probability": _optional_float(record.get("topic_probability")),
                "topic_label": str(summary.get("topic_label", "")),
                "topic_keywords": keywords,
                "topic_keyword_text": str(summary.get("topic_keyword_text", "")),
                "topic_example_text": _optional_str(example_text),
                "topic_example_texts": list(summary.get("topic_example_texts", [])),
                "topic_row_count": _optional_int(summary.get("topic_row_count")),
                "topic_row_share": _optional_float(summary.get("topic_row_share")),
            }
        )
    return rows


def _topic_row_sort_key(row: dict[str, object]) -> tuple[str, str, int]:
    """Return a stable sort key for topic review rows."""
    return (
        str(row.get("date", "")),
        str(row.get("ticker", "")),
        _optional_int(row.get("article_sentence_count")) or -1,
    )


def _topic_summary_sort_key(summary: dict[str, object]) -> tuple[float, int]:
    """Return a stable sort key for topic review summaries."""
    topic_share = _optional_float(summary.get("topic_row_share")) or 0.0
    topic_id = _optional_int(summary.get("topic_id")) or -1
    return (-topic_share, topic_id)


def _topic_keywords_for_group(
    *,
    topic_id: int,
    texts: Sequence[str],
    topic_labeler: Any | None,
    topic_name: str | None,
) -> list[str]:
    """Return up to five human-readable keywords for one topic group."""
    keywords = _extract_topic_keywords(topic_labeler, topic_id)
    if not keywords and topic_name:
        keywords = _keywords_from_topic_name(topic_name)
    if not keywords:
        keywords = _keywords_from_texts(texts)
    return keywords[:5]


def _extract_topic_keywords(topic_labeler: Any | None, topic_id: int) -> list[str]:
    """Extract keywords from an optional topic model wrapper."""
    if topic_labeler is None:
        return []
    topic_getter = getattr(topic_labeler, "get_topic", None)
    if topic_getter is None:
        return []
    try:
        topic_data = topic_getter(topic_id)
    except Exception:  # pragma: no cover - optional topic API fallback
        return []
    if not topic_data:
        return []

    keywords: list[str] = []
    for item in topic_data:
        if isinstance(item, tuple) and item:
            keyword = str(item[0]).strip()
        else:
            keyword = str(item).strip()
        if keyword:
            keywords.append(keyword)
    return keywords


def _keywords_from_topic_name(topic_name: str) -> list[str]:
    """Derive readable keywords from a topic name string."""
    cleaned = re.sub(r"^\d+[_\-\s]*", "", topic_name)
    cleaned = cleaned.replace("/", " ").replace("_", " ").replace("-", " ")
    tokens = [token for token in cleaned.split() if token]
    return [token.lower() for token in tokens if token.lower() not in _TOPIC_FALLBACK_STOPWORDS]


def _keywords_from_texts(texts: Sequence[str]) -> list[str]:
    """Derive fallback keywords from representative texts."""
    tokens: list[str] = []
    for text in texts:
        tokens.extend(
            token.lower()
            for token in re.findall(r"[A-Za-z][A-Za-z0-9'\-]+", text)
            if token.lower() not in _TOPIC_FALLBACK_STOPWORDS and len(token) > 2
        )
    counts = Counter(tokens)
    return [word for word, _ in counts.most_common(5)]


def _representative_texts(group: Any, *, limit: int = 2) -> list[str]:
    """Return up to `limit` representative texts ordered by confidence."""
    rows = group.copy()
    rows["_sort_text_length"] = rows["text"].map(lambda value: len(str(value or "")))
    rows = rows.sort_values(
        by=["topic_probability", "_sort_text_length", "article_id", "date"],
        ascending=[False, False, True, True],
        kind="mergesort",
    )
    texts: list[str] = []
    for value in rows["text"].tolist():
        text = str(value or "").strip()
        if not text or text in texts:
            continue
        texts.append(text)
        if len(texts) >= limit:
            break
    return texts


def _format_topic_label(
    *,
    topic_id: int,
    keywords: Sequence[str],
    topic_name: str | None,
) -> str:
    """Return a human-readable label for one topic."""
    if topic_name:
        label = re.sub(r"^\d+[_\-\s]*", "", topic_name).replace("_", " ").replace("-", " ")
        label = " ".join(label.split())
        if label:
            return _title_case_phrase(label)
    if keywords:
        return " / ".join(_title_case_phrase(keyword) for keyword in keywords[:3])
    if topic_id < 0:
        return "Outlier / Unassigned"
    return f"Topic {topic_id}"


def _title_case_phrase(value: str) -> str:
    """Title-case a phrase while preserving all-caps acronyms."""
    parts: list[str] = []
    for token in value.split():
        if token.isupper() or token.isdigit():
            parts.append(token)
        else:
            parts.append(token[:1].upper() + token[1:].lower())
    return " ".join(parts)


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


def embedding_cache_key(
    record: NewsSentimentRecord | _ArticleDocument,
    *,
    config: TextEmbeddingConfig,
) -> str:
    """Return a reproducible cache key for one article/model pair."""
    payload = "|".join(
        [
            article_identity(record),
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


def article_identity(record: NewsSentimentRecord | _ArticleDocument) -> str:
    """Return a stable identity for one preprocessed article document."""
    if isinstance(record, _ArticleDocument):
        payload = "|".join(
            [
                record.date,
                record.article_id,
                record.normalized_headline or "",
                record.text,
            ]
        )
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()
    payload = "|".join(
        [
            record.date,
            _stable_article_id(record),
            record.normalized_headline or "",
            record.text or "",
        ]
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _article_documents(records: Sequence[NewsSentimentRecord]) -> list[_ArticleDocument]:
    """Collapse sentence/chunk rows into deterministic article-level documents."""
    groups: dict[str, list[NewsSentimentRecord]] = {}
    for record in records:
        if not record.text:
            continue
        groups.setdefault(_article_group_key(record), []).append(record)

    articles: list[_ArticleDocument] = []
    for group_records in groups.values():
        first = sorted(group_records, key=_article_record_sort_key)[0]
        chunks = _ordered_article_chunks(group_records)
        tickers = tuple(
            sorted({record.ticker.strip().upper() for record in group_records if record.ticker})
        )
        if not chunks or not tickers:
            continue
        articles.append(
            _ArticleDocument(
                date=first.date,
                article_id=_stable_article_id(first),
                normalized_headline=first.normalized_headline,
                text=" ".join(chunks),
                tickers=tickers,
                sentence_count=len(chunks),
            )
        )
    return sorted(
        articles,
        key=lambda article: (article.date, article.article_id, article.text),
    )


def _coerce_article_documents(
    records: Sequence[NewsSentimentRecord] | Sequence[_ArticleDocument],
) -> list[_ArticleDocument]:
    """Return article documents from either preprocessed records or article documents."""
    if not records:
        return []
    first = records[0]
    if isinstance(first, _ArticleDocument):
        return list(records)  # type: ignore[arg-type]
    return _article_documents(records)  # type: ignore[arg-type]


def _embedding_cache_key_from_frame(article: _ArticleDocument, embeddings: Any) -> str:
    """Find an article embedding cache key in an embedding cache DataFrame."""
    identity = article_identity(article)
    for row in embeddings.to_dict(orient="records"):
        candidate = _ArticleDocument(
            date=str(row["date"]),
            article_id=str(row["article_id"]),
            normalized_headline=(
                None
                if row.get("normalized_headline") is None
                else str(row["normalized_headline"])
            ),
            text=str(row["text"]),
            tickers=(),
            sentence_count=int(row["article_sentence_count"]),
        )
        if article_identity(candidate) == identity:
            return str(row["embedding_cache_key"])
    raise ValueError(f"Missing embedding cache key for article identity {identity}")


def _batched_articles(
    records: Sequence[_ArticleDocument],
    batch_size: int | None,
) -> list[Sequence[_ArticleDocument]]:
    """Split article documents into deterministic batches when configured."""
    if batch_size is None or batch_size >= len(records):
        return [records]
    return [records[index : index + batch_size] for index in range(0, len(records), batch_size)]


def _prepare_document_text(
    text: str | None,
    *,
    max_document_characters: int | None,
) -> str:
    """Trim one document to a configured character budget for embedding/topic work."""
    value = str(text or "")
    if max_document_characters is None or len(value) <= max_document_characters:
        return value
    return value[:max_document_characters]


def _offset_batch_topic_ids(
    topics: Sequence[int],
    *,
    starting_offset: int,
) -> tuple[list[int], int]:
    """Offset positive batch-local topic ids so merged aggregates do not collide across batches."""
    adjusted_topics: list[int] = []
    max_positive_topic: int | None = None
    for topic in topics:
        normalized = int(topic)
        if normalized < 0:
            adjusted_topics.append(normalized)
            continue
        adjusted_topics.append(normalized + starting_offset)
        max_positive_topic = (
            normalized
            if max_positive_topic is None
            else max(max_positive_topic, normalized)
        )
    if max_positive_topic is None:
        return adjusted_topics, starting_offset
    return adjusted_topics, starting_offset + max_positive_topic + 1


def _stable_article_id(record: NewsSentimentRecord) -> str:
    """Return an article id or a deterministic fallback from sentence text."""
    if record.article_id:
        return record.article_id
    payload = "|".join(
        [
            record.date,
            record.normalized_headline or "",
            str(record.published_at.isoformat() if record.published_at else ""),
            record.url or "",
        ]
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


def _article_group_key(record: NewsSentimentRecord) -> str:
    """Return a deterministic grouping key for rows from the same article."""
    return "|".join(
        [
            record.date,
            _stable_article_id(record),
            record.normalized_headline or "",
            str(record.published_at.isoformat() if record.published_at else ""),
            record.url or "",
        ]
    )


def _article_record_sort_key(record: NewsSentimentRecord) -> tuple[int, int, str]:
    """Return source-order sort keys for preprocessed article rows."""
    source_order = record.source_text_order if record.source_text_order is not None else 0
    chunk_index = record.chunk_index if record.chunk_index is not None else (
        record.sentence_index if record.sentence_index is not None else 0
    )
    return int(source_order), int(chunk_index), str(record.text or "")


def _ordered_article_chunks(records: Sequence[NewsSentimentRecord]) -> list[str]:
    """Return deduplicated article chunks in preprocessed source order."""
    seen: set[str] = set()
    chunks: list[str] = []
    for record in sorted(records, key=_article_record_sort_key):
        text = str(record.text or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        chunks.append(text)
    return chunks


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


def _validate_optional_positive(value: int | None, *, field_name: str) -> None:
    """Require positive integers for optional batching and truncation settings."""
    if value is not None and value <= 0:
        raise ValueError(f"{field_name} must be positive when provided")


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
