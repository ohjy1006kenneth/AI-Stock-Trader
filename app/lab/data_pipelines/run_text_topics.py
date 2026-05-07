"""Modal-ready Layer 1 sentence embeddings and BERTopic label runner."""
from __future__ import annotations

import argparse
import importlib
import io
import json
import os
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Protocol

from loguru import logger

def _resolve_repo_root() -> Path:
    """Return the repository root for local runs and Modal-mounted runs."""
    env_root = os.getenv("AI_STOCK_TRADER_REPO_ROOT")
    if env_root:
        return Path(env_root).resolve()
    resolved = Path(__file__).resolve()
    return resolved.parents[3] if len(resolved.parents) > 3 else resolved.parent


_REPO_ROOT = _resolve_repo_root()
sys.path.insert(0, str(_REPO_ROOT))

from core.contracts.schemas import PipelineManifestRecord, RunStatus  # noqa: E402
from core.features.news_preprocessing import news_sentiment_frame_to_records  # noqa: E402
from core.features.text_topics import (  # noqa: E402
    SentenceEmbedder,
    TextEmbeddingConfig,
    TopicLabeler,
    TopicModelConfig,
    compute_text_topics,
    feature_records_to_frame,
)
from services.r2.paths import (  # noqa: E402
    layer1_text_embedding_path,
    layer1_topic_feature_path,
    layer1_topic_label_path,
    pipeline_manifest_path,
)
from services.r2.writer import R2Writer  # noqa: E402

TEXT_TOPICS_STAGE = "layer1_text_topics"
TEXT_MODEL_CONFIG_PATH = _REPO_ROOT / "config" / "text_models.json"


class ObjectStore(Protocol):
    """Object-store operations required by the text-topic runner."""

    def put_object(self, key: str, data: bytes | str) -> None:
        """Write an object to storage."""

    def get_object(self, key: str) -> bytes:
        """Read an object from storage."""


@dataclass(frozen=True)
class TextTopicPipelineConfig:
    """Configuration for one text embeddings and topic-label run."""

    run_id: str
    as_of_date: str
    preprocessed_news_key: str

    def __post_init__(self) -> None:
        """Validate run identity and input references."""
        if not self.run_id.strip():
            raise ValueError("run_id cannot be empty")
        try:
            datetime.strptime(self.as_of_date, "%Y-%m-%d")
        except ValueError as exc:
            raise ValueError("as_of_date must be YYYY-MM-DD") from exc
        if not self.preprocessed_news_key.strip():
            raise ValueError("preprocessed_news_key cannot be empty")


@dataclass(frozen=True)
class TextModelRuntimeConfig:
    """Modal app, secret, and model configuration loaded from repository config."""

    app_name: str
    r2_secret_name: str
    timeout_seconds: int
    python_version: str
    requirements_path: str
    embedding_config: TextEmbeddingConfig
    topic_config: TopicModelConfig
    min_topic_size: int

    def __post_init__(self) -> None:
        """Validate runtime topic-model settings."""
        if not self.app_name.strip():
            raise ValueError("app_name cannot be empty")
        if not self.r2_secret_name.strip():
            raise ValueError("r2_secret_name cannot be empty")
        if self.timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        if not self.python_version.strip():
            raise ValueError("python_version cannot be empty")
        if not self.requirements_path.strip():
            raise ValueError("requirements_path cannot be empty")
        if self.min_topic_size <= 0:
            raise ValueError("min_topic_size must be positive")


@dataclass(frozen=True)
class TextTopicPipelineResult:
    """Storage summary for one completed text-topic run."""

    run_id: str
    embedding_key: str
    topic_label_key: str
    topic_feature_key: str
    manifest_key: str
    sentence_rows: int
    embedding_rows: int
    topic_label_rows: int
    topic_feature_rows: int


def run_text_topics(
    config: TextTopicPipelineConfig,
    *,
    writer: ObjectStore | None = None,
    embedder: SentenceEmbedder | None = None,
    topic_labeler: TopicLabeler | None = None,
    runtime_config: TextModelRuntimeConfig | None = None,
) -> TextTopicPipelineResult:
    """Run Layer 1 sentence embeddings and BERTopic labels against R2 inputs."""
    active_writer = writer or R2Writer()
    runtime = runtime_config or load_text_model_runtime_config()
    active_embedder = embedder or SentenceTransformerEmbedder(runtime.embedding_config)
    active_labeler = topic_labeler or BERTopicLabeler(runtime)
    started_at = datetime.now(UTC)

    embedding_key = layer1_text_embedding_path(config.as_of_date, config.run_id)
    topic_label_key = layer1_topic_label_path(config.as_of_date, config.run_id)
    topic_feature_key = layer1_topic_feature_path(config.as_of_date, config.run_id)
    manifest_key = pipeline_manifest_path(TEXT_TOPICS_STAGE, config.run_id)
    metadata: dict[str, object] = {
        "as_of_date": config.as_of_date,
        "preprocessed_news_key": config.preprocessed_news_key,
        "embedding_key": embedding_key,
        "topic_label_key": topic_label_key,
        "topic_feature_key": topic_feature_key,
        "embedding_model": runtime.embedding_config.model_name,
        "embedding_revision": runtime.embedding_config.model_revision,
        "topic_model": runtime.topic_config.model_name,
        "topic_model_version": runtime.topic_config.model_version,
    }

    try:
        records = _load_preprocessed_news_records(active_writer, config.preprocessed_news_key)
        result = compute_text_topics(
            records,
            embedder=active_embedder,
            topic_labeler=active_labeler,
            embedding_config=runtime.embedding_config,
            topic_config=runtime.topic_config,
        )
        active_writer.put_object(embedding_key, _frame_to_parquet_bytes(result.embeddings))
        active_writer.put_object(topic_label_key, _frame_to_parquet_bytes(result.topic_labels))
        active_writer.put_object(
            topic_feature_key,
            _frame_to_parquet_bytes(feature_records_to_frame(result.feature_records)),
        )
        metadata.update(
            {
                "sentence_rows": len(records),
                "embedding_rows": len(result.embeddings),
                "topic_label_rows": len(result.topic_labels),
                "topic_feature_rows": len(result.feature_records),
            }
        )
        _write_manifest(
            writer=active_writer,
            key=manifest_key,
            config=config,
            status=RunStatus.COMPLETED,
            started_at=started_at,
            output_path=topic_feature_key,
            metadata=metadata,
        )
        logger.info("Layer 1 text-topic run complete: {}", topic_feature_key)
        return TextTopicPipelineResult(
            run_id=config.run_id,
            embedding_key=embedding_key,
            topic_label_key=topic_label_key,
            topic_feature_key=topic_feature_key,
            manifest_key=manifest_key,
            sentence_rows=len(records),
            embedding_rows=len(result.embeddings),
            topic_label_rows=len(result.topic_labels),
            topic_feature_rows=len(result.feature_records),
        )
    except Exception as exc:
        metadata["error"] = {"type": type(exc).__name__, "message": str(exc)}
        _write_manifest(
            writer=active_writer,
            key=manifest_key,
            config=config,
            status=RunStatus.FAILED,
            started_at=started_at,
            output_path=topic_feature_key,
            metadata=metadata,
        )
        logger.exception("Layer 1 text-topic run failed")
        raise


class SentenceTransformerEmbedder:
    """SentenceTransformers-backed embedder loaded only in Modal/runtime contexts."""

    def __init__(self, config: TextEmbeddingConfig) -> None:
        """Load the configured sentence-transformer model."""
        sentence_transformers = importlib.import_module("sentence_transformers")
        self._model = sentence_transformers.SentenceTransformer(
            config.model_name,
            revision=config.model_revision,
        )

    def encode(self, sentences: Sequence[str]) -> Sequence[Sequence[float]]:
        """Return normalized embeddings for the supplied sentences."""
        vectors = self._model.encode(
            list(sentences),
            normalize_embeddings=True,
            convert_to_numpy=True,
        )
        return vectors.tolist()


class BERTopicLabeler:
    """BERTopic-backed labeler loaded only in Modal/runtime contexts."""

    def __init__(self, runtime_config: TextModelRuntimeConfig) -> None:
        """Create the configured BERTopic model."""
        bertopic = importlib.import_module("bertopic")
        self._model = bertopic.BERTopic(
            min_topic_size=runtime_config.min_topic_size,
            calculate_probabilities=True,
        )

    def fit_transform(
        self,
        documents: Sequence[str],
        embeddings: Sequence[Sequence[float]],
    ) -> tuple[Sequence[int], Sequence[float]]:
        """Return one topic id and confidence per document."""
        np = importlib.import_module("numpy")
        topics, probabilities = self._model.fit_transform(
            list(documents),
            embeddings=np.asarray(embeddings),
        )
        return list(topics), _topic_probabilities(topics, probabilities)


def load_text_model_runtime_config(path: Path = TEXT_MODEL_CONFIG_PATH) -> TextModelRuntimeConfig:
    """Load Modal and model configuration from the repository config file."""
    payload = json.loads(path.read_text(encoding="utf-8"))
    return TextModelRuntimeConfig(
        app_name=str(payload["app_name"]),
        r2_secret_name=str(payload["r2_secret_name"]),
        timeout_seconds=int(payload["timeout_seconds"]),
        python_version=str(payload.get("python_version", "3.11")),
        requirements_path=str(payload.get("requirements_path", "requirements/modal.txt")),
        embedding_config=TextEmbeddingConfig(
            model_name=str(payload["embedding_model_name"]),
            model_revision=str(payload["embedding_model_revision"]),
            embedding_dimension=int(payload["embedding_dimension"]),
        ),
        topic_config=TopicModelConfig(
            model_name=str(payload["topic_model_name"]),
            model_version=str(payload["topic_model_version"]),
        ),
        min_topic_size=int(payload["min_topic_size"]),
    )


def _load_preprocessed_news_records(writer: ObjectStore, key: str) -> list[object]:
    """Load sentence-level NewsSentimentRecord rows from a preprocessing artifact."""
    try:
        import pandas as pd
    except ModuleNotFoundError as exc:
        raise ModuleNotFoundError(
            "pandas and pyarrow are required to load preprocessed news outputs."
        ) from exc
    frame = pd.read_parquet(io.BytesIO(writer.get_object(key)))
    return news_sentiment_frame_to_records(frame)


def _frame_to_parquet_bytes(frame: object) -> bytes:
    """Serialize a pandas DataFrame to Parquet bytes."""
    try:
        import pandas as pd

        importlib.import_module("pyarrow")
    except ModuleNotFoundError as exc:
        raise ModuleNotFoundError(
            "pandas and pyarrow are required to write text-topic outputs."
        ) from exc

    if not isinstance(frame, pd.DataFrame):
        raise TypeError("frame must be a pandas DataFrame")
    buffer = io.BytesIO()
    frame.to_parquet(buffer, index=False)
    return buffer.getvalue()


def _write_manifest(
    *,
    writer: ObjectStore,
    key: str,
    config: TextTopicPipelineConfig,
    status: RunStatus,
    started_at: datetime,
    output_path: str,
    metadata: dict[str, object],
) -> None:
    """Write a pipeline manifest for one text-topic run."""
    manifest = PipelineManifestRecord(
        run_id=config.run_id,
        stage=TEXT_TOPICS_STAGE,
        status=status,
        started_at=started_at,
        finished_at=datetime.now(UTC),
        input_path=config.preprocessed_news_key,
        output_path=output_path,
        metadata=metadata,
    )
    writer.put_object(key, manifest.model_dump_json())


def _topic_probabilities(topics: Sequence[int], probabilities: object) -> list[float]:
    """Normalize BERTopic probability output into one confidence per document."""
    np = importlib.import_module("numpy")
    if probabilities is None:
        return [1.0 for _ in topics]

    array = np.asarray(probabilities)
    if array.ndim == 1:
        return [float(value) for value in array.tolist()]
    if array.ndim == 2:
        return [float(max(row.tolist()) if len(row) else 0.0) for row in array]
    raise ValueError("BERTopic probabilities must be one- or two-dimensional")


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """Parse CLI arguments for local text-topic runs."""
    parser = argparse.ArgumentParser(description="Run Layer 1 text embeddings and topics.")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--as-of-date", required=True, metavar="YYYY-MM-DD")
    parser.add_argument("--preprocessed-news-key", required=True)
    return parser.parse_args(argv)


def _config_from_args(args: argparse.Namespace) -> TextTopicPipelineConfig:
    """Build a validated pipeline config from CLI arguments."""
    return TextTopicPipelineConfig(
        run_id=args.run_id,
        as_of_date=args.as_of_date,
        preprocessed_news_key=args.preprocessed_news_key,
    )


def main(argv: Sequence[str] | None = None) -> int:
    """Run text embedding and topic labeling from the local command line."""
    result = run_text_topics(_config_from_args(_parse_args(argv)))
    logger.info("Manifest written to {}", result.manifest_key)
    return 0


def modal_main(run_id: str, as_of_date: str, preprocessed_news_key: str) -> None:
    """Submit a text-topic run to Modal from the local CLI."""
    globals()["modal_run_text_topics"].remote(
        run_id=run_id,
        as_of_date=as_of_date,
        preprocessed_news_key=preprocessed_news_key,
    )


def _define_modal_app() -> object | None:
    """Create the Modal app when the modal package is installed."""
    try:
        modal = importlib.import_module("modal")
    except ModuleNotFoundError:
        return None

    runtime = load_text_model_runtime_config()
    image = modal.Image.debian_slim(
        python_version=runtime.python_version
    ).pip_install_from_requirements(runtime.requirements_path)
    app = modal.App(runtime.app_name)

    @app.function(
        image=image,
        secrets=[modal.Secret.from_name(runtime.r2_secret_name)],
        timeout=runtime.timeout_seconds,
        serialized=True,
    )
    def modal_run_text_topics(
        run_id: str,
        as_of_date: str,
        preprocessed_news_key: str,
    ) -> dict[str, object]:
        """Run text embeddings and BERTopic labels on Modal."""
        result = run_text_topics(
            TextTopicPipelineConfig(
                run_id=run_id,
                as_of_date=as_of_date,
                preprocessed_news_key=preprocessed_news_key,
            ),
            runtime_config=runtime,
        )
        return {
            "run_id": result.run_id,
            "embedding_key": result.embedding_key,
            "topic_label_key": result.topic_label_key,
            "topic_feature_key": result.topic_feature_key,
            "manifest_key": result.manifest_key,
            "sentence_rows": result.sentence_rows,
            "embedding_rows": result.embedding_rows,
            "topic_label_rows": result.topic_label_rows,
            "topic_feature_rows": result.topic_feature_rows,
        }

    app.local_entrypoint()(modal_main)
    globals()["modal_run_text_topics"] = modal_run_text_topics
    return app


app = _define_modal_app()


if __name__ == "__main__":
    sys.exit(main())
