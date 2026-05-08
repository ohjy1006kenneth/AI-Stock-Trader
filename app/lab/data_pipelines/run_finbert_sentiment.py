"""Modal-ready Layer 1 FinBERT sentiment scoring runner."""
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
from core.features.news_preprocessing import (  # noqa: E402
    news_sentiment_frame_to_records,
    records_to_news_sentiment_frame,
)
from core.features.sentiment_features import (  # noqa: E402
    SentimentScore,
    SentimentScorer,
    load_source_credibility_config,
    score_news_sentiment,
    sentiment_feature_records_from_scored_news,
    sentiment_feature_records_to_frame,
)
from services.r2.paths import (  # noqa: E402
    layer1_sentiment_feature_path,
    layer1_sentiment_score_path,
    pipeline_manifest_path,
)
from services.r2.writer import R2Writer  # noqa: E402

FINBERT_SENTIMENT_STAGE = "layer1_finbert_sentiment"
FINBERT_CONFIG_PATH = _REPO_ROOT / "config" / "finbert_sentiment.json"


class ObjectStore(Protocol):
    """Object-store operations required by the FinBERT runner."""

    def put_object(self, key: str, data: bytes | str) -> None:
        """Write an object to storage."""

    def get_object(self, key: str) -> bytes:
        """Read an object from storage."""


@dataclass(frozen=True)
class FinBERTPipelineConfig:
    """Configuration for one FinBERT sentiment scoring run."""

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
class FinBERTModelRuntimeConfig:
    """Modal app, secret, and FinBERT model configuration."""

    app_name: str
    r2_secret_name: str
    timeout_seconds: int
    python_version: str
    requirements_path: str
    model_name: str
    model_revision: str
    batch_size: int
    default_relevance_score: float
    bucket_timezone: str
    source_credibility_config_path: Path
    device: int

    def __post_init__(self) -> None:
        """Validate runtime model settings."""
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
        if not self.model_name.strip():
            raise ValueError("model_name cannot be empty")
        if not self.model_revision.strip():
            raise ValueError("model_revision cannot be empty")
        if self.batch_size <= 0:
            raise ValueError("batch_size must be positive")
        if self.default_relevance_score < 0.0:
            raise ValueError("default_relevance_score must be non-negative")
        if not self.bucket_timezone.strip():
            raise ValueError("bucket_timezone cannot be empty")


@dataclass(frozen=True)
class FinBERTPipelineResult:
    """Storage summary for one completed FinBERT sentiment run."""

    run_id: str
    scored_news_key: str
    sentiment_feature_key: str
    manifest_key: str
    input_rows: int
    scored_rows: int
    feature_rows: int


def run_finbert_sentiment(
    config: FinBERTPipelineConfig,
    *,
    writer: ObjectStore | None = None,
    scorer: SentimentScorer | None = None,
    runtime_config: FinBERTModelRuntimeConfig | None = None,
) -> FinBERTPipelineResult:
    """Run Layer 1 FinBERT scoring and ticker-day sentiment aggregation."""
    active_writer = writer or R2Writer()
    runtime = runtime_config or load_finbert_runtime_config()
    active_scorer = scorer or FinBERTScorer(runtime)
    started_at = datetime.now(UTC)

    scored_news_key = layer1_sentiment_score_path(config.as_of_date, config.run_id)
    sentiment_feature_key = layer1_sentiment_feature_path(config.as_of_date, config.run_id)
    manifest_key = pipeline_manifest_path(FINBERT_SENTIMENT_STAGE, config.run_id)
    metadata: dict[str, object] = {
        "as_of_date": config.as_of_date,
        "preprocessed_news_key": config.preprocessed_news_key,
        "scored_news_key": scored_news_key,
        "sentiment_feature_key": sentiment_feature_key,
        "model_name": runtime.model_name,
        "model_revision": runtime.model_revision,
        "bucket_timezone": runtime.bucket_timezone,
        "source_credibility_config_path": str(runtime.source_credibility_config_path),
    }

    try:
        records = _load_preprocessed_news_records(active_writer, config.preprocessed_news_key)
        scored_records = score_news_sentiment(
            records,
            scorer=active_scorer,
            batch_size=runtime.batch_size,
            default_relevance_score=runtime.default_relevance_score,
        )
        scored_frame = records_to_news_sentiment_frame(scored_records)
        credibility_config = load_source_credibility_config(
            runtime.source_credibility_config_path
        )
        feature_records = sentiment_feature_records_from_scored_news(
            scored_frame,
            credibility_config=credibility_config,
            bucket_timezone=runtime.bucket_timezone,
        )

        active_writer.put_object(scored_news_key, _frame_to_parquet_bytes(scored_frame))
        active_writer.put_object(
            sentiment_feature_key,
            _frame_to_parquet_bytes(sentiment_feature_records_to_frame(feature_records)),
        )
        metadata.update(
            {
                "input_rows": len(records),
                "scored_rows": len(scored_records),
                "feature_rows": len(feature_records),
            }
        )
        _write_manifest(
            writer=active_writer,
            key=manifest_key,
            config=config,
            status=RunStatus.COMPLETED,
            started_at=started_at,
            output_path=sentiment_feature_key,
            metadata=metadata,
        )
        logger.info("Layer 1 FinBERT sentiment run complete: {}", sentiment_feature_key)
        return FinBERTPipelineResult(
            run_id=config.run_id,
            scored_news_key=scored_news_key,
            sentiment_feature_key=sentiment_feature_key,
            manifest_key=manifest_key,
            input_rows=len(records),
            scored_rows=len(scored_records),
            feature_rows=len(feature_records),
        )
    except Exception as exc:
        metadata["error"] = {"type": type(exc).__name__, "message": str(exc)}
        _write_manifest(
            writer=active_writer,
            key=manifest_key,
            config=config,
            status=RunStatus.FAILED,
            started_at=started_at,
            output_path=sentiment_feature_key,
            metadata=metadata,
        )
        logger.exception("Layer 1 FinBERT sentiment run failed")
        raise


class FinBERTScorer:
    """Transformers-backed FinBERT scorer loaded only in Modal/runtime contexts."""

    def __init__(self, runtime_config: FinBERTModelRuntimeConfig) -> None:
        """Load the configured FinBERT model pipeline."""
        transformers = importlib.import_module("transformers")
        self._batch_size = runtime_config.batch_size
        self._pipeline = transformers.pipeline(
            "text-classification",
            model=runtime_config.model_name,
            tokenizer=runtime_config.model_name,
            revision=runtime_config.model_revision,
            top_k=None,
            device=runtime_config.device,
        )

    def score(self, texts: Sequence[str]) -> Sequence[SentimentScore]:
        """Return FinBERT probabilities for each input text."""
        outputs = self._pipeline(
            list(texts),
            truncation=True,
            batch_size=self._batch_size,
        )
        if outputs and isinstance(outputs[0], dict):
            outputs = [outputs]
        return [_score_from_model_output(output) for output in outputs]


def load_finbert_runtime_config(path: Path = FINBERT_CONFIG_PATH) -> FinBERTModelRuntimeConfig:
    """Load Modal and FinBERT model configuration from the repository config file."""
    payload = json.loads(path.read_text(encoding="utf-8"))
    source_config = payload.get("source_credibility_config_path", "config/source_credibility.json")
    return FinBERTModelRuntimeConfig(
        app_name=str(payload["app_name"]),
        r2_secret_name=str(payload["r2_secret_name"]),
        timeout_seconds=int(payload["timeout_seconds"]),
        python_version=str(payload.get("python_version", "3.11")),
        requirements_path=str(payload.get("requirements_path", "requirements/modal.txt")),
        model_name=str(payload["model_name"]),
        model_revision=str(payload["model_revision"]),
        batch_size=int(payload["batch_size"]),
        default_relevance_score=float(payload["default_relevance_score"]),
        bucket_timezone=str(payload["bucket_timezone"]),
        source_credibility_config_path=(_REPO_ROOT / str(source_config)).resolve(),
        device=int(payload.get("device", -1)),
    )


def _score_from_model_output(output: object) -> SentimentScore:
    """Normalize one transformers pipeline output into SentimentScore."""
    if not isinstance(output, Sequence):
        raise ValueError("FinBERT output must be a sequence of label scores")

    scores: dict[str, float] = {}
    for item in output:
        if not isinstance(item, dict):
            raise ValueError("FinBERT label scores must be mappings")
        label = str(item.get("label", "")).strip().lower()
        score = float(item.get("score", 0.0))
        scores[label] = score

    missing = sorted({"positive", "negative", "neutral"} - set(scores))
    if missing:
        raise ValueError(f"FinBERT output missing labels: {missing}")
    return SentimentScore(
        positive=scores["positive"],
        negative=scores["negative"],
        neutral=scores["neutral"],
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
            "pandas and pyarrow are required to write FinBERT sentiment outputs."
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
    config: FinBERTPipelineConfig,
    status: RunStatus,
    started_at: datetime,
    output_path: str,
    metadata: dict[str, object],
) -> None:
    """Write a pipeline manifest for one FinBERT sentiment run."""
    manifest = PipelineManifestRecord(
        run_id=config.run_id,
        stage=FINBERT_SENTIMENT_STAGE,
        status=status,
        started_at=started_at,
        finished_at=datetime.now(UTC),
        input_path=config.preprocessed_news_key,
        output_path=output_path,
        metadata=metadata,
    )
    writer.put_object(key, manifest.model_dump_json())


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """Parse CLI arguments for local FinBERT sentiment runs."""
    parser = argparse.ArgumentParser(description="Run Layer 1 FinBERT sentiment.")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--as-of-date", required=True, metavar="YYYY-MM-DD")
    parser.add_argument("--preprocessed-news-key", required=True)
    return parser.parse_args(argv)


def _config_from_args(args: argparse.Namespace) -> FinBERTPipelineConfig:
    """Build a validated pipeline config from CLI arguments."""
    return FinBERTPipelineConfig(
        run_id=args.run_id,
        as_of_date=args.as_of_date,
        preprocessed_news_key=args.preprocessed_news_key,
    )


def main(argv: Sequence[str] | None = None) -> int:
    """Run FinBERT sentiment scoring from the local command line."""
    result = run_finbert_sentiment(_config_from_args(_parse_args(argv)))
    logger.info("Manifest written to {}", result.manifest_key)
    return 0


def modal_main(run_id: str, as_of_date: str, preprocessed_news_key: str) -> None:
    """Submit a FinBERT sentiment run to Modal from the local CLI."""
    globals()["modal_run_finbert_sentiment"].remote(
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

    runtime = load_finbert_runtime_config()
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
    def modal_run_finbert_sentiment(
        run_id: str,
        as_of_date: str,
        preprocessed_news_key: str,
    ) -> dict[str, object]:
        """Run FinBERT sentiment scoring on Modal."""
        result = run_finbert_sentiment(
            FinBERTPipelineConfig(
                run_id=run_id,
                as_of_date=as_of_date,
                preprocessed_news_key=preprocessed_news_key,
            ),
            runtime_config=runtime,
        )
        return {
            "run_id": result.run_id,
            "scored_news_key": result.scored_news_key,
            "sentiment_feature_key": result.sentiment_feature_key,
            "manifest_key": result.manifest_key,
            "input_rows": result.input_rows,
            "scored_rows": result.scored_rows,
            "feature_rows": result.feature_rows,
        }

    app.local_entrypoint()(modal_main)
    globals()["modal_run_finbert_sentiment"] = modal_run_finbert_sentiment
    return app


app = _define_modal_app()


if __name__ == "__main__":
    sys.exit(main())
