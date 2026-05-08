"""Modal-ready Layer 1 NLP preprocessing runner for raw news archives."""
from __future__ import annotations

import argparse
import csv
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
    NewsPreprocessingConfig,
    preprocess_news_articles,
    records_to_news_sentiment_frame,
)
from services.r2.paths import (  # noqa: E402
    layer1_news_preprocessing_path,
    pipeline_manifest_path,
    raw_news_path,
    raw_universe_path,
)
from services.r2.writer import R2Writer  # noqa: E402

NLP_PREPROCESSING_STAGE = "layer1_news_preprocessing"
MODAL_CONFIG_PATH = _REPO_ROOT / "config" / "news_preprocessing.json"


class ObjectStore(Protocol):
    """Object-store operations required by the news preprocessing runner."""

    def put_object(self, key: str, data: bytes | str) -> None:
        """Write an object to storage."""

    def get_object(self, key: str) -> bytes:
        """Read an object from storage."""


@dataclass(frozen=True)
class NewsPreprocessingPipelineConfig:
    """Configuration for one raw-news preprocessing run."""

    run_id: str
    as_of_date: str
    min_sentence_chars: int = 2

    def __post_init__(self) -> None:
        """Validate run identity and date settings."""
        if not self.run_id.strip():
            raise ValueError("run_id cannot be empty")
        try:
            datetime.strptime(self.as_of_date, "%Y-%m-%d")
        except ValueError as exc:
            raise ValueError("as_of_date must be YYYY-MM-DD") from exc
        if self.min_sentence_chars <= 0:
            raise ValueError("min_sentence_chars must be positive")


@dataclass(frozen=True)
class NewsPreprocessingPipelineResult:
    """Storage summary for one completed news preprocessing run."""

    run_id: str
    output_key: str
    manifest_key: str
    article_rows: int
    sentence_rows: int


@dataclass(frozen=True)
class ModalRuntimeConfig:
    """Modal app configuration loaded from repository config."""

    app_name: str
    r2_secret_name: str
    timeout_seconds: int


def run_news_preprocessing(
    config: NewsPreprocessingPipelineConfig,
    *,
    writer: ObjectStore | None = None,
) -> NewsPreprocessingPipelineResult:
    """Run Layer 1 news preprocessing against raw R2 news/universe archives."""
    active_writer = writer or R2Writer()
    started_at = datetime.now(UTC)
    output_key = news_preprocessing_output_path(config.run_id, config.as_of_date)
    manifest_key = pipeline_manifest_path(NLP_PREPROCESSING_STAGE, config.run_id)
    metadata: dict[str, object] = {
        "as_of_date": config.as_of_date,
        "raw_news_key": raw_news_path(config.as_of_date),
        "raw_universe_key": raw_universe_path(config.as_of_date),
        "output_key": output_key,
    }

    try:
        articles = _load_raw_news_articles(active_writer, config.as_of_date)
        tickers = _load_point_in_time_tickers(active_writer, config.as_of_date)
        records = preprocess_news_articles(
            articles,
            as_of_date=config.as_of_date,
            point_in_time_tickers=tickers,
            config=NewsPreprocessingConfig(min_sentence_chars=config.min_sentence_chars),
        )
        active_writer.put_object(output_key, _records_to_parquet_bytes(records))
        metadata.update({"article_rows": len(articles), "sentence_rows": len(records)})
        _write_manifest(
            writer=active_writer,
            key=manifest_key,
            config=config,
            status=RunStatus.COMPLETED,
            started_at=started_at,
            output_key=output_key,
            metadata=metadata,
        )
        logger.info("Layer 1 news preprocessing complete: {}", output_key)
        return NewsPreprocessingPipelineResult(
            run_id=config.run_id,
            output_key=output_key,
            manifest_key=manifest_key,
            article_rows=len(articles),
            sentence_rows=len(records),
        )
    except Exception as exc:
        metadata["error"] = {"type": type(exc).__name__, "message": str(exc)}
        _write_manifest(
            writer=active_writer,
            key=manifest_key,
            config=config,
            status=RunStatus.FAILED,
            started_at=started_at,
            output_key=output_key,
            metadata=metadata,
        )
        logger.exception("Layer 1 news preprocessing failed")
        raise


def news_preprocessing_output_path(run_id: str, as_of_date: str) -> str:
    """Return the canonical R2 output key for preprocessed news rows."""
    return layer1_news_preprocessing_path(as_of_date, run_id)


def load_modal_runtime_config(path: Path = MODAL_CONFIG_PATH) -> ModalRuntimeConfig:
    """Load Modal app and secret names from repository config."""
    payload = json.loads(path.read_text(encoding="utf-8"))
    return ModalRuntimeConfig(
        app_name=str(payload["app_name"]),
        r2_secret_name=str(payload["r2_secret_name"]),
        timeout_seconds=int(payload["timeout_seconds"]),
    )


def _load_raw_news_articles(writer: ObjectStore, as_of_date: str) -> list[dict[str, object]]:
    """Load raw JSONL news articles for one date from R2."""
    payload = writer.get_object(raw_news_path(as_of_date)).decode("utf-8")
    rows: list[dict[str, object]] = []
    for line_number, line in enumerate(payload.splitlines(), start=1):
        if not line.strip():
            continue
        parsed = json.loads(line)
        if not isinstance(parsed, dict):
            raise ValueError(f"Raw news line {line_number} must decode to an object")
        rows.append(parsed)
    return rows


def _load_point_in_time_tickers(writer: ObjectStore, as_of_date: str) -> list[str]:
    """Load tickers eligible for Layer 1 processing from the raw universe mask."""
    payload = writer.get_object(raw_universe_path(as_of_date)).decode("utf-8")
    tickers: list[str] = []
    for row in csv.DictReader(io.StringIO(payload)):
        if (
            _truthy(row.get("in_universe"))
            and _truthy(row.get("tradable"), default=True)
            and _truthy(row.get("liquid"), default=True)
            and _truthy(row.get("data_quality_ok"), default=True)
            and not _truthy(row.get("halted"))
        ):
            tickers.append(str(row["ticker"]))
    return tickers


def _records_to_parquet_bytes(records: Sequence[object]) -> bytes:
    """Serialize preprocessed news records to Parquet bytes."""
    try:
        importlib.import_module("pyarrow")
    except ModuleNotFoundError as exc:
        raise ModuleNotFoundError(
            "pandas and pyarrow are required to write preprocessed news outputs."
        ) from exc

    frame = records_to_news_sentiment_frame(records)  # type: ignore[arg-type]
    buffer = io.BytesIO()
    frame.to_parquet(buffer, index=False)
    return buffer.getvalue()


def _write_manifest(
    *,
    writer: ObjectStore,
    key: str,
    config: NewsPreprocessingPipelineConfig,
    status: RunStatus,
    started_at: datetime,
    output_key: str,
    metadata: dict[str, object],
) -> None:
    """Write a pipeline manifest for one news preprocessing run."""
    manifest = PipelineManifestRecord(
        run_id=config.run_id,
        stage=NLP_PREPROCESSING_STAGE,
        status=status,
        started_at=started_at,
        finished_at=datetime.now(UTC),
        input_path=f"{raw_news_path(config.as_of_date)},{raw_universe_path(config.as_of_date)}",
        output_path=output_key,
        metadata=metadata,
    )
    writer.put_object(key, manifest.model_dump_json())


def _truthy(value: str | None, *, default: bool = False) -> bool:
    """Return True for common CSV boolean truth values."""
    if value is None:
        return default
    normalized = str(value).strip().lower()
    if not normalized:
        return default
    return normalized in {"1", "true", "t", "yes", "y"}


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """Parse CLI arguments for local news preprocessing runs."""
    parser = argparse.ArgumentParser(description="Run Layer 1 news preprocessing.")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--as-of-date", required=True, metavar="YYYY-MM-DD")
    parser.add_argument("--min-sentence-chars", type=int, default=2)
    return parser.parse_args(argv)


def _config_from_args(args: argparse.Namespace) -> NewsPreprocessingPipelineConfig:
    """Build a validated pipeline config from CLI arguments."""
    return NewsPreprocessingPipelineConfig(
        run_id=args.run_id,
        as_of_date=args.as_of_date,
        min_sentence_chars=args.min_sentence_chars,
    )


def main(argv: Sequence[str] | None = None) -> int:
    """Run news preprocessing from the local command line."""
    result = run_news_preprocessing(_config_from_args(_parse_args(argv)))
    logger.info("Manifest written to {}", result.manifest_key)
    return 0


def modal_main(run_id: str, as_of_date: str, min_sentence_chars: int = 2) -> None:
    """Submit a news preprocessing run to Modal from the local CLI."""
    globals()["modal_run_news_preprocessing"].remote(
        run_id=run_id,
        as_of_date=as_of_date,
        min_sentence_chars=min_sentence_chars,
    )


def _define_modal_app() -> object | None:
    """Create the Modal app when the modal package is installed."""
    try:
        modal = importlib.import_module("modal")
    except ModuleNotFoundError:
        return None

    runtime = load_modal_runtime_config()
    image = modal.Image.debian_slim(python_version="3.11").pip_install_from_requirements(
        "requirements/modal.txt"
    )
    app = modal.App(runtime.app_name)

    @app.function(
        image=image,
        secrets=[modal.Secret.from_name(runtime.r2_secret_name)],
        timeout=runtime.timeout_seconds,
        serialized=True,
    )
    def modal_run_news_preprocessing(
        run_id: str,
        as_of_date: str,
        min_sentence_chars: int = 2,
    ) -> dict[str, object]:
        """Run Layer 1 news preprocessing on Modal."""
        result = run_news_preprocessing(
            NewsPreprocessingPipelineConfig(
                run_id=run_id,
                as_of_date=as_of_date,
                min_sentence_chars=min_sentence_chars,
            )
        )
        return {
            "run_id": result.run_id,
            "output_key": result.output_key,
            "manifest_key": result.manifest_key,
            "article_rows": result.article_rows,
            "sentence_rows": result.sentence_rows,
        }

    app.local_entrypoint()(modal_main)
    globals()["modal_run_news_preprocessing"] = modal_run_news_preprocessing
    return app


app = _define_modal_app()


if __name__ == "__main__":
    sys.exit(main())
