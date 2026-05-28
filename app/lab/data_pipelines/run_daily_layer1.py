"""Single-command Layer 1 orchestration from Layer 0 archives through validation."""
from __future__ import annotations

import argparse
import csv
import importlib
import io
import json
import os
import sys
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from datetime import date as Date
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

import app.lab.data_pipelines.run_finbert_sentiment as finbert_module  # noqa: E402
import app.lab.data_pipelines.run_hmm_regime_detection as regime_module  # noqa: E402
import app.lab.data_pipelines.run_news_preprocessing as news_module  # noqa: E402
import app.lab.data_pipelines.run_text_topics as text_topics_module  # noqa: E402
from app.lab.data_pipelines.run_finbert_sentiment import (  # noqa: E402
    FINBERT_SENTIMENT_STAGE,
    FinBERTPipelineConfig,
    FinBERTPipelineResult,
    run_finbert_sentiment,
)
from app.lab.data_pipelines.run_hmm_regime_detection import (  # noqa: E402
    REGIME_STAGE,
    HMMRegimePipelineConfig,
    HMMRegimePipelineResult,
    run_hmm_regime_detection,
)
from app.lab.data_pipelines.run_news_preprocessing import (  # noqa: E402
    NLP_PREPROCESSING_STAGE,
    NewsPreprocessingPipelineConfig,
    NewsPreprocessingPipelineResult,
    run_news_preprocessing,
)
from app.lab.data_pipelines.run_text_topics import (  # noqa: E402
    TEXT_TOPICS_STAGE,
    TextTopicPipelineConfig,
    TextTopicPipelineResult,
    run_text_topics,
)
from app.lab.data_pipelines.validate_layer1_archive import (  # noqa: E402
    DEFAULT_REPORT_DIR,
    Layer1ValidationReport,
    build_layer1_output_prefixes,
    render_validation_report,
    validate_layer1_archive,
    write_validation_report,
)
from core.contracts.schemas import (  # noqa: E402
    FeatureRecord,
    PipelineManifestRecord,
    RunStatus,
)
from core.features.assembly import (  # noqa: E402
    Layer1FeatureInput,
    assemble_layer1_feature_records,
)
from core.features.context_features import (  # noqa: E402
    compute_context_features,
    context_features_to_records,
)
from core.features.io import (  # noqa: E402
    parquet_bytes_to_feature_records,
    read_feature_records,
    write_feature_record,
    write_feature_records,
)
from core.features.loaders import (  # noqa: E402
    available_macro_series_by_date,
    load_fundamentals_frame,
    load_macro_frame,
    load_ohlcv_frame,
    load_order_book_frame,
)
from core.features.macro_features import compute_macro_features  # noqa: E402
from core.features.market_features import (  # noqa: E402
    compute_market_features,
    market_features_to_records,
)
from core.features.order_book_features import (  # noqa: E402
    compute_order_book_features,
    order_book_features_to_records,
)
from core.features.regime_detection import regime_features_to_records  # noqa: E402
from core.features.sector_features import (  # noqa: E402
    compute_sector_features,
    load_sector_etf_config,
    sector_features_to_records,
)
from services.order_book.config import load_order_book_feature_config  # noqa: E402
from services.modal.secrets import (  # noqa: E402
    SIMFIN_MODAL_ENV_FILE,
    SIMFIN_MODAL_ENV_KEYS,
    build_modal_secrets,
)
from services.r2.paths import (  # noqa: E402
    layer1_ticker_history_path,
    pipeline_manifest_path,
    raw_fundamentals_path,
    raw_news_path,
    raw_order_book_path,
    raw_price_path,
    raw_universe_path,
)
from services.r2.writer import R2Writer  # noqa: E402

LAYER1_DAILY_STAGE = "layer1"
MODAL_CONFIG_PATH = _REPO_ROOT / "config" / "modal.json"
MODAL_REPO_ROOT = "/workspace/AI-Stock-Trader"
# Sentinel for batch historical assembly: intentionally pre-dates any real trade date
# so assemble_layer1_feature_records's no-leakage guard always passes. Point-in-time
# safety is enforced by the Layer 0 archive/manifest checks rather than these branch timestamps.
SENTINEL_ASSEMBLY_AS_OF = datetime(1900, 1, 1, tzinfo=UTC)
_modal_run_daily_layer1: ModalRemoteFunction | None = None
_modal_run_batched_layer1: ModalRangeRemoteFunction | None = None


class ObjectStore(Protocol):
    """Object-store operations required by the Layer 1 daily orchestrator."""

    def exists(self, key: str) -> bool:
        """Return True when the object exists."""

    def get_object(self, key: str) -> bytes:
        """Read an object payload by key."""

    def list_keys(self, prefix: str) -> list[str]:
        """List object keys by prefix."""

    def put_object(self, key: str, data: bytes | str) -> None:
        """Persist an object payload."""


class ModalRemoteFunction(Protocol):
    """Minimal Modal remote-call surface used by the Pi entrypoint."""

    def remote(
        self,
        *,
        run_id: str,
        as_of_date: str,
        layer0_run_id: str,
        benchmark_ticker: str = "SPY",
        allow_layer0_manifest_date_range: bool = False,
        min_sentence_chars: int = 2,
        hmm_train_start_date: str | None = None,
        hmm_max_iterations: int = 100,
        hmm_min_training_rows: int = 30,
        preprocessed_news_key: str | None = None,
        topic_feature_key: str | None = None,
        sentiment_feature_key: str | None = None,
        regime_output_key: str | None = None,
    ) -> dict[str, object]:
        """Submit the configured Modal function asynchronously."""


class ModalRangeRemoteFunction(Protocol):
    """Minimal Modal remote-call surface used by batched readiness runs."""

    def remote(
        self,
        *,
        run_id: str,
        from_date: str,
        to_date: str,
        layer0_run_id: str,
        benchmark_ticker: str = "SPY",
        allow_layer0_manifest_date_range: bool = False,
        min_sentence_chars: int = 2,
        hmm_train_start_date: str | None = None,
        hmm_max_iterations: int = 100,
        hmm_min_training_rows: int = 30,
    ) -> dict[str, object]:
        """Submit the configured batched Modal function asynchronously."""


class StageModalFunctionCall(Protocol):
    """Handle returned by Modal `.spawn()` calls."""

    def get(self, timeout: float | None = None) -> dict[str, object]:
        """Wait for the spawned stage call and return its payload."""


class StageModalRemoteFunction(Protocol):
    """Generic Modal call surface for one stage runner."""

    def remote(self, **kwargs: object) -> dict[str, object]:
        """Run the configured stage and return its summary payload."""

    def spawn(self, **kwargs: object) -> StageModalFunctionCall:
        """Start the stage asynchronously and return a handle for its payload."""


NewsRunner = Callable[
    [NewsPreprocessingPipelineConfig], NewsPreprocessingPipelineResult
]
TextTopicRunner = Callable[[TextTopicPipelineConfig], TextTopicPipelineResult]
FinBERTRunner = Callable[[FinBERTPipelineConfig], FinBERTPipelineResult]
RegimeRunner = Callable[[HMMRegimePipelineConfig], HMMRegimePipelineResult]


@dataclass(frozen=True)
class Layer1DailyConfig:
    """Configuration for one Layer 1 orchestration run."""

    run_id: str
    from_date: str
    to_date: str
    layer0_run_id: str | None = None
    tickers: tuple[str, ...] = ()
    benchmark_ticker: str = "SPY"
    allow_layer0_manifest_date_range: bool = False
    min_sentence_chars: int = 2
    hmm_train_start_date: str | None = None
    hmm_max_iterations: int = 100
    hmm_min_training_rows: int = 30

    def __post_init__(self) -> None:
        """Validate run identifiers, dates, and optional ticker scope."""
        if not self.run_id.strip():
            raise ValueError("run_id cannot be empty")
        _validate_iso_date(self.from_date, "from_date")
        _validate_iso_date(self.to_date, "to_date")
        if self.from_date > self.to_date:
            raise ValueError("from_date must be <= to_date")
        if self.layer0_run_id is not None and not self.layer0_run_id.strip():
            raise ValueError("layer0_run_id cannot be empty when provided")
        if not self.benchmark_ticker.strip():
            raise ValueError("benchmark_ticker cannot be empty")
        if self.min_sentence_chars <= 0:
            raise ValueError("min_sentence_chars must be positive")
        if self.hmm_train_start_date is not None:
            _validate_iso_date(self.hmm_train_start_date, "hmm_train_start_date")
            if self.hmm_train_start_date >= self.from_date:
                raise ValueError("hmm_train_start_date must be before from_date")
        if self.hmm_max_iterations <= 0:
            raise ValueError("hmm_max_iterations must be positive")
        if self.hmm_min_training_rows <= 0:
            raise ValueError("hmm_min_training_rows must be positive")
        for ticker in self.tickers:
            if not ticker.strip():
                raise ValueError("tickers cannot contain empty strings")


@dataclass(frozen=True)
class Layer1DailyResult:
    """Summary of one completed Layer 1 orchestration run."""

    run_id: str
    manifest_key: str
    validation_report_path: Path
    validation_report_key: str
    processed_dates: tuple[str, ...]
    tickers_processed: int
    history_files_written: int
    feature_rows_written: int
    ready_for_layer2: bool


@dataclass(frozen=True)
class ModalRuntimeConfig:
    """Modal app configuration loaded from repository config."""

    app_name: str
    r2_secret_name: str
    timeout_seconds: int
    batch_timeout_seconds: int
    batch_gpu_type: str | None
    hmm_train_lookback_bdays: int | None
    python_version: str
    requirements_path: str


@dataclass(frozen=True)
class ModalBatchedStageOutputs:
    """Completed branch output keys for one batched remote Layer 1 run."""

    news_output_keys_by_date: dict[str, str]
    topic_output_keys_by_date: dict[str, str]
    sentiment_output_keys_by_date: dict[str, str]
    regime_output_keys_by_date: dict[str, str]


@dataclass(frozen=True)
class OptionalOrderBookBranch:
    """Loaded optional order-book FeatureRecords keyed by ticker."""

    enabled: bool
    provider: str | None
    records_by_ticker: dict[str, tuple[FeatureRecord, ...]]
    archive_keys: tuple[str, ...]
    missing_dates: tuple[str, ...]


class Layer1ValidationError(RuntimeError):
    """Raised when Layer 1 validation completes but is not ready for Layer 2."""

    def __init__(
        self,
        report: Layer1ValidationReport | str,
        report_path: Path | str | None = None,
    ) -> None:
        """Capture the failing validation report."""
        resolved_report: Layer1ValidationReport | None
        resolved_report_path = (
            Path(report_path) if isinstance(report_path, str) else report_path
        )
        if isinstance(report, Layer1ValidationReport):
            summary = _validation_failure_summary(report)
            message = f"Layer 1 validation failed: {summary}"
            if resolved_report_path is not None:
                message += f" (report={resolved_report_path})"
            resolved_report = report
        else:
            message = str(report)
            resolved_report = None
        super().__init__(message)
        self.report = resolved_report
        self.report_path = resolved_report_path

    def __reduce__(
        self,
    ) -> tuple[
        type[Layer1ValidationError],
        tuple[Layer1ValidationReport | str, Path | None],
    ]:
        """Preserve structured report context across remote exception serialization."""
        return (
            type(self),
            (
                self.report if self.report is not None else str(self),
                self.report_path,
            ),
        )


def run_daily_layer1(
    config: Layer1DailyConfig,
    *,
    writer: ObjectStore | None = None,
    news_runner: Callable[..., NewsPreprocessingPipelineResult] = run_news_preprocessing,
    text_topic_runner: Callable[..., TextTopicPipelineResult] = run_text_topics,
    finbert_runner: Callable[..., FinBERTPipelineResult] = run_finbert_sentiment,
    regime_runner: Callable[..., HMMRegimePipelineResult] = run_hmm_regime_detection,
    validation_output_dir: Path | None = None,
    now: datetime | None = None,
) -> Layer1DailyResult:
    """Run Layer 1 end-to-end using only existing Layer 0 archives."""
    active_writer = writer or R2Writer()
    started_at = (now or datetime.now(UTC)).replace(microsecond=0)
    manifest_key = pipeline_manifest_path(LAYER1_DAILY_STAGE, config.run_id)
    report_path: Path | None = None
    report: Layer1ValidationReport | None = None
    tickers_processed = 0
    history_files_written = 0
    feature_rows_written = 0
    processed_dates = _business_dates(config.from_date, config.to_date)
    metadata: dict[str, object] = {
        "from_date": config.from_date,
        "to_date": config.to_date,
        "processed_dates": list(processed_dates),
        "layer0_run_id": config.layer0_run_id or config.run_id,
        "benchmark_ticker": config.benchmark_ticker,
        "requested_tickers": list(config.tickers),
        "allow_layer0_manifest_date_range": config.allow_layer0_manifest_date_range,
        "layer0_manifest_key": pipeline_manifest_path(
            "layer0", config.layer0_run_id or config.run_id
        ),
    }
    as_of_date = _single_as_of_date(config)
    if as_of_date is not None:
        metadata["as_of_date"] = as_of_date

    _write_manifest(
        writer=active_writer,
        key=manifest_key,
        config=config,
        status=RunStatus.RUNNING,
        started_at=started_at,
        metadata=metadata,
    )

    try:
        upstream_manifest = _require_completed_layer0_manifest(
            active_writer,
            config.layer0_run_id or config.run_id,
            as_of_date=as_of_date,
            allow_date_range=config.allow_layer0_manifest_date_range,
        )
        metadata["layer0_finished_at"] = (
            upstream_manifest.finished_at.isoformat()
            if upstream_manifest.finished_at is not None
            else None
        )
        universe_by_date = _load_universe_scope(
            active_writer,
            processed_dates,
            requested_tickers=config.tickers,
        )
        scope_tickers = _scope_tickers(universe_by_date)
        tickers_processed = len(scope_tickers)
        metadata["scope_tickers"] = scope_tickers
        _require_upstream_archives(
            active_writer,
            processed_dates=processed_dates,
            scope_tickers=scope_tickers,
            benchmark_ticker=config.benchmark_ticker,
            universe_by_date=universe_by_date,
            required_macro_series=_manifest_fred_series_ids(upstream_manifest),
        )

        news_results = _run_news_stage(
            active_writer,
            config,
            processed_dates,
            news_runner=news_runner,
        )
        topic_results = _run_text_topic_stage(
            active_writer,
            config,
            processed_dates,
            news_results=news_results,
            text_topic_runner=text_topic_runner,
        )
        sentiment_results = _run_finbert_stage(
            active_writer,
            config,
            processed_dates,
            news_results=news_results,
            finbert_runner=finbert_runner,
        )
        regime_results = _run_regime_stage(
            active_writer,
            config,
            processed_dates,
            regime_runner=regime_runner,
        )

        (
            feature_rows_written,
            history_files_written,
            order_book_branch,
        ) = _assemble_and_write_histories(
            writer=active_writer,
            universe_by_date=universe_by_date,
            benchmark_ticker=config.benchmark_ticker,
            topic_results=topic_results,
            sentiment_results=sentiment_results,
            regime_results=regime_results,
        )

        preliminary_report = validate_layer1_archive(
            run_id=config.run_id,
            from_date=config.from_date,
            to_date=config.to_date,
            universe=universe_by_date,
            reader=active_writer,
            output_prefixes=build_layer1_output_prefixes(processed_dates),
            inspect_related_manifests=True,
        )
        metadata.update(
            {
                "history_files_written": history_files_written,
                "feature_rows_written": feature_rows_written,
                "news_output_keys": {
                    date_text: result.output_key for date_text, result in news_results.items()
                },
                "topic_output_keys": {
                    date_text: result.topic_feature_key
                    for date_text, result in topic_results.items()
                },
                "sentiment_output_keys": {
                    date_text: result.sentiment_feature_key
                    for date_text, result in sentiment_results.items()
                },
                "regime_output_keys": {
                    date_text: result.output_key for date_text, result in regime_results.items()
                },
                "order_book_enabled": order_book_branch.enabled,
                "order_book_provider": order_book_branch.provider,
                "order_book_archive_keys": list(order_book_branch.archive_keys),
                "order_book_missing_dates": list(order_book_branch.missing_dates),
            }
        )
        finished_at = (now or datetime.now(UTC)).replace(microsecond=0)
        final_status = (
            RunStatus.COMPLETED if preliminary_report.ready_for_layer2 else RunStatus.FAILED
        )
        _write_manifest(
            writer=active_writer,
            key=manifest_key,
            config=config,
            status=final_status,
            started_at=started_at,
            finished_at=finished_at,
            metadata=metadata,
        )
        report, report_path = _write_terminal_validation_report(
            writer=active_writer,
            config=config,
            processed_dates=processed_dates,
            universe_by_date=universe_by_date,
            validation_output_dir=validation_output_dir,
            require_completed_manifest=final_status is RunStatus.COMPLETED,
        )
        metadata.update(
            {
                "validation_report_path": str(report_path),
                "validation_report_key": report.report_key,
                "validation_status": report.validation_status,
                "manifest_status": report.manifest_status,
                "ready_for_layer2": report.ready_for_layer2,
                "stale_manifest_keys": report.stale_manifest_keys,
                "related_manifest_keys": [
                    str(entry["key"])
                    for entry in report.related_manifests
                    if "key" in entry
                ],
            }
        )
        if not report.ready_for_layer2:
            metadata["error"] = {
                "type": Layer1ValidationError.__name__,
                "message": "Layer 1 validation failed: ready_for_layer2 is false",
                "validation_report_key": report.report_key,
            }
        _write_manifest(
            writer=active_writer,
            key=manifest_key,
            config=config,
            status=final_status,
            started_at=started_at,
            finished_at=finished_at,
            metadata=metadata,
        )
        logger.info(
            "Layer 1 orchestration complete run_id={} dates={} tickers={} ready_for_layer2={}",
            config.run_id,
            len(processed_dates),
            tickers_processed,
            report.ready_for_layer2,
        )
    except Exception as exc:
        metadata["error"] = {"type": type(exc).__name__, "message": str(exc)}
        finished_at = (now or datetime.now(UTC)).replace(microsecond=0)
        _write_manifest(
            writer=active_writer,
            key=manifest_key,
            config=config,
            status=RunStatus.FAILED,
            started_at=started_at,
            finished_at=finished_at,
            metadata=metadata,
        )
        logger.exception("Layer 1 orchestration failed run_id={}", config.run_id)
        raise
    if report is None or report_path is None:
        raise RuntimeError("Layer 1 validation report was not written")
    if not report.ready_for_layer2:
        raise Layer1ValidationError(report, report_path)
    return Layer1DailyResult(
        run_id=config.run_id,
        manifest_key=manifest_key,
        validation_report_path=report_path,
        validation_report_key=report.report_key,
        processed_dates=processed_dates,
        tickers_processed=tickers_processed,
        history_files_written=history_files_written,
        feature_rows_written=feature_rows_written,
        ready_for_layer2=report.ready_for_layer2,
    )


def load_modal_runtime_config(path: Path = MODAL_CONFIG_PATH) -> ModalRuntimeConfig:
    """Load Modal app and image settings from repository config."""
    payload = json.loads(path.read_text(encoding="utf-8"))
    timeout_seconds = int(payload["timeout_seconds"])
    batch_gpu_type = payload.get("layer1_batch_gpu_type")
    normalized_batch_gpu = None if batch_gpu_type in (None, "") else str(batch_gpu_type)
    hmm_train_lookback_bdays = payload.get("hmm_regime_train_lookback_bdays")
    normalized_hmm_lookback = (
        None
        if hmm_train_lookback_bdays in (None, "")
        else int(hmm_train_lookback_bdays)
    )
    if normalized_hmm_lookback is not None and normalized_hmm_lookback <= 0:
        raise ValueError("hmm_regime_train_lookback_bdays must be positive when configured")
    return ModalRuntimeConfig(
        app_name=str(payload["layer1_daily_app_name"]),
        r2_secret_name=str(payload["r2_secret_name"]),
        timeout_seconds=timeout_seconds,
        batch_timeout_seconds=int(
            payload.get("layer1_batch_timeout_seconds", timeout_seconds)
        ),
        batch_gpu_type=normalized_batch_gpu,
        hmm_train_lookback_bdays=normalized_hmm_lookback,
        python_version=str(payload.get("python_version", "3.11")),
        requirements_path=str(payload.get("requirements_path", "requirements/modal.txt")),
    )


def _run_modal_batched_stage_outputs(
    *,
    writer: ObjectStore,
    config: Layer1DailyConfig,
    news_runner: Callable[..., NewsPreprocessingPipelineResult] = run_news_preprocessing,
    text_topic_runner: Callable[..., TextTopicPipelineResult] = run_text_topics,
    finbert_runner: Callable[..., FinBERTPipelineResult] = run_finbert_sentiment,
    regime_runner: Callable[..., HMMRegimePipelineResult] = run_hmm_regime_detection,
    text_runtime_loader: Callable[[], text_topics_module.TextModelRuntimeConfig] = (
        text_topics_module.load_text_model_runtime_config
    ),
    finbert_runtime_loader: Callable[[], finbert_module.FinBERTModelRuntimeConfig] = (
        finbert_module.load_finbert_runtime_config
    ),
    embedder_factory: Callable[[object], object] = text_topics_module.SentenceTransformerEmbedder,
    topic_labeler_factory: Callable[[object], object] = text_topics_module.BERTopicLabeler,
    scorer_factory: Callable[[object], object] = finbert_module.FinBERTScorer,
) -> ModalBatchedStageOutputs:
    """Run per-date branches inside one Modal context for a multi-date readiness window."""
    processed_dates = _business_dates(config.from_date, config.to_date)
    news_output_keys_by_date: dict[str, str] = {}
    topic_output_keys_by_date: dict[str, str] = {}
    sentiment_output_keys_by_date: dict[str, str] = {}
    regime_output_keys_by_date: dict[str, str] = {}

    for date_text in processed_dates:
        stage_run_id = _stage_run_id(config.run_id, date_text)
        news_result = news_runner(
            NewsPreprocessingPipelineConfig(
                run_id=stage_run_id,
                as_of_date=date_text,
                min_sentence_chars=config.min_sentence_chars,
            ),
            writer=writer,
        )
        news_output_keys_by_date[date_text] = news_result.output_key

    text_runtime = text_runtime_loader()
    embedder = embedder_factory(text_runtime.embedding_config)
    for date_text in processed_dates:
        stage_run_id = _stage_run_id(config.run_id, date_text)
        topic_result = text_topic_runner(
            TextTopicPipelineConfig(
                run_id=stage_run_id,
                as_of_date=date_text,
                preprocessed_news_key=news_output_keys_by_date[date_text],
            ),
            writer=writer,
            embedder=embedder,
            topic_labeler=topic_labeler_factory(text_runtime),
            runtime_config=text_runtime,
        )
        topic_output_keys_by_date[date_text] = topic_result.topic_feature_key

    finbert_runtime = finbert_runtime_loader()
    scorer = scorer_factory(finbert_runtime)
    for date_text in processed_dates:
        stage_run_id = _stage_run_id(config.run_id, date_text)
        sentiment_result = finbert_runner(
            FinBERTPipelineConfig(
                run_id=stage_run_id,
                as_of_date=date_text,
                preprocessed_news_key=news_output_keys_by_date[date_text],
            ),
            writer=writer,
            scorer=scorer,
            runtime_config=finbert_runtime,
        )
        sentiment_output_keys_by_date[date_text] = sentiment_result.sentiment_feature_key

    for date_text in processed_dates:
        stage_run_id = _stage_run_id(config.run_id, date_text)
        regime_result = regime_runner(
            HMMRegimePipelineConfig(
                run_id=stage_run_id,
                train_start_date=config.hmm_train_start_date,
                train_end_date=_previous_business_day(date_text),
                inference_dates=(date_text,),
                benchmark_ticker=config.benchmark_ticker,
                max_iterations=config.hmm_max_iterations,
                min_training_rows=config.hmm_min_training_rows,
            ),
            writer=writer,
        )
        regime_output_keys_by_date[date_text] = regime_result.output_key

    return ModalBatchedStageOutputs(
        news_output_keys_by_date=news_output_keys_by_date,
        topic_output_keys_by_date=topic_output_keys_by_date,
        sentiment_output_keys_by_date=sentiment_output_keys_by_date,
        regime_output_keys_by_date=regime_output_keys_by_date,
    )


def _run_news_stage(
    writer: ObjectStore,
    config: Layer1DailyConfig,
    processed_dates: Sequence[str],
    *,
    news_runner: Callable[..., NewsPreprocessingPipelineResult],
) -> dict[str, NewsPreprocessingPipelineResult]:
    """Run sentence-level news preprocessing for each processed date."""
    results: dict[str, NewsPreprocessingPipelineResult] = {}
    for date_text in processed_dates:
        stage_run_id = _stage_run_id(config.run_id, date_text)
        results[date_text] = news_runner(
            NewsPreprocessingPipelineConfig(
                run_id=stage_run_id,
                as_of_date=date_text,
                min_sentence_chars=config.min_sentence_chars,
            ),
            writer=writer,
        )
    return results


def _run_text_topic_stage(
    writer: ObjectStore,
    config: Layer1DailyConfig,
    processed_dates: Sequence[str],
    *,
    news_results: Mapping[str, NewsPreprocessingPipelineResult],
    text_topic_runner: Callable[..., TextTopicPipelineResult],
) -> dict[str, TextTopicPipelineResult]:
    """Run embeddings and topic labels for each processed date."""
    results: dict[str, TextTopicPipelineResult] = {}
    for date_text in processed_dates:
        stage_run_id = _stage_run_id(config.run_id, date_text)
        results[date_text] = text_topic_runner(
            TextTopicPipelineConfig(
                run_id=stage_run_id,
                as_of_date=date_text,
                preprocessed_news_key=news_results[date_text].output_key,
            ),
            writer=writer,
        )
    return results


def _run_finbert_stage(
    writer: ObjectStore,
    config: Layer1DailyConfig,
    processed_dates: Sequence[str],
    *,
    news_results: Mapping[str, NewsPreprocessingPipelineResult],
    finbert_runner: Callable[..., FinBERTPipelineResult],
) -> dict[str, FinBERTPipelineResult]:
    """Run FinBERT scoring and ticker-day sentiment aggregation for each date."""
    results: dict[str, FinBERTPipelineResult] = {}
    for date_text in processed_dates:
        stage_run_id = _stage_run_id(config.run_id, date_text)
        results[date_text] = finbert_runner(
            FinBERTPipelineConfig(
                run_id=stage_run_id,
                as_of_date=date_text,
                preprocessed_news_key=news_results[date_text].output_key,
            ),
            writer=writer,
        )
    return results


def _run_regime_stage(
    writer: ObjectStore,
    config: Layer1DailyConfig,
    processed_dates: Sequence[str],
    *,
    regime_runner: Callable[..., HMMRegimePipelineResult],
) -> dict[str, HMMRegimePipelineResult]:
    """Run HMM regime inference for each processed date."""
    results: dict[str, HMMRegimePipelineResult] = {}
    for date_text in processed_dates:
        stage_run_id = _stage_run_id(config.run_id, date_text)
        results[date_text] = regime_runner(
            HMMRegimePipelineConfig(
                run_id=stage_run_id,
                train_start_date=config.hmm_train_start_date,
                train_end_date=_previous_business_day(date_text),
                inference_dates=(date_text,),
                benchmark_ticker=config.benchmark_ticker,
                max_iterations=config.hmm_max_iterations,
                min_training_rows=config.hmm_min_training_rows,
            ),
            writer=writer,
        )
    return results


def _assemble_and_write_histories(
    *,
    writer: ObjectStore,
    universe_by_date: Mapping[str, Sequence[str]],
    benchmark_ticker: str,
    topic_results: Mapping[str, TextTopicPipelineResult],
    sentiment_results: Mapping[str, FinBERTPipelineResult],
    regime_results: Mapping[str, HMMRegimePipelineResult],
) -> tuple[int, int, OptionalOrderBookBranch]:
    """Assemble daily features and upsert per-ticker histories."""
    target_dates_by_ticker = _expected_dates_by_ticker(universe_by_date)
    benchmark_bars = load_ohlcv_frame(benchmark_ticker, writer=writer)  # type: ignore[arg-type]
    macro = load_macro_frame(writer=writer)  # type: ignore[arg-type]
    shared_macro_features = compute_macro_features(
        macro,
        benchmark_bars["date"].tolist(),
    )
    topic_records = _load_feature_records_by_key(
        writer,
        {date_text: result.topic_feature_key for date_text, result in topic_results.items()},
    )
    sentiment_records = _assembly_safe_sentiment_records(
        _load_feature_records_by_key(
            writer,
            {
                date_text: result.sentiment_feature_key
                for date_text, result in sentiment_results.items()
            },
        )
    )
    regime_records = _load_regime_records_by_key(
        writer,
        {date_text: result.output_key for date_text, result in regime_results.items()},
    )
    ohlcv_by_ticker: dict[str, object] = {}
    fundamentals_by_ticker: dict[str, object] = {}
    for ticker in sorted(target_dates_by_ticker):
        ohlcv_by_ticker[ticker] = load_ohlcv_frame(ticker, writer=writer)  # type: ignore[arg-type]
        try:
            fundamentals_by_ticker[ticker] = load_fundamentals_frame(  # type: ignore[arg-type]
                ticker,
                writer=writer,
            )
        except FileNotFoundError:
            fundamentals_by_ticker[ticker] = _empty_fundamentals_frame()
    sector_config = load_sector_etf_config()
    sector_records_by_ticker = {
        ticker: sector_features_to_records(frame)
        for ticker, frame in compute_sector_features(
            ohlcv_by_ticker=ohlcv_by_ticker,
            fundamentals_by_ticker=fundamentals_by_ticker,
            target_dates_by_ticker=target_dates_by_ticker,
            sector_price_frames=_load_sector_price_frames(writer, sector_config),
            sector_config=sector_config,
        ).items()
    }
    order_book_branch = _load_order_book_branch(
        writer=writer,
        target_dates_by_ticker=target_dates_by_ticker,
    )

    feature_rows_written = 0
    history_files_written = 0
    for ticker, target_dates in sorted(target_dates_by_ticker.items()):
        ohlcv = ohlcv_by_ticker[ticker]
        fundamentals = fundamentals_by_ticker[ticker]
        market_records = _records_for_target_dates(
            market_features_to_records(
                compute_market_features(ohlcv, ticker, benchmark_bars=benchmark_bars)
            ),
            target_dates,
        )
        context_records = _records_for_target_dates(
            context_features_to_records(
                compute_context_features(
                    fundamentals=fundamentals,
                    ohlcv=ohlcv,
                    macro=macro,
                    ticker=ticker,
                    macro_features=shared_macro_features,
                    target_dates=target_dates,
                )
            ),
            target_dates,
        )
        sector_daily_records = _records_for_target_dates(
            sector_records_by_ticker.get(ticker, []),
            target_dates,
        )
        topic_daily_records = _records_for_target_dates(
            topic_records.get(ticker, []),
            target_dates,
        )
        sentiment_daily_records = _records_for_target_dates(
            sentiment_records.get(ticker, []),
            target_dates,
        )
        order_book_daily_records = _records_for_target_dates(
            order_book_branch.records_by_ticker.get(ticker, ()),
            target_dates,
        )
        regime_daily_records = _broadcast_regime_records(
            ticker=ticker,
            target_dates=target_dates,
            regime_records=regime_records,
        )
        assembled = assemble_layer1_feature_records(
            [
                Layer1FeatureInput(
                    name="market",
                    records=market_records,
                    as_of_timestamp=SENTINEL_ASSEMBLY_AS_OF,
                ),
                Layer1FeatureInput(
                    name="context",
                    records=context_records,
                    as_of_timestamp=SENTINEL_ASSEMBLY_AS_OF,
                ),
                Layer1FeatureInput(
                    name="sector",
                    records=sector_daily_records,
                    as_of_timestamp=SENTINEL_ASSEMBLY_AS_OF,
                ),
                Layer1FeatureInput(
                    name="topics",
                    records=topic_daily_records,
                    as_of_timestamp=SENTINEL_ASSEMBLY_AS_OF,
                ),
                Layer1FeatureInput(
                    name="sentiment",
                    records=sentiment_daily_records,
                    as_of_timestamp=SENTINEL_ASSEMBLY_AS_OF,
                ),
                Layer1FeatureInput(
                    name="order_book",
                    records=order_book_daily_records,
                    as_of_timestamp=SENTINEL_ASSEMBLY_AS_OF,
                ),
                Layer1FeatureInput(
                    name="regime",
                    records=regime_daily_records,
                    as_of_timestamp=SENTINEL_ASSEMBLY_AS_OF,
                ),
            ]
        )
        if not assembled:
            continue

        existing_history = _load_existing_history(writer, ticker)
        merged_history = _merge_feature_histories(existing_history, assembled)
        write_feature_records(merged_history, writer=writer)  # type: ignore[arg-type]
        for record in assembled:
            write_feature_record(record, writer=writer)  # type: ignore[arg-type]
        feature_rows_written += len(assembled)
        history_files_written += 1

    return feature_rows_written, history_files_written, order_book_branch


def _load_order_book_branch(
    *,
    writer: ObjectStore,
    target_dates_by_ticker: Mapping[str, Sequence[str]],
) -> OptionalOrderBookBranch:
    """Load optional order-book FeatureRecords for the requested ticker/date scope."""
    config = load_order_book_feature_config()
    if not config.is_active or config.provider is None:
        return OptionalOrderBookBranch(
            enabled=False,
            provider=config.provider,
            records_by_ticker={},
            archive_keys=(),
            missing_dates=(),
        )

    records_by_ticker: dict[str, list[FeatureRecord]] = {}
    archive_keys: list[str] = []
    missing_dates: list[str] = []
    all_dates = sorted({date_text for dates in target_dates_by_ticker.values() for date_text in dates})
    for date_text in all_dates:
        date_tickers = sorted(
            ticker
            for ticker, ticker_dates in target_dates_by_ticker.items()
            if date_text in ticker_dates
        )
        key = raw_order_book_path(config.provider, date_text)
        if writer.exists(key):
            frame = load_order_book_frame(config.provider, date_text, writer=writer)  # type: ignore[arg-type]
            archive_keys.append(key)
        else:
            missing_dates.append(date_text)
            frame = _empty_order_book_source_frame()

        day_records = order_book_features_to_records(
            compute_order_book_features(
                frame,
                target_date=date_text,
                tickers=date_tickers,
            )
        )
        for record in day_records:
            records_by_ticker.setdefault(record.ticker, []).append(record)

    if missing_dates:
        sample = ", ".join(missing_dates[:5])
        suffix = "" if len(missing_dates) <= 5 else ", ..."
        logger.warning(
            "Order-book branch enabled for provider={} but archives were missing for {} dates: {}{}",
            config.provider,
            len(missing_dates),
            sample,
            suffix,
        )

    return OptionalOrderBookBranch(
        enabled=True,
        provider=config.provider,
        records_by_ticker={
            ticker: tuple(sorted(records, key=lambda record: record.date))
            for ticker, records in sorted(records_by_ticker.items())
        },
        archive_keys=tuple(archive_keys),
        missing_dates=tuple(missing_dates),
    )


def _empty_order_book_source_frame():
    """Return an empty provider-normalized order-book frame."""
    try:
        import pandas as pd
    except ModuleNotFoundError as exc:
        raise ModuleNotFoundError(
            "pandas and pyarrow are required to build empty order-book frames."
        ) from exc
    return pd.DataFrame(
        columns=[
            "date",
            "ticker",
            "captured_at",
            "bid_price",
            "ask_price",
            "bid_size",
            "ask_size",
        ]
    )


def _load_feature_records_by_key(
    writer: ObjectStore,
    keys_by_date: Mapping[str, str],
) -> dict[str, list[FeatureRecord]]:
    """Load ticker-day feature records from existing branch output parquet objects."""
    grouped: dict[str, list[FeatureRecord]] = {}
    for key in keys_by_date.values():
        for record in parquet_bytes_to_feature_records(writer.get_object(key)):
            grouped.setdefault(record.ticker, []).append(record)
    return grouped


def _load_sector_price_frames(writer: ObjectStore, sector_config) -> dict[str, object]:
    """Return the configured sector ETF histories available in storage."""
    frames: dict[str, object] = {}
    for etf_ticker in sorted(set(sector_config.sector_to_etf.values())):
        try:
            frames[etf_ticker] = load_ohlcv_frame(etf_ticker, writer=writer)  # type: ignore[arg-type]
        except FileNotFoundError:
            logger.warning(
                "Sector ETF OHLCV missing for ticker={}; related sector features will be null",
                etf_ticker,
            )
    return frames


def _empty_fundamentals_frame():
    """Return an empty fundamentals frame matching the expected archive columns."""
    try:
        import pandas as pd
    except ModuleNotFoundError as exc:
        raise ModuleNotFoundError(
            "pandas and pyarrow are required to build empty Layer 1 fundamentals frames."
        ) from exc
    return pd.DataFrame(
        columns=[
            "source",
            "ticker",
            "report_date",
            "availability_date",
            "retrieved_at",
            "fiscal_year",
            "fiscal_period",
            "statement",
            "earnings_date",
            "raw_json",
        ]
    )


def _assembly_safe_sentiment_records(
    grouped_records: Mapping[str, Sequence[FeatureRecord]],
) -> dict[str, list[FeatureRecord]]:
    """Drop duplicate sentiment keys that are already owned by the topic branch."""
    cleaned: dict[str, list[FeatureRecord]] = {}
    for ticker, records in grouped_records.items():
        cleaned[ticker] = [
            FeatureRecord(
                date=record.date,
                ticker=record.ticker,
                features={
                    key: value
                    for key, value in record.features.items()
                    if key != "nlp_sentence_count"
                },
            )
            for record in records
        ]
    return cleaned


def _load_regime_records_by_key(
    writer: ObjectStore,
    keys_by_date: Mapping[str, str],
) -> dict[str, FeatureRecord]:
    """Load one market-wide regime record per date from HMM parquet outputs."""
    records_by_date: dict[str, FeatureRecord] = {}
    for key in keys_by_date.values():
        frame = _read_parquet_frame(writer.get_object(key))
        for record in regime_features_to_records(frame):
            records_by_date[record.date] = record
    return records_by_date


def _broadcast_regime_records(
    *,
    ticker: str,
    target_dates: Sequence[str],
    regime_records: Mapping[str, FeatureRecord],
) -> list[FeatureRecord]:
    """Broadcast market-wide regime features onto one ticker's target dates."""
    broadcast: list[FeatureRecord] = []
    for date_text in target_dates:
        source = regime_records.get(date_text)
        if source is None:
            continue
        broadcast.append(
            FeatureRecord(
                date=date_text,
                ticker=ticker,
                features=dict(source.features),
            )
        )
    return broadcast


def _records_for_target_dates(
    records: Sequence[FeatureRecord],
    target_dates: Sequence[str],
) -> list[FeatureRecord]:
    """Filter feature records to the requested target dates."""
    allowed_dates = set(target_dates)
    return [record for record in records if record.date in allowed_dates]


def _load_existing_history(writer: ObjectStore, ticker: str) -> list[FeatureRecord]:
    """Return an existing feature history for a ticker when present."""
    key = layer1_ticker_history_path(ticker)
    if not writer.exists(key):
        return []
    return read_feature_records(ticker, writer=writer)  # type: ignore[arg-type]


def _merge_feature_histories(
    existing_history: Sequence[FeatureRecord],
    new_records: Sequence[FeatureRecord],
) -> list[FeatureRecord]:
    """Replace any overlapping dates and return one sorted ticker history."""
    merged: dict[str, FeatureRecord] = {record.date: record for record in existing_history}
    for record in new_records:
        merged[record.date] = record
    return [merged[date_text] for date_text in sorted(merged)]


def _require_completed_layer0_manifest(
    writer: ObjectStore,
    run_id: str,
    *,
    as_of_date: str | None = None,
    allow_date_range: bool = False,
) -> PipelineManifestRecord:
    """Require a completed Layer 0 manifest before any Layer 1 work begins."""
    key = pipeline_manifest_path("layer0", run_id)
    if not writer.exists(key):
        raise FileNotFoundError(f"Missing required Layer 0 manifest: {key}")
    manifest = PipelineManifestRecord.model_validate_json(writer.get_object(key))
    if manifest.stage != "layer0":
        raise ValueError(f"Expected stage=layer0 manifest, got {manifest.stage!r}")
    if manifest.status is not RunStatus.COMPLETED:
        raise RuntimeError(
            f"Layer 0 manifest must be completed before Layer 1 runs: {key}={manifest.status}"
        )
    if manifest.finished_at is None:
        raise RuntimeError("Layer 0 manifest must include finished_at before Layer 1 runs")
    if as_of_date is not None:
        metadata = manifest.metadata
        manifest_from_date = metadata.get("from_date")
        manifest_to_date = metadata.get("to_date")
        if manifest_from_date == as_of_date and manifest_to_date == as_of_date:
            return manifest
        if (
            allow_date_range
            and isinstance(manifest_from_date, str)
            and isinstance(manifest_to_date, str)
            and manifest_from_date <= as_of_date <= manifest_to_date
        ):
            return manifest
        raise RuntimeError(
            "Layer 0 manifest is stale for daily Layer 1 run: "
            f"expected {as_of_date}, got {manifest_from_date}..{manifest_to_date}"
        )
    return manifest


def _require_upstream_archives(
    writer: ObjectStore,
    *,
    processed_dates: Sequence[str],
    scope_tickers: Sequence[str],
    benchmark_ticker: str,
    universe_by_date: Mapping[str, Sequence[str]],
    required_macro_series: Sequence[str],
) -> None:
    """Require the Layer 0 archives needed by the daily Layer 1 run."""
    required_keys = [raw_price_path(benchmark_ticker)]
    required_keys.extend(raw_news_path(date_text) for date_text in processed_dates)
    required_keys.extend(raw_universe_path(date_text) for date_text in processed_dates)
    for ticker in scope_tickers:
        required_keys.append(raw_price_path(ticker))
        required_keys.append(raw_fundamentals_path(ticker))

    missing = sorted(key for key in required_keys if not writer.exists(key))
    if missing:
        raise FileNotFoundError(
            "Missing required Layer 0 archives for Layer 1 run: "
            + ", ".join(missing)
        )
    macro_available = available_macro_series_by_date(
        list(processed_dates),
        writer=writer,  # type: ignore[arg-type]
        series_ids=required_macro_series or None,
    )
    macro_missing = {
        date_text: sorted(set(required_macro_series) - set(macro_available.get(date_text, [])))
        for date_text in processed_dates
        if set(required_macro_series) - set(macro_available.get(date_text, []))
    }
    if macro_missing:
        raise FileNotFoundError(
            "Missing recoverable raw macro coverage for Layer 1 run: "
            + _format_ticker_dates(macro_missing)
        )
    _require_target_date_price_coverage(
        writer,
        processed_dates=processed_dates,
        benchmark_ticker=benchmark_ticker,
        universe_by_date=universe_by_date,
    )


def _manifest_fred_series_ids(manifest: PipelineManifestRecord) -> list[str]:
    """Return normalized FRED series IDs recorded by the completed Layer 0 manifest."""
    value = manifest.metadata.get("fred_series_ids")
    if not isinstance(value, list):
        return []
    normalized: list[str] = []
    for series_id in value:
        if not isinstance(series_id, str):
            continue
        cleaned = series_id.strip().upper()
        if cleaned:
            normalized.append(cleaned)
    return sorted(set(normalized))


def _require_target_date_price_coverage(
    writer: ObjectStore,
    *,
    processed_dates: Sequence[str],
    benchmark_ticker: str,
    universe_by_date: Mapping[str, Sequence[str]],
) -> None:
    """Fail closed when raw price archives exist but miss expected target dates."""
    expected_dates_by_ticker = _expected_dates_by_ticker(universe_by_date)
    expected_dates_by_ticker[benchmark_ticker] = sorted(set(processed_dates))
    missing_coverage: dict[str, list[str]] = {}
    for ticker, expected_dates in sorted(expected_dates_by_ticker.items()):
        frame = load_ohlcv_frame(ticker, writer=writer)  # type: ignore[arg-type]
        present_dates = {
            str(value)[:10]
            for value in frame["date"].tolist()
        }
        missing_dates = [date_text for date_text in expected_dates if date_text not in present_dates]
        if missing_dates:
            missing_coverage[ticker] = missing_dates
    if missing_coverage:
        raise RuntimeError(
            "Layer 0 raw price archives missing target-date coverage for Layer 1 run: "
            + _format_ticker_dates(missing_coverage)
        )


def _load_universe_scope(
    writer: ObjectStore,
    processed_dates: Sequence[str],
    *,
    requested_tickers: Sequence[str],
) -> dict[str, list[str]]:
    """Load eligible ticker scope from Layer 0 universe masks."""
    requested = {ticker.strip().upper() for ticker in requested_tickers}
    universe: dict[str, list[str]] = {}
    for date_text in processed_dates:
        payload = writer.get_object(raw_universe_path(date_text)).decode("utf-8")
        tickers: list[str] = []
        for row in csv.DictReader(io.StringIO(payload)):
            ticker = str(row.get("ticker", "")).strip().upper()
            if not ticker:
                continue
            if requested and ticker not in requested:
                continue
            if (
                _truthy(row.get("in_universe"))
                and _truthy(row.get("tradable"), default=True)
                and _truthy(row.get("liquid"), default=True)
                and _truthy(row.get("data_quality_ok"), default=True)
                and not _truthy(row.get("halted"))
            ):
                tickers.append(ticker)
        universe[date_text] = sorted(set(tickers))
    return universe


def _scope_tickers(universe_by_date: Mapping[str, Sequence[str]]) -> list[str]:
    """Return the sorted union ticker scope implied by the universe mapping."""
    tickers = {ticker for tickers in universe_by_date.values() for ticker in tickers}
    return sorted(tickers)


def _write_terminal_validation_report(
    *,
    writer: ObjectStore,
    config: Layer1DailyConfig,
    processed_dates: Sequence[str],
    universe_by_date: Mapping[str, Sequence[str]],
    validation_output_dir: Path | None,
    require_completed_manifest: bool,
) -> tuple[Layer1ValidationReport, Path]:
    """Write the final readiness report after the Layer 1 manifest reaches terminal state."""
    report = validate_layer1_archive(
        run_id=config.run_id,
        from_date=config.from_date,
        to_date=config.to_date,
        universe=universe_by_date,
        reader=writer,
        output_prefixes=build_layer1_output_prefixes(processed_dates),
        require_completed_manifest=require_completed_manifest,
        inspect_related_manifests=not require_completed_manifest,
    )
    if report.report_key is None:
        raise ValueError("Layer 1 validation report_key was not populated")
    report_path = write_validation_report(
        report,
        validation_output_dir if validation_output_dir is not None else DEFAULT_REPORT_DIR,
    )
    writer.put_object(report.report_key, render_validation_report(report))
    return report, report_path


def _expected_dates_by_ticker(
    universe_by_date: Mapping[str, Sequence[str]],
) -> dict[str, list[str]]:
    """Invert a date->tickers mapping into sorted ticker->dates."""
    by_ticker: dict[str, list[str]] = {}
    for date_text, tickers in sorted(universe_by_date.items()):
        for ticker in tickers:
            by_ticker.setdefault(ticker, []).append(date_text)
    return by_ticker


def _validation_failure_summary(report: Layer1ValidationReport) -> str:
    """Return a compact operator-facing summary for one failed validation report."""
    if report.regime_failures:
        sample = report.regime_failures[0]
        date_text = sample.get("date")
        reason = sample.get("reason")
        ticker = sample.get("ticker")
        location = f"{ticker}/{date_text}" if ticker else str(date_text)
        return f"regime validation failed at {location}: {reason}"
    if report.regime_warnings:
        sample = report.regime_warnings[0]
        return (
            f"regime warm-up warning on {sample.get('date')}: "
            f"{sample.get('reason')}"
        )
    if report.missing_ticker_files:
        return (
            f"{len(report.missing_ticker_files)} ticker histories missing; "
            f"sample={', '.join(report.missing_ticker_files[:3])}"
        )
    if report.missing_ticker_dates:
        return (
            f"{len(report.missing_ticker_dates)} ticker histories missing expected dates; "
            f"sample={_format_ticker_dates(report.missing_ticker_dates)}"
        )
    if report.schema_failure_keys:
        return (
            f"{len(report.schema_failure_keys)} ticker histories failed schema validation; "
            f"sample={', '.join(report.schema_failure_keys[:3])}"
        )
    if report.manifest_errors:
        return f"manifest check failed: {', '.join(report.manifest_errors)}"
    return "ready_for_layer2 is false"


def _format_ticker_dates(
    ticker_dates: Mapping[str, Sequence[str]],
    *,
    limit: int = 3,
) -> str:
    """Format a small ticker/date mapping for logs and exceptions."""
    pairs = [
        f"{ticker}=[{','.join(sorted(dict.fromkeys(dates))[:3])}]"
        for ticker, dates in sorted(ticker_dates.items())[:limit]
    ]
    if len(ticker_dates) > limit:
        pairs.append(f"...+{len(ticker_dates) - limit} more")
    return "; ".join(pairs)


def _write_manifest(
    *,
    writer: ObjectStore,
    key: str,
    config: Layer1DailyConfig,
    status: RunStatus,
    started_at: datetime,
    metadata: dict[str, object],
    finished_at: datetime | None = None,
) -> None:
    """Persist one Layer 1 orchestration manifest state."""
    manifest = PipelineManifestRecord(
        run_id=config.run_id,
        stage=LAYER1_DAILY_STAGE,
        status=status,
        started_at=started_at,
        finished_at=finished_at,
        input_path="raw/news/,raw/universe/,raw/prices/,raw/fundamentals/,raw/macro/",
        output_path="features/layer1/",
        metadata=metadata,
    )
    writer.put_object(key, manifest.model_dump_json(indent=2))


def _read_parquet_frame(payload: bytes):
    """Read a parquet payload into a pandas DataFrame."""
    try:
        import pandas as pd
    except ModuleNotFoundError as exc:
        raise ModuleNotFoundError(
            "pandas and pyarrow are required to read Layer 1 parquet payloads."
        ) from exc
    return pd.read_parquet(io.BytesIO(payload))


def _stage_run_id(run_id: str, date_text: str) -> str:
    """Return a deterministic per-date branch run identifier."""
    return f"{run_id}-{date_text}"


def _single_as_of_date(config: Layer1DailyConfig) -> str | None:
    """Return the single requested date when the run is a one-day daily invocation."""
    if config.from_date == config.to_date:
        return config.from_date
    return None


def _business_dates(from_date: str, to_date: str) -> tuple[str, ...]:
    """Return business dates between two ISO dates, inclusive."""
    start = Date.fromisoformat(from_date)
    end = Date.fromisoformat(to_date)
    dates: list[str] = []
    current = start
    while current <= end:
        if current.weekday() < 5:
            dates.append(current.isoformat())
        current += timedelta(days=1)
    return tuple(dates)


def _previous_business_day(date_text: str) -> str:
    """Return the prior business day for one ISO date."""
    current = Date.fromisoformat(date_text) - timedelta(days=1)
    while current.weekday() >= 5:
        current -= timedelta(days=1)
    return current.isoformat()


def _subtract_business_days(date_text: str, count: int) -> str:
    """Return the ISO date that is `count` business days before `date_text`."""
    if count < 0:
        raise ValueError("count must be non-negative")
    current = Date.fromisoformat(date_text)
    remaining = count
    while remaining > 0:
        current -= timedelta(days=1)
        if current.weekday() < 5:
            remaining -= 1
    return current.isoformat()


def _truthy(value: str | None, *, default: bool = False) -> bool:
    """Return True for common CSV boolean values."""
    if value is None:
        return default
    normalized = str(value).strip().lower()
    if not normalized:
        return default
    return normalized in {"1", "true", "t", "yes", "y"}


def _validate_iso_date(value: str, field_name: str) -> None:
    """Validate one canonical YYYY-MM-DD string."""
    try:
        parsed = Date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"{field_name} must be YYYY-MM-DD") from exc
    if parsed.isoformat() != value:
        raise ValueError(f"{field_name} must be YYYY-MM-DD")


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """Parse CLI arguments for the Layer 1 orchestrator."""
    parser = argparse.ArgumentParser(description="Run the full Layer 1 daily orchestration.")
    parser.add_argument("--run-id", required=True)
    date_group = parser.add_mutually_exclusive_group(required=True)
    date_group.add_argument("--from-date", metavar="YYYY-MM-DD")
    date_group.add_argument("--as-of-date", metavar="YYYY-MM-DD")
    parser.add_argument("--to-date", required=False, metavar="YYYY-MM-DD")
    parser.add_argument("--layer0-run-id", default=None)
    parser.add_argument("--tickers", nargs="*", default=None)
    parser.add_argument("--benchmark-ticker", default="SPY")
    parser.add_argument(
        "--allow-layer0-manifest-date-range",
        action="store_true",
        help=(
            "Allow a completed Layer 0 manifest whose from/to window contains the requested "
            "single-day as-of date. Intended for historical readiness runs, not the Pi daily path."
        ),
    )
    parser.add_argument("--min-sentence-chars", type=int, default=2)
    parser.add_argument("--hmm-train-start-date", default=None, metavar="YYYY-MM-DD")
    parser.add_argument("--hmm-max-iterations", type=int, default=100)
    parser.add_argument("--hmm-min-training-rows", type=int, default=30)
    parser.add_argument("--validation-output-dir", default=str(DEFAULT_REPORT_DIR))
    args = parser.parse_args(argv)
    if args.as_of_date is not None and args.to_date is not None:
        parser.error("--to-date cannot be used with --as-of-date")
    if args.tickers == []:
        parser.error("--tickers requires at least one ticker when provided")
    return args


def _config_from_args(args: argparse.Namespace) -> Layer1DailyConfig:
    """Build a validated Layer 1 config from CLI arguments."""
    from_date = (args.as_of_date or args.from_date).strip()
    return Layer1DailyConfig(
        run_id=args.run_id.strip(),
        from_date=from_date,
        to_date=(args.to_date or from_date).strip(),
        layer0_run_id=args.layer0_run_id.strip() if args.layer0_run_id else None,
        tickers=tuple(ticker.strip().upper() for ticker in (args.tickers or [])),
        benchmark_ticker=args.benchmark_ticker.strip().upper(),
        allow_layer0_manifest_date_range=bool(args.allow_layer0_manifest_date_range),
        min_sentence_chars=args.min_sentence_chars,
        hmm_train_start_date=(
            args.hmm_train_start_date.strip() if args.hmm_train_start_date else None
        ),
        hmm_max_iterations=args.hmm_max_iterations,
        hmm_min_training_rows=args.hmm_min_training_rows,
    )


def main(argv: Sequence[str] | None = None) -> int:
    """CLI entry point for the Layer 1 daily orchestrator."""
    args = _parse_args(argv)
    config = _config_from_args(args)
    if args.as_of_date is not None and _modal_run_daily_layer1 is not None:
        try:
            modal_main(
                run_id=config.run_id,
                as_of_date=config.from_date,
                layer0_run_id=config.layer0_run_id or config.run_id,
                benchmark_ticker=config.benchmark_ticker,
                allow_layer0_manifest_date_range=config.allow_layer0_manifest_date_range,
                min_sentence_chars=config.min_sentence_chars,
                hmm_train_start_date=config.hmm_train_start_date,
                hmm_max_iterations=config.hmm_max_iterations,
                hmm_min_training_rows=config.hmm_min_training_rows,
            )
        except Exception as exc:  # noqa: BLE001
            logger.error("Layer 1 Modal orchestration failed: {}", exc)
            return 1
        return 0
    if _single_as_of_date(config) is None and _modal_run_batched_layer1 is not None:
        try:
            modal_range_main(
                run_id=config.run_id,
                from_date=config.from_date,
                to_date=config.to_date,
                layer0_run_id=config.layer0_run_id or config.run_id,
                benchmark_ticker=config.benchmark_ticker,
                allow_layer0_manifest_date_range=config.allow_layer0_manifest_date_range,
                min_sentence_chars=config.min_sentence_chars,
                hmm_train_start_date=config.hmm_train_start_date,
                hmm_max_iterations=config.hmm_max_iterations,
                hmm_min_training_rows=config.hmm_min_training_rows,
            )
        except Exception as exc:  # noqa: BLE001
            logger.error("Layer 1 batched Modal orchestration failed: {}", exc)
            return 1
        return 0
    try:
        result = run_daily_layer1(
            config,
            validation_output_dir=Path(args.validation_output_dir),
        )
    except Layer1ValidationError as exc:
        logger.error(str(exc))
        return 1
    except Exception as exc:  # noqa: BLE001
        logger.error("Layer 1 orchestration failed: {}", exc)
        return 1

    logger.info(
        "Layer 1 orchestration manifest={} validation_report={}",
        result.manifest_key,
        result.validation_report_key,
    )
    return 0 if result.ready_for_layer2 else 1


def _resolve_hmm_train_start_date(
    explicit_train_start_date: str | None,
    *,
    reference_date: str,
) -> str | None:
    """Resolve the effective bounded HMM train start date for Modal runs."""
    if explicit_train_start_date is not None:
        return explicit_train_start_date

    runtime = load_modal_runtime_config()
    lookback_bdays = runtime.hmm_train_lookback_bdays
    if lookback_bdays is None:
        return None
    train_end_date = _previous_business_day(reference_date)
    return _subtract_business_days(train_end_date, lookback_bdays)


def modal_range_main(
    run_id: str,
    from_date: str,
    to_date: str,
    layer0_run_id: str,
    benchmark_ticker: str = "SPY",
    allow_layer0_manifest_date_range: bool = False,
    min_sentence_chars: int = 2,
    hmm_train_start_date: str | None = None,
    hmm_max_iterations: int = 100,
    hmm_min_training_rows: int = 30,
) -> None:
    """Submit one multi-date Layer 1 readiness run via the Python entrypoint only."""
    if _modal_run_batched_layer1 is None:
        raise RuntimeError(
            "Batched Modal app is unavailable because the modal package is not installed"
        )
    resolved_hmm_train_start_date = _resolve_hmm_train_start_date(
        hmm_train_start_date,
        reference_date=from_date,
    )
    result = _run_modal_remote_function(
        _modal_run_batched_layer1,
        owning_app=app,
        run_id=run_id,
        from_date=from_date,
        to_date=to_date,
        layer0_run_id=layer0_run_id,
        benchmark_ticker=benchmark_ticker.strip().upper(),
        allow_layer0_manifest_date_range=allow_layer0_manifest_date_range,
        min_sentence_chars=min_sentence_chars,
        hmm_train_start_date=resolved_hmm_train_start_date,
        hmm_max_iterations=hmm_max_iterations,
        hmm_min_training_rows=hmm_min_training_rows,
    )
    manifest_key = result.get("manifest_key")
    ready_for_layer2 = result.get("ready_for_layer2")
    if isinstance(manifest_key, str) and isinstance(ready_for_layer2, bool):
        logger.info(
            "Layer 1 batched Modal run complete manifest={} ready_for_layer2={}",
            manifest_key,
            ready_for_layer2,
        )


def modal_main(
    run_id: str,
    as_of_date: str,
    layer0_run_id: str,
    benchmark_ticker: str = "SPY",
    allow_layer0_manifest_date_range: bool = False,
    min_sentence_chars: int = 2,
    hmm_train_start_date: str | None = None,
    hmm_max_iterations: int = 100,
    hmm_min_training_rows: int = 30,
) -> None:
    """Submit the single-date Layer 1 flow using stage-specific Modal runners."""
    if _modal_run_daily_layer1 is None:
        raise RuntimeError(
            "Modal app is unavailable because the modal package is not installed"
        )
    if allow_layer0_manifest_date_range:
        logger.warning(
            "allow_layer0_manifest_date_range=True is intended for historical readiness runs "
            "only and must not be used on the Pi daily path"
        )
    resolved_hmm_train_start_date = _resolve_hmm_train_start_date(
        hmm_train_start_date,
        reference_date=as_of_date,
    )
    stage_run_id = _stage_run_id(run_id, as_of_date)
    writer = R2Writer()
    preprocessed_news_key = _load_completed_stage_output(
        writer=writer,
        stage=NLP_PREPROCESSING_STAGE,
        run_id=stage_run_id,
        as_of_date=as_of_date,
    )
    if preprocessed_news_key is None:
        news_result = _run_module_modal_remote(
            news_module,
            "modal_run_news_preprocessing",
            run_id=stage_run_id,
            as_of_date=as_of_date,
            min_sentence_chars=min_sentence_chars,
        )
        preprocessed_news_key = _require_result_key(news_result, "output_key")
    # Keep stage dispatch synchronous here. In production readiness runs, `.spawn()`
    # against imported stage apps can leave the daily manifest stuck in `running`
    # without ever producing downstream stage manifests.
    topic_feature_key = _load_completed_stage_output(
        writer=writer,
        stage=TEXT_TOPICS_STAGE,
        run_id=stage_run_id,
        as_of_date=as_of_date,
    )
    if topic_feature_key is None:
        topic_result = _run_module_modal_remote(
            text_topics_module,
            "modal_run_text_topics",
            run_id=stage_run_id,
            as_of_date=as_of_date,
            preprocessed_news_key=preprocessed_news_key,
        )
        topic_feature_key = _require_result_key(topic_result, "topic_feature_key")
    sentiment_feature_key = _load_completed_stage_output(
        writer=writer,
        stage=FINBERT_SENTIMENT_STAGE,
        run_id=stage_run_id,
        as_of_date=as_of_date,
    )
    if sentiment_feature_key is None:
        sentiment_result = _run_module_modal_remote(
            finbert_module,
            "modal_run_finbert_sentiment",
            run_id=stage_run_id,
            as_of_date=as_of_date,
            preprocessed_news_key=preprocessed_news_key,
        )
        sentiment_feature_key = _require_result_key(
            sentiment_result,
            "sentiment_feature_key",
        )
    regime_output_key = _load_completed_stage_output(
        writer=writer,
        stage=REGIME_STAGE,
        run_id=stage_run_id,
        as_of_date=as_of_date,
    )
    if regime_output_key is None:
        regime_result = _run_module_modal_remote(
            regime_module,
            "modal_run_hmm_regime_detection",
            run_id=stage_run_id,
            train_start_date=resolved_hmm_train_start_date,
            train_end_date=_previous_business_day(as_of_date),
            inference_dates=as_of_date,
            benchmark_ticker=benchmark_ticker.strip().upper(),
            max_iterations=hmm_max_iterations,
            min_training_rows=hmm_min_training_rows,
        )
        regime_output_key = _require_result_key(regime_result, "output_key")
    _run_modal_remote_function(
        _modal_run_daily_layer1,
        owning_app=app,
        run_id=run_id,
        as_of_date=as_of_date,
        layer0_run_id=layer0_run_id,
        benchmark_ticker=benchmark_ticker.strip().upper(),
        allow_layer0_manifest_date_range=allow_layer0_manifest_date_range,
        min_sentence_chars=min_sentence_chars,
        hmm_train_start_date=resolved_hmm_train_start_date,
        hmm_max_iterations=hmm_max_iterations,
        hmm_min_training_rows=hmm_min_training_rows,
        preprocessed_news_key=preprocessed_news_key,
        topic_feature_key=topic_feature_key,
        sentiment_feature_key=sentiment_feature_key,
        regime_output_key=regime_output_key,
    )


def _modal_run_batched_layer1_entry(
    run_id: str,
    from_date: str,
    to_date: str,
    layer0_run_id: str,
    benchmark_ticker: str = "SPY",
    allow_layer0_manifest_date_range: bool = False,
    min_sentence_chars: int = 2,
    hmm_train_start_date: str | None = None,
    hmm_max_iterations: int = 100,
    hmm_min_training_rows: int = 30,
) -> dict[str, object]:
    """Run multi-date Layer 1 readiness on Modal without local heavy-ML dependencies."""
    writer = R2Writer()
    config = Layer1DailyConfig(
        run_id=run_id,
        from_date=from_date,
        to_date=to_date,
        layer0_run_id=layer0_run_id,
        benchmark_ticker=benchmark_ticker.strip().upper(),
        allow_layer0_manifest_date_range=allow_layer0_manifest_date_range,
        min_sentence_chars=min_sentence_chars,
        hmm_train_start_date=hmm_train_start_date,
        hmm_max_iterations=hmm_max_iterations,
        hmm_min_training_rows=hmm_min_training_rows,
    )
    stage_outputs = _run_modal_batched_stage_outputs(writer=writer, config=config)
    result = run_daily_layer1(
        config,
        writer=writer,
        news_runner=_existing_news_runner(stage_outputs.news_output_keys_by_date),
        text_topic_runner=_existing_text_topic_runner(stage_outputs.topic_output_keys_by_date),
        finbert_runner=_existing_finbert_runner(stage_outputs.sentiment_output_keys_by_date),
        regime_runner=_existing_regime_runner(stage_outputs.regime_output_keys_by_date),
    )
    return {
        "run_id": result.run_id,
        "manifest_key": result.manifest_key,
        "validation_report_path": str(result.validation_report_path),
        "validation_report_key": result.validation_report_key,
        "processed_dates": list(result.processed_dates),
        "tickers_processed": result.tickers_processed,
        "history_files_written": result.history_files_written,
        "feature_rows_written": result.feature_rows_written,
        "ready_for_layer2": result.ready_for_layer2,
        "from_date": from_date,
        "to_date": to_date,
        "layer0_run_id": layer0_run_id,
        "allow_layer0_manifest_date_range": allow_layer0_manifest_date_range,
        "min_sentence_chars": min_sentence_chars,
        "hmm_train_start_date": hmm_train_start_date,
        "hmm_max_iterations": hmm_max_iterations,
        "hmm_min_training_rows": hmm_min_training_rows,
        "news_output_keys": stage_outputs.news_output_keys_by_date,
        "topic_output_keys": stage_outputs.topic_output_keys_by_date,
        "sentiment_output_keys": stage_outputs.sentiment_output_keys_by_date,
        "regime_output_keys": stage_outputs.regime_output_keys_by_date,
    }


def _modal_run_daily_layer1_entry(
    run_id: str,
    as_of_date: str,
    layer0_run_id: str,
    benchmark_ticker: str = "SPY",
    allow_layer0_manifest_date_range: bool = False,
    min_sentence_chars: int = 2,
    hmm_train_start_date: str | None = None,
    hmm_max_iterations: int = 100,
    hmm_min_training_rows: int = 30,
    preprocessed_news_key: str | None = None,
    topic_feature_key: str | None = None,
    sentiment_feature_key: str | None = None,
    regime_output_key: str | None = None,
) -> dict[str, object]:
    """Run final Layer 1 assembly/validation on Modal for the Pi daily flow."""
    news_runner = (
        _existing_news_runner({as_of_date: preprocessed_news_key})
        if preprocessed_news_key is not None
        else run_news_preprocessing
    )
    text_topic_runner = (
        _existing_text_topic_runner({as_of_date: topic_feature_key})
        if topic_feature_key is not None
        else run_text_topics
    )
    finbert_runner = (
        _existing_finbert_runner({as_of_date: sentiment_feature_key})
        if sentiment_feature_key is not None
        else run_finbert_sentiment
    )
    regime_runner = (
        _existing_regime_runner({as_of_date: regime_output_key})
        if regime_output_key is not None
        else run_hmm_regime_detection
    )
    result = run_daily_layer1(
        Layer1DailyConfig(
            run_id=run_id,
            from_date=as_of_date,
            to_date=as_of_date,
            layer0_run_id=layer0_run_id,
            benchmark_ticker=benchmark_ticker.strip().upper(),
            allow_layer0_manifest_date_range=allow_layer0_manifest_date_range,
            min_sentence_chars=min_sentence_chars,
            hmm_train_start_date=hmm_train_start_date,
            hmm_max_iterations=hmm_max_iterations,
            hmm_min_training_rows=hmm_min_training_rows,
        ),
        news_runner=news_runner,
        text_topic_runner=text_topic_runner,
        finbert_runner=finbert_runner,
        regime_runner=regime_runner,
    )
    return {
        "run_id": result.run_id,
        "manifest_key": result.manifest_key,
        "validation_report_path": str(result.validation_report_path),
        "processed_dates": list(result.processed_dates),
        "tickers_processed": result.tickers_processed,
        "history_files_written": result.history_files_written,
        "feature_rows_written": result.feature_rows_written,
        "ready_for_layer2": result.ready_for_layer2,
        "as_of_date": as_of_date,
        "layer0_run_id": layer0_run_id,
        "allow_layer0_manifest_date_range": allow_layer0_manifest_date_range,
        "min_sentence_chars": min_sentence_chars,
        "hmm_train_start_date": hmm_train_start_date,
        "hmm_max_iterations": hmm_max_iterations,
        "hmm_min_training_rows": hmm_min_training_rows,
        "preprocessed_news_key": preprocessed_news_key,
        "topic_feature_key": topic_feature_key,
        "sentiment_feature_key": sentiment_feature_key,
        "regime_output_key": regime_output_key,
    }


def _define_modal_app() -> object | None:
    """Create the Modal app when the modal package is installed."""
    global _modal_run_batched_layer1, _modal_run_daily_layer1

    try:
        modal = importlib.import_module("modal")
    except ModuleNotFoundError:
        return None

    runtime = load_modal_runtime_config()
    image = _build_modal_image(modal, runtime)
    app = modal.App(runtime.app_name)
    secrets = build_modal_secrets(
        modal,
        named_secret_names=(runtime.r2_secret_name,),
        env_file=SIMFIN_MODAL_ENV_FILE,
        env_keys=SIMFIN_MODAL_ENV_KEYS,
    )
    modal_run_daily_layer1 = app.function(
        image=image,
        secrets=secrets,
        timeout=runtime.timeout_seconds,
    )(_modal_run_daily_layer1_entry)
    batched_options: dict[str, object] = {
        "image": image,
        "secrets": secrets,
        "timeout": runtime.batch_timeout_seconds,
    }
    if runtime.batch_gpu_type is not None:
        batched_options["gpu"] = runtime.batch_gpu_type
    modal_run_batched_layer1 = app.function(**batched_options)(_modal_run_batched_layer1_entry)
    app.local_entrypoint()(modal_main)
    _modal_run_batched_layer1 = modal_run_batched_layer1
    _modal_run_daily_layer1 = modal_run_daily_layer1
    return app


def _build_modal_image(modal_module: object, runtime: ModalRuntimeConfig):
    """Build the Modal image while preserving local `-r base.txt` includes."""
    requirements_path = Path(runtime.requirements_path)
    requirements_dir = requirements_path.parent
    remote_requirements_path = f"{MODAL_REPO_ROOT}/{requirements_path.as_posix()}"
    return (
        modal_module.Image.debian_slim(python_version=runtime.python_version)
        .add_local_dir(_REPO_ROOT / "app", f"{MODAL_REPO_ROOT}/app", copy=True)
        .add_local_dir(_REPO_ROOT / "core", f"{MODAL_REPO_ROOT}/core", copy=True)
        .add_local_dir(_REPO_ROOT / "services", f"{MODAL_REPO_ROOT}/services", copy=True)
        .add_local_dir(_REPO_ROOT / "config", f"{MODAL_REPO_ROOT}/config", copy=True)
        .add_local_dir(
            _REPO_ROOT / requirements_dir,
            f"{MODAL_REPO_ROOT}/{requirements_dir.as_posix()}",
            copy=True,
        )
        .env(
            {
                "AI_STOCK_TRADER_REPO_ROOT": MODAL_REPO_ROOT,
                "PYTHONPATH": MODAL_REPO_ROOT,
            }
        )
        .workdir(MODAL_REPO_ROOT)
        .run_commands(f"python -m pip install -r {remote_requirements_path}")
    )


def _modal_stage_remote(module: object, attribute_name: str) -> StageModalRemoteFunction:
    """Return one stage-specific Modal remote function or raise a clear error."""
    remote_function = getattr(module, attribute_name, None)
    if remote_function is None:
        raise RuntimeError(
            f"Modal stage runner {attribute_name!r} is unavailable; ensure the modal package "
            "is installed before invoking the Layer 1 daily entrypoint."
        )
    return remote_function


def _run_module_modal_remote(
    module: object,
    attribute_name: str,
    **kwargs: object,
) -> dict[str, object]:
    """Call one stage-specific Modal function with its owning app context when available."""
    return _run_modal_remote_function(
        _modal_stage_remote(module, attribute_name),
        owning_app=getattr(module, "app", None),
        **kwargs,
    )


def _run_modal_remote_function(
    remote_function: object,
    *,
    owning_app: object | None,
    **kwargs: object,
) -> dict[str, object]:
    """Hydrate a Modal function through its owning app when running from plain Python."""
    try:
        result = remote_function.remote(**kwargs)
    except Exception as exc:  # noqa: BLE001
        if "has not been hydrated" not in str(exc):
            raise
        app_run = getattr(owning_app, "run", None)
        if not callable(app_run):
            raise
        with app_run():
            result = remote_function.remote(**kwargs)
    if result is None:
        return {}
    if not isinstance(result, dict):
        raise RuntimeError(f"Modal remote call returned unexpected payload: {result!r}")
    return result


def _require_result_key(result: Mapping[str, object], field_name: str) -> str:
    """Require one string key from a stage remote-call payload."""
    value = result.get(field_name)
    if not isinstance(value, str) or not value.strip():
        raise RuntimeError(f"Stage result is missing required field {field_name!r}: {result!r}")
    return value


def _load_completed_stage_output(
    *,
    writer: ObjectStore,
    stage: str,
    run_id: str,
    as_of_date: str,
) -> str | None:
    """Return a completed stage output key when an exact single-date stage already succeeded."""
    manifest_key = pipeline_manifest_path(stage, run_id)
    if not writer.exists(manifest_key):
        return None
    try:
        manifest = PipelineManifestRecord.model_validate_json(writer.get_object(manifest_key))
    except Exception as exc:  # noqa: BLE001
        logger.warning("Ignoring unreadable stage manifest {}: {}", manifest_key, exc)
        return None
    if manifest.status is not RunStatus.COMPLETED:
        return None
    manifest_date = str(manifest.metadata.get("as_of_date", "")).strip()
    if manifest_date and manifest_date != as_of_date:
        return None
    if manifest.output_path is None or not manifest.output_path.strip():
        return None
    if not writer.exists(manifest.output_path):
        return None
    logger.info("Reusing completed {} artifact for {}: {}", stage, as_of_date, manifest.output_path)
    return manifest.output_path


def _existing_news_runner(
    output_keys_by_date: Mapping[str, str],
) -> Callable[..., NewsPreprocessingPipelineResult]:
    """Return a lightweight runner that reuses completed preprocessing outputs."""

    def _runner(
        config: NewsPreprocessingPipelineConfig,
        *,
        writer: ObjectStore,
    ) -> NewsPreprocessingPipelineResult:
        output_key = _required_output_key(output_keys_by_date, config.as_of_date, "news")
        manifest_key = pipeline_manifest_path(NLP_PREPROCESSING_STAGE, config.run_id)
        _require_completed_stage_manifest(
            writer=writer,
            manifest_key=manifest_key,
            stage=NLP_PREPROCESSING_STAGE,
            output_key=output_key,
        )
        return NewsPreprocessingPipelineResult(
            run_id=config.run_id,
            output_key=output_key,
            manifest_key=manifest_key,
            article_rows=0,
            sentence_rows=0,
        )

    return _runner


def _existing_text_topic_runner(
    output_keys_by_date: Mapping[str, str],
) -> Callable[..., TextTopicPipelineResult]:
    """Return a lightweight runner that reuses completed text-topic outputs."""

    def _runner(
        config: TextTopicPipelineConfig,
        *,
        writer: ObjectStore,
    ) -> TextTopicPipelineResult:
        output_key = _required_output_key(output_keys_by_date, config.as_of_date, "topic features")
        manifest_key = pipeline_manifest_path(TEXT_TOPICS_STAGE, config.run_id)
        _require_completed_stage_manifest(
            writer=writer,
            manifest_key=manifest_key,
            stage=TEXT_TOPICS_STAGE,
            output_key=output_key,
        )
        return TextTopicPipelineResult(
            run_id=config.run_id,
            embedding_key="",
            topic_label_key="",
            topic_feature_key=output_key,
            manifest_key=manifest_key,
            sentence_rows=0,
            embedding_rows=0,
            topic_label_rows=0,
            topic_feature_rows=0,
        )

    return _runner


def _existing_finbert_runner(
    output_keys_by_date: Mapping[str, str],
) -> Callable[..., FinBERTPipelineResult]:
    """Return a lightweight runner that reuses completed FinBERT outputs."""

    def _runner(
        config: FinBERTPipelineConfig,
        *,
        writer: ObjectStore,
    ) -> FinBERTPipelineResult:
        output_key = _required_output_key(output_keys_by_date, config.as_of_date, "sentiment")
        manifest_key = pipeline_manifest_path(FINBERT_SENTIMENT_STAGE, config.run_id)
        _require_completed_stage_manifest(
            writer=writer,
            manifest_key=manifest_key,
            stage=FINBERT_SENTIMENT_STAGE,
            output_key=output_key,
        )
        return FinBERTPipelineResult(
            run_id=config.run_id,
            scored_news_key="",
            sentiment_feature_key=output_key,
            manifest_key=manifest_key,
            input_rows=0,
            scored_rows=0,
            feature_rows=0,
        )

    return _runner


def _existing_regime_runner(
    output_keys_by_date: Mapping[str, str],
) -> Callable[..., HMMRegimePipelineResult]:
    """Return a lightweight runner that reuses completed HMM regime outputs."""

    def _runner(
        config: HMMRegimePipelineConfig,
        *,
        writer: ObjectStore,
    ) -> HMMRegimePipelineResult:
        if not config.inference_dates:
            raise ValueError("HMMRegimePipelineConfig.inference_dates must not be empty")
        inference_date = config.inference_dates[0]
        output_key = _required_output_key(output_keys_by_date, inference_date, "regime")
        manifest_key = pipeline_manifest_path(REGIME_STAGE, config.run_id)
        _require_completed_stage_manifest(
            writer=writer,
            manifest_key=manifest_key,
            stage=REGIME_STAGE,
            output_key=output_key,
        )
        return HMMRegimePipelineResult(
            run_id=config.run_id,
            output_key=output_key,
            manifest_key=manifest_key,
            training_rows=0,
            complete_training_rows=0,
            regime_rows=0,
        )

    return _runner


def _required_output_key(
    output_keys_by_date: Mapping[str, str],
    as_of_date: str,
    branch_name: str,
) -> str:
    """Return the configured existing output key for one date-specific branch."""
    output_key = output_keys_by_date.get(as_of_date)
    if output_key is None:
        raise RuntimeError(f"Missing {branch_name} output key for {as_of_date}")
    return output_key


def _require_completed_stage_manifest(
    *,
    writer: ObjectStore,
    manifest_key: str,
    stage: str,
    output_key: str,
) -> None:
    """Require a completed stage manifest and its expected output object."""
    if not writer.exists(manifest_key):
        raise FileNotFoundError(f"Missing required stage manifest: {manifest_key}")
    manifest = PipelineManifestRecord.model_validate_json(writer.get_object(manifest_key))
    if manifest.stage != stage:
        raise ValueError(f"Expected stage={stage} manifest, got {manifest.stage!r}")
    if manifest.status is not RunStatus.COMPLETED:
        raise RuntimeError(
            f"Stage manifest must be completed before Layer 1 assembly runs: "
            f"{manifest_key}={manifest.status}"
        )
    if manifest.output_path != output_key:
        raise RuntimeError(
            f"Stage manifest output mismatch for {manifest_key}: "
            f"expected {output_key}, got {manifest.output_path!r}"
        )
    if not writer.exists(output_key):
        raise FileNotFoundError(f"Missing required stage output: {output_key}")


app = _define_modal_app()


if __name__ == "__main__":
    raise SystemExit(main())
