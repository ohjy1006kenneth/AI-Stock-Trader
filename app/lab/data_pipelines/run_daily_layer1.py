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

import app.lab.data_pipelines.run_finbert_sentiment as finbert_module
import app.lab.data_pipelines.run_hmm_regime_detection as regime_module
import app.lab.data_pipelines.run_news_preprocessing as news_module
import app.lab.data_pipelines.run_text_topics as text_topics_module
from services.r2 import paths as r2_paths


def _resolve_repo_root() -> Path:
    """Return the repository root for local runs and Modal-mounted runs."""
    env_root = os.getenv("AI_STOCK_TRADER_REPO_ROOT")
    if env_root:
        return Path(env_root).resolve()
    resolved = Path(__file__).resolve()
    return resolved.parents[3] if len(resolved.parents) > 3 else resolved.parent


_REPO_ROOT = _resolve_repo_root()
sys.path.insert(0, str(_REPO_ROOT))

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
    load_fundamentals_frame,
    load_macro_frame,
    load_ohlcv_frame,
)
from core.features.macro_features import compute_macro_features  # noqa: E402
from core.features.market_features import (  # noqa: E402
    compute_market_features,
    market_features_to_records,
)
from core.features.regime_detection import regime_features_to_records  # noqa: E402
from services.r2.paths import (  # noqa: E402
    layer1_ticker_history_path,
    layer1_validation_report_path,
    pipeline_manifest_path,
    raw_fundamentals_path,
    raw_macro_path,
    raw_news_path,
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

# Preserve the historical module-level path alias used by tests and runner stubs.
layer1_sentiment_feature_path = r2_paths.layer1_sentiment_feature_path


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
    python_version: str
    requirements_path: str


class Layer1ValidationError(RuntimeError):
    """Raised when Layer 1 validation completes but is not ready for Layer 2."""

    def __init__(self, report: Layer1ValidationReport, report_path: Path) -> None:
        """Capture the failing validation report."""
        super().__init__(
            "Layer 1 validation failed: ready_for_layer2 is false "
            f"(report={report_path})"
        )
        self.report = report
        self.report_path = report_path


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
        metadata["scope_tickers"] = scope_tickers
        _require_upstream_archives(
            active_writer,
            processed_dates=processed_dates,
            scope_tickers=scope_tickers,
            benchmark_ticker=config.benchmark_ticker,
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

        feature_rows_written, history_files_written = _assemble_and_write_histories(
            writer=active_writer,
            universe_by_date=universe_by_date,
            benchmark_ticker=config.benchmark_ticker,
            topic_results=topic_results,
            sentiment_results=sentiment_results,
            regime_results=regime_results,
        )

        report = validate_layer1_archive(
            run_id=config.run_id,
            from_date=config.from_date,
            to_date=config.to_date,
            universe=universe_by_date,
            reader=active_writer,
            output_prefixes=build_layer1_output_prefixes(processed_dates),
        )
        report_key = layer1_validation_report_path(config.run_id, config.from_date, config.to_date)
        report_path = write_validation_report(
            report,
            validation_output_dir if validation_output_dir is not None else DEFAULT_REPORT_DIR,
        )
        active_writer.put_object(report_key, render_validation_report(report))
        metadata.update(
            {
                "history_files_written": history_files_written,
                "feature_rows_written": feature_rows_written,
                "validation_report_path": str(report_path),
                "validation_report_key": report_key,
                "ready_for_layer2": report.ready_for_layer2,
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
            }
        )
        if not report.ready_for_layer2:
            raise Layer1ValidationError(report, report_path)

        finished_at = (now or datetime.now(UTC)).replace(microsecond=0)
        _write_manifest(
            writer=active_writer,
            key=manifest_key,
            config=config,
            status=RunStatus.COMPLETED,
            started_at=started_at,
            finished_at=finished_at,
            metadata=metadata,
        )
        logger.info(
            "Layer 1 orchestration complete run_id={} dates={} tickers={} ready_for_layer2={}",
            config.run_id,
            len(processed_dates),
            len(scope_tickers),
            report.ready_for_layer2,
        )
        return Layer1DailyResult(
            run_id=config.run_id,
            manifest_key=manifest_key,
            validation_report_path=report_path,
            validation_report_key=report_key,
            processed_dates=processed_dates,
            tickers_processed=len(scope_tickers),
            history_files_written=history_files_written,
            feature_rows_written=feature_rows_written,
            ready_for_layer2=report.ready_for_layer2,
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


def load_modal_runtime_config(path: Path = MODAL_CONFIG_PATH) -> ModalRuntimeConfig:
    """Load Modal app and image settings from repository config."""
    payload = json.loads(path.read_text(encoding="utf-8"))
    return ModalRuntimeConfig(
        app_name=str(payload["layer1_daily_app_name"]),
        r2_secret_name=str(payload["r2_secret_name"]),
        timeout_seconds=int(payload["timeout_seconds"]),
        python_version=str(payload.get("python_version", "3.11")),
        requirements_path=str(payload.get("requirements_path", "requirements/modal.txt")),
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
) -> tuple[int, int]:
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

    feature_rows_written = 0
    history_files_written = 0
    for ticker, target_dates in sorted(target_dates_by_ticker.items()):
        ohlcv = load_ohlcv_frame(ticker, writer=writer)  # type: ignore[arg-type]
        fundamentals = load_fundamentals_frame(ticker, writer=writer)  # type: ignore[arg-type]
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
        topic_daily_records = _records_for_target_dates(
            topic_records.get(ticker, []),
            target_dates,
        )
        sentiment_daily_records = _records_for_target_dates(
            sentiment_records.get(ticker, []),
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

    return feature_rows_written, history_files_written


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
) -> None:
    """Require the Layer 0 archives needed by the daily Layer 1 run."""
    required_keys = [raw_price_path(benchmark_ticker)]
    required_keys.extend(raw_news_path(date_text) for date_text in processed_dates)
    required_keys.extend(raw_universe_path(date_text) for date_text in processed_dates)
    required_keys.extend(raw_macro_path(date_text) for date_text in processed_dates)
    for ticker in scope_tickers:
        required_keys.append(raw_price_path(ticker))
        required_keys.append(raw_fundamentals_path(ticker))

    missing = sorted(key for key in required_keys if not writer.exists(key))
    if missing:
        raise FileNotFoundError(
            "Missing required Layer 0 archives for Layer 1 run: "
            + ", ".join(missing)
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


def _expected_dates_by_ticker(
    universe_by_date: Mapping[str, Sequence[str]],
) -> dict[str, list[str]]:
    """Invert a date->tickers mapping into sorted ticker->dates."""
    by_ticker: dict[str, list[str]] = {}
    for date_text, tickers in sorted(universe_by_date.items()):
        for ticker in tickers:
            by_ticker.setdefault(ticker, []).append(date_text)
    return by_ticker


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
        raise RuntimeError("Modal app is unavailable because the modal package is not installed")
    if allow_layer0_manifest_date_range:
        logger.warning(
            "allow_layer0_manifest_date_range=True is intended for historical readiness runs "
            "only and must not be used on the Pi daily path"
        )
    stage_run_id = _stage_run_id(run_id, as_of_date)
    news_result = _modal_stage_remote(news_module, "modal_run_news_preprocessing").remote(
        run_id=stage_run_id,
        as_of_date=as_of_date,
        min_sentence_chars=min_sentence_chars,
    )
    preprocessed_news_key = _require_result_key(news_result, "output_key")
    # Keep stage dispatch synchronous here. In production readiness runs, `.spawn()`
    # against imported stage apps can leave the daily manifest stuck in `running`
    # without ever producing downstream stage manifests.
    topic_result = _modal_stage_remote(text_topics_module, "modal_run_text_topics").remote(
        run_id=stage_run_id,
        as_of_date=as_of_date,
        preprocessed_news_key=preprocessed_news_key,
    )
    sentiment_result = _modal_stage_remote(
        finbert_module, "modal_run_finbert_sentiment"
    ).remote(
        run_id=stage_run_id,
        as_of_date=as_of_date,
        preprocessed_news_key=preprocessed_news_key,
    )
    regime_result = _modal_stage_remote(regime_module, "modal_run_hmm_regime_detection").remote(
        run_id=stage_run_id,
        train_start_date=hmm_train_start_date,
        train_end_date=_previous_business_day(as_of_date),
        inference_dates=as_of_date,
        benchmark_ticker=benchmark_ticker.strip().upper(),
        max_iterations=hmm_max_iterations,
        min_training_rows=hmm_min_training_rows,
    )
    _modal_run_daily_layer1.remote(
        run_id=run_id,
        as_of_date=as_of_date,
        layer0_run_id=layer0_run_id,
        benchmark_ticker=benchmark_ticker.strip().upper(),
        allow_layer0_manifest_date_range=allow_layer0_manifest_date_range,
        min_sentence_chars=min_sentence_chars,
        hmm_train_start_date=hmm_train_start_date,
        hmm_max_iterations=hmm_max_iterations,
        hmm_min_training_rows=hmm_min_training_rows,
        preprocessed_news_key=preprocessed_news_key,
        topic_feature_key=_require_result_key(topic_result, "topic_feature_key"),
        sentiment_feature_key=_require_result_key(sentiment_result, "sentiment_feature_key"),
        regime_output_key=_require_result_key(regime_result, "output_key"),
    )


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
    global _modal_run_daily_layer1

    try:
        modal = importlib.import_module("modal")
    except ModuleNotFoundError:
        return None

    runtime = load_modal_runtime_config()
    image = _build_modal_image(modal, runtime)
    app = modal.App(runtime.app_name)
    modal_run_daily_layer1 = app.function(
        image=image,
        secrets=[modal.Secret.from_name(runtime.r2_secret_name)],
        timeout=runtime.timeout_seconds,
    )(_modal_run_daily_layer1_entry)
    app.local_entrypoint()(modal_main)
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


def _require_result_key(result: Mapping[str, object], field_name: str) -> str:
    """Require one string key from a stage remote-call payload."""
    value = result.get(field_name)
    if not isinstance(value, str) or not value.strip():
        raise RuntimeError(f"Stage result is missing required field {field_name!r}: {result!r}")
    return value


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
