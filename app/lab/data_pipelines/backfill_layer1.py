"""Modal-ready Layer 1 production-run orchestrator.

Reads Layer 0 R2 archives and produces aligned feature histories for every
ticker in the supplied ticker list. Per-branch feature
computation respects each module's documented leakage invariant; the final
assembly step also runs `assemble_layer1_feature_records` to defend against
input misconfiguration.

The orchestrator iterates per ticker:
    1. Load OHLCV + fundamentals + macro from R2.
    2. Compute market features (M2.2) using SPY (or another benchmark) as
       cross-asset context when available.
    3. Compute context features (M2.3 + M2.4) — fundamentals merged with
       macro/rates broadcast across every trading day.
    4. Wrap each branch's output in a `Layer1FeatureInput` and assemble into
       FeatureRecords with leakage validation.
    5. Persist each ticker history as `features/layer1/{ticker}.parquet`.

NLP and regime features have their own dedicated runners
(`run_text_topics.py`, `run_finbert_sentiment.py`, regime training); the
production validator (`validate_layer1_archive.py`) checks history-file presence
across the universe.
"""
from __future__ import annotations

import argparse
import importlib
import io
import json
import sys
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Protocol
from zoneinfo import ZoneInfo

from loguru import logger

_REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(_REPO_ROOT))

from core.contracts.schemas import FeatureRecord, PipelineManifestRecord, RunStatus  # noqa: E402
from core.features.assembly import Layer1FeatureInput, assemble_layer1_feature_records  # noqa: E402
from core.features.context_features import (  # noqa: E402
    compute_context_features,
    context_features_to_records,
)
from core.features.io import parquet_bytes_to_feature_records, write_feature_records  # noqa: E402
from core.features.loaders import (  # noqa: E402
    load_fundamentals_frame,
    load_macro_frame,
    load_ohlcv_frame,
    load_order_book_frame,
)
from core.features.market_features import (  # noqa: E402
    compute_market_features,
    market_features_to_records,
)
from core.features.order_book_features import (  # noqa: E402
    compute_order_book_features,
    order_book_features_to_records,
)
from core.features.regime_detection import HMM_REGIME_FEATURE_COLUMNS  # noqa: E402
from core.features.sector_features import (  # noqa: E402
    compute_sector_features,
    load_sector_etf_config,
    sector_features_to_records,
)
from services.modal.secrets import (  # noqa: E402
    SIMFIN_MODAL_ENV_FILE,
    SIMFIN_MODAL_ENV_KEYS,
    build_modal_secrets,
)
from services.order_book.config import load_order_book_feature_config  # noqa: E402
from services.r2.paths import (  # noqa: E402
    build_r2_key,
    layer1_regime_path,
    legacy_layer1_regime_path,
    pipeline_manifest_path,
    raw_order_book_path,
)
from services.r2.writer import R2Writer  # noqa: E402

LAYER1_BACKFILL_STAGE = "layer1_backfill"
FINBERT_SENTIMENT_STAGE = "layer1_finbert_sentiment"
TEXT_TOPICS_STAGE = "layer1_text_topics"
REGIME_STAGE = "layer1_5_regime"
MODAL_CONFIG_PATH = _REPO_ROOT / "config" / "modal.json"
MODAL_CONFIG_PATH = _REPO_ROOT / "config" / "modal.json"
SENTINEL_ASSEMBLY_AS_OF = datetime(1900, 1, 1, 0, 0, 0, tzinfo=ZoneInfo("America/New_York"))


class ObjectStore(Protocol):
    """Object-store operations required by the Layer 1 backfill runner."""

    def put_object(self, key: str, data: bytes | str) -> None:
        """Write an object to storage."""

    def get_object(self, key: str) -> bytes:
        """Read an object from storage."""

    def list_keys(self, prefix: str) -> list[str]:
        """List object keys beneath a prefix."""


@dataclass(frozen=True)
class Layer1BackfillConfig:
    """Configuration for one Layer 1 backfill run."""

    run_id: str
    tickers: tuple[str, ...]
    benchmark_ticker: str = "SPY"
    require_sentiment_features: bool = False
    require_topic_features: bool = False
    require_regime_features: bool = False

    def __post_init__(self) -> None:
        """Validate run identity and ticker list."""
        if not self.run_id.strip():
            raise ValueError("run_id cannot be empty")
        if not self.tickers:
            raise ValueError("at least one ticker must be supplied")
        for ticker in self.tickers:
            if not ticker.strip():
                raise ValueError("tickers cannot contain empty strings")


@dataclass(frozen=True)
class OptionalFeatureBranch:
    """Loaded per-ticker optional FeatureRecord artifacts for one branch."""

    name: str
    records_by_ticker: dict[str, tuple[FeatureRecord, ...]]
    covered_dates: frozenset[str]
    artifact_keys: tuple[str, ...]


@dataclass(frozen=True)
class OptionalRegimeBranch:
    """Loaded market-wide regime features keyed by date."""

    features_by_date: dict[str, dict[str, object]]
    covered_dates: frozenset[str]
    artifact_keys: tuple[str, ...]


@dataclass(frozen=True)
class OptionalOrderBookBranch:
    """Loaded order-book FeatureRecords keyed by ticker."""

    enabled: bool
    provider: str | None
    records_by_ticker: dict[str, tuple[FeatureRecord, ...]]
    covered_dates: frozenset[str]
    archive_keys: tuple[str, ...]
    missing_dates: tuple[str, ...]


@dataclass(frozen=True)
class Layer1BackfillResult:
    """Summary of one Layer 1 backfill run."""

    run_id: str
    tickers_processed: int
    ticker_files_written: int
    feature_rows_written: int
    started_at: datetime
    finished_at: datetime
    manifest_key: str


@dataclass(frozen=True)
class ModalRuntimeConfig:
    """Modal app configuration for Layer 1 backfill."""

    app_name: str
    r2_secret_name: str
    timeout_seconds: int
    python_version: str = "3.11"
    requirements_path: str = "requirements/modal.txt"

    def __post_init__(self) -> None:
        """Validate Modal runtime settings loaded from repository config."""
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


def backfill_layer1(
    config: Layer1BackfillConfig,
    *,
    writer: ObjectStore | None = None,
    now: datetime | None = None,
) -> Layer1BackfillResult:
    """Compute aligned Layer 1 feature histories for every requested ticker."""
    started = (now or datetime.now(UTC)).replace(microsecond=0)
    active_writer = writer or R2Writer()

    macro_frame = load_macro_frame(writer=active_writer)
    benchmark_bars = _try_load_benchmark(active_writer, config.benchmark_ticker)
    ohlcv_by_ticker: dict[str, object] = {}
    fundamentals_by_ticker: dict[str, object] = {}

    ticker_files_written = 0
    feature_rows_written = 0
    processed_dates: set[str] = set()
    sentiment_branch = OptionalFeatureBranch(
        name="sentiment",
        records_by_ticker={},
        covered_dates=frozenset(),
        artifact_keys=(),
    )
    topic_branch = OptionalFeatureBranch(
        name="topics",
        records_by_ticker={},
        covered_dates=frozenset(),
        artifact_keys=(),
    )
    regime_branch = OptionalRegimeBranch(
        features_by_date={},
        covered_dates=frozenset(),
        artifact_keys=(),
    )
    order_book_branch = OptionalOrderBookBranch(
        enabled=False,
        provider=None,
        records_by_ticker={},
        covered_dates=frozenset(),
        archive_keys=(),
        missing_dates=(),
    )
    try:
        sentiment_branch = _load_optional_feature_branch(
            writer=active_writer,
            stage=FINBERT_SENTIMENT_STAGE,
            branch_name="sentiment",
            require_artifacts=config.require_sentiment_features,
        )
        topic_branch = _load_optional_feature_branch(
            writer=active_writer,
            stage=TEXT_TOPICS_STAGE,
            branch_name="topics",
            require_artifacts=config.require_topic_features,
        )
        regime_branch = _load_optional_regime_branch(
            writer=active_writer,
            require_artifacts=config.require_regime_features,
        )

        for ticker in config.tickers:
            logger.info("Backfilling Layer 1 features for ticker={}", ticker)
            try:
                ohlcv = load_ohlcv_frame(ticker, writer=active_writer)
            except FileNotFoundError:
                logger.warning("Skipping ticker={} (no OHLCV archive)", ticker)
                continue
            try:
                fundamentals = load_fundamentals_frame(ticker, writer=active_writer)
            except FileNotFoundError:
                fundamentals = _empty_fundamentals_frame(ohlcv)
            ohlcv_by_ticker[ticker] = ohlcv
            fundamentals_by_ticker[ticker] = fundamentals
            processed_dates.update(frozenset(str(value) for value in ohlcv["date"].astype(str).tolist()))

        sector_config = load_sector_etf_config()
        sector_records_by_ticker = {
            ticker: sector_features_to_records(frame)
            for ticker, frame in compute_sector_features(
                ohlcv_by_ticker=ohlcv_by_ticker,
                fundamentals_by_ticker=fundamentals_by_ticker,
                sector_price_frames=_load_sector_price_frames(active_writer, sector_config),
                sector_config=sector_config,
            ).items()
        }
        order_book_branch = _load_optional_order_book_branch(
            writer=active_writer,
            ohlcv_by_ticker=ohlcv_by_ticker,
        )

        for ticker in config.tickers:
            if ticker not in ohlcv_by_ticker:
                continue
            ohlcv = ohlcv_by_ticker[ticker]
            fundamentals = fundamentals_by_ticker[ticker]
            market_records = _compute_market_records(
                ticker=ticker,
                ohlcv=ohlcv,
                benchmark_bars=benchmark_bars,
            )
            context_records = _compute_context_records(
                ticker=ticker,
                ohlcv=ohlcv,
                fundamentals=fundamentals,
                macro=macro_frame,
            )
            ticker_dates = frozenset(str(value) for value in ohlcv["date"].astype(str).tolist())

            inputs = [
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
                    records=sector_records_by_ticker.get(ticker, []),
                    as_of_timestamp=SENTINEL_ASSEMBLY_AS_OF,
                ),
            ]
            sentiment_input = _optional_branch_input(
                name="sentiment",
                records=_records_for_ticker_branch(
                    sentiment_branch,
                    ticker=ticker,
                    allowed_dates=ticker_dates,
                ),
            )
            if sentiment_input is not None:
                inputs.append(sentiment_input)

            topic_input = _optional_branch_input(
                name="topics",
                records=_records_for_ticker_branch(
                    topic_branch,
                    ticker=ticker,
                    allowed_dates=ticker_dates,
                ),
            )
            if topic_input is not None:
                inputs.append(topic_input)

            order_book_input = _optional_branch_input(
                name="order_book",
                records=_records_for_ticker_branch(
                    order_book_branch,
                    ticker=ticker,
                    allowed_dates=ticker_dates,
                ),
            )
            if order_book_input is not None:
                inputs.append(order_book_input)

            regime_input = _optional_branch_input(
                name="regime",
                records=_regime_records_for_ticker(
                    regime_branch,
                    ticker=ticker,
                    allowed_dates=ticker_dates,
                ),
            )
            if regime_input is not None:
                inputs.append(regime_input)

            assembled = assemble_layer1_feature_records(inputs)
            written_keys = write_feature_records(assembled, writer=active_writer)
            ticker_files_written += len(written_keys)
            feature_rows_written += len(assembled)

        missing_sentiment_dates = _missing_branch_dates(processed_dates, sentiment_branch.covered_dates)
        missing_topic_dates = _missing_branch_dates(processed_dates, topic_branch.covered_dates)
        missing_regime_dates = _missing_branch_dates(processed_dates, regime_branch.covered_dates)
        _log_missing_optional_branch_dates("sentiment", missing_sentiment_dates)
        _log_missing_optional_branch_dates("topics", missing_topic_dates)
        _log_missing_optional_branch_dates("order_book", order_book_branch.missing_dates)
        _log_missing_optional_branch_dates("regime", missing_regime_dates)

        finished = (now or datetime.now(UTC)).replace(microsecond=0)
        manifest_key = _write_manifest(
            active_writer,
            run_id=config.run_id,
            status=RunStatus.COMPLETED,
            started_at=started,
            finished_at=finished,
            metadata={
                "tickers_processed": len(config.tickers),
                "ticker_files_written": ticker_files_written,
                "feature_rows_written": feature_rows_written,
                "benchmark_ticker": config.benchmark_ticker,
                "sentiment_artifacts_loaded": len(sentiment_branch.artifact_keys),
                "topic_artifacts_loaded": len(topic_branch.artifact_keys),
                "order_book_enabled": order_book_branch.enabled,
                "order_book_provider": order_book_branch.provider,
                "order_book_artifacts_loaded": len(order_book_branch.archive_keys),
                "missing_order_book_dates": len(order_book_branch.missing_dates),
                "regime_artifacts_loaded": len(regime_branch.artifact_keys),
                "missing_sentiment_dates": len(missing_sentiment_dates),
                "missing_topic_dates": len(missing_topic_dates),
                "missing_regime_dates": len(missing_regime_dates),
            },
        )
        logger.info(
            "Layer 1 backfill finished run_id={} ticker_files={} rows={}",
            config.run_id,
            ticker_files_written,
            feature_rows_written,
        )
        return Layer1BackfillResult(
            run_id=config.run_id,
            tickers_processed=len(config.tickers),
            ticker_files_written=ticker_files_written,
            feature_rows_written=feature_rows_written,
            started_at=started,
            finished_at=finished,
            manifest_key=manifest_key,
        )
    except Exception:
        finished = (now or datetime.now(UTC)).replace(microsecond=0)
        _write_manifest(
            active_writer,
            run_id=config.run_id,
            status=RunStatus.FAILED,
            started_at=started,
            finished_at=finished,
            metadata={
                "tickers_processed": len(config.tickers),
                "ticker_files_written": ticker_files_written,
                "feature_rows_written": feature_rows_written,
                "benchmark_ticker": config.benchmark_ticker,
                "sentiment_artifacts_loaded": len(sentiment_branch.artifact_keys),
                "topic_artifacts_loaded": len(topic_branch.artifact_keys),
                "order_book_enabled": order_book_branch.enabled,
                "order_book_provider": order_book_branch.provider,
                "order_book_artifacts_loaded": len(order_book_branch.archive_keys),
                "regime_artifacts_loaded": len(regime_branch.artifact_keys),
            },
        )
        raise


def _load_optional_feature_branch(
    *,
    writer: ObjectStore,
    stage: str,
    branch_name: str,
    require_artifacts: bool,
) -> OptionalFeatureBranch:
    """Load the latest completed daily FeatureRecord artifact for each branch date."""
    manifests = _latest_completed_daily_manifests(writer=writer, stage=stage)
    if not manifests:
        message = f"No completed {branch_name} artifacts found for stage={stage}"
        if require_artifacts:
            raise FileNotFoundError(message)
        logger.warning(message)
        return OptionalFeatureBranch(
            name=branch_name,
            records_by_ticker={},
            covered_dates=frozenset(),
            artifact_keys=(),
        )

    records_by_ticker: dict[str, list[FeatureRecord]] = {}
    covered_dates: set[str] = set()
    artifact_keys: list[str] = []
    seen_keys: set[tuple[str, str]] = set()

    for as_of_date, manifest in sorted(manifests.items()):
        output_path = _manifest_output_path(manifest, branch_name=branch_name)
        try:
            payload = writer.get_object(output_path)
        except FileNotFoundError:
            message = (
                f"{branch_name} artifact missing for date={as_of_date} "
                f"run_id={manifest.run_id}: {output_path}"
            )
            if require_artifacts:
                raise FileNotFoundError(message) from None
            logger.warning(message)
            continue

        records = parquet_bytes_to_feature_records(payload)
        _validate_daily_feature_records(
            records,
            branch_name=branch_name,
            as_of_date=as_of_date,
            artifact_key=output_path,
        )
        artifact_keys.append(output_path)
        covered_dates.add(as_of_date)
        for record in records:
            key = (record.date, record.ticker)
            if key in seen_keys:
                raise ValueError(
                    f"Duplicate {branch_name} FeatureRecord across selected artifacts for "
                    f"{record.date}/{record.ticker}"
                )
            seen_keys.add(key)
            records_by_ticker.setdefault(record.ticker, []).append(record)

    return OptionalFeatureBranch(
        name=branch_name,
        records_by_ticker={
            ticker: tuple(sorted(ticker_records, key=lambda record: record.date))
            for ticker, ticker_records in sorted(records_by_ticker.items())
        },
        covered_dates=frozenset(covered_dates),
        artifact_keys=tuple(artifact_keys),
    )


def _load_optional_regime_branch(
    *,
    writer: ObjectStore,
    require_artifacts: bool,
) -> OptionalRegimeBranch:
    """Load the latest completed regime row for each inference date."""
    manifests = _completed_manifests(writer=writer, stage=REGIME_STAGE)
    if not manifests:
        message = "No completed regime artifacts found for stage=layer1_5_regime"
        if require_artifacts:
            raise FileNotFoundError(message)
        logger.warning(message)
        return OptionalRegimeBranch(
            features_by_date={},
            covered_dates=frozenset(),
            artifact_keys=(),
        )

    selected_rows: dict[str, tuple[datetime, str, dict[str, object]]] = {}
    for manifest in manifests:
        output_paths = _manifest_regime_output_paths(manifest)
        legacy_single_output = len(output_paths) == 1

        expected_dates = _metadata_inference_dates(manifest)
        train_end_date = _metadata_iso_date(manifest, "train_end_date", required=False)
        ranking_timestamp = manifest.finished_at or manifest.started_at

        for output_path in output_paths:
            try:
                frame = _parquet_bytes_to_frame(writer.get_object(output_path))
            except FileNotFoundError:
                message = f"regime artifact missing for run_id={manifest.run_id}: {output_path}"
                if require_artifacts:
                    raise FileNotFoundError(message) from None
                logger.warning(message)
                continue

            _require_columns(frame, ("date", *HMM_REGIME_FEATURE_COLUMNS), branch_name="regime")
            seen_dates_in_file: set[str] = set()
            for row in frame.to_dict(orient="records"):
                date_value = _validated_iso_date(str(row["date"]), label="regime date")
                expected_output_path = layer1_regime_path(date_value, manifest.run_id)
                legacy_output_path = legacy_layer1_regime_path(manifest.run_id)
                if (
                    output_path not in {expected_output_path, legacy_output_path}
                    and not legacy_single_output
                ):
                    raise ValueError(
                        f"regime manifest {manifest.run_id} output_path for date={date_value} "
                        f"must equal {expected_output_path}, got {output_path}"
                    )
                if date_value in seen_dates_in_file:
                    raise ValueError(
                        f"Duplicate regime rows found in artifact {output_path} "
                        f"for date={date_value}"
                    )
                seen_dates_in_file.add(date_value)
                if expected_dates is not None and date_value not in expected_dates:
                    raise ValueError(
                        f"regime artifact {output_path} contains unexpected date={date_value}"
                    )
                if train_end_date is not None and date_value <= train_end_date:
                    raise ValueError(
                        f"regime artifact {output_path} contains non-forward date={date_value} "
                        f"for train_end_date={train_end_date}"
                    )

                normalized_features = {
                    "regime_label": _optional_string(row.get("regime_label")),
                    "regime_confidence": _optional_numeric(row.get("regime_confidence")),
                    "regime_prob_bear": _optional_numeric(row.get("regime_prob_bear")),
                    "regime_prob_sideways": _optional_numeric(row.get("regime_prob_sideways")),
                    "regime_prob_bull": _optional_numeric(row.get("regime_prob_bull")),
                }
                current = selected_rows.get(date_value)
                if current is None or ranking_timestamp >= current[0]:
                    selected_rows[date_value] = (
                        ranking_timestamp,
                        output_path,
                        normalized_features,
                    )

    if not selected_rows and require_artifacts:
        raise FileNotFoundError(
            "No readable regime artifacts were available for Layer 1 assembly"
        )

    return OptionalRegimeBranch(
        features_by_date={
            date_value: payload[2]
            for date_value, payload in sorted(selected_rows.items())
        },
        covered_dates=frozenset(selected_rows.keys()),
        artifact_keys=tuple(sorted({payload[1] for payload in selected_rows.values()})),
    )


def _latest_completed_daily_manifests(
    *,
    writer: ObjectStore,
    stage: str,
) -> dict[str, PipelineManifestRecord]:
    """Return the latest completed manifest for each daily branch date."""
    manifests = _completed_manifests(writer=writer, stage=stage)
    selected: dict[str, PipelineManifestRecord] = {}
    for manifest in manifests:
        as_of_date = _metadata_iso_date(manifest, "as_of_date", required=True)
        current = selected.get(as_of_date)
        current_finished_at = current.finished_at if current is not None else None
        finished_at = manifest.finished_at or manifest.started_at
        if current is None or current_finished_at is None or finished_at >= current_finished_at:
            selected[as_of_date] = manifest
    return selected


def _completed_manifests(
    *,
    writer: ObjectStore,
    stage: str,
) -> list[PipelineManifestRecord]:
    """Load completed manifests for one pipeline stage."""
    prefix = build_r2_key("artifacts", "manifests", stage)
    manifests: list[PipelineManifestRecord] = []
    for key in sorted(writer.list_keys(prefix)):
        manifest = PipelineManifestRecord.model_validate_json(writer.get_object(key))
        if manifest.status == RunStatus.COMPLETED:
            manifests.append(manifest)
    return manifests


def _manifest_regime_output_paths(manifest: PipelineManifestRecord) -> tuple[str, ...]:
    """Return regime output paths from new per-date metadata or legacy output_path."""
    raw_output_keys = manifest.metadata.get("output_keys_by_date")
    if isinstance(raw_output_keys, dict):
        paths = tuple(
            sorted(
                value
                for value in raw_output_keys.values()
                if isinstance(value, str) and value.strip()
            )
        )
        if paths:
            return paths
    return (_manifest_output_path(manifest, branch_name="regime"),)


def _manifest_output_path(manifest: PipelineManifestRecord, *, branch_name: str) -> str:
    """Return the output path declared by a completed manifest."""
    if manifest.output_path is None or not manifest.output_path.strip():
        raise ValueError(f"{branch_name} manifest {manifest.run_id} is missing output_path")
    return manifest.output_path


def _metadata_iso_date(
    manifest: PipelineManifestRecord,
    field_name: str,
    *,
    required: bool,
) -> str | None:
    """Return one validated ISO date from manifest metadata."""
    raw_value = manifest.metadata.get(field_name)
    if raw_value is None:
        if required:
            raise ValueError(f"manifest {manifest.run_id} is missing metadata[{field_name!r}]")
        return None
    if not isinstance(raw_value, str):
        raise ValueError(f"manifest {manifest.run_id} metadata[{field_name!r}] must be a string")
    return _validated_iso_date(raw_value, label=field_name)


def _metadata_inference_dates(
    manifest: PipelineManifestRecord,
) -> set[str] | None:
    """Return the validated regime inference dates from manifest metadata when present."""
    raw_value = manifest.metadata.get("inference_dates")
    if raw_value is None:
        return None
    if not isinstance(raw_value, list):
        raise ValueError(f"manifest {manifest.run_id} metadata['inference_dates'] must be a list")
    return {
        _validated_iso_date(str(value), label="inference_dates")
        for value in raw_value
    }


def _validate_daily_feature_records(
    records: Sequence[FeatureRecord],
    *,
    branch_name: str,
    as_of_date: str,
    artifact_key: str,
) -> None:
    """Reject duplicate or cross-date rows inside one daily branch artifact."""
    seen_keys: set[tuple[str, str]] = set()
    for record in records:
        if record.date != as_of_date:
            raise ValueError(
                f"{branch_name} artifact {artifact_key} contains record date={record.date} "
                f"outside as_of_date={as_of_date}"
            )
        key = (record.date, record.ticker)
        if key in seen_keys:
            raise ValueError(
                f"Duplicate {branch_name} rows found in artifact {artifact_key} for "
                f"{record.date}/{record.ticker}"
            )
        seen_keys.add(key)


def _records_for_ticker_branch(
    branch: OptionalFeatureBranch | OptionalOrderBookBranch,
    *,
    ticker: str,
    allowed_dates: frozenset[str],
) -> list[FeatureRecord]:
    """Return selected optional branch rows for one ticker and date set."""
    return [
        record
        for record in branch.records_by_ticker.get(ticker, ())
        if record.date in allowed_dates
    ]


def _regime_records_for_ticker(
    branch: OptionalRegimeBranch,
    *,
    ticker: str,
    allowed_dates: frozenset[str],
) -> list[FeatureRecord]:
    """Broadcast market-wide regime features onto one ticker's available dates."""
    return [
        FeatureRecord(date=date_value, ticker=ticker, features=dict(features))
        for date_value, features in sorted(branch.features_by_date.items())
        if date_value in allowed_dates
    ]


def _optional_branch_input(
    *,
    name: str,
    records: Sequence[FeatureRecord],
) -> Layer1FeatureInput | None:
    """Wrap optional records in a validated assembly input when rows exist."""
    if not records:
        return None
    return Layer1FeatureInput(
        name=name,
        records=list(records),
        as_of_timestamp=SENTINEL_ASSEMBLY_AS_OF,
    )


def _missing_branch_dates(processed_dates: set[str], covered_dates: frozenset[str]) -> list[str]:
    """Return processed dates that had no selected optional artifact coverage."""
    return sorted(date_value for date_value in processed_dates if date_value not in covered_dates)


def _log_missing_optional_branch_dates(branch_name: str, missing_dates: Sequence[str]) -> None:
    """Log one aggregate warning when optional branch coverage is incomplete."""
    if not missing_dates:
        return
    sample = ", ".join(missing_dates[:5])
    suffix = "" if len(missing_dates) <= 5 else ", ..."
    logger.warning(
        "Layer 1 {} artifacts missing for {} processed dates: {}{}",
        branch_name,
        len(missing_dates),
        sample,
        suffix,
    )


def _compute_market_records(
    *,
    ticker: str,
    ohlcv,
    benchmark_bars,
) -> list[FeatureRecord]:
    """Return market FeatureRecords stripped of the all-NaN warm-up rows."""
    features = compute_market_features(ohlcv, ticker, benchmark_bars=benchmark_bars)
    records = market_features_to_records(features)
    return [record for record in records if any(value is not None for value in record.features.values())]


def _compute_context_records(
    *,
    ticker: str,
    ohlcv,
    fundamentals,
    macro,
) -> list[FeatureRecord]:
    """Return context FeatureRecords for the given ticker."""
    features = compute_context_features(
        fundamentals=fundamentals,
        ohlcv=ohlcv,
        macro=macro,
        ticker=ticker,
    )
    return context_features_to_records(features)


def _load_optional_order_book_branch(
    *,
    writer: ObjectStore,
    ohlcv_by_ticker: dict[str, object],
) -> OptionalOrderBookBranch:
    """Load optional order-book records for every available OHLCV date in scope."""
    config = load_order_book_feature_config()
    if not config.is_active or config.provider is None:
        return OptionalOrderBookBranch(
            enabled=False,
            provider=config.provider,
            records_by_ticker={},
            covered_dates=frozenset(),
            archive_keys=(),
            missing_dates=(),
        )

    records_by_ticker: dict[str, list[FeatureRecord]] = {}
    covered_dates: set[str] = set()
    archive_keys: list[str] = []
    missing_dates: set[str] = set()
    date_tickers_map: dict[str, set[str]] = {}
    for ticker, ohlcv in ohlcv_by_ticker.items():
        for date_value in ohlcv["date"].astype(str).tolist():
            date_tickers_map.setdefault(str(date_value), set()).add(ticker)

    for date_text, tickers in sorted(date_tickers_map.items()):
        key = raw_order_book_path(config.provider, date_text)
        if writer.exists(key):
            frame = load_order_book_frame(config.provider, date_text, writer=writer)
            covered_dates.add(date_text)
            archive_keys.append(key)
        else:
            frame = _empty_order_book_source_frame()
            missing_dates.add(date_text)

        day_records = order_book_features_to_records(
            compute_order_book_features(
                frame,
                target_date=date_text,
                tickers=sorted(tickers),
            )
        )
        for record in day_records:
            records_by_ticker.setdefault(record.ticker, []).append(record)

    return OptionalOrderBookBranch(
        enabled=True,
        provider=config.provider,
        records_by_ticker={
            ticker: tuple(sorted(records, key=lambda record: record.date))
            for ticker, records in sorted(records_by_ticker.items())
        },
        covered_dates=frozenset(covered_dates),
        archive_keys=tuple(archive_keys),
        missing_dates=tuple(sorted(missing_dates)),
    )


def _try_load_benchmark(writer: ObjectStore, ticker: str):
    """Return the benchmark OHLCV frame when available, else None."""
    try:
        return load_ohlcv_frame(ticker, writer=writer)
    except FileNotFoundError:
        logger.warning("Benchmark OHLCV missing for ticker={}; cross-asset features will be NaN", ticker)
        return None


def _load_sector_price_frames(writer: ObjectStore, sector_config) -> dict[str, object]:
    """Return the configured sector ETF histories available to the backfill."""
    frames: dict[str, object] = {}
    for etf_ticker in sorted(set(sector_config.sector_to_etf.values())):
        try:
            frames[etf_ticker] = load_ohlcv_frame(etf_ticker, writer=writer)
        except FileNotFoundError:
            logger.warning(
                "Sector ETF OHLCV missing for ticker={}; related sector features will be null",
                etf_ticker,
            )
    return frames


def _empty_fundamentals_frame(ohlcv):
    """Return an empty fundamentals frame matching the columns expected by features."""
    import pandas as pd

    return pd.DataFrame(
        columns=[
            "report_date",
            "availability_date",
            "fiscal_year",
            "fiscal_period",
            "raw_json",
            "earnings_date",
        ]
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


def _parquet_bytes_to_frame(payload: bytes):
    """Deserialize Parquet bytes into a pandas DataFrame."""
    pd = _require_pandas()
    return pd.read_parquet(io.BytesIO(payload))


def _require_pandas():
    """Import pandas/pyarrow lazily with a clear dependency error."""
    try:
        import pandas as pd

        importlib.import_module("pyarrow")
    except ModuleNotFoundError as exc:
        raise ModuleNotFoundError(
            "pandas and pyarrow are required to assemble Layer 1 feature histories."
        ) from exc
    return pd


def _require_columns(frame, columns: Sequence[str], *, branch_name: str) -> None:
    """Validate that a DataFrame contains each required branch-artifact column."""
    missing = [column for column in columns if column not in frame.columns]
    if missing:
        raise ValueError(f"{branch_name} artifact missing required columns: {missing}")


def _optional_numeric(value: object) -> float | None:
    """Normalize optional numeric artifact values, converting NaN to None."""
    if value is None:
        return None
    numeric = float(value)
    if numeric != numeric:
        return None
    return numeric


def _optional_string(value: object) -> str | None:
    """Normalize optional string artifact values."""
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    return text


def _validated_iso_date(value: str, *, label: str) -> str:
    """Validate a canonical YYYY-MM-DD string and return it unchanged."""
    try:
        parsed = datetime.strptime(value, "%Y-%m-%d")
    except ValueError as exc:
        raise ValueError(f"{label} must be YYYY-MM-DD: {value}") from exc
    return parsed.date().isoformat()


def _write_manifest(
    writer: ObjectStore,
    *,
    run_id: str,
    status: RunStatus,
    started_at: datetime,
    finished_at: datetime,
    metadata: dict,
) -> str:
    """Persist a pipeline manifest entry for the backfill run."""
    manifest = PipelineManifestRecord(
        run_id=run_id,
        stage=LAYER1_BACKFILL_STAGE,
        status=status,
        started_at=started_at,
        finished_at=finished_at,
        metadata=metadata,
    )
    key = pipeline_manifest_path(LAYER1_BACKFILL_STAGE, run_id)
    payload = manifest.model_dump_json(indent=2).encode("utf-8")
    writer.put_object(key, payload)
    return key


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """Parse CLI arguments for the Layer 1 backfill runner."""
    parser = argparse.ArgumentParser(description="Run the Layer 1 production backfill.")
    parser.add_argument("--run-id", required=True, help="Run identifier for the backfill batch.")
    parser.add_argument(
        "--tickers",
        required=True,
        help="Comma-separated tickers, or @path/to/tickers.json for a JSON array.",
    )
    parser.add_argument(
        "--benchmark-ticker",
        default="SPY",
        help="Benchmark ticker used for cross-asset features (default: SPY).",
    )
    parser.add_argument(
        "--require-sentiment-features",
        action="store_true",
        help="Fail closed when no completed FinBERT sentiment artifacts are available.",
    )
    parser.add_argument(
        "--require-topic-features",
        action="store_true",
        help="Fail closed when no completed topic-feature artifacts are available.",
    )
    parser.add_argument(
        "--require-regime-features",
        action="store_true",
        help="Fail closed when no completed regime artifacts are available.",
    )
    return parser.parse_args(argv)


def _resolve_tickers(value: str) -> tuple[str, ...]:
    """Resolve the --tickers argument either inline or from a JSON file."""
    if value.startswith("@"):
        with Path(value[1:]).open("r", encoding="utf-8") as fp:
            payload = json.load(fp)
        if not isinstance(payload, list):
            raise ValueError("Ticker JSON file must contain an array of strings")
        return _validate_tickers(payload)
    return _validate_tickers([token.strip() for token in value.split(",") if token.strip()])


def _validate_tickers(values: Iterable[object]) -> tuple[str, ...]:
    """Coerce an iterable to a tuple of non-empty ticker strings."""
    cleaned: list[str] = []
    for value in values:
        if not isinstance(value, str):
            raise ValueError("ticker entries must be strings")
        stripped = value.strip().upper()
        if not stripped:
            raise ValueError("ticker entries cannot be empty")
        cleaned.append(stripped)
    return tuple(cleaned)


def load_modal_runtime_config(path: Path = MODAL_CONFIG_PATH) -> ModalRuntimeConfig:
    """Load Modal app and image settings from repository config."""
    payload = json.loads(path.read_text(encoding="utf-8"))
    return ModalRuntimeConfig(
        app_name=str(payload["layer1_backfill_app_name"]),
        r2_secret_name=str(payload["r2_secret_name"]),
        timeout_seconds=int(payload["layer1_backfill_timeout_seconds"]),
        python_version=str(payload.get("python_version", "3.11")),
        requirements_path=str(payload.get("requirements_path", "requirements/modal.txt")),
    )


def main(argv: Sequence[str] | None = None) -> int:
    """CLI entry point for `python -m app.lab.data_pipelines.backfill_layer1`."""
    args = _parse_args(argv)
    tickers = _resolve_tickers(args.tickers)
    config = Layer1BackfillConfig(
        run_id=args.run_id.strip(),
        tickers=tickers,
        benchmark_ticker=args.benchmark_ticker.strip().upper(),
        require_sentiment_features=bool(args.require_sentiment_features),
        require_topic_features=bool(args.require_topic_features),
        require_regime_features=bool(args.require_regime_features),
    )
    backfill_layer1(config)
    return 0


def modal_main(run_id: str, tickers: str, benchmark_ticker: str = "SPY") -> None:
    """Submit a Layer 1 backfill run to Modal from the local CLI."""
    globals()["modal_run_backfill_layer1"].remote(
        run_id=run_id,
        tickers=list(_resolve_tickers(tickers)),
        benchmark_ticker=benchmark_ticker.strip().upper(),
    )


def _define_modal_app() -> object | None:
    """Create the Modal app when the modal package is installed."""
    try:
        modal = importlib.import_module("modal")
    except ModuleNotFoundError:
        return None

    runtime = load_modal_runtime_config()
    image = modal.Image.debian_slim(
        python_version=runtime.python_version
    ).pip_install_from_requirements(runtime.requirements_path)
    app = modal.App(runtime.app_name)
    secrets = build_modal_secrets(
        modal,
        named_secret_names=(runtime.r2_secret_name,),
        env_file=SIMFIN_MODAL_ENV_FILE,
        env_keys=SIMFIN_MODAL_ENV_KEYS,
    )

    @app.function(
        image=image,
        secrets=secrets,
        timeout=runtime.timeout_seconds,
        serialized=True,
    )
    def modal_run_backfill_layer1(
        run_id: str,
        tickers: list[str],
        benchmark_ticker: str = "SPY",
    ) -> dict[str, object]:
        """Run the Layer 1 feature backfill on Modal."""
        result = backfill_layer1(
            Layer1BackfillConfig(
                run_id=run_id,
                tickers=tuple(tickers),
                benchmark_ticker=benchmark_ticker.strip().upper(),
            )
        )
        return {
            "run_id": result.run_id,
            "tickers_processed": result.tickers_processed,
            "ticker_files_written": result.ticker_files_written,
            "feature_rows_written": result.feature_rows_written,
            "manifest_key": result.manifest_key,
        }

    app.local_entrypoint()(modal_main)
    globals()["modal_run_backfill_layer1"] = modal_run_backfill_layer1
    return app


app = _define_modal_app()


if __name__ == "__main__":
    raise SystemExit(main())
