"""Layer 1 archive validator.

Mirrors the Layer 0 validator: confirms that every ticker in the declared
universe has a per-ticker feature history at `features/layer1/{ticker}.parquet`,
then checks that each file contains the expected universe dates. It emits a
JSON report under
`artifacts/reports/integration/layer1_archive_validation_{run_id}_{from}_to_{to}.json`.
Daily Layer 1 orchestration also uploads the rendered JSON to the durable R2 key
`artifacts/reports/integration/layer1_archive_validation_{run_id}_{from}_to_{to}.json`.

The validator does not re-run feature computation. It checks history-file
presence, row coverage, basic schema integrity, and related manifest state so
operators can see which run is authoritative and which sibling manifests are
stale. `ready_for_layer2` flips to True iff every expected ticker history is
present, the present histories round-trip through the FeatureRecord contract
with the expected dates, and any requested manifest check passes.
"""
from __future__ import annotations

import argparse
import csv
import io
import json
import math
import os
import random
import re
import sys
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass, field
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

from core.contracts.schemas import (  # noqa: E402
    FeatureRecord,
    PipelineManifestRecord,
    RunStatus,
)
from core.features.io import parquet_bytes_to_feature_records  # noqa: E402
from core.features.regime_detection import (  # noqa: E402
    HMM_REGIME_COLUMNS,
    REGIME_LABELS,
    REGIME_PROBABILITY_COLUMNS,
    REGIME_PROBABILITY_SUM_TOLERANCE,
)
from services.r2.paths import (  # noqa: E402
    layer1_feature_path,
    layer1_news_preprocessing_path,
    layer1_regime_path,
    layer1_sentiment_feature_path,
    layer1_sentiment_score_path,
    layer1_text_embedding_path,
    layer1_ticker_history_path,
    layer1_topic_feature_path,
    layer1_topic_label_path,
    layer1_validation_report_path,
    pipeline_manifest_path,
    raw_universe_path,
)

DEFAULT_REPORT_DIR = Path("artifacts/reports/integration")
LAYER1_MANIFEST_PREFIX = "artifacts/manifests/layer1/"
_RUN_ID_VERSION_RE = re.compile(r"^(?P<family>.+)-v(?P<version>\d+)$")
_LAYER1_CANONICAL_HISTORY_KEY_RE = re.compile(
    r"^features/layer1/(?P<ticker>[^/]+)\.parquet$"
)
_LAYER1_DATED_SHARD_KEY_RE = re.compile(
    r"^features/layer1/(?P<date>\d{4}-\d{2}-\d{2})/(?P<ticker>[^/]+)\.parquet$"
)
_LAYER1_INTERMEDIATE_PREFIXES = (
    "news_sentiment",
    "text_embeddings",
    "topic_labels",
    "topic_features",
    "news_sentiment_scored",
    "sentiment_features",
)


class ArchiveReader(Protocol):
    """Object-store operations required by the Layer 1 validator."""

    def exists(self, key: str) -> bool:
        """Return True if the key exists in the archive."""

    def get_object(self, key: str) -> bytes:
        """Return the bytes stored at `key`."""

    def list_keys(self, prefix: str) -> list[str]:
        """List keys beneath `prefix`."""


@dataclass(frozen=True)
class Layer1ValidationReport:
    """Summary of one Layer 1 archive validation pass."""

    run_id: str
    from_date: str
    to_date: str
    validation_status: str
    expected_ticker_files: int
    present_ticker_files: int
    expected_rows: int
    present_rows: int
    schema_failures: int
    row_count_failures: int
    manifest_key: str | None = None
    report_key: str | None = None
    manifest_status: str | None = None
    manifest_finished_at: str | None = None
    manifest_errors: list[str] = field(default_factory=list)
    related_manifests: list[dict[str, object]] = field(default_factory=list)
    stale_manifest_keys: list[str] = field(default_factory=list)
    missing_ticker_files: list[str] = field(default_factory=list)
    schema_failure_keys: list[str] = field(default_factory=list)
    row_count_failure_keys: list[str] = field(default_factory=list)
    requested_dates: list[str] = field(default_factory=list)
    universe_counts_by_date: dict[str, int] = field(default_factory=dict)
    present_ticker_counts_by_date: dict[str, int] = field(default_factory=dict)
    present_rows_by_ticker: dict[str, int] = field(default_factory=dict)
    missing_tickers_by_date: dict[str, list[str]] = field(default_factory=dict)
    missing_ticker_dates: dict[str, list[str]] = field(default_factory=dict)
    unexpected_tickers_by_date: dict[str, list[str]] = field(default_factory=dict)
    unexpected_ticker_dates: dict[str, list[str]] = field(default_factory=dict)
    duplicate_tickers_by_date: dict[str, list[str]] = field(default_factory=dict)
    duplicate_ticker_dates: dict[str, list[str]] = field(default_factory=dict)
    foreign_ticker_rows: dict[str, list[str]] = field(default_factory=dict)
    skipped_tickers: list[dict[str, object]] = field(default_factory=list)
    skipped_dates: list[dict[str, object]] = field(default_factory=list)
    archive_layout_failures: list[str] = field(default_factory=list)
    archive_layout_warnings: list[str] = field(default_factory=list)
    canonical_history_key_count: int = 0
    canonical_history_ticker_count: int = 0
    listed_expected_history_ticker_count: int = 0
    canonical_history_sample_keys: list[str] = field(default_factory=list)
    missing_listed_expected_tickers: list[str] = field(default_factory=list)
    dated_shard_key_count: int = 0
    dated_shard_unique_date_count: int = 0
    dated_shard_unique_ticker_count: int = 0
    dated_shard_date_samples: list[str] = field(default_factory=list)
    dated_shard_ticker_samples: list[str] = field(default_factory=list)
    dated_shard_counts_by_date: dict[str, int] = field(default_factory=dict)
    dated_shard_counts_by_ticker: dict[str, int] = field(default_factory=dict)
    dated_shard_missing_tickers_by_date: dict[str, list[str]] = field(default_factory=dict)
    dated_shard_non_aapl_examples: list[str] = field(default_factory=list)
    intermediate_prefix_counts: dict[str, int] = field(default_factory=dict)
    output_prefixes: dict[str, str] = field(default_factory=dict)
    leakage_spot_checks: list[dict[str, object]] = field(default_factory=list)
    regime_validation_status: str = "not_checked"
    layer2_regime_required_dates: list[str] = field(default_factory=list)
    layer2_regime_optional_dates: list[str] = field(default_factory=list)
    regime_diagnostics_by_date: dict[str, dict[str, object]] = field(default_factory=dict)
    regime_feature_coverage_by_date: dict[str, dict[str, object]] = field(default_factory=dict)
    regime_window_summary: dict[str, object] = field(default_factory=dict)
    regime_failures: list[dict[str, object]] = field(default_factory=list)
    regime_warnings: list[dict[str, object]] = field(default_factory=list)
    ready_for_layer2: bool = False

    def to_dict(self) -> dict:
        """Return a JSON-serializable dict of the validation summary."""
        return asdict(self)


def validate_layer1_archive(
    *,
    run_id: str,
    from_date: str,
    to_date: str,
    universe: Mapping[str, Sequence[str]],
    reader: ArchiveReader,
    output_prefixes: Mapping[str, str] | None = None,
    require_completed_manifest: bool = False,
    inspect_related_manifests: bool = False,
) -> Layer1ValidationReport:
    """Validate that every ticker in `universe` has a complete feature history.

    Args:
        run_id: Identifier for this validation pass (mirrors Layer 0 runs).
        from_date: Inclusive YYYY-MM-DD lower bound for the report metadata.
        to_date: Inclusive YYYY-MM-DD upper bound for the report metadata.
        universe: Mapping of `date` (YYYY-MM-DD) to the list of tickers expected
            to have a Layer 1 shard on that date.
        reader: Object store providing `exists`/`get_object`/`list_keys`.
        require_completed_manifest: Fail closed if the exact Layer 1 manifest is
            absent or not completed.
        inspect_related_manifests: Include exact/sibling manifest state in the
            report without requiring a completed exact manifest.

    Returns:
        Layer1ValidationReport.
    """
    _validate_iso_date(from_date, "from_date")
    _validate_iso_date(to_date, "to_date")

    manifest_key = pipeline_manifest_path("layer1", run_id)
    report_key = layer1_validation_report_path(run_id, from_date, to_date)
    expected_dates_by_ticker = _expected_dates_by_ticker(universe)
    requested_dates = _date_range_strings(from_date, to_date)
    universe_counts_by_date = {
        as_of_date: len({_normalize_ticker(ticker) for ticker in tickers})
        for as_of_date, tickers in sorted(universe.items())
    }
    missing: list[str] = []
    schema_failures: list[str] = []
    row_count_failures: list[str] = []
    present_files = 0
    present_rows = 0
    present_rows_by_ticker: dict[str, int] = {}
    missing_ticker_dates: dict[str, list[str]] = {}
    unexpected_ticker_dates: dict[str, list[str]] = {}
    duplicate_ticker_dates: dict[str, list[str]] = {}
    foreign_ticker_rows: dict[str, list[str]] = {}
    skipped_tickers: list[dict[str, object]] = []
    skipped_dates: list[dict[str, object]] = []
    feature_rows_by_ticker_date: dict[tuple[str, str], FeatureRecord] = {}
    actual_dates_by_ticker: dict[str, set[str]] = {}

    for ticker, expected_dates in sorted(expected_dates_by_ticker.items()):
        key = layer1_ticker_history_path(ticker)
        if not reader.exists(key):
            missing.append(key)
            expected_window_dates = sorted(expected_dates)
            missing_ticker_dates[ticker] = expected_window_dates
            skipped_tickers.append(
                {
                    "ticker": ticker,
                    "reason": "missing_history_file",
                    "history_key": key,
                    "expected_dates": expected_window_dates,
                }
            )
            for date_text in expected_window_dates:
                skipped_dates.append(
                    {
                        "ticker": ticker,
                        "date": date_text,
                        "reason": "missing_history_file",
                    }
                )
            continue
        present_files += 1
        try:
            records = parquet_bytes_to_feature_records(reader.get_object(key))
        except Exception as exc:  # noqa: BLE001 — record any decode failure
            logger.warning("Layer 1 ticker history {} failed schema check: {}", key, exc)
            schema_failures.append(key)
            skipped_tickers.append(
                {
                    "ticker": ticker,
                    "reason": "schema_validation_failed",
                    "history_key": key,
                }
            )
            continue

        ticker_dates: list[str] = []
        window_dates: list[str] = []
        expected_date_counts: dict[str, int] = {}
        foreign_rows: list[str] = []
        for record in records:
            if record.ticker != ticker:
                foreign_rows.append(f"{record.date}/{record.ticker}")
                continue
            if record.date in requested_dates:
                window_dates.append(record.date)
            if record.date in expected_dates:
                ticker_dates.append(record.date)
                expected_date_counts[record.date] = expected_date_counts.get(record.date, 0) + 1
                feature_rows_by_ticker_date.setdefault((ticker, record.date), record)

        actual_dates = set(ticker_dates)
        actual_dates_by_ticker[ticker] = actual_dates
        missing_dates = sorted(expected_dates - actual_dates)
        unexpected_dates = sorted(set(window_dates) - expected_dates)
        duplicate_dates = sorted(
            date_text for date_text, count in expected_date_counts.items() if count > 1
        )
        present_rows_by_ticker[ticker] = len(ticker_dates)
        present_rows += len(ticker_dates)
        if missing_dates:
            missing_ticker_dates[ticker] = missing_dates
        if unexpected_dates:
            unexpected_ticker_dates[ticker] = unexpected_dates
        if duplicate_dates:
            duplicate_ticker_dates[ticker] = duplicate_dates
        if foreign_rows:
            foreign_ticker_rows[ticker] = sorted(foreign_rows)

        if (
            bool(foreign_rows)
            or bool(missing_dates)
            or bool(unexpected_dates)
            or bool(duplicate_dates)
        ):
            logger.warning(
                "Layer 1 ticker history {} window coverage mismatch expected={} actual={} "
                "missing={} unexpected={} duplicates={} foreign={}",
                key,
                len(expected_dates),
                len(actual_dates),
                len(missing_dates),
                len(unexpected_dates),
                len(duplicate_dates),
                bool(foreign_rows),
            )
            row_count_failures.append(key)
            for date_text in missing_dates:
                skipped_dates.append(
                    {
                        "ticker": ticker,
                        "date": date_text,
                        "reason": "missing_window_row",
                    }
                )

    expected_files = len(expected_dates_by_ticker)
    expected_rows = sum(len(dates) for dates in expected_dates_by_ticker.values())
    ready = expected_rows > 0 and not missing and not schema_failures and not row_count_failures
    (
        present_ticker_counts_by_date,
        missing_tickers_by_date,
        unexpected_tickers_by_date,
        duplicate_tickers_by_date,
    ) = _summarize_date_level_coverage(
        universe=universe,
        actual_dates_by_ticker=actual_dates_by_ticker,
        unexpected_ticker_dates=unexpected_ticker_dates,
        duplicate_ticker_dates=duplicate_ticker_dates,
    )
    archive_layout = _inspect_archive_layout(
        reader=reader,
        expected_dates_by_ticker=expected_dates_by_ticker,
        universe=universe,
        requested_dates=requested_dates,
    )
    if archive_layout.failures:
        ready = False
    leakage_spot_checks = _build_leakage_spot_checks(
        reader=reader,
        expected_dates_by_ticker=expected_dates_by_ticker,
        run_id=run_id,
    )
    if any(check["status"] == "fail" for check in leakage_spot_checks):
        ready = False
    (
        regime_status,
        layer2_regime_required_dates,
        layer2_regime_optional_dates,
        regime_diagnostics_by_date,
        regime_feature_coverage_by_date,
        regime_window_summary,
        regime_failures,
        regime_warnings,
    ) = _validate_regime_handoff(
        run_id=run_id,
        universe=universe,
        feature_rows_by_ticker_date=feature_rows_by_ticker_date,
        reader=reader,
    )
    if regime_failures or regime_warnings:
        ready = False
    manifest_state = _empty_manifest_inspection()
    if require_completed_manifest or inspect_related_manifests:
        manifest_state = _inspect_layer1_manifests(
            reader=reader,
            run_id=run_id,
            require_completed_manifest=require_completed_manifest,
        )
    if manifest_state.manifest_errors:
        ready = False
    has_leakage_failures = any(check["status"] == "fail" for check in leakage_spot_checks)
    only_regime_warnings = (
        not ready
        and not missing
        and not schema_failures
        and not row_count_failures
        and not manifest_state.manifest_errors
        and not has_leakage_failures
        and bool(regime_warnings)
        and not regime_failures
    )
    validation_status = "completed" if ready else ("warning" if only_regime_warnings else "failed")
    return Layer1ValidationReport(
        run_id=run_id,
        from_date=from_date,
        to_date=to_date,
        validation_status=validation_status,
        expected_ticker_files=expected_files,
        present_ticker_files=present_files,
        expected_rows=expected_rows,
        present_rows=present_rows,
        schema_failures=len(schema_failures),
        row_count_failures=len(row_count_failures),
        manifest_key=manifest_key,
        report_key=report_key,
        manifest_status=manifest_state.manifest_status,
        manifest_finished_at=manifest_state.manifest_finished_at,
        manifest_errors=manifest_state.manifest_errors,
        related_manifests=manifest_state.related_manifests,
        stale_manifest_keys=manifest_state.stale_manifest_keys,
        missing_ticker_files=missing,
        schema_failure_keys=schema_failures,
        row_count_failure_keys=row_count_failures,
        requested_dates=requested_dates,
        universe_counts_by_date=universe_counts_by_date,
        present_ticker_counts_by_date=present_ticker_counts_by_date,
        present_rows_by_ticker=present_rows_by_ticker,
        missing_tickers_by_date=missing_tickers_by_date,
        missing_ticker_dates=missing_ticker_dates,
        unexpected_tickers_by_date=unexpected_tickers_by_date,
        unexpected_ticker_dates=unexpected_ticker_dates,
        duplicate_tickers_by_date=duplicate_tickers_by_date,
        duplicate_ticker_dates=duplicate_ticker_dates,
        foreign_ticker_rows=foreign_ticker_rows,
        skipped_tickers=skipped_tickers,
        skipped_dates=skipped_dates,
        archive_layout_failures=archive_layout.failures,
        archive_layout_warnings=archive_layout.warnings,
        canonical_history_key_count=archive_layout.canonical_history_key_count,
        canonical_history_ticker_count=archive_layout.canonical_history_ticker_count,
        listed_expected_history_ticker_count=archive_layout.listed_expected_history_ticker_count,
        canonical_history_sample_keys=archive_layout.canonical_history_sample_keys,
        missing_listed_expected_tickers=archive_layout.missing_listed_expected_tickers,
        dated_shard_key_count=archive_layout.dated_shard_key_count,
        dated_shard_unique_date_count=archive_layout.dated_shard_unique_date_count,
        dated_shard_unique_ticker_count=archive_layout.dated_shard_unique_ticker_count,
        dated_shard_date_samples=archive_layout.dated_shard_date_samples,
        dated_shard_ticker_samples=archive_layout.dated_shard_ticker_samples,
        dated_shard_counts_by_date=archive_layout.dated_shard_counts_by_date,
        dated_shard_counts_by_ticker=archive_layout.dated_shard_counts_by_ticker,
        dated_shard_missing_tickers_by_date=archive_layout.dated_shard_missing_tickers_by_date,
        dated_shard_non_aapl_examples=archive_layout.dated_shard_non_aapl_examples,
        intermediate_prefix_counts=archive_layout.intermediate_prefix_counts,
        output_prefixes=dict(output_prefixes or {}),
        leakage_spot_checks=leakage_spot_checks,
        regime_validation_status=regime_status,
        layer2_regime_required_dates=layer2_regime_required_dates,
        layer2_regime_optional_dates=layer2_regime_optional_dates,
        regime_diagnostics_by_date=regime_diagnostics_by_date,
        regime_feature_coverage_by_date=regime_feature_coverage_by_date,
        regime_window_summary=regime_window_summary,
        regime_failures=regime_failures,
        regime_warnings=regime_warnings,
        ready_for_layer2=ready,
    )


@dataclass(frozen=True)
class ManifestInspectionResult:
    """Manifest-state summary for one validated Layer 1 run family."""

    manifest_status: str | None
    manifest_finished_at: str | None
    manifest_errors: list[str]
    related_manifests: list[dict[str, object]]
    stale_manifest_keys: list[str]


@dataclass(frozen=True)
class ArchiveLayoutInspectionResult:
    """Archive-layout summary used to explain canonical versus intermediate outputs."""

    failures: list[str]
    warnings: list[str]
    canonical_history_key_count: int
    canonical_history_ticker_count: int
    listed_expected_history_ticker_count: int
    canonical_history_sample_keys: list[str]
    missing_listed_expected_tickers: list[str]
    dated_shard_key_count: int
    dated_shard_unique_date_count: int
    dated_shard_unique_ticker_count: int
    dated_shard_date_samples: list[str]
    dated_shard_ticker_samples: list[str]
    dated_shard_counts_by_date: dict[str, int]
    dated_shard_counts_by_ticker: dict[str, int]
    dated_shard_missing_tickers_by_date: dict[str, list[str]]
    dated_shard_non_aapl_examples: list[str]
    intermediate_prefix_counts: dict[str, int]


def _empty_manifest_inspection() -> ManifestInspectionResult:
    """Return the default manifest summary for validations that skip inspection."""
    return ManifestInspectionResult(
        manifest_status=None,
        manifest_finished_at=None,
        manifest_errors=[],
        related_manifests=[],
        stale_manifest_keys=[],
    )


def build_layer1_output_prefixes(processed_dates: Sequence[str]) -> dict[str, str]:
    """Return deterministic R2 prefixes relevant to one Layer 1 readiness report."""
    latest_date = processed_dates[-1] if processed_dates else ""
    prefix_date = latest_date or "2000-01-01"
    prefixes = {
        "layer1_history": _prefix_for_key(layer1_ticker_history_path("<TICKER>")),
        # Keep the more explicit names as transition aliases for report consumers that now
        # distinguish canonical per-ticker histories from dated shards.
        "layer1_canonical_history": _prefix_for_key(layer1_ticker_history_path("<TICKER>")),
        "layer1_daily_shards": _prefix_for_key(layer1_feature_path(prefix_date, "<TICKER>")),
        "layer1_dated_shards": _prefix_for_key(layer1_feature_path(prefix_date, "<TICKER>")),
        "news_sentiment": _prefix_for_key(
            layer1_news_preprocessing_path(prefix_date, "<RUN_ID>")
        ),
        "text_embeddings": _prefix_for_key(layer1_text_embedding_path(prefix_date, "<RUN_ID>")),
        "topic_labels": _prefix_for_key(layer1_topic_label_path(prefix_date, "<RUN_ID>")),
        "topic_features": _prefix_for_key(layer1_topic_feature_path(prefix_date, "<RUN_ID>")),
        "sentiment_scores": _prefix_for_key(
            layer1_sentiment_score_path(prefix_date, "<RUN_ID>")
        ),
        "sentiment_features": _prefix_for_key(
            layer1_sentiment_feature_path(prefix_date, "<RUN_ID>")
        ),
        "regime_outputs": _prefix_for_key(layer1_regime_path("<RUN_ID>")),
        "regime_manifests": _prefix_for_key(
            pipeline_manifest_path("layer1_5_regime", "<RUN_ID>")
        ),
        "layer1_manifests": _prefix_for_key(pipeline_manifest_path("layer1", "<RUN_ID>")),
        "validation_reports": _prefix_for_key(
            layer1_validation_report_path("<RUN_ID>", prefix_date, prefix_date)
        ),
    }
    if latest_date:
        prefixes["latest_processed_date"] = latest_date
    return prefixes


def write_validation_report(
    report: Layer1ValidationReport,
    output_dir: Path | None = None,
) -> Path:
    """Persist the validation report as deterministic JSON and return its path."""
    target_dir = output_dir if output_dir is not None else DEFAULT_REPORT_DIR
    target_dir.mkdir(parents=True, exist_ok=True)
    filename = validation_report_filename(report)
    path = target_dir / filename
    path.write_text(render_validation_report(report), encoding="utf-8")
    return path


def validation_report_filename(report: Layer1ValidationReport) -> str:
    """Return the deterministic local filename for a Layer 1 validation report."""
    return (
        f"layer1_archive_validation_{report.run_id}_{report.from_date}"
        f"_to_{report.to_date}.json"
    )


def render_validation_report(report: Layer1ValidationReport) -> str:
    """Render one validation report to deterministic JSON text."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def load_universe_mapping(path: Path) -> dict[str, list[str]]:
    """Load a `{date: [tickers...]}` JSON mapping for validation."""
    with path.open("r", encoding="utf-8") as fp:
        data = json.load(fp)
    if not isinstance(data, dict):
        raise ValueError("Universe JSON must be an object mapping date -> [tickers]")
    universe: dict[str, list[str]] = {}
    for as_of_date, tickers in data.items():
        if not isinstance(as_of_date, str):
            raise ValueError("Universe JSON keys must be YYYY-MM-DD strings")
        if not isinstance(tickers, list):
            raise ValueError(f"Universe JSON value for {as_of_date} must be a list")
        cleaned: list[str] = []
        for ticker in tickers:
            if not isinstance(ticker, str) or not ticker.strip():
                raise ValueError("Universe JSON ticker entries must be non-empty strings")
            cleaned.append(ticker.strip().upper())
        universe[as_of_date] = cleaned
    return universe


def load_universe_mapping_from_r2(
    *,
    from_date: str,
    to_date: str,
    reader: ArchiveReader,
    requested_tickers: Sequence[str] | None = None,
) -> dict[str, list[str]]:
    """Load the eligible Layer 1 universe directly from Layer 0 R2 universe masks."""
    allowed_tickers = (
        {_normalize_ticker(ticker) for ticker in requested_tickers}
        if requested_tickers is not None
        else None
    )
    universe: dict[str, list[str]] = {}
    for date_text in _date_range_strings(from_date, to_date):
        payload = reader.get_object(raw_universe_path(date_text)).decode("utf-8")
        date_tickers: list[str] = []
        for row in csv.DictReader(io.StringIO(payload)):
            ticker = _normalize_csv_ticker(row.get("ticker"))
            if ticker is None:
                continue
            if allowed_tickers is not None and ticker not in allowed_tickers:
                continue
            if (
                _truthy(row.get("in_universe"))
                and _truthy(row.get("tradable"), default=True)
                and _truthy(row.get("liquid"), default=True)
                and _truthy(row.get("data_quality_ok"), default=True)
                and not _truthy(row.get("halted"))
            ):
                date_tickers.append(ticker)
        universe[date_text] = sorted(set(date_tickers))
    return universe


def _summarize_date_level_coverage(
    *,
    universe: Mapping[str, Sequence[str]],
    actual_dates_by_ticker: Mapping[str, set[str]],
    unexpected_ticker_dates: Mapping[str, list[str]],
    duplicate_ticker_dates: Mapping[str, list[str]],
) -> tuple[
    dict[str, int],
    dict[str, list[str]],
    dict[str, list[str]],
    dict[str, list[str]],
]:
    """Invert ticker-level validation details into operator-facing date summaries."""
    expected_tickers_by_date = {
        as_of_date: {_normalize_ticker(ticker) for ticker in tickers}
        for as_of_date, tickers in universe.items()
    }
    present_tickers_by_date: dict[str, set[str]] = {
        as_of_date: set() for as_of_date in expected_tickers_by_date
    }
    unexpected_tickers_by_date: dict[str, set[str]] = {}
    duplicate_tickers_by_date: dict[str, set[str]] = {}

    for ticker, dates in actual_dates_by_ticker.items():
        for date_text in sorted(dates):
            if date_text in present_tickers_by_date:
                present_tickers_by_date[date_text].add(ticker)
    for ticker, dates in unexpected_ticker_dates.items():
        for date_text in dates:
            unexpected_tickers_by_date.setdefault(date_text, set()).add(ticker)
    for ticker, dates in duplicate_ticker_dates.items():
        for date_text in dates:
            duplicate_tickers_by_date.setdefault(date_text, set()).add(ticker)

    present_ticker_counts_by_date = {
        as_of_date: len(present_tickers_by_date.get(as_of_date, set()))
        for as_of_date in sorted(expected_tickers_by_date)
    }
    missing_tickers_by_date = {
        as_of_date: sorted(
            expected_tickers_by_date.get(as_of_date, set())
            - present_tickers_by_date.get(as_of_date, set())
        )
        for as_of_date in sorted(expected_tickers_by_date)
        if expected_tickers_by_date.get(as_of_date, set())
        - present_tickers_by_date.get(as_of_date, set())
    }
    return (
        present_ticker_counts_by_date,
        missing_tickers_by_date,
        {
            as_of_date: sorted(tickers)
            for as_of_date, tickers in sorted(unexpected_tickers_by_date.items())
        },
        {
            as_of_date: sorted(tickers)
            for as_of_date, tickers in sorted(duplicate_tickers_by_date.items())
        },
    )


def _expected_dates_by_ticker(
    universe: Mapping[str, Sequence[str]],
) -> dict[str, set[str]]:
    """Invert a date->tickers universe mapping into ticker->expected dates."""
    expected: dict[str, set[str]] = {}
    for as_of_date, tickers in sorted(universe.items()):
        _validate_iso_date(as_of_date, "universe date")
        for ticker in tickers:
            if not isinstance(ticker, str):
                raise ValueError("universe ticker entries must be strings")
            normalized_ticker = ticker.strip().upper()
            if not normalized_ticker:
                raise ValueError("universe ticker entries must be non-empty strings")
            expected.setdefault(normalized_ticker, set()).add(as_of_date)
    return expected


def _date_range_strings(from_date: str, to_date: str) -> list[str]:
    """Return every date between the inclusive ISO bounds."""
    start = Date.fromisoformat(from_date)
    end = Date.fromisoformat(to_date)
    if start > end:
        raise ValueError("from_date must be <= to_date")
    current = start
    dates: list[str] = []
    while current <= end:
        dates.append(current.isoformat())
        current = current.fromordinal(current.toordinal() + 1)
    return dates


def _normalize_ticker(ticker: str) -> str:
    """Normalize one ticker symbol for report aggregation."""
    normalized = ticker.strip().upper()
    if not normalized:
        raise ValueError("ticker entries must be non-empty strings")
    return normalized


def _inspect_archive_layout(
    *,
    reader: ArchiveReader,
    expected_dates_by_ticker: Mapping[str, set[str]],
    universe: Mapping[str, Sequence[str]],
    requested_dates: Sequence[str],
) -> ArchiveLayoutInspectionResult:
    """Inspect Layer 1 key layout so operators can distinguish canonical from legacy views."""
    failures: list[str] = []
    warnings: list[str] = []
    try:
        keys = reader.list_keys("features/layer1/")
    except Exception as exc:  # noqa: BLE001
        return ArchiveLayoutInspectionResult(
            failures=[f"archive_listing_failed:{exc}"],
            warnings=[],
            canonical_history_key_count=0,
            canonical_history_ticker_count=0,
            listed_expected_history_ticker_count=0,
            canonical_history_sample_keys=[],
            missing_listed_expected_tickers=sorted(expected_dates_by_ticker),
            dated_shard_key_count=0,
            dated_shard_unique_date_count=0,
            dated_shard_unique_ticker_count=0,
            dated_shard_date_samples=[],
            dated_shard_ticker_samples=[],
            dated_shard_counts_by_date={},
            dated_shard_counts_by_ticker={},
            dated_shard_missing_tickers_by_date={},
            dated_shard_non_aapl_examples=[],
            intermediate_prefix_counts={prefix: 0 for prefix in _LAYER1_INTERMEDIATE_PREFIXES},
        )

    expected_tickers = sorted(expected_dates_by_ticker)
    expected_ticker_set = set(expected_tickers)
    # The original operator report referenced an AAPL-only console view; keep AAPL as the
    # preferred contrast anchor when it is part of the requested universe, otherwise fall
    # back to the first expected ticker so the diagnostic still highlights "other ticker"
    # dated shards in narrower windows.
    dated_shard_example_anchor = "AAPL" if "AAPL" in expected_ticker_set else None
    if dated_shard_example_anchor is None and expected_tickers:
        dated_shard_example_anchor = expected_tickers[0]
    expected_tickers_by_date = {
        as_of_date: {_normalize_ticker(ticker) for ticker in tickers}
        for as_of_date, tickers in universe.items()
    }
    canonical_history_keys: list[str] = []
    canonical_history_tickers: set[str] = set()
    dated_shard_keys: list[str] = []
    dated_shard_dates: set[str] = set()
    dated_shard_tickers: set[str] = set()
    dated_shard_counts_by_date: dict[str, int] = {date_text: 0 for date_text in requested_dates}
    dated_shard_counts_by_ticker: dict[str, int] = {ticker: 0 for ticker in expected_tickers}
    dated_shard_present_tickers_by_date: dict[str, set[str]] = {
        date_text: set() for date_text in requested_dates
    }
    dated_shard_non_aapl_examples: list[str] = []
    intermediate_prefix_counts = {
        prefix: 0 for prefix in _LAYER1_INTERMEDIATE_PREFIXES
    }

    for key in keys:
        canonical_match = _LAYER1_CANONICAL_HISTORY_KEY_RE.fullmatch(key)
        if canonical_match is not None:
            canonical_history_keys.append(key)
            canonical_history_tickers.add(_normalize_ticker(canonical_match.group("ticker")))
            continue

        dated_match = _LAYER1_DATED_SHARD_KEY_RE.fullmatch(key)
        if dated_match is not None:
            dated_shard_keys.append(key)
            date_text = dated_match.group("date")
            ticker = _normalize_ticker(dated_match.group("ticker"))
            dated_shard_dates.add(date_text)
            dated_shard_tickers.add(ticker)
            if date_text in dated_shard_counts_by_date:
                dated_shard_counts_by_date[date_text] += 1
                if ticker in expected_tickers_by_date.get(date_text, set()):
                    dated_shard_present_tickers_by_date.setdefault(date_text, set()).add(ticker)
            if ticker in dated_shard_counts_by_ticker:
                dated_shard_counts_by_ticker[ticker] += 1
            if ticker != dated_shard_example_anchor and len(dated_shard_non_aapl_examples) < 20:
                dated_shard_non_aapl_examples.append(key)
            continue

        for prefix in _LAYER1_INTERMEDIATE_PREFIXES:
            if key.startswith(f"features/layer1/{prefix}/"):
                intermediate_prefix_counts[prefix] += 1
                break

    listed_expected_history_tickers = sorted(expected_ticker_set & canonical_history_tickers)
    missing_listed_expected_tickers = sorted(expected_ticker_set - canonical_history_tickers)
    if missing_listed_expected_tickers:
        failures.append("canonical_history_listing_incomplete")
    if len(listed_expected_history_tickers) == 1 and len(expected_tickers) > 1:
        failures.append("single_ticker_canonical_history_listing")

    dated_shard_missing_tickers_by_date = {
        date_text: sorted(
            expected_tickers_by_date.get(date_text, set())
            - dated_shard_present_tickers_by_date.get(date_text, set())
        )
        for date_text in requested_dates
        if expected_tickers_by_date.get(date_text, set())
        - dated_shard_present_tickers_by_date.get(date_text, set())
    }
    if dated_shard_missing_tickers_by_date:
        warnings.append("dated_shards_partial_non_authoritative")

    return ArchiveLayoutInspectionResult(
        failures=failures,
        warnings=warnings,
        canonical_history_key_count=len(canonical_history_keys),
        canonical_history_ticker_count=len(canonical_history_tickers),
        listed_expected_history_ticker_count=len(listed_expected_history_tickers),
        canonical_history_sample_keys=canonical_history_keys[:20],
        missing_listed_expected_tickers=missing_listed_expected_tickers,
        dated_shard_key_count=len(dated_shard_keys),
        dated_shard_unique_date_count=len(dated_shard_dates),
        dated_shard_unique_ticker_count=len(dated_shard_tickers),
        dated_shard_date_samples=_sample_sorted(dated_shard_dates),
        dated_shard_ticker_samples=_sample_sorted(dated_shard_tickers),
        dated_shard_counts_by_date={
            date_text: count
            for date_text, count in sorted(dated_shard_counts_by_date.items())
        },
        dated_shard_counts_by_ticker={
            ticker: count for ticker, count in sorted(dated_shard_counts_by_ticker.items())
        },
        dated_shard_missing_tickers_by_date=dated_shard_missing_tickers_by_date,
        dated_shard_non_aapl_examples=dated_shard_non_aapl_examples,
        intermediate_prefix_counts=intermediate_prefix_counts,
    )


def _sample_sorted(values: set[str], *, limit: int = 20) -> list[str]:
    """Return a deterministic bounded sample from a set of strings."""
    return sorted(values)[:limit]


def _build_leakage_spot_checks(
    *,
    reader: ArchiveReader,
    expected_dates_by_ticker: Mapping[str, set[str]],
    run_id: str,
    sample_limit: int = 10,
) -> list[dict[str, object]]:
    """Run lightweight alignment checks on a sample of daily Layer 1 shards."""
    candidate_pairs = [
        (date_text, ticker)
        for ticker, date_values in sorted(expected_dates_by_ticker.items())
        for date_text in sorted(date_values)
    ]
    if len(candidate_pairs) <= sample_limit:
        sampled_pairs = candidate_pairs
    else:
        sampled_pairs = sorted(random.Random(run_id).sample(candidate_pairs, k=sample_limit))

    failures: list[dict[str, object]] = []
    seen_daily_shard = False
    for date_text, ticker in sampled_pairs:
        key = layer1_feature_path(date_text, ticker)
        if not reader.exists(key):
            failures.append(
                {
                    "ticker": ticker,
                    "date": date_text,
                    "reason": "missing_daily_shard",
                    "key": key,
                }
            )
            continue
        seen_daily_shard = True
        try:
            records = parquet_bytes_to_feature_records(reader.get_object(key))
        except Exception as exc:  # noqa: BLE001
            failures.append(
                {
                    "ticker": ticker,
                    "date": date_text,
                    "reason": "daily_shard_decode_failed",
                    "key": key,
                    "error": str(exc),
                }
            )
            continue
        if len(records) != 1:
            failures.append(
                {
                    "ticker": ticker,
                    "date": date_text,
                    "reason": "daily_shard_row_count_mismatch",
                    "key": key,
                    "rows": len(records),
                }
            )
            continue
        record = records[0]
        if record.date != date_text or record.ticker != ticker:
            failures.append(
                {
                    "ticker": ticker,
                    "date": date_text,
                    "reason": "daily_shard_identity_mismatch",
                    "key": key,
                    "record_date": record.date,
                    "record_ticker": record.ticker,
                }
            )

    hard_failures = [
        failure for failure in failures if failure.get("reason") != "missing_daily_shard"
    ]
    if not seen_daily_shard:
        status = "skipped"
    elif hard_failures:
        status = "fail"
    elif failures:
        status = "warning"
    else:
        status = "pass"
    return [
        {
            "name": "daily_shard_identity_alignment",
            "sampled_pairs": [
                {"date": date_text, "ticker": ticker} for date_text, ticker in sampled_pairs
            ],
            "status": status,
            "failures": failures,
        }
    ]


def _validate_regime_handoff(
    *,
    run_id: str,
    universe: Mapping[str, Sequence[str]],
    feature_rows_by_ticker_date: Mapping[tuple[str, str], FeatureRecord],
    reader: ArchiveReader,
) -> tuple[
    str,
    list[str],
    list[str],
    dict[str, dict[str, object]],
    dict[str, dict[str, object]],
    dict[str, object],
    list[dict[str, object]],
    list[dict[str, object]],
]:
    """Validate Layer 1 regime completeness against Layer 1.5 diagnostics."""
    if not universe:
        return ("not_checked", [], [], {}, {}, _empty_regime_window_summary(), [], [])
    diagnostics_by_date: dict[str, dict[str, object]] = {}
    coverage_by_date: dict[str, dict[str, object]] = {}
    regime_failures: list[dict[str, object]] = []
    regime_warnings: list[dict[str, object]] = []
    required_dates: list[str] = []
    optional_dates: list[str] = []

    for date_text, tickers in sorted(universe.items()):
        normalized_tickers = sorted({_normalize_ticker(value) for value in tickers})
        diagnostic = _load_regime_diagnostic(run_id=run_id, date_text=date_text, reader=reader)
        diagnostics_by_date[date_text] = diagnostic
        if bool(diagnostic.get("required_for_layer2")):
            required_dates.append(date_text)
        else:
            optional_dates.append(date_text)
        label_distribution: dict[str, int] = {}
        sample_rows: list[dict[str, object]] = []
        coverage = {
            "expected_ticker_rows": len(normalized_tickers),
            "present_history_rows": 0,
            "full_feature_rows": 0,
            "explicit_null_rows": 0,
            "partial_feature_rows": 0,
            "rows_missing_feature_keys": 0,
            "invalid_value_rows": 0,
            "coverage_rate": None,
            "explicit_null_rate": None,
            "missing_or_partial_rate": None,
            "label_distribution": label_distribution,
            "confidence_min": None,
            "confidence_max": None,
            "probability_sum_min": None,
            "probability_sum_max": None,
            "required_for_layer2": bool(diagnostic.get("required_for_layer2")),
            "artifact_status": diagnostic.get("status"),
            "artifact_reason": diagnostic.get("reason"),
            "output_key": diagnostic.get("output_key"),
            "output_present": bool(diagnostic.get("output_present")),
            "manifest_key": diagnostic.get("manifest_key"),
            "manifest_present": bool(diagnostic.get("manifest_present")),
            "manifest_status": diagnostic.get("manifest_status"),
            "sample_rows": sample_rows,
        }
        if diagnostic["status"] == "failure":
            coverage_by_date[date_text] = _finalize_regime_coverage(coverage)
            regime_failures.append(diagnostic)
            continue

        explicit_null_tickers: list[str] = []
        for ticker in normalized_tickers:
            record = feature_rows_by_ticker_date.get((ticker, date_text))
            if record is None:
                continue
            coverage["present_history_rows"] = int(coverage["present_history_rows"]) + 1
            presence = _inspect_regime_feature_presence(record.features)
            label = _normalize_optional_value(record.features.get("regime_label"))
            confidence = _safe_float(record.features.get("regime_confidence"))
            probability_sum = _regime_probability_sum(record.features)
            if len(sample_rows) < 3:
                sample_rows.append(
                    {
                        "ticker": ticker,
                        "state": presence["state"],
                        "regime_label": label,
                        "regime_confidence": confidence,
                        "probability_sum": probability_sum,
                    }
                )
            if diagnostic["status"] == "ready":
                if presence["state"] != "full":
                    if presence["state"] == "missing":
                        coverage["rows_missing_feature_keys"] = (
                            int(coverage["rows_missing_feature_keys"]) + 1
                        )
                    elif presence["state"] == "partial":
                        coverage["partial_feature_rows"] = (
                            int(coverage["partial_feature_rows"]) + 1
                        )
                    else:
                        coverage["explicit_null_rows"] = (
                            int(coverage["explicit_null_rows"]) + 1
                        )
                    regime_failures.append(
                        {
                            "date": date_text,
                            "ticker": ticker,
                            "status": "failure",
                            "reason": "required_regime_fields_missing",
                            "required_for_layer2": True,
                            "missing_keys": presence["missing_keys"],
                        }
                    )
                    continue
                coverage["full_feature_rows"] = int(coverage["full_feature_rows"]) + 1
                value_errors = _regime_value_errors(
                    date_text=date_text,
                    ticker=ticker,
                    features=record.features,
                )
                if value_errors:
                    coverage["invalid_value_rows"] = int(coverage["invalid_value_rows"]) + 1
                    regime_failures.append(
                        {
                            "date": date_text,
                            "ticker": ticker,
                            "status": "failure",
                            "reason": "invalid_regime_values",
                            "required_for_layer2": True,
                            "errors": value_errors,
                        }
                    )
                else:
                    if isinstance(label, str):
                        normalized_label = label.strip().lower()
                        label_distribution[normalized_label] = (
                            label_distribution.get(normalized_label, 0) + 1
                        )
                    _update_regime_coverage_ranges(
                        coverage=coverage,
                        confidence=confidence,
                        probability_sum=probability_sum,
                    )
            else:
                if presence["missing_keys"]:
                    coverage["rows_missing_feature_keys"] = (
                        int(coverage["rows_missing_feature_keys"]) + 1
                    )
                    regime_failures.append(
                        {
                            "date": date_text,
                            "ticker": ticker,
                            "status": "failure",
                            "reason": "regime_placeholder_keys_missing",
                            "required_for_layer2": False,
                            "missing_keys": presence["missing_keys"],
                        }
                    )
                    continue
                if presence["state"] == "partial":
                    coverage["partial_feature_rows"] = int(coverage["partial_feature_rows"]) + 1
                    regime_failures.append(
                        {
                            "date": date_text,
                            "ticker": ticker,
                            "status": "failure",
                            "reason": "regime_warning_rows_must_use_explicit_nulls",
                            "required_for_layer2": False,
                        }
                    )
                    continue
                if presence["state"] == "full":
                    coverage["full_feature_rows"] = int(coverage["full_feature_rows"]) + 1
                    coverage["invalid_value_rows"] = int(coverage["invalid_value_rows"]) + 1
                    regime_failures.append(
                        {
                            "date": date_text,
                            "ticker": ticker,
                            "status": "failure",
                            "reason": "regime_row_conflicts_with_warning_diagnostic",
                            "required_for_layer2": False,
                        }
                    )
                    continue
                coverage["explicit_null_rows"] = int(coverage["explicit_null_rows"]) + 1
                explicit_null_tickers.append(ticker)
        if diagnostic["status"] == "warning" and explicit_null_tickers:
            regime_warnings.append(
                {
                    "date": date_text,
                    "status": "warning",
                    "reason": diagnostic.get("reason"),
                    "required_for_layer2": False,
                    "ticker_count": len(explicit_null_tickers),
                    "tickers": explicit_null_tickers[:20],
                    "complete_training_rows": diagnostic.get("complete_training_rows"),
                    "min_training_rows": diagnostic.get("min_training_rows"),
                    "missing_features": diagnostic.get("missing_features"),
                }
            )
        coverage_by_date[date_text] = _finalize_regime_coverage(coverage)

    status = "completed"
    if regime_failures:
        status = "failed"
    elif regime_warnings:
        status = "warning"
    regime_window_summary = _summarize_regime_window(
        required_dates=required_dates,
        optional_dates=optional_dates,
        diagnostics_by_date=diagnostics_by_date,
        coverage_by_date=coverage_by_date,
    )
    return (
        status,
        required_dates,
        optional_dates,
        diagnostics_by_date,
        coverage_by_date,
        regime_window_summary,
        regime_failures,
        regime_warnings,
    )


def _load_regime_diagnostic(
    *,
    run_id: str,
    date_text: str,
    reader: ArchiveReader,
) -> dict[str, object]:
    """Load one Layer 1.5 regime artifact row and normalize its readiness diagnostics."""
    stage_run_id = f"{run_id}-{date_text}"
    output_key = layer1_regime_path(stage_run_id)
    manifest_key = pipeline_manifest_path("layer1_5_regime", stage_run_id)
    diagnostic: dict[str, object] = {
        "date": date_text,
        "status": "failure",
        "reason": "missing_regime_output",
        "required_for_layer2": False,
        "output_key": output_key,
        "output_present": False,
        "manifest_key": manifest_key,
        "manifest_present": False,
        "manifest_status": None,
    }
    if not reader.exists(manifest_key):
        diagnostic["reason"] = "missing_regime_manifest"
        diagnostic["output_present"] = reader.exists(output_key)
        return diagnostic
    diagnostic["manifest_present"] = True
    try:
        manifest = PipelineManifestRecord.model_validate_json(reader.get_object(manifest_key))
    except Exception as exc:  # noqa: BLE001
        diagnostic["reason"] = "invalid_regime_manifest"
        diagnostic["manifest_error"] = str(exc)
        diagnostic["output_present"] = reader.exists(output_key)
        return diagnostic
    diagnostic["manifest_status"] = str(manifest.status)
    if manifest.stage != "layer1_5_regime":
        diagnostic["reason"] = "regime_manifest_stage_mismatch"
        diagnostic["manifest_stage"] = manifest.stage
        diagnostic["output_present"] = reader.exists(output_key)
        return diagnostic
    if manifest.status is not RunStatus.COMPLETED:
        diagnostic["reason"] = "regime_manifest_not_completed"
        diagnostic["output_present"] = reader.exists(output_key)
        return diagnostic
    if manifest.output_path != output_key:
        diagnostic["reason"] = "regime_manifest_output_mismatch"
        diagnostic["manifest_output_path"] = manifest.output_path
        diagnostic["output_present"] = reader.exists(output_key)
        return diagnostic
    inference_dates = manifest.metadata.get("inference_dates")
    if isinstance(inference_dates, list):
        normalized_dates = [str(value) for value in inference_dates]
        diagnostic["manifest_inference_dates"] = normalized_dates
        if date_text not in normalized_dates:
            diagnostic["reason"] = "regime_manifest_missing_inference_date"
            diagnostic["output_present"] = reader.exists(output_key)
            return diagnostic
    if not reader.exists(output_key):
        return diagnostic
    diagnostic["output_present"] = True

    try:
        frame = _read_parquet_frame(reader.get_object(output_key))
    except Exception as exc:  # noqa: BLE001
        diagnostic["reason"] = "regime_output_unreadable"
        diagnostic["output_error"] = str(exc)
        return diagnostic
    missing = sorted(set(HMM_REGIME_COLUMNS) - set(frame.columns))
    if missing:
        diagnostic["reason"] = "regime_output_missing_columns"
        diagnostic["missing_columns"] = missing
        return diagnostic
    rows = frame[frame["date"].astype(str) == date_text].reset_index(drop=True)
    if len(rows) != 1:
        diagnostic["reason"] = "regime_output_row_count_mismatch"
        diagnostic["row_count"] = len(rows)
        return diagnostic

    row = rows.iloc[0].to_dict()
    required_for_layer2 = bool(row.get("regime_required_for_layer2"))
    if "regime_required_for_layer2" not in row:
        required_for_layer2 = _row_has_populated_regime_values(row)
    missing_features = _split_csv_values(row.get("regime_missing_features"))
    default_status = "ready" if required_for_layer2 else "warning"
    default_reason = "ready" if required_for_layer2 else "legacy_missing_diagnostics"
    diagnostic.update(
        {
            "status": str(row.get("regime_readiness_status") or default_status),
            "reason": str(row.get("regime_readiness_reason") or default_reason),
            "required_for_layer2": required_for_layer2,
            "missing_features": missing_features,
            "complete_training_rows": _safe_int(row.get("complete_training_rows")),
            "min_training_rows": _safe_int(row.get("min_training_rows")),
            "probability_sum": _safe_float(row.get("regime_probability_sum")),
        }
    )
    if required_for_layer2 and not _row_has_populated_regime_values(row):
        diagnostic["status"] = "failure"
        diagnostic["reason"] = "required_regime_output_is_null"
    return diagnostic


def _empty_regime_window_summary() -> dict[str, object]:
    """Return the default Layer 1.5 readiness summary for empty validations."""
    return {
        "expected_dates": 0,
        "output_dates_present": [],
        "output_dates_missing": [],
        "manifest_dates_present": [],
        "manifest_dates_missing": [],
        "required_dates": [],
        "optional_dates": [],
        "ready_dates": [],
        "warning_dates": [],
        "failure_dates": [],
        "expected_ticker_rows": 0,
        "present_history_rows": 0,
        "full_feature_rows": 0,
        "explicit_null_rows": 0,
        "partial_feature_rows": 0,
        "rows_missing_feature_keys": 0,
        "invalid_value_rows": 0,
        "coverage_rate": None,
        "explicit_null_rate": None,
        "missing_or_partial_rate": None,
        "label_distribution": {},
        "confidence_min": None,
        "confidence_max": None,
        "probability_sum_min": None,
        "probability_sum_max": None,
        "sample_rows": [],
    }


def _finalize_regime_coverage(coverage: dict[str, object]) -> dict[str, object]:
    """Attach derived ratio fields to one date-level regime coverage summary."""
    expected = int(coverage["expected_ticker_rows"])
    full_rows = int(coverage["full_feature_rows"])
    explicit_null_rows = int(coverage["explicit_null_rows"])
    partial_rows = int(coverage["partial_feature_rows"])
    missing_rows = int(coverage["rows_missing_feature_keys"])
    coverage["coverage_rate"] = _safe_ratio(full_rows, expected)
    coverage["explicit_null_rate"] = _safe_ratio(explicit_null_rows, expected)
    coverage["missing_or_partial_rate"] = _safe_ratio(partial_rows + missing_rows, expected)
    coverage["label_distribution"] = dict(
        sorted(
            {
                str(label): int(count)
                for label, count in dict(coverage["label_distribution"]).items()
            }.items()
        )
    )
    return coverage


def _summarize_regime_window(
    *,
    required_dates: Sequence[str],
    optional_dates: Sequence[str],
    diagnostics_by_date: Mapping[str, Mapping[str, object]],
    coverage_by_date: Mapping[str, Mapping[str, object]],
) -> dict[str, object]:
    """Summarize Layer 1.5 archive coverage across the validated readiness window."""
    summary = _empty_regime_window_summary()
    summary["expected_dates"] = len(diagnostics_by_date)
    summary["required_dates"] = list(required_dates)
    summary["optional_dates"] = list(optional_dates)
    output_dates_present: list[str] = []
    output_dates_missing: list[str] = []
    manifest_dates_present: list[str] = []
    manifest_dates_missing: list[str] = []
    ready_dates: list[str] = []
    warning_dates: list[str] = []
    failure_dates: list[str] = []
    label_distribution: dict[str, int] = {}
    sample_rows: list[dict[str, object]] = []

    for date_text, diagnostic in sorted(diagnostics_by_date.items()):
        if bool(diagnostic.get("output_present")):
            output_dates_present.append(date_text)
        else:
            output_dates_missing.append(date_text)
        if bool(diagnostic.get("manifest_present")):
            manifest_dates_present.append(date_text)
        else:
            manifest_dates_missing.append(date_text)
        status = str(diagnostic.get("status") or "")
        if status == "ready":
            ready_dates.append(date_text)
        elif status == "warning":
            warning_dates.append(date_text)
        else:
            failure_dates.append(date_text)

        coverage = coverage_by_date.get(date_text, {})
        summary["expected_ticker_rows"] = int(summary["expected_ticker_rows"]) + int(
            coverage.get("expected_ticker_rows", 0)
        )
        summary["present_history_rows"] = int(summary["present_history_rows"]) + int(
            coverage.get("present_history_rows", 0)
        )
        summary["full_feature_rows"] = int(summary["full_feature_rows"]) + int(
            coverage.get("full_feature_rows", 0)
        )
        summary["explicit_null_rows"] = int(summary["explicit_null_rows"]) + int(
            coverage.get("explicit_null_rows", 0)
        )
        summary["partial_feature_rows"] = int(summary["partial_feature_rows"]) + int(
            coverage.get("partial_feature_rows", 0)
        )
        summary["rows_missing_feature_keys"] = int(
            summary["rows_missing_feature_keys"]
        ) + int(coverage.get("rows_missing_feature_keys", 0))
        summary["invalid_value_rows"] = int(summary["invalid_value_rows"]) + int(
            coverage.get("invalid_value_rows", 0)
        )
        for label, count in dict(coverage.get("label_distribution", {})).items():
            normalized_label = str(label).strip().lower()
            if not normalized_label:
                continue
            label_distribution[normalized_label] = (
                label_distribution.get(normalized_label, 0) + int(count)
            )
        _update_regime_coverage_ranges(
            coverage=summary,
            confidence=_safe_float(coverage.get("confidence_min")),
            probability_sum=_safe_float(coverage.get("probability_sum_min")),
        )
        _update_regime_coverage_ranges(
            coverage=summary,
            confidence=_safe_float(coverage.get("confidence_max")),
            probability_sum=_safe_float(coverage.get("probability_sum_max")),
        )
        for sample in coverage.get("sample_rows", []):
            if len(sample_rows) >= 10:
                break
            if isinstance(sample, dict):
                sample_rows.append({"date": date_text, **sample})

    summary["output_dates_present"] = output_dates_present
    summary["output_dates_missing"] = output_dates_missing
    summary["manifest_dates_present"] = manifest_dates_present
    summary["manifest_dates_missing"] = manifest_dates_missing
    summary["ready_dates"] = ready_dates
    summary["warning_dates"] = warning_dates
    summary["failure_dates"] = failure_dates
    summary["label_distribution"] = dict(sorted(label_distribution.items()))
    summary["sample_rows"] = sample_rows
    summary["coverage_rate"] = _safe_ratio(
        int(summary["full_feature_rows"]),
        int(summary["expected_ticker_rows"]),
    )
    summary["explicit_null_rate"] = _safe_ratio(
        int(summary["explicit_null_rows"]),
        int(summary["expected_ticker_rows"]),
    )
    summary["missing_or_partial_rate"] = _safe_ratio(
        int(summary["partial_feature_rows"]) + int(summary["rows_missing_feature_keys"]),
        int(summary["expected_ticker_rows"]),
    )
    return summary


def _update_regime_coverage_ranges(
    *,
    coverage: dict[str, object],
    confidence: float | None,
    probability_sum: float | None,
) -> None:
    """Update min/max regime confidence and probability-sum bounds in place."""
    if confidence is not None:
        existing_min = _safe_float(coverage.get("confidence_min"))
        existing_max = _safe_float(coverage.get("confidence_max"))
        coverage["confidence_min"] = (
            confidence if existing_min is None else min(existing_min, confidence)
        )
        coverage["confidence_max"] = (
            confidence if existing_max is None else max(existing_max, confidence)
        )
    if probability_sum is not None:
        existing_min = _safe_float(coverage.get("probability_sum_min"))
        existing_max = _safe_float(coverage.get("probability_sum_max"))
        coverage["probability_sum_min"] = (
            probability_sum if existing_min is None else min(existing_min, probability_sum)
        )
        coverage["probability_sum_max"] = (
            probability_sum if existing_max is None else max(existing_max, probability_sum)
        )


def _safe_ratio(numerator: int, denominator: int) -> float | None:
    """Return a rounded ratio when the denominator is non-zero."""
    if denominator <= 0:
        return None
    return numerator / denominator


def _regime_probability_sum(features: Mapping[str, object]) -> float | None:
    """Return the probability sum for one FeatureRecord's regime fields."""
    probabilities = [
        _safe_float(features.get(column_name)) for column_name in REGIME_PROBABILITY_COLUMNS
    ]
    if any(value is None for value in probabilities):
        return None
    return float(sum(value for value in probabilities if value is not None))


def _inspect_regime_feature_presence(features: Mapping[str, object]) -> dict[str, object]:
    """Summarize whether the expected regime keys are present and null/non-null."""
    missing_keys = [
        name for name in HMM_REGIME_COLUMNS[1:] if name not in features
    ]
    values = [features.get(name) for name in HMM_REGIME_COLUMNS[1:]]
    populated = [_normalize_optional_value(value) is not None for value in values]
    if missing_keys:
        state = "missing"
    elif all(populated):
        state = "full"
    elif not any(populated):
        state = "none"
    else:
        state = "partial"
    return {"state": state, "missing_keys": missing_keys}


def _regime_value_errors(
    *,
    date_text: str,
    ticker: str,
    features: Mapping[str, object],
) -> list[str]:
    """Return validation errors for one fully populated ticker-day regime feature set."""
    errors: list[str] = []
    label = str(features.get("regime_label")).strip().lower()
    if label not in REGIME_LABELS:
        errors.append(f"{ticker}/{date_text}: invalid regime_label={label!r}")
        return errors
    confidence = _safe_float(features.get("regime_confidence"))
    probabilities = {
        label_name: _safe_float(features.get(column_name))
        for label_name, column_name in zip(REGIME_LABELS, REGIME_PROBABILITY_COLUMNS, strict=True)
    }
    if confidence is None or any(value is None for value in probabilities.values()):
        errors.append(f"{ticker}/{date_text}: regime fields must be populated together")
        return errors
    if confidence < 0.0 or confidence > 1.0:
        errors.append(f"{ticker}/{date_text}: regime_confidence out of range")
    if any(value < 0.0 or value > 1.0 for value in probabilities.values()):
        errors.append(f"{ticker}/{date_text}: regime probabilities out of range")
    probability_sum = sum(probabilities.values())
    if abs(probability_sum - 1.0) > REGIME_PROBABILITY_SUM_TOLERANCE:
        errors.append(
            f"{ticker}/{date_text}: regime probabilities sum to {probability_sum:.6f}"
        )
    max_label = max(probabilities, key=probabilities.get)
    if label != max_label:
        errors.append(
            f"{ticker}/{date_text}: regime_label={label!r} does not match max probability"
        )
    if abs(confidence - probabilities[label]) > REGIME_PROBABILITY_SUM_TOLERANCE:
        errors.append(
            f"{ticker}/{date_text}: regime_confidence does not match regime_prob_{label}"
        )
    return errors


def _read_parquet_frame(payload: bytes):
    """Read a parquet payload into a pandas DataFrame."""
    try:
        import pandas as pd
    except ModuleNotFoundError as exc:
        raise ModuleNotFoundError(
            "pandas and pyarrow are required to read Layer 1 parquet payloads."
        ) from exc
    return pd.read_parquet(io.BytesIO(payload))


def _row_has_populated_regime_values(row: Mapping[str, object]) -> bool:
    """Return True when a regime artifact row is fully populated."""
    values = [_normalize_optional_value(row.get(name)) for name in HMM_REGIME_COLUMNS[1:]]
    return all(value is not None for value in values)


def _normalize_optional_value(value: object) -> object | None:
    """Normalize common null-like feature values to None."""
    if value is None:
        return None
    numeric = _safe_float(value)
    if numeric is not None:
        return numeric if math.isfinite(numeric) else None
    if isinstance(value, float) and not math.isfinite(value):
        return None
    normalized = str(value).strip()
    return normalized or None


def _safe_float(value: object) -> float | None:
    """Return a finite float or None when the value is null-like."""
    if value is None:
        return None
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if numeric != numeric:
        return None
    return numeric


def _safe_int(value: object) -> int | None:
    """Return an integer when coercion is lossless enough for diagnostics."""
    numeric = _safe_float(value)
    if numeric is None:
        return None
    return int(numeric)


def _split_csv_values(value: object) -> list[str]:
    """Return a normalized list from a comma-delimited diagnostic string."""
    if value is None:
        return []
    return [item for item in str(value).split(",") if item]


def _inspect_layer1_manifests(
    *,
    reader: ArchiveReader,
    run_id: str,
    require_completed_manifest: bool,
) -> ManifestInspectionResult:
    """Inspect exact and sibling Layer 1 manifests for operator-facing reporting."""
    exact_key = pipeline_manifest_path("layer1", run_id)
    related_manifests: list[dict[str, object]] = []
    manifest_status: str | None = None
    manifest_finished_at: str | None = None
    manifest_errors: list[str] = []
    family = _manifest_family(run_id)
    exact_manifest_found = False
    exact_manifest_loaded = False

    for key in _sorted_manifest_keys(reader.list_keys(LAYER1_MANIFEST_PREFIX)):
        related_run_id = _manifest_run_id_from_key(key)
        if not _same_manifest_family(run_id, related_run_id, family):
            continue
        if key == exact_key:
            exact_manifest_found = True
        if not reader.exists(key):
            entry = {
                "key": key,
                "run_id": related_run_id,
                "status": "missing",
                "finished_at": None,
                "error": "object_missing",
            }
            related_manifests.append(entry)
            if key == exact_key:
                manifest_errors.append("exact_manifest_missing_during_read")
            continue
        try:
            payload = reader.get_object(key).decode("utf-8")
        except (FileNotFoundError, KeyError) as exc:
            entry = {
                "key": key,
                "run_id": related_run_id,
                "status": "missing",
                "finished_at": None,
                "error": str(exc),
            }
            related_manifests.append(entry)
            if key == exact_key:
                manifest_errors.append("exact_manifest_missing_during_read")
            continue
        try:
            manifest = json.loads(payload)
        except json.JSONDecodeError as exc:
            entry = {
                "key": key,
                "run_id": related_run_id,
                "status": "invalid",
                "finished_at": None,
                "error": str(exc),
            }
            related_manifests.append(entry)
            if key == exact_key:
                manifest_errors.append("exact_manifest_invalid_json")
            continue

        status = manifest.get("status")
        finished_at = manifest.get("finished_at")
        entry = {
            "key": key,
            "run_id": str(manifest.get("run_id", related_run_id)),
            "status": str(status) if status is not None else None,
            "finished_at": str(finished_at) if finished_at is not None else None,
        }
        related_manifests.append(entry)
        if key == exact_key:
            exact_manifest_loaded = True
            manifest_status = entry["status"]
            manifest_finished_at = entry["finished_at"]

    stale_manifest_keys = [
        str(entry["key"])
        for entry in related_manifests
        if entry.get("status") == "running" and entry.get("key") != exact_key
    ]
    if require_completed_manifest:
        if not exact_manifest_found:
            manifest_errors.append("missing_exact_manifest")
        elif exact_manifest_loaded and manifest_status != "completed":
            manifest_errors.append(
                f"exact_manifest_not_completed:{manifest_status or 'unknown'}"
            )

    return ManifestInspectionResult(
        manifest_status=manifest_status,
        manifest_finished_at=manifest_finished_at,
        manifest_errors=manifest_errors,
        related_manifests=related_manifests,
        stale_manifest_keys=stale_manifest_keys,
    )


def _manifest_family(run_id: str) -> str:
    """Return the version-family prefix for one Layer 1 run identifier."""
    match = _RUN_ID_VERSION_RE.fullmatch(run_id)
    if match is None:
        return run_id
    return str(match.group("family"))


def _same_manifest_family(requested_run_id: str, candidate_run_id: str, family: str) -> bool:
    """Return True when two run identifiers belong to the same readiness family."""
    if candidate_run_id == requested_run_id:
        return True
    if family == requested_run_id:
        return False
    match = _RUN_ID_VERSION_RE.fullmatch(candidate_run_id)
    return match is not None and str(match.group("family")) == family


def _manifest_run_id_from_key(key: str) -> str:
    """Return the run identifier encoded in one Layer 1 manifest key."""
    return Path(key).stem


def _sorted_manifest_keys(keys: Sequence[str]) -> list[str]:
    """Sort manifest keys by family and numeric version when present."""
    return sorted(keys, key=_manifest_sort_key)


def _manifest_sort_key(key: str) -> tuple[str, int, str]:
    """Return a stable sort key for manifest names with optional -vN suffixes."""
    run_id = _manifest_run_id_from_key(key)
    match = _RUN_ID_VERSION_RE.fullmatch(run_id)
    if match is None:
        return (run_id, -1, run_id)
    return (str(match.group("family")), int(match.group("version")), run_id)


def _prefix_for_key(key: str) -> str:
    """Return the containing prefix for one canonical R2 object key."""
    return f"{Path(key).parent.as_posix()}/"


def _validate_iso_date(value: str, label: str) -> None:
    """Raise ValueError if `value` is not a canonical YYYY-MM-DD string."""
    try:
        parsed = Date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"{label} must be YYYY-MM-DD: {value!r}") from exc
    if parsed.isoformat() != value:
        raise ValueError(f"{label} must be YYYY-MM-DD: {value!r}")


def _normalize_csv_ticker(value: object) -> str | None:
    """Normalize a CSV ticker field, returning None for blank rows."""
    if value is None:
        return None
    ticker = str(value).strip().upper()
    return ticker or None


def _truthy(value: object, *, default: bool = False) -> bool:
    """Return True for common CSV boolean values."""
    if value is None:
        return default
    normalized = str(value).strip().lower()
    if not normalized:
        return default
    return normalized in {"1", "true", "t", "yes", "y"}


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """Parse CLI arguments for the validator."""
    parser = argparse.ArgumentParser(description="Validate the Layer 1 feature archive.")
    parser.add_argument("--run-id", required=True, help="Run identifier for the validation pass.")
    parser.add_argument("--from-date", required=True, help="Inclusive lower bound (YYYY-MM-DD).")
    parser.add_argument("--to-date", required=True, help="Inclusive upper bound (YYYY-MM-DD).")
    source_group = parser.add_mutually_exclusive_group(required=True)
    source_group.add_argument(
        "--universe",
        help="Path to a JSON file mapping date -> [tickers].",
    )
    source_group.add_argument(
        "--use-r2-universe",
        action="store_true",
        help="Load the validation universe directly from Layer 0 raw/universe/*.csv in R2.",
    )
    parser.add_argument(
        "--tickers",
        nargs="*",
        default=None,
        help="Optional ticker filter when --use-r2-universe is selected.",
    )
    parser.add_argument(
        "--output-dir",
        default=str(DEFAULT_REPORT_DIR),
        help=f"Output directory for the JSON report (default: {DEFAULT_REPORT_DIR}).",
    )
    args = parser.parse_args(argv)
    if args.tickers == []:
        parser.error("--tickers requires at least one ticker when provided")
    if args.tickers is not None and not args.use_r2_universe:
        parser.error("--tickers may only be used with --use-r2-universe")
    return args


def main(argv: Sequence[str] | None = None) -> int:
    """CLI entry point for `python -m app.lab.data_pipelines.validate_layer1_archive`."""
    from services.r2.writer import R2Writer

    args = _parse_args(argv)
    reader = R2Writer()
    if args.use_r2_universe:
        universe = load_universe_mapping_from_r2(
            from_date=args.from_date.strip(),
            to_date=args.to_date.strip(),
            reader=reader,
            requested_tickers=args.tickers,
        )
    else:
        universe = load_universe_mapping(Path(args.universe))
    report = validate_layer1_archive(
        run_id=args.run_id.strip(),
        from_date=args.from_date.strip(),
        to_date=args.to_date.strip(),
        universe=universe,
        reader=reader,
        output_prefixes=build_layer1_output_prefixes(sorted(universe.keys())),
        require_completed_manifest=True,
    )
    output_path = write_validation_report(report, Path(args.output_dir))
    logger.info(
        "Layer 1 validation written to {} ready_for_layer2={}",
        output_path,
        report.ready_for_layer2,
    )
    return 0 if report.ready_for_layer2 else 1


if __name__ == "__main__":
    raise SystemExit(main())
