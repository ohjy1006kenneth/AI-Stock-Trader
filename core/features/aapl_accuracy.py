"""AAPL-only Layer 1 feature accuracy and parameter calibration helpers."""
from __future__ import annotations

import importlib
import io
import json
import math
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from datetime import date as Date
from pathlib import Path
from typing import Any, Protocol

from core.contracts.schemas import FeatureRecord, PipelineManifestRecord
from core.features.catalog import feature_catalog, feature_family_map, validate_feature_value
from core.features.io import parquet_bytes_to_feature_record
from core.labels.forward_returns import compute_forward_return_labels
from services.r2.paths import (
    layer1_aapl_accuracy_report_path,
    layer1_feature_path,
    pipeline_manifest_path,
    raw_price_path,
    raw_universe_path,
)
from services.r2.writer import R2Writer

DEFAULT_AAPL_ACCURACY_CONFIG_PATH = Path("config/layer1_aapl_accuracy.json")
DEFAULT_AAPL_ACCURACY_OUTPUT_DIR = Path("artifacts/reports/diagnostics")


class AAPLAccuracyReader(Protocol):
    """Object-store operations required by the AAPL accuracy workflow."""

    def exists(self, key: str) -> bool:
        """Return True when the object exists."""

    def get_object(self, key: str) -> bytes:
        """Read an object payload by key."""

    def put_object(self, key: str, data: bytes | str) -> None:
        """Persist an object payload by key."""


@dataclass(frozen=True)
class AAPLQualityThresholds:
    """Acceptance thresholds for one AAPL Layer 1 accuracy report."""

    min_feature_rows: int = 1
    max_required_feature_null_rate: float = 0.35
    min_label_pairs: int = 3
    min_abs_best_candidate_correlation: float = 0.0

    def __post_init__(self) -> None:
        """Validate threshold ranges."""
        if self.min_feature_rows < 1:
            raise ValueError("min_feature_rows must be at least 1")
        if not 0.0 <= self.max_required_feature_null_rate <= 1.0:
            raise ValueError("max_required_feature_null_rate must be in [0.0, 1.0]")
        if self.min_label_pairs < 1:
            raise ValueError("min_label_pairs must be at least 1")
        if self.min_abs_best_candidate_correlation < 0.0:
            raise ValueError("min_abs_best_candidate_correlation must be non-negative")


@dataclass(frozen=True)
class MarketParameterCandidate:
    """Configurable candidate parameters for AAPL market-feature calibration."""

    name: str
    return_window_days: int
    volatility_window_days: int
    volume_window_days: int

    def __post_init__(self) -> None:
        """Validate candidate name and positive windows."""
        if not self.name.strip():
            raise ValueError("market parameter candidate name cannot be empty")
        for field_name, value in (
            ("return_window_days", self.return_window_days),
            ("volatility_window_days", self.volatility_window_days),
            ("volume_window_days", self.volume_window_days),
        ):
            if value <= 0:
                raise ValueError(f"{field_name} must be positive")


@dataclass(frozen=True)
class AAPLFeatureAccuracyConfig:
    """Configuration for one AAPL-only Layer 1 accuracy and calibration run."""

    ticker: str = "AAPL"
    benchmark_ticker: str = "SPY"
    target_horizon_days: int = 5
    quality_thresholds: AAPLQualityThresholds = field(default_factory=AAPLQualityThresholds)
    market_parameter_candidates: tuple[MarketParameterCandidate, ...] = (
        MarketParameterCandidate(
            name="baseline_momentum",
            return_window_days=21,
            volatility_window_days=21,
            volume_window_days=20,
        ),
    )

    def __post_init__(self) -> None:
        """Validate the fixed AAPL pilot scope and label horizon."""
        if self.ticker.strip().upper() != "AAPL":
            raise ValueError("AAPL accuracy workflow is intentionally limited to ticker=AAPL")
        if not self.benchmark_ticker.strip():
            raise ValueError("benchmark_ticker cannot be empty")
        if self.target_horizon_days <= 0:
            raise ValueError("target_horizon_days must be positive")
        if not self.market_parameter_candidates:
            raise ValueError("market_parameter_candidates must not be empty")


@dataclass(frozen=True)
class AAPLFeatureAccuracyReport:
    """Durable report for the AAPL-only Layer 1 feature accuracy pilot."""

    run_id: str
    layer1_run_id: str
    layer0_run_id: str | None
    ticker: str
    benchmark_ticker: str
    from_date: str
    to_date: str
    generated_at: str
    report_key: str
    input_evidence: dict[str, object]
    output_paths: dict[str, object]
    parameter_config: dict[str, object]
    feature_quality: dict[str, object]
    catalog_failures: list[dict[str, object]]
    optimization_results: list[dict[str, object]]
    best_parameter_candidate: dict[str, object] | None
    acceptance: dict[str, object]
    recommendation_for_issue_202: str

    def to_dict(self) -> dict[str, object]:
        """Return a JSON-serializable report payload."""
        return asdict(self)


def load_aapl_feature_accuracy_config(
    path: Path = DEFAULT_AAPL_ACCURACY_CONFIG_PATH,
) -> AAPLFeatureAccuracyConfig:
    """Load the AAPL feature-accuracy configuration from JSON."""
    payload = json.loads(path.read_text(encoding="utf-8"))
    thresholds = AAPLQualityThresholds(**dict(payload.get("quality_thresholds", {})))
    candidates = tuple(
        MarketParameterCandidate(**dict(candidate))
        for candidate in payload.get("market_parameter_candidates", [])
    )
    return AAPLFeatureAccuracyConfig(
        ticker=str(payload.get("ticker", "AAPL")).strip().upper(),
        benchmark_ticker=str(payload.get("benchmark_ticker", "SPY")).strip().upper(),
        target_horizon_days=int(payload.get("target_horizon_days", 5)),
        quality_thresholds=thresholds,
        market_parameter_candidates=candidates,
    )


def build_aapl_feature_accuracy_report(
    *,
    run_id: str,
    from_date: str,
    to_date: str,
    layer1_run_id: str | None = None,
    layer0_run_id: str | None = None,
    config: AAPLFeatureAccuracyConfig | None = None,
    writer: AAPLAccuracyReader | None = None,
    now: datetime | None = None,
) -> AAPLFeatureAccuracyReport:
    """Build and upload the AAPL-only Layer 1 feature accuracy report."""
    _validate_iso_date(from_date, "from_date")
    _validate_iso_date(to_date, "to_date")
    if from_date > to_date:
        raise ValueError("from_date must be <= to_date")
    if not run_id.strip():
        raise ValueError("run_id cannot be empty")

    active_config = config or load_aapl_feature_accuracy_config()
    active_writer = writer or R2Writer()
    active_layer1_run_id = layer1_run_id or run_id
    ticker = active_config.ticker.strip().upper()
    dates = _business_dates(from_date, to_date)
    feature_records, missing_feature_keys = _load_aapl_date_first_features(
        writer=active_writer,
        dates=dates,
        ticker=ticker,
    )
    price_key = raw_price_path(ticker)
    price_frame, raw_price_status = _load_raw_price_frame(active_writer, price_key)
    feature_quality, catalog_failures = _summarize_feature_quality(feature_records)
    optimization_results = _evaluate_market_parameter_candidates(
        price_frame=price_frame,
        feature_dates=[record.date for record in feature_records],
        ticker=ticker,
        horizon_days=active_config.target_horizon_days,
        candidates=active_config.market_parameter_candidates,
    )
    best_candidate = _best_candidate(optimization_results)
    report_key = layer1_aapl_accuracy_report_path(run_id, from_date, to_date)
    layer1_manifest_key = pipeline_manifest_path("layer1", active_layer1_run_id)
    layer0_manifest_key = (
        pipeline_manifest_path("layer0", layer0_run_id) if layer0_run_id is not None else None
    )
    input_evidence = {
        "layer0_manifest_key": layer0_manifest_key,
        "layer0_manifest_status": _manifest_status(active_writer, layer0_manifest_key),
        "layer1_manifest_key": layer1_manifest_key,
        "layer1_manifest_status": _manifest_status(active_writer, layer1_manifest_key),
        "raw_price_key": price_key,
        "raw_price_status": raw_price_status,
        "raw_price_rows": int(len(price_frame)),
        "universe_keys": [raw_universe_path(date_text) for date_text in dates],
    }
    output_paths = {
        "report_key": report_key,
        "feature_output_key_examples": [
            layer1_feature_path(record.date, record.ticker) for record in feature_records[:5]
        ],
        "missing_feature_keys": missing_feature_keys,
        "r2_output_prefixes": {
            "date_first_features": "features/{YYYY-MM-DD}/AAPL.parquet",
            "diagnostic_reports": "artifacts/reports/diagnostics/",
        },
    }
    acceptance = _build_acceptance(
        feature_rows=len(feature_records),
        missing_feature_keys=missing_feature_keys,
        feature_quality=feature_quality,
        catalog_failures=catalog_failures,
        best_candidate=best_candidate,
        raw_price_status=raw_price_status,
        thresholds=active_config.quality_thresholds,
    )
    recommendation = _recommend_issue_202(acceptance)
    report = AAPLFeatureAccuracyReport(
        run_id=run_id,
        layer1_run_id=active_layer1_run_id,
        layer0_run_id=layer0_run_id,
        ticker=ticker,
        benchmark_ticker=active_config.benchmark_ticker.strip().upper(),
        from_date=from_date,
        to_date=to_date,
        generated_at=(now or datetime.now(UTC)).replace(microsecond=0).isoformat(),
        report_key=report_key,
        input_evidence=input_evidence,
        output_paths=output_paths,
        parameter_config=_config_to_dict(active_config),
        feature_quality=feature_quality,
        catalog_failures=catalog_failures,
        optimization_results=optimization_results,
        best_parameter_candidate=best_candidate,
        acceptance=acceptance,
        recommendation_for_issue_202=recommendation,
    )
    active_writer.put_object(report_key, render_aapl_feature_accuracy_report(report))
    return report


def write_aapl_feature_accuracy_report(
    report: AAPLFeatureAccuracyReport,
    *,
    output_dir: Path | None = None,
) -> Path:
    """Write the AAPL feature accuracy report to a local JSON file."""
    target_dir = output_dir or DEFAULT_AAPL_ACCURACY_OUTPUT_DIR
    target_dir.mkdir(parents=True, exist_ok=True)
    path = target_dir / aapl_feature_accuracy_report_filename(report)
    path.write_text(render_aapl_feature_accuracy_report(report), encoding="utf-8")
    return path


def aapl_feature_accuracy_report_filename(report: AAPLFeatureAccuracyReport) -> str:
    """Return the deterministic local JSON filename for one AAPL accuracy report."""
    return (
        f"layer1_aapl_feature_accuracy_{report.run_id}_{report.from_date}"
        f"_to_{report.to_date}.json"
    )


def render_aapl_feature_accuracy_report(report: AAPLFeatureAccuracyReport) -> str:
    """Render the AAPL feature accuracy report as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def _load_aapl_date_first_features(
    *,
    writer: AAPLAccuracyReader,
    dates: Sequence[str],
    ticker: str,
) -> tuple[list[FeatureRecord], list[str]]:
    records: list[FeatureRecord] = []
    missing_keys: list[str] = []
    for date_text in dates:
        key = layer1_feature_path(date_text, ticker)
        if not writer.exists(key):
            missing_keys.append(key)
            continue
        record = parquet_bytes_to_feature_record(writer.get_object(key))
        if record.date != date_text or record.ticker != ticker:
            raise ValueError(
                "AAPL feature shard identity mismatch: "
                f"key={key} actual={record.date}/{record.ticker}"
            )
        records.append(record)
    return records, missing_keys


def _load_raw_price_frame(writer: AAPLAccuracyReader, key: str) -> tuple[Any, str]:
    """Return the raw price frame and structured availability status for diagnostics."""
    if not writer.exists(key):
        return _empty_frame(), "missing"
    return _read_parquet_frame(writer.get_object(key)), "available"


def _summarize_feature_quality(
    records: Sequence[FeatureRecord],
) -> tuple[dict[str, object], list[dict[str, object]]]:
    catalog = feature_catalog()
    families = feature_family_map()
    required_names = sorted(name for name, rule in catalog.items() if rule.required)
    null_required = 0
    required_observations = 0
    family_summary: dict[str, dict[str, int]] = {}
    catalog_failures: list[dict[str, object]] = []

    for record in records:
        for feature_name, rule in catalog.items():
            if not rule.required:
                continue
            required_observations += 1
            value = record.features.get(feature_name)
            if value is None:
                null_required += 1
            message = validate_feature_value(feature_name, value, rule)
            if message is not None:
                catalog_failures.append(
                    {
                        "date": record.date,
                        "ticker": record.ticker,
                        "feature": feature_name,
                        "message": message,
                    }
                )
        for feature_name, value in record.features.items():
            family = families.get(feature_name)
            family_key = family.key if family is not None else "uncataloged"
            summary = family_summary.setdefault(
                family_key,
                {"observations": 0, "nulls": 0, "finite_numeric": 0},
            )
            summary["observations"] += 1
            if value is None:
                summary["nulls"] += 1
            if _to_finite_float(value) is not None:
                summary["finite_numeric"] += 1

    null_rate = (
        null_required / required_observations if required_observations > 0 else 1.0
    )
    return (
        {
            "feature_rows": len(records),
            "feature_dates": [record.date for record in records],
            "catalog_required_feature_count": len(required_names),
            "required_feature_observations": required_observations,
            "required_feature_nulls": null_required,
            "required_feature_null_rate": null_rate,
            "catalog_failure_count": len(catalog_failures),
            "family_summary": family_summary,
        },
        catalog_failures,
    )


def _evaluate_market_parameter_candidates(
    *,
    price_frame: Any,
    feature_dates: Sequence[str],
    ticker: str,
    horizon_days: int,
    candidates: Sequence[MarketParameterCandidate],
) -> list[dict[str, object]]:
    pd = _require_pandas()
    if len(price_frame) == 0 or not feature_dates:
        return [
            {
                "name": candidate.name,
                "status": "insufficient_data",
                "label_pairs": 0,
                "abs_correlation_score": None,
                "parameters": asdict(candidate),
            }
            for candidate in candidates
        ]

    frame = price_frame.sort_values("date").drop_duplicates("date").reset_index(drop=True)
    labels = compute_forward_return_labels(frame, ticker)
    label_column = f"forward_return_{horizon_days}d"
    if label_column not in labels.columns:
        future = frame["adj_close"].astype(float).shift(-horizon_days)
        labels[label_column] = future / frame["adj_close"].astype(float) - 1.0
    allowed_dates = set(feature_dates)
    labels = labels.loc[labels["date"].isin(allowed_dates), ["date", label_column]]

    adj_close = frame["adj_close"].astype(float)
    returns_1d = adj_close.pct_change(1)
    volume = frame["volume"].astype(float)
    results: list[dict[str, object]] = []
    for candidate in candidates:
        candidate_frame = pd.DataFrame(
            {
                "date": frame["date"],
                "candidate_return": adj_close.pct_change(
                    candidate.return_window_days
                ).shift(1),
                "candidate_realized_vol": returns_1d.rolling(
                    candidate.volatility_window_days
                ).std().shift(1),
                "candidate_volume_ratio": (
                    volume / volume.rolling(candidate.volume_window_days).mean()
                ).shift(1),
            }
        )
        joined = candidate_frame.merge(labels, on="date", how="inner")
        correlations = {
            feature_name: _correlation(joined[feature_name], joined[label_column])
            for feature_name in (
                "candidate_return",
                "candidate_realized_vol",
                "candidate_volume_ratio",
            )
        }
        finite_correlations = [
            abs(value) for value in correlations.values() if value is not None
        ]
        label_pairs = int(joined[label_column].notna().sum())
        results.append(
            {
                "name": candidate.name,
                "status": "completed" if finite_correlations else "insufficient_data",
                "label_pairs": label_pairs,
                "target_label": label_column,
                "correlations": correlations,
                "abs_correlation_score": max(finite_correlations)
                if finite_correlations
                else None,
                "parameters": asdict(candidate),
            }
        )
    return results


def _build_acceptance(
    *,
    feature_rows: int,
    missing_feature_keys: Sequence[str],
    feature_quality: Mapping[str, object],
    catalog_failures: Sequence[Mapping[str, object]],
    best_candidate: Mapping[str, object] | None,
    raw_price_status: str,
    thresholds: AAPLQualityThresholds,
) -> dict[str, object]:
    required_null_rate = float(feature_quality.get("required_feature_null_rate", 1.0))
    best_score = (
        _to_finite_float(best_candidate.get("abs_correlation_score"))
        if best_candidate is not None
        else None
    )
    checks = {
        "has_raw_price_data": raw_price_status == "available",
        "has_min_feature_rows": feature_rows >= thresholds.min_feature_rows,
        "has_no_missing_date_first_shards": len(missing_feature_keys) == 0,
        "required_feature_null_rate_ok": (
            required_null_rate <= thresholds.max_required_feature_null_rate
        ),
        "catalog_validation_ok": len(catalog_failures) == 0,
        "has_min_label_pairs": (
            best_candidate is not None
            and int(best_candidate.get("label_pairs", 0)) >= thresholds.min_label_pairs
        ),
        "best_candidate_correlation_ok": (
            best_score is not None
            and best_score >= thresholds.min_abs_best_candidate_correlation
        ),
    }
    return {
        "checks": checks,
        "accepted": all(checks.values()),
        "thresholds": asdict(thresholds),
    }


def _recommend_issue_202(acceptance: Mapping[str, object]) -> str:
    checks = acceptance.get("checks")
    if not isinstance(checks, Mapping):
        return "needs_human_review"
    if bool(acceptance.get("accepted")):
        return "proceed"
    if not checks.get("has_raw_price_data", False):
        return "do_not_proceed"
    if not checks.get("has_no_missing_date_first_shards", False):
        return "do_not_proceed"
    if not checks.get("catalog_validation_ok", False):
        return "do_not_proceed"
    return "needs_human_review"


def _best_candidate(results: Sequence[Mapping[str, object]]) -> dict[str, object] | None:
    completed = [
        dict(result)
        for result in results
        if _to_finite_float(result.get("abs_correlation_score")) is not None
    ]
    if not completed:
        return None
    return max(
        completed,
        key=lambda result: float(result["abs_correlation_score"]),
    )


def _manifest_status(writer: AAPLAccuracyReader, key: str | None) -> str | None:
    if key is None:
        return None
    if not writer.exists(key):
        return "missing"
    manifest = PipelineManifestRecord.model_validate_json(writer.get_object(key))
    return str(manifest.status.value)


def _config_to_dict(config: AAPLFeatureAccuracyConfig) -> dict[str, object]:
    return {
        "ticker": config.ticker,
        "benchmark_ticker": config.benchmark_ticker,
        "target_horizon_days": config.target_horizon_days,
        "quality_thresholds": asdict(config.quality_thresholds),
        "market_parameter_candidates": [
            asdict(candidate) for candidate in config.market_parameter_candidates
        ],
    }


def _business_dates(from_date: str, to_date: str) -> tuple[str, ...]:
    start = Date.fromisoformat(from_date)
    end = Date.fromisoformat(to_date)
    current = start
    dates: list[str] = []
    while current <= end:
        if current.weekday() < 5:
            dates.append(current.isoformat())
        current = current.fromordinal(current.toordinal() + 1)
    return tuple(dates)


def _correlation(left: Any, right: Any) -> float | None:
    paired = _require_pandas().DataFrame({"left": left, "right": right}).dropna()
    if len(paired) < 2:
        return None
    value = paired["left"].corr(paired["right"])
    return _to_finite_float(value)


def _to_finite_float(value: object) -> float | None:
    if value is None:
        return None
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(numeric) or math.isinf(numeric):
        return None
    return numeric


def _read_parquet_frame(payload: bytes) -> Any:
    pd = _require_pandas()
    return pd.read_parquet(io.BytesIO(payload))


def _empty_frame() -> Any:
    pd = _require_pandas()
    return pd.DataFrame()


def _validate_iso_date(value: str, field_name: str) -> None:
    try:
        parsed = Date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"{field_name} must be YYYY-MM-DD") from exc
    if parsed.isoformat() != value:
        raise ValueError(f"{field_name} must be YYYY-MM-DD")


def _require_pandas() -> Any:
    try:
        import pandas as pd

        importlib.import_module("pyarrow")
    except ModuleNotFoundError as exc:
        raise ModuleNotFoundError(
            "pandas and pyarrow are required for the AAPL feature accuracy workflow."
        ) from exc
    return pd
