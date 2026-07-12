"""Read-only Layer 1 audit dashboard backend and local report helpers."""
from __future__ import annotations

import io
import json
from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal

from core.contracts.schemas import FeatureRecord
from core.features.catalog import (
    FEATURE_FAMILY_SPECS,
    FeatureFamilySpec,
    FeatureRule,
    feature_catalog,
    feature_family_map,
    to_float_or_none,
    validate_feature_value,
)
from core.features.io import read_feature_history_window
from core.features.market_spotchecks import (
    MarketFeatureSpotCheckRecord,
    build_market_feature_spot_checks,
    summarize_market_feature_spot_checks,
)
from services.r2.paths import (
    layer1_regime_path,
    layer1_ticker_history_path,
)
from services.r2.writer import R2Writer

DashboardStatus = Literal["pass", "warn", "fail"]
DEFAULT_DASHBOARD_OUTPUT_DIR = Path("artifacts/reports/diagnostics")
IQR_MULTIPLIER = 3.0
MIN_DISTRIBUTION_OBSERVATIONS = 4


@dataclass(frozen=True)
class DashboardLoadWarning:
    """Non-fatal history loading warning for one ticker."""

    ticker: str
    history_key: str
    message: str

    def to_dict(self) -> dict[str, object]:
        """Return a JSON-serializable representation."""
        return asdict(self)


@dataclass(frozen=True)
class DashboardSelectionRow:
    """One selected `(date, ticker)` row loaded for the dashboard window."""

    row_key: str
    date: str
    ticker: str
    feature_count: int

    def to_dict(self) -> dict[str, object]:
        """Return a JSON-serializable representation."""
        return asdict(self)


@dataclass(frozen=True)
class DashboardCoverageByDate:
    """Coverage summary for one observed date in the selected dashboard window."""

    date: str
    status: DashboardStatus
    requested_ticker_count: int
    present_ticker_count: int
    missing_ticker_count: int
    missing_tickers: list[str]

    def to_dict(self) -> dict[str, object]:
        """Return a JSON-serializable representation."""
        return asdict(self)


@dataclass(frozen=True)
class DashboardCoverageByTicker:
    """Coverage summary for one selected ticker across the observed dates."""

    ticker: str
    history_key: str
    status: DashboardStatus
    observed_date_count: int
    present_date_count: int
    missing_date_count: int
    missing_dates: list[str]

    def to_dict(self) -> dict[str, object]:
        """Return a JSON-serializable representation."""
        return asdict(self)


@dataclass(frozen=True)
class FeatureHeatmapCell:
    """Per-row feature completeness and validity cell for the heatmap."""

    row_key: str
    date: str
    ticker: str
    feature_name: str
    family: str
    family_label: str
    status: DashboardStatus
    is_present: bool
    is_null: bool
    is_valid: bool
    value: float | int | str | bool | None = None
    message: str | None = None

    def to_dict(self) -> dict[str, object]:
        """Return a JSON-serializable representation."""
        return asdict(self)


@dataclass(frozen=True)
class FeatureNullRateSummary:
    """Null/missing summary for one feature across the selected history window."""

    feature_name: str
    family: str
    family_label: str
    status: DashboardStatus
    required: bool
    nullable: bool
    records_evaluated: int
    present_count: int
    missing_count: int
    null_count: int
    invalid_count: int
    valid_non_null_count: int
    missing_rate: float
    null_rate: float
    invalid_rate: float

    def to_dict(self) -> dict[str, object]:
        """Return a JSON-serializable representation."""
        return asdict(self)


@dataclass(frozen=True)
class FeatureFamilyStatus:
    """Family-level readiness summary for dashboard status cards."""

    family: str
    family_label: str
    status: DashboardStatus
    feature_count: int
    required_feature_count: int
    records_evaluated: int
    total_cells: int
    present_count: int
    missing_count: int
    required_missing_count: int
    optional_missing_count: int
    null_count: int
    invalid_count: int
    outlier_count: int
    missing_rate: float
    null_rate: float
    invalid_rate: float

    def to_dict(self) -> dict[str, object]:
        """Return a JSON-serializable representation."""
        return asdict(self)


@dataclass(frozen=True)
class OutlierRecord:
    """Numeric outlier or range-violation record for the dashboard."""

    row_key: str
    date: str
    ticker: str
    feature_name: str
    family: str
    family_label: str
    status: DashboardStatus
    rule_type: Literal["distribution_outlier", "range_violation"]
    value: float
    lower_bound: float | None
    upper_bound: float | None
    message: str

    def to_dict(self) -> dict[str, object]:
        """Return a JSON-serializable representation."""
        return asdict(self)


@dataclass(frozen=True)
class Layer1AuditDashboardReport:
    """Read-only visualization backend payload for the Layer 1 audit dashboard."""

    run_id: str
    from_date: str
    to_date: str
    tickers: tuple[str, ...]
    generated_at: str
    rows_loaded: int
    catalog_feature_count: int
    encountered_unknown_features: tuple[str, ...]
    family_definitions: list[dict[str, object]]
    selection_rows: list[dict[str, object]]
    coverage_by_date: list[dict[str, object]]
    coverage_by_ticker: list[dict[str, object]]
    load_warnings: list[dict[str, object]]
    heatmap_cells: list[dict[str, object]]
    feature_null_summaries: list[dict[str, object]]
    family_status_summaries: list[dict[str, object]]
    outlier_records: list[dict[str, object]]
    spot_check_records: list[dict[str, object]]
    formula_audit_cards: list[dict[str, object]]
    pilot_window_evidence: dict[str, object]
    summary: dict[str, int]

    def to_dict(self) -> dict[str, object]:
        """Return a JSON-serializable representation."""
        return asdict(self)


@dataclass(frozen=True)
class DashboardReportPaths:
    """Filesystem targets written for one dashboard report."""

    json_path: Path
    summary_path: Path


def build_layer1_audit_dashboard_report(
    *,
    run_id: str,
    from_date: str,
    to_date: str,
    tickers: Sequence[str],
    writer: R2Writer | None = None,
) -> Layer1AuditDashboardReport:
    """Build a read-only dashboard dataset from stored Layer 1 ticker histories."""
    start_text, end_text = _normalize_date_window(from_date=from_date, to_date=to_date)
    normalized_tickers = _normalize_tickers(tickers)
    if not normalized_tickers:
        raise ValueError("tickers must contain at least one non-empty ticker")

    active_writer = writer or R2Writer()
    loaded_rows: list[FeatureRecord] = []
    load_warnings: list[DashboardLoadWarning] = []
    for ticker in normalized_tickers:
        history_key = layer1_ticker_history_path(ticker)
        try:
            records = read_feature_history_window(
                ticker,
                start_date=start_text,
                end_date=end_text,
                writer=active_writer,
            )
        except FileNotFoundError:
            load_warnings.append(
                DashboardLoadWarning(
                    ticker=ticker,
                    history_key=history_key,
                    message="Layer 1 per-ticker history file is missing.",
                )
            )
            continue
        if not records:
            load_warnings.append(
                DashboardLoadWarning(
                    ticker=ticker,
                    history_key=history_key,
                    message="No Layer 1 history rows fell inside the selected date window.",
                )
            )
            continue
        loaded_rows.extend(records)

    sorted_rows = sorted(loaded_rows, key=lambda record: (record.date, record.ticker))
    selection_rows = [
        DashboardSelectionRow(
            row_key=_row_key(record),
            date=record.date,
            ticker=record.ticker,
            feature_count=len(record.features),
        )
        for record in sorted_rows
    ]
    coverage_by_date, coverage_by_ticker = _build_coverage_summaries(
        selection_rows=selection_rows,
        requested_tickers=normalized_tickers,
    )

    catalog = feature_catalog()
    family_by_feature = feature_family_map()
    unknown_features = sorted(
        {
            feature_name
            for record in sorted_rows
            for feature_name in record.features
            if feature_name not in catalog
        }
    )
    feature_names = _ordered_feature_names(catalog=catalog, unknown_features=unknown_features)
    heatmap_cells = _build_heatmap_cells(
        records=sorted_rows,
        feature_names=feature_names,
        catalog=catalog,
        family_by_feature=family_by_feature,
    )
    outlier_records = _build_outlier_records(
        cells=heatmap_cells,
        catalog=catalog,
        family_by_feature=family_by_feature,
    )
    feature_null_summaries = _build_feature_null_summaries(
        heatmap_cells=heatmap_cells,
        catalog=catalog,
        family_by_feature=family_by_feature,
    )
    family_status_summaries = _build_family_status_summaries(
        feature_summaries=feature_null_summaries,
        outlier_records=outlier_records,
    )
    spot_check_records, formula_audit_cards = build_market_feature_spot_checks(
        records=sorted_rows,
        writer=active_writer,
    )
    pilot_window_evidence = _build_pilot_window_evidence(
        selection_rows=selection_rows,
        requested_dates=(start_text, end_text),
        requested_tickers=normalized_tickers,
        writer=active_writer,
    )
    summary = _build_dashboard_summary(
        selection_rows=selection_rows,
        coverage_by_date=coverage_by_date,
        coverage_by_ticker=coverage_by_ticker,
        load_warnings=load_warnings,
        family_status_summaries=family_status_summaries,
        outlier_records=outlier_records,
        spot_check_records=spot_check_records,
    )

    return Layer1AuditDashboardReport(
        run_id=run_id,
        from_date=start_text,
        to_date=end_text,
        tickers=normalized_tickers,
        generated_at=datetime.now(UTC).replace(microsecond=0).isoformat(),
        rows_loaded=len(selection_rows),
        catalog_feature_count=len(catalog),
        encountered_unknown_features=tuple(unknown_features),
        family_definitions=[
            {
                "family": spec.key,
                "family_label": spec.label,
                "feature_names": list(spec.feature_names),
            }
            for spec in FEATURE_FAMILY_SPECS
        ],
        selection_rows=[row.to_dict() for row in selection_rows],
        coverage_by_date=[item.to_dict() for item in coverage_by_date],
        coverage_by_ticker=[item.to_dict() for item in coverage_by_ticker],
        load_warnings=[warning.to_dict() for warning in load_warnings],
        heatmap_cells=[cell.to_dict() for cell in heatmap_cells],
        feature_null_summaries=[summary_item.to_dict() for summary_item in feature_null_summaries],
        family_status_summaries=[summary_item.to_dict() for summary_item in family_status_summaries],
        outlier_records=[record.to_dict() for record in outlier_records],
        spot_check_records=[record.to_dict() for record in spot_check_records],
        formula_audit_cards=[card.to_dict() for card in formula_audit_cards],
        pilot_window_evidence=pilot_window_evidence,
        summary=summary,
    )


def write_layer1_audit_dashboard_report(
    report: Layer1AuditDashboardReport,
    *,
    output_dir: Path | None = None,
) -> DashboardReportPaths:
    """Write the durable JSON report and operator summary for the dashboard backend."""
    target_dir = output_dir or DEFAULT_DASHBOARD_OUTPUT_DIR
    target_dir.mkdir(parents=True, exist_ok=True)
    json_path = target_dir / dashboard_report_json_filename(report)
    summary_path = target_dir / dashboard_report_summary_filename(report)
    json_path.write_text(json.dumps(report.to_dict(), indent=2, sort_keys=True), encoding="utf-8")
    summary_path.write_text(render_layer1_audit_dashboard_summary(report), encoding="utf-8")
    return DashboardReportPaths(json_path=json_path, summary_path=summary_path)


def dashboard_report_json_filename(report: Layer1AuditDashboardReport) -> str:
    """Return the deterministic JSON filename for one dashboard report."""
    return (
        f"layer1_feature_audit_dashboard_{report.run_id}_{report.from_date}"
        f"_to_{report.to_date}.json"
    )


def dashboard_report_summary_filename(report: Layer1AuditDashboardReport) -> str:
    """Return the deterministic text-summary filename for one dashboard report."""
    return (
        f"layer1_feature_audit_dashboard_{report.run_id}_{report.from_date}"
        f"_to_{report.to_date}.txt"
    )


def render_layer1_audit_dashboard_summary(report: Layer1AuditDashboardReport) -> str:
    """Render a concise operator summary for the dashboard backend report."""
    lines = [
        "Layer 1 Audit Dashboard Backend",
        f"Run ID: {report.run_id}",
        f"Date window: {report.from_date} -> {report.to_date}",
        f"Tickers: {', '.join(report.tickers)}",
        f"Rows loaded: {report.rows_loaded}",
        (
            "Coverage: "
            f"dates PASS={report.summary.get('coverage_date_pass_count', 0)} "
            f"WARN={report.summary.get('coverage_date_warn_count', 0)}; "
            f"tickers PASS={report.summary.get('coverage_ticker_pass_count', 0)} "
            f"WARN={report.summary.get('coverage_ticker_warn_count', 0)} "
            f"FAIL={report.summary.get('coverage_ticker_fail_count', 0)}"
        ),
        f"Load warnings: {report.summary.get('load_warning_count', 0)}",
        (
            "Family status counts: "
            f"PASS={report.summary.get('family_pass_count', 0)} "
            f"WARN={report.summary.get('family_warn_count', 0)} "
            f"FAIL={report.summary.get('family_fail_count', 0)}"
        ),
        f"Outlier records: {report.summary.get('outlier_count', 0)}",
        (
            "Market spot checks: "
            f"PASS={report.summary.get('spot_check_pass_count', 0)} "
            f"WARN={report.summary.get('spot_check_warn_count', 0)} "
            f"FAIL={report.summary.get('spot_check_fail_count', 0)}"
        ),
        "",
        "Family Status:",
    ]
    for item in report.family_status_summaries:
        lines.append(
            "  "
            f"{item['family_label']}: {str(item['status']).upper()} "
            f"missing_rate={item['missing_rate']:.4f} "
            f"null_rate={item['null_rate']:.4f} "
            f"invalid_rate={item['invalid_rate']:.4f} "
            f"outliers={item['outlier_count']}"
        )
    if report.load_warnings:
        lines.extend(["", "Load Warnings:"])
        for item in report.load_warnings:
            lines.append(f"  {item['ticker']}: {item['message']}")
    partial_dates = [item for item in report.coverage_by_date if item["status"] != "pass"]
    partial_tickers = [item for item in report.coverage_by_ticker if item["status"] != "pass"]
    if partial_dates:
        lines.extend(["", "Partial Date Coverage:"])
        for item in partial_dates[:10]:
            lines.append(
                "  "
                f"{item['date']}: present={item['present_ticker_count']}/"
                f"{item['requested_ticker_count']} missing={', '.join(item['missing_tickers'])}"
            )
    if partial_tickers:
        lines.extend(["", "Partial Ticker Coverage:"])
        for item in partial_tickers[:10]:
            lines.append(
                "  "
                f"{item['ticker']}: present={item['present_date_count']}/"
                f"{item['observed_date_count']} missing={', '.join(item['missing_dates'])}"
            )
    if report.outlier_records:
        lines.extend(["", "Outlier Samples:"])
        for item in report.outlier_records[:10]:
            lines.append(
                "  "
                f"{item['ticker']} {item['date']} {item['feature_name']} "
                f"{item['rule_type']} value={item['value']}"
            )
    if report.spot_check_records:
        lines.extend(["", "Market Spot Check Samples:"])
        for item in report.spot_check_records[:10]:
            lines.append(
                "  "
                f"{item['ticker']} {item['date']} {item['feature_name']} "
                f"{str(item['status']).upper()} stored={item['stored_value']} "
                f"expected={item['expected_value']}"
            )
    return "\n".join(lines) + "\n"


def _build_pilot_window_evidence(
    *,
    selection_rows: Sequence[DashboardSelectionRow],
    requested_dates: Sequence[str],
    requested_tickers: Sequence[str],
    writer: R2Writer,
) -> dict[str, object]:
    """Load sentence/topic/regime evidence linked to the selected pilot window."""
    selected_dates = _normalize_date_hints(requested_dates) or tuple(
        sorted({row.date for row in selection_rows})
    )
    selected_tickers = _normalize_ticker_hints(requested_tickers) or tuple(
        sorted({row.ticker for row in selection_rows})
    )
    return {
        "selection": {
            "dates": list(selected_dates),
            "tickers": list(selected_tickers),
        },
        "sentiment": _build_sentiment_evidence(
            writer=writer,
            selected_dates=selected_dates,
            selected_tickers=selected_tickers,
        ),
        "topics": _build_topic_evidence(
            writer=writer,
            selected_dates=selected_dates,
            selected_tickers=selected_tickers,
        ),
        "regime": _build_regime_evidence(
            writer=writer,
            selected_dates=selected_dates,
        ),
    }


def _build_sentiment_evidence(
    *,
    writer: R2Writer,
    selected_dates: Sequence[str],
    selected_tickers: Sequence[str],
) -> dict[str, object]:
    manifests = _stage_manifests(writer, "layer1_finbert_sentiment", selected_dates)
    if not manifests:
        return _missing_stage_evidence(
            section="sentiment",
            stage="layer1_finbert_sentiment",
            selected_dates=selected_dates,
            rows_key="rows",
        )
    rows: list[dict[str, object]] = []
    blockers: list[dict[str, object]] = []
    for manifest in manifests:
        metadata_obj = manifest.get("metadata")
        metadata = metadata_obj if isinstance(metadata_obj, Mapping) else {}
        scored_key = str(metadata.get("scored_news_key") or manifest.get("output_path") or "")
        if not scored_key:
            blockers.append(
                {
                    "section": "sentiment",
                    "manifest_key": str(manifest.get("manifest_key", "")),
                    "run_id": str(manifest.get("run_id", "")),
                    "artifact_key": "",
                    "reason": "Layer 1 FinBERT sentiment manifest does not reference a scored-news artifact.",
                }
            )
            continue
        if not writer.exists(scored_key):
            blockers.append(
                {
                    "section": "sentiment",
                    "manifest_key": str(manifest.get("manifest_key", "")),
                    "run_id": str(manifest.get("run_id", "")),
                    "artifact_key": scored_key,
                    "reason": "Layer 1 FinBERT scored-news artifact is missing.",
                }
            )
            continue
        try:
            frame = _read_parquet_frame(writer, scored_key)
        except FileNotFoundError:
            blockers.append(
                {
                    "section": "sentiment",
                    "manifest_key": str(manifest.get("manifest_key", "")),
                    "run_id": str(manifest.get("run_id", "")),
                    "artifact_key": scored_key,
                    "reason": "Layer 1 FinBERT scored-news artifact could not be read.",
                }
            )
            continue
        for record in _frame_records(frame):
            if record.get("date") not in selected_dates or record.get("ticker") not in selected_tickers:
                continue
            rows.append(
                {
                    "manifest_key": manifest.get("manifest_key", ""),
                    "run_id": str(manifest.get("run_id", "")),
                    "scored_news_key": scored_key,
                    "date": str(record.get("date", "")),
                    "ticker": str(record.get("ticker", "")),
                    "article_id": _optional_text(record.get("article_id")),
                    "sentence_index": _optional_int(record.get("sentence_index")),
                    "headline": _optional_text(record.get("headline")),
                    "text": _optional_text(record.get("text")),
                    "source": _optional_text(record.get("source")),
                    "published_at": _optional_text(record.get("published_at")),
                    "sentiment_positive": _optional_float(record.get("sentiment_positive")),
                    "sentiment_negative": _optional_float(record.get("sentiment_negative")),
                    "sentiment_neutral": _optional_float(record.get("sentiment_neutral")),
                    "sentiment_score": _optional_float(record.get("sentiment_score")),
                    "relevance_score": _optional_float(record.get("relevance_score")),
                }
            )
    rows.sort(key=_topic_row_sort_key)
    return {
        "status": "pass" if rows else "warn",
        "row_count": len(rows),
        "manifest_count": len(manifests),
        "manifest_keys": [str(item.get("manifest_key", "")) for item in manifests],
        "blockers": blockers,
        "missing_artifact_keys": [str(item.get("artifact_key", "")) for item in blockers if item.get("artifact_key")],
        "rows": rows,
    }


def _build_topic_evidence(
    *,
    writer: R2Writer,
    selected_dates: Sequence[str],
    selected_tickers: Sequence[str],
) -> dict[str, object]:
    manifests = _stage_manifests(writer, "layer1_text_topics", selected_dates)
    if not manifests:
        return _missing_stage_topic_evidence(
            stage="layer1_text_topics",
            selected_dates=selected_dates,
        )

    label_rows: list[dict[str, object]] = []
    feature_rows: list[dict[str, object]] = []
    review_topics: list[dict[str, object]] = []
    review_rows: list[dict[str, object]] = []
    blockers: list[dict[str, object]] = []
    review_statuses: list[str] = []

    for manifest in manifests:
        metadata_obj = manifest.get("metadata")
        metadata = metadata_obj if isinstance(metadata_obj, Mapping) else {}
        label_key = str(metadata.get("topic_label_key") or manifest.get("output_path") or "")
        feature_key = str(metadata.get("topic_feature_key") or "")
        review_key = str(metadata.get("topic_review_key") or "")
        loaded_review = False

        if review_key:
            if not writer.exists(review_key):
                blockers.append(
                    {
                        "section": "topics",
                        "manifest_key": str(manifest.get("manifest_key", "")),
                        "run_id": str(manifest.get("run_id", "")),
                        "artifact_key": review_key,
                        "reason": "Layer 1 BERTopic review artifact is missing.",
                    }
                )
            else:
                try:
                    review_payload = json.loads(writer.get_object(review_key).decode("utf-8"))
                except FileNotFoundError:
                    blockers.append(
                        {
                            "section": "topics",
                            "manifest_key": str(manifest.get("manifest_key", "")),
                            "run_id": str(manifest.get("run_id", "")),
                            "artifact_key": review_key,
                            "reason": "Layer 1 BERTopic review artifact could not be read.",
                        }
                    )
                else:
                    review_statuses.append(
                        str(
                            review_payload.get("diversity_status")
                            or review_payload.get("status")
                            or "warn"
                        )
                    )
                    for topic in review_payload.get("topics", []):
                        if isinstance(topic, Mapping):
                            review_topics.append(dict(topic))
                    for row in review_payload.get("rows", []):
                        if not isinstance(row, Mapping):
                            continue
                        row_date = str(row.get("date", ""))
                        row_ticker = str(row.get("ticker", ""))
                        if row_date not in selected_dates or row_ticker not in selected_tickers:
                            continue
                        review_rows.append(dict(row))
                    loaded_review = True

        if not loaded_review and label_key:
            if not writer.exists(label_key):
                blockers.append(
                    {
                        "section": "topics",
                        "manifest_key": str(manifest.get("manifest_key", "")),
                        "run_id": str(manifest.get("run_id", "")),
                        "artifact_key": label_key,
                        "reason": "Layer 1 BERTopic label artifact is missing.",
                    }
                )
            else:
                try:
                    frame = _read_parquet_frame(writer, label_key)
                except FileNotFoundError:
                    blockers.append(
                        {
                            "section": "topics",
                            "manifest_key": str(manifest.get("manifest_key", "")),
                            "run_id": str(manifest.get("run_id", "")),
                            "artifact_key": label_key,
                            "reason": "Layer 1 BERTopic label artifact could not be read.",
                        }
                    )
                else:
                    for record in _frame_records(frame):
                        if record.get("date") not in selected_dates or record.get("ticker") not in selected_tickers:
                            continue
                        label_rows.append(
                            {
                                "manifest_key": manifest.get("manifest_key", ""),
                                "run_id": str(manifest.get("run_id", "")),
                                "topic_label_key": label_key,
                                "date": str(record.get("date", "")),
                                "ticker": str(record.get("ticker", "")),
                                "article_id": _optional_text(record.get("article_id")),
                                "sentence_index": _optional_int(record.get("sentence_index")),
                                "text": _optional_text(record.get("text")),
                                "embedding_cache_key": _optional_text(record.get("embedding_cache_key")),
                                "topic_model": _optional_text(record.get("topic_model")),
                                "topic_model_version": _optional_text(record.get("topic_model_version")),
                                "topic_id": _optional_int(record.get("topic_id")),
                                "topic_probability": _optional_float(record.get("topic_probability")),
                            }
                        )

        if feature_key:
            if not writer.exists(feature_key):
                blockers.append(
                    {
                        "section": "topics",
                        "manifest_key": str(manifest.get("manifest_key", "")),
                        "run_id": str(manifest.get("run_id", "")),
                        "artifact_key": feature_key,
                        "reason": "Layer 1 BERTopic feature artifact is missing.",
                    }
                )
                continue
            try:
                feature_frame = _read_parquet_frame(writer, feature_key)
            except FileNotFoundError:
                blockers.append(
                    {
                        "section": "topics",
                        "manifest_key": str(manifest.get("manifest_key", "")),
                        "run_id": str(manifest.get("run_id", "")),
                        "artifact_key": feature_key,
                        "reason": "Layer 1 BERTopic feature artifact could not be read.",
                    }
                )
                continue
            for record in _frame_records(feature_frame):
                if record.get("date") not in selected_dates or record.get("ticker") not in selected_tickers:
                    continue
                features_obj = record.get("features")
                features = features_obj if isinstance(features_obj, Mapping) else {}
                feature_rows.append(
                    {
                        "manifest_key": manifest.get("manifest_key", ""),
                        "run_id": str(manifest.get("run_id", "")),
                        "topic_feature_key": feature_key,
                        "date": str(record.get("date", "")),
                        "ticker": str(record.get("ticker", "")),
                        "nlp_sentence_count": _optional_int(features.get("nlp_sentence_count")),
                        "nlp_topic_count": _optional_int(features.get("nlp_topic_count")),
                        "nlp_dominant_topic_id": _optional_int(features.get("nlp_dominant_topic_id")),
                        "nlp_dominant_topic_probability": _optional_float(
                            features.get("nlp_dominant_topic_probability")
                        ),
                        "nlp_mean_topic_probability": _optional_float(
                            features.get("nlp_mean_topic_probability")
                        ),
                    }
                )

    if review_rows:
        rows = sorted(review_rows, key=_topic_row_sort_key)
        topics = sorted(review_topics, key=_topic_summary_sort_key)
        diversity_status = review_statuses[0] if review_statuses else str(
            review_topics[0].get("diversity_status", "diverse") if review_topics else "warn"
        )
        status = "pass" if diversity_status == "diverse" else "warn"
    else:
        rows, topics = _topic_rows_from_label_rows(label_rows)
        if topics:
            dominant_share = max(
                _optional_float(item.get("topic_row_share")) or 0.0 for item in topics
            )
            diversity_status = (
                "diverse" if len(topics) > 1 and dominant_share < 0.85 else "insufficient_diversity"
            )
        else:
            diversity_status = "insufficient_diversity"
        status = "pass" if diversity_status == "diverse" else "warn"

    feature_rows.sort(key=lambda item: (item["date"], item["ticker"]))
    return {
        "status": status,
        "diversity_status": diversity_status,
        "row_count": len(rows),
        "feature_row_count": len(feature_rows),
        "topic_count": len(topics),
        "manifest_count": len(manifests),
        "manifest_keys": [str(item.get("manifest_key", "")) for item in manifests],
        "blockers": blockers,
        "missing_artifact_keys": [
            str(item.get("artifact_key", "")) for item in blockers if item.get("artifact_key")
        ],
        "rows": rows,
        "topics": topics,
        "feature_rows": feature_rows,
    }


def _topic_rows_from_label_rows(label_rows: Sequence[dict[str, object]]) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    """Synthesize human-readable topic review payloads from label-only topic rows."""
    if not label_rows:
        return [], []

    grouped: dict[int, list[dict[str, object]]] = defaultdict(list)
    for row in label_rows:
        grouped[_optional_int(row.get("topic_id")) or -1].append(row)

    total_rows = len(label_rows)
    summaries = {
        topic_id: _topic_summary_from_label_group(topic_id, rows, total_rows)
        for topic_id, rows in grouped.items()
    }
    ordered_topics = sorted(
        summaries.values(),
        key=lambda item: (-float(item.get("topic_row_share", 0.0)), int(item.get("topic_id", -1))),
    )

    rows: list[dict[str, object]] = []
    for row in label_rows:
        topic_id = _optional_int(row.get("topic_id")) or -1
        summary = summaries[topic_id]
        rows.append(
            {
                **row,
                "topic_label": summary["topic_label"],
                "topic_keywords": summary["topic_keywords"],
                "topic_keyword_text": summary["topic_keyword_text"],
                "topic_example_text": summary["topic_example_text"],
                "topic_row_count": summary["topic_row_count"],
                "topic_row_share": summary["topic_row_share"],
            }
        )
    rows.sort(key=_topic_row_sort_key)
    return rows, ordered_topics


def _topic_summary_from_label_group(
    topic_id: int,
    rows: Sequence[dict[str, object]],
    total_rows: int,
) -> dict[str, object]:
    """Return a fallback topic summary when no review artifact exists."""
    example_text = next((str(row.get("text", "")).strip() for row in rows if str(row.get("text", "")).strip()), "")
    row_count = len(rows)
    row_share = row_count / max(1, total_rows)
    topic_label = "Outlier / Unassigned" if topic_id < 0 else f"Topic {topic_id}"
    return {
        "topic_id": topic_id,
        "topic_label": topic_label,
        "topic_keywords": [],
        "topic_keyword_text": "",
        "topic_example_text": example_text,
        "topic_example_texts": [example_text] if example_text else [],
        "topic_row_count": row_count,
        "topic_row_share": row_share,
        "topic_probability_mean": _mean_optional_float(
            [row.get("topic_probability") for row in rows]
        ),
        "topic_probability_max": _max_optional_float(
            [row.get("topic_probability") for row in rows]
        ),
        "diversity_status": "insufficient_diversity" if len(rows) <= 1 else "diverse",
    }


def _mean_optional_float(values: Sequence[object]) -> float | None:
    """Return the mean of nullable numeric values."""
    numeric_values = [value for value in (_optional_float(item) for item in values) if value is not None]
    if not numeric_values:
        return None
    return sum(numeric_values) / len(numeric_values)


def _max_optional_float(values: Sequence[object]) -> float | None:
    """Return the max of nullable numeric values."""
    numeric_values = [value for value in (_optional_float(item) for item in values) if value is not None]
    if not numeric_values:
        return None
    return max(numeric_values)


def _topic_row_sort_key(row: Mapping[str, object]) -> tuple[str, str, int]:
    """Return a stable sort key for topic review rows."""
    sentence_index = _optional_int(row.get("sentence_index"))
    return (
        str(row.get("date", "")),
        str(row.get("ticker", "")),
        sentence_index if sentence_index is not None else -1,
    )


def _topic_summary_sort_key(summary: Mapping[str, object]) -> tuple[float, int]:
    """Return a stable sort key for topic review summaries."""
    topic_share = _optional_float(summary.get("topic_row_share")) or 0.0
    topic_id = _optional_int(summary.get("topic_id")) or -1
    return (-topic_share, topic_id)


def _build_regime_evidence(
    *,
    writer: R2Writer,
    selected_dates: Sequence[str],
) -> dict[str, object]:
    """Load regime evidence for the selected dates."""
    manifests = _stage_manifests(writer, "layer1_5_regime", selected_dates)
    if not manifests:
        return _missing_stage_evidence(
            section="regime",
            stage="layer1_5_regime",
            selected_dates=selected_dates,
            rows_key="rows",
        )
    rows: list[dict[str, object]] = []
    blockers: list[dict[str, object]] = []
    for manifest in manifests:
        metadata_obj = manifest.get("metadata")
        metadata = metadata_obj if isinstance(metadata_obj, Mapping) else {}
        output_key = str(manifest.get("output_path") or metadata.get("output_path") or "")
        if not output_key:
            output_key = layer1_regime_path(str(manifest.get("run_id", "")))
        if not writer.exists(output_key):
            blockers.append(
                {
                    "section": "regime",
                    "manifest_key": str(manifest.get("manifest_key", "")),
                    "run_id": str(manifest.get("run_id", "")),
                    "artifact_key": output_key,
                    "reason": "Layer 1 regime artifact is missing.",
                }
            )
            continue
        try:
            frame = _read_parquet_frame(writer, output_key)
        except FileNotFoundError:
            blockers.append(
                {
                    "section": "regime",
                    "manifest_key": str(manifest.get("manifest_key", "")),
                    "run_id": str(manifest.get("run_id", "")),
                    "artifact_key": output_key,
                    "reason": "Layer 1 regime artifact could not be read.",
                }
            )
            continue
        for record in _frame_records(frame):
            if record.get("date") not in selected_dates:
                continue
            rows.append(
                {
                    "manifest_key": manifest.get("manifest_key", ""),
                    "run_id": str(manifest.get("run_id", "")),
                    "regime_key": output_key,
                    "date": str(record.get("date", "")),
                    "regime_label": _optional_text(record.get("regime_label")),
                    "regime_confidence": _optional_float(record.get("regime_confidence")),
                    "regime_prob_bear": _optional_float(record.get("regime_prob_bear")),
                    "regime_prob_sideways": _optional_float(record.get("regime_prob_sideways")),
                    "regime_prob_bull": _optional_float(record.get("regime_prob_bull")),
                    "regime_readiness_status": _optional_text(record.get("regime_readiness_status")),
                    "regime_readiness_reason": _optional_text(record.get("regime_readiness_reason")),
                    "regime_probability_sum": _optional_float(record.get("regime_probability_sum")),
                }
            )
    rows.sort(key=lambda item: item["date"])
    return {
        "status": "pass" if rows else "warn",
        "row_count": len(rows),
        "manifest_count": len(manifests),
        "manifest_keys": [str(item.get("manifest_key", "")) for item in manifests],
        "blockers": blockers,
        "missing_artifact_keys": [str(item.get("artifact_key", "")) for item in blockers if item.get("artifact_key")],
        "rows": rows,
    }


def _manifest_sort_key(item: Mapping[str, object]) -> tuple[str, str]:
    metadata_obj = item.get("metadata")
    metadata = metadata_obj if isinstance(metadata_obj, Mapping) else {}
    return (str(metadata.get("as_of_date", "")), str(item.get("run_id", "")))


def _stage_manifests(
    writer: R2Writer,
    stage: str,
    selected_dates: Sequence[str],
) -> list[dict[str, object]]:
    manifest_prefix = f"artifacts/manifests/{stage}/"
    manifests: list[dict[str, object]] = []
    for manifest_key in writer.list_keys(manifest_prefix):
        try:
            manifest_data = json.loads(writer.get_object(manifest_key).decode("utf-8"))
        except (FileNotFoundError, UnicodeDecodeError, json.JSONDecodeError):
            continue
        if not isinstance(manifest_data, dict):
            continue
        manifest: dict[str, object] = manifest_data
        metadata_obj = manifest.get("metadata")
        manifest_dates = {str(date) for date in selected_dates}
        manifest_date = ""
        if isinstance(metadata_obj, Mapping):
            manifest_date = str(metadata_obj.get("as_of_date", "")).strip()
            if not manifest_date:
                inference_dates = metadata_obj.get("inference_dates")
                if isinstance(inference_dates, Sequence) and inference_dates:
                    manifest_date = str(inference_dates[0]).strip()
        if manifest_date and manifest_dates and manifest_date not in manifest_dates:
            continue
        manifests.append({**manifest, "manifest_key": manifest_key})
    manifests.sort(key=_manifest_sort_key)
    return manifests


def _missing_stage_evidence(
    *,
    section: str,
    stage: str,
    selected_dates: Sequence[str],
    rows_key: str,
) -> dict[str, object]:
    date_text = ", ".join(selected_dates) if selected_dates else "the selected window"
    stage_label = _stage_label(stage)
    artifact_key = (
        f"artifacts/manifests/{stage}/<as_of_date={selected_dates[0] if selected_dates else 'unknown'}>"
    )
    return {
        "status": "warn",
        "row_count": 0,
        "manifest_count": 0,
        "manifest_keys": [],
        "blockers": [
            {
                "section": section,
                "manifest_key": f"artifacts/manifests/{stage}/",
                "run_id": "",
                "artifact_key": artifact_key,
                "reason": f"No {stage_label} manifest was published for {date_text}.",
            }
        ],
        "missing_artifact_keys": [artifact_key],
        rows_key: [],
    }


def _missing_stage_topic_evidence(
    *,
    stage: str,
    selected_dates: Sequence[str],
) -> dict[str, object]:
    base = _missing_stage_evidence(
        section="topics",
        stage=stage,
        selected_dates=selected_dates,
        rows_key="rows",
    )
    base["feature_row_count"] = 0
    base["feature_rows"] = []
    return base


def _stage_label(stage: str) -> str:
    labels = {
        "layer1_finbert_sentiment": "Layer 1 FinBERT sentiment",
        "layer1_text_topics": "Layer 1 BERTopic",
        "layer1_5_regime": "Layer 1 regime",
    }
    return labels.get(stage, stage)


def _normalize_date_hints(values: Sequence[str]) -> tuple[str, ...]:
    return tuple(
        sorted(
            {
                _coerce_iso_date(str(value).strip())
                for value in values
                if str(value).strip()
            }
        )
    )


def _normalize_ticker_hints(values: Sequence[str]) -> tuple[str, ...]:
    return tuple(
        sorted(
            {
                str(value).strip().upper()
                for value in values
                if str(value).strip()
            }
        )
    )


def _read_parquet_frame(writer: R2Writer, key: str):
    """Load a Parquet frame from object storage."""
    pd = _require_pandas()
    return pd.read_parquet(io.BytesIO(writer.get_object(key)))


def _frame_records(frame) -> list[dict[str, object]]:
    records: list[dict[str, object]] = []
    for row in frame.to_dict(orient="records"):
        records.append({name: _json_safe(value) for name, value in row.items()})
    return records


def _json_safe(value: object) -> object:
    if value is None or isinstance(value, (str, bool)):
        return value
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if hasattr(value, "item"):
        try:
            value = getattr(value, "item")()
        except Exception:
            return str(value)
        return _json_safe(value)
    if isinstance(value, float):
        if value != value or value in {float("inf"), float("-inf")}:  # NaN/inf
            return None
        return value
    return value


def _optional_int(value: object) -> int | None:
    numeric = _optional_float(value)
    if numeric is None:
        return None
    return int(numeric)


def _optional_float(value: object) -> float | None:
    return to_float_or_none(value)


def _optional_text(value: object) -> str | None:
    if value is None:
        return None
    text = str(value)
    return text if text else None


def _require_pandas():
    """Import pandas lazily when evidence loading needs Parquet access."""
    try:
        import pandas as pd
    except ModuleNotFoundError as exc:
        raise ModuleNotFoundError("pandas is required for pilot window evidence loading.") from exc
    return pd


def _build_heatmap_cells(
    *,
    records: Sequence[FeatureRecord],
    feature_names: Sequence[str],
    catalog: Mapping[str, FeatureRule],
    family_by_feature: Mapping[str, FeatureFamilySpec],
) -> list[FeatureHeatmapCell]:
    cells: list[FeatureHeatmapCell] = []
    for record in records:
        row_key = _row_key(record)
        for feature_name in feature_names:
            rule = catalog.get(feature_name)
            family = _family_for_feature(feature_name, family_by_feature=family_by_feature)
            if feature_name not in record.features:
                status = "fail" if rule is not None and rule.required else "warn"
                cells.append(
                    FeatureHeatmapCell(
                        row_key=row_key,
                        date=record.date,
                        ticker=record.ticker,
                        feature_name=feature_name,
                        family=family.key,
                        family_label=family.label,
                        status=status,
                        is_present=False,
                        is_null=False,
                        is_valid=False,
                        message=(
                            "Required feature missing from stored Layer 1 history row."
                            if status == "fail"
                            else "Optional feature absent from stored Layer 1 history row."
                        ),
                    )
                )
                continue

            value = record.features[feature_name]
            if rule is None:
                cells.append(
                    FeatureHeatmapCell(
                        row_key=row_key,
                        date=record.date,
                        ticker=record.ticker,
                        feature_name=feature_name,
                        family=family.key,
                        family_label=family.label,
                        status="warn",
                        is_present=True,
                        is_null=value is None,
                        is_valid=True,
                        value=value,
                        message="Feature is present but not part of the canonical audit catalog.",
                    )
                )
                continue

            message = validate_feature_value(feature_name, value, rule)
            if value is None:
                status = "warn" if message is None else "fail"
                is_valid = message is None
            else:
                status = "pass" if message is None else "fail"
                is_valid = message is None
            cells.append(
                FeatureHeatmapCell(
                    row_key=row_key,
                    date=record.date,
                    ticker=record.ticker,
                    feature_name=feature_name,
                    family=family.key,
                    family_label=family.label,
                    status=status,
                    is_present=True,
                    is_null=value is None,
                    is_valid=is_valid,
                    value=value,
                    message=message,
                )
            )
    return cells


def _build_feature_null_summaries(
    *,
    heatmap_cells: Sequence[FeatureHeatmapCell],
    catalog: Mapping[str, FeatureRule],
    family_by_feature: Mapping[str, FeatureFamilySpec],
) -> list[FeatureNullRateSummary]:
    grouped: dict[str, list[FeatureHeatmapCell]] = defaultdict(list)
    for cell in heatmap_cells:
        grouped[cell.feature_name].append(cell)

    summaries: list[FeatureNullRateSummary] = []
    for feature_name in sorted(grouped):
        cells = grouped[feature_name]
        rule = catalog.get(feature_name)
        family = _family_for_feature(feature_name, family_by_feature=family_by_feature)
        present_count = sum(1 for cell in cells if cell.is_present)
        missing_count = len(cells) - present_count
        null_count = sum(1 for cell in cells if cell.is_present and cell.is_null)
        invalid_count = sum(1 for cell in cells if cell.is_present and not cell.is_valid)
        valid_non_null_count = sum(
            1 for cell in cells if cell.is_present and cell.is_valid and not cell.is_null
        )
        required = False if rule is None else rule.required
        nullable = True if rule is None else rule.nullable
        status = _status_for_feature_summary(
            required=required,
            missing_count=missing_count,
            null_count=null_count,
            invalid_count=invalid_count,
        )
        summaries.append(
            FeatureNullRateSummary(
                feature_name=feature_name,
                family=family.key,
                family_label=family.label,
                status=status,
                required=required,
                nullable=nullable,
                records_evaluated=len(cells),
                present_count=present_count,
                missing_count=missing_count,
                null_count=null_count,
                invalid_count=invalid_count,
                valid_non_null_count=valid_non_null_count,
                missing_rate=_safe_rate(missing_count, len(cells)),
                null_rate=_safe_rate(null_count, len(cells)),
                invalid_rate=_safe_rate(invalid_count, len(cells)),
            )
        )
    return summaries


def _build_family_status_summaries(
    *,
    feature_summaries: Sequence[FeatureNullRateSummary],
    outlier_records: Sequence[OutlierRecord],
) -> list[FeatureFamilyStatus]:
    by_family: dict[str, list[FeatureNullRateSummary]] = defaultdict(list)
    outlier_count_by_family: dict[str, int] = defaultdict(int)
    for summary in feature_summaries:
        by_family[summary.family].append(summary)
    for record in outlier_records:
        outlier_count_by_family[record.family] += 1

    family_statuses: list[FeatureFamilyStatus] = []
    for spec in FEATURE_FAMILY_SPECS:
        summaries = by_family.get(spec.key, [])
        if not summaries:
            family_statuses.append(
                FeatureFamilyStatus(
                    family=spec.key,
                    family_label=spec.label,
                    status="warn",
                    feature_count=len(spec.feature_names),
                    required_feature_count=0,
                    records_evaluated=0,
                    total_cells=0,
                    present_count=0,
                    missing_count=0,
                    required_missing_count=0,
                    optional_missing_count=0,
                    null_count=0,
                    invalid_count=0,
                    outlier_count=0,
                    missing_rate=0.0,
                    null_rate=0.0,
                    invalid_rate=0.0,
                )
            )
            continue
        total_cells = sum(item.records_evaluated for item in summaries)
        required_missing = sum(
            item.missing_count for item in summaries if item.required
        )
        optional_missing = sum(
            item.missing_count for item in summaries if not item.required
        )
        invalid_count = sum(item.invalid_count for item in summaries)
        null_count = sum(item.null_count for item in summaries)
        outlier_count = outlier_count_by_family.get(spec.key, 0)
        present_count = sum(item.present_count for item in summaries)
        missing_count = sum(item.missing_count for item in summaries)
        status = _status_for_family_summary(
            required_missing_count=required_missing,
            optional_missing_count=optional_missing,
            null_count=null_count,
            invalid_count=invalid_count,
            outlier_count=outlier_count,
        )
        family_statuses.append(
            FeatureFamilyStatus(
                family=spec.key,
                family_label=spec.label,
                status=status,
                feature_count=len(summaries),
                required_feature_count=sum(1 for item in summaries if item.required),
                records_evaluated=max(item.records_evaluated for item in summaries),
                total_cells=total_cells,
                present_count=present_count,
                missing_count=missing_count,
                required_missing_count=required_missing,
                optional_missing_count=optional_missing,
                null_count=null_count,
                invalid_count=invalid_count,
                outlier_count=outlier_count,
                missing_rate=_safe_rate(missing_count, total_cells),
                null_rate=_safe_rate(null_count, total_cells),
                invalid_rate=_safe_rate(invalid_count, total_cells),
            )
        )
    return family_statuses


def _build_outlier_records(
    *,
    cells: Sequence[FeatureHeatmapCell],
    catalog: Mapping[str, FeatureRule],
    family_by_feature: Mapping[str, FeatureFamilySpec],
) -> list[OutlierRecord]:
    grouped: dict[str, list[FeatureHeatmapCell]] = defaultdict(list)
    outliers: list[OutlierRecord] = []
    for cell in cells:
        grouped[cell.feature_name].append(cell)
        rule = catalog.get(cell.feature_name)
        if (
            rule is None
            or rule.kind != "number"
            or not cell.is_present
            or cell.value is None
        ):
            continue
        numeric = to_float_or_none(cell.value)
        if numeric is None:
            continue
        lower_bound = rule.minimum
        upper_bound = rule.maximum
        if (lower_bound is not None and numeric < lower_bound) or (
            upper_bound is not None and numeric > upper_bound
        ):
            family = _family_for_feature(cell.feature_name, family_by_feature=family_by_feature)
            outliers.append(
                OutlierRecord(
                    row_key=cell.row_key,
                    date=cell.date,
                    ticker=cell.ticker,
                    feature_name=cell.feature_name,
                    family=family.key,
                    family_label=family.label,
                    status="fail",
                    rule_type="range_violation",
                    value=numeric,
                    lower_bound=lower_bound,
                    upper_bound=upper_bound,
                    message=cell.message or "Feature value violated the configured range rule.",
                )
            )

    for feature_name, feature_cells in grouped.items():
        rule = catalog.get(feature_name)
        if rule is None or rule.kind != "number":
            continue
        numeric_cells = []
        for cell in feature_cells:
            if not cell.is_present or cell.value is None or not cell.is_valid:
                continue
            numeric = to_float_or_none(cell.value)
            if numeric is None:
                continue
            numeric_cells.append((cell, numeric))
        if len(numeric_cells) < MIN_DISTRIBUTION_OBSERVATIONS:
            continue
        numeric_values = sorted(value for _, value in numeric_cells)
        q1 = _quantile(numeric_values, 0.25)
        q3 = _quantile(numeric_values, 0.75)
        iqr = q3 - q1
        if iqr <= 0.0:
            continue
        lower_bound = q1 - IQR_MULTIPLIER * iqr
        upper_bound = q3 + IQR_MULTIPLIER * iqr
        family = _family_for_feature(feature_name, family_by_feature=family_by_feature)
        for cell, numeric in numeric_cells:
            if lower_bound <= numeric <= upper_bound:
                continue
            outliers.append(
                OutlierRecord(
                    row_key=cell.row_key,
                    date=cell.date,
                    ticker=cell.ticker,
                    feature_name=feature_name,
                    family=family.key,
                    family_label=family.label,
                    status="warn",
                    rule_type="distribution_outlier",
                    value=numeric,
                    lower_bound=lower_bound,
                    upper_bound=upper_bound,
                    message=(
                        "Feature value falls outside the dashboard's IQR outlier fence "
                        f"({IQR_MULTIPLIER:.1f}x IQR)."
                    ),
                )
            )
    return sorted(
        outliers,
        key=lambda record: (
            record.rule_type,
            record.feature_name,
            record.date,
            record.ticker,
        ),
    )


def _build_coverage_summaries(
    *,
    selection_rows: Sequence[DashboardSelectionRow],
    requested_tickers: Sequence[str],
) -> tuple[list[DashboardCoverageByDate], list[DashboardCoverageByTicker]]:
    """Summarize selected-window coverage by observed date and requested ticker."""
    requested = tuple(
        sorted(
            {
                ticker.strip().upper()
                for ticker in requested_tickers
                if ticker.strip()
            }
        )
    )
    rows_by_date: dict[str, set[str]] = defaultdict(set)
    rows_by_ticker: dict[str, set[str]] = defaultdict(set)
    for row in selection_rows:
        rows_by_date[row.date].add(row.ticker)
        rows_by_ticker[row.ticker].add(row.date)

    observed_dates = sorted(rows_by_date)
    coverage_by_date: list[DashboardCoverageByDate] = []
    for date_text in observed_dates:
        present_tickers = rows_by_date.get(date_text, set())
        missing_tickers = sorted(set(requested) - present_tickers)
        status: DashboardStatus = "pass" if not missing_tickers else "warn"
        coverage_by_date.append(
            DashboardCoverageByDate(
                date=date_text,
                status=status,
                requested_ticker_count=len(requested),
                present_ticker_count=len(present_tickers),
                missing_ticker_count=len(missing_tickers),
                missing_tickers=missing_tickers,
            )
        )

    coverage_by_ticker: list[DashboardCoverageByTicker] = []
    observed_date_set = set(observed_dates)
    for ticker in requested:
        present_dates = rows_by_ticker.get(ticker, set())
        missing_dates = sorted(observed_date_set - present_dates)
        if observed_dates and not present_dates:
            status = "fail"
        elif missing_dates:
            status = "warn"
        else:
            status = "pass"
        coverage_by_ticker.append(
            DashboardCoverageByTicker(
                ticker=ticker,
                history_key=layer1_ticker_history_path(ticker),
                status=status,
                observed_date_count=len(observed_dates),
                present_date_count=len(present_dates),
                missing_date_count=len(missing_dates),
                missing_dates=missing_dates,
            )
        )
    return coverage_by_date, coverage_by_ticker


def _build_dashboard_summary(
    *,
    selection_rows: Sequence[DashboardSelectionRow],
    coverage_by_date: Sequence[DashboardCoverageByDate],
    coverage_by_ticker: Sequence[DashboardCoverageByTicker],
    load_warnings: Sequence[DashboardLoadWarning],
    family_status_summaries: Sequence[FeatureFamilyStatus],
    outlier_records: Sequence[OutlierRecord],
    spot_check_records: Sequence[MarketFeatureSpotCheckRecord],
) -> dict[str, int]:
    counts = {"pass": 0, "warn": 0, "fail": 0}
    # Date coverage only summarizes dates that were actually observed in loaded rows, so
    # fully missing dates are surfaced through ticker failures/load warnings rather than a
    # separate date-level fail bucket.
    coverage_date_counts = {"pass": 0, "warn": 0}
    coverage_ticker_counts = {"pass": 0, "warn": 0, "fail": 0}
    for item in family_status_summaries:
        counts[item.status] += 1
    for item in coverage_by_date:
        coverage_date_counts[item.status] += 1
    for item in coverage_by_ticker:
        coverage_ticker_counts[item.status] += 1
    spot_check_counts = summarize_market_feature_spot_checks(spot_check_records)
    return {
        "rows_loaded": len(selection_rows),
        "coverage_date_pass_count": coverage_date_counts["pass"],
        "coverage_date_warn_count": coverage_date_counts["warn"],
        "coverage_ticker_pass_count": coverage_ticker_counts["pass"],
        "coverage_ticker_warn_count": coverage_ticker_counts["warn"],
        "coverage_ticker_fail_count": coverage_ticker_counts["fail"],
        "load_warning_count": len(load_warnings),
        "family_pass_count": counts["pass"],
        "family_warn_count": counts["warn"],
        "family_fail_count": counts["fail"],
        "outlier_count": len(outlier_records),
        "spot_check_pass_count": spot_check_counts["pass"],
        "spot_check_warn_count": spot_check_counts["warn"],
        "spot_check_fail_count": spot_check_counts["fail"],
    }


def _ordered_feature_names(
    *,
    catalog: Mapping[str, FeatureRule],
    unknown_features: Sequence[str],
) -> list[str]:
    ordered: list[str] = []
    seen: set[str] = set()
    for spec in FEATURE_FAMILY_SPECS:
        for feature_name in spec.feature_names:
            if feature_name in catalog and feature_name not in seen:
                seen.add(feature_name)
                ordered.append(feature_name)
    remaining_catalog = sorted(name for name in catalog if name not in seen)
    ordered.extend(remaining_catalog)
    ordered.extend(sorted(unknown_features))
    return ordered


def _family_for_feature(
    feature_name: str,
    *,
    family_by_feature: Mapping[str, FeatureFamilySpec],
) -> FeatureFamilySpec:
    return family_by_feature.get(
        feature_name,
        FeatureFamilySpec(
            key="uncataloged",
            label="Uncataloged",
            feature_names=(feature_name,),
        ),
    )


def _status_for_feature_summary(
    *,
    required: bool,
    missing_count: int,
    null_count: int,
    invalid_count: int,
) -> DashboardStatus:
    if invalid_count > 0 or (required and missing_count > 0):
        return "fail"
    if missing_count > 0 or null_count > 0:
        return "warn"
    return "pass"


def _status_for_family_summary(
    *,
    required_missing_count: int,
    optional_missing_count: int,
    null_count: int,
    invalid_count: int,
    outlier_count: int,
) -> DashboardStatus:
    if invalid_count > 0 or required_missing_count > 0:
        return "fail"
    if optional_missing_count > 0 or null_count > 0 or outlier_count > 0:
        return "warn"
    return "pass"


def _normalize_date_window(*, from_date: str, to_date: str) -> tuple[str, str]:
    start = _coerce_iso_date(from_date)
    end = _coerce_iso_date(to_date)
    if start > end:
        raise ValueError("from_date must be less than or equal to to_date")
    return start, end


def _normalize_tickers(tickers: Sequence[str]) -> tuple[str, ...]:
    unique: list[str] = []
    seen: set[str] = set()
    for ticker in tickers:
        normalized = str(ticker).strip().upper()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        unique.append(normalized)
    return tuple(unique)


def _coerce_iso_date(value: str) -> str:
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"Date must be YYYY-MM-DD: {value}") from exc
    if parsed.date().isoformat() != value:
        raise ValueError(f"Date must be YYYY-MM-DD: {value}")
    return value


def _row_key(record: FeatureRecord) -> str:
    return f"{record.date}|{record.ticker}"


def _quantile(values: Sequence[float], probability: float) -> float:
    if not values:
        raise ValueError("values must not be empty")
    if len(values) == 1:
        return values[0]
    position = (len(values) - 1) * probability
    lower_index = int(position)
    upper_index = min(lower_index + 1, len(values) - 1)
    lower_value = values[lower_index]
    upper_value = values[upper_index]
    if lower_index == upper_index:
        return lower_value
    fraction = position - lower_index
    return lower_value + (upper_value - lower_value) * fraction


def _safe_rate(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return numerator / denominator
