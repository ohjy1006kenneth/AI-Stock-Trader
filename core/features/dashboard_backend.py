"""Read-only Layer 1 audit dashboard backend and local report helpers."""
from __future__ import annotations

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
    build_market_feature_spot_checks,
    summarize_market_feature_spot_checks,
)
from services.r2.paths import layer1_ticker_history_path
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
    load_warnings: list[dict[str, object]]
    heatmap_cells: list[dict[str, object]]
    feature_null_summaries: list[dict[str, object]]
    family_status_summaries: list[dict[str, object]]
    outlier_records: list[dict[str, object]]
    spot_check_records: list[dict[str, object]]
    formula_audit_cards: list[dict[str, object]]
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
    summary = _build_dashboard_summary(
        selection_rows=selection_rows,
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
        load_warnings=[warning.to_dict() for warning in load_warnings],
        heatmap_cells=[cell.to_dict() for cell in heatmap_cells],
        feature_null_summaries=[summary_item.to_dict() for summary_item in feature_null_summaries],
        family_status_summaries=[summary_item.to_dict() for summary_item in family_status_summaries],
        outlier_records=[record.to_dict() for record in outlier_records],
        spot_check_records=[record.to_dict() for record in spot_check_records],
        formula_audit_cards=[card.to_dict() for card in formula_audit_cards],
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


def _build_dashboard_summary(
    *,
    selection_rows: Sequence[DashboardSelectionRow],
    load_warnings: Sequence[DashboardLoadWarning],
    family_status_summaries: Sequence[FeatureFamilyStatus],
    outlier_records: Sequence[OutlierRecord],
    spot_check_records: Sequence[object],
) -> dict[str, int]:
    counts = {"pass": 0, "warn": 0, "fail": 0}
    for item in family_status_summaries:
        counts[item.status] += 1
    spot_check_counts = summarize_market_feature_spot_checks(spot_check_records)
    return {
        "rows_loaded": len(selection_rows),
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
