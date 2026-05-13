"""UI-oriented payload builder for the Layer 0/1 feature audit dashboard."""
from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping, Sequence


def build_layer1_audit_dashboard_ui_payload(report: object) -> dict[str, object]:
    """Return a UI-oriented JSON payload derived from the backend dashboard report."""
    report_dict = _coerce_report_dict(report)
    selection_rows = _list_of_dicts(report_dict.get("selection_rows"))
    feature_summaries = _list_of_dicts(report_dict.get("feature_null_summaries"))
    family_summaries = _list_of_dicts(report_dict.get("family_status_summaries"))
    heatmap_cells = _list_of_dicts(report_dict.get("heatmap_cells"))
    outlier_records = _list_of_dicts(report_dict.get("outlier_records"))
    spot_check_records = _list_of_dicts(report_dict.get("spot_check_records"))
    formula_cards = _list_of_dicts(report_dict.get("formula_audit_cards"))
    family_definitions = _list_of_dicts(report_dict.get("family_definitions"))

    family_order = {
        str(item.get("family")): index
        for index, item in enumerate(family_definitions)
    }
    tickers = _sorted_unique(str(item.get("ticker", "")) for item in selection_rows)
    dates = _sorted_unique(str(item.get("date", "")) for item in selection_rows)
    feature_names = [str(item.get("feature_name", "")) for item in feature_summaries]
    spot_check_features = _sorted_unique(
        str(item.get("feature_name", ""))
        for item in spot_check_records
    )
    outlier_features = _sorted_unique(
        str(item.get("feature_name", ""))
        for item in outlier_records
    )
    focus_date = dates[-1] if dates else ""

    cells_by_feature_row: dict[str, dict[str, dict[str, object]]] = defaultdict(dict)
    for cell in heatmap_cells:
        feature_name = str(cell.get("feature_name", ""))
        row_key = str(cell.get("row_key", ""))
        if not feature_name or not row_key:
            continue
        cells_by_feature_row[feature_name][row_key] = _heatmap_cell_payload(cell)

    heatmap_columns = [
        {
            "row_key": str(row.get("row_key", "")),
            "date": str(row.get("date", "")),
            "ticker": str(row.get("ticker", "")),
            "feature_count": int(row.get("feature_count", 0)),
            "label": f"{row.get('date', '')} {row.get('ticker', '')}".strip(),
        }
        for row in selection_rows
    ]
    heatmap_rows: list[dict[str, object]] = []
    for summary in feature_summaries:
        feature_name = str(summary.get("feature_name", ""))
        heatmap_rows.append(
            {
                "feature_name": feature_name,
                "family": str(summary.get("family", "")),
                "family_label": str(summary.get("family_label", "")),
                "status": str(summary.get("status", "warn")),
                "required": bool(summary.get("required", False)),
                "nullable": bool(summary.get("nullable", True)),
                "missing_rate": _to_float(summary.get("missing_rate")),
                "null_rate": _to_float(summary.get("null_rate")),
                "invalid_rate": _to_float(summary.get("invalid_rate")),
                "issue_count": (
                    int(summary.get("missing_count", 0))
                    + int(summary.get("null_count", 0))
                    + int(summary.get("invalid_count", 0))
                ),
                "cells": [
                    cells_by_feature_row.get(feature_name, {}).get(
                        str(column.get("row_key", "")),
                        {
                            "row_key": str(column.get("row_key", "")),
                            "date": str(column.get("date", "")),
                            "ticker": str(column.get("ticker", "")),
                            "status": "warn",
                            "is_present": False,
                            "is_null": False,
                            "is_valid": False,
                            "value_label": "missing",
                            "message": "Cell missing from heatmap payload.",
                        },
                    )
                    for column in heatmap_columns
                ],
            }
        )

    family_panels = [
        {
            **item,
            "status": str(item.get("status", "warn")),
            "missing_rate": _to_float(item.get("missing_rate")),
            "null_rate": _to_float(item.get("null_rate")),
            "invalid_rate": _to_float(item.get("invalid_rate")),
            "issue_count": (
                int(item.get("missing_count", 0))
                + int(item.get("null_count", 0))
                + int(item.get("invalid_count", 0))
                + int(item.get("outlier_count", 0))
            ),
        }
        for item in sorted(
            family_summaries,
            key=lambda entry: (
                family_order.get(str(entry.get("family", "")), len(family_order)),
                str(entry.get("family", "")),
            ),
        )
    ]

    feature_null_bars = [
        {
            "feature_name": str(item.get("feature_name", "")),
            "family": str(item.get("family", "")),
            "family_label": str(item.get("family_label", "")),
            "status": str(item.get("status", "warn")),
            "missing_rate": _to_float(item.get("missing_rate")),
            "null_rate": _to_float(item.get("null_rate")),
            "invalid_rate": _to_float(item.get("invalid_rate")),
            "records_evaluated": int(item.get("records_evaluated", 0)),
        }
        for item in sorted(
            feature_summaries,
            key=lambda entry: (
                -_status_rank(str(entry.get("status", "warn"))),
                -(
                    _to_float(entry.get("missing_rate"))
                    + _to_float(entry.get("null_rate"))
                    + _to_float(entry.get("invalid_rate"))
                ),
                str(entry.get("feature_name", "")),
            ),
        )
    ]
    family_null_bars = [
        {
            "family": str(item.get("family", "")),
            "family_label": str(item.get("family_label", "")),
            "status": str(item.get("status", "warn")),
            "missing_rate": _to_float(item.get("missing_rate")),
            "null_rate": _to_float(item.get("null_rate")),
            "invalid_rate": _to_float(item.get("invalid_rate")),
            "outlier_count": int(item.get("outlier_count", 0)),
        }
        for item in family_panels
    ]

    spot_checks_by_series: dict[tuple[str, str], list[dict[str, object]]] = defaultdict(list)
    feature_status_counts: dict[str, dict[str, int]] = defaultdict(
        lambda: {"pass": 0, "warn": 0, "fail": 0}
    )
    for record in spot_check_records:
        feature_name = str(record.get("feature_name", ""))
        ticker = str(record.get("ticker", ""))
        if not feature_name or not ticker:
            continue
        status = str(record.get("status", "warn"))
        feature_status_counts[feature_name][status] += 1
        spot_checks_by_series[(feature_name, ticker)].append(
            {
                "date": str(record.get("date", "")),
                "row_key": str(record.get("row_key", "")),
                "status": status,
                "stored_value": _optional_float(record.get("stored_value")),
                "expected_value": _optional_float(record.get("expected_value")),
                "absolute_difference": _optional_float(record.get("absolute_difference")),
                "relative_difference": _optional_float(record.get("relative_difference")),
                "source_window_start": _optional_text(record.get("source_window_start")),
                "source_window_end": _optional_text(record.get("source_window_end")),
                "source_bar_count": int(record.get("source_bar_count", 0)),
                "message": _optional_text(record.get("message")),
                "missing_reason": _optional_text(record.get("missing_reason")),
                "point_in_time_safe": bool(record.get("point_in_time_safe", False)),
                "raw_inputs": dict(record.get("raw_inputs", {}))
                if isinstance(record.get("raw_inputs"), Mapping)
                else {},
            }
        )
    spot_check_series = [
        {
            "feature_name": feature_name,
            "ticker": ticker,
            "points": sorted(points, key=lambda item: str(item.get("date", ""))),
            "pass_count": sum(1 for item in points if item.get("status") == "pass"),
            "warn_count": sum(1 for item in points if item.get("status") == "warn"),
            "fail_count": sum(1 for item in points if item.get("status") == "fail"),
        }
        for (feature_name, ticker), points in sorted(
            spot_checks_by_series.items(),
            key=lambda item: (item[0][0], item[0][1]),
        )
    ]
    spot_check_feature_options = [
        {
            "feature_name": feature_name,
            "pass_count": counts["pass"],
            "warn_count": counts["warn"],
            "fail_count": counts["fail"],
        }
        for feature_name, counts in sorted(feature_status_counts.items())
    ]

    normalized_formula_cards = [
        {
            "row_key": str(item.get("row_key", "")),
            "date": str(item.get("date", "")),
            "ticker": str(item.get("ticker", "")),
            "feature_name": str(item.get("feature_name", "")),
            "status": str(item.get("status", "warn")),
            "title": str(item.get("title", "")),
            "formula": str(item.get("formula", "")),
            "calculation": str(item.get("calculation", "")),
            "point_in_time_note": str(item.get("point_in_time_note", "")),
            "expected_value": _optional_float(item.get("expected_value")),
            "stored_value": _optional_float(item.get("stored_value")),
            "message": _optional_text(item.get("message")),
            "missing_reason": _optional_text(item.get("missing_reason")),
            "raw_inputs": dict(item.get("raw_inputs", {}))
            if isinstance(item.get("raw_inputs"), Mapping)
            else {},
        }
        for item in sorted(
            formula_cards,
            key=lambda entry: (
                -_status_rank(str(entry.get("status", "warn"))),
                str(entry.get("feature_name", "")),
                str(entry.get("ticker", "")),
                str(entry.get("date", "")),
            ),
        )
    ]

    date_index = {date_value: index for index, date_value in enumerate(dates)}
    outlier_points = [
        {
            "row_key": str(item.get("row_key", "")),
            "date": str(item.get("date", "")),
            "date_index": date_index.get(str(item.get("date", "")), -1),
            "ticker": str(item.get("ticker", "")),
            "feature_name": str(item.get("feature_name", "")),
            "family": str(item.get("family", "")),
            "family_label": str(item.get("family_label", "")),
            "rule_type": str(item.get("rule_type", "")),
            "status": str(item.get("status", "fail")),
            "value": _to_float(item.get("value")),
            "lower_bound": _optional_float(item.get("lower_bound")),
            "upper_bound": _optional_float(item.get("upper_bound")),
            "severity_score": _outlier_severity(item),
            "message": str(item.get("message", "")),
        }
        for item in sorted(
            outlier_records,
            key=lambda entry: (
                str(entry.get("feature_name", "")),
                str(entry.get("date", "")),
                str(entry.get("ticker", "")),
            ),
        )
    ]

    return {
        "meta": {
            "title": "Layer 0/1 Feature Audit Dashboard",
            "read_only_notice": (
                "Read-only Layer 0/1 QA surface. This dashboard does not train models, "
                "score Layer 2 outputs, or route trades."
            ),
            "qa_scope": "Layer 0 and Layer 1 only",
            "status_help": [
                {
                    "status": "pass",
                    "label": "PASS",
                    "description": "Stored values are present, valid, and within audit tolerance.",
                },
                {
                    "status": "warn",
                    "label": "WARN",
                    "description": (
                        "The audit found optional missingness, skipped recomputation, or "
                        "non-blocking data quality issues that still deserve review."
                    ),
                },
                {
                    "status": "fail",
                    "label": "FAIL",
                    "description": (
                        "The audit found invalid values, missing required features, or "
                        "stored-vs-computed mismatches."
                    ),
                },
            ],
        },
        "report": {
            "run_id": str(report_dict.get("run_id", "")),
            "from_date": str(report_dict.get("from_date", "")),
            "to_date": str(report_dict.get("to_date", "")),
            "generated_at": str(report_dict.get("generated_at", "")),
            "tickers": list(report_dict.get("tickers", [])),
            "rows_loaded": int(report_dict.get("rows_loaded", 0)),
            "catalog_feature_count": int(report_dict.get("catalog_feature_count", 0)),
            "encountered_unknown_features": list(
                report_dict.get("encountered_unknown_features", [])
            ),
            "summary": dict(report_dict.get("summary", {}))
            if isinstance(report_dict.get("summary"), Mapping)
            else {},
            "load_warnings": _list_of_dicts(report_dict.get("load_warnings")),
        },
        "controls": {
            "available_dates": dates,
            "available_tickers": tickers,
            "available_families": family_definitions,
            "available_features": feature_names,
            "available_spot_check_features": spot_check_features,
            "available_outlier_features": outlier_features,
            "default_focus_date": focus_date,
            "default_spot_check_feature": _default_spot_check_feature(
                feature_options=spot_check_feature_options
            ),
            "default_outlier_feature": outlier_features[0] if outlier_features else "",
        },
        "family_panels": family_panels,
        "heatmap": {
            "columns": heatmap_columns,
            "rows": heatmap_rows,
        },
        "null_rates": {
            "by_feature": feature_null_bars,
            "by_family": family_null_bars,
        },
        "spot_checks": {
            "feature_options": spot_check_feature_options,
            "series": spot_check_series,
        },
        "formula_cards": normalized_formula_cards,
        "outliers": {
            "points": outlier_points,
            "table_rows": outlier_points,
        },
    }


def _coerce_report_dict(report: object) -> dict[str, object]:
    if isinstance(report, Mapping):
        return dict(report)
    to_dict = getattr(report, "to_dict", None)
    if callable(to_dict):
        data = to_dict()
        if isinstance(data, Mapping):
            return dict(data)
    raise TypeError("report must be a mapping or expose to_dict()")


def _list_of_dicts(value: object) -> list[dict[str, object]]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        return []
    items: list[dict[str, object]] = []
    for entry in value:
        if isinstance(entry, Mapping):
            items.append(dict(entry))
    return items


def _sorted_unique(values: Sequence[str]) -> list[str]:
    return sorted({value for value in values if value})


def _heatmap_cell_payload(cell: Mapping[str, object]) -> dict[str, object]:
    value = cell.get("value")
    return {
        "row_key": str(cell.get("row_key", "")),
        "date": str(cell.get("date", "")),
        "ticker": str(cell.get("ticker", "")),
        "status": str(cell.get("status", "warn")),
        "is_present": bool(cell.get("is_present", False)),
        "is_null": bool(cell.get("is_null", False)),
        "is_valid": bool(cell.get("is_valid", False)),
        "value": value,
        "value_label": _value_label(value=value),
        "message": _optional_text(cell.get("message")),
    }


def _default_spot_check_feature(*, feature_options: Sequence[Mapping[str, object]]) -> str:
    for item in feature_options:
        if int(item.get("fail_count", 0)) > 0:
            return str(item.get("feature_name", ""))
    for item in feature_options:
        if int(item.get("warn_count", 0)) > 0:
            return str(item.get("feature_name", ""))
    return str(feature_options[0].get("feature_name", "")) if feature_options else ""


def _outlier_severity(record: Mapping[str, object]) -> float:
    value = _optional_float(record.get("value"))
    lower_bound = _optional_float(record.get("lower_bound"))
    upper_bound = _optional_float(record.get("upper_bound"))
    if value is None:
        return 0.0
    if lower_bound is not None and value < lower_bound:
        scale = max(abs(lower_bound), 1.0)
        return abs(value - lower_bound) / scale
    if upper_bound is not None and value > upper_bound:
        scale = max(abs(upper_bound), 1.0)
        return abs(value - upper_bound) / scale
    return abs(value)


def _value_label(*, value: object) -> str:
    if value is None:
        return "null"
    numeric = _optional_float(value)
    if numeric is not None:
        return f"{numeric:.6f}"
    return str(value)


def _status_rank(status: str) -> int:
    return {"pass": 0, "warn": 1, "fail": 2}.get(status, 1)


def _to_float(value: object) -> float:
    numeric = _optional_float(value)
    return 0.0 if numeric is None else numeric


def _optional_float(value: object) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _optional_text(value: object) -> str | None:
    if value is None:
        return None
    text = str(value)
    return text if text else None
