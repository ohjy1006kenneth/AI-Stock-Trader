from __future__ import annotations

import math
from pathlib import Path

import pytest

from app.lab.feature_audit_dashboard import _DashboardDefaults, _render_dashboard_html
from core.features.dashboard_backend import build_layer1_audit_dashboard_report
from core.features.dashboard_ui import build_layer1_audit_dashboard_ui_payload
from tests.fixtures.layer1_audit_support import local_writer
from tests.fixtures.layer1_dashboard_support import seed_layer1_dashboard_fixture


def test_build_layer1_audit_dashboard_ui_payload_groups_backend_data_for_the_web_ui(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The UI payload should expose controls, heatmap rows, and grouped chart series."""
    writer = local_writer(tmp_path, monkeypatch)
    fixture = seed_layer1_dashboard_fixture(writer)
    report = build_layer1_audit_dashboard_report(
        run_id="dashboard-ui",
        from_date=str(fixture["from_date"]),
        to_date=str(fixture["to_date"]),
        tickers=list(fixture["tickers"]),
        writer=writer,
    )

    payload = build_layer1_audit_dashboard_ui_payload(report)

    assert payload["meta"]["qa_scope"] == "Layer 0 and Layer 1 only"
    assert payload["report"]["rows_loaded"] == 6
    assert payload["controls"]["available_tickers"] == ["AAPL", "MSFT"]
    assert payload["controls"]["default_focus_date"] == "2024-05-08"
    assert payload["controls"]["default_spot_check_feature"] == "returns_1d"

    heatmap_rows = {
        item["feature_name"]: item
        for item in payload["heatmap"]["rows"]
    }
    assert len(payload["heatmap"]["columns"]) == 6
    assert len(heatmap_rows["returns_1d"]["cells"]) == 6
    assert heatmap_rows["returns_1d"]["cells"][-1]["status"] == "pass"

    family_panels = {
        item["family"]: item
        for item in payload["family_panels"]
    }
    assert family_panels["market"]["status"] == "fail"
    assert family_panels["nlp_topic"]["status"] == "warn"

    spot_series = {
        (item["feature_name"], item["ticker"]): item
        for item in payload["spot_checks"]["series"]
    }
    assert spot_series[("returns_1d", "AAPL")]["fail_count"] == 1
    assert spot_series[("returns_1d", "MSFT")]["warn_count"] == 3

    outlier_points = payload["outliers"]["points"]
    assert any(item["feature_name"] == "rsi_14" for item in outlier_points)
    assert any(item["rule_type"] == "distribution_outlier" for item in outlier_points)


def test_build_layer1_audit_dashboard_ui_payload_accepts_mapping_reports(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The payload builder should accept a mapping as well as a report object."""
    writer = local_writer(tmp_path, monkeypatch)
    fixture = seed_layer1_dashboard_fixture(writer)
    report = build_layer1_audit_dashboard_report(
        run_id="dashboard-ui-mapping",
        from_date=str(fixture["from_date"]),
        to_date=str(fixture["to_date"]),
        tickers=list(fixture["tickers"]),
        writer=writer,
    )

    payload = build_layer1_audit_dashboard_ui_payload(report.to_dict())

    assert payload["report"]["run_id"] == "dashboard-ui-mapping"
    assert payload["controls"]["available_outlier_features"] == ["returns_1d", "rsi_14"]


def test_build_layer1_audit_dashboard_ui_payload_handles_empty_report() -> None:
    """The payload builder should return empty UI sections for an empty backend report."""
    empty_report = {
        "run_id": "empty",
        "from_date": "2024-01-01",
        "to_date": "2024-01-01",
        "tickers": [],
        "rows_loaded": 0,
        "catalog_feature_count": 0,
        "generated_at": "",
        "encountered_unknown_features": [],
        "summary": {},
        "load_warnings": [],
        "selection_rows": [],
        "feature_null_summaries": [],
        "family_status_summaries": [],
        "heatmap_cells": [],
        "outlier_records": [],
        "spot_check_records": [],
        "formula_audit_cards": [],
        "family_definitions": [],
    }

    payload = build_layer1_audit_dashboard_ui_payload(empty_report)

    assert payload["report"]["rows_loaded"] == 0
    assert payload["controls"]["available_tickers"] == []
    assert payload["heatmap"]["rows"] == []
    assert payload["spot_checks"]["series"] == []
    assert payload["formula_cards"] == []
    assert payload["outliers"]["points"] == []


def test_build_layer1_audit_dashboard_ui_payload_defaults_missing_sections() -> None:
    """The payload builder should tolerate sparse report mappings."""
    payload = build_layer1_audit_dashboard_ui_payload(
        {
            "run_id": "sparse",
            "from_date": "2024-01-01",
            "to_date": "2024-01-01",
        }
    )

    assert payload["report"]["run_id"] == "sparse"
    assert payload["report"]["rows_loaded"] == 0
    assert payload["controls"]["available_dates"] == []
    assert payload["controls"]["available_features"] == []
    assert payload["family_panels"] == []
    assert payload["null_rates"]["by_feature"] == []


def test_build_layer1_audit_dashboard_ui_payload_sanitizes_non_finite_values() -> None:
    """The payload builder should drop NaN and infinite values from numeric UI fields."""
    payload = build_layer1_audit_dashboard_ui_payload(
        {
            "run_id": "non-finite",
            "from_date": "2024-01-01",
            "to_date": "2024-01-01",
            "selection_rows": [
                {
                    "row_key": "2024-01-01:AAPL",
                    "date": "2024-01-01",
                    "ticker": "AAPL",
                    "feature_count": 1,
                }
            ],
            "feature_null_summaries": [
                {
                    "feature_name": "returns_1d",
                    "family": "market",
                    "family_label": "Market",
                    "status": "warn",
                    "required": True,
                    "nullable": False,
                    "missing_rate": math.nan,
                    "null_rate": math.inf,
                    "invalid_rate": -math.inf,
                    "missing_count": 0,
                    "null_count": 0,
                    "invalid_count": 1,
                    "records_evaluated": 1,
                }
            ],
            "spot_check_records": [
                {
                    "feature_name": "returns_1d",
                    "ticker": "AAPL",
                    "date": "2024-01-01",
                    "row_key": "2024-01-01:AAPL",
                    "status": "fail",
                    "stored_value": math.nan,
                    "expected_value": math.inf,
                    "absolute_difference": -math.inf,
                    "relative_difference": "nan",
                }
            ],
            "formula_audit_cards": [
                {
                    "row_key": "2024-01-01:AAPL",
                    "date": "2024-01-01",
                    "ticker": "AAPL",
                    "feature_name": "returns_1d",
                    "status": "fail",
                    "title": "Returns 1D",
                    "formula": "close_t / close_t-1 - 1",
                    "calculation": "nan / inf - 1",
                    "point_in_time_note": "Only prior bars should be used.",
                    "expected_value": math.nan,
                    "stored_value": math.inf,
                }
            ],
            "outlier_records": [
                {
                    "row_key": "2024-01-01:AAPL",
                    "date": "2024-01-01",
                    "ticker": "AAPL",
                    "feature_name": "returns_1d",
                    "family": "market",
                    "family_label": "Market",
                    "rule_type": "range_violation",
                    "status": "fail",
                    "value": math.nan,
                    "lower_bound": -math.inf,
                    "upper_bound": math.inf,
                    "message": "Non-finite value",
                }
            ],
        }
    )

    feature_row = payload["heatmap"]["rows"][0]
    spot_point = payload["spot_checks"]["series"][0]["points"][0]
    formula_card = payload["formula_cards"][0]
    outlier_point = payload["outliers"]["points"][0]

    assert feature_row["missing_rate"] == 0.0
    assert feature_row["null_rate"] == 0.0
    assert feature_row["invalid_rate"] == 0.0
    assert spot_point["stored_value"] is None
    assert spot_point["expected_value"] is None
    assert spot_point["absolute_difference"] is None
    assert spot_point["relative_difference"] is None
    assert formula_card["stored_value"] is None
    assert formula_card["expected_value"] is None
    assert outlier_point["lower_bound"] is None
    assert outlier_point["upper_bound"] is None


def test_render_dashboard_html_keeps_original_point_indexes_in_line_paths() -> None:
    """The rendered dashboard HTML should preserve original point indexes in chart paths."""
    html = _render_dashboard_html(
        _DashboardDefaults(
            from_date="2024-01-01",
            to_date="2024-01-02",
            tickers=("AAPL",),
            host="127.0.0.1",
            port=8765,
        )
    )

    assert "function linePath(points, valueKey, x, y)" in html
    assert 'const storedPath = linePath(points, "stored_value", x, y);' in html
    assert 'const expectedPath = linePath(points, "expected_value", x, y);' in html
    assert "${y(point.expected_value)})" not in html
    assert "const escapeHtml = (value) => String(value ?? \"\")" in html
