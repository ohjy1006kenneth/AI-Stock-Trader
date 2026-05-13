from __future__ import annotations

from pathlib import Path

import pytest

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
