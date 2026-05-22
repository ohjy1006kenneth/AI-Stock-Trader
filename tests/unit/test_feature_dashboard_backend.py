from __future__ import annotations

from pathlib import Path

import pytest

from core.features.dashboard_backend import (
    build_layer1_audit_dashboard_report,
    render_layer1_audit_dashboard_summary,
    write_layer1_audit_dashboard_report,
)
from core.features.io import feature_records_to_parquet_bytes, read_feature_records
from services.r2.paths import layer1_ticker_history_path
from tests.fixtures.layer1_audit_support import local_writer
from tests.fixtures.layer1_dashboard_support import seed_layer1_dashboard_fixture


def test_build_layer1_audit_dashboard_report_builds_visualization_inputs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Dashboard backend emits visualization payloads including market spot checks."""
    writer = local_writer(tmp_path, monkeypatch)
    fixture = seed_layer1_dashboard_fixture(writer)

    report = build_layer1_audit_dashboard_report(
        run_id="dashboard-unit",
        from_date=str(fixture["from_date"]),
        to_date=str(fixture["to_date"]),
        tickers=list(fixture["tickers"]),
        writer=writer,
    )

    assert report.rows_loaded == 6
    assert report.summary["coverage_date_pass_count"] == 3
    assert report.summary["coverage_date_warn_count"] == 0
    assert "coverage_date_fail_count" not in report.summary
    assert report.summary["coverage_ticker_pass_count"] == 2
    assert report.summary["coverage_ticker_warn_count"] == 0
    assert report.summary["family_fail_count"] >= 1
    assert report.summary["outlier_count"] >= 2
    assert len(report.heatmap_cells) == report.rows_loaded * report.catalog_feature_count
    assert len(report.spot_check_records) == report.rows_loaded * 5
    assert len(report.formula_audit_cards) == len(report.spot_check_records)
    assert report.summary["spot_check_pass_count"] == 14
    assert report.summary["spot_check_warn_count"] == 15
    assert report.summary["spot_check_fail_count"] == 1

    family_by_key = {
        item["family"]: item
        for item in report.family_status_summaries
    }
    coverage_by_date = {item["date"]: item for item in report.coverage_by_date}
    coverage_by_ticker = {item["ticker"]: item for item in report.coverage_by_ticker}
    assert coverage_by_date["2024-05-06"]["status"] == "pass"
    assert coverage_by_date["2024-05-06"]["missing_tickers"] == []
    assert coverage_by_date["2024-05-07"]["status"] == "pass"
    assert coverage_by_ticker["AAPL"]["status"] == "pass"
    assert coverage_by_ticker["MSFT"]["status"] == "pass"
    assert coverage_by_ticker["MSFT"]["missing_dates"] == []
    assert family_by_key["market"]["status"] == "fail"
    assert family_by_key["nlp_topic"]["status"] == "warn"
    assert family_by_key["macro_context"]["invalid_count"] == 0
    assert family_by_key["regime"]["status"] == "pass"

    feature_by_name = {
        item["feature_name"]: item
        for item in report.feature_null_summaries
    }
    assert feature_by_name["beta_60d"]["missing_count"] == 1
    assert feature_by_name["beta_60d"]["status"] == "fail"
    assert feature_by_name["nlp_sentiment_score"]["null_count"] == 1
    assert feature_by_name["nlp_sentiment_score"]["status"] == "warn"

    outlier_keys = {
        (item["feature_name"], item["rule_type"])
        for item in report.outlier_records
    }
    assert ("rsi_14", "range_violation") in outlier_keys
    assert ("returns_1d", "distribution_outlier") in outlier_keys

    spot_checks = {
        (item["ticker"], item["date"], item["feature_name"]): item
        for item in report.spot_check_records
    }
    assert spot_checks[("AAPL", "2024-05-08", "returns_1d")]["status"] == "fail"
    assert spot_checks[("MSFT", "2024-05-06", "returns_1d")]["status"] == "warn"
    assert (
        spot_checks[("MSFT", "2024-05-06", "returns_1d")]["missing_reason"]
        == "Raw Layer 0 OHLCV archive is missing for this ticker."
    )

    formula_cards = {
        (item["ticker"], item["date"], item["feature_name"]): item
        for item in report.formula_audit_cards
    }
    assert "adj_close" in formula_cards[("AAPL", "2024-05-06", "returns_1d")]["calculation"]
    assert (
        formula_cards[("MSFT", "2024-05-06", "returns_1d")]["status"] == "warn"
    )


def test_write_layer1_audit_dashboard_report_writes_json_and_summary(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Dashboard reports persist machine-readable and operator-readable artifacts."""
    writer = local_writer(tmp_path, monkeypatch)
    fixture = seed_layer1_dashboard_fixture(writer)
    report = build_layer1_audit_dashboard_report(
        run_id="dashboard-write",
        from_date=str(fixture["from_date"]),
        to_date=str(fixture["to_date"]),
        tickers=list(fixture["tickers"]),
        writer=writer,
    )

    output_paths = write_layer1_audit_dashboard_report(report, output_dir=tmp_path / "reports")
    summary = render_layer1_audit_dashboard_summary(report)

    assert output_paths.json_path.exists()
    assert output_paths.summary_path.exists()
    assert '"run_id": "dashboard-write"' in output_paths.json_path.read_text(encoding="utf-8")
    assert "Layer 1 Audit Dashboard Backend" in summary
    assert "Family Status" in output_paths.summary_path.read_text(encoding="utf-8")
    assert "Market spot checks" in summary


def test_build_layer1_audit_dashboard_report_surfaces_partial_coverage(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Coverage summaries flag missing ticker/date rows in the selected window."""
    writer = local_writer(tmp_path, monkeypatch)
    fixture = seed_layer1_dashboard_fixture(writer)
    msft_history = read_feature_records("MSFT", writer=writer)
    writer.put_object(
        layer1_ticker_history_path("MSFT"),
        feature_records_to_parquet_bytes(msft_history[1:]),
    )

    report = build_layer1_audit_dashboard_report(
        run_id="dashboard-partial-coverage",
        from_date=str(fixture["from_date"]),
        to_date=str(fixture["to_date"]),
        tickers=list(fixture["tickers"]),
        writer=writer,
    )
    summary = render_layer1_audit_dashboard_summary(report)

    coverage_by_date = {item["date"]: item for item in report.coverage_by_date}
    coverage_by_ticker = {item["ticker"]: item for item in report.coverage_by_ticker}

    assert report.summary["coverage_date_warn_count"] == 1
    assert "coverage_date_fail_count" not in report.summary
    assert report.summary["coverage_ticker_warn_count"] == 1
    assert coverage_by_date["2024-05-06"]["status"] == "warn"
    assert coverage_by_date["2024-05-06"]["missing_tickers"] == ["MSFT"]
    assert coverage_by_ticker["MSFT"]["status"] == "warn"
    assert coverage_by_ticker["MSFT"]["missing_dates"] == ["2024-05-06"]
    assert "dates PASS=2 WARN=1; tickers PASS=1 WARN=1 FAIL=0" in summary
    assert "Partial Date Coverage" in summary
    assert "Partial Ticker Coverage" in summary
