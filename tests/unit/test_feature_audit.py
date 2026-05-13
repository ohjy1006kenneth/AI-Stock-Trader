from __future__ import annotations

from pathlib import Path

import pytest

from core.features.audit import (
    audit_layer1_features,
    render_audit_summary,
    write_audit_report,
)
from core.features.io import feature_records_to_parquet_bytes
from services.r2.paths import layer1_ticker_history_path
from tests.fixtures.layer1_audit_support import local_writer, seed_layer1_audit_fixture


def test_write_audit_report_writes_json_and_summary(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Audit reports persist both machine-readable and operator-readable outputs."""
    writer = local_writer(tmp_path, monkeypatch)
    fixture = seed_layer1_audit_fixture(writer)

    report = audit_layer1_features(
        run_id="audit-unit",
        as_of_date=str(fixture["as_of_date"]),
        tickers=[str(fixture["ticker"])],
        benchmark_ticker=str(fixture["benchmark_ticker"]),
        writer=writer,
    )
    output_paths = write_audit_report(report, output_dir=tmp_path / "reports")
    summary = render_audit_summary(report)

    assert output_paths.json_path.exists()
    assert output_paths.summary_path.exists()
    assert '"run_id": "audit-unit"' in output_paths.json_path.read_text(encoding="utf-8")
    assert "Layer 1 Feature Audit" in summary
    assert "AAPL" in summary


def test_audit_layer1_features_flags_history_mismatch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Stored history drift is surfaced as a failing branch comparison."""
    writer = local_writer(tmp_path, monkeypatch)
    fixture = seed_layer1_audit_fixture(writer)
    history_record = fixture["history_record"].model_copy(
        update={
            "features": {
                **fixture["history_record"].features,
                "returns_1d": 9.99,
            }
        }
    )
    writer.put_object(
        layer1_ticker_history_path(str(fixture["ticker"])),
        feature_records_to_parquet_bytes([history_record]),
    )

    report = audit_layer1_features(
        run_id="audit-mismatch",
        as_of_date=str(fixture["as_of_date"]),
        tickers=[str(fixture["ticker"])],
        benchmark_ticker=str(fixture["benchmark_ticker"]),
        writer=writer,
    )

    assert report.summary["fail"] > 0
    market_result = next(
        result for result in report.branch_results if result["branch"] == "market"
    )
    assert market_result["status"] == "fail"
    assert any("returns_1d" in mismatch for mismatch in market_result["mismatches"])
