from __future__ import annotations

from pathlib import Path

import pytest

from core.features.audit import (
    audit_layer1_features,
    render_audit_summary,
    write_audit_report,
)
from core.features.io import feature_records_to_parquet_bytes
from services.r2.paths import layer1_ticker_history_path, raw_news_path
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


def test_audit_layer1_features_requires_non_empty_tickers(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The audit rejects empty ticker inputs before attempting archive reads."""
    writer = local_writer(tmp_path, monkeypatch)

    with pytest.raises(ValueError, match="tickers must contain at least one non-empty ticker"):
        audit_layer1_features(
            run_id="audit-empty",
            as_of_date="2024-05-06",
            tickers=["", " "],
            writer=writer,
        )


def test_audit_layer1_features_warns_when_raw_news_archive_missing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Missing Layer 0 news archives produce a warning instead of aborting the audit."""
    writer = local_writer(tmp_path, monkeypatch)
    fixture = seed_layer1_audit_fixture(writer)
    writer.delete_object(raw_news_path(str(fixture["as_of_date"])))

    report = audit_layer1_features(
        run_id="audit-missing-news",
        as_of_date=str(fixture["as_of_date"]),
        tickers=[str(fixture["ticker"])],
        benchmark_ticker=str(fixture["benchmark_ticker"]),
        writer=writer,
    )

    assert report.summary["fail"] == 0
    assert any(
        finding["status"] == "warn"
        and finding["category"] == "news"
        and "Raw Layer 0 news archive missing" in finding["message"]
        for finding in report.findings
    )


def test_audit_layer1_features_warns_and_skips_malformed_news_jsonl(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Malformed raw news JSONL rows are warned on and skipped during recomputation."""
    writer = local_writer(tmp_path, monkeypatch)
    fixture = seed_layer1_audit_fixture(writer)
    news_key = raw_news_path(str(fixture["as_of_date"]))
    writer.put_object(news_key, writer.get_object(news_key) + b"{malformed-json}\n")

    report = audit_layer1_features(
        run_id="audit-malformed-news",
        as_of_date=str(fixture["as_of_date"]),
        tickers=[str(fixture["ticker"])],
        benchmark_ticker=str(fixture["benchmark_ticker"]),
        writer=writer,
    )

    assert report.summary["fail"] == 0
    assert any(
        finding["status"] == "warn"
        and finding["category"] == "news"
        and "Skipped malformed JSON Lines rows" in finding["message"]
        for finding in report.findings
    )
    assert any(
        finding["status"] == "pass"
        and finding["category"] == "news"
        and "matches raw Layer 0 inputs" in finding["message"]
        for finding in report.findings
    )


def test_audit_layer1_features_flags_nan_feature_values(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """NaN values in stored feature histories fail catalog validation."""
    writer = local_writer(tmp_path, monkeypatch)
    fixture = seed_layer1_audit_fixture(writer)
    history_record = fixture["history_record"].model_copy(
        update={
            "features": {
                **fixture["history_record"].features,
                "returns_1d": float("nan"),
            }
        }
    )
    writer.put_object(
        layer1_ticker_history_path(str(fixture["ticker"])),
        feature_records_to_parquet_bytes([history_record]),
    )

    report = audit_layer1_features(
        run_id="audit-nan-history",
        as_of_date=str(fixture["as_of_date"]),
        tickers=[str(fixture["ticker"])],
        benchmark_ticker=str(fixture["benchmark_ticker"]),
        writer=writer,
    )

    assert report.summary["fail"] > 0
    assert report.catalog_summary["value_failures"] > 0
    assert any(
        finding["status"] == "fail"
        and finding["category"] == "catalog"
        and "returns_1d" in str(finding["details"])
        for finding in report.findings
    )
