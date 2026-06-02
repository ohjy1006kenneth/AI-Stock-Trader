from __future__ import annotations

import io
import json
from pathlib import Path

import pandas as pd
import pytest

from core.features.audit import (
    audit_layer1_features,
    render_audit_summary,
    write_audit_report,
)
from core.features.io import feature_records_to_parquet_bytes
from services.r2.paths import layer1_regime_path, layer1_ticker_history_path, raw_news_path
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
    assert "Layer 1 Run ID: auto-select latest completed branch manifests" in summary
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


def test_audit_layer1_features_flags_invalid_regime_probabilities(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Regime audit fails when the stored artifact probabilities are incoherent."""
    writer = local_writer(tmp_path, monkeypatch)
    fixture = seed_layer1_audit_fixture(writer)
    regime_key = layer1_regime_path(str(fixture["as_of_date"]), "audit-regime")
    buffer = io.BytesIO()
    pd.DataFrame(
        [
            {
                "date": str(fixture["as_of_date"]),
                "regime_label": "bull",
                "regime_confidence": 0.7,
                "regime_prob_bear": 0.1,
                "regime_prob_sideways": 0.1,
                "regime_prob_bull": 0.8,
            }
        ]
    ).to_parquet(buffer, index=False)
    writer.put_object(
        regime_key,
        buffer.getvalue(),
    )
    history_record = fixture["history_record"].model_copy(
        update={
            "features": {
                **fixture["history_record"].features,
                "regime_confidence": 0.7,
            }
        }
    )
    writer.put_object(
        layer1_ticker_history_path(str(fixture["ticker"])),
        feature_records_to_parquet_bytes([history_record]),
    )

    report = audit_layer1_features(
        run_id="audit-bad-regime",
        as_of_date=str(fixture["as_of_date"]),
        tickers=[str(fixture["ticker"])],
        benchmark_ticker=str(fixture["benchmark_ticker"]),
        writer=writer,
    )

    assert report.summary["fail"] > 0
    assert any(
        finding["status"] == "fail"
        and finding["category"] == "regime"
        and "probabilities are not internally coherent" in finding["message"]
        for finding in report.findings
    )


def test_audit_layer1_features_uses_exact_layer1_run_id_for_regime_selection(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An explicit Layer 1 run id should resolve the matching per-date regime manifest."""
    writer = local_writer(tmp_path, monkeypatch)
    fixture = seed_layer1_audit_fixture(writer)
    as_of_date = str(fixture["as_of_date"])

    stale_regime_key = layer1_regime_path(as_of_date, "stale-layer1-2024-05-06")
    buffer = io.BytesIO()
    pd.DataFrame(
        [
            {
                "date": as_of_date,
                "regime_label": "bear",
                "regime_confidence": 0.9,
                "regime_prob_bear": 0.9,
                "regime_prob_sideways": 0.05,
                "regime_prob_bull": 0.05,
                "regime_required_for_layer2": True,
                "regime_readiness_status": "ready",
                "regime_readiness_reason": "ready",
                "regime_missing_features": "",
                "regime_probability_sum": 1.0,
            }
        ]
    ).to_parquet(buffer, index=False)
    writer.put_object(stale_regime_key, buffer.getvalue())
    writer.put_object(
        "artifacts/manifests/layer1_5_regime/stale-layer1-2024-05-06.json",
        json.dumps(
            {
                "run_id": "stale-layer1-2024-05-06",
                "stage": "layer1_5_regime",
                "status": "completed",
                "started_at": "2024-05-06T12:00:00Z",
                "finished_at": "2024-05-06T12:05:00Z",
                "input_path": "raw/prices/SPY.parquet,raw/macro/",
                "output_path": stale_regime_key,
                "metadata": {
                    "train_end_date": "2024-05-03",
                    "inference_dates": [as_of_date],
                    "regime_readiness_by_date": {
                        as_of_date: {
                            "status": "ready",
                            "reason": "ready",
                            "required_for_layer2": True,
                            "missing_features": [],
                            "probability_sum": 1.0,
                        }
                    },
                },
            }
        ),
    )

    exact_run_id = "layer1-current"
    exact_stage_run_id = f"{exact_run_id}-{as_of_date}"
    exact_regime_key = layer1_regime_path(as_of_date, exact_stage_run_id)
    writer.put_object(
        exact_regime_key,
        writer.get_object(layer1_regime_path(as_of_date, "audit-regime")),
    )
    writer.put_object(
        f"artifacts/manifests/layer1_5_regime/{exact_stage_run_id}.json",
        json.dumps(
            {
                "run_id": exact_stage_run_id,
                "stage": "layer1_5_regime",
                "status": "completed",
                "started_at": "2024-05-06T12:10:00Z",
                "finished_at": "2024-05-06T12:15:00Z",
                "input_path": "raw/prices/SPY.parquet,raw/macro/",
                "output_path": exact_regime_key,
                "metadata": {
                    "train_end_date": "2024-05-03",
                    "inference_dates": [as_of_date],
                    "regime_readiness_by_date": {
                        as_of_date: {
                            "status": "ready",
                            "reason": "ready",
                            "required_for_layer2": True,
                            "missing_features": [],
                            "probability_sum": 1.0,
                        }
                    },
                },
            }
        ),
    )

    report = audit_layer1_features(
        run_id="audit-exact-layer1-run",
        layer1_run_id=exact_run_id,
        as_of_date=as_of_date,
        tickers=[str(fixture["ticker"])],
        benchmark_ticker=str(fixture["benchmark_ticker"]),
        writer=writer,
    )

    assert report.layer1_run_id == exact_run_id
    regime_result = next(
        result for result in report.branch_results if result["branch"] == "regime"
    )
    assert regime_result["status"] == "pass"
    summary = render_audit_summary(report)
    assert f"Layer 1 Run ID: {exact_run_id}" in summary
