from __future__ import annotations

from pathlib import Path

import pytest

from core.features.audit import audit_layer1_features
from tests.fixtures.layer1_audit_support import local_writer, seed_layer1_audit_fixture


def test_layer1_feature_audit_passes_on_seeded_local_archives(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The audit recomputes deterministic branches from local Layer 0/1 archives."""
    writer = local_writer(tmp_path, monkeypatch)
    fixture = seed_layer1_audit_fixture(writer)

    report = audit_layer1_features(
        run_id="audit-integration",
        as_of_date=str(fixture["as_of_date"]),
        tickers=[str(fixture["ticker"])],
        benchmark_ticker=str(fixture["benchmark_ticker"]),
        writer=writer,
    )

    assert report.summary["fail"] == 0
    assert report.summary["warn"] == 0
    assert all(result["status"] == "pass" for result in report.branch_results)
    assert any(
        finding["category"] == "news" and finding["status"] == "pass"
        for finding in report.findings
    )
    assert any(
        finding["category"] == "leakage" and finding["status"] == "pass"
        for finding in report.findings
    )
