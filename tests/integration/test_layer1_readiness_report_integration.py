from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from app.lab.data_pipelines.validate_layer1_archive import load_universe_mapping_from_r2
from app.lab.data_pipelines.run_daily_layer1 import Layer1DailyConfig, run_daily_layer1
from core.contracts.schemas import FeatureRecord, PipelineManifestRecord, RunStatus
from core.features.io import read_feature_records, write_feature_records
from services.r2.paths import pipeline_manifest_path
from tests.fixtures.layer1_support import (
    fake_news_runner,
    fake_regime_runner,
    fake_sentiment_runner,
    fake_topic_runner,
    local_writer,
    seed_layer0_archives,
)


def test_layer1_readiness_report_accepts_historical_layer0_manifest_window(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A readiness run can validate one day inside a historical Layer 0 manifest window."""
    writer = local_writer(tmp_path, monkeypatch)
    seed_layer0_archives(
        writer,
        dates=["2024-01-03"],
        tickers=["AAPL"],
        include_layer0_manifest=False,
    )
    writer.put_object(
        pipeline_manifest_path("layer0", "layer0-history-window"),
        PipelineManifestRecord(
            run_id="layer0-history-window",
            stage="layer0",
            status=RunStatus.COMPLETED,
            started_at=datetime(2024, 1, 5, 20, 0, tzinfo=UTC),
            finished_at=datetime(2024, 1, 5, 20, 30, tzinfo=UTC),
            output_path="raw/",
            metadata={
                "from_date": "2024-01-01",
                "to_date": "2024-01-05",
            },
        ).model_dump_json(),
    )
    write_feature_records(
        [
            FeatureRecord(
                date="2024-01-02",
                ticker="AAPL",
                features={"returns_1d": 0.01},
            )
        ],
        writer=writer,
    )

    result = run_daily_layer1(
        Layer1DailyConfig(
            run_id="layer1-readiness-window",
            from_date="2024-01-03",
            to_date="2024-01-03",
            layer0_run_id="layer0-history-window",
            allow_layer0_manifest_date_range=True,
        ),
        writer=writer,
        news_runner=fake_news_runner(writer, ["AAPL"]),
        text_topic_runner=fake_topic_runner(writer, ["AAPL"]),
        finbert_runner=fake_sentiment_runner(writer, ["AAPL"]),
        regime_runner=fake_regime_runner(writer),
        validation_output_dir=tmp_path / "reports",
        now=datetime(2024, 1, 6, 12, 0, tzinfo=UTC),
    )

    report = json.loads(result.validation_report_path.read_text(encoding="utf-8"))
    history = read_feature_records("AAPL", writer=writer)

    assert result.ready_for_layer2 is True
    assert report["run_id"] == "layer1-readiness-window"
    assert report["requested_dates"] == ["2024-01-03"]
    assert report["universe_counts_by_date"] == {"2024-01-03": 1}
    assert report["present_rows"] == 1
    assert report["present_rows_by_ticker"] == {"AAPL": 1}
    assert report["missing_ticker_dates"] == {}
    assert report["output_prefixes"]["layer1_history"] == "features/layer1/"
    assert report["output_prefixes"]["regime_outputs"] == "features/layer1_5/regime/"
    assert report["leakage_spot_checks"][0]["status"] == "pass"
    assert [record.date for record in history] == ["2024-01-02", "2024-01-03"]


def test_load_universe_mapping_from_r2_reads_canonical_universe_masks(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The readiness validator can derive its universe directly from Layer 0 R2 archives."""
    writer = local_writer(tmp_path, monkeypatch)
    seed_layer0_archives(
        writer,
        dates=["2024-01-03", "2024-01-04"],
        tickers=["AAPL", "MSFT"],
        include_layer0_manifest=False,
    )

    universe = load_universe_mapping_from_r2(
        from_date="2024-01-03",
        to_date="2024-01-04",
        reader=writer,
        requested_tickers=("AAPL",),
    )

    assert universe == {
        "2024-01-03": ["AAPL"],
        "2024-01-04": ["AAPL"],
    }
