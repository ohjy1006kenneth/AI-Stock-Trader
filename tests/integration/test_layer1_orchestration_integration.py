from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from app.lab.data_pipelines.run_daily_layer1 import Layer1DailyConfig, run_daily_layer1
from core.features.io import read_feature_records
from services.r2.paths import layer1_ticker_history_path
from tests.fixtures.layer1_support import (
    fake_news_runner,
    fake_regime_runner,
    fake_sentiment_runner,
    fake_topic_runner,
    local_writer,
    seed_layer0_archives,
)


def test_layer1_orchestration_runs_end_to_end_with_filtered_universe_scope(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The orchestration flow derives scope from universe masks and respects ticker filters."""
    writer = local_writer(tmp_path, monkeypatch)
    seed_layer0_archives(
        writer,
        dates=["2024-01-03", "2024-01-04"],
        tickers=["AAPL", "MSFT"],
        layer0_run_ids=("layer1-daily",),
    )

    result = run_daily_layer1(
        Layer1DailyConfig(
            run_id="layer1-integration",
            from_date="2024-01-03",
            to_date="2024-01-04",
            layer0_run_id="layer1-daily",
            tickers=("AAPL",),
        ),
        writer=writer,
        news_runner=fake_news_runner(writer, ["AAPL"]),
        text_topic_runner=fake_topic_runner(writer, ["AAPL"]),
        finbert_runner=fake_sentiment_runner(writer, ["AAPL"]),
        regime_runner=fake_regime_runner(writer),
        validation_output_dir=tmp_path / "reports",
        now=datetime(2024, 1, 5, 12, 0, tzinfo=UTC),
    )

    history = read_feature_records("AAPL", writer=writer)

    assert result.ready_for_layer2 is True
    assert result.tickers_processed == 1
    assert [record.date for record in history] == ["2024-01-03", "2024-01-04"]
    assert writer.exists(layer1_ticker_history_path("AAPL")) is True
    assert writer.exists(layer1_ticker_history_path("MSFT")) is False
