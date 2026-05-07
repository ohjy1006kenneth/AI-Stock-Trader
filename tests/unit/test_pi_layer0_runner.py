from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from app.pi.fetchers.layer0 import run_layer0_incremental
from core.contracts.schemas import RunStatus
from core.data.layer0_pipeline import Layer0PipelineResult


class _ArchiveConfig:
    def __init__(self, series_ids: tuple[str, ...]) -> None:
        self.series_ids = series_ids


def test_run_layer0_incremental_builds_daily_config_and_forwards_dependencies(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Pi Layer 0 runner loads archive config and delegates to the shared pipeline."""
    captured: dict[str, object] = {}
    writer = object()

    monkeypatch.setattr(
        "app.pi.fetchers.layer0.load_fred_archive_config",
        lambda path: _ArchiveConfig(("FEDFUNDS", "DGS10")),
    )
    monkeypatch.setattr(
        "app.pi.fetchers.layer0.AlpacaMarketDataConfig.from_env",
        lambda: "alpaca-config",
    )
    monkeypatch.setattr(
        "app.pi.fetchers.layer0.AlpacaMarketDataClient",
        lambda config: ("market-data", config),
    )
    monkeypatch.setattr(
        "app.pi.fetchers.layer0.AlpacaNewsClient",
        lambda config: ("news", config),
    )
    monkeypatch.setattr(
        "app.pi.fetchers.layer0.SimFinClientConfig.from_env",
        lambda: "simfin-config",
    )
    monkeypatch.setattr(
        "app.pi.fetchers.layer0.SimFinFundamentalsFetcher",
        lambda config: ("fundamentals", config),
    )
    monkeypatch.setattr(
        "app.pi.fetchers.layer0.FredClientConfig.from_env",
        lambda: "fred-config",
    )
    monkeypatch.setattr(
        "app.pi.fetchers.layer0.FredMacroFetcher",
        lambda config: ("macro", config),
    )

    def _run_daily_layer0_incremental(**kwargs) -> Layer0PipelineResult:
        captured.update(kwargs)
        return Layer0PipelineResult(
            run_id="layer0-daily-2026-04-06",
            manifest_key="artifacts/manifests/layer0/layer0-daily-2026-04-06.json",
            status=RunStatus.COMPLETED,
            output_keys=(),
            metadata={"mode": "daily_incremental"},
        )

    monkeypatch.setattr(
        "app.pi.fetchers.layer0.run_daily_layer0_incremental",
        _run_daily_layer0_incremental,
    )

    result = run_layer0_incremental(
        as_of_date=date(2026, 4, 6),
        tickers=("AAPL", "MSFT"),
        overwrite=True,
        run_id="layer0-daily-2026-04-06",
        news_limit=25,
        simfin_limit=250,
        fred_limit=125,
        config_path=Path("config/fred.json"),
        writer=writer,
    )

    assert result.run_id == "layer0-daily-2026-04-06"
    config = captured["config"]
    assert config.as_of_date == date(2026, 4, 6)
    assert tuple(config.tickers) == ("AAPL", "MSFT")
    assert tuple(config.fred_series_ids) == ("FEDFUNDS", "DGS10")
    assert config.overwrite is True
    assert config.run_id == "layer0-daily-2026-04-06"
    assert config.news_limit == 25
    assert config.simfin_limit == 250
    assert config.fred_limit == 125
    assert captured["live_price_fetcher"] == ("market-data", "alpaca-config")
    assert captured["news_fetcher"] == ("news", "alpaca-config")
    assert captured["fundamentals_fetcher"] == ("fundamentals", "simfin-config")
    assert captured["macro_fetcher"] == ("macro", "fred-config")
    assert captured["writer"] is writer
