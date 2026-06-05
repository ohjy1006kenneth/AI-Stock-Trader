from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from app.lab.data_pipelines import run_aapl_layer1_accuracy as aapl_runner
from app.lab.data_pipelines.run_aapl_layer1_accuracy import parse_args
from app.lab.data_pipelines.run_daily_layer1 import Layer1DailyConfig, run_daily_layer1
from core.features.aapl_accuracy import (
    AAPLFeatureAccuracyConfig,
    AAPLQualityThresholds,
    MarketParameterCandidate,
    build_aapl_feature_accuracy_report,
    load_aapl_feature_accuracy_config,
    render_aapl_feature_accuracy_report,
    write_aapl_feature_accuracy_report,
)
from services.r2.paths import layer1_aapl_accuracy_report_path, raw_price_path
from tests.fixtures.layer1_support import (
    fake_news_runner,
    fake_regime_runner,
    fake_sentiment_runner,
    fake_topic_runner,
    local_writer,
    seed_layer0_archives,
)


def test_load_aapl_feature_accuracy_config_requires_aapl_scope(tmp_path: Path) -> None:
    """The AAPL accuracy config is loaded from JSON and rejects non-AAPL scope."""
    config_path = tmp_path / "config.json"
    config_path.write_text(
        json.dumps(
            {
                "ticker": "AAPL",
                "benchmark_ticker": "SPY",
                "target_horizon_days": 5,
                "quality_thresholds": {
                    "min_feature_rows": 2,
                    "max_required_feature_null_rate": 1.0,
                    "min_label_pairs": 2,
                    "min_abs_best_candidate_correlation": 0.0,
                },
                "market_parameter_candidates": [
                    {
                        "name": "candidate",
                        "return_window_days": 5,
                        "volatility_window_days": 5,
                        "volume_window_days": 5,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    config = load_aapl_feature_accuracy_config(config_path)

    assert config.ticker == "AAPL"
    assert config.quality_thresholds.min_feature_rows == 2
    assert config.market_parameter_candidates[0].name == "candidate"

    config_path.write_text(
        json.dumps(
            {
                "ticker": "MSFT",
                "market_parameter_candidates": [
                    {
                        "name": "candidate",
                        "return_window_days": 5,
                        "volatility_window_days": 5,
                        "volume_window_days": 5,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="ticker=AAPL"):
        load_aapl_feature_accuracy_config(config_path)


def test_build_aapl_feature_accuracy_report_validates_date_first_pilot(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An AAPL-only Layer 1 pilot produces a durable diagnostic report."""
    writer = local_writer(tmp_path, monkeypatch)
    seed_layer0_archives(
        writer,
        dates=["2024-01-03", "2024-01-04", "2024-01-05", "2024-01-08"],
        tickers=["AAPL"],
        layer0_run_ids=("layer1-aapl-accuracy",),
    )
    run_daily_layer1(
        Layer1DailyConfig(
            run_id="layer1-aapl-accuracy",
            from_date="2024-01-03",
            to_date="2024-01-08",
            tickers=("AAPL",),
        ),
        writer=writer,
        news_runner=fake_news_runner(writer, ["AAPL"]),
        text_topic_runner=fake_topic_runner(writer, ["AAPL"]),
        finbert_runner=fake_sentiment_runner(writer, ["AAPL"]),
        regime_runner=fake_regime_runner(writer),
        validation_output_dir=tmp_path / "reports",
        now=datetime(2024, 1, 9, 12, 0, tzinfo=UTC),
    )
    config = AAPLFeatureAccuracyConfig(
        quality_thresholds=AAPLQualityThresholds(
            min_feature_rows=4,
            max_required_feature_null_rate=1.0,
            min_label_pairs=1,
            min_abs_best_candidate_correlation=0.0,
        ),
        market_parameter_candidates=(
            MarketParameterCandidate(
                name="short",
                return_window_days=5,
                volatility_window_days=5,
                volume_window_days=5,
            ),
        ),
    )

    report = build_aapl_feature_accuracy_report(
        run_id="layer1-aapl-accuracy",
        from_date="2024-01-03",
        to_date="2024-01-08",
        layer0_run_id="layer1-aapl-accuracy",
        config=config,
        writer=writer,
        now=datetime(2024, 1, 9, 13, 0, tzinfo=UTC),
    )
    local_path = write_aapl_feature_accuracy_report(report, output_dir=tmp_path / "out")
    payload = json.loads(render_aapl_feature_accuracy_report(report))

    assert report.report_key == layer1_aapl_accuracy_report_path(
        "layer1-aapl-accuracy",
        "2024-01-03",
        "2024-01-08",
    )
    assert writer.exists(report.report_key) is True
    assert local_path.exists()
    assert report.output_paths["missing_feature_keys"] == []
    assert payload["feature_quality"]["feature_rows"] == 4
    assert payload["acceptance"]["accepted"] is True
    assert payload["recommendation_for_issue_202"] == "proceed"
    assert payload["output_paths"]["feature_output_key_examples"][0] == (
        "features/2024-01-03/AAPL.parquet"
    )


def test_build_aapl_feature_accuracy_report_blocks_when_date_first_shard_missing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Missing AAPL date-first shards keep the #202 recommendation blocked."""
    writer = local_writer(tmp_path, monkeypatch)
    seed_layer0_archives(
        writer,
        dates=["2024-01-03", "2024-01-04"],
        tickers=["AAPL"],
        layer0_run_ids=("layer1-aapl-missing",),
    )
    config = AAPLFeatureAccuracyConfig(
        quality_thresholds=AAPLQualityThresholds(
            min_feature_rows=1,
            max_required_feature_null_rate=1.0,
            min_label_pairs=1,
        ),
    )

    report = build_aapl_feature_accuracy_report(
        run_id="layer1-aapl-missing",
        from_date="2024-01-03",
        to_date="2024-01-04",
        layer0_run_id="layer1-aapl-missing",
        config=config,
        writer=writer,
    )

    assert report.acceptance["accepted"] is False
    assert report.output_paths["missing_feature_keys"] == [
        "features/2024-01-03/AAPL.parquet",
        "features/2024-01-04/AAPL.parquet",
    ]
    assert report.recommendation_for_issue_202 == "do_not_proceed"


def test_build_aapl_feature_accuracy_report_blocks_when_raw_price_data_missing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Missing raw AAPL prices produce report evidence instead of an object-read crash."""
    writer = local_writer(tmp_path, monkeypatch)
    seed_layer0_archives(
        writer,
        dates=["2024-01-03", "2024-01-04", "2024-01-05", "2024-01-08"],
        tickers=["AAPL"],
        layer0_run_ids=("layer1-aapl-no-price",),
    )
    run_daily_layer1(
        Layer1DailyConfig(
            run_id="layer1-aapl-no-price",
            from_date="2024-01-03",
            to_date="2024-01-08",
            tickers=("AAPL",),
        ),
        writer=writer,
        news_runner=fake_news_runner(writer, ["AAPL"]),
        text_topic_runner=fake_topic_runner(writer, ["AAPL"]),
        finbert_runner=fake_sentiment_runner(writer, ["AAPL"]),
        regime_runner=fake_regime_runner(writer),
        validation_output_dir=tmp_path / "reports",
        now=datetime(2024, 1, 9, 12, 0, tzinfo=UTC),
    )
    writer.delete_object(raw_price_path("AAPL"))
    config = AAPLFeatureAccuracyConfig(
        quality_thresholds=AAPLQualityThresholds(
            min_feature_rows=4,
            max_required_feature_null_rate=1.0,
            min_label_pairs=1,
            min_abs_best_candidate_correlation=0.0,
        ),
        market_parameter_candidates=(
            MarketParameterCandidate(
                name="short",
                return_window_days=5,
                volatility_window_days=5,
                volume_window_days=5,
            ),
        ),
    )

    report = build_aapl_feature_accuracy_report(
        run_id="layer1-aapl-no-price",
        from_date="2024-01-03",
        to_date="2024-01-08",
        layer0_run_id="layer1-aapl-no-price",
        config=config,
        writer=writer,
    )

    assert writer.exists(report.report_key) is True
    assert report.input_evidence["raw_price_key"] == "raw/prices/AAPL.parquet"
    assert report.input_evidence["raw_price_status"] == "missing"
    assert report.input_evidence["raw_price_rows"] == 0
    assert report.output_paths["missing_feature_keys"] == []
    assert report.optimization_results == [
        {
            "name": "short",
            "status": "insufficient_data",
            "label_pairs": 0,
            "abs_correlation_score": None,
            "parameters": {
                "name": "short",
                "return_window_days": 5,
                "volatility_window_days": 5,
                "volume_window_days": 5,
            },
        }
    ]
    assert report.acceptance["checks"]["has_raw_price_data"] is False
    assert report.acceptance["accepted"] is False
    assert report.recommendation_for_issue_202 == "do_not_proceed"


def test_parse_args_rejects_non_aapl_ticker() -> None:
    """The CLI cannot be widened into the broad multi-ticker backfill."""
    with pytest.raises(SystemExit):
        parse_args(
            [
                "--run-id",
                "layer1-aapl",
                "--ticker",
                "MSFT",
                "--from-date",
                "2024-01-03",
                "--to-date",
                "2024-01-04",
            ]
        )


def test_aapl_run_layer1_helper_submits_scoped_modal_range(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The AAPL Layer 1 pilot delegates date ranges to Modal with an AAPL-only scope."""
    recorded: list[dict[str, object]] = []

    monkeypatch.setattr(
        aapl_runner,
        "modal_range_main",
        lambda **kwargs: recorded.append(kwargs) or {"manifest_key": "manifest"},
    )
    monkeypatch.setattr(
        aapl_runner,
        "modal_main",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("unexpected single-date run")),
    )

    result = aapl_runner._run_scoped_layer1_on_modal(
        run_id="layer1-aapl",
        from_date="2024-01-03",
        to_date="2024-01-05",
        layer0_run_id="layer0-range",
        benchmark_ticker="SPY",
        allow_layer0_manifest_date_range=True,
        min_sentence_chars=3,
        hmm_train_start_date="2023-10-01",
        hmm_max_iterations=42,
        hmm_min_training_rows=12,
    )

    assert result == {"manifest_key": "manifest"}
    assert recorded == [
        {
            "run_id": "layer1-aapl",
            "from_date": "2024-01-03",
            "to_date": "2024-01-05",
            "layer0_run_id": "layer0-range",
            "tickers": ("AAPL",),
            "benchmark_ticker": "SPY",
            "allow_layer0_manifest_date_range": True,
            "min_sentence_chars": 3,
            "hmm_train_start_date": "2023-10-01",
            "hmm_max_iterations": 42,
            "hmm_min_training_rows": 12,
        }
    ]
