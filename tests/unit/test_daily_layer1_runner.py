from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from app.lab.data_pipelines import run_daily_layer1 as daily_layer1_module
from app.lab.data_pipelines.run_daily_layer1 import (
    LAYER1_DAILY_STAGE,
    Layer1DailyConfig,
    Layer1ValidationError,
    _existing_finbert_runner,
    _existing_news_runner,
    _existing_regime_runner,
    _existing_text_topic_runner,
    load_modal_runtime_config,
    main,
    run_daily_layer1,
)
from app.lab.data_pipelines.run_finbert_sentiment import FINBERT_SENTIMENT_STAGE
from app.lab.data_pipelines.run_hmm_regime_detection import REGIME_STAGE
from app.lab.data_pipelines.run_news_preprocessing import NLP_PREPROCESSING_STAGE
from app.lab.data_pipelines.run_text_topics import TEXT_TOPICS_STAGE
from app.lab.data_pipelines.validate_layer1_archive import Layer1ValidationReport
from core.contracts.schemas import PipelineManifestRecord, RunStatus
from core.features.io import feature_records_to_parquet_bytes, read_feature_records
from services.r2.paths import (
    layer1_sentiment_feature_path,
    layer1_validation_report_path,
    pipeline_manifest_path,
)
from services.r2.writer import R2Writer
from tests.fixtures.layer1_support import (
    fake_news_runner,
    fake_regime_runner,
    fake_sentiment_runner,
    fake_topic_runner,
    local_writer,
    seed_layer0_archives,
)


def test_run_daily_layer1_happy_path_writes_history_and_completed_manifest(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The orchestrator writes completed manifests, daily shards, and ticker histories."""
    writer = local_writer(tmp_path, monkeypatch)
    seed_layer0_archives(
        writer,
        dates=["2024-01-03", "2024-01-04"],
        tickers=["AAPL"],
        layer0_run_ids=("layer1-daily",),
    )

    result = run_daily_layer1(
        Layer1DailyConfig(
            run_id="layer1-daily",
            from_date="2024-01-03",
            to_date="2024-01-04",
        ),
        writer=writer,
        news_runner=fake_news_runner(writer, ["AAPL"]),
        text_topic_runner=fake_topic_runner(writer, ["AAPL"]),
        finbert_runner=fake_sentiment_runner(writer, ["AAPL"]),
        regime_runner=fake_regime_runner(writer),
        validation_output_dir=tmp_path / "reports",
        now=datetime(2024, 1, 5, 12, 0, tzinfo=UTC),
    )

    manifest = PipelineManifestRecord.model_validate_json(writer.get_object(result.manifest_key))
    history = read_feature_records("AAPL", writer=writer)

    assert result.ready_for_layer2 is True
    assert result.history_files_written == 1
    assert result.feature_rows_written == 2
    assert history[0].date == "2024-01-03"
    assert history[1].date == "2024-01-04"
    assert history[0].features["nlp_topic_count"] == 1
    assert history[0].features["nlp_sentiment_score"] == pytest.approx(0.25)
    assert history[0].features["regime_label"] == "bull"
    assert writer.exists("features/layer1/2024-01-03/AAPL.parquet") is True
    assert writer.exists(
        layer1_validation_report_path("layer1-daily", "2024-01-03", "2024-01-04")
    ) is True
    assert manifest.stage == LAYER1_DAILY_STAGE
    assert manifest.status is RunStatus.COMPLETED
    assert manifest.metadata["ready_for_layer2"] is True
    assert Path(str(manifest.metadata["validation_report_path"])).exists()
    assert (
        manifest.metadata["validation_report_key"]
        == layer1_validation_report_path("layer1-daily", "2024-01-03", "2024-01-04")
    )
    report_payload = json.loads(writer.get_object(result.validation_report_key).decode("utf-8"))
    assert report_payload["manifest_status"] == "completed"
    assert report_payload["ready_for_layer2"] is True
    assert report_payload["related_manifests"] == [
        {
            "key": result.manifest_key,
            "run_id": "layer1-daily",
            "status": "completed",
            "finished_at": "2024-01-05T12:00:00Z",
        }
    ]


def test_run_daily_layer1_prefers_topic_sentence_count_when_sentiment_conflicts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Layer 1 assembly keeps the topic-owned sentence count when sentiment disagrees."""
    writer = local_writer(tmp_path, monkeypatch)
    seed_layer0_archives(
        writer,
        dates=["2024-01-03"],
        tickers=["AAPL"],
        layer0_run_ids=("layer1-conflicting-sentences",),
    )

    def conflicting_sentiment_runner(config, *, writer: R2Writer):
        output_key = layer1_sentiment_feature_path(config.as_of_date, config.run_id)
        records = [
            daily_layer1_module.FeatureRecord(
                date=config.as_of_date,
                ticker="AAPL",
                features={
                    "nlp_sentiment_score": 0.25,
                    "nlp_article_count": 1,
                    "nlp_sentence_count": 3,
                },
            )
        ]
        writer.put_object(output_key, feature_records_to_parquet_bytes(records))
        return daily_layer1_module.FinBERTPipelineResult(
            run_id=config.run_id,
            scored_news_key="unused",
            sentiment_feature_key=output_key,
            manifest_key=pipeline_manifest_path("layer1_finbert_sentiment", config.run_id),
            input_rows=3,
            scored_rows=3,
            feature_rows=1,
        )

    result = run_daily_layer1(
        Layer1DailyConfig(
            run_id="layer1-conflicting-sentences",
            from_date="2024-01-03",
            to_date="2024-01-03",
        ),
        writer=writer,
        news_runner=fake_news_runner(writer, ["AAPL"]),
        text_topic_runner=fake_topic_runner(writer, ["AAPL"]),
        finbert_runner=conflicting_sentiment_runner,
        regime_runner=fake_regime_runner(writer),
        validation_output_dir=tmp_path / "reports",
        now=datetime(2024, 1, 4, 12, 0, tzinfo=UTC),
    )

    history = read_feature_records("AAPL", writer=writer)

    assert result.ready_for_layer2 is True
    assert history[0].features["nlp_sentence_count"] == 1
    assert history[0].features["nlp_sentiment_score"] == pytest.approx(0.25)


def test_run_daily_layer1_computes_shared_macro_features_once(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Daily assembly reuses one market-wide macro frame across all tickers."""
    writer = local_writer(tmp_path, monkeypatch)
    seed_layer0_archives(
        writer,
        dates=["2024-01-01", "2024-01-02", "2024-01-03"],
        tickers=["AAPL", "MSFT"],
        layer0_run_ids=("layer1-shared-macro",),
    )
    original_compute_macro_features = daily_layer1_module.compute_macro_features
    call_count = 0
    target_dates_arg: list[str] = []

    def counting_compute_macro_features(macro, target_dates):
        nonlocal call_count
        call_count += 1
        target_dates_arg[:] = [str(value) for value in target_dates]
        return original_compute_macro_features(macro, target_dates)

    monkeypatch.setattr(
        daily_layer1_module,
        "compute_macro_features",
        counting_compute_macro_features,
    )

    result = run_daily_layer1(
        Layer1DailyConfig(
            run_id="layer1-shared-macro",
            from_date="2024-01-03",
            to_date="2024-01-03",
            allow_layer0_manifest_date_range=True,
        ),
        writer=writer,
        news_runner=fake_news_runner(writer, ["AAPL", "MSFT"]),
        text_topic_runner=fake_topic_runner(writer, ["AAPL", "MSFT"]),
        finbert_runner=fake_sentiment_runner(writer, ["AAPL", "MSFT"]),
        regime_runner=fake_regime_runner(writer),
        validation_output_dir=tmp_path / "reports",
        now=datetime(2024, 1, 4, 12, 0, tzinfo=UTC),
    )

    assert result.ready_for_layer2 is True
    assert call_count == 1
    assert "2024-01-03" in target_dates_arg
    assert len(target_dates_arg) > 1


def test_run_daily_layer1_single_date_manifest_contains_modal_wait_metadata(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Single-date runs keep the manifest metadata that the Pi wait loop expects."""
    writer = local_writer(tmp_path, monkeypatch)
    seed_layer0_archives(
        writer,
        dates=["2024-01-03"],
        tickers=["AAPL"],
        layer0_run_ids=("layer1-single-day",),
    )

    result = run_daily_layer1(
        Layer1DailyConfig(
            run_id="layer1-single-day",
            from_date="2024-01-03",
            to_date="2024-01-03",
        ),
        writer=writer,
        news_runner=fake_news_runner(writer, ["AAPL"]),
        text_topic_runner=fake_topic_runner(writer, ["AAPL"]),
        finbert_runner=fake_sentiment_runner(writer, ["AAPL"]),
        regime_runner=fake_regime_runner(writer),
        validation_output_dir=tmp_path / "reports",
        now=datetime(2024, 1, 4, 12, 0, tzinfo=UTC),
    )

    manifest = PipelineManifestRecord.model_validate_json(writer.get_object(result.manifest_key))

    assert manifest.metadata["as_of_date"] == "2024-01-03"
    assert manifest.metadata["layer0_run_id"] == "layer1-single-day"
    assert result.validation_report_key == layer1_validation_report_path(
        "layer1-single-day", "2024-01-03", "2024-01-03"
    )


def test_run_daily_layer1_fails_closed_when_layer0_manifest_is_missing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Missing Layer 0 manifest prevents any Layer 1 work from starting."""
    writer = local_writer(tmp_path, monkeypatch)
    seed_layer0_archives(
        writer,
        dates=["2024-01-03"],
        tickers=["AAPL"],
        include_layer0_manifest=False,
    )

    with pytest.raises(FileNotFoundError, match="Layer 0 manifest"):
        run_daily_layer1(
            Layer1DailyConfig(
                run_id="layer1-missing-manifest",
                from_date="2024-01-03",
                to_date="2024-01-03",
            ),
            writer=writer,
            news_runner=fake_news_runner(writer, ["AAPL"]),
            text_topic_runner=fake_topic_runner(writer, ["AAPL"]),
            finbert_runner=fake_sentiment_runner(writer, ["AAPL"]),
            regime_runner=fake_regime_runner(writer),
            validation_output_dir=tmp_path / "reports",
        )

    manifest = PipelineManifestRecord.model_validate_json(
        writer.get_object(pipeline_manifest_path(LAYER1_DAILY_STAGE, "layer1-missing-manifest"))
    )
    assert manifest.status is RunStatus.FAILED


def test_run_daily_layer1_writes_failed_manifest_on_branch_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Any branch exception bubbles up after the orchestrator records failure."""
    writer = local_writer(tmp_path, monkeypatch)
    seed_layer0_archives(
        writer,
        dates=["2024-01-03"],
        tickers=["AAPL"],
        layer0_run_ids=("layer1-branch-fail",),
    )

    def exploding_finbert(*args, **kwargs):
        raise RuntimeError("simulated finbert failure")

    with pytest.raises(RuntimeError, match="simulated finbert failure"):
        run_daily_layer1(
            Layer1DailyConfig(
                run_id="layer1-branch-fail",
                from_date="2024-01-03",
                to_date="2024-01-03",
            ),
            writer=writer,
            news_runner=fake_news_runner(writer, ["AAPL"]),
            text_topic_runner=fake_topic_runner(writer, ["AAPL"]),
            finbert_runner=exploding_finbert,
            regime_runner=fake_regime_runner(writer),
            validation_output_dir=tmp_path / "reports",
        )

    manifest = PipelineManifestRecord.model_validate_json(
        writer.get_object(pipeline_manifest_path(LAYER1_DAILY_STAGE, "layer1-branch-fail"))
    )
    assert manifest.status is RunStatus.FAILED
    assert manifest.metadata["error"]["type"] == "RuntimeError"


def test_run_daily_layer1_rerun_replaces_target_dates_without_duplicates(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Re-running the same dates is idempotent for the per-ticker history output."""
    writer = local_writer(tmp_path, monkeypatch)
    seed_layer0_archives(
        writer,
        dates=["2024-01-03", "2024-01-04"],
        tickers=["AAPL"],
        layer0_run_ids=("layer1-rerun",),
    )
    config = Layer1DailyConfig(
        run_id="layer1-rerun",
        from_date="2024-01-03",
        to_date="2024-01-04",
    )

    run_daily_layer1(
        config,
        writer=writer,
        news_runner=fake_news_runner(writer, ["AAPL"]),
        text_topic_runner=fake_topic_runner(writer, ["AAPL"]),
        finbert_runner=fake_sentiment_runner(writer, ["AAPL"]),
        regime_runner=fake_regime_runner(writer),
        validation_output_dir=tmp_path / "reports",
    )
    first_history = read_feature_records("AAPL", writer=writer)

    run_daily_layer1(
        config,
        writer=writer,
        news_runner=fake_news_runner(writer, ["AAPL"]),
        text_topic_runner=fake_topic_runner(writer, ["AAPL"]),
        finbert_runner=fake_sentiment_runner(writer, ["AAPL"]),
        regime_runner=fake_regime_runner(writer),
        validation_output_dir=tmp_path / "reports",
    )
    second_history = read_feature_records("AAPL", writer=writer)

    assert [record.date for record in first_history] == ["2024-01-03", "2024-01-04"]
    assert second_history == first_history


def test_run_daily_layer1_reuses_completed_branch_outputs_when_precomputed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The orchestrator can assemble histories from precomputed stage outputs."""
    writer = local_writer(tmp_path, monkeypatch)
    seed_layer0_archives(
        writer,
        dates=["2024-01-03"],
        tickers=["AAPL"],
        layer0_run_ids=("layer1-precomputed",),
    )
    stage_run_id = "layer1-precomputed-2024-01-03"
    news_result = fake_news_runner(writer, ["AAPL"])(
        daily_layer1_module.NewsPreprocessingPipelineConfig(
            run_id=stage_run_id,
            as_of_date="2024-01-03",
        ),
        writer=writer,
    )
    _write_completed_stage_manifest(
        writer=writer,
        stage=NLP_PREPROCESSING_STAGE,
        run_id=stage_run_id,
        output_path=news_result.output_key,
    )
    topic_result = fake_topic_runner(writer, ["AAPL"])(
        daily_layer1_module.TextTopicPipelineConfig(
            run_id=stage_run_id,
            as_of_date="2024-01-03",
            preprocessed_news_key=news_result.output_key,
        ),
        writer=writer,
    )
    _write_completed_stage_manifest(
        writer=writer,
        stage=TEXT_TOPICS_STAGE,
        run_id=stage_run_id,
        output_path=topic_result.topic_feature_key,
    )
    sentiment_result = fake_sentiment_runner(writer, ["AAPL"])(
        daily_layer1_module.FinBERTPipelineConfig(
            run_id=stage_run_id,
            as_of_date="2024-01-03",
            preprocessed_news_key=news_result.output_key,
        ),
        writer=writer,
    )
    _write_completed_stage_manifest(
        writer=writer,
        stage=FINBERT_SENTIMENT_STAGE,
        run_id=stage_run_id,
        output_path=sentiment_result.sentiment_feature_key,
    )
    regime_result = fake_regime_runner(writer)(
        daily_layer1_module.HMMRegimePipelineConfig(
            run_id=stage_run_id,
            train_start_date=None,
            train_end_date="2024-01-02",
            inference_dates=("2024-01-03",),
            benchmark_ticker="SPY",
            max_iterations=100,
            min_training_rows=30,
        ),
        writer=writer,
    )
    _write_completed_stage_manifest(
        writer=writer,
        stage=REGIME_STAGE,
        run_id=stage_run_id,
        output_path=regime_result.output_key,
    )

    result = run_daily_layer1(
        Layer1DailyConfig(
            run_id="layer1-precomputed",
            from_date="2024-01-03",
            to_date="2024-01-03",
        ),
        writer=writer,
        news_runner=_existing_news_runner({"2024-01-03": news_result.output_key}),
        text_topic_runner=_existing_text_topic_runner(
            {"2024-01-03": topic_result.topic_feature_key}
        ),
        finbert_runner=_existing_finbert_runner(
            {"2024-01-03": sentiment_result.sentiment_feature_key}
        ),
        regime_runner=_existing_regime_runner({"2024-01-03": regime_result.output_key}),
        validation_output_dir=tmp_path / "reports",
    )

    history = read_feature_records("AAPL", writer=writer)

    assert result.ready_for_layer2 is True
    assert result.history_files_written == 1
    assert history[0].features["nlp_topic_count"] == 1
    assert history[0].features["nlp_sentiment_score"] == pytest.approx(0.25)
    assert history[0].features["regime_label"] == "bull"


def test_run_daily_layer1_marks_stale_sibling_manifests_in_report_and_metadata(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Final readiness artifacts call out interrupted sibling runs without deleting them."""
    writer = local_writer(tmp_path, monkeypatch)
    seed_layer0_archives(
        writer,
        dates=["2024-01-03"],
        tickers=["AAPL"],
        layer0_run_ids=("layer1-readiness-2024-01-03-v7",),
    )
    stale_key = pipeline_manifest_path("layer1", "layer1-readiness-2024-01-03-v4")
    writer.put_object(
        stale_key,
        PipelineManifestRecord(
            run_id="layer1-readiness-2024-01-03-v4",
            stage=LAYER1_DAILY_STAGE,
            status=RunStatus.RUNNING,
            started_at=datetime(2024, 1, 3, 12, 0, tzinfo=UTC),
            output_path="features/layer1/",
        ).model_dump_json(),
    )

    result = run_daily_layer1(
        Layer1DailyConfig(
            run_id="layer1-readiness-2024-01-03-v7",
            from_date="2024-01-03",
            to_date="2024-01-03",
        ),
        writer=writer,
        news_runner=fake_news_runner(writer, ["AAPL"]),
        text_topic_runner=fake_topic_runner(writer, ["AAPL"]),
        finbert_runner=fake_sentiment_runner(writer, ["AAPL"]),
        regime_runner=fake_regime_runner(writer),
        validation_output_dir=tmp_path / "reports",
        now=datetime(2024, 1, 5, 12, 0, tzinfo=UTC),
    )

    manifest = PipelineManifestRecord.model_validate_json(writer.get_object(result.manifest_key))
    report_payload = json.loads(writer.get_object(result.validation_report_key).decode("utf-8"))

    assert manifest.status is RunStatus.COMPLETED
    assert manifest.metadata["stale_manifest_keys"] == [stale_key]
    assert manifest.metadata["supersedes_manifest_keys"] == [stale_key]
    assert stale_key in manifest.metadata["related_manifest_keys"]
    assert report_payload["manifest_status"] == "completed"
    assert report_payload["stale_manifest_keys"] == [stale_key]
    assert report_payload["ready_for_layer2"] is True


def test_main_returns_nonzero_when_validation_is_not_ready(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The CLI returns nonzero and logs the validation failure path explicitly."""
    report = Layer1ValidationReport(
        run_id="layer1-cli-fail",
        from_date="2024-01-03",
        to_date="2024-01-03",
        validation_status="failed",
        expected_ticker_files=1,
        present_ticker_files=0,
        expected_rows=1,
        present_rows=0,
        schema_failures=0,
        row_count_failures=0,
        ready_for_layer2=False,
    )
    error = Layer1ValidationError(report, tmp_path / "report.json")
    logged_messages: list[str] = []

    monkeypatch.setattr(
        daily_layer1_module,
        "run_daily_layer1",
        lambda *args, **kwargs: (_ for _ in ()).throw(error),
    )
    monkeypatch.setattr(
        daily_layer1_module.logger,
        "error",
        lambda message: logged_messages.append(message),
    )

    exit_code = main(
        [
            "--run-id",
            "layer1-cli-fail",
            "--from-date",
            "2024-01-03",
            "--validation-output-dir",
            str(tmp_path / "reports"),
        ]
    )

    assert exit_code == 1
    assert logged_messages == [str(error)]


def test_main_single_date_delegates_to_modal_orchestration_when_available(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Single-date CLI runs use the Modal orchestration path when the app is available."""
    recorded: list[dict[str, object]] = []

    monkeypatch.setattr(daily_layer1_module, "_modal_run_daily_layer1", object())
    monkeypatch.setattr(
        daily_layer1_module,
        "modal_main",
        lambda **kwargs: recorded.append(kwargs),
    )
    monkeypatch.setattr(
        daily_layer1_module,
        "run_daily_layer1",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("unexpected local run")),
    )

    exit_code = main(
        [
            "--run-id",
            "layer1-single-day",
            "--as-of-date",
            "2024-01-03",
            "--layer0-run-id",
            "layer0-daily-2024-01-03",
            "--benchmark-ticker",
            " spy ",
            "--min-sentence-chars",
            "7",
            "--hmm-train-start-date",
            "2023-11-01",
            "--hmm-max-iterations",
            "44",
            "--hmm-min-training-rows",
            "9",
            "--allow-layer0-manifest-date-range",
        ]
    )

    assert exit_code == 0
    assert recorded == [
        {
            "run_id": "layer1-single-day",
            "as_of_date": "2024-01-03",
            "layer0_run_id": "layer0-daily-2024-01-03",
            "benchmark_ticker": "SPY",
            "allow_layer0_manifest_date_range": True,
            "min_sentence_chars": 7,
            "hmm_train_start_date": "2023-11-01",
            "hmm_max_iterations": 44,
            "hmm_min_training_rows": 9,
        }
    ]


def test_existing_regime_runner_rejects_empty_inference_dates(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Precomputed regime reuse requires at least one requested inference date."""
    writer = local_writer(tmp_path, monkeypatch)
    runner = _existing_regime_runner({"2024-01-03": "features/layer1_5/regime/run.parquet"})

    with pytest.raises(ValueError, match="inference_dates must not be empty"):
        runner(
            daily_layer1_module.HMMRegimePipelineConfig(
                run_id="layer1-precomputed-2024-01-03",
                train_end_date="2024-01-02",
                inference_dates=(),
            ),
            writer=writer,
        )


def test_layer1_daily_config_rejects_invalid_dates() -> None:
    """The orchestrator config validates ISO dates and ordering."""
    with pytest.raises(ValueError, match="from_date"):
        Layer1DailyConfig(run_id="run", from_date="2024-1-3", to_date="2024-01-04")
    with pytest.raises(ValueError, match="from_date must be <="):
        Layer1DailyConfig(run_id="run", from_date="2024-01-05", to_date="2024-01-04")


def test_load_modal_runtime_config_reads_repo_config() -> None:
    """The daily Layer 1 Modal app name lives in repository config."""
    config = load_modal_runtime_config()

    assert config.app_name
    assert config.r2_secret_name
    assert config.timeout_seconds > 0


def _write_completed_stage_manifest(
    *,
    writer: R2Writer,
    stage: str,
    run_id: str,
    output_path: str,
) -> None:
    """Persist a completed stage manifest for precomputed branch tests."""
    manifest = PipelineManifestRecord(
        run_id=run_id,
        stage=stage,
        status=RunStatus.COMPLETED,
        started_at=datetime(2024, 1, 3, 12, 0, tzinfo=UTC),
        finished_at=datetime(2024, 1, 3, 12, 1, tzinfo=UTC),
        output_path=output_path,
    )
    writer.put_object(
        pipeline_manifest_path(stage, run_id),
        manifest.model_dump_json(),
    )
