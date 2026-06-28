from __future__ import annotations

import io
import json
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
import pytest

from app.lab.data_pipelines import verify_aapl_pilot_evidence as evidence_cli
from app.lab.data_pipelines.run_daily_layer1 import Layer1DailyConfig, run_daily_layer1
from core.contracts.schemas import NewsSentimentRecord, PipelineManifestRecord, RunStatus
from core.features.aapl_accuracy import (
    AAPLFeatureAccuracyConfig,
    AAPLQualityThresholds,
    MarketParameterCandidate,
    build_aapl_feature_accuracy_report,
)
from core.features.aapl_evidence import (
    build_aapl_pilot_evidence_bundle,
    build_layer1_aapl_evidence_report,
    render_aapl_pilot_human_review_csv,
    render_aapl_pilot_human_review_markdown,
    write_aapl_pilot_evidence_outputs,
)
from core.features.news_preprocessing import records_to_news_sentiment_frame
from core.features.semantic_review_dashboard import build_layer1_semantic_review_dashboard_payload
from services.r2.paths import (
    layer1_news_preprocessing_path,
    layer1_regime_path,
    layer1_sentiment_feature_path,
    layer1_sentiment_score_path,
    layer1_topic_feature_path,
    pipeline_manifest_path,
    raw_price_path,
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


def test_build_aapl_pilot_evidence_bundle_separates_machine_and_human_review(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Passing objective gates still require explicit human semantic acceptance."""
    writer = _seed_successful_aapl_pilot(tmp_path, monkeypatch)

    bundle = build_aapl_pilot_evidence_bundle(
        run_id="layer1-aapl-evidence",
        layer0_run_id="layer0-aapl-evidence",
        from_date="2024-01-03",
        to_date="2024-01-05",
        writer=writer,
        now=datetime(2024, 1, 8, 12, 0, tzinfo=UTC),
    )
    csv_text = render_aapl_pilot_human_review_csv(bundle)
    markdown = render_aapl_pilot_human_review_markdown(bundle)

    assert bundle.machine_integrity_status == "pass"
    assert bundle.human_semantic_review_status == "pending"
    assert bundle.recommendation_for_issue_202 == "needs_human_review"
    assert len(bundle.human_review_rows) == 3
    assert "FinBERT, topic-model, and HMM semantic correctness is a human decision" in markdown
    assert "Market update." in csv_text
    assert bundle.artifact_keys["raw_price"] == "raw/prices/AAPL.parquet"


def test_build_layer1_aapl_evidence_report_loads_cached_bundle_when_stage_artifacts_are_missing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The report builder can fall back to a cached AAPL pilot evidence bundle."""
    diagnostics_dir = tmp_path / "artifacts" / "reports" / "diagnostics"
    diagnostics_dir.mkdir(parents=True)
    monkeypatch.setattr(
        "core.features.aapl_evidence.DEFAULT_AAPL_PILOT_EVIDENCE_OUTPUT_DIR",
        diagnostics_dir,
    )
    bundle_path = diagnostics_dir / "aapl_pilot_evidence_cached-run.json"
    bundle_path.write_text(
        json.dumps(
            {
                "human_review_rows": [
                    {
                        "date": "2026-05-21",
                        "ticker": "AAPL",
                        "review_status": "pending",
                        "raw_article_id": "aapl-001",
                        "raw_headline": "Apple expands AI search in Safari",
                        "raw_snippet": "Apple expands AI search in Safari and highlights AAPL search demand.",
                        "raw_source": "benzinga",
                        "raw_published_at": "2026-05-21T14:00:00Z",
                        "raw_news_key": "raw/news/2026-05-21.jsonl",
                        "preprocessed_news_key": "features/2026-05-21/news_sentiment/cached-run-2026-05-21.parquet",
                        "finbert_scored_news_key": "features/2026-05-21/news_sentiment_scored/cached-run-2026-05-21.parquet",
                        "finbert_positive": 0.91,
                        "finbert_negative": 0.03,
                        "finbert_neutral": 0.06,
                        "finbert_score": 0.88,
                        "relevance_score": 0.92,
                        "regime": "bull",
                        "regime_confidence": 0.97,
                        "regime_prob_bear": 0.01,
                        "regime_prob_sideways": 0.02,
                        "regime_prob_bull": 0.97,
                        "notes": "",
                    },
                    {
                        "date": "2026-05-22",
                        "ticker": "AAPL",
                        "review_status": "pending",
                        "raw_article_id": "aapl-002",
                        "raw_headline": "Apple supplier sees weaker iPhone demand",
                        "raw_snippet": "Apple supplier commentary suggests weaker iPhone demand and reduced AAPL relevance.",
                        "raw_source": "reuters",
                        "raw_published_at": "2026-05-22T14:00:00Z",
                        "raw_news_key": "raw/news/2026-05-22.jsonl",
                        "preprocessed_news_key": "features/2026-05-22/news_sentiment/cached-run-2026-05-22.parquet",
                        "finbert_scored_news_key": "features/2026-05-22/news_sentiment_scored/cached-run-2026-05-22.parquet",
                        "finbert_positive": 0.18,
                        "finbert_negative": 0.62,
                        "finbert_neutral": 0.20,
                        "finbert_score": -0.44,
                        "relevance_score": 0.41,
                        "regime": "sideways",
                        "regime_confidence": 0.88,
                        "regime_prob_bear": 0.14,
                        "regime_prob_sideways": 0.73,
                        "regime_prob_bull": 0.13,
                        "notes": "",
                    },
                ]
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    report = build_layer1_aapl_evidence_report(
        run_id="cached-run",
        from_date="2026-05-21",
        to_date="2026-05-22",
        ticker="AAPL",
        writer=R2Writer(local_root=tmp_path / "empty-r2"),
    )

    assert report.row_count == 2
    assert report.article_count == 2
    assert report.date_count == 2
    assert report.load_warnings[0]["scope"] == "cached_bundle"
    assert report.load_warnings[0]["raw_lookup_warnings"]

    payload = build_layer1_semantic_review_dashboard_payload(report)
    smoke = payload["smoke"]
    assert smoke["status"] == "fail"
    assert smoke["ready_for_final_human_acceptance"] is False
    failure_reasons = {item["reason"] for item in smoke["failures"]}
    assert "cached_bundle_fallback" in failure_reasons
    cached_failure = next(
        item for item in smoke["failures"] if item["reason"] == "cached_bundle_fallback"
    )
    assert "features/2026-05-21/news_sentiment_scored/cached-run.parquet" in cached_failure[
        "missing_or_tried_keys"
    ]


def test_build_aapl_pilot_evidence_bundle_allows_proceed_only_after_human_acceptance(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The #202 recommendation becomes proceed only with passing gates and acceptance."""
    writer = _seed_successful_aapl_pilot(tmp_path, monkeypatch)

    bundle = build_aapl_pilot_evidence_bundle(
        run_id="layer1-aapl-evidence",
        layer0_run_id="layer0-aapl-evidence",
        from_date="2024-01-03",
        to_date="2024-01-05",
        human_semantic_review_status="accepted",
        writer=writer,
    )

    assert bundle.machine_integrity_status == "pass"
    assert bundle.recommendation_for_issue_202 == "proceed"


def test_build_aapl_pilot_evidence_bundle_skips_non_trading_dates(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """AAPL pilot evidence expects artifacts for trading sessions, not market holidays."""
    writer = _seed_successful_aapl_pilot_holiday_window(tmp_path, monkeypatch)

    bundle = build_aapl_pilot_evidence_bundle(
        run_id="layer1-aapl-holiday-evidence",
        layer0_run_id="layer0-aapl-holiday-evidence",
        from_date="2026-05-22",
        to_date="2026-05-26",
        human_semantic_review_status="accepted",
        writer=writer,
    )
    feature_gate = next(gate for gate in bundle.gates if gate.name == "date_first_feature_coverage")
    missing_gate = next(gate for gate in bundle.gates if gate.name == "expected_artifacts_exist")

    assert bundle.machine_integrity_status == "pass"
    assert bundle.recommendation_for_issue_202 == "proceed"
    assert bundle.artifact_keys["expected_trading_dates"] == ["2026-05-22", "2026-05-26"]
    assert bundle.artifact_keys["skipped_non_trading_dates"] == [
        "2026-05-23",
        "2026-05-24",
        "2026-05-25",
    ]
    assert feature_gate.details["expected_trading_dates"] == ["2026-05-22", "2026-05-26"]
    assert feature_gate.details["skipped_non_trading_dates"] == [
        "2026-05-23",
        "2026-05-24",
        "2026-05-25",
    ]
    assert "features/2026-05-25/AAPL.parquet" not in missing_gate.details["missing_keys"]
    assert [row.date for row in bundle.human_review_rows] == ["2026-05-22", "2026-05-26"]


def test_build_aapl_pilot_evidence_bundle_fails_closed_on_missing_sentiment_artifact(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Missing expected sentiment artifacts fail the machine-integrity gate."""
    writer = _seed_successful_aapl_pilot(tmp_path, monkeypatch)
    missing_key = layer1_sentiment_score_path(
        "2024-01-04",
        "layer1-aapl-evidence-2024-01-04",
    )
    writer.delete_object(missing_key)

    bundle = build_aapl_pilot_evidence_bundle(
        run_id="layer1-aapl-evidence",
        layer0_run_id="layer0-aapl-evidence",
        from_date="2024-01-03",
        to_date="2024-01-05",
        human_semantic_review_status="accepted",
        writer=writer,
    )
    missing_gate = next(
        gate for gate in bundle.gates if gate.name == "expected_artifacts_exist"
    )

    assert bundle.machine_integrity_status == "fail"
    assert bundle.recommendation_for_issue_202 == "do_not_proceed"
    assert missing_key in missing_gate.details["missing_keys"]


def test_evidence_cli_writes_outputs_and_returns_pending_status(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The CLI writes all evidence files and exits nonzero while human review is pending."""
    writer = _seed_successful_aapl_pilot(tmp_path, monkeypatch)
    monkeypatch.setattr(evidence_cli, "R2Writer", lambda: writer)
    json_path = tmp_path / "out" / "evidence.json"
    markdown_path = tmp_path / "out" / "review.md"
    csv_path = tmp_path / "out" / "review.csv"

    exit_code = evidence_cli.main(
        [
            "--run-id",
            "layer1-aapl-evidence",
            "--from-date",
            "2024-01-03",
            "--to-date",
            "2024-01-05",
            "--layer0-run-id",
            "layer0-aapl-evidence",
            "--write-json",
            str(json_path),
            "--write-markdown",
            str(markdown_path),
            "--write-csv",
            str(csv_path),
        ]
    )

    payload = json.loads(json_path.read_text(encoding="utf-8"))
    assert exit_code == 1
    assert payload["machine_integrity_status"] == "pass"
    assert payload["recommendation_for_issue_202"] == "needs_human_review"
    assert markdown_path.exists()
    assert csv_path.exists()


def test_write_aapl_pilot_evidence_outputs_creates_parent_directories(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Evidence JSON, Markdown, and CSV files are written deterministically."""
    writer = _seed_successful_aapl_pilot(tmp_path, monkeypatch)
    bundle = build_aapl_pilot_evidence_bundle(
        run_id="layer1-aapl-evidence",
        layer0_run_id="layer0-aapl-evidence",
        from_date="2024-01-03",
        to_date="2024-01-05",
        writer=writer,
    )

    paths = write_aapl_pilot_evidence_outputs(
        bundle,
        json_path=tmp_path / "nested" / "evidence.json",
        markdown_path=tmp_path / "nested" / "review.md",
        csv_path=tmp_path / "nested" / "review.csv",
    )

    assert paths["json"].exists()
    assert paths["markdown"].exists()
    assert paths["csv"].exists()


def test_evidence_cli_rejects_non_aapl_scope() -> None:
    """The evidence CLI cannot be widened into a broad ticker review."""
    with pytest.raises(SystemExit):
        evidence_cli.parse_args(
            [
                "--run-id",
                "layer1-aapl-evidence",
                "--ticker",
                "MSFT",
                "--from-date",
                "2024-01-03",
                "--to-date",
                "2024-01-05",
                "--layer0-run-id",
                "layer0-aapl-evidence",
            ]
        )


def _seed_successful_aapl_pilot(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> R2Writer:
    """Seed a deterministic AAPL pilot with evidence artifacts in local R2."""
    writer = local_writer(tmp_path, monkeypatch)
    dates = ["2024-01-03", "2024-01-04", "2024-01-05"]
    seed_layer0_archives(
        writer,
        dates=dates,
        tickers=["AAPL"],
        layer0_run_ids=("layer0-aapl-evidence",),
    )
    run_daily_layer1(
        Layer1DailyConfig(
            run_id="layer1-aapl-evidence",
            from_date="2024-01-03",
            to_date="2024-01-05",
            layer0_run_id="layer0-aapl-evidence",
            tickers=("AAPL",),
            allow_layer0_manifest_date_range=True,
        ),
        writer=writer,
        news_runner=fake_news_runner(writer, ["AAPL"]),
        text_topic_runner=fake_topic_runner(writer, ["AAPL"]),
        finbert_runner=fake_sentiment_runner(writer, ["AAPL"]),
        regime_runner=fake_regime_runner(writer),
        validation_output_dir=tmp_path / "reports",
        now=datetime(2024, 1, 8, 12, 0, tzinfo=UTC),
    )
    for date_text in dates:
        stage_run_id = f"layer1-aapl-evidence-{date_text}"
        _write_scored_news(writer, date_text, stage_run_id)
        _write_stage_manifest(
            writer,
            stage="layer1_news_preprocessing",
            run_id=stage_run_id,
            output_path=layer1_news_preprocessing_path(date_text, stage_run_id),
            metadata={"as_of_date": date_text, "requested_tickers": ["AAPL"]},
        )
        _write_stage_manifest(
            writer,
            stage="layer1_text_topics",
            run_id=stage_run_id,
            output_path=layer1_topic_feature_path(date_text, stage_run_id),
            metadata={"as_of_date": date_text, "requested_tickers": ["AAPL"]},
        )
        _write_stage_manifest(
            writer,
            stage="layer1_finbert_sentiment",
            run_id=stage_run_id,
            output_path=layer1_sentiment_feature_path(date_text, stage_run_id),
            metadata={
                "as_of_date": date_text,
                "requested_tickers": ["AAPL"],
                "scored_news_key": layer1_sentiment_score_path(date_text, stage_run_id),
            },
        )
        _write_stage_manifest(
            writer,
            stage="layer1_5_regime",
            run_id=stage_run_id,
            output_path=layer1_regime_path(date_text, stage_run_id),
            metadata={"as_of_date": date_text, "inference_dates": [date_text]},
        )
    build_aapl_feature_accuracy_report(
        run_id="layer1-aapl-evidence",
        from_date="2024-01-03",
        to_date="2024-01-05",
        layer0_run_id="layer0-aapl-evidence",
        config=AAPLFeatureAccuracyConfig(
            quality_thresholds=AAPLQualityThresholds(
                min_feature_rows=3,
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
        ),
        writer=writer,
        now=datetime(2024, 1, 8, 13, 0, tzinfo=UTC),
    )
    return writer


def _seed_successful_aapl_pilot_holiday_window(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> R2Writer:
    """Seed a Memorial Day AAPL pilot with only trading-session artifacts."""
    writer = local_writer(tmp_path, monkeypatch)
    dates = ["2026-05-22", "2026-05-26"]
    seed_layer0_archives(
        writer,
        dates=dates,
        tickers=["AAPL"],
        layer0_run_ids=("layer0-aapl-holiday-evidence",),
    )
    _write_memorial_day_price_frame(writer, "AAPL")
    _write_memorial_day_price_frame(writer, "SPY")
    run_daily_layer1(
        Layer1DailyConfig(
            run_id="layer1-aapl-holiday-evidence",
            from_date="2026-05-22",
            to_date="2026-05-26",
            layer0_run_id="layer0-aapl-holiday-evidence",
            tickers=("AAPL",),
            allow_layer0_manifest_date_range=True,
        ),
        writer=writer,
        news_runner=fake_news_runner(writer, ["AAPL"]),
        text_topic_runner=fake_topic_runner(writer, ["AAPL"]),
        finbert_runner=fake_sentiment_runner(writer, ["AAPL"]),
        regime_runner=fake_regime_runner(writer),
        validation_output_dir=tmp_path / "reports",
        now=datetime(2026, 5, 26, 22, 0, tzinfo=UTC),
    )
    for date_text in dates:
        stage_run_id = f"layer1-aapl-holiday-evidence-{date_text}"
        _write_scored_news(writer, date_text, stage_run_id)
        _write_stage_manifest(
            writer,
            stage="layer1_news_preprocessing",
            run_id=stage_run_id,
            output_path=layer1_news_preprocessing_path(date_text, stage_run_id),
            metadata={"as_of_date": date_text, "requested_tickers": ["AAPL"]},
        )
        _write_stage_manifest(
            writer,
            stage="layer1_text_topics",
            run_id=stage_run_id,
            output_path=layer1_topic_feature_path(date_text, stage_run_id),
            metadata={"as_of_date": date_text, "requested_tickers": ["AAPL"]},
        )
        _write_stage_manifest(
            writer,
            stage="layer1_finbert_sentiment",
            run_id=stage_run_id,
            output_path=layer1_sentiment_feature_path(date_text, stage_run_id),
            metadata={
                "as_of_date": date_text,
                "requested_tickers": ["AAPL"],
                "scored_news_key": layer1_sentiment_score_path(date_text, stage_run_id),
            },
        )
        _write_stage_manifest(
            writer,
            stage="layer1_5_regime",
            run_id=stage_run_id,
            output_path=layer1_regime_path(date_text, stage_run_id),
            metadata={"as_of_date": date_text, "inference_dates": [date_text]},
        )
    build_aapl_feature_accuracy_report(
        run_id="layer1-aapl-holiday-evidence",
        from_date="2026-05-22",
        to_date="2026-05-26",
        layer0_run_id="layer0-aapl-holiday-evidence",
        config=AAPLFeatureAccuracyConfig(
            target_horizon_days=1,
            quality_thresholds=AAPLQualityThresholds(
                min_feature_rows=2,
                max_required_feature_null_rate=1.0,
                min_label_pairs=1,
                min_abs_best_candidate_correlation=0.0,
            ),
            market_parameter_candidates=(
                MarketParameterCandidate(
                    name="one_day",
                    return_window_days=1,
                    volatility_window_days=1,
                    volume_window_days=1,
                ),
            ),
        ),
        writer=writer,
        now=datetime(2026, 5, 26, 22, 30, tzinfo=UTC),
    )
    return writer


def _write_memorial_day_price_frame(writer: R2Writer, ticker: str) -> None:
    """Write synthetic 2026 raw prices with Memorial Day absent."""
    rows: list[dict[str, object]] = []
    close = 100.0 if ticker != "SPY" else 400.0
    dates = [
        day.date().isoformat()
        for day in pd.bdate_range("2026-02-02", "2026-05-28")
        if day.date().isoformat() != "2026-05-25"
    ]
    for index, date_text in enumerate(dates):
        close += 0.5
        rows.append(
            {
                "date": date_text,
                "ticker": ticker,
                "open": close - 0.25,
                "high": close + 0.5,
                "low": close - 0.5,
                "close": close,
                "adj_close": close,
                "volume": 1_000_000 + index,
                "dollar_volume": close * (1_000_000 + index),
            }
        )
    _write_parquet(writer, raw_price_path(ticker), pd.DataFrame(rows))


def _write_scored_news(writer: R2Writer, date_text: str, stage_run_id: str) -> None:
    """Write one deterministic scored FinBERT row for AAPL."""
    records = [
        NewsSentimentRecord(
            date=date_text,
            ticker="AAPL",
            headline="Market update.",
            text="Stocks moved higher.",
            article_id=f"article-{date_text}",
            sentence_index=0,
            source="benzinga",
            published_at=f"{date_text}T12:00:00+00:00",
            sentiment_positive=0.7,
            sentiment_negative=0.1,
            sentiment_neutral=0.2,
            sentiment_score=0.6,
            relevance_score=1.0,
        )
    ]
    _write_parquet(
        writer,
        layer1_sentiment_score_path(date_text, stage_run_id),
        records_to_news_sentiment_frame(records),
    )


def _write_stage_manifest(
    writer: R2Writer,
    *,
    stage: str,
    run_id: str,
    output_path: str,
    metadata: dict[str, object],
) -> None:
    """Write a completed stage manifest for one seeded artifact."""
    writer.put_object(
        pipeline_manifest_path(stage, run_id),
        PipelineManifestRecord(
            run_id=run_id,
            stage=stage,
            status=RunStatus.COMPLETED,
            started_at=datetime(2024, 1, 8, 12, 0, tzinfo=UTC),
            finished_at=datetime(2024, 1, 8, 12, 5, tzinfo=UTC),
            output_path=output_path,
            metadata=metadata,
        ).model_dump_json(),
    )


def _write_parquet(writer: R2Writer, key: str, frame: pd.DataFrame) -> None:
    """Serialize a DataFrame to a local object-store key."""
    buffer = io.BytesIO()
    frame.to_parquet(buffer, index=False)
    writer.put_object(key, buffer.getvalue())
