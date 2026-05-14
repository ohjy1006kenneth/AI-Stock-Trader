from __future__ import annotations

import csv
import io
import json
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
import pytest

from app.lab.data_pipelines.run_finbert_sentiment import (
    FinBERTPipelineConfig,
    FinBERTPipelineResult,
)
from app.lab.data_pipelines.run_hmm_regime_detection import (
    HMMRegimePipelineConfig,
    HMMRegimePipelineResult,
)
from app.lab.data_pipelines.run_news_preprocessing import (
    NewsPreprocessingPipelineConfig,
    NewsPreprocessingPipelineResult,
)
from app.lab.data_pipelines.run_text_topics import (
    TextTopicPipelineConfig,
    TextTopicPipelineResult,
)
from core.contracts.schemas import (
    FeatureRecord,
    NewsSentimentRecord,
    PipelineManifestRecord,
    RunStatus,
)
from core.features.io import feature_records_to_parquet_bytes
from core.features.news_preprocessing import records_to_news_sentiment_frame
from core.features.regime_detection import HMM_REGIME_COLUMNS
from services.r2.client import (
    R2_ACCESS_KEY_ENV,
    R2_BUCKET_ENV,
    R2_ENDPOINT_ENV,
    R2_SECRET_KEY_ENV,
)
from services.r2.paths import (
    layer1_news_preprocessing_path,
    layer1_regime_path,
    layer1_sentiment_feature_path,
    layer1_topic_feature_path,
    pipeline_manifest_path,
    raw_fundamentals_path,
    raw_macro_path,
    raw_news_path,
    raw_price_path,
    raw_universe_path,
)
from services.r2.writer import R2Writer


def local_writer(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> R2Writer:
    """Return a local mock R2 writer regardless of developer env files."""
    for name in (R2_ENDPOINT_ENV, R2_ACCESS_KEY_ENV, R2_SECRET_KEY_ENV, R2_BUCKET_ENV):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setattr("services.r2.client.R2_ENV_FILE", tmp_path / "missing-r2.env")
    return R2Writer(local_root=tmp_path)


def seed_layer0_archives(
    writer: R2Writer,
    *,
    dates: Sequence[str],
    tickers: Sequence[str],
    include_layer0_manifest: bool = True,
    layer0_run_ids: Sequence[str] | None = None,
) -> None:
    """Write the minimal Layer 0 archives required by the Layer 1 orchestrator."""
    if include_layer0_manifest:
        manifest = PipelineManifestRecord(
            run_id="layer1-daily",
            stage="layer0",
            status=RunStatus.COMPLETED,
            started_at=datetime(2024, 1, 2, 22, 0, tzinfo=UTC),
            finished_at=datetime(2024, 1, 2, 22, 30, tzinfo=UTC),
            output_path="raw/",
            metadata={
                "from_date": min(dates),
                "to_date": max(dates),
                "fred_series_ids": [
                    "FEDFUNDS",
                    "DGS3MO",
                    "DGS2",
                    "DGS10",
                    "VIXCLS",
                    "DTWEXBGS",
                    "CPIAUCSL",
                    "BAMLH0A0HYM2",
                ],
            },
        )
        for run_id in layer0_run_ids or ("layer1-daily",):
            writer.put_object(
                pipeline_manifest_path("layer0", run_id),
                manifest.model_copy(update={"run_id": run_id}).model_dump_json(),
            )

    for ticker in [*tickers, "SPY"]:
        _write_parquet(writer, raw_price_path(ticker), _price_history(ticker))
    for ticker in tickers:
        _write_parquet(writer, raw_fundamentals_path(ticker), _empty_fundamentals(ticker))
    for date_text in dates:
        writer.put_object(raw_news_path(date_text), _raw_news_jsonl(date_text, tickers))
        writer.put_object(raw_universe_path(date_text), _raw_universe_csv(date_text, tickers))
        _write_parquet(writer, raw_macro_path(date_text), _macro_archive(date_text))


def fake_news_runner(writer: R2Writer, tickers: Sequence[str]):
    """Return a deterministic news runner for orchestrator tests."""

    def _runner(config: NewsPreprocessingPipelineConfig, *, writer: R2Writer):
        records = [
            NewsSentimentRecord(
                date=config.as_of_date,
                ticker=tickers[0],
                headline="Market update.",
                text="Stocks moved higher.",
                article_id=f"article-{config.as_of_date}",
                sentence_index=0,
                source="benzinga",
                published_at=f"{config.as_of_date}T12:00:00+00:00",
            )
        ]
        output_key = layer1_news_preprocessing_path(config.as_of_date, config.run_id)
        buffer = io.BytesIO()
        records_to_news_sentiment_frame(records).to_parquet(buffer, index=False)
        writer.put_object(output_key, buffer.getvalue())
        return NewsPreprocessingPipelineResult(
            run_id=config.run_id,
            output_key=output_key,
            manifest_key=pipeline_manifest_path("layer1_news_preprocessing", config.run_id),
            article_rows=1,
            sentence_rows=1,
        )

    return _runner


def fake_topic_runner(writer: R2Writer, tickers: Sequence[str]):
    """Return a deterministic topic runner for orchestrator tests."""

    def _runner(config: TextTopicPipelineConfig, *, writer: R2Writer):
        output_key = layer1_topic_feature_path(config.as_of_date, config.run_id)
        records = [
            FeatureRecord(
                date=config.as_of_date,
                ticker=tickers[0],
                features={"nlp_topic_count": 1, "nlp_sentence_count": 1},
            )
        ]
        writer.put_object(output_key, feature_records_to_parquet_bytes(records))
        return TextTopicPipelineResult(
            run_id=config.run_id,
            embedding_key="unused",
            topic_label_key="unused",
            topic_feature_key=output_key,
            manifest_key=pipeline_manifest_path("layer1_text_topics", config.run_id),
            sentence_rows=1,
            embedding_rows=1,
            topic_label_rows=1,
            topic_feature_rows=1,
        )

    return _runner


def fake_sentiment_runner(writer: R2Writer, tickers: Sequence[str]):
    """Return a deterministic sentiment runner for orchestrator tests."""

    def _runner(config: FinBERTPipelineConfig, *, writer: R2Writer):
        output_key = layer1_sentiment_feature_path(config.as_of_date, config.run_id)
        records = [
            FeatureRecord(
                date=config.as_of_date,
                ticker=tickers[0],
                features={
                    "nlp_sentiment_score": 0.25,
                    "nlp_article_count": 1,
                    "nlp_sentence_count": 1,
                },
            )
        ]
        writer.put_object(output_key, feature_records_to_parquet_bytes(records))
        return FinBERTPipelineResult(
            run_id=config.run_id,
            scored_news_key="unused",
            sentiment_feature_key=output_key,
            manifest_key=pipeline_manifest_path("layer1_finbert_sentiment", config.run_id),
            input_rows=1,
            scored_rows=1,
            feature_rows=1,
        )

    return _runner


def fake_regime_runner(writer: R2Writer):
    """Return a deterministic HMM regime runner for orchestrator tests."""

    def _runner(config: HMMRegimePipelineConfig, *, writer: R2Writer):
        output_key = layer1_regime_path(config.run_id)
        frame = pd.DataFrame(
            [
                {
                    "date": config.inference_dates[0],
                    "regime_label": "bull",
                    "regime_confidence": 0.8,
                    "regime_prob_bear": 0.1,
                    "regime_prob_sideways": 0.1,
                    "regime_prob_bull": 0.8,
                }
            ],
            columns=list(HMM_REGIME_COLUMNS),
        )
        _write_parquet(writer, output_key, frame)
        return HMMRegimePipelineResult(
            run_id=config.run_id,
            output_key=output_key,
            manifest_key=pipeline_manifest_path("layer1_5_regime", config.run_id),
            training_rows=30,
            complete_training_rows=30,
            regime_rows=1,
        )

    return _runner


def _write_parquet(writer: R2Writer, key: str, frame: pd.DataFrame) -> None:
    """Serialize a DataFrame and store it under one object key."""
    buffer = io.BytesIO()
    frame.to_parquet(buffer, index=False)
    writer.put_object(key, buffer.getvalue())


def _price_history(ticker: str) -> pd.DataFrame:
    """Build a synthetic OHLCV history with enough bars for rolling features."""
    rows: list[dict[str, object]] = []
    close = 100.0 if ticker != "SPY" else 400.0
    for index, day in enumerate(pd.bdate_range("2023-10-02", periods=80)):
        close += 0.5
        rows.append(
            {
                "date": day.date().isoformat(),
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
    return pd.DataFrame(rows)


def _empty_fundamentals(ticker: str) -> pd.DataFrame:
    """Return an empty but schema-compatible fundamentals archive."""
    return pd.DataFrame(
        columns=[
            "source",
            "ticker",
            "report_date",
            "availability_date",
            "retrieved_at",
            "fiscal_year",
            "fiscal_period",
            "statement",
            "earnings_date",
            "raw_json",
        ]
    ).assign(ticker=ticker)


def _raw_news_jsonl(date_text: str, tickers: Sequence[str]) -> str:
    """Build a one-line raw news archive for the supplied tickers."""
    payload = {
        "id": f"article-{date_text}",
        "headline": "Market update.",
        "summary": "Stocks moved higher.",
        "created_at": f"{date_text}T12:00:00+00:00",
        "source": "benzinga",
        "symbols": list(tickers),
    }
    return json.dumps(payload) + "\n"


def _raw_universe_csv(date_text: str, tickers: Sequence[str]) -> str:
    """Build a raw universe mask CSV for one business date."""
    fieldnames = [
        "date",
        "ticker",
        "in_universe",
        "tradable",
        "liquid",
        "halted",
        "data_quality_ok",
        "reason",
    ]
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=fieldnames)
    writer.writeheader()
    for ticker in tickers:
        writer.writerow(
            {
                "date": date_text,
                "ticker": ticker,
                "in_universe": "True",
                "tradable": "True",
                "liquid": "True",
                "halted": "False",
                "data_quality_ok": "True",
                "reason": "",
            }
        )
    return buffer.getvalue()


def _macro_archive(date_text: str) -> pd.DataFrame:
    """Return point-in-time macro rows available before the target date."""
    previous_day = (datetime.fromisoformat(date_text) - pd.Timedelta(days=1)).date().isoformat()
    rows = []
    for series_id, value in (
        ("FEDFUNDS", 5.25),
        ("DGS3MO", 5.10),
        ("DGS2", 4.60),
        ("DGS10", 4.20),
        ("VIXCLS", 18.0),
        ("DTWEXBGS", 120.0),
        ("CPIAUCSL", 305.0),
        ("BAMLH0A0HYM2", 3.50),
    ):
        rows.append(
            {
                "source": "fred",
                "series_id": series_id,
                "observation_date": previous_day,
                "realtime_start": previous_day,
                "realtime_end": previous_day,
                "retrieved_at": f"{date_text}T00:00:00+00:00",
                "value": value,
                "is_missing": False,
                "raw": {"series_id": series_id},
            }
        )
    return pd.DataFrame(rows)
