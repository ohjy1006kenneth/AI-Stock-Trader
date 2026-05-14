from __future__ import annotations

import csv
import io
import json
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
import pytest

from core.contracts.schemas import FeatureRecord, PipelineManifestRecord, RunStatus
from core.features.context_features import (
    compute_context_features,
    context_features_to_records,
)
from core.features.io import feature_records_to_parquet_bytes
from core.features.macro_features import compute_macro_features
from core.features.market_features import compute_market_features, market_features_to_records
from core.features.news_preprocessing import (
    preprocess_news_articles,
    records_to_news_sentiment_frame,
)
from core.features.regime_detection import HMM_REGIME_COLUMNS
from core.features.sector_features import compute_sector_features, sector_features_to_records
from core.features.sentiment_features import sentiment_feature_records_from_scored_news
from core.features.text_topics import TOPIC_LABEL_COLUMNS, topic_labels_to_feature_records
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
    layer1_sentiment_score_path,
    layer1_ticker_history_path,
    layer1_topic_feature_path,
    layer1_topic_label_path,
    pipeline_manifest_path,
    raw_fundamentals_path,
    raw_macro_path,
    raw_news_path,
    raw_price_path,
    raw_universe_path,
)
from services.r2.writer import R2Writer


def local_writer(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> R2Writer:
    """Return a local mock R2 writer with cloud credentials disabled."""
    for name in (R2_ENDPOINT_ENV, R2_ACCESS_KEY_ENV, R2_SECRET_KEY_ENV, R2_BUCKET_ENV):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setattr("services.r2.client.R2_ENV_FILE", tmp_path / "missing-r2.env")
    return R2Writer(local_root=tmp_path)


def seed_layer1_audit_fixture(
    writer: R2Writer,
    *,
    as_of_date: str = "2024-05-06",
    ticker: str = "AAPL",
    benchmark_ticker: str = "SPY",
) -> dict[str, object]:
    """Seed a deterministic Layer 0/1 sample suitable for the audit harness."""
    ohlcv = _price_history(ticker, end_date=as_of_date)
    benchmark = _price_history(benchmark_ticker, end_date=as_of_date, start_price=400.0)
    fundamentals = _fundamentals_archive(ticker)
    macro_by_key = _macro_archives()
    raw_articles = _raw_articles(ticker, as_of_date=as_of_date)

    _write_parquet(writer, raw_price_path(ticker), ohlcv)
    _write_parquet(writer, raw_price_path(benchmark_ticker), benchmark)
    _write_parquet(writer, raw_fundamentals_path(ticker), fundamentals)
    for key, frame in macro_by_key.items():
        _write_parquet(writer, key, frame)
    writer.put_object(raw_news_path(as_of_date), _jsonl(raw_articles))
    writer.put_object(raw_universe_path(as_of_date), _raw_universe_csv([ticker]))

    preprocessed_records = preprocess_news_articles(
        raw_articles,
        as_of_date=as_of_date,
        point_in_time_tickers=[ticker],
    )
    preprocessed_key = layer1_news_preprocessing_path(as_of_date, "audit-news")
    _write_parquet(writer, preprocessed_key, records_to_news_sentiment_frame(preprocessed_records))
    _write_manifest(
        writer,
        stage="layer1_news_preprocessing",
        run_id="audit-news",
        output_path=preprocessed_key,
        metadata={"as_of_date": as_of_date},
    )

    topic_label_key = layer1_topic_label_path(as_of_date, "audit-topics")
    topic_feature_key = layer1_topic_feature_path(as_of_date, "audit-topics")
    topic_labels = pd.DataFrame(
        [
            {
                "date": as_of_date,
                "ticker": ticker,
                "article_id": str(preprocessed_records[0].article_id),
                "sentence_index": int(preprocessed_records[0].sentence_index or 0),
                "text": str(preprocessed_records[0].text),
                "embedding_cache_key": "audit-embedding-1",
                "topic_model": "BERTopic",
                "topic_model_version": "test",
                "topic_id": 7,
                "topic_probability": 0.9,
            }
        ],
        columns=list(TOPIC_LABEL_COLUMNS),
    )
    _write_parquet(writer, topic_label_key, topic_labels)
    topic_feature_records = topic_labels_to_feature_records(topic_labels)
    writer.put_object(topic_feature_key, feature_records_to_parquet_bytes(topic_feature_records))
    _write_manifest(
        writer,
        stage="layer1_text_topics",
        run_id="audit-topics",
        output_path=topic_feature_key,
        metadata={
            "as_of_date": as_of_date,
            "topic_label_key": topic_label_key,
            "topic_feature_key": topic_feature_key,
        },
    )

    scored_news_key = layer1_sentiment_score_path(as_of_date, "audit-sentiment")
    sentiment_feature_key = layer1_sentiment_feature_path(as_of_date, "audit-sentiment")
    scored_news = pd.DataFrame(
        [
            {
                "date": as_of_date,
                "ticker": ticker,
                "article_id": str(preprocessed_records[0].article_id),
                "source": "Reuters",
                "published_at": "2024-05-06T11:30:00Z",
                "sentiment_positive": 0.8,
                "sentiment_negative": 0.1,
                "sentiment_neutral": 0.1,
                "sentiment_score": 0.7,
                "relevance_score": 1.0,
            }
        ]
    )
    _write_parquet(writer, scored_news_key, scored_news)
    sentiment_feature_records = sentiment_feature_records_from_scored_news(scored_news)
    writer.put_object(
        sentiment_feature_key,
        feature_records_to_parquet_bytes(sentiment_feature_records),
    )
    _write_manifest(
        writer,
        stage="layer1_finbert_sentiment",
        run_id="audit-sentiment",
        output_path=sentiment_feature_key,
        metadata={
            "as_of_date": as_of_date,
            "scored_news_key": scored_news_key,
            "sentiment_feature_key": sentiment_feature_key,
        },
    )

    regime_key = layer1_regime_path("audit-regime")
    regime_output = pd.DataFrame(
        [
            {
                "date": as_of_date,
                "regime_label": "bull",
                "regime_confidence": 0.8,
                "regime_prob_bear": 0.1,
                "regime_prob_sideways": 0.1,
                "regime_prob_bull": 0.8,
            }
        ],
        columns=list(HMM_REGIME_COLUMNS),
    )
    _write_parquet(writer, regime_key, regime_output)
    _write_manifest(
        writer,
        stage="layer1_5_regime",
        run_id="audit-regime",
        output_path=regime_key,
        metadata={
            "train_end_date": "2024-05-03",
            "inference_dates": [as_of_date],
        },
    )

    market_record = _single_record(
        market_features_to_records(
            compute_market_features(ohlcv, ticker, benchmark_bars=benchmark)
        ),
        as_of_date,
    )
    macro_frame = pd.concat(list(macro_by_key.values()), ignore_index=True)
    context_record = _single_record(
        context_features_to_records(
            compute_context_features(
                fundamentals=fundamentals,
                ohlcv=ohlcv,
                macro=macro_frame,
                ticker=ticker,
                macro_features=compute_macro_features(macro_frame, ohlcv["date"].tolist()),
                target_dates=(as_of_date,),
            )
        ),
        as_of_date,
    )
    sector_record = _single_record(
        sector_features_to_records(
            compute_sector_features(
                ohlcv_by_ticker={ticker: ohlcv},
                fundamentals_by_ticker={ticker: fundamentals},
                target_dates_by_ticker={ticker: (as_of_date,)},
            )[ticker]
        ),
        as_of_date,
    )
    topic_record = _single_record(topic_feature_records, as_of_date)
    sentiment_record = _single_record(sentiment_feature_records, as_of_date)
    history_record = FeatureRecord(
        date=as_of_date,
        ticker=ticker,
        features={
            **dict(market_record.features if market_record is not None else {}),
            **dict(context_record.features if context_record is not None else {}),
            **dict(sector_record.features if sector_record is not None else {}),
            **dict(topic_record.features if topic_record is not None else {}),
            **dict(sentiment_record.features if sentiment_record is not None else {}),
            "regime_label": "bull",
            "regime_confidence": 0.8,
            "regime_prob_bear": 0.1,
            "regime_prob_sideways": 0.1,
            "regime_prob_bull": 0.8,
        },
    )
    writer.put_object(
        layer1_ticker_history_path(ticker),
        feature_records_to_parquet_bytes([history_record]),
    )

    return {
        "as_of_date": as_of_date,
        "ticker": ticker,
        "benchmark_ticker": benchmark_ticker,
        "history_record": history_record,
    }


def _single_record(records: list[FeatureRecord], as_of_date: str) -> FeatureRecord | None:
    """Return the single record for `as_of_date`, if present."""
    matches = [record for record in records if record.date == as_of_date]
    if not matches:
        return None
    if len(matches) > 1:
        raise ValueError(f"Multiple records found for {as_of_date}")
    return matches[0]


def _write_parquet(writer: R2Writer, key: str, frame: pd.DataFrame) -> None:
    """Serialize a DataFrame and persist it through the active R2 writer."""
    buffer = io.BytesIO()
    frame.to_parquet(buffer, index=False)
    writer.put_object(key, buffer.getvalue())


def _write_manifest(
    writer: R2Writer,
    *,
    stage: str,
    run_id: str,
    output_path: str,
    metadata: dict[str, object],
) -> None:
    """Persist one completed pipeline manifest for a stage artifact."""
    manifest = PipelineManifestRecord(
        run_id=run_id,
        stage=stage,
        status=RunStatus.COMPLETED,
        started_at=datetime(2024, 5, 6, 12, 0, tzinfo=UTC),
        finished_at=datetime(2024, 5, 6, 12, 5, tzinfo=UTC),
        output_path=output_path,
        metadata=metadata,
    )
    writer.put_object(pipeline_manifest_path(stage, run_id), manifest.model_dump_json())


def _price_history(
    ticker: str,
    *,
    end_date: str,
    start_price: float = 100.0,
) -> pd.DataFrame:
    """Build an OHLCV history with enough lookback for rolling features."""
    rows: list[dict[str, object]] = []
    close = start_price
    end_timestamp = pd.Timestamp(end_date)
    for index, day in enumerate(pd.bdate_range(end=end_timestamp, periods=90)):
        close += 0.4
        rows.append(
            {
                "date": day.date().isoformat(),
                "ticker": ticker,
                "open": close - 0.2,
                "high": close + 0.5,
                "low": close - 0.5,
                "close": close,
                "adj_close": close,
                "volume": 1_000_000 + index * 100,
                "dollar_volume": close * (1_000_000 + index * 100),
            }
        )
    return pd.DataFrame(rows)


def _fundamentals_archive(ticker: str) -> pd.DataFrame:
    """Return a small point-in-time fundamentals history with a prior-year comparator."""
    rows = [
        {
            "source": "simfin",
            "ticker": ticker,
            "report_date": "2023-03-31",
            "availability_date": "2023-05-04",
            "retrieved_at": "2023-05-04T00:00:00Z",
            "fiscal_year": 2023,
            "fiscal_period": "Q1",
            "statement": "pl",
            "earnings_date": "2023-05-04",
            "raw_json": json.dumps(
                {
                    "revenue": 900.0,
                    "netIncome": 90.0,
                    "totalAssets": 4_000.0,
                    "totalLiabilities": 1_700.0,
                    "sharesBasic": 100.0,
                    "eps": 0.9,
                },
                sort_keys=True,
                separators=(",", ":"),
            ),
        },
        {
            "source": "simfin",
            "ticker": ticker,
            "report_date": "2024-03-31",
            "availability_date": "2024-05-03",
            "retrieved_at": "2024-05-03T00:00:00Z",
            "fiscal_year": 2024,
            "fiscal_period": "Q1",
            "statement": "pl",
            "earnings_date": "2024-05-07",
            "raw_json": json.dumps(
                {
                    "revenue": 1_000.0,
                    "netIncome": 100.0,
                    "totalAssets": 4_200.0,
                    "totalLiabilities": 1_800.0,
                    "sharesBasic": 100.0,
                    "eps": 1.0,
                },
                sort_keys=True,
                separators=(",", ":"),
            ),
        },
    ]
    return pd.DataFrame(rows)


def _macro_archives() -> dict[str, pd.DataFrame]:
    """Return per-day raw macro shards covering the audit date."""
    rows_by_date: dict[str, list[dict[str, object]]] = {
        "2024-04-01": [
            _macro_row("CPIAUCSL", "2024-04-01", "2024-05-01", 310.0),
        ],
        "2024-05-01": [
            _macro_row("FEDFUNDS", "2024-05-01", "2024-05-01", 5.33),
        ],
        "2024-05-02": [
            _macro_row("DGS3MO", "2024-05-02", "2024-05-02", 5.25),
            _macro_row("DGS2", "2024-05-02", "2024-05-02", 4.95),
            _macro_row("DGS10", "2024-05-02", "2024-05-02", 4.60),
            _macro_row("VIXCLS", "2024-05-02", "2024-05-02", 14.5),
            _macro_row("DTWEXBGS", "2024-05-02", "2024-05-02", 120.0),
            _macro_row("BAMLH0A0HYM2", "2024-05-02", "2024-05-02", 3.4),
        ],
        "2024-05-03": [
            _macro_row("DGS3MO", "2024-05-03", "2024-05-03", 5.20),
            _macro_row("DGS2", "2024-05-03", "2024-05-03", 4.90),
            _macro_row("DGS10", "2024-05-03", "2024-05-03", 4.55),
            _macro_row("VIXCLS", "2024-05-03", "2024-05-03", 15.0),
            _macro_row("DTWEXBGS", "2024-05-03", "2024-05-03", 119.5),
            _macro_row("BAMLH0A0HYM2", "2024-05-03", "2024-05-03", 3.5),
        ],
    }
    return {
        raw_macro_path(observation_date): pd.DataFrame(rows)
        for observation_date, rows in rows_by_date.items()
    }


def _macro_row(
    series_id: str,
    observation_date: str,
    realtime_start: str,
    value: float,
) -> dict[str, object]:
    """Return one normalized FRED observation row."""
    return {
        "source": "fred",
        "series_id": series_id,
        "observation_date": observation_date,
        "realtime_start": realtime_start,
        "realtime_end": realtime_start,
        "retrieved_at": f"{realtime_start}T00:00:00Z",
        "value": value,
        "is_missing": False,
        "raw": {"series_id": series_id},
    }


def _raw_articles(ticker: str, *, as_of_date: str) -> list[dict[str, object]]:
    """Return one raw news article matching the Layer 0 JSON Lines archive shape."""
    return [
        {
            "id": f"article-{as_of_date}",
            "headline": f"{ticker} beats estimates.",
            "summary": "Revenue and margins improved.",
            "content": "Management raised guidance.",
            "symbols": [ticker],
            "source": "Reuters",
            "url": f"https://example.com/{ticker.lower()}-{as_of_date}",
            "published_at": "2024-05-06T11:30:00Z",
        }
    ]


def _jsonl(rows: list[dict[str, object]]) -> str:
    """Serialize objects as a JSON Lines string."""
    return "\n".join(json.dumps(row, sort_keys=True) for row in rows) + "\n"


def _raw_universe_csv(tickers: list[str]) -> str:
    """Return a minimal Layer 0 universe-mask CSV payload."""
    buffer = io.StringIO()
    writer = csv.DictWriter(
        buffer,
        fieldnames=[
            "date",
            "ticker",
            "in_universe",
            "tradable",
            "liquid",
            "halted",
            "data_quality_ok",
        ],
    )
    writer.writeheader()
    for ticker in tickers:
        writer.writerow(
            {
                "date": "2024-05-06",
                "ticker": ticker,
                "in_universe": "true",
                "tradable": "true",
                "liquid": "true",
                "halted": "false",
                "data_quality_ok": "true",
            }
        )
    return buffer.getvalue()
