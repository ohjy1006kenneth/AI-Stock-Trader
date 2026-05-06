from __future__ import annotations

import io
import json
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
import pytest

from app.lab.data_pipelines.backfill_layer1 import (
    FINBERT_SENTIMENT_STAGE,
    LAYER1_BACKFILL_STAGE,
    REGIME_STAGE,
    TEXT_TOPICS_STAGE,
    Layer1BackfillConfig,
    _resolve_tickers,
    backfill_layer1,
)
from core.contracts.schemas import FeatureRecord, PipelineManifestRecord, RunStatus
from core.features.io import feature_records_to_parquet_bytes
from core.features.io import read_feature_records
from services.r2.client import (
    R2_ACCESS_KEY_ENV,
    R2_BUCKET_ENV,
    R2_ENDPOINT_ENV,
    R2_SECRET_KEY_ENV,
)
from services.r2.paths import (
    layer1_regime_path,
    layer1_sentiment_feature_path,
    layer1_ticker_history_path,
    layer1_topic_feature_path,
    pipeline_manifest_path,
    raw_fundamentals_path,
    raw_macro_path,
    raw_price_path,
)
from services.r2.writer import R2Writer


def _local_writer(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> R2Writer:
    """Return a local mock R2 writer regardless of developer env files."""
    for name in (R2_ENDPOINT_ENV, R2_ACCESS_KEY_ENV, R2_SECRET_KEY_ENV, R2_BUCKET_ENV):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setattr("services.r2.client.R2_ENV_FILE", tmp_path / "missing-r2.env")
    return R2Writer(local_root=tmp_path)


def _write_synthetic_ohlcv(
    writer: R2Writer,
    ticker: str,
    *,
    num_bars: int,
    start: pd.Timestamp = pd.Timestamp("2024-01-02"),
) -> None:
    """Persist synthetic OHLCV bars for one ticker beneath the local mock root."""
    rows = []
    for offset in range(num_bars):
        date = (start + pd.tseries.offsets.BDay(offset)).date().isoformat()
        price = 100.0 + offset
        rows.append(
            {
                "date": date,
                "ticker": ticker,
                "open": price,
                "high": price * 1.01,
                "low": price * 0.99,
                "close": price,
                "adj_close": price,
                "volume": 1_000_000,
                "dollar_volume": price * 1_000_000,
            }
        )
    buffer = io.BytesIO()
    pd.DataFrame(rows).to_parquet(buffer, index=False)
    writer.put_object(raw_price_path(ticker), buffer.getvalue())


def _write_empty_macro_shard(writer: R2Writer) -> None:
    """Persist a single empty macro shard so the macro loader returns a valid frame."""
    frame = pd.DataFrame(
        columns=[
            "source",
            "series_id",
            "observation_date",
            "realtime_start",
            "realtime_end",
            "retrieved_at",
            "value",
            "is_missing",
        ]
    )
    buffer = io.BytesIO()
    frame.to_parquet(buffer, index=False)
    writer.put_object(raw_macro_path("2024-01-02"), buffer.getvalue())


def _write_empty_fundamentals(writer: R2Writer, ticker: str) -> None:
    """Persist an empty fundamentals shard for one ticker."""
    frame = pd.DataFrame(
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
    )
    buffer = io.BytesIO()
    frame.to_parquet(buffer, index=False)
    writer.put_object(raw_fundamentals_path(ticker), buffer.getvalue())


def _write_daily_feature_artifact(
    writer: R2Writer,
    *,
    stage: str,
    branch_name: str,
    as_of_date: str,
    run_id: str,
    records: list[FeatureRecord],
    finished_at: datetime,
) -> None:
    """Persist one completed daily feature artifact plus its manifest."""
    if branch_name == "sentiment":
        output_key = layer1_sentiment_feature_path(as_of_date, run_id)
    elif branch_name == "topics":
        output_key = layer1_topic_feature_path(as_of_date, run_id)
    else:
        raise ValueError(f"Unsupported branch_name: {branch_name}")

    writer.put_object(output_key, feature_records_to_parquet_bytes(records))
    manifest = PipelineManifestRecord(
        run_id=run_id,
        stage=stage,
        status=RunStatus.COMPLETED,
        started_at=finished_at,
        finished_at=finished_at,
        output_path=output_key,
        metadata={"as_of_date": as_of_date},
    )
    writer.put_object(
        pipeline_manifest_path(stage, run_id),
        manifest.model_dump_json().encode("utf-8"),
    )


def _write_regime_artifact(
    writer: R2Writer,
    *,
    run_id: str,
    rows: list[dict[str, object]],
    train_end_date: str,
    inference_dates: tuple[str, ...],
    finished_at: datetime,
) -> None:
    """Persist one completed regime artifact plus its manifest."""
    buffer = io.BytesIO()
    pd.DataFrame(rows).to_parquet(buffer, index=False)
    output_key = layer1_regime_path(run_id)
    writer.put_object(output_key, buffer.getvalue())
    manifest = PipelineManifestRecord(
        run_id=run_id,
        stage=REGIME_STAGE,
        status=RunStatus.COMPLETED,
        started_at=finished_at,
        finished_at=finished_at,
        output_path=output_key,
        metadata={
            "train_end_date": train_end_date,
            "inference_dates": list(inference_dates),
        },
    )
    writer.put_object(
        pipeline_manifest_path(REGIME_STAGE, run_id),
        manifest.model_dump_json().encode("utf-8"),
    )


def test_backfill_layer1_writes_feature_histories_and_manifest(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The backfill writes one feature history per ticker plus a manifest."""
    writer = _local_writer(tmp_path, monkeypatch)
    _write_synthetic_ohlcv(writer, "AAPL", num_bars=30)
    _write_synthetic_ohlcv(writer, "SPY", num_bars=30)
    _write_empty_macro_shard(writer)
    _write_empty_fundamentals(writer, "AAPL")

    config = Layer1BackfillConfig(run_id="layer1-test", tickers=("AAPL",))
    fixed_now = datetime(2024, 2, 15, 12, 0, tzinfo=UTC)
    result = backfill_layer1(config, writer=writer, now=fixed_now)

    assert result.tickers_processed == 1
    assert result.ticker_files_written == 1
    assert result.feature_rows_written > 0
    feature_keys = writer.list_keys("features/layer1/")
    assert feature_keys == [layer1_ticker_history_path("AAPL")]
    loaded_records = read_feature_records("AAPL", writer=writer)
    assert len(loaded_records) == result.feature_rows_written
    manifest_payload = writer.get_object(result.manifest_key)
    payload = json.loads(manifest_payload.decode("utf-8"))
    assert payload["status"] == "completed"
    assert payload["metadata"]["ticker_files_written"] == 1
    assert payload["metadata"]["feature_rows_written"] == result.feature_rows_written
    assert payload["metadata"]["benchmark_ticker"] == "SPY"


def test_backfill_layer1_merges_optional_branch_outputs_into_final_histories(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Layer 1 histories include sentiment, topic, and regime branch features."""
    writer = _local_writer(tmp_path, monkeypatch)
    _write_synthetic_ohlcv(writer, "AAPL", num_bars=3)
    _write_synthetic_ohlcv(writer, "SPY", num_bars=3)
    _write_empty_macro_shard(writer)
    _write_empty_fundamentals(writer, "AAPL")

    finished_at = datetime(2024, 2, 14, 22, 0, tzinfo=UTC)
    for as_of_date, sentiment_score, topic_id, regime_label in (
        ("2024-01-02", 0.40, 7, "bull"),
        ("2024-01-03", 0.20, 3, "sideways"),
        ("2024-01-04", -0.10, 1, "bear"),
    ):
        _write_daily_feature_artifact(
            writer,
            stage=FINBERT_SENTIMENT_STAGE,
            branch_name="sentiment",
            as_of_date=as_of_date,
            run_id=f"sentiment-{as_of_date}",
            records=[
                FeatureRecord(
                    date=as_of_date,
                    ticker="AAPL",
                    features={
                        "nlp_sentiment_score": sentiment_score,
                        "nlp_sentence_count": 2,
                    },
                )
            ],
            finished_at=finished_at,
        )
        _write_daily_feature_artifact(
            writer,
            stage=TEXT_TOPICS_STAGE,
            branch_name="topics",
            as_of_date=as_of_date,
            run_id=f"topics-{as_of_date}",
            records=[
                FeatureRecord(
                    date=as_of_date,
                    ticker="AAPL",
                    features={
                        "nlp_sentence_count": 2,
                        "nlp_topic_count": 1,
                        "nlp_dominant_topic_id": topic_id,
                    },
                )
            ],
            finished_at=finished_at,
        )

    _write_regime_artifact(
        writer,
        run_id="regime-run",
        train_end_date="2024-01-01",
        inference_dates=("2024-01-02", "2024-01-03", "2024-01-04"),
        finished_at=finished_at,
        rows=[
            {
                "date": "2024-01-02",
                "regime_label": "bull",
                "regime_confidence": 0.80,
                "regime_prob_bear": 0.10,
                "regime_prob_sideways": 0.10,
                "regime_prob_bull": 0.80,
            },
            {
                "date": "2024-01-03",
                "regime_label": "sideways",
                "regime_confidence": 0.70,
                "regime_prob_bear": 0.15,
                "regime_prob_sideways": 0.70,
                "regime_prob_bull": 0.15,
            },
            {
                "date": "2024-01-04",
                "regime_label": "bear",
                "regime_confidence": 0.90,
                "regime_prob_bear": 0.90,
                "regime_prob_sideways": 0.05,
                "regime_prob_bull": 0.05,
            },
        ],
    )

    backfill_layer1(Layer1BackfillConfig(run_id="layer1-optional", tickers=("AAPL",)), writer=writer)

    records = read_feature_records("AAPL", writer=writer)
    by_date = {record.date: record.features for record in records}
    assert by_date["2024-01-02"]["nlp_sentiment_score"] == 0.40
    assert by_date["2024-01-02"]["nlp_topic_count"] == 1
    assert by_date["2024-01-02"]["nlp_dominant_topic_id"] == 7
    assert by_date["2024-01-02"]["regime_label"] == "bull"
    assert by_date["2024-01-04"]["regime_prob_bear"] == 0.90


def test_backfill_layer1_skips_tickers_with_no_ohlcv(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A ticker without an OHLCV archive is skipped without aborting the run."""
    writer = _local_writer(tmp_path, monkeypatch)
    _write_synthetic_ohlcv(writer, "SPY", num_bars=30)
    _write_empty_macro_shard(writer)

    config = Layer1BackfillConfig(run_id="layer1-skip", tickers=("AAPL",))
    fixed_now = datetime(2024, 2, 15, 12, 0, tzinfo=UTC)
    result = backfill_layer1(config, writer=writer, now=fixed_now)

    assert result.ticker_files_written == 0
    assert result.feature_rows_written == 0
    manifest_payload = writer.get_object(
        pipeline_manifest_path(LAYER1_BACKFILL_STAGE, "layer1-skip"),
    )
    payload = json.loads(manifest_payload.decode("utf-8"))
    assert payload["status"] == "completed"


def test_backfill_layer1_fails_closed_when_optional_branch_is_required(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Configured required optional branches abort the run when no artifact is available."""
    writer = _local_writer(tmp_path, monkeypatch)
    _write_synthetic_ohlcv(writer, "AAPL", num_bars=5)
    _write_synthetic_ohlcv(writer, "SPY", num_bars=5)
    _write_empty_macro_shard(writer)
    _write_empty_fundamentals(writer, "AAPL")

    with pytest.raises(FileNotFoundError, match="No completed sentiment artifacts"):
        backfill_layer1(
            Layer1BackfillConfig(
                run_id="layer1-require-sentiment",
                tickers=("AAPL",),
                require_sentiment_features=True,
            ),
            writer=writer,
        )

    manifest_payload = writer.get_object(
        pipeline_manifest_path(LAYER1_BACKFILL_STAGE, "layer1-require-sentiment"),
    )
    payload = json.loads(manifest_payload.decode("utf-8"))
    assert payload["status"] == "failed"


def test_backfill_layer1_rejects_duplicate_optional_branch_rows(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Duplicate `(date, ticker)` rows inside one optional branch artifact are rejected."""
    writer = _local_writer(tmp_path, monkeypatch)
    _write_synthetic_ohlcv(writer, "AAPL", num_bars=5)
    _write_synthetic_ohlcv(writer, "SPY", num_bars=5)
    _write_empty_macro_shard(writer)
    _write_empty_fundamentals(writer, "AAPL")
    _write_daily_feature_artifact(
        writer,
        stage=FINBERT_SENTIMENT_STAGE,
        branch_name="sentiment",
        as_of_date="2024-01-02",
        run_id="sentiment-dup",
        records=[
            FeatureRecord(
                date="2024-01-02",
                ticker="AAPL",
                features={"nlp_sentiment_score": 0.4},
            ),
            FeatureRecord(
                date="2024-01-02",
                ticker="AAPL",
                features={"nlp_sentiment_score": 0.2},
            ),
        ],
        finished_at=datetime(2024, 2, 14, 22, 0, tzinfo=UTC),
    )

    with pytest.raises(ValueError, match="Duplicate sentiment rows found"):
        backfill_layer1(Layer1BackfillConfig(run_id="layer1-dup", tickers=("AAPL",)), writer=writer)


def test_backfill_layer1_rejects_cross_date_optional_branch_rows(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Daily optional branch artifacts cannot leak rows from another trading date."""
    writer = _local_writer(tmp_path, monkeypatch)
    _write_synthetic_ohlcv(writer, "AAPL", num_bars=5)
    _write_synthetic_ohlcv(writer, "SPY", num_bars=5)
    _write_empty_macro_shard(writer)
    _write_empty_fundamentals(writer, "AAPL")
    _write_daily_feature_artifact(
        writer,
        stage=TEXT_TOPICS_STAGE,
        branch_name="topics",
        as_of_date="2024-01-02",
        run_id="topics-leak",
        records=[
            FeatureRecord(
                date="2024-01-03",
                ticker="AAPL",
                features={"nlp_sentence_count": 1, "nlp_topic_count": 1},
            )
        ],
        finished_at=datetime(2024, 2, 14, 22, 0, tzinfo=UTC),
    )

    with pytest.raises(ValueError, match="outside as_of_date=2024-01-02"):
        backfill_layer1(
            Layer1BackfillConfig(run_id="layer1-cross-date", tickers=("AAPL",)),
            writer=writer,
        )


def test_backfill_layer1_writes_failed_manifest_on_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failure during processing still emits a failed manifest before raising."""
    writer = _local_writer(tmp_path, monkeypatch)
    _write_synthetic_ohlcv(writer, "AAPL", num_bars=5)
    _write_empty_macro_shard(writer)
    _write_empty_fundamentals(writer, "AAPL")

    def _exploding_market_features(*args, **kwargs):
        raise RuntimeError("simulated market-feature failure")

    monkeypatch.setattr(
        "app.lab.data_pipelines.backfill_layer1.compute_market_features",
        _exploding_market_features,
    )

    config = Layer1BackfillConfig(run_id="layer1-fail", tickers=("AAPL",))
    fixed_now = datetime(2024, 2, 15, 12, 0, tzinfo=UTC)

    with pytest.raises(RuntimeError, match="simulated market-feature failure"):
        backfill_layer1(config, writer=writer, now=fixed_now)

    manifest_payload = writer.get_object(
        pipeline_manifest_path(LAYER1_BACKFILL_STAGE, "layer1-fail"),
    )
    payload = json.loads(manifest_payload.decode("utf-8"))
    assert payload["status"] == "failed"


def test_layer1_backfill_config_rejects_invalid_inputs() -> None:
    """Layer1BackfillConfig validates run_id and ticker contents."""
    with pytest.raises(ValueError, match="run_id"):
        Layer1BackfillConfig(run_id="", tickers=("AAPL",))
    with pytest.raises(ValueError, match="at least one ticker"):
        Layer1BackfillConfig(run_id="run", tickers=())
    with pytest.raises(ValueError, match="tickers cannot contain empty"):
        Layer1BackfillConfig(run_id="run", tickers=("AAPL", "  "))


def test_resolve_tickers_supports_inline_and_file_inputs(tmp_path: Path) -> None:
    """The CLI helper accepts inline CSV and @path/to/file.json inputs."""
    inline = _resolve_tickers("aapl, MSFT, googl")
    assert inline == ("AAPL", "MSFT", "GOOGL")

    json_path = tmp_path / "tickers.json"
    json_path.write_text(json.dumps(["spy", "qqq"]), encoding="utf-8")
    file_based = _resolve_tickers(f"@{json_path}")
    assert file_based == ("SPY", "QQQ")
