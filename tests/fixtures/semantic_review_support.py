"""Semantic review dashboard test fixtures and R2 seeding helpers."""
from __future__ import annotations

import json
from io import BytesIO
from pathlib import Path
from typing import Any

import pandas as pd

from core.contracts.schemas import PipelineManifestRecord, RunStatus
from services.r2.paths import (
    layer1_news_preprocessing_path,
    layer1_news_relevance_gate_path,
    layer1_regime_path,
    layer1_sentiment_feature_path,
    layer1_sentiment_score_path,
    layer1_text_embedding_path,
    layer1_topic_label_path,
    pipeline_manifest_path,
    raw_price_path,
)
from services.r2.writer import R2Writer

_FIXTURE_PATH = Path(__file__).resolve().parent / "semantic_review" / "semantic_review_fixture.json"


def load_semantic_review_fixture() -> dict[str, Any]:
    """Load the semantic-review fixture payload from disk."""
    with _FIXTURE_PATH.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def build_semantic_review_frames() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Return scored-news, regime, and benchmark price frames for the fixture."""
    fixture = load_semantic_review_fixture()
    scored_frame = pd.DataFrame(fixture["scored_rows"])
    regime_frame = pd.DataFrame(fixture["regime_rows"])
    benchmark_frame = pd.DataFrame(fixture.get("benchmark_price_rows", []))
    return scored_frame, regime_frame, benchmark_frame


def seed_semantic_review_fixture(
    *,
    local_root: Path,
    run_id: str | None = None,
    include_benchmark_price_rows: bool = True,
) -> dict[str, Any]:
    """Write the semantic-review fixture into a local mock R2 tree."""
    fixture = load_semantic_review_fixture()
    active_run_id = run_id or str(fixture["run_id"])
    writer = R2Writer(local_root=local_root)
    scored_frame = pd.DataFrame(fixture["scored_rows"])
    regime_frame = pd.DataFrame(fixture["regime_rows"])
    benchmark_price_frame = pd.DataFrame(fixture.get("benchmark_price_rows", []))
    regime_frame = _regime_frame_with_review_metadata(regime_frame)
    preprocessing_frame = _preprocessing_frame(scored_frame)
    embedding_frame = _embedding_frame(scored_frame)
    topic_label_frame = _topic_label_frame(scored_frame)
    relevance_gate_frame = _relevance_gate_frame(scored_frame)
    semantic_aggregate_frame = _semantic_aggregate_frame(scored_frame)

    for date_text, date_frame in scored_frame.groupby("date", sort=True):
        parquet_bytes = _dataframe_to_parquet_bytes(date_frame)
        writer.put_object(layer1_sentiment_score_path(str(date_text), active_run_id), parquet_bytes)
        writer.put_object(
            layer1_news_preprocessing_path(str(date_text), active_run_id),
            _dataframe_to_parquet_bytes(preprocessing_frame[preprocessing_frame["date"] == date_text]),
        )
        writer.put_object(
            layer1_text_embedding_path(str(date_text), active_run_id),
            _dataframe_to_parquet_bytes(embedding_frame[embedding_frame["date"] == date_text]),
        )
        writer.put_object(
            layer1_topic_label_path(str(date_text), active_run_id),
            _dataframe_to_parquet_bytes(topic_label_frame[topic_label_frame["date"] == date_text]),
        )
        writer.put_object(
            layer1_news_relevance_gate_path(str(date_text), active_run_id),
            _dataframe_to_parquet_bytes(relevance_gate_frame[relevance_gate_frame["date"] == date_text]),
        )
        writer.put_object(
            layer1_sentiment_feature_path(str(date_text), active_run_id),
            _dataframe_to_parquet_bytes(
                semantic_aggregate_frame[semantic_aggregate_frame["date"] == date_text]
            ),
        )

    for date_text, date_frame in regime_frame.groupby("date", sort=True):
        output_key = layer1_regime_path(str(date_text), active_run_id)
        writer.put_object(output_key, _dataframe_to_parquet_bytes(date_frame))
    writer.put_object(
        pipeline_manifest_path("layer1_5_regime", active_run_id),
        _regime_manifest_json(active_run_id, sorted(regime_frame["date"].astype(str).unique())),
    )
    writer.put_object(raw_price_path("AAPL"), _dataframe_to_parquet_bytes(_price_frame()))
    if include_benchmark_price_rows and not benchmark_price_frame.empty:
        writer.put_object(raw_price_path("SPY"), _dataframe_to_parquet_bytes(benchmark_price_frame))
    return {
        "run_id": active_run_id,
        "writer": writer,
        "scored_rows": scored_frame,
        "regime_rows": regime_frame,
        "benchmark_price_rows": benchmark_price_frame,
        "local_root": local_root,
    }


def _dataframe_to_parquet_bytes(frame: pd.DataFrame) -> bytes:
    """Serialize a DataFrame to parquet bytes."""
    buffer = BytesIO()
    frame.to_parquet(buffer, index=False)
    return buffer.getvalue()


def _regime_frame_with_review_metadata(frame: pd.DataFrame) -> pd.DataFrame:
    """Return fixture HMM rows with readiness metadata used by the dashboard."""
    enriched = frame.copy()
    enriched["regime_readiness_status"] = "ready"
    enriched["regime_readiness_reason"] = "ready"
    enriched["regime_required_for_layer2"] = True
    enriched["regime_missing_features"] = ""
    enriched["regime_probability_sum"] = (
        enriched["prob_bear"] + enriched["prob_sideways"] + enriched["prob_bull"]
    )
    enriched["training_rows"] = 42
    enriched["complete_training_rows"] = 40
    enriched["min_training_rows"] = 30
    return enriched


def _regime_manifest_json(run_id: str, inference_dates: list[str]) -> str:
    """Return a completed HMM manifest with fixture training-window metadata."""
    metadata = {
        "benchmark_ticker": "SPY",
        "train_start_date": "2026-02-02",
        "train_end_date": "2026-05-20",
        "inference_dates": inference_dates,
        "ready_inference_dates": inference_dates,
        "warning_inference_dates": [],
        "macro_load_start_date": "2026-02-02",
        "macro_load_end_date": inference_dates[-1],
        "training_rows": 42,
        "complete_training_rows": 40,
        "dropped_feature_columns": [],
        "regime_layer2_ready": True,
        "regime_readiness_by_date": {
            date_text: {
                "status": "ready",
                "reason": "ready",
                "required_for_layer2": True,
                "missing_features": [],
                "probability_sum": 1.0,
            }
            for date_text in inference_dates
        },
    }
    manifest = PipelineManifestRecord(
        run_id=run_id,
        stage="layer1_5_regime",
        status=RunStatus.COMPLETED,
        started_at=pd.Timestamp("2026-05-22T20:00:00Z").to_pydatetime(),
        finished_at=pd.Timestamp("2026-05-22T20:05:00Z").to_pydatetime(),
        input_path=f"{raw_price_path('SPY')},raw/macro/",
        output_path=layer1_regime_path(inference_dates[0], run_id),
        metadata=metadata,
    )
    return manifest.model_dump_json()


def _price_frame() -> pd.DataFrame:
    """Return raw AAPL price rows for the semantic-review fixture window."""
    rows = [
        {
            "date": "2026-05-21",
            "ticker": "AAPL",
            "open": 189.5,
            "high": 193.2,
            "low": 188.8,
            "close": 192.4,
            "adj_close": 192.4,
            "volume": 52000000,
            "dollar_volume": 10004800000.0,
        },
        {
            "date": "2026-05-22",
            "ticker": "AAPL",
            "open": 192.2,
            "high": 195.0,
            "low": 191.6,
            "close": 194.8,
            "adj_close": 194.8,
            "volume": 48500000,
            "dollar_volume": 9447800000.0,
        },
    ]
    return pd.DataFrame(rows)


def _preprocessing_frame(scored_frame: pd.DataFrame) -> pd.DataFrame:
    """Return representative ticker/entity preprocessing rows."""
    frame = scored_frame.copy()
    frame["normalized_headline"] = frame["headline"].str.lower().str.replace(r"[^a-z0-9]+", " ", regex=True).str.strip()
    frame["chunk_index"] = frame["sentence_index"]
    frame["source_text_field"] = "body"
    frame["source_text_order"] = frame["sentence_index"]
    frame["source_text_provenance"] = frame.apply(
        lambda row: json.dumps(
            {
                "article_id": row["article_id"],
                "article_tickers": [row["ticker"]],
                "chunk_tickers": [row["ticker"]] if "Ferrari" not in row["headline"] else [],
                "entity_mentions": ["Apple"] if row["ticker"] == "AAPL" and "Ferrari" not in row["headline"] else [],
                "raw_headline": row["headline"],
            },
            sort_keys=True,
        ),
        axis=1,
    )
    frame["ticker_mentions"] = frame.apply(
        lambda row: json.dumps([row["ticker"]] if "Ferrari" not in row["headline"] else []),
        axis=1,
    )
    frame["entity_mentions"] = frame.apply(
        lambda row: json.dumps(["Apple"] if "Ferrari" not in row["headline"] else ["Ferrari"]),
        axis=1,
    )
    return frame[
        [
            "date",
            "ticker",
            "headline",
            "normalized_headline",
            "text",
            "article_id",
            "sentence_index",
            "chunk_index",
            "source",
            "url",
            "published_at",
            "source_text_field",
            "source_text_order",
            "source_text_provenance",
            "ticker_mentions",
            "entity_mentions",
        ]
    ]


def _embedding_frame(scored_frame: pd.DataFrame) -> pd.DataFrame:
    """Return representative article embedding cache rows."""
    rows: list[dict[str, object]] = []
    for article_id, group in scored_frame.groupby("article_id", sort=True):
        first = group.iloc[0]
        rows.append(
            {
                "date": first["date"],
                "article_id": article_id,
                "normalized_headline": str(first["headline"]).lower(),
                "text": " ".join(group["text"].astype(str).tolist()),
                "article_sentence_count": int(len(group)),
                "embedding_model": "sentence-transformers/test",
                "embedding_revision": "rev-1",
                "embedding_cache_key": f"embed-{article_id}",
                "embedding_json": "[0.1,0.2,0.3]",
            }
        )
    return pd.DataFrame(rows)


def _topic_label_frame(scored_frame: pd.DataFrame) -> pd.DataFrame:
    """Return representative BERTopic article labels."""
    rows: list[dict[str, object]] = []
    for index, (article_id, group) in enumerate(scored_frame.groupby("article_id", sort=True)):
        first = group.iloc[0]
        rows.append(
            {
                "date": first["date"],
                "ticker": first["ticker"],
                "article_id": article_id,
                "normalized_headline": str(first["headline"]).lower(),
                "text": " ".join(group["text"].astype(str).tolist()),
                "article_sentence_count": int(len(group)),
                "embedding_cache_key": f"embed-{article_id}",
                "topic_model": "bertopic-test",
                "topic_model_version": "v1",
                "topic_id": index % 2,
                "topic_probability": 0.82,
                "topic_label": "earnings and demand",
                "topic_keywords": json.dumps(["earnings", "demand", "iphone"]),
            }
        )
    return pd.DataFrame(rows)


def _relevance_gate_frame(scored_frame: pd.DataFrame) -> pd.DataFrame:
    """Return representative pre-FinBERT relevance-gate audit rows."""
    rows: list[dict[str, object]] = []
    for _, row in scored_frame.iterrows():
        accepted = "Ferrari" not in row["headline"]
        rows.append(
            {
                "date": row["date"],
                "ticker": row["ticker"],
                "article_id": row["article_id"],
                "sentence_index": row["sentence_index"],
                "chunk_index": row["sentence_index"],
                "headline": row["headline"],
                "text": row["text"],
                "source": row["source"],
                "published_at": row["published_at"],
                "relevance_decision": "accepted" if accepted else "rejected",
                "relevance_score": row["relevance_score"],
                "ticker_relevance_score": 1.0 if accepted else 0.0,
                "financial_relevance_score": 0.8,
                "topic_relevance_score": 0.82,
                "reason_codes": json.dumps(
                    ["target_entity_mention"] if accepted else ["low_ticker_relevance"]
                ),
                "ticker_evidence": json.dumps({"source_tickers": [row["ticker"]]}),
                "entity_evidence": json.dumps(["Apple"] if accepted else ["Ferrari"]),
                "topic_id": 1,
                "topic_probability": 0.82,
                "embedding_cache_key": f"embed-{row['article_id']}",
                "has_embedding": True,
            }
        )
    return pd.DataFrame(rows)


def _semantic_aggregate_frame(scored_frame: pd.DataFrame) -> pd.DataFrame:
    """Return representative source-weighted semantic aggregate rows."""
    rows: list[dict[str, object]] = []
    for date_text, group in scored_frame.groupby("date", sort=True):
        features = {
            "nlp_sentiment_score": float(group["sentiment_score"].mean()),
            "nlp_article_count": int(group["article_id"].nunique()),
            "nlp_sentence_count": int(len(group)),
            "nlp_relevance_score": float(group["relevance_score"].mean()),
            "nlp_source_weight_mean": 1.25,
            "nlp_source_weight_sum": 2.5,
            "nlp_effective_weight_sum": 2.0,
            "nlp_relevance_accepted_count": int(len(group)),
            "nlp_relevance_borderline_count": 0,
            "nlp_contributing_article_ids": json.dumps(sorted(group["article_id"].unique())),
            "nlp_topic_sentiment_summary": json.dumps(
                [{"topic_id": 1, "sentiment_score": float(group["sentiment_score"].mean())}]
            ),
            "nlp_source_weight_summary": json.dumps(
                [{"source": "benzinga", "source_weight": 1.25, "sentence_count": int(len(group))}]
            ),
            "nlp_relevance_reason_codes": json.dumps(["target_entity_mention"]),
            "nlp_semantic_warning_codes": json.dumps([]),
        }
        rows.append({"date": date_text, "ticker": "AAPL", "features": json.dumps(features)})
    return pd.DataFrame(rows)
