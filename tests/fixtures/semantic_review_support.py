"""Semantic review dashboard test fixtures and R2 seeding helpers."""
from __future__ import annotations

import json
from io import BytesIO
from pathlib import Path
from typing import Any

import pandas as pd

from services.r2.paths import layer1_regime_path, layer1_sentiment_score_path
from services.r2.writer import R2Writer

_FIXTURE_PATH = Path(__file__).resolve().parent / "semantic_review" / "semantic_review_fixture.json"


def load_semantic_review_fixture() -> dict[str, Any]:
    """Load the semantic-review fixture payload from disk."""
    with _FIXTURE_PATH.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def build_semantic_review_frames() -> tuple[pd.DataFrame, pd.DataFrame]:
    """Return scored-news and regime frames for the semantic-review fixture."""
    fixture = load_semantic_review_fixture()
    scored_frame = pd.DataFrame(fixture["scored_rows"])
    regime_frame = pd.DataFrame(fixture["regime_rows"])
    return scored_frame, regime_frame


def seed_semantic_review_fixture(
    *,
    local_root: Path,
    run_id: str | None = None,
) -> dict[str, Any]:
    """Write the semantic-review fixture into a local mock R2 tree."""
    fixture = load_semantic_review_fixture()
    active_run_id = run_id or str(fixture["run_id"])
    writer = R2Writer(local_root=local_root)
    scored_frame = pd.DataFrame(fixture["scored_rows"])
    regime_frame = pd.DataFrame(fixture["regime_rows"])

    for date_text, date_frame in scored_frame.groupby("date", sort=True):
        parquet_bytes = _dataframe_to_parquet_bytes(date_frame)
        writer.put_object(layer1_sentiment_score_path(str(date_text), active_run_id), parquet_bytes)

    writer.put_object(
        layer1_regime_path(active_run_id),
        _dataframe_to_parquet_bytes(regime_frame),
    )
    return {
        "run_id": active_run_id,
        "writer": writer,
        "scored_rows": scored_frame,
        "regime_rows": regime_frame,
        "local_root": local_root,
    }


def _dataframe_to_parquet_bytes(frame: pd.DataFrame) -> bytes:
    """Serialize a DataFrame to parquet bytes."""
    buffer = BytesIO()
    frame.to_parquet(buffer, index=False)
    return buffer.getvalue()
