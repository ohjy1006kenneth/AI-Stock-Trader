from __future__ import annotations

import io
from pathlib import Path

import pandas as pd
import pytest

from core.features.loaders import load_macro_frame, load_order_book_frame
from services.r2.paths import raw_macro_path, raw_order_book_path
from tests.fixtures.layer1_support import local_writer


def test_load_macro_frame_deduplicates_equivalent_vintages_across_archive_dates(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Run-date and legacy observation-date shards should not duplicate one macro vintage."""
    writer = local_writer(tmp_path, monkeypatch)
    vintage = pd.DataFrame(
        [
            {
                "source": "fred",
                "series_id": "DGS10",
                "observation_date": "2026-05-12",
                "realtime_start": "2026-05-12",
                "realtime_end": "2026-05-12",
                "retrieved_at": "2026-05-12T20:00:00+00:00",
                "value": 4.42,
                "is_missing": False,
                "raw": {"series_id": "DGS10"},
            }
        ]
    )
    snapshot = vintage.assign(retrieved_at="2026-05-13T20:00:00+00:00")
    for key, frame in (
        (raw_macro_path("2026-05-12"), vintage),
        (raw_macro_path("2026-05-13"), snapshot),
    ):
        buffer = io.BytesIO()
        frame.to_parquet(buffer, index=False)
        writer.put_object(key, buffer.getvalue())

    frame = load_macro_frame(writer=writer)

    assert len(frame.index) == 1
    assert frame.loc[0, "series_id"] == "DGS10"
    assert frame.loc[0, "snapshot_date"] == "2026-05-13"
    assert frame.loc[0, "observation_date"] == "2026-05-12"
    assert frame.loc[0, "retrieved_at"] == "2026-05-13T20:00:00+00:00"


def test_load_order_book_frame_reads_one_provider_day_archive(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Layer 1 order-book loading stays R2-backed and provider/date scoped."""
    writer = local_writer(tmp_path, monkeypatch)
    archive = pd.DataFrame(
        [
            {
                "date": "2026-05-13",
                "ticker": "AAPL",
                "captured_at": "2026-05-13T14:15:00+00:00",
                "bid_price": 100.0,
                "ask_price": 100.05,
                "bid_size": 600,
                "ask_size": 400,
            }
        ]
    )
    buffer = io.BytesIO()
    archive.to_parquet(buffer, index=False)
    writer.put_object(raw_order_book_path("alpaca", "2026-05-13"), buffer.getvalue())

    frame = load_order_book_frame("alpaca", "2026-05-13", writer=writer)

    assert len(frame.index) == 1
    assert frame.loc[0, "ticker"] == "AAPL"
