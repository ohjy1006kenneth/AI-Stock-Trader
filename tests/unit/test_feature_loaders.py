from __future__ import annotations

import io
from pathlib import Path

import pandas as pd
import pytest

from core.features.loaders import load_macro_frame
from services.r2.paths import raw_macro_path
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
