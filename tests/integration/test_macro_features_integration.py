from __future__ import annotations

import io
from pathlib import Path

import pandas as pd
import pytest

from core.features import compute_macro_features, load_macro_frame
from services.r2 import client as r2_client
from services.r2.paths import raw_macro_path
from services.r2.writer import R2Writer


def _write_macro_day(
    writer: R2Writer,
    observation_date: str,
    rows: list[dict[str, object]],
) -> None:
    """Persist one synthetic FRED macro parquet shard under the local R2 mock."""
    buffer = io.BytesIO()
    pd.DataFrame(rows).to_parquet(buffer, index=False)
    writer.put_object(raw_macro_path(observation_date), buffer.getvalue())


def _macro_row(
    *,
    series_id: str,
    observation_date: str,
    realtime_start: str,
    value: float,
) -> dict[str, object]:
    """Build one normalized macro archive row."""
    return {
        "source": "fred",
        "series_id": series_id,
        "observation_date": observation_date,
        "realtime_start": realtime_start,
        "realtime_end": realtime_start,
        "retrieved_at": "2024-01-01T00:00:00+00:00",
        "value": value,
        "is_missing": False,
        "raw": {"fixture": True},
    }


def test_macro_features_round_trip_through_local_r2_without_same_day_leakage(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Layer 1 macro features read R2 archives and avoid lagged-release same-day leakage."""
    for env_name in r2_client.REQUIRED_R2_ENV_VARS:
        monkeypatch.delenv(env_name, raising=False)
    monkeypatch.setattr(r2_client, "R2_ENV_FILE", tmp_path / "missing-r2.env")

    writer = R2Writer(local_root=tmp_path)
    _write_macro_day(
        writer,
        "2024-02-01",
        [
            _macro_row(
                series_id="CPIAUCSL",
                observation_date="2024-02-01",
                realtime_start="2024-03-12",
                value=310.0,
            )
        ],
    )
    _write_macro_day(
        writer,
        "2024-03-01",
        [
            _macro_row(
                series_id="CPIAUCSL",
                observation_date="2024-03-01",
                realtime_start="2024-04-10",
                value=312.0,
            )
        ],
    )
    _write_macro_day(
        writer,
        "2024-04-09",
        [
            _macro_row(
                series_id="DGS10",
                observation_date="2024-04-09",
                realtime_start="2024-04-09",
                value=4.42,
            )
        ],
    )

    macro = load_macro_frame(writer=writer)
    features = compute_macro_features(macro, ["2024-04-10", "2024-04-11"])

    assert features.loc[0, "cpi_level"] == pytest.approx(310.0)
    assert features.loc[1, "cpi_level"] == pytest.approx(312.0)
    assert features.loc[0, "treasury_10y"] == pytest.approx(4.42)
    assert features.loc[1, "treasury_10y"] == pytest.approx(4.42)
