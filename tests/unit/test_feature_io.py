from __future__ import annotations

import io
from pathlib import Path

import pandas as pd
import pytest
from pydantic import ValidationError

import core.features.io as feature_io
from core.contracts.schemas import FeatureRecord
from services.r2.writer import R2Writer


def test_write_feature_record_round_trips_through_local_r2(tmp_path: Path) -> None:
    """Feature shards should round-trip through the local mock R2 backend."""
    writer = R2Writer(local_root=tmp_path)
    record = FeatureRecord(
        date="2026-04-21",
        ticker="AAPL",
        features={
            "returns_1d": 0.0125,
            "news_count_7d": 3,
            "regime_label": "bull",
            "has_fresh_filing": True,
            "days_to_earnings": None,
        },
    )

    key = feature_io.write_feature_record(record, writer=writer)
    loaded_record = feature_io.read_feature_record("2026-04-21", "AAPL", writer=writer)

    assert key == "features/layer1/2026-04-21/AAPL.parquet"
    assert writer.exists(key) is True
    assert loaded_record == record


def test_write_feature_record_rejects_non_conforming_rows(tmp_path: Path) -> None:
    """Feature shard writes should fail fast on invalid FeatureRecord payloads."""
    writer = R2Writer(local_root=tmp_path)

    with pytest.raises(ValidationError):
        feature_io.write_feature_record(
            {
                "date": "2026-04-21",
                "ticker": "AAPL",
                "features": {"returns_1d": [0.01]},
            },
            writer=writer,
        )


def test_parquet_bytes_to_feature_record_rejects_multi_row_archives() -> None:
    """One shard file must not contain multiple FeatureRecord rows."""
    frame = pd.DataFrame(
        [
            {"date": "2026-04-21", "ticker": "AAPL", "features": "{\"returns_1d\":0.01}"},
            {"date": "2026-04-21", "ticker": "MSFT", "features": "{\"returns_1d\":0.02}"},
        ]
    )
    buffer = io.BytesIO()
    frame.to_parquet(buffer, index=False)

    with pytest.raises(ValueError, match="exactly one FeatureRecord row"):
        feature_io.parquet_bytes_to_feature_record(buffer.getvalue())


def test_feature_record_serializer_reports_missing_pyarrow(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Feature shard serialization should fail clearly when pyarrow is unavailable."""
    record = FeatureRecord(
        date="2026-04-21",
        ticker="AAPL",
        features={"returns_1d": 0.01},
    )
    real_import_module = feature_io.importlib.import_module

    def fake_import_module(name: str, package: str | None = None) -> object:
        if name == "pyarrow":
            raise ModuleNotFoundError("No module named 'pyarrow'")
        return real_import_module(name, package)

    monkeypatch.setattr(feature_io.importlib, "import_module", fake_import_module)

    with pytest.raises(ModuleNotFoundError, match="pandas and pyarrow are required"):
        feature_io.feature_record_to_parquet_bytes(record)
