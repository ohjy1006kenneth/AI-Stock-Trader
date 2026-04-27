from __future__ import annotations

from pathlib import Path

from core.contracts.schemas import FeatureRecord
from core.features.io import read_feature_records, write_feature_record
from scripts.migrate_layer1_per_ticker import (
    collect_legacy_layer1_shards,
    migrate_layer1_per_ticker,
)
from services.r2.paths import layer1_ticker_history_path
from services.r2.writer import R2Writer


def test_collect_legacy_layer1_shards_ignores_new_layout_and_text_artifacts() -> None:
    """Only date/ticker Layer 1 shard keys are selected for migration."""
    grouped = collect_legacy_layer1_shards(
        [
            "features/layer1/2024-01-02/AAPL.parquet",
            "features/layer1/AAPL.parquet",
            "features/layer1/text_embeddings/2024-01-02/run.parquet",
            "features/layer1/2024-01-03/AAPL.parquet",
            "features/layer1/2024-01-02/MSFT.parquet",
        ]
    )

    assert grouped == {
        "AAPL": [
            "features/layer1/2024-01-02/AAPL.parquet",
            "features/layer1/2024-01-03/AAPL.parquet",
        ],
        "MSFT": ["features/layer1/2024-01-02/MSFT.parquet"],
    }


def test_migrate_layer1_per_ticker_writes_histories(tmp_path: Path) -> None:
    """Legacy date/ticker shards are packed into one history file per ticker."""
    writer = R2Writer(local_root=tmp_path)
    write_feature_record(
        FeatureRecord(date="2024-01-02", ticker="AAPL", features={"returns_1d": 0.01}),
        writer=writer,
    )
    write_feature_record(
        FeatureRecord(date="2024-01-03", ticker="AAPL", features={"returns_1d": 0.02}),
        writer=writer,
    )

    result = migrate_layer1_per_ticker(writer=writer)
    records = read_feature_records("AAPL", writer=writer)

    assert result.legacy_shards_found == 2
    assert result.tickers_found == 1
    assert result.ticker_files_written == 1
    assert writer.exists(layer1_ticker_history_path("AAPL")) is True
    assert [record.date for record in records] == ["2024-01-02", "2024-01-03"]


def test_migrate_layer1_per_ticker_dry_run_does_not_write(tmp_path: Path) -> None:
    """Dry runs report legacy shard counts without creating history files."""
    writer = R2Writer(local_root=tmp_path)
    write_feature_record(
        FeatureRecord(date="2024-01-02", ticker="AAPL", features={"returns_1d": 0.01}),
        writer=writer,
    )

    result = migrate_layer1_per_ticker(writer=writer, dry_run=True)

    assert result.legacy_shards_found == 1
    assert result.ticker_files_written == 0
    assert writer.exists(layer1_ticker_history_path("AAPL")) is False
