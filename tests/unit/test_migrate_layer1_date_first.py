from __future__ import annotations

from pathlib import Path

import pytest

from core.contracts.schemas import FeatureRecord
from core.features.io import (
    feature_record_to_parquet_bytes,
    read_feature_record,
    write_feature_records,
)
from scripts.migrate_layer1_date_first import (
    collect_legacy_layer1_feature_keys,
    migrate_layer1_date_first,
)
from services.r2.paths import layer1_feature_path
from services.r2.writer import R2Writer


def test_collect_legacy_layer1_feature_keys_ignores_intermediate_artifacts() -> None:
    """Only legacy history and legacy date/ticker feature shards are migration inputs."""
    history_keys, shard_keys = collect_legacy_layer1_feature_keys(
        [
            "features/layer1/AAPL.parquet",
            "features/layer1/2024-01-02/AAPL.parquet",
            "features/layer1/text_embeddings/2024-01-02/run.parquet",
            "features/2024-01-02/AAPL.parquet",
        ]
    )

    assert history_keys == ["features/layer1/AAPL.parquet"]
    assert shard_keys == ["features/layer1/2024-01-02/AAPL.parquet"]


def test_migrate_layer1_date_first_writes_canonical_shards(tmp_path: Path) -> None:
    """Legacy per-ticker histories are copied into date-first feature shards."""
    writer = R2Writer(local_root=tmp_path)
    write_feature_records(
        [
            FeatureRecord(date="2024-01-02", ticker="AAPL", features={"returns_1d": 0.01}),
            FeatureRecord(date="2024-01-03", ticker="AAPL", features={"returns_1d": 0.02}),
        ],
        writer=writer,
    )

    result = migrate_layer1_date_first(writer=writer)
    first_record = read_feature_record("2024-01-02", "AAPL", writer=writer)

    assert result.legacy_history_files_found == 1
    assert result.date_first_shards_written == 2
    assert writer.exists(layer1_feature_path("2024-01-03", "AAPL")) is True
    assert first_record.features == {"returns_1d": 0.01}


def test_migrate_layer1_date_first_dry_run_does_not_write(tmp_path: Path) -> None:
    """Dry runs report counts without writing canonical date-first shards."""
    writer = R2Writer(local_root=tmp_path)
    writer.put_object(
        "features/layer1/2024-01-02/AAPL.parquet",
        feature_record_to_parquet_bytes(
            FeatureRecord(date="2024-01-02", ticker="AAPL", features={"returns_1d": 0.01})
        ),
    )

    result = migrate_layer1_date_first(writer=writer, dry_run=True)

    assert result.legacy_dated_shards_found == 1
    assert result.date_first_shards_written == 0
    assert writer.exists(layer1_feature_path("2024-01-02", "AAPL")) is False


def test_migrate_layer1_date_first_rejects_conflicting_duplicate_records(
    tmp_path: Path,
) -> None:
    """Conflicting legacy rows for the same date/ticker fail instead of picking one."""
    writer = R2Writer(local_root=tmp_path)
    write_feature_records(
        [FeatureRecord(date="2024-01-02", ticker="AAPL", features={"returns_1d": 0.01})],
        writer=writer,
    )
    writer.put_object(
        "features/layer1/2024-01-02/AAPL.parquet",
        feature_record_to_parquet_bytes(
            FeatureRecord(date="2024-01-02", ticker="AAPL", features={"returns_1d": 0.02})
        ),
    )

    with pytest.raises(ValueError, match="Conflicting Layer 1 records"):
        migrate_layer1_date_first(writer=writer)
