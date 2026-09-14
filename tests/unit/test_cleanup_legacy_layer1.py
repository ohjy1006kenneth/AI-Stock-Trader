"""Tests for legacy Layer 1 cleanup in migrate_layer1_date_first."""

from __future__ import annotations

import tempfile
from pathlib import Path

from scripts.migrate_layer1_date_first import (
    LEGACY_LAYER1_PREFIX,
    Layer1CleanupResult,
    cleanup_legacy_layer1_keys,
)
from services.r2.paths import layer1_feature_path
from services.r2.writer import LocalR2Client


class MockObjectStore:
    """In-memory object store for testing."""

    def __init__(self) -> None:
        self._store: dict[str, bytes] = {}

    def put(self, key: str, data: bytes) -> None:
        self._store[key] = data

    def list_keys(self, prefix: str) -> list[str]:
        return sorted(key for key in self._store if key.startswith(prefix))

    def get_object(self, key: str) -> bytes:
        return self._store[key]

    def put_object(self, key: str, data: bytes | str) -> None:
        if isinstance(data, str):
            data = data.encode()
        self._store[key] = data

    def delete_object(self, key: str) -> None:
        self._store.pop(key, None)

    def exists(self, key: str) -> bool:
        return key in self._store


def _make_sample_parquet() -> bytes:
    """Return minimal valid parquet bytes for testing."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    table = pa.table({"date": ["2026-09-11"], "ticker": ["AAPL"]})
    buf = pa.BufferOutputStream()
    pq.write_table(table, buf)
    return buf.getvalue().to_pybytes()


def test_cleanup_deletes_all_when_canonicals_exist():
    """All legacy keys are deleted when canonical date-first shards exist."""
    store = MockObjectStore()
    parquet_bytes = _make_sample_parquet()

    # Set up legacy keys
    store.put("features/layer1/AAPL.parquet", parquet_bytes)
    store.put("features/layer1/2026-09-11/AAPL.parquet", parquet_bytes)
    store.put("features/layer1/2026-09-11/MSFT.parquet", parquet_bytes)
    store.put("features/layer1/MSFT.parquet", parquet_bytes)

    # Set up canonical replacements
    canonical_aapl = layer1_feature_path("2026-09-11", "AAPL")
    canonical_msft = layer1_feature_path("2026-09-11", "MSFT")
    store.put(canonical_aapl, parquet_bytes)
    store.put(canonical_msft, parquet_bytes)

    result = cleanup_legacy_layer1_keys(writer=store, dry_run=False)

    assert result.history_files_deleted == 2
    assert result.dated_shards_deleted == 2
    assert result.skipped_no_canonical == 0
    assert result.dry_run is False

    # Verify all legacy keys are gone
    legacy_keys = store.list_keys(LEGACY_LAYER1_PREFIX)
    assert legacy_keys == []


def test_cleanup_skips_when_canonical_missing():
    """Legacy keys are NOT deleted when canonical date-first shards are absent."""
    store = MockObjectStore()
    parquet_bytes = _make_sample_parquet()

    # Set up legacy keys but NO canonical replacements
    store.put("features/layer1/AAPL.parquet", parquet_bytes)
    store.put("features/layer1/2026-09-11/AAPL.parquet", parquet_bytes)

    result = cleanup_legacy_layer1_keys(writer=store, dry_run=False)

    assert result.history_files_deleted == 0
    assert result.dated_shards_deleted == 0
    assert result.skipped_no_canonical == 2
    assert result.dry_run is False

    # Verify legacy keys still exist
    legacy_keys = store.list_keys(LEGACY_LAYER1_PREFIX)
    assert len(legacy_keys) == 2


def test_cleanup_partial_canonical_coverage():
    """Only keys with canonical replacements are deleted."""
    store = MockObjectStore()
    parquet_bytes = _make_sample_parquet()

    # Legacy keys
    store.put("features/layer1/AAPL.parquet", parquet_bytes)
    store.put("features/layer1/2026-09-11/AAPL.parquet", parquet_bytes)
    store.put("features/layer1/2026-09-11/MSFT.parquet", parquet_bytes)
    store.put("features/layer1/MSFT.parquet", parquet_bytes)

    # Only AAPL has canonical replacement; MSFT does not
    canonical_aapl = layer1_feature_path("2026-09-11", "AAPL")
    store.put(canonical_aapl, parquet_bytes)

    result = cleanup_legacy_layer1_keys(writer=store, dry_run=False)

    assert result.history_files_deleted == 1  # Only AAPL history
    assert result.dated_shards_deleted == 1  # Only AAPL dated shard
    assert result.skipped_no_canonical == 2  # MSFT keys skipped
    assert result.dry_run is False

    # Verify AAPL legacy keys gone, MSFT still there
    legacy_keys = store.list_keys(LEGACY_LAYER1_PREFIX)
    assert "features/layer1/MSFT.parquet" in legacy_keys
    assert "features/layer1/2026-09-11/MSFT.parquet" in legacy_keys
    assert "features/layer1/AAPL.parquet" not in legacy_keys
    assert "features/layer1/2026-09-11/AAPL.parquet" not in legacy_keys


def test_cleanup_dry_run_preserves_all():
    """Dry run mode reports what would be deleted but does not modify anything."""
    store = MockObjectStore()
    parquet_bytes = _make_sample_parquet()

    store.put("features/layer1/AAPL.parquet", parquet_bytes)
    store.put("features/layer1/2026-09-11/AAPL.parquet", parquet_bytes)

    canonical_aapl = layer1_feature_path("2026-09-11", "AAPL")
    store.put(canonical_aapl, parquet_bytes)

    result = cleanup_legacy_layer1_keys(writer=store, dry_run=True)

    assert result.history_files_deleted == 1
    assert result.dated_shards_deleted == 1
    assert result.skipped_no_canonical == 0
    assert result.dry_run is True

    # Verify nothing was actually deleted
    legacy_keys = store.list_keys(LEGACY_LAYER1_PREFIX)
    assert len(legacy_keys) == 2


def test_cleanup_empty_store():
    """Cleanup on an empty store returns zero counts."""
    store = MockObjectStore()
    result = cleanup_legacy_layer1_keys(writer=store, dry_run=False)

    assert result.history_files_deleted == 0
    assert result.dated_shards_deleted == 0
    assert result.skipped_no_canonical == 0


def test_cleanup_result_is_frozen_dataclass():
    """Layer1CleanupResult must be frozen."""
    result = Layer1CleanupResult(
        history_files_deleted=1,
        dated_shards_deleted=1,
        skipped_no_canonical=0,
        dry_run=False,
    )
    try:
        result.history_files_deleted = 2  # type: ignore
        assert False, "Expected FrozenInstanceError"
    except Exception:
        pass


def test_cleanup_with_local_r2_client():
    """Verify cleanup works end-to-end with LocalR2Client (filesystem-backed)."""
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp) / "r2_mock"
        client = LocalR2Client(root)
        parquet_bytes = _make_sample_parquet()

        # Set up legacy + canonical
        client.put_object("features/layer1/AAPL.parquet", parquet_bytes)
        client.put_object("features/layer1/2026-09-11/AAPL.parquet", parquet_bytes)
        canonical = layer1_feature_path("2026-09-11", "AAPL")
        client.put_object(canonical, parquet_bytes)

        result = cleanup_legacy_layer1_keys(writer=client, dry_run=False)

        assert result.history_files_deleted == 1
        assert result.dated_shards_deleted == 1
        assert result.skipped_no_canonical == 0

        # Verify files actually deleted from disk
        assert not (root / "features/layer1/AAPL.parquet").exists()
        assert not (root / "features/layer1/2026-09-11/AAPL.parquet").exists()
        assert (root / canonical).exists()


def test_cleanup_history_only_needs_any_canonical_shard():
    """History file only needs ANY canonical shard for the ticker, not the same date."""
    store = MockObjectStore()
    parquet_bytes = _make_sample_parquet()

    # Legacy history for AAPL covering multiple dates
    store.put("features/layer1/AAPL.parquet", parquet_bytes)

    # Canonical shard exists for a different date than what history might have
    canonical_sep12 = layer1_feature_path("2026-09-12", "AAPL")
    store.put(canonical_sep12, parquet_bytes)

    result = cleanup_legacy_layer1_keys(writer=store, dry_run=False)

    assert result.history_files_deleted == 1
    assert result.skipped_no_canonical == 0
