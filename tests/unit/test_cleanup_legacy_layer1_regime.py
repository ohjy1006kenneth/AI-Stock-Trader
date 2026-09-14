"""Tests for legacy Layer 1.5 regime cleanup and cleanup manifest generation."""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

from scripts.migrate_layer1_date_first import (
    LEGACY_LAYER1_5_PREFIX,
    Layer1CleanupResult,
    Layer1RegimeCleanupResult,
    cleanup_legacy_layer1_regime_keys,
    write_cleanup_manifest,
)
from services.r2.writer import LocalR2Client


class MockObjectStore:
    """In-memory object store for testing."""

    def __init__(self) -> None:
        self._store: dict[str, bytes | str] = {}

    def put(self, key: str, data: bytes) -> None:
        self._store[key] = data

    def list_keys(self, prefix: str) -> list[str]:
        return sorted(key for key in self._store if key.startswith(prefix))

    def get_object(self, key: str) -> bytes:
        raw = self._store[key]
        if isinstance(raw, str):
            return raw.encode()
        return raw

    def put_object(self, key: str, data: bytes | str) -> None:
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


# -----------------------------------------------------------------------
# cleanup_legacy_layer1_regime_keys
# -----------------------------------------------------------------------


def test_regime_cleanup_deletes_when_canonical_exists():
    """Legacy regime keys are deleted when canonical date-first regime exists."""
    store = MockObjectStore()
    parquet_bytes = _make_sample_parquet()

    # Legacy regime keys
    store.put("features/layer1_5/regime/run-001.parquet", parquet_bytes)
    store.put("features/layer1_5/regime/run-002.parquet", parquet_bytes)

    # Canonical regime replacements
    store.put("features/2026-09-11/regime/run-001.parquet", parquet_bytes)
    store.put("features/2026-09-12/regime/run-002.parquet", parquet_bytes)

    result = cleanup_legacy_layer1_regime_keys(writer=store, dry_run=False)

    assert result.regime_files_deleted == 2
    assert result.skipped_no_canonical == 0
    assert result.dry_run is False

    # Verify legacy keys are gone
    legacy = store.list_keys(LEGACY_LAYER1_5_PREFIX)
    assert legacy == []


def test_regime_cleanup_skips_when_canonical_missing():
    """Legacy regime keys are NOT deleted when no canonical regime exists."""
    store = MockObjectStore()
    parquet_bytes = _make_sample_parquet()

    store.put("features/layer1_5/regime/run-001.parquet", parquet_bytes)
    # No canonical regime shard for run-001

    result = cleanup_legacy_layer1_regime_keys(writer=store, dry_run=False)

    assert result.regime_files_deleted == 0
    assert result.skipped_no_canonical == 1
    assert result.dry_run is False

    # Legacy key still present
    assert "features/layer1_5/regime/run-001.parquet" in store._store


def test_regime_cleanup_partial_coverage():
    """Only regime keys with canonical replacements are deleted."""
    store = MockObjectStore()
    parquet_bytes = _make_sample_parquet()

    store.put("features/layer1_5/regime/run-A.parquet", parquet_bytes)
    store.put("features/layer1_5/regime/run-B.parquet", parquet_bytes)

    # Only run-A has a canonical replacement
    store.put("features/2026-09-11/regime/run-A.parquet", parquet_bytes)

    result = cleanup_legacy_layer1_regime_keys(writer=store, dry_run=False)

    assert result.regime_files_deleted == 1
    assert result.skipped_no_canonical == 1


def test_regime_cleanup_dry_run_preserves_all():
    """Dry run reports what would be deleted but does not modify anything."""
    store = MockObjectStore()
    parquet_bytes = _make_sample_parquet()

    store.put("features/layer1_5/regime/run-001.parquet", parquet_bytes)
    store.put("features/2026-09-11/regime/run-001.parquet", parquet_bytes)

    result = cleanup_legacy_layer1_regime_keys(writer=store, dry_run=True)

    assert result.regime_files_deleted == 1
    assert result.skipped_no_canonical == 0
    assert result.dry_run is True

    # Key still present
    assert "features/layer1_5/regime/run-001.parquet" in store._store


def test_regime_cleanup_empty_store():
    """Cleanup on empty store returns zero counts."""
    store = MockObjectStore()
    result = cleanup_legacy_layer1_regime_keys(writer=store, dry_run=False)

    assert result.regime_files_deleted == 0
    assert result.skipped_no_canonical == 0


def test_regime_cleanup_result_is_frozen():
    """Layer1RegimeCleanupResult must be frozen."""
    result = Layer1RegimeCleanupResult(
        regime_files_deleted=1,
        skipped_no_canonical=0,
        dry_run=False,
    )
    try:
        result.regime_files_deleted = 2  # type: ignore
        assert False, "Expected FrozenInstanceError"
    except Exception:
        pass


def test_regime_cleanup_with_local_r2_client():
    """Verify regime cleanup works end-to-end with LocalR2Client."""
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp) / "r2_mock"
        client = LocalR2Client(root)
        parquet_bytes = _make_sample_parquet()

        client.put_object("features/layer1_5/regime/run-001.parquet", parquet_bytes)
        client.put_object("features/2026-09-11/regime/run-001.parquet", parquet_bytes)

        result = cleanup_legacy_layer1_regime_keys(writer=client, dry_run=False)

        assert result.regime_files_deleted == 1
        assert result.skipped_no_canonical == 0
        assert not (root / "features" / "layer1_5" / "regime" / "run-001.parquet").exists()
        assert (root / "features" / "2026-09-11" / "regime" / "run-001.parquet").exists()


# -----------------------------------------------------------------------
# write_cleanup_manifest
# -----------------------------------------------------------------------


def test_write_cleanup_manifest_creates_json():
    """A real run writes a JSON manifest and returns the key."""
    store = MockObjectStore()
    feature_result = Layer1CleanupResult(
        history_files_deleted=2,
        dated_shards_deleted=3,
        skipped_no_canonical=0,
        dry_run=False,
    )
    regime_result = Layer1RegimeCleanupResult(
        regime_files_deleted=1,
        skipped_no_canonical=0,
        dry_run=False,
    )

    key = write_cleanup_manifest(
        feature_result,
        regime_result,
        writer=store,
        run_id="test-run-001",
    )

    assert key == "artifacts/reports/integration/layer1_cleanup_test-run-001.json"
    assert key in store._store

    manifest = json.loads(store._store[key])
    assert manifest["run_id"] == "test-run-001"
    assert manifest["feature_cleanup"]["history_files_deleted"] == 2
    assert manifest["feature_cleanup"]["dated_shards_deleted"] == 3
    assert manifest["regime_cleanup"]["regime_files_deleted"] == 1
    assert "generated_at" in manifest


def test_write_cleanup_manifest_dry_run_returns_none():
    """A dry run does not write a manifest."""
    store = MockObjectStore()
    feature_result = Layer1CleanupResult(
        history_files_deleted=1,
        dated_shards_deleted=1,
        skipped_no_canonical=0,
        dry_run=True,
    )
    regime_result = Layer1RegimeCleanupResult(
        regime_files_deleted=0,
        skipped_no_canonical=1,
        dry_run=True,
    )

    key = write_cleanup_manifest(
        feature_result,
        regime_result,
        writer=store,
        run_id="dry-001",
    )

    assert key is None
    assert len(store._store) == 0


def test_write_cleanup_manifest_mixed_dry_run_skips():
    """If either result is dry_run, the manifest is not written."""
    store = MockObjectStore()
    feature_result = Layer1CleanupResult(
        history_files_deleted=1,
        dated_shards_deleted=0,
        skipped_no_canonical=0,
        dry_run=False,
    )
    regime_result = Layer1RegimeCleanupResult(
        regime_files_deleted=0,
        skipped_no_canonical=1,
        dry_run=True,
    )

    key = write_cleanup_manifest(
        feature_result,
        regime_result,
        writer=store,
        run_id="mixed-001",
    )

    assert key is None


def test_write_cleanup_manifest_default_run_id():
    """If no run_id is passed, a date-based one is generated."""
    store = MockObjectStore()
    feature_result = Layer1CleanupResult(
        history_files_deleted=0,
        dated_shards_deleted=0,
        skipped_no_canonical=0,
        dry_run=False,
    )
    regime_result = Layer1RegimeCleanupResult(
        regime_files_deleted=0,
        skipped_no_canonical=0,
        dry_run=False,
    )

    key = write_cleanup_manifest(
        feature_result,
        regime_result,
        writer=store,
    )

    assert key is not None
    assert key.startswith("artifacts/reports/integration/layer1_cleanup_")
    assert key.endswith(".json")
