from __future__ import annotations

import argparse
import json
import re
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from loguru import logger

_REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_REPO_ROOT))

from core.contracts.schemas import FeatureRecord  # noqa: E402
from core.features.io import (  # noqa: E402
    feature_record_to_parquet_bytes,
    parquet_bytes_to_feature_record,
    parquet_bytes_to_feature_records,
)
from services.r2.paths import layer1_feature_path  # noqa: E402
from services.r2.writer import R2Writer  # noqa: E402

LEGACY_LAYER1_PREFIX = "features/layer1/"
LEGACY_LAYER1_HISTORY_RE = re.compile(r"^features/layer1/(?P<ticker>[^/]+)\.parquet$")
LEGACY_LAYER1_SHARD_RE = re.compile(
    r"^features/layer1/(?P<date>\d{4}-\d{2}-\d{2})/(?P<ticker>[^/]+)\.parquet$"
)
# Legacy Layer 1.5 regime prefix (features/layer1_5/regime/).
LEGACY_LAYER1_5_PREFIX = "features/layer1_5/"
LEGACY_LAYER1_5_REGIME_RE = re.compile(r"^features/layer1_5/regime/(?P<run_id>[^/]+)\.parquet$")
# Canonical (date‑first) Layer 1 shard layout introduced by the migration.
CANONICAL_LAYER1_SHARD_RE = re.compile(
    r"^features/(?P<date>\d{4}-\d{2}-\d{2})/(?P<ticker>[^/]+)\.parquet$"
)
# Canonical (date‑first) regime shard layout.
CANONICAL_REGIME_SHARD_RE = re.compile(
    r"^features/(?P<date>\d{4}-\d{2}-\d{2})/regime/(?P<run_id>[^/]+)\.parquet$"
)


class ObjectStore(Protocol):
    """Object-store operations required by the Layer 1 date-first migration."""

    def list_keys(self, prefix: str) -> list[str]:
        """List keys beneath a prefix."""

    def get_object(self, key: str) -> bytes:
        """Return the bytes stored at a key."""

    def put_object(self, key: str, data: bytes | str) -> None:
        """Write bytes or text to a key."""

    # The cleanup utilities need these mutating operations. They are present on the
    # concrete ``R2Writer`` implementation but not required for the migration flow,
    # so we add them to the protocol for type completeness.
    def delete_object(self, key: str) -> None:
        """Delete a key from the store when it exists."""

    def exists(self, key: str) -> bool:
        """Return True if ``key`` exists in the store."""


@dataclass(frozen=True)
class Layer1DateFirstMigrationResult:
    """Summary of one Layer 1 date-first migration run."""

    legacy_history_files_found: int
    legacy_dated_shards_found: int
    date_first_shards_written: int
    dry_run: bool


# ---------------------------------------------------------------------------
# Legacy cleanup utilities
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Layer1CleanupResult:
    """Result of cleaning up legacy Layer 1 keys.

    ``history_files_deleted`` – number of legacy per‑ticker history parquet files
    removed.
    ``dated_shards_deleted`` – number of legacy date‑sharded parquet files removed.
    ``skipped_no_canonical`` – legacy keys that were retained because no canonical
    date‑first shard existed for the corresponding ticker/date.
    ``dry_run`` – whether the operation was a dry run.
    """

    history_files_deleted: int
    dated_shards_deleted: int
    skipped_no_canonical: int
    dry_run: bool


def collect_legacy_layer1_feature_keys(keys: Sequence[str]) -> tuple[list[str], list[str]]:
    """Return legacy history and dated-shard keys that can feed date-first migration."""
    history_keys: list[str] = []
    shard_keys: list[str] = []
    for key in keys:
        if LEGACY_LAYER1_HISTORY_RE.fullmatch(key):
            history_keys.append(key)
        elif LEGACY_LAYER1_SHARD_RE.fullmatch(key):
            shard_keys.append(key)
    return sorted(history_keys), sorted(shard_keys)


def cleanup_legacy_layer1_keys(
    *,
    writer: ObjectStore | None = None,
    dry_run: bool = False,
) -> Layer1CleanupResult:
    """Remove legacy Layer‑1 files when a canonical date‑first shard exists.

    - History files (``features/layer1/<TICKER>.parquet``) are deleted when *any*
      canonical shard for that ticker exists.
    - Dated shard files (``features/layer1/<DATE>/<TICKER>.parquet``) are deleted only
      when the exact canonical shard ``layer1_feature_path(date, ticker)`` is present.

    ``dry_run`` performs the same checks but does not mutate the store.
    """
    active_writer = writer or R2Writer()

    # Gather all legacy keys under the deprecated prefix.
    all_keys = active_writer.list_keys(LEGACY_LAYER1_PREFIX)
    history_keys, shard_keys = collect_legacy_layer1_feature_keys(all_keys)

    # Determine which tickers have at least one **canonical** date‑first shard.
    # The legacy pattern must not be used here; otherwise legacy shards are counted as
    # canonical and the cleanup incorrectly deletes legacy data.
    canonical_tickers: set[str] = set()
    for key in active_writer.list_keys(""):
        match = CANONICAL_LAYER1_SHARD_RE.fullmatch(key)
        if match:
            canonical_tickers.add(match.group("ticker"))

    history_deleted = 0
    shards_deleted = 0
    skipped = 0

    # Process history files – delete if any canonical exists for the ticker.
    for key in history_keys:
        ticker = LEGACY_LAYER1_HISTORY_RE.fullmatch(key).group("ticker")
        if ticker in canonical_tickers:
            if not dry_run:
                active_writer.delete_object(key)
            history_deleted += 1
        else:
            skipped += 1

    # Process dated shards – delete only if exact canonical exists.
    for key in shard_keys:
        m = LEGACY_LAYER1_SHARD_RE.fullmatch(key)
        if m is None:
            continue
        date, ticker = m.group("date"), m.group("ticker")
        canonical_key = layer1_feature_path(date, ticker)
        if active_writer.exists(canonical_key):
            if not dry_run:
                active_writer.delete_object(key)
            shards_deleted += 1
        else:
            skipped += 1

    return Layer1CleanupResult(
        history_files_deleted=history_deleted,
        dated_shards_deleted=shards_deleted,
        skipped_no_canonical=skipped,
        dry_run=dry_run,
    )


@dataclass(frozen=True)
class Layer1RegimeCleanupResult:
    """Result of cleaning up legacy Layer 1.5 regime keys.

    ``regime_files_deleted`` – number of legacy regime parquet files removed.
    ``skipped_no_canonical`` – legacy regime keys retained because no canonical
    date-first regime shard existed for the same run id.
    ``dry_run`` – whether the operation was a dry run.
    """

    regime_files_deleted: int
    skipped_no_canonical: int
    dry_run: bool


def cleanup_legacy_layer1_regime_keys(
    *,
    writer: ObjectStore | None = None,
    dry_run: bool = False,
) -> Layer1RegimeCleanupResult:
    """Remove legacy Layer 1.5 regime files when a canonical date-first regime exists.

    A legacy key ``features/layer1_5/regime/<run_id>.parquet`` is deleted only when
    at least one canonical regime shard ``features/<date>/regime/<run_id>.parquet``
    is present in the store.

    ``dry_run`` performs the same checks but does not mutate the store.
    """
    active_writer = writer or R2Writer()

    # Gather all legacy regime keys under the deprecated prefix.
    all_keys = active_writer.list_keys(LEGACY_LAYER1_5_PREFIX)
    legacy_regime_keys: list[str] = [
        key for key in all_keys if LEGACY_LAYER1_5_REGIME_RE.fullmatch(key)
    ]

    # Collect canonical regime run ids from date-first layout.
    canonical_run_ids: set[str] = set()
    for key in active_writer.list_keys("features/"):
        match = CANONICAL_REGIME_SHARD_RE.fullmatch(key)
        if match:
            canonical_run_ids.add(match.group("run_id"))

    deleted = 0
    skipped = 0

    for key in legacy_regime_keys:
        m = LEGACY_LAYER1_5_REGIME_RE.fullmatch(key)
        if m is None:
            continue
        run_id = m.group("run_id")
        if run_id in canonical_run_ids:
            if not dry_run:
                active_writer.delete_object(key)
            deleted += 1
        else:
            skipped += 1

    return Layer1RegimeCleanupResult(
        regime_files_deleted=deleted,
        skipped_no_canonical=skipped,
        dry_run=dry_run,
    )


def write_cleanup_manifest(
    feature_result: Layer1CleanupResult,
    regime_result: Layer1RegimeCleanupResult,
    *,
    writer: ObjectStore | None = None,
    run_id: str | None = None,
) -> str | None:
    """Write a JSON cleanup manifest to R2 and return the key (or None on dry-run).

    The manifest records counts, skipped keys, and timestamps so downstream workers
    and humans can audit the cleanup without re-running it.
    """
    from datetime import datetime as _dt  # lazy import to avoid cycle

    active_writer = writer or R2Writer()
    if run_id is None:
        run_id = _dt.utcnow().strftime("%Y-%m-%d")

    manifest = {
        "run_id": run_id,
        "generated_at": _dt.utcnow().isoformat() + "Z",
        "feature_cleanup": {
            "history_files_deleted": feature_result.history_files_deleted,
            "dated_shards_deleted": feature_result.dated_shards_deleted,
            "skipped_no_canonical": feature_result.skipped_no_canonical,
            "dry_run": feature_result.dry_run,
        },
        "regime_cleanup": {
            "regime_files_deleted": regime_result.regime_files_deleted,
            "skipped_no_canonical": regime_result.skipped_no_canonical,
            "dry_run": regime_result.dry_run,
        },
    }

    if feature_result.dry_run or regime_result.dry_run:
        logger.info("Cleanup manifest (dry-run): {}", manifest)
        return None

    key = f"artifacts/reports/integration/layer1_cleanup_{run_id}.json"
    active_writer.put_object(key, json.dumps(manifest, indent=2))
    logger.info("Cleanup manifest written to {}", key)
    return key


def migrate_layer1_date_first(
    *,
    writer: ObjectStore | None = None,
    dry_run: bool = False,
) -> Layer1DateFirstMigrationResult:
    """Copy legacy Layer 1 artifacts into canonical date-first feature shards."""
    active_writer = writer or R2Writer()
    history_keys, shard_keys = collect_legacy_layer1_feature_keys(
        active_writer.list_keys(LEGACY_LAYER1_PREFIX)
    )
    records = _records_from_legacy_histories(active_writer, history_keys)
    records.extend(_records_from_legacy_shards(active_writer, shard_keys))

    deduped_records = _deduplicate_records(records)
    if not dry_run:
        for record in deduped_records:
            key = layer1_feature_path(record.date, record.ticker)
            logger.info("Writing date-first Layer 1 shard {}", key)
            active_writer.put_object(key, feature_record_to_parquet_bytes(record))

    return Layer1DateFirstMigrationResult(
        legacy_history_files_found=len(history_keys),
        legacy_dated_shards_found=len(shard_keys),
        date_first_shards_written=0 if dry_run else len(deduped_records),
        dry_run=dry_run,
    )


def _records_from_legacy_histories(
    writer: ObjectStore,
    keys: Sequence[str],
) -> list[FeatureRecord]:
    """Read FeatureRecord rows from legacy per-ticker history files."""
    records: list[FeatureRecord] = []
    for key in keys:
        match = LEGACY_LAYER1_HISTORY_RE.fullmatch(key)
        if match is None:
            continue
        expected_ticker = match.group("ticker")
        for record in parquet_bytes_to_feature_records(writer.get_object(key)):
            if record.ticker != expected_ticker:
                raise ValueError(
                    f"Legacy history ticker mismatch for key={key}: "
                    f"expected {expected_ticker}, got {record.ticker}"
                )
            records.append(record)
    return records


def _records_from_legacy_shards(
    writer: ObjectStore,
    keys: Sequence[str],
) -> list[FeatureRecord]:
    """Read FeatureRecord rows from legacy date/ticker shards."""
    records: list[FeatureRecord] = []
    for key in keys:
        match = LEGACY_LAYER1_SHARD_RE.fullmatch(key)
        if match is None:
            continue
        record = parquet_bytes_to_feature_record(writer.get_object(key))
        expected_date = match.group("date")
        expected_ticker = match.group("ticker")
        if record.date != expected_date or record.ticker != expected_ticker:
            raise ValueError(
                f"Legacy shard identity mismatch for key={key}: expected "
                f"{expected_date}/{expected_ticker}, got {record.date}/{record.ticker}"
            )
        records.append(record)
    return records


def _deduplicate_records(records: Sequence[FeatureRecord]) -> list[FeatureRecord]:
    """Deduplicate identical date/ticker rows and reject conflicting payloads."""
    deduped: dict[tuple[str, str], FeatureRecord] = {}
    for record in records:
        key = (record.date, record.ticker)
        existing = deduped.get(key)
        if existing is not None and existing.features != record.features:
            raise ValueError(
                f"Conflicting Layer 1 records for date={record.date} ticker={record.ticker}"
            )
        deduped[key] = record
    return [deduped[key] for key in sorted(deduped)]


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """Parse CLI arguments for the date-first migration script."""
    parser = argparse.ArgumentParser(
        description=(
            "Migrate Layer 1 legacy artifacts into date-first feature shards, "
            "and clean up legacy Layer 1 and Layer 1.5 regime keys."
        )
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only report how many keys would be deleted or written.",
    )
    parser.add_argument(
        "--old-prefix",
        default=LEGACY_LAYER1_PREFIX,
        help=f"Legacy feature prefix to clean (default: {LEGACY_LAYER1_PREFIX!r}).",
    )
    parser.add_argument(
        "--old-regime-prefix",
        default=LEGACY_LAYER1_5_PREFIX,
        help=f"Legacy regime prefix to clean (default: {LEGACY_LAYER1_5_PREFIX!r}).",
    )
    parser.add_argument(
        "--new-prefix",
        default="features/",
        help="Canonical feature prefix (default: 'features/').",
    )
    parser.add_argument(
        "--manifest-run-id",
        default=None,
        help="Custom run id for the cleanup manifest filename.",
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Execute migration + cleanup (default: dry-run unless --execute is set).",
    )
    parser.add_argument(
        "--cleanup-only",
        action="store_true",
        help="Skip migration; only run legacy cleanup.",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    """CLI entry point for the Layer 1 date-first storage-layout migration."""
    args = _parse_args(argv)
    is_dry_run = args.dry_run or not args.execute

    # Run migration unless --cleanup-only.
    if not args.cleanup_only:
        result = migrate_layer1_date_first(dry_run=is_dry_run)
        logger.info(
            "Layer 1 date-first migration complete histories={} legacy_shards={} "
            "date_first_shards_written={} dry_run={}",
            result.legacy_history_files_found,
            result.legacy_dated_shards_found,
            result.date_first_shards_written,
            result.dry_run,
        )

    # Run legacy feature cleanup.
    feature_result = cleanup_legacy_layer1_keys(dry_run=is_dry_run)
    logger.info(
        "Legacy Layer 1 cleanup: history_deleted={} shard_deleted={} skipped={} dry_run={}",
        feature_result.history_files_deleted,
        feature_result.dated_shards_deleted,
        feature_result.skipped_no_canonical,
        feature_result.dry_run,
    )

    # Run legacy regime cleanup.
    regime_result = cleanup_legacy_layer1_regime_keys(dry_run=is_dry_run)
    logger.info(
        "Legacy Layer 1.5 regime cleanup: deleted={} skipped={} dry_run={}",
        regime_result.regime_files_deleted,
        regime_result.skipped_no_canonical,
        regime_result.dry_run,
    )

    # Write cleanup manifest.
    manifest_key = write_cleanup_manifest(
        feature_result,
        regime_result,
        run_id=args.manifest_run_id,
    )
    if manifest_key:
        logger.info("Cleanup manifest stored at {}", manifest_key)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
