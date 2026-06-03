from __future__ import annotations

import argparse
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


class ObjectStore(Protocol):
    """Object-store operations required by the Layer 1 date-first migration."""

    def list_keys(self, prefix: str) -> list[str]:
        """List keys beneath a prefix."""

    def get_object(self, key: str) -> bytes:
        """Return the bytes stored at a key."""

    def put_object(self, key: str, data: bytes | str) -> None:
        """Write bytes or text to a key."""


@dataclass(frozen=True)
class Layer1DateFirstMigrationResult:
    """Summary of one Layer 1 date-first migration run."""

    legacy_history_files_found: int
    legacy_dated_shards_found: int
    date_first_shards_written: int
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
        description="Migrate Layer 1 legacy artifacts into date-first feature shards."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only report how many date-first shards would be written.",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    """CLI entry point for the Layer 1 date-first storage-layout migration."""
    args = _parse_args(argv)
    result = migrate_layer1_date_first(dry_run=args.dry_run)
    logger.info(
        "Layer 1 date-first migration complete histories={} legacy_shards={} "
        "date_first_shards_written={} dry_run={}",
        result.legacy_history_files_found,
        result.legacy_dated_shards_found,
        result.date_first_shards_written,
        result.dry_run,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
