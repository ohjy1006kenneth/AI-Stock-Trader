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
from core.features.io import parquet_bytes_to_feature_record, write_feature_records  # noqa: E402
from services.r2.writer import R2Writer  # noqa: E402

LEGACY_LAYER1_PREFIX = "features/layer1/"
LEGACY_LAYER1_SHARD_RE = re.compile(
    r"^features/layer1/(?P<date>\d{4}-\d{2}-\d{2})/(?P<ticker>[^/]+)\.parquet$"
)


class ObjectStore(Protocol):
    """Object-store operations required by the Layer 1 migration."""

    def list_keys(self, prefix: str) -> list[str]:
        """List keys beneath a prefix."""

    def get_object(self, key: str) -> bytes:
        """Return the bytes stored at a key."""

    def put_object(self, key: str, data: bytes | str) -> None:
        """Write bytes or text to a key."""


@dataclass(frozen=True)
class Layer1MigrationResult:
    """Summary of one Layer 1 per-ticker migration run."""

    legacy_shards_found: int
    tickers_found: int
    ticker_files_written: int
    dry_run: bool


def collect_legacy_layer1_shards(keys: Sequence[str]) -> dict[str, list[str]]:
    """Group legacy date-partitioned Layer 1 shard keys by ticker."""
    grouped: dict[str, list[str]] = {}
    for key in keys:
        match = LEGACY_LAYER1_SHARD_RE.match(key)
        if match is None:
            continue
        ticker = match.group("ticker")
        grouped.setdefault(ticker, []).append(key)
    return {ticker: sorted(ticker_keys) for ticker, ticker_keys in sorted(grouped.items())}


def migrate_layer1_per_ticker(
    *,
    writer: ObjectStore | None = None,
    dry_run: bool = False,
) -> Layer1MigrationResult:
    """Pack legacy Layer 1 date/ticker shards into per-ticker history files."""
    active_writer = writer or R2Writer()
    legacy_keys = collect_legacy_layer1_shards(active_writer.list_keys(LEGACY_LAYER1_PREFIX))
    legacy_shards_found = sum(len(keys) for keys in legacy_keys.values())
    ticker_files_written = 0

    for ticker, keys in legacy_keys.items():
        logger.info("Migrating ticker={} legacy_shards={}", ticker, len(keys))
        if dry_run:
            continue
        records = [_read_legacy_record(active_writer, key, ticker) for key in keys]
        written_keys = write_feature_records(records, writer=active_writer)
        ticker_files_written += len(written_keys)

    return Layer1MigrationResult(
        legacy_shards_found=legacy_shards_found,
        tickers_found=len(legacy_keys),
        ticker_files_written=ticker_files_written,
        dry_run=dry_run,
    )


def _read_legacy_record(writer: ObjectStore, key: str, expected_ticker: str) -> FeatureRecord:
    """Read and verify one legacy Layer 1 shard."""
    record = parquet_bytes_to_feature_record(writer.get_object(key))
    if record.ticker != expected_ticker:
        raise ValueError(
            f"Legacy shard ticker mismatch for key={key}: "
            f"expected {expected_ticker}, got {record.ticker}"
        )
    return record


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """Parse CLI arguments for the migration script."""
    parser = argparse.ArgumentParser(
        description="Migrate Layer 1 date/ticker shards into per-ticker history files."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only report how many legacy shards would be migrated.",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    """CLI entry point for the Layer 1 storage-layout migration."""
    args = _parse_args(argv)
    result = migrate_layer1_per_ticker(dry_run=args.dry_run)
    logger.info(
        "Layer 1 migration complete legacy_shards={} tickers={} files_written={} dry_run={}",
        result.legacy_shards_found,
        result.tickers_found,
        result.ticker_files_written,
        result.dry_run,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
