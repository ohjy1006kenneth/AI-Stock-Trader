"""Historical FRED macro/rates backfill into the canonical R2 raw archive."""
from __future__ import annotations

import argparse
import importlib
import json
import sys
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import date
from io import BytesIO
from pathlib import Path
from typing import Protocol

from loguru import logger

_REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(_REPO_ROOT))

from services.fred.macro_fetcher import (  # noqa: E402
    DEFAULT_FRED_CONFIG_PATH,
    DEFAULT_FRED_PAGE_LIMIT,
    FredClientConfig,
    FredMacroFetcher,
    load_fred_archive_config,
)
from services.r2.paths import raw_macro_path  # noqa: E402
from services.r2.writer import R2Writer  # noqa: E402


class ObjectWriter(Protocol):
    """Subset of R2Writer used by the FRED backfill."""

    def put_object(self, key: str, data: bytes | str) -> None:
        """Write an object to storage."""

    def exists(self, key: str) -> bool:
        """Return True when an object already exists."""


class MacroFetcher(Protocol):
    """Subset of FredMacroFetcher used by the backfill."""

    def fetch_all_macro_observations(
        self,
        *,
        series_ids: Sequence[str],
        start_date: str,
        end_date: str,
        limit: int,
    ) -> list[dict[str, object]]:
        """Fetch all raw FRED rows for configured series/date range."""


MacroSerializer = Callable[[list[dict[str, object]]], bytes]


@dataclass(frozen=True)
class BackfillResult:
    """Summary of a FRED macro/rates backfill run."""

    requested_series: int
    written: int
    skipped: int
    empty: int
    total_rows: int
    output_key: str


def backfill_fred_archive(
    from_date: date,
    to_date: date,
    *,
    fetcher: MacroFetcher,
    writer: ObjectWriter,
    series_ids: Sequence[str],
    overwrite: bool = False,
    limit: int = DEFAULT_FRED_PAGE_LIMIT,
    serializer: MacroSerializer | None = None,
) -> BackfillResult:
    """Backfill FRED macro/rate observations into R2."""
    if from_date > to_date:
        raise ValueError("from_date must be <= to_date")
    if limit <= 0:
        raise ValueError("limit must be positive")

    normalized_series_ids = _normalize_series_ids(series_ids)

    output_key = raw_macro_path(from_date, to_date)
    if writer.exists(output_key) and not overwrite:
        logger.info("Skipping existing FRED macro archive {}", output_key)
        return BackfillResult(
            requested_series=len(normalized_series_ids),
            written=0,
            skipped=1,
            empty=0,
            total_rows=0,
            output_key=output_key,
        )

    rows = fetcher.fetch_all_macro_observations(
        series_ids=normalized_series_ids,
        start_date=from_date.isoformat(),
        end_date=to_date.isoformat(),
        limit=limit,
    )
    payload_serializer = serializer or _macro_to_parquet_bytes
    writer.put_object(output_key, payload_serializer(_sort_macro_observations(rows)))
    logger.info("Wrote {} FRED macro/rate rows to {}", len(rows), output_key)
    return BackfillResult(
        requested_series=len(normalized_series_ids),
        written=1,
        skipped=0,
        empty=0 if rows else 1,
        total_rows=len(rows),
        output_key=output_key,
    )


def _macro_to_parquet_bytes(rows: list[dict[str, object]]) -> bytes:
    """Serialize normalized FRED macro observations to Parquet bytes."""
    try:
        import pandas as pd
        importlib.import_module("pyarrow")
    except ModuleNotFoundError as exc:
        raise ModuleNotFoundError(
            "pandas and pyarrow are required to serialize FRED macro observations to Parquet. "
            "Install the Pi, Modal, or dev requirements before running the live backfill."
        ) from exc

    frame = pd.DataFrame([_parquet_ready_row(row) for row in rows])
    buffer = BytesIO()
    frame.to_parquet(buffer, index=False)
    return buffer.getvalue()


def _parquet_ready_row(row: dict[str, object]) -> dict[str, object]:
    """Convert nested raw payloads to deterministic JSON strings for Parquet."""
    output = dict(row)
    raw = output.pop("raw", None)
    if raw is not None:
        output["raw_json"] = json.dumps(raw, sort_keys=True, separators=(",", ":"))
    return output


def _sort_macro_observations(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    """Sort normalized FRED observations deterministically before serialization."""
    return sorted(
        rows,
        key=lambda row: (
            str(row.get("series_id") or ""),
            str(row.get("observation_date") or ""),
            str(row.get("realtime_start") or ""),
            str(row.get("realtime_end") or ""),
        ),
    )


def _parse_args() -> argparse.Namespace:
    """Parse CLI arguments for the historical FRED backfill."""
    parser = argparse.ArgumentParser(description="Backfill FRED macro/rates into R2.")
    parser.add_argument(
        "--config",
        default=str(DEFAULT_FRED_CONFIG_PATH),
        help="Path to the FRED archive config JSON.",
    )
    parser.add_argument("--from-date", metavar="YYYY-MM-DD")
    parser.add_argument("--to-date", metavar="YYYY-MM-DD")
    parser.add_argument(
        "--series-ids",
        nargs="*",
        default=None,
        help="Optional FRED series IDs. Defaults to config/fred_series.json.",
    )
    parser.add_argument("--overwrite", action="store_true", help="Rewrite existing R2 objects.")
    parser.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_FRED_PAGE_LIMIT,
        help=f"Page size for FRED pagination (default: {DEFAULT_FRED_PAGE_LIMIT}).",
    )
    args = parser.parse_args()
    if args.series_ids == []:
        parser.error("--series-ids requires at least one series when provided")
    return args


def main() -> int:
    """Run the FRED macro/rates backfill from the command line."""
    args = _parse_args()
    archive_config = load_fred_archive_config(Path(args.config))

    try:
        from_date = date.fromisoformat(args.from_date or archive_config.default_start_date)
        to_date = _resolve_to_date(args.to_date or archive_config.default_end_date)
    except ValueError as exc:
        logger.error("Invalid date argument: {}", exc)
        return 1

    if from_date > to_date:
        logger.error("--from-date must be <= --to-date")
        return 1

    series_ids = args.series_ids or archive_config.series_ids
    writer = R2Writer()
    fetcher = FredMacroFetcher(FredClientConfig.from_env())
    result = backfill_fred_archive(
        from_date=from_date,
        to_date=to_date,
        fetcher=fetcher,
        writer=writer,
        series_ids=series_ids,
        overwrite=args.overwrite,
        limit=args.limit,
    )
    logger.info("FRED macro/rates backfill complete: {}", result)
    return 0


def _resolve_to_date(value: str) -> date:
    """Resolve an explicit YYYY-MM-DD date or the config-level latest sentinel."""
    if value.strip().lower() == "latest":
        return date.today()
    return date.fromisoformat(value)


def _normalize_series_ids(series_ids: Sequence[str]) -> tuple[str, ...]:
    """Validate and normalize configured FRED series IDs."""
    if not series_ids:
        raise ValueError("series_ids must contain at least one series")
    normalized: set[str] = set()
    for series_id in series_ids:
        if not isinstance(series_id, str):
            raise TypeError("series_ids values must be strings")
        cleaned = series_id.strip().upper()
        if not cleaned:
            raise ValueError("series_ids cannot contain empty values")
        normalized.add(cleaned)
    return tuple(sorted(normalized))


if __name__ == "__main__":
    sys.exit(main())
