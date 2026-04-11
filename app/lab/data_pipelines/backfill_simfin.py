"""Historical SimFin fundamentals backfill into the canonical R2 raw archive."""
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

from services.r2.paths import raw_fundamentals_path  # noqa: E402
from services.r2.writer import R2Writer  # noqa: E402
from services.simfin.fundamentals_fetcher import (  # noqa: E402
    DEFAULT_SIMFIN_PAGE_LIMIT,
    DEFAULT_SIMFIN_PERIODS,
    DEFAULT_SIMFIN_STATEMENTS,
    SimFinClientConfig,
    SimFinFundamentalsFetcher,
)
from services.wikipedia.sp500_universe import get_all_historical_tickers  # noqa: E402


class ObjectWriter(Protocol):
    """Subset of R2Writer used by the SimFin backfill."""

    def put_object(self, key: str, data: bytes | str) -> None:
        """Write an object to storage."""

    def exists(self, key: str) -> bool:
        """Return True when an object already exists."""


class FundamentalsFetcher(Protocol):
    """Subset of SimFinFundamentalsFetcher used by the backfill."""

    def fetch_all_fundamentals(
        self,
        *,
        tickers: Sequence[str],
        start_date: str,
        end_date: str,
        statements: Sequence[str],
        periods: Sequence[str],
        limit: int,
    ) -> list[dict[str, object]]:
        """Fetch all raw SimFin rows for a ticker/date range."""


FundamentalsSerializer = Callable[[list[dict[str, object]]], bytes]


@dataclass(frozen=True)
class BackfillResult:
    """Summary of a SimFin fundamentals backfill run."""

    requested_tickers: int
    written: int
    skipped: int
    empty: int
    total_rows: int
    output_key: str


def backfill_simfin_archive(
    from_date: date,
    to_date: date,
    *,
    fetcher: FundamentalsFetcher,
    writer: ObjectWriter,
    tickers: list[str] | None = None,
    statements: Sequence[str] = DEFAULT_SIMFIN_STATEMENTS,
    periods: Sequence[str] = DEFAULT_SIMFIN_PERIODS,
    overwrite: bool = False,
    limit: int = DEFAULT_SIMFIN_PAGE_LIMIT,
    serializer: FundamentalsSerializer | None = None,
) -> BackfillResult:
    """Backfill SimFin as-reported fundamentals into R2."""
    if from_date > to_date:
        raise ValueError("from_date must be <= to_date")
    if limit <= 0:
        raise ValueError("limit must be positive")

    ticker_source = (
        sorted(set(get_all_historical_tickers(from_date.isoformat(), to_date.isoformat())))
        if tickers is None
        else sorted(set(tickers))
    )
    if not ticker_source:
        raise ValueError("tickers must contain at least one ticker")

    output_key = raw_fundamentals_path(from_date, to_date)
    if writer.exists(output_key) and not overwrite:
        logger.info("Skipping existing SimFin fundamentals archive {}", output_key)
        return BackfillResult(
            requested_tickers=len(ticker_source),
            written=0,
            skipped=1,
            empty=0,
            total_rows=0,
            output_key=output_key,
        )

    rows = fetcher.fetch_all_fundamentals(
        tickers=ticker_source,
        start_date=from_date.isoformat(),
        end_date=to_date.isoformat(),
        statements=statements,
        periods=periods,
        limit=limit,
    )
    payload_serializer = serializer or _fundamentals_to_parquet_bytes
    writer.put_object(output_key, payload_serializer(_sort_fundamentals(rows)))
    logger.info("Wrote {} SimFin fundamentals rows to {}", len(rows), output_key)
    return BackfillResult(
        requested_tickers=len(ticker_source),
        written=1,
        skipped=0,
        empty=0 if rows else 1,
        total_rows=len(rows),
        output_key=output_key,
    )


def _fundamentals_to_parquet_bytes(rows: list[dict[str, object]]) -> bytes:
    """Serialize normalized SimFin fundamentals to Parquet bytes."""
    try:
        import pandas as pd
        importlib.import_module("pyarrow")
    except ModuleNotFoundError as exc:
        raise ModuleNotFoundError(
            "pandas and pyarrow are required to serialize SimFin fundamentals to Parquet. "
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


def _sort_fundamentals(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    """Sort normalized fundamentals deterministically before archive serialization."""
    return sorted(
        rows,
        key=lambda row: (
            str(row.get("ticker") or ""),
            str(row.get("report_date") or ""),
            str(row.get("availability_date") or ""),
            str(row.get("statement") or ""),
        ),
    )


def _parse_args() -> argparse.Namespace:
    """Parse CLI arguments for the historical SimFin backfill."""
    parser = argparse.ArgumentParser(description="Backfill SimFin fundamentals into R2.")
    parser.add_argument("--from-date", required=True, metavar="YYYY-MM-DD")
    parser.add_argument("--to-date", required=True, metavar="YYYY-MM-DD")
    parser.add_argument(
        "--tickers",
        nargs="*",
        default=None,
        help="Optional ticker list. Defaults to all historical S&P 500 constituents.",
    )
    parser.add_argument(
        "--statements",
        nargs="*",
        default=DEFAULT_SIMFIN_STATEMENTS,
        help="SimFin statement groups to fetch.",
    )
    parser.add_argument(
        "--periods",
        nargs="*",
        default=DEFAULT_SIMFIN_PERIODS,
        help="SimFin periods to fetch.",
    )
    parser.add_argument("--overwrite", action="store_true", help="Rewrite existing R2 objects.")
    parser.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_SIMFIN_PAGE_LIMIT,
        help=f"Page size for SimFin pagination (default: {DEFAULT_SIMFIN_PAGE_LIMIT}).",
    )
    args = parser.parse_args()
    if args.tickers == []:
        parser.error("--tickers requires at least one ticker when provided")
    if args.statements == []:
        parser.error("--statements requires at least one statement when provided")
    if args.periods == []:
        parser.error("--periods requires at least one period when provided")
    return args


def main() -> int:
    """Run the SimFin fundamentals backfill from the command line."""
    args = _parse_args()
    try:
        from_date = date.fromisoformat(args.from_date)
        to_date = date.fromisoformat(args.to_date)
    except ValueError as exc:
        logger.error("Invalid date argument: {}", exc)
        return 1

    if from_date > to_date:
        logger.error("--from-date must be <= --to-date")
        return 1

    writer = R2Writer()
    fetcher = SimFinFundamentalsFetcher(SimFinClientConfig.from_env())
    result = backfill_simfin_archive(
        from_date=from_date,
        to_date=to_date,
        fetcher=fetcher,
        writer=writer,
        tickers=args.tickers,
        statements=args.statements,
        periods=args.periods,
        overwrite=args.overwrite,
        limit=args.limit,
    )
    logger.info("SimFin fundamentals backfill complete: {}", result)
    return 0


if __name__ == "__main__":
    sys.exit(main())
