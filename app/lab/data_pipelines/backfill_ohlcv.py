"""Historical Tiingo OHLCV backfill into the canonical R2 raw price archive."""
from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, date, datetime
from io import BytesIO
from pathlib import Path
from typing import Protocol

from loguru import logger

_REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(_REPO_ROOT))

from core.contracts.schemas import OHLCVRecord  # noqa: E402
from services.r2.paths import raw_price_path, raw_security_master_path  # noqa: E402
from services.r2.writer import R2Writer  # noqa: E402
from services.tiingo.ohlcv_fetcher import TiingoClientConfig, TiingoOHLCVFetcher  # noqa: E402
from services.tiingo.security_master import TiingoSecurity, TiingoSecurityMaster  # noqa: E402
from services.wikipedia.sp500_universe import get_all_historical_tickers  # noqa: E402


class ObjectWriter(Protocol):
    """Subset of R2Writer used by the OHLCV backfill."""

    def put_object(self, key: str, data: bytes | str) -> None:
        """Write an object to storage."""

    def exists(self, key: str) -> bool:
        """Return True when an object already exists."""


class OHLCVFetcher(Protocol):
    """Subset of TiingoOHLCVFetcher used by the OHLCV backfill."""

    def fetch_security_records(
        self,
        security: TiingoSecurity,
        from_date: str,
        to_date: str,
    ) -> list[OHLCVRecord]:
        """Fetch validated OHLCV records for one resolved Tiingo security."""


RecordSerializer = Callable[[list[OHLCVRecord]], bytes]


@dataclass(frozen=True)
class BackfillResult:
    """Summary of a Tiingo OHLCV backfill run."""

    requested: int
    written: int
    skipped: int
    empty: int
    reference_key: str


def backfill_ohlcv_archive(
    from_date: date,
    to_date: date,
    *,
    fetcher: OHLCVFetcher,
    security_master: TiingoSecurityMaster,
    writer: ObjectWriter,
    tickers: list[str] | None = None,
    overwrite: bool = False,
    record_serializer: RecordSerializer | None = None,
) -> BackfillResult:
    """Backfill Tiingo OHLCV history into R2 keyed by stable security identity."""
    if from_date > to_date:
        raise ValueError("from_date must be <= to_date")

    ticker_source = (
        get_all_historical_tickers(from_date.isoformat(), to_date.isoformat())
        if tickers is None
        else tickers
    )
    requested_tickers = sorted(set(ticker_source))
    securities = [
        security
        for security in security_master.resolve_many(requested_tickers)
        if _security_overlaps_range(security, from_date, to_date)
    ]
    reference_key = raw_security_master_path(to_date)
    _write_reference_mapping(
        writer=writer,
        key=reference_key,
        from_date=from_date,
        to_date=to_date,
        securities=securities,
        overwrite=overwrite,
    )

    written = 0
    skipped = 0
    empty = 0
    serializer = record_serializer or _records_to_parquet_bytes

    for security in securities:
        price_key = raw_price_path(security.security_id)
        if writer.exists(price_key) and not overwrite:
            skipped += 1
            logger.info("Skipping existing Tiingo OHLCV archive {}", price_key)
            continue

        fetch_from_date, fetch_to_date = _security_fetch_range(security, from_date, to_date)
        records = fetcher.fetch_security_records(
            security=security,
            from_date=fetch_from_date.isoformat(),
            to_date=fetch_to_date.isoformat(),
        )
        if not records:
            empty += 1
            logger.warning("No Tiingo OHLCV rows returned for {}", security.ticker)
            continue

        writer.put_object(price_key, serializer(records))
        written += 1
        logger.info("Wrote {} Tiingo OHLCV rows to {}", len(records), price_key)

    return BackfillResult(
        requested=len(securities),
        written=written,
        skipped=skipped,
        empty=empty,
        reference_key=reference_key,
    )


def _records_to_parquet_bytes(records: list[OHLCVRecord]) -> bytes:
    """Serialize OHLCV records to Parquet bytes."""
    try:
        import pandas as pd
    except ModuleNotFoundError as exc:
        raise ModuleNotFoundError(
            "pandas and pyarrow are required to serialize Tiingo OHLCV records to Parquet. "
            "Install the Pi, Modal, or dev requirements before running the live backfill."
        ) from exc

    frame = pd.DataFrame([record.model_dump() for record in records])
    buffer = BytesIO()
    frame.to_parquet(buffer, index=False)
    return buffer.getvalue()


def _security_overlaps_range(security: TiingoSecurity, from_date: date, to_date: date) -> bool:
    """Return True when a security's Tiingo date range overlaps the requested range."""
    if security.end_date is not None and date.fromisoformat(security.end_date) < from_date:
        return False
    if security.start_date is not None and date.fromisoformat(security.start_date) > to_date:
        return False
    return True


def _security_fetch_range(security: TiingoSecurity, from_date: date, to_date: date) -> tuple[date, date]:
    """Clamp the requested backfill window to one security's active Tiingo date range."""
    fetch_from = from_date
    fetch_to = to_date
    if security.start_date is not None:
        fetch_from = max(fetch_from, date.fromisoformat(security.start_date))
    if security.end_date is not None:
        fetch_to = min(fetch_to, date.fromisoformat(security.end_date))
    return fetch_from, fetch_to


def _write_reference_mapping(
    *,
    writer: ObjectWriter,
    key: str,
    from_date: date,
    to_date: date,
    securities: list[TiingoSecurity],
    overwrite: bool,
) -> None:
    """Write the ticker-to-security reference mapping needed for archive resolution."""
    if writer.exists(key) and not overwrite:
        logger.info("Skipping existing Tiingo security reference {}", key)
        return

    payload = {
        "source": "tiingo",
        "from_date": from_date.isoformat(),
        "to_date": to_date.isoformat(),
        "generated_at": datetime.now(UTC).isoformat(),
        "securities": [security.to_reference_row() for security in securities],
    }
    writer.put_object(key, json.dumps(payload, sort_keys=True, separators=(",", ":")))


def _parse_args() -> argparse.Namespace:
    """Parse CLI arguments for the historical OHLCV backfill."""
    parser = argparse.ArgumentParser(description="Backfill Tiingo historical OHLCV into R2.")
    parser.add_argument("--from-date", required=True, metavar="YYYY-MM-DD")
    parser.add_argument("--to-date", required=True, metavar="YYYY-MM-DD")
    parser.add_argument(
        "--tickers",
        nargs="*",
        default=None,
        help="Optional ticker list. Defaults to all historical S&P 500 constituents.",
    )
    parser.add_argument("--overwrite", action="store_true", help="Rewrite existing R2 objects.")
    return parser.parse_args()


def main() -> int:
    """Run the Tiingo OHLCV backfill from the command line."""
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
    fetcher = TiingoOHLCVFetcher(TiingoClientConfig.from_env())
    security_master = TiingoSecurityMaster.fetch_supported_tickers()
    result = backfill_ohlcv_archive(
        from_date=from_date,
        to_date=to_date,
        fetcher=fetcher,
        security_master=security_master,
        writer=writer,
        tickers=args.tickers,
        overwrite=args.overwrite,
    )
    logger.info("Tiingo OHLCV backfill complete: {}", result)
    return 0


if __name__ == "__main__":
    sys.exit(main())
