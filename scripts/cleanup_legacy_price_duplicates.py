from __future__ import annotations

import argparse
import importlib
import json
import re
import sys
from collections.abc import Sequence
from dataclasses import asdict, dataclass
from io import BytesIO
from pathlib import Path
from typing import Protocol

from loguru import logger

_REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_REPO_ROOT))

from services.r2.paths import (  # noqa: E402
    RAW_PRICE_PREFIX,
    is_legacy_raw_price_key,
    raw_price_path,
)
from services.r2.writer import R2Writer  # noqa: E402

DEFAULT_REPORT_PATH = Path("artifacts/reports/diagnostics/legacy_price_duplicates.json")
_LEGACY_KEY_RE = re.compile(
    r"^raw/prices/(?P<ticker>.+)_(?P<start>[^_]+)_(?P<end>[^_]+)\.parquet$"
)


class ObjectStore(Protocol):
    """Object-store operations required for duplicate-price cleanup."""

    def list_keys(self, prefix: str) -> list[str]:
        """List object keys beneath a prefix."""

    def get_object(self, key: str) -> bytes:
        """Return the bytes stored at one object key."""

    def delete_object(self, key: str) -> None:
        """Delete one object key."""

    def exists(self, key: str) -> bool:
        """Return True when one object key exists."""


@dataclass(frozen=True)
class LegacyPriceDuplicateRecord:
    """Verification summary for one legacy raw-price object."""

    legacy_key: str
    canonical_key: str
    ticker: str
    legacy_row_count: int
    canonical_range_row_count: int
    legacy_min_date: str | None
    legacy_max_date: str | None
    missing_dates: list[str]
    verification_status: str
    reason: str


@dataclass(frozen=True)
class LegacyPriceDuplicateReport:
    """Inventory and verification summary for legacy raw-price objects."""

    candidate_count: int
    verified_safe_count: int
    unsafe_count: int
    deleted_count: int
    apply: bool
    allow_unverified_delete: bool
    records: list[LegacyPriceDuplicateRecord]


def audit_legacy_price_duplicates(
    writer: ObjectStore | None = None,
) -> LegacyPriceDuplicateReport:
    """Inventory and verify legacy raw-price objects against canonical ticker archives."""
    active_writer = writer or R2Writer()
    candidate_keys = [
        key for key in active_writer.list_keys(RAW_PRICE_PREFIX) if is_legacy_raw_price_key(key)
    ]
    records = [_build_duplicate_record(active_writer, key) for key in sorted(candidate_keys)]
    verified_safe_count = sum(record.verification_status == "verified_safe" for record in records)
    return LegacyPriceDuplicateReport(
        candidate_count=len(records),
        verified_safe_count=verified_safe_count,
        unsafe_count=len(records) - verified_safe_count,
        deleted_count=0,
        apply=False,
        allow_unverified_delete=False,
        records=records,
    )


def delete_legacy_price_duplicates(
    report: LegacyPriceDuplicateReport,
    *,
    writer: ObjectStore | None = None,
    allow_unverified_delete: bool = False,
) -> LegacyPriceDuplicateReport:
    """Delete verified legacy keys, optionally including unverified ones."""
    active_writer = writer or R2Writer()
    deleted_count = 0

    for record in report.records:
        if record.verification_status == "verified_safe" or allow_unverified_delete:
            active_writer.delete_object(record.legacy_key)
            deleted_count += 1

    return LegacyPriceDuplicateReport(
        candidate_count=report.candidate_count,
        verified_safe_count=report.verified_safe_count,
        unsafe_count=report.unsafe_count,
        deleted_count=deleted_count,
        apply=True,
        allow_unverified_delete=allow_unverified_delete,
        records=report.records,
    )


def write_report(report: LegacyPriceDuplicateReport, output_path: Path) -> Path:
    """Write one duplicate-cleanup report as JSON."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(asdict(report), indent=2, sort_keys=True) + "\n"
    output_path.write_text(payload, encoding="utf-8")
    return output_path


def _build_duplicate_record(
    writer: ObjectStore,
    legacy_key: str,
) -> LegacyPriceDuplicateRecord:
    """Verify one legacy raw-price object against its canonical ticker archive."""
    ticker = _legacy_ticker_from_key(legacy_key)
    canonical_key = raw_price_path(ticker)
    legacy_dates, legacy_row_count = _read_dates_and_row_count(writer.get_object(legacy_key), legacy_key)
    legacy_min_date = min(legacy_dates) if legacy_dates else None
    legacy_max_date = max(legacy_dates) if legacy_dates else None

    if not writer.exists(canonical_key):
        return LegacyPriceDuplicateRecord(
            legacy_key=legacy_key,
            canonical_key=canonical_key,
            ticker=ticker,
            legacy_row_count=legacy_row_count,
            canonical_range_row_count=0,
            legacy_min_date=legacy_min_date,
            legacy_max_date=legacy_max_date,
            missing_dates=legacy_dates,
            verification_status="missing_canonical",
            reason=f"Canonical archive missing: {canonical_key}",
        )

    canonical_dates, canonical_row_count = _read_dates_and_row_count(
        writer.get_object(canonical_key),
        canonical_key,
    )
    missing_dates = sorted(set(legacy_dates) - set(canonical_dates))
    if missing_dates:
        return LegacyPriceDuplicateRecord(
            legacy_key=legacy_key,
            canonical_key=canonical_key,
            ticker=ticker,
            legacy_row_count=legacy_row_count,
            canonical_range_row_count=_count_rows_in_range(
                canonical_dates,
                legacy_min_date,
                legacy_max_date,
            ),
            legacy_min_date=legacy_min_date,
            legacy_max_date=legacy_max_date,
            missing_dates=missing_dates,
            verification_status="canonical_missing_dates",
            reason="Canonical archive does not fully cover the legacy date set.",
        )

    canonical_range_row_count = _count_rows_in_range(
        canonical_dates,
        legacy_min_date,
        legacy_max_date,
    )
    if canonical_range_row_count < legacy_row_count:
        return LegacyPriceDuplicateRecord(
            legacy_key=legacy_key,
            canonical_key=canonical_key,
            ticker=ticker,
            legacy_row_count=legacy_row_count,
            canonical_range_row_count=canonical_range_row_count,
            legacy_min_date=legacy_min_date,
            legacy_max_date=legacy_max_date,
            missing_dates=[],
            verification_status="canonical_insufficient_rows",
            reason="Canonical archive has fewer rows than the legacy archive range.",
        )

    return LegacyPriceDuplicateRecord(
        legacy_key=legacy_key,
        canonical_key=canonical_key,
        ticker=ticker,
        legacy_row_count=legacy_row_count,
        canonical_range_row_count=canonical_range_row_count or canonical_row_count,
        legacy_min_date=legacy_min_date,
        legacy_max_date=legacy_max_date,
        missing_dates=[],
        verification_status="verified_safe",
        reason="Canonical archive fully covers the legacy rows.",
    )


def _read_dates_and_row_count(payload: bytes, key: str) -> tuple[list[str], int]:
    """Read normalized ISO dates and row count from one Parquet payload."""
    if not payload:
        return [], 0
    try:
        import pandas as pd

        importlib.import_module("pyarrow")
    except ModuleNotFoundError as exc:
        raise ModuleNotFoundError(
            "pandas and pyarrow are required to verify legacy raw price duplicates."
        ) from exc

    frame = pd.read_parquet(BytesIO(payload))
    if frame.empty:
        return [], 0
    if "date" not in frame.columns:
        raise ValueError(f"Parquet payload missing required 'date' column: {key}")
    normalized_dates = pd.to_datetime(frame["date"], errors="coerce")
    if normalized_dates.isna().any():
        raise ValueError(f"Parquet payload contains non-date values in 'date': {key}")
    return sorted(normalized_dates.dt.date.astype(str).tolist()), int(len(frame.index))


def _count_rows_in_range(
    normalized_dates: Sequence[str],
    start_date: str | None,
    end_date: str | None,
) -> int:
    """Count rows whose normalized date falls inside the inclusive legacy window."""
    if start_date is None or end_date is None:
        return 0
    return sum(start_date <= current_date <= end_date for current_date in normalized_dates)


def _legacy_ticker_from_key(key: str) -> str:
    """Extract the canonical ticker symbol from one legacy raw-price key."""
    match = _LEGACY_KEY_RE.fullmatch(key)
    if match is None:
        raise ValueError(f"Legacy raw price key does not match the expected pattern: {key}")
    return match.group("ticker")


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """Parse CLI arguments for the cleanup utility."""
    parser = argparse.ArgumentParser(
        description="Inventory and clean up legacy raw/prices/*_*.parquet objects."
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Delete legacy keys after verification.",
    )
    parser.add_argument(
        "--allow-unverified-delete",
        action="store_true",
        help="Also delete legacy keys that fail strict verification.",
    )
    parser.add_argument(
        "--output-path",
        default=str(DEFAULT_REPORT_PATH),
        help="Write the JSON audit report to this path.",
    )
    args = parser.parse_args(argv)
    if args.allow_unverified_delete and not args.apply:
        parser.error("--allow-unverified-delete requires --apply")
    return args


def main(argv: Sequence[str] | None = None) -> int:
    """CLI entry point for legacy raw-price duplicate cleanup."""
    args = _parse_args(argv)
    report = audit_legacy_price_duplicates()
    if args.apply:
        report = delete_legacy_price_duplicates(
            report,
            allow_unverified_delete=args.allow_unverified_delete,
        )
    output_path = write_report(report, Path(args.output_path))
    logger.info(
        "legacy_price_duplicates candidates={} safe={} unsafe={} deleted={} apply={} "
        "allow_unverified_delete={} report={}",
        report.candidate_count,
        report.verified_safe_count,
        report.unsafe_count,
        report.deleted_count,
        report.apply,
        report.allow_unverified_delete,
        output_path,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
