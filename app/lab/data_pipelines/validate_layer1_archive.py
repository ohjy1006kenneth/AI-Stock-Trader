"""Layer 1 archive validator.

Mirrors the Layer 0 validator: confirms that every ticker in the declared
universe has a per-ticker feature history at `features/layer1/{ticker}.parquet`,
then checks that each file contains the expected universe dates. It emits a
JSON report under
`artifacts/reports/integration/layer1_archive_validation_{from}_to_{to}.json`.

The validator does not re-run feature computation. It only checks history-file
presence, row coverage, and basic schema integrity. `ready_for_layer2` flips to
True iff every expected ticker history is present and the present histories
round-trip through the FeatureRecord contract with the expected dates.
"""
from __future__ import annotations

import argparse
import csv
import io
import json
import os
import random
import sys
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass, field
from datetime import date as Date
from pathlib import Path
from typing import Protocol

from loguru import logger

def _resolve_repo_root() -> Path:
    """Return the repository root for local runs and Modal-mounted runs."""
    env_root = os.getenv("AI_STOCK_TRADER_REPO_ROOT")
    if env_root:
        return Path(env_root).resolve()
    resolved = Path(__file__).resolve()
    return resolved.parents[3] if len(resolved.parents) > 3 else resolved.parent


_REPO_ROOT = _resolve_repo_root()
sys.path.insert(0, str(_REPO_ROOT))

from core.features.io import parquet_bytes_to_feature_records  # noqa: E402
from services.r2.paths import (  # noqa: E402
    layer1_feature_path,
    layer1_ticker_history_path,
    raw_universe_path,
)

DEFAULT_REPORT_DIR = Path("artifacts/reports/integration")


class ArchiveReader(Protocol):
    """Object-store operations required by the Layer 1 validator."""

    def exists(self, key: str) -> bool:
        """Return True if the key exists in the archive."""

    def get_object(self, key: str) -> bytes:
        """Return the bytes stored at `key`."""

    def list_keys(self, prefix: str) -> list[str]:
        """List keys beneath `prefix`."""


@dataclass(frozen=True)
class Layer1ValidationReport:
    """Summary of one Layer 1 archive validation pass."""

    run_id: str
    from_date: str
    to_date: str
    expected_ticker_files: int
    present_ticker_files: int
    expected_rows: int
    present_rows: int
    schema_failures: int
    row_count_failures: int
    missing_ticker_files: list[str] = field(default_factory=list)
    schema_failure_keys: list[str] = field(default_factory=list)
    row_count_failure_keys: list[str] = field(default_factory=list)
    requested_dates: list[str] = field(default_factory=list)
    universe_counts_by_date: dict[str, int] = field(default_factory=dict)
    present_rows_by_ticker: dict[str, int] = field(default_factory=dict)
    missing_ticker_dates: dict[str, list[str]] = field(default_factory=dict)
    unexpected_ticker_dates: dict[str, list[str]] = field(default_factory=dict)
    duplicate_ticker_dates: dict[str, list[str]] = field(default_factory=dict)
    foreign_ticker_rows: dict[str, list[str]] = field(default_factory=dict)
    skipped_tickers: list[dict[str, object]] = field(default_factory=list)
    skipped_dates: list[dict[str, object]] = field(default_factory=list)
    output_prefixes: dict[str, str] = field(default_factory=dict)
    leakage_spot_checks: list[dict[str, object]] = field(default_factory=list)
    ready_for_layer2: bool = False

    def to_dict(self) -> dict:
        """Return a JSON-serializable dict of the validation summary."""
        return asdict(self)


def validate_layer1_archive(
    *,
    run_id: str,
    from_date: str,
    to_date: str,
    universe: Mapping[str, Sequence[str]],
    reader: ArchiveReader,
    output_prefixes: Mapping[str, str] | None = None,
) -> Layer1ValidationReport:
    """Validate that every ticker in `universe` has a complete feature history.

    Args:
        run_id: Identifier for this validation pass (mirrors Layer 0 runs).
        from_date: Inclusive YYYY-MM-DD lower bound for the report metadata.
        to_date: Inclusive YYYY-MM-DD upper bound for the report metadata.
        universe: Mapping of `date` (YYYY-MM-DD) to the list of tickers expected
            to have a Layer 1 shard on that date.
        reader: Object store providing `exists`/`get_object`/`list_keys`.

    Returns:
        Layer1ValidationReport.
    """
    _validate_iso_date(from_date, "from_date")
    _validate_iso_date(to_date, "to_date")

    expected_dates_by_ticker = _expected_dates_by_ticker(universe)
    requested_dates = _date_range_strings(from_date, to_date)
    universe_counts_by_date = {
        as_of_date: len({_normalize_ticker(ticker) for ticker in tickers})
        for as_of_date, tickers in sorted(universe.items())
    }
    missing: list[str] = []
    schema_failures: list[str] = []
    row_count_failures: list[str] = []
    present_files = 0
    present_rows = 0
    present_rows_by_ticker: dict[str, int] = {}
    missing_ticker_dates: dict[str, list[str]] = {}
    unexpected_ticker_dates: dict[str, list[str]] = {}
    duplicate_ticker_dates: dict[str, list[str]] = {}
    foreign_ticker_rows: dict[str, list[str]] = {}
    skipped_tickers: list[dict[str, object]] = []
    skipped_dates: list[dict[str, object]] = []

    for ticker, expected_dates in sorted(expected_dates_by_ticker.items()):
        key = layer1_ticker_history_path(ticker)
        if not reader.exists(key):
            missing.append(key)
            expected_window_dates = sorted(expected_dates)
            missing_ticker_dates[ticker] = expected_window_dates
            skipped_tickers.append(
                {
                    "ticker": ticker,
                    "reason": "missing_history_file",
                    "history_key": key,
                    "expected_dates": expected_window_dates,
                }
            )
            for date_text in expected_window_dates:
                skipped_dates.append(
                    {
                        "ticker": ticker,
                        "date": date_text,
                        "reason": "missing_history_file",
                    }
                )
            continue
        present_files += 1
        try:
            records = parquet_bytes_to_feature_records(reader.get_object(key))
        except Exception as exc:  # noqa: BLE001 — record any decode failure
            logger.warning("Layer 1 ticker history {} failed schema check: {}", key, exc)
            schema_failures.append(key)
            skipped_tickers.append(
                {
                    "ticker": ticker,
                    "reason": "schema_validation_failed",
                    "history_key": key,
                }
            )
            continue

        ticker_dates: list[str] = []
        date_counts: dict[str, int] = {}
        foreign_rows: list[str] = []
        for record in records:
            if record.ticker != ticker:
                foreign_rows.append(f"{record.date}/{record.ticker}")
                continue
            if record.date in expected_dates:
                ticker_dates.append(record.date)
                date_counts[record.date] = date_counts.get(record.date, 0) + 1

        actual_dates = set(ticker_dates)
        missing_dates = sorted(expected_dates - actual_dates)
        unexpected_dates = sorted(actual_dates - expected_dates)
        duplicate_dates = sorted(
            date_text for date_text, count in date_counts.items() if count > 1
        )
        present_rows_by_ticker[ticker] = len(ticker_dates)
        present_rows += len(ticker_dates)
        if missing_dates:
            missing_ticker_dates[ticker] = missing_dates
        if unexpected_dates:
            unexpected_ticker_dates[ticker] = unexpected_dates
        if duplicate_dates:
            duplicate_ticker_dates[ticker] = duplicate_dates
        if foreign_rows:
            foreign_ticker_rows[ticker] = sorted(foreign_rows)

        if (
            bool(foreign_rows)
            or bool(missing_dates)
            or bool(unexpected_dates)
            or bool(duplicate_dates)
        ):
            logger.warning(
                "Layer 1 ticker history {} window coverage mismatch expected={} actual={} "
                "missing={} unexpected={} duplicates={} foreign={}",
                key,
                len(expected_dates),
                len(actual_dates),
                len(missing_dates),
                len(unexpected_dates),
                len(duplicate_dates),
                bool(foreign_rows),
            )
            row_count_failures.append(key)
            for date_text in missing_dates:
                skipped_dates.append(
                    {
                        "ticker": ticker,
                        "date": date_text,
                        "reason": "missing_window_row",
                    }
                )

    expected_files = len(expected_dates_by_ticker)
    expected_rows = sum(len(dates) for dates in expected_dates_by_ticker.values())
    ready = expected_rows > 0 and not missing and not schema_failures and not row_count_failures
    leakage_spot_checks = _build_leakage_spot_checks(
        reader=reader,
        expected_dates_by_ticker=expected_dates_by_ticker,
        run_id=run_id,
    )
    if any(check["status"] == "fail" for check in leakage_spot_checks):
        ready = False
    return Layer1ValidationReport(
        run_id=run_id,
        from_date=from_date,
        to_date=to_date,
        expected_ticker_files=expected_files,
        present_ticker_files=present_files,
        expected_rows=expected_rows,
        present_rows=present_rows,
        schema_failures=len(schema_failures),
        row_count_failures=len(row_count_failures),
        missing_ticker_files=missing,
        schema_failure_keys=schema_failures,
        row_count_failure_keys=row_count_failures,
        requested_dates=requested_dates,
        universe_counts_by_date=universe_counts_by_date,
        present_rows_by_ticker=present_rows_by_ticker,
        missing_ticker_dates=missing_ticker_dates,
        unexpected_ticker_dates=unexpected_ticker_dates,
        duplicate_ticker_dates=duplicate_ticker_dates,
        foreign_ticker_rows=foreign_ticker_rows,
        skipped_tickers=skipped_tickers,
        skipped_dates=skipped_dates,
        output_prefixes=dict(output_prefixes or {}),
        leakage_spot_checks=leakage_spot_checks,
        ready_for_layer2=ready,
    )


def write_validation_report(
    report: Layer1ValidationReport,
    output_dir: Path | None = None,
) -> Path:
    """Persist the validation report as deterministic JSON and return its path."""
    target_dir = output_dir if output_dir is not None else DEFAULT_REPORT_DIR
    target_dir.mkdir(parents=True, exist_ok=True)
    filename = (
        f"layer1_archive_validation_{report.from_date}_to_{report.to_date}.json"
    )
    path = target_dir / filename
    path.write_text(json.dumps(report.to_dict(), indent=2, sort_keys=True), encoding="utf-8")
    return path


def load_universe_mapping(path: Path) -> dict[str, list[str]]:
    """Load a `{date: [tickers...]}` JSON mapping for validation."""
    with path.open("r", encoding="utf-8") as fp:
        data = json.load(fp)
    if not isinstance(data, dict):
        raise ValueError("Universe JSON must be an object mapping date -> [tickers]")
    universe: dict[str, list[str]] = {}
    for as_of_date, tickers in data.items():
        if not isinstance(as_of_date, str):
            raise ValueError("Universe JSON keys must be YYYY-MM-DD strings")
        if not isinstance(tickers, list):
            raise ValueError(f"Universe JSON value for {as_of_date} must be a list")
        cleaned: list[str] = []
        for ticker in tickers:
            if not isinstance(ticker, str) or not ticker.strip():
                raise ValueError("Universe JSON ticker entries must be non-empty strings")
            cleaned.append(ticker.strip().upper())
        universe[as_of_date] = cleaned
    return universe


def load_universe_mapping_from_r2(
    *,
    from_date: str,
    to_date: str,
    reader: ArchiveReader,
    requested_tickers: Sequence[str] | None = None,
) -> dict[str, list[str]]:
    """Load the eligible Layer 1 universe directly from Layer 0 R2 universe masks."""
    allowed_tickers = (
        {_normalize_ticker(ticker) for ticker in requested_tickers}
        if requested_tickers is not None
        else None
    )
    universe: dict[str, list[str]] = {}
    for date_text in _date_range_strings(from_date, to_date):
        payload = reader.get_object(raw_universe_path(date_text)).decode("utf-8")
        date_tickers: list[str] = []
        for row in csv.DictReader(io.StringIO(payload)):
            ticker = _normalize_csv_ticker(row.get("ticker"))
            if ticker is None:
                continue
            if allowed_tickers is not None and ticker not in allowed_tickers:
                continue
            if (
                _truthy(row.get("in_universe"))
                and _truthy(row.get("tradable"), default=True)
                and _truthy(row.get("liquid"), default=True)
                and _truthy(row.get("data_quality_ok"), default=True)
                and not _truthy(row.get("halted"))
            ):
                date_tickers.append(ticker)
        universe[date_text] = sorted(set(date_tickers))
    return universe


def _expected_dates_by_ticker(
    universe: Mapping[str, Sequence[str]],
) -> dict[str, set[str]]:
    """Invert a date->tickers universe mapping into ticker->expected dates."""
    expected: dict[str, set[str]] = {}
    for as_of_date, tickers in sorted(universe.items()):
        _validate_iso_date(as_of_date, "universe date")
        for ticker in tickers:
            if not isinstance(ticker, str):
                raise ValueError("universe ticker entries must be strings")
            normalized_ticker = ticker.strip().upper()
            if not normalized_ticker:
                raise ValueError("universe ticker entries must be non-empty strings")
            expected.setdefault(normalized_ticker, set()).add(as_of_date)
    return expected


def _date_range_strings(from_date: str, to_date: str) -> list[str]:
    """Return every date between the inclusive ISO bounds."""
    start = Date.fromisoformat(from_date)
    end = Date.fromisoformat(to_date)
    if start > end:
        raise ValueError("from_date must be <= to_date")
    current = start
    dates: list[str] = []
    while current <= end:
        dates.append(current.isoformat())
        current = current.fromordinal(current.toordinal() + 1)
    return dates


def _normalize_ticker(ticker: str) -> str:
    """Normalize one ticker symbol for report aggregation."""
    normalized = ticker.strip().upper()
    if not normalized:
        raise ValueError("ticker entries must be non-empty strings")
    return normalized


def _build_leakage_spot_checks(
    *,
    reader: ArchiveReader,
    expected_dates_by_ticker: Mapping[str, set[str]],
    run_id: str,
    sample_limit: int = 10,
) -> list[dict[str, object]]:
    """Run lightweight alignment checks on a sample of daily Layer 1 shards."""
    candidate_pairs = [
        (date_text, ticker)
        for ticker, date_values in sorted(expected_dates_by_ticker.items())
        for date_text in sorted(date_values)
    ]
    if len(candidate_pairs) <= sample_limit:
        sampled_pairs = candidate_pairs
    else:
        sampled_pairs = sorted(random.Random(run_id).sample(candidate_pairs, k=sample_limit))

    failures: list[dict[str, object]] = []
    seen_daily_shard = False
    for date_text, ticker in sampled_pairs:
        key = layer1_feature_path(date_text, ticker)
        if not reader.exists(key):
            failures.append(
                {
                    "ticker": ticker,
                    "date": date_text,
                    "reason": "missing_daily_shard",
                    "key": key,
                }
            )
            continue
        seen_daily_shard = True
        try:
            records = parquet_bytes_to_feature_records(reader.get_object(key))
        except Exception as exc:  # noqa: BLE001
            failures.append(
                {
                    "ticker": ticker,
                    "date": date_text,
                    "reason": "daily_shard_decode_failed",
                    "key": key,
                    "error": str(exc),
                }
            )
            continue
        if len(records) != 1:
            failures.append(
                {
                    "ticker": ticker,
                    "date": date_text,
                    "reason": "daily_shard_row_count_mismatch",
                    "key": key,
                    "rows": len(records),
                }
            )
            continue
        record = records[0]
        if record.date != date_text or record.ticker != ticker:
            failures.append(
                {
                    "ticker": ticker,
                    "date": date_text,
                    "reason": "daily_shard_identity_mismatch",
                    "key": key,
                    "record_date": record.date,
                    "record_ticker": record.ticker,
                }
            )

    return [
        {
            "name": "daily_shard_identity_alignment",
            "sampled_pairs": [
                {"date": date_text, "ticker": ticker} for date_text, ticker in sampled_pairs
            ],
            "status": (
                "skipped"
                if not seen_daily_shard
                else ("pass" if not failures else "fail")
            ),
            "failures": failures,
        }
    ]


def _validate_iso_date(value: str, label: str) -> None:
    """Raise ValueError if `value` is not a canonical YYYY-MM-DD string."""
    try:
        parsed = Date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"{label} must be YYYY-MM-DD: {value!r}") from exc
    if parsed.isoformat() != value:
        raise ValueError(f"{label} must be YYYY-MM-DD: {value!r}")


def _normalize_csv_ticker(value: object) -> str | None:
    """Normalize a CSV ticker field, returning None for blank rows."""
    if value is None:
        return None
    ticker = str(value).strip().upper()
    return ticker or None


def _truthy(value: object, *, default: bool = False) -> bool:
    """Return True for common CSV boolean values."""
    if value is None:
        return default
    normalized = str(value).strip().lower()
    if not normalized:
        return default
    return normalized in {"1", "true", "t", "yes", "y"}


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """Parse CLI arguments for the validator."""
    parser = argparse.ArgumentParser(description="Validate the Layer 1 feature archive.")
    parser.add_argument("--run-id", required=True, help="Run identifier for the validation pass.")
    parser.add_argument("--from-date", required=True, help="Inclusive lower bound (YYYY-MM-DD).")
    parser.add_argument("--to-date", required=True, help="Inclusive upper bound (YYYY-MM-DD).")
    source_group = parser.add_mutually_exclusive_group(required=True)
    source_group.add_argument(
        "--universe",
        help="Path to a JSON file mapping date -> [tickers].",
    )
    source_group.add_argument(
        "--use-r2-universe",
        action="store_true",
        help="Load the validation universe directly from Layer 0 raw/universe/*.csv in R2.",
    )
    parser.add_argument(
        "--tickers",
        nargs="*",
        default=None,
        help="Optional ticker filter when --use-r2-universe is selected.",
    )
    parser.add_argument(
        "--output-dir",
        default=str(DEFAULT_REPORT_DIR),
        help=f"Output directory for the JSON report (default: {DEFAULT_REPORT_DIR}).",
    )
    args = parser.parse_args(argv)
    if args.tickers == []:
        parser.error("--tickers requires at least one ticker when provided")
    if args.tickers is not None and not args.use_r2_universe:
        parser.error("--tickers may only be used with --use-r2-universe")
    return args


def main(argv: Sequence[str] | None = None) -> int:
    """CLI entry point for `python -m app.lab.data_pipelines.validate_layer1_archive`."""
    from services.r2.writer import R2Writer

    args = _parse_args(argv)
    reader = R2Writer()
    if args.use_r2_universe:
        universe = load_universe_mapping_from_r2(
            from_date=args.from_date.strip(),
            to_date=args.to_date.strip(),
            reader=reader,
            requested_tickers=args.tickers,
        )
    else:
        universe = load_universe_mapping(Path(args.universe))
    report = validate_layer1_archive(
        run_id=args.run_id.strip(),
        from_date=args.from_date.strip(),
        to_date=args.to_date.strip(),
        universe=universe,
        reader=reader,
    )
    output_path = write_validation_report(report, Path(args.output_dir))
    logger.info(
        "Layer 1 validation written to {} ready_for_layer2={}",
        output_path,
        report.ready_for_layer2,
    )
    return 0 if report.ready_for_layer2 else 1


if __name__ == "__main__":
    raise SystemExit(main())
