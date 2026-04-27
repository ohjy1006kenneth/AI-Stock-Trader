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
import json
import sys
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass, field
from datetime import date as Date
from pathlib import Path
from typing import Protocol

from loguru import logger

_REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(_REPO_ROOT))

from core.features.io import parquet_bytes_to_feature_records  # noqa: E402
from services.r2.paths import layer1_ticker_history_path  # noqa: E402

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
    missing: list[str] = []
    schema_failures: list[str] = []
    row_count_failures: list[str] = []
    present_files = 0
    present_rows = 0

    for ticker, expected_dates in sorted(expected_dates_by_ticker.items()):
        key = layer1_ticker_history_path(ticker)
        if not reader.exists(key):
            missing.append(key)
            continue
        present_files += 1
        try:
            records = parquet_bytes_to_feature_records(reader.get_object(key))
        except Exception as exc:  # noqa: BLE001 — record any decode failure
            logger.warning("Layer 1 ticker history {} failed schema check: {}", key, exc)
            schema_failures.append(key)
            continue

        present_rows += len(records)
        actual_dates = {record.date for record in records if record.ticker == ticker}
        has_foreign_rows = any(record.ticker != ticker for record in records)
        if (
            has_foreign_rows
            or actual_dates != expected_dates
            or len(records) != len(expected_dates)
        ):
            logger.warning(
                "Layer 1 ticker history {} row coverage mismatch expected={} actual={} foreign={}",
                key,
                len(expected_dates),
                len(actual_dates),
                has_foreign_rows,
            )
            row_count_failures.append(key)

    expected_files = len(expected_dates_by_ticker)
    expected_rows = sum(len(dates) for dates in expected_dates_by_ticker.values())
    ready = expected_rows > 0 and not missing and not schema_failures and not row_count_failures
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


def _validate_iso_date(value: str, label: str) -> None:
    """Raise ValueError if `value` is not a canonical YYYY-MM-DD string."""
    try:
        parsed = Date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"{label} must be YYYY-MM-DD: {value!r}") from exc
    if parsed.isoformat() != value:
        raise ValueError(f"{label} must be YYYY-MM-DD: {value!r}")


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """Parse CLI arguments for the validator."""
    parser = argparse.ArgumentParser(description="Validate the Layer 1 feature archive.")
    parser.add_argument("--run-id", required=True, help="Run identifier for the validation pass.")
    parser.add_argument("--from-date", required=True, help="Inclusive lower bound (YYYY-MM-DD).")
    parser.add_argument("--to-date", required=True, help="Inclusive upper bound (YYYY-MM-DD).")
    parser.add_argument(
        "--universe",
        required=True,
        help="Path to a JSON file mapping date -> [tickers].",
    )
    parser.add_argument(
        "--output-dir",
        default=str(DEFAULT_REPORT_DIR),
        help=f"Output directory for the JSON report (default: {DEFAULT_REPORT_DIR}).",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    """CLI entry point for `python -m app.lab.data_pipelines.validate_layer1_archive`."""
    from services.r2.writer import R2Writer

    args = _parse_args(argv)
    universe = load_universe_mapping(Path(args.universe))
    reader = R2Writer()
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
