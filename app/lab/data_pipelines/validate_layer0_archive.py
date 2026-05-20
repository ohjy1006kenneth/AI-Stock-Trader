"""Validate Layer 0 R2 archive coverage for the Alpaca SIP backfill window."""

from __future__ import annotations

import argparse
import importlib
import json
import sys
from collections.abc import Callable, Sequence
from dataclasses import asdict, dataclass
from datetime import date, timedelta
from io import BytesIO
from pathlib import Path
from typing import Protocol

from loguru import logger

_REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(_REPO_ROOT))

from core.features.loaders import available_macro_series_by_date  # noqa: E402
from services.r2.paths import (  # noqa: E402
    is_canonical_raw_price_key,
    layer0_ohlcv_provenance_report_path,
    pipeline_manifest_path,
    raw_fundamentals_path,
    raw_macro_path,
    raw_news_path,
    raw_universe_path,
)
from services.r2.writer import R2Writer  # noqa: E402
from services.wikipedia.sp500_universe import (  # noqa: E402
    fetch_html,
    parse_change_log,
    parse_current_tickers,
    reconstruct_at_date,
)


class ArchiveReader(Protocol):
    """Subset of R2Writer required to inspect an existing Layer 0 archive."""

    def exists(self, key: str) -> bool:
        """Return True when an object key already exists."""

    def list_keys(self, prefix: str) -> list[str]:
        """List object keys beneath the given prefix."""

    def get_object(self, key: str) -> bytes:
        """Read an object payload by key."""

DEFAULT_VALIDATION_FROM_DATE = "2017-01-01"
DEFAULT_REPORT_DIR = Path("artifacts/reports/integration")
DEFAULT_FUNDAMENTALS_MIN_ROWS = 1
FundamentalsRowCounter = Callable[[ArchiveReader, str], int]
_EXPECTED_OHLCV_POLICIES = {
    "historical_backfill": {
        "policy_id": "alpaca_historical_1day_adjustment_all",
        "provider": "alpaca",
        "request_adjustment": "all",
        "stored_ohlc_basis": "provider_adjusted",
        "normalized_adj_close_policy": "copy_close_to_adj_close",
        "feed": "sip",
    },
    # Deliberately omit `feed` here: daily runs may use IEX or SIP depending on deployment
    # configuration, so validation only enforces the raw-bar policy that must hold everywhere.
    "daily_incremental": {
        "policy_id": "alpaca_live_1day_adjustment_raw",
        "provider": "alpaca",
        "request_adjustment": "raw",
        "stored_ohlc_basis": "raw",
        "normalized_adj_close_policy": "copy_close_to_adj_close",
    },
}


@dataclass(frozen=True)
class Layer0ArchiveValidationReport:
    """Summary of Layer 0 R2 archive coverage for one validation window."""

    from_date: str
    to_date: str
    price_archive_count: int
    canonical_price_archive_count: int
    news_days_expected: int
    news_days_present: int
    universe_days_expected: int
    universe_days_present: int
    fundamentals_ticker_count: int
    fundamentals_tickers_expected: int
    fundamentals_tickers_present: int
    fundamentals_min_rows: int
    fundamentals_tickers_below_min_rows: list[str]
    macro_day_count: int
    macro_days_expected: int
    macro_days_recoverable: int
    macro_available_series_by_date: dict[str, list[str]]
    macro_missing_series_by_date: dict[str, list[str]]
    missing_macro_dates: list[str]
    macro_recovered_from_legacy_dates: list[str]
    manifest_present: bool
    ohlcv_provenance_report_present: bool
    ohlcv_provenance_policy_id: str | None
    ohlcv_provenance_validation_errors: list[str]
    ohlcv_split_like_discontinuity_count: int
    missing_news_dates: list[str]
    noncanonical_price_keys: list[str]
    missing_universe_dates: list[str]
    ready_for_layer1: bool


def validate_layer0_archive(
    *,
    from_date: date,
    to_date: date,
    run_id: str,
    reader: ArchiveReader,
    active_fundamentals_tickers: Sequence[str] | None = None,
    fundamentals_min_rows: int = DEFAULT_FUNDAMENTALS_MIN_ROWS,
    fundamentals_row_counter: FundamentalsRowCounter | None = None,
) -> Layer0ArchiveValidationReport:
    """Validate expected Layer 0 archive keys for the Alpaca SIP historical window."""
    if from_date > to_date:
        raise ValueError("from_date must be <= to_date")
    if fundamentals_min_rows < 0:
        raise ValueError("fundamentals_min_rows must be non-negative")

    price_keys = reader.list_keys("raw/prices/")
    canonical_price_keys = [key for key in price_keys if is_canonical_raw_price_key(key)]
    noncanonical_price_keys = sorted(
        key for key in price_keys if not is_canonical_raw_price_key(key)
    )
    fundamentals_keys = reader.list_keys("raw/fundamentals/")
    macro_keys = reader.list_keys("raw/macro/")
    calendar_days = _date_range(from_date, to_date)
    business_days = [day for day in calendar_days if day.weekday() < 5]
    missing_news = [day.isoformat() for day in calendar_days if not reader.exists(raw_news_path(day))]
    missing_universe = [
        day.isoformat() for day in business_days if not reader.exists(raw_universe_path(day))
    ]
    manifest_key = pipeline_manifest_path("layer0", run_id)
    manifest_present = reader.exists(manifest_key)
    manifest_payload = _read_json_object(reader, manifest_key) if manifest_present else None
    requested_macro_series = _manifest_fred_series_ids(manifest_payload)
    fundamentals_expected = 0
    fundamentals_present = len(fundamentals_keys)
    fundamentals_below_min: list[str] = []
    if active_fundamentals_tickers is not None:
        active_tickers = sorted({_normalize_ticker(ticker) for ticker in active_fundamentals_tickers})
        fundamentals_expected = len(active_tickers)
        fundamentals_present = 0
        counter = fundamentals_row_counter or _count_parquet_rows
        for ticker in active_tickers:
            key = raw_fundamentals_path(ticker)
            if not reader.exists(key):
                fundamentals_below_min.append(ticker)
                continue
            fundamentals_present += 1
            if counter(reader, key) < fundamentals_min_rows:
                fundamentals_below_min.append(ticker)

    provenance_report = _validate_ohlcv_provenance(
        reader=reader,
        run_id=run_id,
        manifest_payload=manifest_payload,
    )
    macro_available_series_by_date = available_macro_series_by_date(
        [day.isoformat() for day in business_days],
        writer=reader,  # type: ignore[arg-type]
        series_ids=requested_macro_series or None,
    )
    macro_missing_series_by_date: dict[str, list[str]] = {}
    missing_macro_dates: list[str] = []
    macro_recovered_from_legacy_dates: list[str] = []
    requested_macro_series_set = set(requested_macro_series)
    for day in business_days:
        date_text = day.isoformat()
        available_series = sorted(macro_available_series_by_date.get(date_text, []))
        available_series_set = set(available_series)
        if requested_macro_series_set:
            missing_series = sorted(requested_macro_series_set - available_series_set)
        else:
            missing_series = []
        macro_missing_series_by_date[date_text] = missing_series
        if (requested_macro_series_set and missing_series) or (
            not requested_macro_series_set and not available_series
        ):
            missing_macro_dates.append(date_text)
        if not reader.exists(raw_macro_path(date_text)) and available_series:
            macro_recovered_from_legacy_dates.append(date_text)

    ready = bool(canonical_price_keys) and not noncanonical_price_keys
    ready = ready and not missing_news and not missing_universe
    ready = ready and bool(fundamentals_keys) and bool(macro_keys) and manifest_present
    ready = ready and not fundamentals_below_min
    ready = ready and not missing_macro_dates
    ready = ready and provenance_report.ready

    return Layer0ArchiveValidationReport(
        from_date=from_date.isoformat(),
        to_date=to_date.isoformat(),
        price_archive_count=len(price_keys),
        canonical_price_archive_count=len(canonical_price_keys),
        news_days_expected=len(calendar_days),
        news_days_present=len(calendar_days) - len(missing_news),
        universe_days_expected=len(business_days),
        universe_days_present=len(business_days) - len(missing_universe),
        fundamentals_ticker_count=len(fundamentals_keys),
        fundamentals_tickers_expected=fundamentals_expected,
        fundamentals_tickers_present=fundamentals_present,
        fundamentals_min_rows=fundamentals_min_rows,
        fundamentals_tickers_below_min_rows=fundamentals_below_min,
        macro_day_count=len(macro_keys),
        macro_days_expected=len(business_days),
        macro_days_recoverable=len(business_days) - len(missing_macro_dates),
        macro_available_series_by_date=macro_available_series_by_date,
        macro_missing_series_by_date=macro_missing_series_by_date,
        missing_macro_dates=missing_macro_dates,
        macro_recovered_from_legacy_dates=macro_recovered_from_legacy_dates,
        manifest_present=manifest_present,
        ohlcv_provenance_report_present=provenance_report.report_present,
        ohlcv_provenance_policy_id=provenance_report.policy_id,
        ohlcv_provenance_validation_errors=provenance_report.errors,
        ohlcv_split_like_discontinuity_count=provenance_report.split_like_discontinuity_count,
        missing_news_dates=missing_news,
        noncanonical_price_keys=noncanonical_price_keys,
        missing_universe_dates=missing_universe,
        ready_for_layer1=ready,
    )


def write_validation_report(report: Layer0ArchiveValidationReport, output_dir: Path) -> Path:
    """Write one validation report JSON file under the integration reports directory."""
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / f"layer0_archive_validation_{report.from_date}_to_{report.to_date}.json"
    payload = json.dumps(asdict(report), indent=2, sort_keys=True) + "\n"
    path.write_text(payload, encoding="utf-8")
    return path


def _date_range(start: date, end: date) -> list[date]:
    days: list[date] = []
    current = start
    while current <= end:
        days.append(current)
        current += timedelta(days=1)
    return days


def _count_parquet_rows(reader: ArchiveReader, key: str) -> int:
    """Return the number of rows in a Parquet object-store payload."""
    payload = reader.get_object(key)
    if not payload:
        return 0
    try:
        importlib.import_module("pyarrow")
        from pyarrow import parquet
    except ModuleNotFoundError as exc:
        raise ModuleNotFoundError("pyarrow is required to validate fundamentals row coverage.") from exc
    return int(parquet.read_metadata(BytesIO(payload)).num_rows)


def _manifest_fred_series_ids(manifest_payload: dict[str, object] | None) -> list[str]:
    """Return normalized FRED series IDs declared in the Layer 0 manifest metadata."""
    if not isinstance(manifest_payload, dict):
        return []
    metadata = manifest_payload.get("metadata")
    if not isinstance(metadata, dict):
        return []
    value = metadata.get("fred_series_ids")
    if not isinstance(value, list):
        return []
    normalized: list[str] = []
    for series_id in value:
        if not isinstance(series_id, str):
            continue
        cleaned = series_id.strip().upper()
        if cleaned:
            normalized.append(cleaned)
    return sorted(set(normalized))


@dataclass(frozen=True)
class _OhlcvProvenanceValidation:
    """Result of validating Layer 0 OHLCV adjustment provenance artifacts."""

    ready: bool
    report_present: bool
    policy_id: str | None
    errors: list[str]
    split_like_discontinuity_count: int


def _validate_ohlcv_provenance(
    *,
    reader: ArchiveReader,
    run_id: str,
    manifest_payload: dict[str, object] | None,
) -> _OhlcvProvenanceValidation:
    """Validate manifest and report metadata for Layer 0 OHLCV adjustment provenance."""
    if manifest_payload is None:
        return _OhlcvProvenanceValidation(
            ready=False,
            report_present=False,
            policy_id=None,
            errors=["missing_manifest_payload"],
            split_like_discontinuity_count=0,
        )

    errors: list[str] = []
    metadata = manifest_payload.get("metadata")
    if not isinstance(metadata, dict):
        return _OhlcvProvenanceValidation(
            ready=False,
            report_present=False,
            policy_id=None,
            errors=["manifest_missing_metadata"],
            split_like_discontinuity_count=0,
        )

    mode = str(metadata.get("mode") or "").strip()
    expected_policy = _EXPECTED_OHLCV_POLICIES.get(mode)
    if expected_policy is None:
        errors.append(f"unsupported_layer0_mode:{mode or 'missing'}")

    prices_metadata = metadata.get("prices")
    if not isinstance(prices_metadata, dict):
        return _OhlcvProvenanceValidation(
            ready=False,
            report_present=False,
            policy_id=None,
            errors=errors + ["manifest_missing_prices_metadata"],
            split_like_discontinuity_count=0,
        )

    provenance_metadata = prices_metadata.get("adjustment_provenance")
    if not isinstance(provenance_metadata, dict):
        errors.append("manifest_missing_adjustment_provenance")
        provenance_metadata = {}

    report_key = str(
        prices_metadata.get("provenance_report_key") or layer0_ohlcv_provenance_report_path(run_id)
    ).strip()
    report_present = bool(report_key) and reader.exists(report_key)
    if report_key != layer0_ohlcv_provenance_report_path(run_id):
        errors.append("unexpected_provenance_report_key")
    if not report_present:
        errors.append("missing_provenance_report")
        return _OhlcvProvenanceValidation(
            ready=False,
            report_present=False,
            policy_id=str(provenance_metadata.get("policy_id") or "") or None,
            errors=errors,
            split_like_discontinuity_count=0,
        )

    report_payload = _read_json_object(reader, report_key)
    report_provenance = (
        report_payload.get("price_adjustment_provenance")
        if isinstance(report_payload, dict)
        else None
    )
    if not isinstance(report_provenance, dict):
        errors.append("report_missing_price_adjustment_provenance")
        report_provenance = {}

    archive_summary = report_payload.get("archive_summary") if isinstance(report_payload, dict) else None
    if not isinstance(archive_summary, dict):
        errors.append("report_missing_archive_summary")
        archive_summary = {}

    policy_id = str(report_provenance.get("policy_id") or provenance_metadata.get("policy_id") or "")
    split_like_discontinuity_count = int(archive_summary.get("split_like_discontinuity_count", 0))
    if expected_policy is not None:
        for field_name, expected_value in expected_policy.items():
            manifest_value = provenance_metadata.get(field_name)
            report_value = report_provenance.get(field_name)
            if manifest_value != expected_value:
                errors.append(f"manifest_{field_name}_expected_{expected_value}")
            if report_value != expected_value:
                errors.append(f"report_{field_name}_expected_{expected_value}")
            if manifest_value != report_value:
                errors.append(f"manifest_report_mismatch_{field_name}")

    observed_rows = int(archive_summary.get("observed_rows", 0))
    equal_rows = int(archive_summary.get("close_equals_adj_close_rows", 0))
    different_rows = int(archive_summary.get("close_diff_adj_close_rows", 0))
    if observed_rows != equal_rows + different_rows:
        errors.append("archive_summary_row_counts_inconsistent")
    if different_rows != 0:
        errors.append("normalized_adj_close_policy_violated")

    return _OhlcvProvenanceValidation(
        ready=not errors,
        report_present=True,
        policy_id=policy_id or None,
        errors=errors,
        split_like_discontinuity_count=split_like_discontinuity_count,
    )


def _read_json_object(reader: ArchiveReader, key: str) -> dict[str, object]:
    """Read and decode one JSON object payload from the archive."""
    payload = reader.get_object(key)
    parsed = json.loads(payload)
    if not isinstance(parsed, dict):
        raise ValueError(f"Expected JSON object at {key}")
    return dict(parsed)


def _normalize_ticker(ticker: str) -> str:
    """Normalize ticker text to the canonical raw fundamentals archive symbol."""
    if not isinstance(ticker, str):
        raise TypeError("ticker must be a string")
    cleaned = ticker.strip().upper().replace(".", "-")
    if not cleaned:
        raise ValueError("ticker cannot be empty")
    return cleaned


def _sp500_constituents(as_of_date: date) -> list[str]:
    """Return S&P 500 constituents at the validation end date."""
    html = fetch_html()
    current_tickers = parse_current_tickers(html)
    events = parse_change_log(html)
    return reconstruct_at_date(current_tickers, events, as_of_date.isoformat())


def _parse_args() -> argparse.Namespace:
    """Parse Layer 0 validation CLI arguments."""
    parser = argparse.ArgumentParser(description="Validate Layer 0 R2 archive coverage.")
    parser.add_argument("--from-date", default=DEFAULT_VALIDATION_FROM_DATE, metavar="YYYY-MM-DD")
    parser.add_argument("--to-date", required=True, metavar="YYYY-MM-DD")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--output-dir", default=str(DEFAULT_REPORT_DIR))
    parser.add_argument(
        "--fundamentals-min-rows",
        type=int,
        default=DEFAULT_FUNDAMENTALS_MIN_ROWS,
        help="Minimum required rows per active constituent fundamentals archive.",
    )
    parser.add_argument(
        "--active-fundamentals-tickers",
        nargs="*",
        default=None,
        help="Optional active ticker scope; defaults to S&P 500 constituents at --to-date.",
    )
    args = parser.parse_args()
    if args.active_fundamentals_tickers == []:
        parser.error("--active-fundamentals-tickers requires at least one ticker when provided")
    return args


def main() -> int:
    """Run Layer 0 archive validation from the command line."""
    args = _parse_args()
    try:
        from_date = date.fromisoformat(args.from_date)
        to_date = date.fromisoformat(args.to_date)
    except ValueError as exc:
        logger.error("Invalid date argument: {}", exc)
        return 1

    active_tickers = args.active_fundamentals_tickers or _sp500_constituents(to_date)
    report = validate_layer0_archive(
        from_date=from_date,
        to_date=to_date,
        run_id=args.run_id,
        reader=R2Writer(),
        active_fundamentals_tickers=active_tickers,
        fundamentals_min_rows=args.fundamentals_min_rows,
    )
    path = write_validation_report(report, Path(args.output_dir))
    logger.info("Layer 0 archive validation report written to {}", path)
    logger.info("ready_for_layer1={}", report.ready_for_layer1)
    return 0 if report.ready_for_layer1 else 2


if __name__ == "__main__":
    sys.exit(main())
