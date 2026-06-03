from __future__ import annotations

import importlib
import io
import json
from collections.abc import Mapping, Sequence
from datetime import date as Date
from datetime import datetime

from core.contracts.schemas import FeatureRecord
from services.r2.paths import layer1_feature_path, layer1_ticker_history_path
from services.r2.writer import R2Writer


def feature_record_to_parquet_bytes(record: FeatureRecord | Mapping[str, object]) -> bytes:
    """Serialize one validated FeatureRecord into Parquet bytes."""
    return feature_records_to_parquet_bytes([record])


def feature_records_to_parquet_bytes(
    records: Sequence[FeatureRecord | Mapping[str, object]],
) -> bytes:
    """Serialize validated FeatureRecords into Parquet bytes."""
    validated_records = _coerce_feature_records(records)
    try:
        import pandas as pd

        importlib.import_module("pyarrow")
    except ModuleNotFoundError as exc:
        raise ModuleNotFoundError(
            "pandas and pyarrow are required to serialize FeatureRecord rows to Parquet."
        ) from exc

    frame = pd.DataFrame([_parquet_ready_row(record) for record in validated_records])
    buffer = io.BytesIO()
    frame.to_parquet(buffer, index=False)
    return buffer.getvalue()


def parquet_bytes_to_feature_record(data: bytes) -> FeatureRecord:
    """Deserialize one Layer 1 feature shard from Parquet bytes."""
    records = parquet_bytes_to_feature_records(data)
    if len(records) != 1:
        raise ValueError(
            "Layer 1 feature shards must contain exactly one FeatureRecord row per parquet file."
        )
    return records[0]


def parquet_bytes_to_feature_records(data: bytes) -> list[FeatureRecord]:
    """Deserialize a Layer 1 feature history Parquet payload."""
    try:
        import pandas as pd

        importlib.import_module("pyarrow")
    except ModuleNotFoundError as exc:
        raise ModuleNotFoundError(
            "pandas and pyarrow are required to deserialize FeatureRecord rows from Parquet."
        ) from exc

    frame = pd.read_parquet(io.BytesIO(data))
    rows = frame.to_dict("records")
    return [_feature_record_from_row(row) for row in rows]


def write_feature_record(
    record: FeatureRecord | Mapping[str, object],
    writer: R2Writer | None = None,
) -> str:
    """Validate and persist one FeatureRecord shard to the active R2 backend."""
    validated_record = _coerce_feature_record(record)
    key = layer1_feature_path(validated_record.date, validated_record.ticker)
    active_writer = writer or R2Writer()
    active_writer.put_object(key, feature_record_to_parquet_bytes(validated_record))
    return key


def write_feature_records(
    records: Sequence[FeatureRecord | Mapping[str, object]],
    writer: R2Writer | None = None,
) -> list[str]:
    """Validate and persist FeatureRecords to canonical shards plus legacy histories."""
    validated_records = _coerce_feature_records(records)
    grouped_records: dict[str, list[FeatureRecord]] = {}
    for record in validated_records:
        grouped_records.setdefault(record.ticker, []).append(record)

    active_writer = writer or R2Writer()
    legacy_history_keys: list[str] = []
    for ticker, ticker_records in sorted(grouped_records.items()):
        sorted_records = sorted(ticker_records, key=lambda record: record.date)
        _validate_unique_dates(ticker, sorted_records)
        for record in sorted_records:
            active_writer.put_object(
                layer1_feature_path(record.date, record.ticker),
                feature_record_to_parquet_bytes(record),
            )
        key = layer1_ticker_history_path(ticker)
        active_writer.put_object(key, feature_records_to_parquet_bytes(sorted_records))
        legacy_history_keys.append(key)
    return legacy_history_keys


def read_feature_record(
    as_of_date: str | Date | datetime,
    ticker: str,
    writer: R2Writer | None = None,
) -> FeatureRecord:
    """Read one FeatureRecord shard from the active R2 backend."""
    key = layer1_feature_path(as_of_date, ticker)
    active_writer = writer or R2Writer()
    return parquet_bytes_to_feature_record(active_writer.get_object(key))


def read_feature_records(
    ticker: str,
    writer: R2Writer | None = None,
) -> list[FeatureRecord]:
    """Read one ticker's Layer 1 FeatureRecord history from the active R2 backend."""
    key = layer1_ticker_history_path(ticker)
    active_writer = writer or R2Writer()
    return parquet_bytes_to_feature_records(active_writer.get_object(key))


def read_feature_history_window(
    ticker: str,
    *,
    start_date: str | Date | datetime | None = None,
    end_date: str | Date | datetime | None = None,
    writer: R2Writer | None = None,
) -> list[FeatureRecord]:
    """Read one ticker's Layer 1 history filtered to an inclusive date window."""
    start_text, end_text = _normalize_date_bounds(start_date=start_date, end_date=end_date)
    records = read_feature_records(ticker, writer=writer)
    return [
        record
        for record in records
        if (start_text is None or record.date >= start_text)
        and (end_text is None or record.date <= end_text)
    ]


def read_feature_histories_window(
    tickers: Sequence[str],
    *,
    start_date: str | Date | datetime | None = None,
    end_date: str | Date | datetime | None = None,
    writer: R2Writer | None = None,
    skip_missing: bool = False,
) -> dict[str, list[FeatureRecord]]:
    """Read selected Layer 1 histories filtered to an inclusive date window."""
    start_text, end_text = _normalize_date_bounds(start_date=start_date, end_date=end_date)
    histories: dict[str, list[FeatureRecord]] = {}
    for ticker in tickers:
        try:
            histories[ticker] = read_feature_history_window(
                ticker,
                start_date=start_text,
                end_date=end_text,
                writer=writer,
            )
        except FileNotFoundError:
            if not skip_missing:
                raise
    return histories


def _coerce_feature_record(record: FeatureRecord | Mapping[str, object]) -> FeatureRecord:
    """Normalize input into a validated FeatureRecord instance."""
    if isinstance(record, FeatureRecord):
        return record
    return FeatureRecord(**dict(record))


def _coerce_feature_records(
    records: Sequence[FeatureRecord | Mapping[str, object]],
) -> list[FeatureRecord]:
    """Normalize input rows into validated FeatureRecord instances."""
    if not records:
        raise ValueError("At least one FeatureRecord is required")
    return [_coerce_feature_record(record) for record in records]


def _validate_unique_dates(ticker: str, records: Sequence[FeatureRecord]) -> None:
    """Raise if a ticker history contains duplicate dates."""
    seen_dates: set[str] = set()
    duplicate_dates: set[str] = set()
    for record in records:
        if record.date in seen_dates:
            duplicate_dates.add(record.date)
        seen_dates.add(record.date)
    if duplicate_dates:
        duplicates = ", ".join(sorted(duplicate_dates))
        raise ValueError(f"Duplicate Layer 1 feature dates for ticker={ticker}: {duplicates}")


def _parquet_ready_row(record: FeatureRecord) -> dict[str, object]:
    """Convert a FeatureRecord into a deterministic Parquet-compatible row."""
    return {
        "date": record.date,
        "ticker": record.ticker,
        "features": json.dumps(record.features, sort_keys=True, separators=(",", ":")),
    }


def _feature_record_from_row(row: Mapping[str, object]) -> FeatureRecord:
    """Convert a Parquet row back into the canonical FeatureRecord contract."""
    raw_features = row.get("features")
    if not isinstance(raw_features, str):
        raise ValueError("Feature shard parquet rows must store the features field as JSON text.")

    parsed_features = json.loads(raw_features)
    if not isinstance(parsed_features, dict):
        raise ValueError("Feature shard parquet rows must decode to a feature dictionary.")

    return FeatureRecord(
        date=str(row["date"]),
        ticker=str(row["ticker"]),
        features=parsed_features,
    )


def _normalize_date_bounds(
    *,
    start_date: str | Date | datetime | None,
    end_date: str | Date | datetime | None,
) -> tuple[str | None, str | None]:
    """Normalize inclusive date bounds and validate the window ordering."""
    start_text = None if start_date is None else _coerce_date_text(start_date)
    end_text = None if end_date is None else _coerce_date_text(end_date)
    if start_text is not None and end_text is not None and start_text > end_text:
        raise ValueError("start_date must be less than or equal to end_date")
    return start_text, end_text


def _coerce_date_text(value: str | Date | datetime) -> str:
    """Normalize a date-like value to YYYY-MM-DD."""
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, Date):
        return value.isoformat()
    stripped = value.strip()
    try:
        return Date.fromisoformat(stripped).isoformat()
    except ValueError as exc:
        raise ValueError(f"Date must be YYYY-MM-DD: {value}") from exc
