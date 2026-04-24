from __future__ import annotations

import importlib
import io
import json
from collections.abc import Mapping
from datetime import date as Date
from datetime import datetime

from core.contracts.schemas import FeatureRecord
from services.r2.paths import layer1_feature_path
from services.r2.writer import R2Writer


def feature_record_to_parquet_bytes(record: FeatureRecord | Mapping[str, object]) -> bytes:
    """Serialize one validated FeatureRecord into Parquet bytes."""
    validated_record = _coerce_feature_record(record)
    try:
        import pandas as pd

        importlib.import_module("pyarrow")
    except ModuleNotFoundError as exc:
        raise ModuleNotFoundError(
            "pandas and pyarrow are required to serialize FeatureRecord rows to Parquet."
        ) from exc

    frame = pd.DataFrame([_parquet_ready_row(validated_record)])
    buffer = io.BytesIO()
    frame.to_parquet(buffer, index=False)
    return buffer.getvalue()


def parquet_bytes_to_feature_record(data: bytes) -> FeatureRecord:
    """Deserialize one Layer 1 feature shard from Parquet bytes."""
    try:
        import pandas as pd

        importlib.import_module("pyarrow")
    except ModuleNotFoundError as exc:
        raise ModuleNotFoundError(
            "pandas and pyarrow are required to deserialize FeatureRecord rows from Parquet."
        ) from exc

    frame = pd.read_parquet(io.BytesIO(data))
    rows = frame.to_dict("records")
    if len(rows) != 1:
        raise ValueError(
            "Layer 1 feature shards must contain exactly one FeatureRecord row per parquet file."
        )
    return _feature_record_from_row(rows[0])


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


def read_feature_record(
    as_of_date: str | Date | datetime,
    ticker: str,
    writer: R2Writer | None = None,
) -> FeatureRecord:
    """Read one FeatureRecord shard from the active R2 backend."""
    key = layer1_feature_path(as_of_date, ticker)
    active_writer = writer or R2Writer()
    return parquet_bytes_to_feature_record(active_writer.get_object(key))


def _coerce_feature_record(record: FeatureRecord | Mapping[str, object]) -> FeatureRecord:
    """Normalize input into a validated FeatureRecord instance."""
    if isinstance(record, FeatureRecord):
        return record
    return FeatureRecord(**dict(record))


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
