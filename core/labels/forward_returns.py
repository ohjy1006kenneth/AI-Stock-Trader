"""Layer 1 supervised-learning labels: forward returns and survival flags.

Labels are computed from the canonical Layer 0 OHLCV archive
(`raw/prices/{TICKER}.parquet`) and persisted to a separate label archive
(`labels/layer1/{date}/{ticker}.parquet`) so Layer 2 training can opt in per
experiment without coupling features to targets.

For row date T, `forward_return_Hd` = `adj_close(T+H) / adj_close(T) - 1`.
A delisting (or end-of-history) leaves later horizons with `None` and the
matching `survives_to_tH` set to `0`. Forward returns therefore never silently
drop a ticker; the survival flag exposes the gap.

This module reads only Layer 0 R2 archives — Layer 1 is forbidden from making
direct external provider calls. The Modal-runnable entrypoint at
`app/lab/data_pipelines/run_label_generation.py` orchestrates archive reads,
label computation, and per-(date, ticker) persistence.
"""
from __future__ import annotations

import importlib
import io
import json
import math
from collections.abc import Mapping
from datetime import date as Date
from datetime import datetime
from typing import TYPE_CHECKING, Any

from core.contracts.schemas import FeatureRecord
from services.r2.paths import layer1_label_path
from services.r2.writer import R2Writer

if TYPE_CHECKING:
    import pandas as pd

LABEL_FEATURE_COLUMNS: tuple[str, ...] = (
    "forward_return_1d",
    "forward_return_5d",
    "forward_return_20d",
    "forward_log_return_1d",
    "forward_log_return_5d",
    "forward_log_return_20d",
    "survives_to_t1",
    "survives_to_t5",
    "survives_to_t20",
)

LABEL_HORIZONS: tuple[int, ...] = (1, 5, 20)

REQUIRED_OHLCV_COLUMNS: frozenset[str] = frozenset({"date", "adj_close"})


def compute_forward_return_labels(
    ohlcv: pd.DataFrame,
    ticker: str,
) -> pd.DataFrame:
    """Return per-date forward-return labels for one ticker.

    Args:
        ohlcv: OHLCV frame matching the OHLCVRecord contract; only `date` and
            `adj_close` are consulted but the full schema is required so this
            function can be composed with `core.features.loaders.load_ohlcv_frame`.
        ticker: Ticker symbol stamped on every output row.

    Returns:
        DataFrame with columns (`date`, `ticker`, *LABEL_FEATURE_COLUMNS*). One
        row per trading day in the OHLCV frame, sorted ascending. Rows whose
        forward window extends past the end of the archive emit `None` for the
        return columns and `0` for the matching survival flag.
    """
    pd = _require_pandas()

    _validate_columns(ohlcv)

    if len(ohlcv) == 0:
        return _empty_frame(pd)

    frame = ohlcv.sort_values("date").drop_duplicates("date").reset_index(drop=True)
    adj_close = frame["adj_close"].astype(float)

    labels = pd.DataFrame({"date": frame["date"].to_numpy(), "ticker": ticker})
    for horizon in LABEL_HORIZONS:
        future = adj_close.shift(-horizon)
        simple_return = future / adj_close - 1.0
        log_return = (future / adj_close).map(_safe_log)
        survives = future.notna().astype("int64")

        labels[f"forward_return_{horizon}d"] = simple_return
        labels[f"forward_log_return_{horizon}d"] = log_return
        labels[f"survives_to_t{horizon}"] = survives

    return labels[["date", "ticker", *LABEL_FEATURE_COLUMNS]]


def forward_return_labels_to_records(labels: pd.DataFrame) -> list[FeatureRecord]:
    """Convert a labels frame into validated FeatureRecord rows."""
    records: list[FeatureRecord] = []
    for row in labels.to_dict(orient="records"):
        feature_values: dict[str, float | int | bool | None] = {
            name: _normalize_label_value(name, row.get(name)) for name in LABEL_FEATURE_COLUMNS
        }
        records.append(
            FeatureRecord(
                date=str(row["date"]),
                ticker=str(row["ticker"]),
                features=feature_values,
            )
        )
    return records


def write_label_record(
    record: FeatureRecord | Mapping[str, object],
    writer: R2Writer | None = None,
) -> str:
    """Validate and persist one label shard to the active R2 backend."""
    pd = _require_pandas()
    validated = _coerce_feature_record(record)
    key = layer1_label_path(validated.date, validated.ticker)
    active_writer = writer or R2Writer()
    payload = _label_record_to_parquet_bytes(pd, validated)
    active_writer.put_object(key, payload)
    return key


def read_label_record(
    as_of_date: str | Date | datetime,
    ticker: str,
    writer: R2Writer | None = None,
) -> FeatureRecord:
    """Read one label shard from the active R2 backend."""
    pd = _require_pandas()
    key = layer1_label_path(as_of_date, ticker)
    active_writer = writer or R2Writer()
    return _parquet_bytes_to_label_record(pd, active_writer.get_object(key))


def _label_record_to_parquet_bytes(pd: Any, record: FeatureRecord) -> bytes:
    """Serialize one FeatureRecord-shaped label into Parquet bytes."""
    frame = pd.DataFrame([_parquet_ready_row(record)])
    buffer = io.BytesIO()
    frame.to_parquet(buffer, index=False)
    return buffer.getvalue()


def _parquet_bytes_to_label_record(pd: Any, data: bytes) -> FeatureRecord:
    """Deserialize one Layer 1 label shard from Parquet bytes."""
    frame = pd.read_parquet(io.BytesIO(data))
    rows = frame.to_dict(orient="records")
    if len(rows) != 1:
        raise ValueError(
            "Layer 1 label shards must contain exactly one record per parquet file."
        )
    raw_features = rows[0].get("features")
    if not isinstance(raw_features, str):
        raise ValueError("Label shard parquet rows must store the features field as JSON text.")

    parsed_features = json.loads(raw_features)
    if not isinstance(parsed_features, dict):
        raise ValueError("Label shard parquet rows must decode to a feature dictionary.")

    return FeatureRecord(
        date=str(rows[0]["date"]),
        ticker=str(rows[0]["ticker"]),
        features=parsed_features,
    )


def _parquet_ready_row(record: FeatureRecord) -> dict[str, object]:
    """Convert a FeatureRecord into a deterministic Parquet-compatible row."""
    return {
        "date": record.date,
        "ticker": record.ticker,
        "features": json.dumps(record.features, sort_keys=True, separators=(",", ":")),
    }


def _coerce_feature_record(record: FeatureRecord | Mapping[str, object]) -> FeatureRecord:
    """Normalize input into a validated FeatureRecord instance."""
    if isinstance(record, FeatureRecord):
        return record
    return FeatureRecord(**dict(record))


def _validate_columns(ohlcv: pd.DataFrame) -> None:
    """Raise when the OHLCV frame is missing a required column."""
    missing = sorted(REQUIRED_OHLCV_COLUMNS - set(ohlcv.columns))
    if missing:
        raise ValueError(f"OHLCV frame missing required columns: {missing}")


def _empty_frame(pd: Any) -> pd.DataFrame:
    """Return an empty labels frame with canonical columns."""
    return pd.DataFrame(columns=["date", "ticker", *LABEL_FEATURE_COLUMNS])


def _safe_log(ratio: Any) -> float | None:
    """Return ln(ratio) when ratio is finite and positive, else None."""
    if ratio is None:
        return None
    try:
        numeric = float(ratio)
    except (TypeError, ValueError):
        return None
    if math.isnan(numeric) or numeric <= 0.0:
        return None
    return math.log(numeric)


def _normalize_label_value(name: str, value: Any) -> float | int | bool | None:
    """Coerce a pandas/numpy scalar to a FeatureRecord-compatible primitive."""
    if value is None:
        return None
    if name.startswith("survives_to_t"):
        try:
            survives = int(value)
        except (TypeError, ValueError):
            return None
        return 1 if survives else 0
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(numeric) or math.isinf(numeric):
        return None
    return numeric


def _require_pandas() -> Any:
    """Import pandas/pyarrow lazily with a clear error when absent."""
    try:
        import pandas as pd

        importlib.import_module("pyarrow")
    except ModuleNotFoundError as exc:
        raise ModuleNotFoundError(
            "pandas and pyarrow are required for label generation."
        ) from exc
    return pd
