"""Readers for Layer 0 R2 archives consumed by Layer 1 feature computation.

Layer 1 never calls external data providers; it only reads the canonical Parquet
shards produced by Layer 0. These helpers centralize the Parquet-to-DataFrame
decoding so feature modules can stay storage-agnostic.
"""
from __future__ import annotations

import importlib
import io
from collections.abc import Sequence
from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING, Any

from core.data.macro_archive import (
    MACRO_SNAPSHOT_DATE_COLUMN,
    build_latest_available_macro_snapshot,
)
from services.r2.paths import (
    is_canonical_raw_macro_key,
    raw_fundamentals_path,
    raw_macro_date_from_key,
    raw_order_book_path,
    raw_price_path,
)
from services.r2.writer import R2Writer

if TYPE_CHECKING:
    import pandas as pd

MACRO_PARQUET_LOAD_MAX_WORKERS = 32
_EMPTY_MACRO_COLUMNS: tuple[str, ...] = (
    "source",
    "series_id",
    MACRO_SNAPSHOT_DATE_COLUMN,
    "observation_date",
    "realtime_start",
    "realtime_end",
    "retrieved_at",
    "value",
    "is_missing",
    "raw",
)


def load_ohlcv_frame(
    ticker: str,
    writer: R2Writer | None = None,
) -> pd.DataFrame:
    """Return the OHLCV frame for one ticker, sorted ascending by date.

    Reads `raw/prices/{ticker}.parquet` through the active R2 (or local mock)
    backend and returns a pandas DataFrame matching the OHLCVRecord columns.
    """
    pd = _require_pandas()
    active_writer = writer or R2Writer()
    payload = active_writer.get_object(raw_price_path(ticker))
    frame = pd.read_parquet(io.BytesIO(payload))
    return frame.sort_values("date").drop_duplicates("date").reset_index(drop=True)


def load_fundamentals_frame(
    ticker: str,
    writer: R2Writer | None = None,
) -> pd.DataFrame:
    """Return the Layer 0 fundamentals archive for one ticker, sorted by availability date.

    Reads `raw/fundamentals/{ticker}.parquet` through the active R2 (or local
    mock) backend. The returned frame carries the normalized Layer 0 columns
    (`report_date`, `availability_date`, `fiscal_year`, `fiscal_period`,
    `statement`, optional `earnings_date`, `raw_json`, ...). SEC-backed rows
    keep the same point-in-time archive contract but may omit `earnings_date`,
    so downstream earnings-calendar features resolve to `None` for SEC-only
    ticker histories.
    """
    pd = _require_pandas()
    active_writer = writer or R2Writer()
    payload = active_writer.get_object(raw_fundamentals_path(ticker))
    frame = pd.read_parquet(io.BytesIO(payload))
    if "availability_date" in frame.columns:
        return frame.sort_values("availability_date").reset_index(drop=True)
    return frame.reset_index(drop=True)


def load_macro_frame(
    writer: R2Writer | None = None,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
) -> pd.DataFrame:
    """Return concatenated Layer 0 FRED macro shards sorted point-in-time safely.

    Reads all `raw/macro/YYYY-MM-DD.parquet` shards through the active R2 (or
    local mock) backend. Optional date bounds restrict the run-date shard range
    before any object fetches happen, which keeps Modal catch-up runs from
    serially downloading the entire archive when only a bounded HMM window is
    needed. Layer 1 callers pass the resulting frame to
    `compute_macro_features`; no external data-provider calls are made here.
    """
    pd = _require_pandas()
    active_writer = writer or R2Writer()
    keys = sorted(
        key
        for key in active_writer.list_keys("raw/macro/")
        if is_canonical_raw_macro_key(key)
        and (start_date is None or raw_macro_date_from_key(key) >= start_date)
        and (end_date is None or raw_macro_date_from_key(key) <= end_date)
    )
    if not keys:
        return pd.DataFrame(columns=list(_EMPTY_MACRO_COLUMNS))

    def _load_one_macro_shard(key: str) -> pd.DataFrame:
        payload = active_writer.get_object(key)
        frame = pd.read_parquet(io.BytesIO(payload))
        if MACRO_SNAPSHOT_DATE_COLUMN not in frame.columns:
            frame[MACRO_SNAPSHOT_DATE_COLUMN] = raw_macro_date_from_key(key)
        return frame

    max_workers = min(MACRO_PARQUET_LOAD_MAX_WORKERS, len(keys))
    if max_workers <= 1:
        frames = [_load_one_macro_shard(key) for key in keys]
    else:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            frames = list(executor.map(_load_one_macro_shard, keys))
    frame = pd.concat(frames, ignore_index=True)
    identity_columns = [
        column
        for column in ("series_id", "observation_date", "realtime_start", "realtime_end")
        if column in frame.columns
    ]
    if identity_columns:
        dedupe_order = identity_columns + [
            column for column in ("retrieved_at",) if column in frame.columns
        ]
        frame = (
            frame.sort_values(dedupe_order)
            .drop_duplicates(subset=identity_columns, keep="last")
            .sort_values(identity_columns)
            .reset_index(drop=True)
        )
        return frame
    return frame.reset_index(drop=True)


def load_order_book_frame(
    provider: str,
    as_of_date: str,
    writer: R2Writer | None = None,
) -> pd.DataFrame:
    """Return one provider/day raw order-book archive frame from R2."""
    pd = _require_pandas()
    active_writer = writer or R2Writer()
    payload = active_writer.get_object(raw_order_book_path(provider, as_of_date))
    frame = pd.read_parquet(io.BytesIO(payload))
    return frame.reset_index(drop=True)


def available_macro_series_by_date(
    target_dates: Sequence[str],
    *,
    writer: R2Writer | None = None,
    series_ids: Sequence[str] | None = None,
) -> dict[str, list[str]]:
    """Return recoverable macro series IDs for each requested snapshot date."""
    normalized_dates = sorted({str(value).strip() for value in target_dates if str(value).strip()})
    if not normalized_dates:
        return {}

    macro_frame = load_macro_frame(writer=writer)
    if len(macro_frame.index) == 0:
        return {date_text: [] for date_text in normalized_dates}

    rows = macro_frame.to_dict("records")
    available: dict[str, list[str]] = {}
    for date_text in normalized_dates:
        snapshot_rows = build_latest_available_macro_snapshot(
            rows,
            snapshot_date=date_text,
            series_ids=series_ids,
        )
        available[date_text] = [str(row["series_id"]) for row in snapshot_rows]
    return available


def _require_pandas() -> Any:
    """Import pandas/pyarrow lazily with a clear error when absent."""
    try:
        import pandas as pd

        importlib.import_module("pyarrow")
    except ModuleNotFoundError as exc:
        raise ModuleNotFoundError(
            "pandas and pyarrow are required to load OHLCV archives from R2."
        ) from exc
    return pd
