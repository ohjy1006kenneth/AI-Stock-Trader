"""Readers for Layer 0 R2 archives consumed by Layer 1 feature computation.

Layer 1 never calls external data providers; it only reads the canonical Parquet
shards produced by Layer 0. These helpers centralize the Parquet-to-DataFrame
decoding so feature modules can stay storage-agnostic.
"""
from __future__ import annotations

import importlib
import io
from typing import TYPE_CHECKING, Any

from services.r2.paths import raw_fundamentals_path, raw_price_path
from services.r2.writer import R2Writer

if TYPE_CHECKING:
    import pandas as pd


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
    """Return the SimFin fundamentals archive for one ticker, sorted by availability date.

    Reads `raw/fundamentals/{ticker}.parquet` through the active R2 (or local
    mock) backend. The returned frame carries the normalized SimFin columns
    (`report_date`, `availability_date`, `fiscal_year`, `fiscal_period`,
    `statement`, `earnings_date`, `raw_json`, ...).
    """
    pd = _require_pandas()
    active_writer = writer or R2Writer()
    payload = active_writer.get_object(raw_fundamentals_path(ticker))
    frame = pd.read_parquet(io.BytesIO(payload))
    if "availability_date" in frame.columns:
        return frame.sort_values("availability_date").reset_index(drop=True)
    return frame.reset_index(drop=True)


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
