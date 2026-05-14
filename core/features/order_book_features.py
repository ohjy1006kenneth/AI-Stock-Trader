"""Optional Layer 1 order-book feature computation from archived pre-open snapshots.

The branch is provider-agnostic and expects normalized rows with:
- `date`
- `ticker`
- `captured_at`
- `bid_price`
- `ask_price`
- `bid_size`
- `ask_size`

Rows captured at or after the target market open are ignored so the emitted
FeatureRecords remain safe to join on the target `(date, ticker)`.
"""
from __future__ import annotations

import importlib
import math
from collections.abc import Sequence
from datetime import time
from typing import TYPE_CHECKING, Any

from core.contracts.schemas import FeatureRecord

if TYPE_CHECKING:
    import pandas as pd

ORDER_BOOK_FEATURE_COLUMNS: tuple[str, ...] = (
    "l2_bid_ask_spread",
    "l2_quoted_spread_bps",
    "l2_book_imbalance",
    "l2_snapshot_count",
)

REQUIRED_ORDER_BOOK_COLUMNS: frozenset[str] = frozenset(
    {"date", "ticker", "captured_at", "bid_price", "ask_price", "bid_size", "ask_size"}
)


def compute_order_book_features(
    frame: pd.DataFrame,
    *,
    target_date: str,
    tickers: Sequence[str],
    market_timezone: str = "America/New_York",
    market_open: time = time(9, 30),
) -> pd.DataFrame:
    """Aggregate one day of archived order-book rows into per-ticker features.

    Invalid snapshots (missing values, negative sizes, non-positive prices,
    or crossed quotes) are ignored. Tickers with no usable pre-open rows still
    get one output row with null numeric features and `l2_snapshot_count=0`.
    """
    pd = _require_pandas()
    _validate_columns(frame)
    if market_open.tzinfo is not None:
        raise ValueError("market_open must be a naive local market time")

    normalized_tickers = tuple(
        sorted({str(ticker).strip().upper() for ticker in tickers if str(ticker).strip()})
    )
    if not normalized_tickers:
        return _empty_frame(pd)

    base = pd.DataFrame({"ticker": list(normalized_tickers)})
    base.insert(0, "date", target_date)
    base["l2_bid_ask_spread"] = float("nan")
    base["l2_quoted_spread_bps"] = float("nan")
    base["l2_book_imbalance"] = float("nan")
    base["l2_snapshot_count"] = 0

    if len(frame.index) == 0:
        return base[["date", "ticker", *ORDER_BOOK_FEATURE_COLUMNS]]

    working = frame.copy()
    working["date"] = working["date"].astype(str)
    working["ticker"] = working["ticker"].astype(str).str.strip().str.upper()
    working = working[
        working["date"].eq(target_date) & working["ticker"].isin(normalized_tickers)
    ].reset_index(drop=True)
    if len(working.index) == 0:
        return base[["date", "ticker", *ORDER_BOOK_FEATURE_COLUMNS]]

    captured_at = pd.to_datetime(working["captured_at"], utc=True, errors="coerce")
    local_captured_at = captured_at.dt.tz_convert(market_timezone)
    market_open_at = pd.Timestamp(f"{target_date} {market_open.isoformat()}").tz_localize(
        market_timezone
    )
    working = working.assign(_captured_at=captured_at, _local_captured_at=local_captured_at)
    working = working[working["_local_captured_at"] < market_open_at].reset_index(drop=True)
    if len(working.index) == 0:
        return base[["date", "ticker", *ORDER_BOOK_FEATURE_COLUMNS]]

    numeric_columns = ("bid_price", "ask_price", "bid_size", "ask_size")
    for column in numeric_columns:
        working[column] = pd.to_numeric(working[column], errors="coerce")
    working = working.dropna(subset=["_captured_at", *numeric_columns]).reset_index(drop=True)
    if len(working.index) == 0:
        return base[["date", "ticker", *ORDER_BOOK_FEATURE_COLUMNS]]

    working = working[
        (working["bid_price"] > 0.0)
        & (working["ask_price"] > 0.0)
        & (working["bid_size"] >= 0.0)
        & (working["ask_size"] >= 0.0)
        & (working["ask_price"] >= working["bid_price"])
        & ((working["bid_size"] + working["ask_size"]) > 0.0)
    ].reset_index(drop=True)
    if len(working.index) == 0:
        return base[["date", "ticker", *ORDER_BOOK_FEATURE_COLUMNS]]

    working["l2_bid_ask_spread"] = working["ask_price"] - working["bid_price"]
    working["l2_quoted_spread_bps"] = (
        working["l2_bid_ask_spread"] / ((working["ask_price"] + working["bid_price"]) / 2.0)
    ) * 10_000.0
    working["l2_book_imbalance"] = (
        (working["bid_size"] - working["ask_size"])
        / (working["bid_size"] + working["ask_size"])
    )
    working = working.sort_values(["ticker", "_captured_at"]).reset_index(drop=True)

    latest = working.groupby("ticker", sort=True).tail(1).copy()
    counts = (
        working.groupby("ticker", sort=True)
        .size()
        .rename("l2_snapshot_count")
        .reset_index()
    )
    latest = latest.merge(counts, on="ticker", how="left")
    latest = latest[
        [
            "ticker",
            "l2_bid_ask_spread",
            "l2_quoted_spread_bps",
            "l2_book_imbalance",
            "l2_snapshot_count",
        ]
    ]

    result = base.merge(latest, on="ticker", how="left", suffixes=("", "_latest"))
    for column in ORDER_BOOK_FEATURE_COLUMNS:
        latest_column = f"{column}_latest"
        if latest_column in result.columns:
            result[column] = result[latest_column].combine_first(result[column])
            result = result.drop(columns=[latest_column])
    return result[["date", "ticker", *ORDER_BOOK_FEATURE_COLUMNS]]


def order_book_features_to_records(features: pd.DataFrame) -> list[FeatureRecord]:
    """Convert a per-ticker order-book feature frame into FeatureRecords."""
    records: list[FeatureRecord] = []
    for row in features.to_dict(orient="records"):
        feature_values = {
            name: _normalize_feature_value(row.get(name)) for name in ORDER_BOOK_FEATURE_COLUMNS
        }
        records.append(
            FeatureRecord(
                date=str(row["date"]),
                ticker=str(row["ticker"]),
                features=feature_values,
            )
        )
    return records


def _validate_columns(frame: pd.DataFrame) -> None:
    """Raise when the raw order-book archive is missing required columns."""
    missing = sorted(REQUIRED_ORDER_BOOK_COLUMNS - set(frame.columns))
    if missing:
        raise ValueError(f"Order-book frame missing required columns: {missing}")


def _empty_frame(pd: Any) -> pd.DataFrame:
    """Return an empty order-book feature frame with canonical columns."""
    return pd.DataFrame(columns=["date", "ticker", *ORDER_BOOK_FEATURE_COLUMNS])


def _normalize_feature_value(value: Any) -> float | int | bool | None:
    """Convert a pandas/numpy scalar to a FeatureRecord-compatible primitive."""
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return value
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(numeric) or math.isinf(numeric):
        return None
    if float(numeric).is_integer():
        return int(numeric)
    return numeric


def _require_pandas() -> Any:
    """Import pandas/pyarrow lazily with a clear dependency error."""
    try:
        import pandas as pd

        importlib.import_module("pyarrow")
    except ModuleNotFoundError as exc:
        raise ModuleNotFoundError(
            "pandas and pyarrow are required to compute order-book features."
        ) from exc
    return pd
