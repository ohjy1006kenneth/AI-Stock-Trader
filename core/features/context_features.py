"""Layer 1 context feature composition.

Context features combine ticker-specific fundamentals and earnings-calendar
signals with market-wide macro/rates signals. All source computations preserve
their own point-in-time guards; this module only aligns them on date/ticker.
"""
from __future__ import annotations

import math
from typing import TYPE_CHECKING, Any

from core.contracts.schemas import FeatureRecord
from core.features.fundamentals_features import (
    FUNDAMENTAL_FEATURE_COLUMNS,
    compute_fundamentals_features,
)
from core.features.macro_features import MACRO_FEATURE_COLUMNS, compute_macro_features

if TYPE_CHECKING:
    import pandas as pd

CONTEXT_FEATURE_COLUMNS: tuple[str, ...] = (
    *FUNDAMENTAL_FEATURE_COLUMNS,
    *MACRO_FEATURE_COLUMNS,
)


def compute_context_features(
    fundamentals: pd.DataFrame,
    ohlcv: pd.DataFrame,
    macro: pd.DataFrame,
    ticker: str,
) -> pd.DataFrame:
    """Return aligned fundamentals, earnings-calendar, macro, and rates features.

    Args:
        fundamentals: Raw SimFin archive rows for one ticker.
        ohlcv: Adjusted OHLCV frame for the same ticker. Its trading dates define
            the emitted context feature dates.
        macro: Concatenated Layer 0 FRED macro/rates archive rows.
        ticker: Ticker symbol stamped on every output row.

    Returns:
        DataFrame with columns (`date`, `ticker`, *CONTEXT_FEATURE_COLUMNS*).
        Macro and rates values are market-wide and joined onto every ticker row.
    """
    fundamental_features = compute_fundamentals_features(
        fundamentals=fundamentals,
        ohlcv=ohlcv,
        ticker=ticker,
    )
    if len(fundamental_features) == 0:
        return _empty_context_frame(fundamental_features)

    macro_features = compute_macro_features(macro, fundamental_features["date"].tolist())
    context = fundamental_features.merge(macro_features, on="date", how="left")
    return context[["date", "ticker", *CONTEXT_FEATURE_COLUMNS]]


def context_features_to_records(features: pd.DataFrame) -> list[FeatureRecord]:
    """Convert a context-features frame into FeatureRecord instances."""
    records: list[FeatureRecord] = []
    for row in features.to_dict(orient="records"):
        feature_values = {
            name: _normalize_feature_value(row.get(name)) for name in CONTEXT_FEATURE_COLUMNS
        }
        records.append(
            FeatureRecord(
                date=str(row["date"]),
                ticker=str(row["ticker"]),
                features=feature_values,
            )
        )
    return records


def _empty_context_frame(frame: pd.DataFrame) -> pd.DataFrame:
    """Return an empty context frame using the caller's pandas implementation."""
    return frame[["date", "ticker"]].assign(
        **{column: None for column in CONTEXT_FEATURE_COLUMNS}
    )[["date", "ticker", *CONTEXT_FEATURE_COLUMNS]]


def _normalize_feature_value(value: Any) -> float | int | bool | None:
    """Convert a pandas/numpy scalar to a FeatureRecord-compatible primitive."""
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(numeric) or math.isinf(numeric):
        return None
    if isinstance(value, int) and not isinstance(value, bool):
        return int(value)
    return numeric
