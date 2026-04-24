"""Layer 1.5 HMM regime training-data preparation.

This module defines the fitting input that must exist before HMM regime
detection can be trained reliably. It is intentionally model-free: callers pass
Layer 0 benchmark OHLCV archives and FRED macro archives, and receive a
date-indexed numeric feature frame suitable for downstream HMM fitting.
"""
from __future__ import annotations

import importlib
import math
from typing import TYPE_CHECKING, Any

from core.features.macro_features import compute_macro_features

if TYPE_CHECKING:
    import pandas as pd

HMM_TRAINING_FEATURE_COLUMNS: tuple[str, ...] = (
    "spy_log_return_1d",
    "spy_return_5d",
    "spy_realized_vol_21d",
    "spy_realized_vol_63d",
    "spy_vol_ratio_21_63",
    "spy_drawdown_63d",
    "vix_level",
    "vix_change_5d",
    "yield_curve_slope_10y_2y",
    "yield_curve_slope_10y_3m",
    "high_yield_spread",
)

HMM_TRAINING_COLUMNS: tuple[str, ...] = (
    "date",
    *HMM_TRAINING_FEATURE_COLUMNS,
    "is_complete",
)

REQUIRED_BENCHMARK_COLUMNS: frozenset[str] = frozenset(
    {"date", "open", "high", "low", "close", "adj_close", "volume"}
)

TRADING_DAYS_PER_YEAR = 252


def build_hmm_training_frame(
    benchmark_bars: pd.DataFrame,
    macro: pd.DataFrame,
) -> pd.DataFrame:
    """Return the market-wide HMM training feature frame.

    Args:
        benchmark_bars: Benchmark OHLCV rows, typically SPY, normalized to the
            `OHLCVRecord` column shape.
        macro: Layer 0 FRED archive rows consumed by `compute_macro_features`.

    Returns:
        DataFrame with one row per benchmark date and columns
        (`date`, *HMM_TRAINING_FEATURE_COLUMNS*, `is_complete`). Market features
        emitted for date T use benchmark bars strictly before T; macro features
        use FRED observations known strictly before T.
    """
    pd = _require_pandas()
    _validate_benchmark_columns(benchmark_bars)

    if len(benchmark_bars) == 0:
        return _empty_frame(pd)

    bars = _sorted_bars(benchmark_bars)
    dates = bars["date"].tolist()
    macro_features = compute_macro_features(macro, dates)
    market_features = _compute_benchmark_regime_features(pd, bars)
    frame = market_features.merge(macro_features, on="date", how="left")

    frame = frame[["date", *HMM_TRAINING_FEATURE_COLUMNS]]
    frame["is_complete"] = frame[list(HMM_TRAINING_FEATURE_COLUMNS)].apply(
        lambda row: all(_is_finite_number(value) for value in row),
        axis=1,
    )
    return frame[list(HMM_TRAINING_COLUMNS)]


def complete_hmm_training_matrix(training_frame: pd.DataFrame) -> pd.DataFrame:
    """Return complete numeric HMM fitting rows indexed by date.

    This helper is the handoff to a future HMM fitter. It drops incomplete rows,
    validates the expected training-frame columns, and returns only numeric
    feature columns indexed by `date`.
    """
    _validate_training_columns(training_frame)
    complete_rows = training_frame[training_frame["is_complete"].astype(bool)].copy()
    matrix = complete_rows[["date", *HMM_TRAINING_FEATURE_COLUMNS]].set_index("date")
    return matrix.astype(float)


def _compute_benchmark_regime_features(pd: Any, bars: pd.DataFrame) -> pd.DataFrame:
    """Return leakage-safe benchmark-derived regime features."""
    adj_close = bars["adj_close"].astype(float)
    log_return = (adj_close / adj_close.shift(1)).map(
        lambda value: math.log(value) if _is_finite_number(value) and value > 0.0 else float("nan")
    )
    simple_return = adj_close.pct_change(1, fill_method=None)
    rolling_high_63 = adj_close.rolling(63).max()

    features = pd.DataFrame({"date": bars["date"]})
    features["spy_log_return_1d"] = log_return
    features["spy_return_5d"] = adj_close.pct_change(5, fill_method=None)
    features["spy_realized_vol_21d"] = simple_return.rolling(21).std() * math.sqrt(
        TRADING_DAYS_PER_YEAR
    )
    features["spy_realized_vol_63d"] = simple_return.rolling(63).std() * math.sqrt(
        TRADING_DAYS_PER_YEAR
    )
    features["spy_vol_ratio_21_63"] = (
        features["spy_realized_vol_21d"] / features["spy_realized_vol_63d"]
    )
    features["spy_drawdown_63d"] = adj_close / rolling_high_63 - 1.0

    shifted = features.drop(columns=["date"]).shift(1)
    shifted.insert(0, "date", features["date"].to_numpy())
    return shifted


def _validate_benchmark_columns(bars: pd.DataFrame) -> None:
    """Raise when benchmark OHLCV rows are missing required columns."""
    missing = sorted(REQUIRED_BENCHMARK_COLUMNS - set(bars.columns))
    if missing:
        raise ValueError(f"Benchmark OHLCV frame missing required columns: {missing}")


def _validate_training_columns(training_frame: pd.DataFrame) -> None:
    """Raise when a HMM training frame is missing required columns."""
    missing = sorted(set(HMM_TRAINING_COLUMNS) - set(training_frame.columns))
    if missing:
        raise ValueError(f"HMM training frame missing required columns: {missing}")


def _sorted_bars(bars: pd.DataFrame) -> pd.DataFrame:
    """Return date-sorted benchmark bars with duplicate dates removed."""
    return bars.sort_values("date").drop_duplicates("date").reset_index(drop=True).copy()


def _empty_frame(pd: Any) -> pd.DataFrame:
    """Return an empty HMM training frame with canonical columns."""
    return pd.DataFrame(columns=list(HMM_TRAINING_COLUMNS))


def _is_finite_number(value: Any) -> bool:
    """Return True when value is numeric and finite."""
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return False
    return math.isfinite(numeric)


def _require_pandas() -> Any:
    """Import pandas/pyarrow lazily with a clear error when absent."""
    try:
        import pandas as pd

        importlib.import_module("pyarrow")
    except ModuleNotFoundError as exc:
        raise ModuleNotFoundError(
            "pandas and pyarrow are required for HMM training-data preparation."
        ) from exc
    return pd
