"""Layer 1 market-branch feature computation from adjusted OHLCV bars.

All features are shifted by one bar so that the value emitted on date T depends
only on bars strictly before T. Consumers can therefore join these rows into
the aligned FeatureRecord contract without re-checking the temporal invariant.

Cross-asset features (`spy_*`, `beta_60d`) are populated only when a benchmark
OHLCV frame (typically SPY) is supplied; otherwise those columns are NaN.
"""
from __future__ import annotations

import importlib
import math
from collections.abc import Iterable
from typing import TYPE_CHECKING, Any

from core.contracts.schemas import FeatureRecord

if TYPE_CHECKING:
    import pandas as pd

MARKET_FEATURE_COLUMNS: tuple[str, ...] = (
    "returns_1d",
    "returns_5d",
    "returns_21d",
    "returns_63d",
    "momentum_21",
    "realized_vol_5d",
    "realized_vol_21d",
    "vol_ratio_5_21",
    "atr_14",
    "price_vs_sma20",
    "price_vs_sma50",
    "golden_cross_50_200",
    "rsi_14",
    "macd_signal",
    "volume_ratio_20",
    "price_volume_corr_10",
    "overnight_gap",
    "spy_return_1d",
    "spy_return_5d",
    "stock_vs_spy_return_5d",
    "beta_60d",
)

REQUIRED_OHLCV_COLUMNS: frozenset[str] = frozenset(
    {"date", "open", "high", "low", "close", "adj_close", "volume"}
)

TRADING_DAYS_PER_YEAR = 252


def compute_market_features(
    bars: pd.DataFrame,
    ticker: str,
    *,
    benchmark_bars: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Return per-date market features for one ticker with leakage guard applied.

    Args:
        bars: OHLCV frame matching the OHLCVRecord contract (date, open, high,
            low, close, adj_close, volume columns; one row per trading day).
        ticker: Ticker symbol to stamp on every emitted row.
        benchmark_bars: Optional benchmark OHLCV frame (typically SPY) used for
            `spy_*` and `beta_60d` features. When omitted those columns are NaN.

    Returns:
        DataFrame with columns (`date`, `ticker`, *MARKET_FEATURE_COLUMNS*). The
        value emitted on row date T depends only on bars strictly before T.

    Raises:
        ValueError: If the OHLCV frame is missing a required column.
        ModuleNotFoundError: If pandas/pyarrow are not installed.
    """
    pd = _require_pandas()

    _validate_columns(bars)
    if len(bars) == 0:
        return _empty_frame(pd)

    frame = _sorted_bars(bars, pd)
    adj_close = frame["adj_close"].astype(float)
    open_price = frame["open"].astype(float)
    high = frame["high"].astype(float)
    low = frame["low"].astype(float)
    close = frame["close"].astype(float)
    volume = frame["volume"].astype(float)

    features = pd.DataFrame(index=frame.index)

    # Momentum
    features["returns_1d"] = adj_close.pct_change(1)
    features["returns_5d"] = adj_close.pct_change(5)
    features["returns_21d"] = adj_close.pct_change(21)
    features["returns_63d"] = adj_close.pct_change(63)
    features["momentum_21"] = adj_close.pct_change(21).shift(1)

    # Volatility
    annualization = math.sqrt(TRADING_DAYS_PER_YEAR)
    daily_return = features["returns_1d"]
    features["realized_vol_5d"] = daily_return.rolling(5).std() * annualization
    features["realized_vol_21d"] = daily_return.rolling(21).std() * annualization
    features["vol_ratio_5_21"] = features["realized_vol_5d"] / features["realized_vol_21d"]
    features["atr_14"] = _compute_atr(pd, high=high, low=low, close=close, window=14)

    # Trend
    sma_20 = adj_close.rolling(20).mean()
    sma_50 = adj_close.rolling(50).mean()
    sma_200 = adj_close.rolling(200).mean()
    features["price_vs_sma20"] = adj_close / sma_20 - 1.0
    features["price_vs_sma50"] = adj_close / sma_50 - 1.0
    features["golden_cross_50_200"] = (sma_50 > sma_200).astype("int64")
    features["rsi_14"] = _compute_rsi(pd, close=adj_close, window=14)
    features["macd_signal"] = _compute_macd_signal(adj_close)

    # Volume
    features["volume_ratio_20"] = volume / volume.rolling(20).mean()
    features["price_volume_corr_10"] = daily_return.rolling(10).corr(volume.pct_change(1))

    # Gap — use adjusted close on T-1 so splits do not produce phantom gaps
    features["overnight_gap"] = open_price / close.shift(1) - 1.0

    # Cross-asset
    _attach_benchmark_features(
        features,
        pd,
        feature_dates=frame["date"].tolist(),
        daily_return=daily_return,
        benchmark_bars=benchmark_bars,
    )

    # Leakage guard: shift so row T reflects only data before T's open
    features = features.shift(1)

    features.insert(0, "ticker", ticker)
    features.insert(0, "date", frame["date"].to_numpy())
    return features[["date", "ticker", *MARKET_FEATURE_COLUMNS]]


def market_features_to_records(features: pd.DataFrame) -> list[FeatureRecord]:
    """Convert a market-features frame into a list of FeatureRecord instances."""
    records: list[FeatureRecord] = []
    for row in features.to_dict(orient="records"):
        ticker = str(row["ticker"])
        feature_values = {
            name: _normalize_feature_value(row.get(name)) for name in MARKET_FEATURE_COLUMNS
        }
        records.append(
            FeatureRecord(
                date=str(row["date"]),
                ticker=ticker,
                features=feature_values,
            )
        )
    return records


def _attach_benchmark_features(
    features: pd.DataFrame,
    pd: Any,
    *,
    feature_dates: Iterable[str],
    daily_return: pd.Series,
    benchmark_bars: pd.DataFrame | None,
) -> None:
    """Populate the `spy_*` and `beta_60d` columns on the feature frame in-place."""
    cross_columns = ("spy_return_1d", "spy_return_5d", "stock_vs_spy_return_5d", "beta_60d")
    if benchmark_bars is None or len(benchmark_bars) == 0:
        for column in cross_columns:
            features[column] = float("nan")
        return

    benchmark = benchmark_bars.sort_values("date").drop_duplicates("date").reset_index(drop=True)
    bench_adj = benchmark.set_index("date")["adj_close"].astype(float)
    aligned = bench_adj.reindex(list(feature_dates))
    bench_return_1d = aligned.pct_change(1).reset_index(drop=True)
    bench_return_5d = aligned.pct_change(5).reset_index(drop=True)

    features["spy_return_1d"] = bench_return_1d.to_numpy()
    features["spy_return_5d"] = bench_return_5d.to_numpy()
    features["stock_vs_spy_return_5d"] = features["returns_5d"] - bench_return_5d.to_numpy()

    covariance = daily_return.rolling(60).cov(bench_return_1d)
    variance = bench_return_1d.rolling(60).var()
    features["beta_60d"] = covariance / variance


def _compute_atr(
    pd: Any,
    *,
    high: pd.Series,
    low: pd.Series,
    close: pd.Series,
    window: int,
) -> pd.Series:
    """Return the Wilder-style average true range over `window` bars."""
    prev_close = close.shift(1)
    true_range = pd.concat(
        [
            (high - low).abs(),
            (high - prev_close).abs(),
            (low - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    return true_range.rolling(window).mean()


def _compute_rsi(pd: Any, *, close: pd.Series, window: int) -> pd.Series:
    """Return the classic relative-strength index over `window` bars."""
    delta = close.diff()
    gain = delta.clip(lower=0.0)
    loss = (-delta).clip(lower=0.0)
    avg_gain = gain.rolling(window).mean()
    avg_loss = loss.rolling(window).mean()
    rs = avg_gain / avg_loss
    return 100.0 - 100.0 / (1.0 + rs)


def _compute_macd_signal(close: pd.Series) -> pd.Series:
    """Return the MACD histogram (MACD line minus signal line)."""
    ema_12 = close.ewm(span=12, adjust=False).mean()
    ema_26 = close.ewm(span=26, adjust=False).mean()
    macd_line = ema_12 - ema_26
    signal_line = macd_line.ewm(span=9, adjust=False).mean()
    return macd_line - signal_line


def _validate_columns(bars: pd.DataFrame) -> None:
    """Raise if any required OHLCV column is missing."""
    missing = sorted(REQUIRED_OHLCV_COLUMNS - set(bars.columns))
    if missing:
        raise ValueError(f"OHLCV frame missing required columns: {missing}")


def _sorted_bars(bars: pd.DataFrame, pd: Any) -> pd.DataFrame:
    """Return a date-sorted copy with contiguous row indices."""
    return bars.sort_values("date").drop_duplicates("date").reset_index(drop=True).copy()


def _empty_frame(pd: Any) -> pd.DataFrame:
    """Return an empty feature frame with canonical columns."""
    return pd.DataFrame(columns=["date", "ticker", *MARKET_FEATURE_COLUMNS])


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
    if isinstance(value, (int,)) and not isinstance(value, bool):
        return int(value)
    return numeric


def _require_pandas() -> Any:
    """Import pandas/pyarrow lazily with a clear error when absent."""
    try:
        import pandas as pd

        importlib.import_module("pyarrow")
    except ModuleNotFoundError as exc:
        raise ModuleNotFoundError(
            "pandas and pyarrow are required for market feature computation."
        ) from exc
    return pd
