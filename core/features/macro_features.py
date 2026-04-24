"""Layer 1 macro context features from the Layer 0 FRED archive.

Macro features are market-wide: the same value applies to every ticker on a
given date. This module emits one row per target date with no `ticker` column;
feature assembly (M2.8) broadcasts these onto the per-(date, ticker) aligned
feature table.

Leakage rule:
- Each FRED observation carries a `realtime_start` date — when the value first
  became publicly known. A feature on date T may only use observations with
  `realtime_start < T`. This is stricter than the observation_date and correctly
  handles lagged releases (e.g. CPI for March publishes in mid-April).
- Daily series are forward-filled across weekends/holidays from the last known
  value. Monthly series are forward-filled across every trading day until the
  next release.
"""
from __future__ import annotations

import importlib
import math
from collections.abc import Iterable
from datetime import date as Date
from typing import TYPE_CHECKING, Any

from core.contracts.schemas import FeatureRecord

if TYPE_CHECKING:
    import pandas as pd

MACRO_FEATURE_COLUMNS: tuple[str, ...] = (
    "fed_funds_rate",
    "fed_funds_rate_change_5d",
    "treasury_3m",
    "treasury_2y",
    "treasury_10y",
    "treasury_10y_change_5d",
    "yield_curve_slope_10y_2y",
    "yield_curve_slope_10y_3m",
    "vix_level",
    "vix_change_5d",
    "dollar_index",
    "dollar_index_change_5d",
    "cpi_level",
    "cpi_change_yoy",
    "high_yield_spread",
)

SERIES_ID_BY_FEATURE: dict[str, str] = {
    "fed_funds_rate": "FEDFUNDS",
    "treasury_3m": "DGS3MO",
    "treasury_2y": "DGS2",
    "treasury_10y": "DGS10",
    "vix_level": "VIXCLS",
    "dollar_index": "DTWEXBGS",
    "cpi_level": "CPIAUCSL",
    "high_yield_spread": "BAMLH0A0HYM2",
}

REQUIRED_MACRO_COLUMNS: frozenset[str] = frozenset(
    {"series_id", "observation_date", "realtime_start", "value", "is_missing"}
)

CHANGE_5D_LOOKBACK_BDAYS = 5


def compute_macro_features(
    macro: pd.DataFrame,
    target_dates: Iterable[str],
) -> pd.DataFrame:
    """Return market-wide macro features for each target date.

    Args:
        macro: Concatenated Layer 0 FRED shards with the canonical columns
            (`series_id`, `observation_date`, `realtime_start`, `value`,
            `is_missing`).
        target_dates: Iterable of YYYY-MM-DD strings to emit features for.

    Returns:
        DataFrame with columns (`date`, *MACRO_FEATURE_COLUMNS*). One row per
        unique target date, sorted ascending. Callers broadcast across tickers
        during feature assembly (M2.8).
    """
    pd = _require_pandas()

    _validate_columns(macro)

    sorted_dates = sorted({str(value).strip() for value in target_dates if str(value).strip()})
    if not sorted_dates:
        return _empty_frame(pd)

    histories = _build_point_in_time_histories(macro)
    levels = _compute_level_frame(pd, sorted_dates, histories)
    return _compose_features(pd, levels)


def macro_features_to_records(
    features: pd.DataFrame,
    ticker: str,
) -> list[FeatureRecord]:
    """Broadcast macro feature rows onto one ticker as FeatureRecord instances."""
    records: list[FeatureRecord] = []
    for row in features.to_dict(orient="records"):
        feature_values = {
            name: _normalize_feature_value(row.get(name)) for name in MACRO_FEATURE_COLUMNS
        }
        records.append(
            FeatureRecord(
                date=str(row["date"]),
                ticker=ticker,
                features=feature_values,
            )
        )
    return records


def _build_point_in_time_histories(
    macro: pd.DataFrame,
) -> dict[str, list[tuple[str, str, float]]]:
    """Return an observation/vintage history per series_id, sorted ascending."""
    histories: dict[str, list[tuple[str, str, float]]] = {}
    if len(macro) == 0:
        return histories

    filtered = macro[~macro["is_missing"].astype(bool)]
    for series_id, group in filtered.groupby("series_id"):
        ordered = group.sort_values(["observation_date", "realtime_start"])
        pairs: list[tuple[str, str, float]] = []
        for observation_date, realtime_start, raw_value in zip(
            ordered["observation_date"].tolist(),
            ordered["realtime_start"].tolist(),
            ordered["value"].tolist(),
            strict=True,
        ):
            observation = _to_iso_date(observation_date)
            realtime = _to_iso_date(realtime_start)
            numeric = _to_float(raw_value)
            if observation is None or realtime is None or numeric is None:
                continue
            pairs.append((observation, realtime, numeric))
        if pairs:
            histories[str(series_id)] = pairs
    return histories


def _compute_level_frame(
    pd: Any,
    sorted_dates: list[str],
    histories: dict[str, list[tuple[str, str, float]]],
) -> pd.DataFrame:
    """Return a date-indexed level frame, one column per subscribed series_id."""
    data: dict[str, list[float | None]] = {"date": list(sorted_dates)}
    for feature_name, series_id in SERIES_ID_BY_FEATURE.items():
        history = histories.get(series_id, [])
        column: list[float | None] = []
        for target in sorted_dates:
            column.append(_latest_available_value(history, target))
        data[feature_name] = column
    return pd.DataFrame(data)


def _latest_available_value(
    history: list[tuple[str, str, float]],
    target_date: str,
) -> float | None:
    """Return the latest observation known strictly before the target date."""
    best_observation: str | None = None
    best_realtime: str | None = None
    best_value: float | None = None

    for observation_date, realtime_start, value in history:
        if observation_date >= target_date:
            break
        if realtime_start >= target_date:
            continue
        if (
            best_observation is None
            or observation_date > best_observation
            or (observation_date == best_observation and realtime_start > (best_realtime or ""))
        ):
            best_observation = observation_date
            best_realtime = realtime_start
            best_value = value

    return best_value


def _compose_features(pd: Any, levels: pd.DataFrame) -> pd.DataFrame:
    """Derive rate-change and spread features from the per-date level frame."""
    features = pd.DataFrame({"date": levels["date"]})
    features["fed_funds_rate"] = levels["fed_funds_rate"]
    features["fed_funds_rate_change_5d"] = _change_over_lookback(levels["fed_funds_rate"])
    features["treasury_3m"] = levels["treasury_3m"]
    features["treasury_2y"] = levels["treasury_2y"]
    features["treasury_10y"] = levels["treasury_10y"]
    features["treasury_10y_change_5d"] = _change_over_lookback(levels["treasury_10y"])
    features["yield_curve_slope_10y_2y"] = _difference(
        levels["treasury_10y"], levels["treasury_2y"]
    )
    features["yield_curve_slope_10y_3m"] = _difference(
        levels["treasury_10y"], levels["treasury_3m"]
    )
    features["vix_level"] = levels["vix_level"]
    features["vix_change_5d"] = _change_over_lookback(levels["vix_level"])
    features["dollar_index"] = levels["dollar_index"]
    features["dollar_index_change_5d"] = _change_over_lookback(levels["dollar_index"])
    features["cpi_level"] = levels["cpi_level"]
    features["cpi_change_yoy"] = _percent_change_over_lookback(
        levels["cpi_level"], lookback=252
    )
    features["high_yield_spread"] = levels["high_yield_spread"]
    return features[["date", *MACRO_FEATURE_COLUMNS]]


def _change_over_lookback(series: pd.Series) -> pd.Series:
    """Return `series(t) - series(t - CHANGE_5D_LOOKBACK_BDAYS)`."""
    return series - series.shift(CHANGE_5D_LOOKBACK_BDAYS)


def _percent_change_over_lookback(series: pd.Series, *, lookback: int) -> pd.Series:
    """Return `(series(t) - series(t - lookback)) / series(t - lookback)`."""
    prior = series.shift(lookback)
    ratio = (series - prior) / prior.where(prior != 0)
    return ratio


def _difference(left: pd.Series, right: pd.Series) -> pd.Series:
    """Return the elementwise difference of two series (NaN when either side is NaN)."""
    return left - right


def _validate_columns(macro: pd.DataFrame) -> None:
    """Raise when the FRED archive frame is missing a required column."""
    missing = sorted(REQUIRED_MACRO_COLUMNS - set(macro.columns))
    if missing:
        raise ValueError(f"Macro frame missing required columns: {missing}")


def _empty_frame(pd: Any) -> pd.DataFrame:
    """Return an empty macro feature frame with canonical columns."""
    return pd.DataFrame(columns=["date", *MACRO_FEATURE_COLUMNS])


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
    return numeric


def _to_float(value: Any) -> float | None:
    """Coerce a scalar to a finite float or return None."""
    if value is None:
        return None
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(numeric) or math.isinf(numeric):
        return None
    return numeric


def _to_iso_date(value: Any) -> str | None:
    """Coerce a date-like scalar to YYYY-MM-DD or return None."""
    if value is None:
        return None
    text = str(value).strip()
    if not text or text.lower() == "nat":
        return None
    candidate = text.split("T", maxsplit=1)[0].split(" ", maxsplit=1)[0]
    try:
        return Date.fromisoformat(candidate).isoformat()
    except ValueError:
        return None


def _require_pandas() -> Any:
    """Import pandas/pyarrow lazily with a clear error when absent."""
    try:
        import pandas as pd

        importlib.import_module("pyarrow")
    except ModuleNotFoundError as exc:
        raise ModuleNotFoundError(
            "pandas and pyarrow are required for macro feature computation."
        ) from exc
    return pd
