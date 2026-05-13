"""Point-in-time-safe raw-vs-stored spot checks for deterministic market features."""
from __future__ import annotations

import math
import statistics
from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from typing import TYPE_CHECKING, Literal

from core.contracts.schemas import FeatureRecord
from core.features.loaders import load_ohlcv_frame
from services.r2.writer import R2Writer

if TYPE_CHECKING:
    import pandas as pd

SpotCheckStatus = Literal["pass", "warn", "fail"]
MARKET_SPOTCHECK_FEATURES: tuple[str, ...] = (
    "returns_1d",
    "returns_5d",
    "realized_vol_21d",
    "volume_ratio_20",
    "rsi_14",
)
DEFAULT_ABSOLUTE_TOLERANCE = 1e-9
DEFAULT_RELATIVE_TOLERANCE = 1e-7
_TRADING_DAYS_PER_YEAR = 252


@dataclass(frozen=True)
class MarketFeatureSpotCheckRecord:
    """One raw-vs-stored comparison for a deterministic market feature."""

    row_key: str
    date: str
    ticker: str
    feature_name: str
    status: SpotCheckStatus
    point_in_time_safe: bool
    source_window_start: str | None
    source_window_end: str | None
    source_bar_count: int
    stored_value: float | int | str | bool | None
    expected_value: float | None
    absolute_difference: float | None
    relative_difference: float | None
    tolerance_absolute: float
    tolerance_relative: float
    raw_inputs: dict[str, object]
    message: str
    missing_reason: str | None = None

    def to_dict(self) -> dict[str, object]:
        """Return a JSON-serializable representation."""
        return asdict(self)


@dataclass(frozen=True)
class MarketFeatureFormulaAuditCard:
    """Human-readable formula payload for one deterministic feature spot check."""

    row_key: str
    date: str
    ticker: str
    feature_name: str
    status: SpotCheckStatus
    title: str
    formula: str
    calculation: str
    point_in_time_note: str
    expected_value: float | None
    stored_value: float | int | str | bool | None
    raw_inputs: dict[str, object]
    message: str
    missing_reason: str | None = None

    def to_dict(self) -> dict[str, object]:
        """Return a JSON-serializable representation."""
        return asdict(self)


@dataclass(frozen=True)
class _RecomputedFeature:
    """Internal recomputation payload before stored-value comparison."""

    feature_name: str
    expected_value: float | None
    raw_inputs: dict[str, object]
    formula: str
    calculation: str
    point_in_time_safe: bool
    point_in_time_note: str
    source_window_start: str | None
    source_window_end: str | None
    source_bar_count: int
    missing_reason: str | None = None


def build_market_feature_spot_checks(
    *,
    records: Sequence[FeatureRecord],
    writer: R2Writer | None = None,
    feature_names: Sequence[str] = MARKET_SPOTCHECK_FEATURES,
    absolute_tolerance: float = DEFAULT_ABSOLUTE_TOLERANCE,
    relative_tolerance: float = DEFAULT_RELATIVE_TOLERANCE,
) -> tuple[list[MarketFeatureSpotCheckRecord], list[MarketFeatureFormulaAuditCard]]:
    """Build raw-vs-stored spot checks and formula cards for selected Layer 1 rows."""
    active_writer = writer or R2Writer()
    supported_features = tuple(_normalize_feature_names(feature_names))
    checks: list[MarketFeatureSpotCheckRecord] = []
    cards: list[MarketFeatureFormulaAuditCard] = []

    by_ticker: dict[str, list[FeatureRecord]] = defaultdict(list)
    for record in records:
        by_ticker[record.ticker].append(record)

    for ticker in sorted(by_ticker):
        ticker_records = sorted(by_ticker[ticker], key=lambda item: item.date)
        try:
            bars = _prepare_bars(load_ohlcv_frame(ticker, writer=active_writer))
        except FileNotFoundError:
            for record in ticker_records:
                for feature_name in supported_features:
                    check, card = _missing_source_artifact(
                        record=record,
                        feature_name=feature_name,
                        absolute_tolerance=absolute_tolerance,
                        relative_tolerance=relative_tolerance,
                        reason="Raw Layer 0 OHLCV archive is missing for this ticker.",
                    )
                    checks.append(check)
                    cards.append(card)
            continue
        except ValueError as exc:
            reason = f"Raw Layer 0 OHLCV archive is invalid for this ticker: {exc}"
            for record in ticker_records:
                for feature_name in supported_features:
                    check, card = _missing_source_artifact(
                        record=record,
                        feature_name=feature_name,
                        absolute_tolerance=absolute_tolerance,
                        relative_tolerance=relative_tolerance,
                        reason=reason,
                    )
                    checks.append(check)
                    cards.append(card)
            continue

        for record in ticker_records:
            for feature_name in supported_features:
                recomputed = _recompute_feature(
                    bars=bars,
                    feature_date=record.date,
                    feature_name=feature_name,
                )
                check, card = _build_spot_check_outputs(
                    record=record,
                    recomputed=recomputed,
                    absolute_tolerance=absolute_tolerance,
                    relative_tolerance=relative_tolerance,
                )
                checks.append(check)
                cards.append(card)

    return checks, cards


def summarize_market_feature_spot_checks(
    checks: Sequence[MarketFeatureSpotCheckRecord],
) -> dict[str, int]:
    """Return PASS/WARN/FAIL counts for the supplied spot-check records."""
    counts = {"pass": 0, "warn": 0, "fail": 0}
    for check in checks:
        counts[check.status] += 1
    return counts


def _normalize_feature_names(feature_names: Sequence[str]) -> list[str]:
    unique: list[str] = []
    seen: set[str] = set()
    for feature_name in feature_names:
        normalized = str(feature_name).strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        unique.append(normalized)
    return unique


def _prepare_bars(bars: pd.DataFrame) -> pd.DataFrame:
    if "date" not in bars.columns:
        raise ValueError("missing required columns: date")
    frame = bars.copy()
    frame["date"] = frame["date"].astype(str)
    return frame.sort_values("date").drop_duplicates("date").reset_index(drop=True)


def _missing_source_artifact(
    *,
    record: FeatureRecord,
    feature_name: str,
    absolute_tolerance: float,
    relative_tolerance: float,
    reason: str,
) -> tuple[MarketFeatureSpotCheckRecord, MarketFeatureFormulaAuditCard]:
    row_key = _row_key(record)
    formula = _formula_template(feature_name)
    note = f"Spot check skipped for {record.date}; {reason}"
    check = MarketFeatureSpotCheckRecord(
        row_key=row_key,
        date=record.date,
        ticker=record.ticker,
        feature_name=feature_name,
        status="warn",
        point_in_time_safe=True,
        source_window_start=None,
        source_window_end=None,
        source_bar_count=0,
        stored_value=record.features.get(feature_name),
        expected_value=None,
        absolute_difference=None,
        relative_difference=None,
        tolerance_absolute=absolute_tolerance,
        tolerance_relative=relative_tolerance,
        raw_inputs={},
        message=reason,
        missing_reason=reason,
    )
    card = MarketFeatureFormulaAuditCard(
        row_key=row_key,
        date=record.date,
        ticker=record.ticker,
        feature_name=feature_name,
        status="warn",
        title=f"{record.ticker} {feature_name} on {record.date}",
        formula=formula,
        calculation=reason,
        point_in_time_note=note,
        expected_value=None,
        stored_value=record.features.get(feature_name),
        raw_inputs={},
        message=reason,
        missing_reason=reason,
    )
    return check, card


def _build_spot_check_outputs(
    *,
    record: FeatureRecord,
    recomputed: _RecomputedFeature,
    absolute_tolerance: float,
    relative_tolerance: float,
) -> tuple[MarketFeatureSpotCheckRecord, MarketFeatureFormulaAuditCard]:
    row_key = _row_key(record)
    stored_value = record.features.get(recomputed.feature_name)
    stored_numeric = _to_float_or_none(stored_value)

    if recomputed.missing_reason is not None:
        status: SpotCheckStatus = "warn"
        message = recomputed.missing_reason
        absolute_difference = None
        relative_difference = None
    elif stored_value is None:
        status = "fail"
        message = "Stored Layer 1 value is missing for a recomputable market feature."
        absolute_difference = None
        relative_difference = None
    elif stored_numeric is None:
        status = "fail"
        message = "Stored Layer 1 value is not numeric for a recomputable market feature."
        absolute_difference = None
        relative_difference = None
    else:
        expected_value = recomputed.expected_value
        if expected_value is None:
            raise ValueError("expected_value must be set when missing_reason is absent")
        absolute_difference = abs(stored_numeric - expected_value)
        denominator = max(abs(expected_value), absolute_tolerance)
        relative_difference = absolute_difference / denominator
        matches = (
            absolute_difference <= absolute_tolerance
            or relative_difference <= relative_tolerance
        )
        status = "pass" if matches else "fail"
        message = (
            "Stored Layer 1 value matches recomputation within tolerance."
            if matches
            else "Stored Layer 1 value differs from recomputation beyond tolerance."
        )

    check = MarketFeatureSpotCheckRecord(
        row_key=row_key,
        date=record.date,
        ticker=record.ticker,
        feature_name=recomputed.feature_name,
        status=status,
        point_in_time_safe=recomputed.point_in_time_safe,
        source_window_start=recomputed.source_window_start,
        source_window_end=recomputed.source_window_end,
        source_bar_count=recomputed.source_bar_count,
        stored_value=stored_value,
        expected_value=recomputed.expected_value,
        absolute_difference=absolute_difference,
        relative_difference=relative_difference,
        tolerance_absolute=absolute_tolerance,
        tolerance_relative=relative_tolerance,
        raw_inputs=recomputed.raw_inputs,
        message=message,
        missing_reason=recomputed.missing_reason,
    )
    card = MarketFeatureFormulaAuditCard(
        row_key=row_key,
        date=record.date,
        ticker=record.ticker,
        feature_name=recomputed.feature_name,
        status=status,
        title=f"{record.ticker} {recomputed.feature_name} on {record.date}",
        formula=recomputed.formula,
        calculation=recomputed.calculation,
        point_in_time_note=recomputed.point_in_time_note,
        expected_value=recomputed.expected_value,
        stored_value=stored_value,
        raw_inputs=recomputed.raw_inputs,
        message=message,
        missing_reason=recomputed.missing_reason,
    )
    return check, card


def _recompute_feature(
    *,
    bars: pd.DataFrame,
    feature_date: str,
    feature_name: str,
) -> _RecomputedFeature:
    formula = _formula_template(feature_name)
    missing_columns = sorted(_required_columns_for_feature(feature_name) - set(bars.columns))
    if missing_columns:
        return _missing_recompute(
            feature_name=feature_name,
            feature_date=feature_date,
            formula=formula,
            reason=(
                "Raw Layer 0 OHLCV archive is missing required columns for "
                f"{feature_name}: {', '.join(missing_columns)}."
            ),
            raw_inputs={
                "feature_date": feature_date,
                "available_columns": [str(column) for column in bars.columns],
            },
        )
    if feature_name == "returns_1d":
        return _recompute_returns(bars=bars, feature_date=feature_date, periods=1)
    if feature_name == "returns_5d":
        return _recompute_returns(bars=bars, feature_date=feature_date, periods=5)
    if feature_name == "realized_vol_21d":
        return _recompute_realized_vol_21d(bars=bars, feature_date=feature_date)
    if feature_name == "volume_ratio_20":
        return _recompute_volume_ratio_20(bars=bars, feature_date=feature_date)
    if feature_name == "rsi_14":
        return _recompute_rsi_14(bars=bars, feature_date=feature_date)
    raise ValueError(f"Unsupported market spot-check feature: {feature_name}")


def _recompute_returns(
    *,
    bars: pd.DataFrame,
    feature_date: str,
    periods: int,
) -> _RecomputedFeature:
    feature_name = f"returns_{periods}d"
    row_index = _feature_row_index(bars=bars, feature_date=feature_date)
    if row_index is None:
        return _missing_recompute(
            feature_name=feature_name,
            feature_date=feature_date,
            formula=_formula_template(feature_name),
            reason="Feature date is missing from the raw Layer 0 OHLCV archive.",
        )
    required_index = periods + 1
    if row_index < required_index:
        return _insufficient_history(
            bars=bars,
            feature_name=feature_name,
            feature_date=feature_date,
            formula=_formula_template(feature_name),
            available_bars=row_index,
            required_bars=required_index,
        )

    source_rows = bars.iloc[row_index - (periods + 1) : row_index]
    adj_close_values, invalid_recompute = _coerce_finite_values(
        source_rows,
        column="adj_close",
        feature_name=feature_name,
        feature_date=feature_date,
        formula=_formula_template(feature_name),
    )
    if invalid_recompute is not None:
        return invalid_recompute
    if adj_close_values is None:
        raise ValueError("adj_close_values must be set when invalid_recompute is absent")
    start_close = adj_close_values[0]
    end_close = adj_close_values[-1]
    expected_value = end_close / start_close - 1.0
    source_dates = source_rows["date"].tolist()
    raw_inputs = {
        "feature_date": feature_date,
        "window_dates": source_dates,
        "window_rows": _rows_to_dicts(source_rows, columns=("date", "adj_close")),
        "lookback_periods": periods,
    }
    calculation = (
        f"{feature_name}({feature_date}) = adj_close({source_dates[-1]}) / "
        f"adj_close({source_dates[0]}) - 1 = {_format_number(end_close)} / "
        f"{_format_number(start_close)} - 1 = {_format_number(expected_value)}"
    )
    return _complete_recompute(
        feature_name=feature_name,
        feature_date=feature_date,
        expected_value=expected_value,
        raw_inputs=raw_inputs,
        formula=_formula_template(feature_name),
        calculation=calculation,
        source_dates=source_dates,
    )


def _recompute_realized_vol_21d(
    *,
    bars: pd.DataFrame,
    feature_date: str,
) -> _RecomputedFeature:
    feature_name = "realized_vol_21d"
    row_index = _feature_row_index(bars=bars, feature_date=feature_date)
    if row_index is None:
        return _missing_recompute(
            feature_name=feature_name,
            feature_date=feature_date,
            formula=_formula_template(feature_name),
            reason="Feature date is missing from the raw Layer 0 OHLCV archive.",
        )
    if row_index < 22:
        return _insufficient_history(
            bars=bars,
            feature_name=feature_name,
            feature_date=feature_date,
            formula=_formula_template(feature_name),
            available_bars=row_index,
            required_bars=22,
        )

    source_rows = bars.iloc[row_index - 22 : row_index]
    adj_close, invalid_recompute = _coerce_finite_values(
        source_rows,
        column="adj_close",
        feature_name=feature_name,
        feature_date=feature_date,
        formula=_formula_template(feature_name),
    )
    if invalid_recompute is not None:
        return invalid_recompute
    if adj_close is None:
        raise ValueError("adj_close must be set when invalid_recompute is absent")
    daily_returns = [
        adj_close[index] / adj_close[index - 1] - 1.0 for index in range(1, len(adj_close))
    ]
    expected_value = statistics.stdev(daily_returns) * math.sqrt(_TRADING_DAYS_PER_YEAR)
    source_dates = source_rows["date"].tolist()
    raw_inputs = {
        "feature_date": feature_date,
        "window_dates": source_dates,
        "window_rows": _rows_to_dicts(source_rows, columns=("date", "adj_close")),
        "daily_returns": [_round_float(value) for value in daily_returns],
        "annualization_factor": _TRADING_DAYS_PER_YEAR,
    }
    calculation = (
        f"{feature_name}({feature_date}) = sqrt(252) * stdev("
        f"[{', '.join(_format_number(value) for value in daily_returns)}]) = "
        f"{_format_number(expected_value)}"
    )
    return _complete_recompute(
        feature_name=feature_name,
        feature_date=feature_date,
        expected_value=expected_value,
        raw_inputs=raw_inputs,
        formula=_formula_template(feature_name),
        calculation=calculation,
        source_dates=source_dates,
    )


def _recompute_volume_ratio_20(
    *,
    bars: pd.DataFrame,
    feature_date: str,
) -> _RecomputedFeature:
    feature_name = "volume_ratio_20"
    row_index = _feature_row_index(bars=bars, feature_date=feature_date)
    if row_index is None:
        return _missing_recompute(
            feature_name=feature_name,
            feature_date=feature_date,
            formula=_formula_template(feature_name),
            reason="Feature date is missing from the raw Layer 0 OHLCV archive.",
        )
    if row_index < 20:
        return _insufficient_history(
            bars=bars,
            feature_name=feature_name,
            feature_date=feature_date,
            formula=_formula_template(feature_name),
            available_bars=row_index,
            required_bars=20,
        )

    source_rows = bars.iloc[row_index - 20 : row_index]
    volumes, invalid_recompute = _coerce_finite_values(
        source_rows,
        column="volume",
        feature_name=feature_name,
        feature_date=feature_date,
        formula=_formula_template(feature_name),
    )
    if invalid_recompute is not None:
        return invalid_recompute
    if volumes is None:
        raise ValueError("volumes must be set when invalid_recompute is absent")
    current_volume = volumes[-1]
    mean_volume = statistics.fmean(volumes)
    source_dates = source_rows["date"].tolist()
    raw_inputs = {
        "feature_date": feature_date,
        "window_dates": source_dates,
        "window_rows": _rows_to_dicts(source_rows, columns=("date", "volume")),
        "mean_volume_20": _round_float(mean_volume),
    }
    if mean_volume == 0.0:
        return _missing_recompute(
            feature_name=feature_name,
            feature_date=feature_date,
            formula=_formula_template(feature_name),
            reason="Volume ratio is undefined because the 20-bar mean volume is zero.",
            raw_inputs=raw_inputs,
        )
    expected_value = current_volume / mean_volume
    calculation = (
        f"{feature_name}({feature_date}) = volume({source_dates[-1]}) / "
        f"mean(volume[{source_dates[0]}..{source_dates[-1]}]) = "
        f"{_format_number(current_volume)} / {_format_number(mean_volume)} = "
        f"{_format_number(expected_value)}"
    )
    return _complete_recompute(
        feature_name=feature_name,
        feature_date=feature_date,
        expected_value=expected_value,
        raw_inputs=raw_inputs,
        formula=_formula_template(feature_name),
        calculation=calculation,
        source_dates=source_dates,
    )


def _recompute_rsi_14(
    *,
    bars: pd.DataFrame,
    feature_date: str,
) -> _RecomputedFeature:
    feature_name = "rsi_14"
    row_index = _feature_row_index(bars=bars, feature_date=feature_date)
    if row_index is None:
        return _missing_recompute(
            feature_name=feature_name,
            feature_date=feature_date,
            formula=_formula_template(feature_name),
            reason="Feature date is missing from the raw Layer 0 OHLCV archive.",
        )
    if row_index < 15:
        return _insufficient_history(
            bars=bars,
            feature_name=feature_name,
            feature_date=feature_date,
            formula=_formula_template(feature_name),
            available_bars=row_index,
            required_bars=15,
        )

    source_rows = bars.iloc[row_index - 15 : row_index]
    closes, invalid_recompute = _coerce_finite_values(
        source_rows,
        column="adj_close",
        feature_name=feature_name,
        feature_date=feature_date,
        formula=_formula_template(feature_name),
    )
    if invalid_recompute is not None:
        return invalid_recompute
    if closes is None:
        raise ValueError("closes must be set when invalid_recompute is absent")
    deltas = [closes[index] - closes[index - 1] for index in range(1, len(closes))]
    gains = [max(delta, 0.0) for delta in deltas]
    losses = [max(-delta, 0.0) for delta in deltas]
    avg_gain = statistics.fmean(gains)
    avg_loss = statistics.fmean(losses)
    if avg_loss == 0.0 and avg_gain == 0.0:
        return _missing_recompute(
            feature_name=feature_name,
            feature_date=feature_date,
            formula=_formula_template(feature_name),
            reason="RSI is undefined because both average gain and average loss are zero.",
            raw_inputs={
                "feature_date": feature_date,
                "window_dates": source_rows["date"].tolist(),
                "window_rows": _rows_to_dicts(source_rows, columns=("date", "adj_close")),
                "deltas": [_round_float(value) for value in deltas],
                "gains": [_round_float(value) for value in gains],
                "losses": [_round_float(value) for value in losses],
            },
        )
    if avg_loss == 0.0:
        expected_value = 100.0
        rs_text = "infinity"
    else:
        rs = avg_gain / avg_loss
        expected_value = 100.0 - 100.0 / (1.0 + rs)
        rs_text = _format_number(rs)

    source_dates = source_rows["date"].tolist()
    raw_inputs = {
        "feature_date": feature_date,
        "window_dates": source_dates,
        "window_rows": _rows_to_dicts(source_rows, columns=("date", "adj_close")),
        "deltas": [_round_float(value) for value in deltas],
        "gains": [_round_float(value) for value in gains],
        "losses": [_round_float(value) for value in losses],
        "average_gain_14": _round_float(avg_gain),
        "average_loss_14": _round_float(avg_loss),
    }
    calculation = (
        f"{feature_name}({feature_date}) = 100 - 100 / (1 + "
        f"avg_gain_14 / avg_loss_14) = 100 - 100 / (1 + {rs_text}) = "
        f"{_format_number(expected_value)}"
    )
    return _complete_recompute(
        feature_name=feature_name,
        feature_date=feature_date,
        expected_value=expected_value,
        raw_inputs=raw_inputs,
        formula=_formula_template(feature_name),
        calculation=calculation,
        source_dates=source_dates,
    )


def _feature_row_index(*, bars: pd.DataFrame, feature_date: str) -> int | None:
    matches = bars.index[bars["date"] == feature_date].tolist()
    if not matches:
        return None
    return int(matches[0])


def _required_columns_for_feature(feature_name: str) -> frozenset[str]:
    columns_by_feature: Mapping[str, frozenset[str]] = {
        "returns_1d": frozenset({"adj_close"}),
        "returns_5d": frozenset({"adj_close"}),
        "realized_vol_21d": frozenset({"adj_close"}),
        "volume_ratio_20": frozenset({"volume"}),
        "rsi_14": frozenset({"adj_close"}),
    }
    return columns_by_feature.get(feature_name, frozenset())


def _coerce_finite_values(
    source_rows: pd.DataFrame,
    *,
    column: str,
    feature_name: str,
    feature_date: str,
    formula: str,
) -> tuple[list[float] | None, _RecomputedFeature | None]:
    values: list[float] = []
    invalid_rows: list[dict[str, object]] = []
    for row in source_rows.loc[:, ["date", column]].to_dict(orient="records"):
        try:
            numeric_value = float(row[column])
        except (TypeError, ValueError):
            invalid_rows.append({"date": str(row["date"]), column: row[column]})
            continue
        if math.isnan(numeric_value) or math.isinf(numeric_value):
            invalid_rows.append({"date": str(row["date"]), column: row[column]})
            continue
        values.append(numeric_value)
    if invalid_rows:
        return None, _missing_recompute(
            feature_name=feature_name,
            feature_date=feature_date,
            formula=formula,
            reason=(
                f"Raw Layer 0 OHLCV archive has non-finite {column} values inside the "
                "source window."
            ),
            raw_inputs={
                "feature_date": feature_date,
                "window_dates": [str(value) for value in source_rows["date"].tolist()],
                "window_rows": _rows_to_dicts(source_rows, columns=("date", column)),
                "invalid_rows": invalid_rows,
            },
        )
    return values, None


def _missing_recompute(
    *,
    feature_name: str,
    feature_date: str,
    formula: str,
    reason: str,
    raw_inputs: dict[str, object] | None = None,
) -> _RecomputedFeature:
    note = f"Spot check skipped for {feature_date}; {reason}"
    return _RecomputedFeature(
        feature_name=feature_name,
        expected_value=None,
        raw_inputs={} if raw_inputs is None else raw_inputs,
        formula=formula,
        calculation=reason,
        point_in_time_safe=True,
        point_in_time_note=note,
        source_window_start=None,
        source_window_end=None,
        source_bar_count=0,
        missing_reason=reason,
    )


def _insufficient_history(
    *,
    bars: pd.DataFrame,
    feature_name: str,
    feature_date: str,
    formula: str,
    available_bars: int,
    required_bars: int,
) -> _RecomputedFeature:
    available_rows = bars.iloc[:available_bars]
    return _missing_recompute(
        feature_name=feature_name,
        feature_date=feature_date,
        formula=formula,
        reason=(
            "Insufficient prior OHLCV history for deterministic recomputation: "
            f"requires {required_bars} prior bars, found {available_bars}."
        ),
        raw_inputs={
            "feature_date": feature_date,
            "available_window_dates": available_rows["date"].tolist(),
            "available_window_rows": _rows_to_dicts(
                available_rows,
                columns=("date", "adj_close", "volume"),
            ),
            "required_prior_bars": required_bars,
            "available_prior_bars": available_bars,
        },
    )


def _complete_recompute(
    *,
    feature_name: str,
    feature_date: str,
    expected_value: float,
    raw_inputs: dict[str, object],
    formula: str,
    calculation: str,
    source_dates: Sequence[str],
) -> _RecomputedFeature:
    latest_source_date = source_dates[-1] if source_dates else None
    point_in_time_safe = all(source_date < feature_date for source_date in source_dates)
    note = (
        f"Only source rows strictly before {feature_date} were used; latest source date was "
        f"{latest_source_date}."
        if latest_source_date is not None
        else f"No source rows were used for {feature_date}."
    )
    return _RecomputedFeature(
        feature_name=feature_name,
        expected_value=expected_value,
        raw_inputs=raw_inputs,
        formula=formula,
        calculation=calculation,
        point_in_time_safe=point_in_time_safe,
        point_in_time_note=note,
        source_window_start=source_dates[0] if source_dates else None,
        source_window_end=latest_source_date,
        source_bar_count=len(source_dates),
    )


def _rows_to_dicts(frame: pd.DataFrame, *, columns: Sequence[str]) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for row in frame.loc[:, list(columns)].to_dict(orient="records"):
        normalized: dict[str, object] = {}
        for key, value in row.items():
            if key == "date":
                normalized[key] = str(value)
            elif value is None:
                normalized[key] = None
            elif isinstance(value, bool):
                normalized[key] = value
            elif isinstance(value, int):
                normalized[key] = value
            else:
                try:
                    numeric_value = float(value)
                except (TypeError, ValueError):
                    normalized[key] = value
                    continue
                if math.isnan(numeric_value) or math.isinf(numeric_value):
                    normalized[key] = None
                    continue
                normalized[key] = _round_float(numeric_value)
        rows.append(normalized)
    return rows


def _row_key(record: FeatureRecord) -> str:
    return f"{record.date}|{record.ticker}"


def _to_float_or_none(value: float | int | str | bool | None) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(numeric) or math.isinf(numeric):
        return None
    return numeric


def _formula_template(feature_name: str) -> str:
    formulas: Mapping[str, str] = {
        "returns_1d": "adj_close(t-1) / adj_close(t-2) - 1",
        "returns_5d": "adj_close(t-1) / adj_close(t-6) - 1",
        "realized_vol_21d": "sqrt(252) * stdev(returns_1d[t-21..t-1])",
        "volume_ratio_20": "volume(t-1) / mean(volume[t-20..t-1])",
        "rsi_14": "100 - 100 / (1 + avg_gain_14 / avg_loss_14)",
    }
    return formulas.get(feature_name, feature_name)


def _format_number(value: float) -> str:
    return f"{value:.6f}"


def _round_float(value: float) -> float:
    return float(f"{value:.10f}")
