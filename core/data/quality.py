from __future__ import annotations

from bisect import bisect_right
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date as Date
from datetime import timedelta

from core.contracts.schemas import OHLCVRecord, UniverseRecord


@dataclass(frozen=True)
class QualityFilterConfig:
    """Configurable thresholds for Layer 0 liquidity and data quality filters."""

    rolling_window_days: int = 20
    min_average_dollar_volume: float = 1_000_000.0
    min_close_price: float = 5.0
    max_single_day_move: float = 0.40
    max_consecutive_missing_bars: int = 3

    def __post_init__(self) -> None:
        """Validate filter thresholds at construction time."""
        if self.rolling_window_days <= 0:
            raise ValueError("rolling_window_days must be positive")
        if self.min_average_dollar_volume < 0.0:
            raise ValueError("min_average_dollar_volume must be non-negative")
        if self.min_close_price < 0.0:
            raise ValueError("min_close_price must be non-negative")
        if self.max_single_day_move < 0.0:
            raise ValueError("max_single_day_move must be non-negative")
        if self.max_consecutive_missing_bars < 0:
            raise ValueError("max_consecutive_missing_bars must be non-negative")


def apply_quality_filters(
    universe: list[UniverseRecord],
    ohlcv_window: Mapping[str, Sequence[OHLCVRecord]],
    config: QualityFilterConfig,
) -> list[UniverseRecord]:
    """Apply Layer 0 liquidity and data quality filters to universe records."""
    quality_windows = prepare_quality_windows(ohlcv_window)
    return apply_prepared_quality_filters(universe, quality_windows, config)


def apply_prepared_quality_filters(
    universe: list[UniverseRecord],
    quality_windows: Mapping[str, _TickerQualityWindow],
    config: QualityFilterConfig,
) -> list[UniverseRecord]:
    """Apply quality filters using pre-indexed OHLCV bars."""
    return [_filter_record(record, quality_windows, config) for record in universe]


@dataclass(frozen=True)
class _TickerQualityWindow:
    """Pre-indexed OHLCV bars for one ticker."""

    bars: tuple[OHLCVRecord, ...]
    dates: tuple[str, ...]
    date_set: frozenset[str]


def _filter_record(
    record: UniverseRecord,
    ohlcv_window: Mapping[str, _TickerQualityWindow],
    config: QualityFilterConfig,
) -> UniverseRecord:
    """Return one universe record with quality flags updated."""
    ticker = record.ticker.upper()
    quality_window = ohlcv_window.get(ticker)
    cutoff = _bar_cutoff(quality_window, record.date)
    latest_bar = (
        quality_window.bars[cutoff - 1]
        if quality_window is not None and cutoff >= 1
        else None
    )
    previous_bar = (
        quality_window.bars[cutoff - 2]
        if quality_window is not None and cutoff >= 2
        else None
    )

    liquid = record.liquid
    data_quality_ok = record.data_quality_ok
    halted = record.halted
    reasons = _split_reasons(record.reason)

    if cutoff == 0:
        data_quality_ok = False
        reasons.append("missing_ohlcv_window")
    else:
        assert quality_window is not None
        average_dollar_volume = _average_dollar_volume(
            quality_window.bars[max(0, cutoff - config.rolling_window_days) : cutoff]
        )
        if average_dollar_volume < config.min_average_dollar_volume:
            liquid = False
            reasons.append("average_dollar_volume_below_minimum")

    if latest_bar is not None and latest_bar.close < config.min_close_price:
        liquid = False
        reasons.append("close_price_below_minimum")

    latest_has_zero_volume = latest_bar is not None and latest_bar.volume == 0
    if latest_has_zero_volume:
        data_quality_ok = False
        reasons.append("zero_volume")

    large_price_move = (
        latest_bar is not None
        and previous_bar is not None
        and _single_day_move(latest_bar, previous_bar) > config.max_single_day_move
    )
    if large_price_move:
        data_quality_ok = False
        reasons.append("single_day_move_above_maximum")

    if latest_has_zero_volume and large_price_move:
        halted = True
        reasons.append("halt_detected")

    missing_streak = _max_consecutive_missing_bars(
        as_of_date=record.date,
        bar_dates=quality_window.date_set if quality_window is not None else frozenset(),
        rolling_window_days=config.rolling_window_days,
    )
    if missing_streak > config.max_consecutive_missing_bars:
        data_quality_ok = False
        reasons.append("consecutive_missing_bars_above_maximum")

    return record.model_copy(
        update={
            "liquid": liquid,
            "data_quality_ok": data_quality_ok,
            "halted": halted,
            "reason": _join_reasons(reasons),
        }
    )


def prepare_quality_windows(
    ohlcv_window: Mapping[str, Sequence[OHLCVRecord]],
) -> dict[str, _TickerQualityWindow]:
    """Return sorted, deduplicated bars keyed by normalized ticker."""
    quality_windows: dict[str, _TickerQualityWindow] = {}
    for raw_ticker, bars in ohlcv_window.items():
        ticker = raw_ticker.upper()
        sorted_bars = tuple(_sorted_ticker_bars(ticker, bars))
        dates = tuple(bar.date for bar in sorted_bars)
        quality_windows[ticker] = _TickerQualityWindow(
            bars=sorted_bars,
            dates=dates,
            date_set=frozenset(dates),
        )
    return quality_windows


def _bar_cutoff(
    quality_window: _TickerQualityWindow | None,
    as_of_date: str,
) -> int:
    """Return the exclusive bar index at or before one date."""
    if quality_window is None:
        return 0
    return bisect_right(quality_window.dates, as_of_date)


def _sorted_ticker_bars(ticker: str, bars: Sequence[OHLCVRecord]) -> list[OHLCVRecord]:
    """Return bars for a ticker sorted by date with duplicate dates collapsed."""
    by_date: dict[str, OHLCVRecord] = {}
    for bar in bars:
        if bar.ticker.upper() == ticker:
            by_date[bar.date] = bar
    return [by_date[date] for date in sorted(by_date)]


def _average_dollar_volume(bars: Sequence[OHLCVRecord]) -> float:
    """Return average dollar volume for available bars."""
    if not bars:
        return 0.0
    return sum(bar.dollar_volume for bar in bars) / len(bars)


def _single_day_move(latest_bar: OHLCVRecord, previous_bar: OHLCVRecord) -> float:
    """Return absolute close-to-close move."""
    return abs(latest_bar.close / previous_bar.close - 1.0)


def _max_consecutive_missing_bars(
    *,
    as_of_date: str,
    bar_dates: set[str],
    rolling_window_days: int,
) -> int:
    """Return the maximum missing business-day streak in the rolling window."""
    expected_dates = _recent_business_dates(as_of_date, rolling_window_days)
    max_streak = 0
    current_streak = 0
    for expected_date in expected_dates:
        if expected_date in bar_dates:
            current_streak = 0
            continue
        current_streak += 1
        max_streak = max(max_streak, current_streak)
    return max_streak


def _recent_business_dates(as_of_date: str, count: int) -> list[str]:
    """Return the most recent business dates ending on or before as_of_date."""
    current = Date.fromisoformat(as_of_date)
    dates: list[str] = []
    while len(dates) < count:
        if current.weekday() < 5:
            dates.append(current.isoformat())
        current -= timedelta(days=1)
    return list(reversed(dates))


def _split_reasons(reason: str | None) -> list[str]:
    """Split an existing reason string into stable reason tokens."""
    if not reason:
        return []
    return [part.strip() for part in reason.split(";") if part.strip()]


def _join_reasons(reasons: Sequence[str]) -> str | None:
    """Join unique reason tokens in first-seen order."""
    unique_reasons = list(dict.fromkeys(reasons))
    return "; ".join(unique_reasons) if unique_reasons else None
