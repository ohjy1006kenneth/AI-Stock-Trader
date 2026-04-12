from __future__ import annotations

from datetime import date, timedelta

import pytest

from core.contracts.schemas import OHLCVRecord, UniverseRecord
from core.data.quality import QualityFilterConfig, apply_quality_filters


def test_passing_ticker_keeps_default_flags() -> None:
    """A ticker that passes every filter keeps liquid and data quality flags."""
    universe = [_universe("PASS")]
    window = {"PASS": _bars("PASS", close=25.0, dollar_volume=2_000_000.0)}

    result = apply_quality_filters(universe, window, _config())

    assert result == [universe[0]]


def test_average_dollar_volume_below_minimum_sets_liquid_false() -> None:
    """A low 20-day average dollar volume fails the liquidity filter."""
    result = apply_quality_filters(
        [_universe("LOWADV")],
        {"LOWADV": _bars("LOWADV", close=25.0, dollar_volume=500_000.0)},
        _config(min_average_dollar_volume=1_000_000.0),
    )

    assert result[0].liquid is False
    assert result[0].data_quality_ok is True
    assert result[0].reason == "average_dollar_volume_below_minimum"


def test_close_below_minimum_sets_liquid_false() -> None:
    """A penny-stock close fails the liquidity filter."""
    result = apply_quality_filters(
        [_universe("PENNY")],
        {"PENNY": _bars("PENNY", close=4.99, dollar_volume=2_000_000.0)},
        _config(min_close_price=5.0),
    )

    assert result[0].liquid is False
    assert result[0].data_quality_ok is True
    assert result[0].reason == "close_price_below_minimum"


def test_zero_volume_sets_data_quality_false() -> None:
    """A latest zero-volume bar fails the data quality filter."""
    bars = _bars("ZEROVOL", close=25.0, dollar_volume=2_000_000.0)
    bars[-1] = _bar("ZEROVOL", "2025-01-10", close=25.0, volume=0, dollar_volume=0.0)

    result = apply_quality_filters([_universe("ZEROVOL")], {"ZEROVOL": bars}, _config())

    assert result[0].liquid is True
    assert result[0].data_quality_ok is False
    assert result[0].halted is False
    assert result[0].reason == "zero_volume"


def test_single_day_move_above_maximum_sets_data_quality_false() -> None:
    """A latest close-to-close move above the configured limit fails quality."""
    bars = _bars("GAPPER", close=25.0, dollar_volume=2_000_000.0)
    bars[-2] = _bar("GAPPER", "2025-01-09", close=20.0, dollar_volume=2_000_000.0)
    bars[-1] = _bar("GAPPER", "2025-01-10", close=29.0, dollar_volume=2_900_000.0)

    result = apply_quality_filters(
        [_universe("GAPPER")],
        {"GAPPER": bars},
        _config(max_single_day_move=0.40),
    )

    assert result[0].data_quality_ok is False
    assert result[0].halted is False
    assert result[0].reason == "single_day_move_above_maximum"


def test_consecutive_missing_bars_above_maximum_sets_data_quality_false() -> None:
    """More than N consecutive missing business-day bars fails quality."""
    bars = [
        _bar("MISS", "2025-01-06", close=25.0, dollar_volume=2_000_000.0),
        _bar("MISS", "2025-01-07", close=25.0, dollar_volume=2_000_000.0),
    ]

    result = apply_quality_filters(
        [_universe("MISS")],
        {"MISS": bars},
        _config(max_consecutive_missing_bars=2),
    )

    assert result[0].data_quality_ok is False
    assert result[0].liquid is True
    assert result[0].reason == "consecutive_missing_bars_above_maximum"


def test_zero_volume_and_large_gap_sets_halted() -> None:
    """A zero-volume latest bar with a large price gap is marked halted."""
    bars = _bars("HALT", close=25.0, dollar_volume=2_000_000.0)
    bars[-2] = _bar("HALT", "2025-01-09", close=20.0, dollar_volume=2_000_000.0)
    bars[-1] = _bar("HALT", "2025-01-10", close=29.0, volume=0, dollar_volume=0.0)

    result = apply_quality_filters(
        [_universe("HALT")],
        {"HALT": bars},
        _config(max_single_day_move=0.40),
    )

    assert result[0].data_quality_ok is False
    assert result[0].halted is True
    assert result[0].reason == "zero_volume; single_day_move_above_maximum; halt_detected"


def test_missing_ohlcv_window_sets_data_quality_false() -> None:
    """A universe ticker without a matching OHLCV window fails closed."""
    result = apply_quality_filters([_universe("NODATA")], {}, _config())

    assert result[0].data_quality_ok is False
    assert result[0].liquid is True
    assert result[0].reason == "missing_ohlcv_window; consecutive_missing_bars_above_maximum"


def test_existing_reason_is_preserved_and_deduplicated() -> None:
    """Existing reason text is preserved while duplicate new reasons collapse."""
    universe = [
        _universe(
            "LOWADV",
            reason="manual_review; average_dollar_volume_below_minimum",
        )
    ]
    result = apply_quality_filters(
        universe,
        {"LOWADV": _bars("LOWADV", close=25.0, dollar_volume=500_000.0)},
        _config(min_average_dollar_volume=1_000_000.0),
    )

    assert result[0].reason == "manual_review; average_dollar_volume_below_minimum"


@pytest.mark.parametrize(
    "updates",
    [
        {"rolling_window_days": 0},
        {"min_average_dollar_volume": -1.0},
        {"min_close_price": -1.0},
        {"max_single_day_move": -0.01},
        {"max_consecutive_missing_bars": -1},
    ],
)
def test_config_rejects_invalid_thresholds(updates: dict[str, float | int]) -> None:
    """Quality filter thresholds must be valid before filters run."""
    with pytest.raises(ValueError):
        _config(**updates)


def _config(**updates: float | int) -> QualityFilterConfig:
    """Return a compact test config."""
    values: dict[str, float | int] = {
        "rolling_window_days": 5,
        "min_average_dollar_volume": 1_000_000.0,
        "min_close_price": 5.0,
        "max_single_day_move": 0.40,
        "max_consecutive_missing_bars": 3,
    }
    values.update(updates)
    return QualityFilterConfig(**values)


def _universe(ticker: str, reason: str | None = None) -> UniverseRecord:
    """Return a baseline universe record for the test as-of date."""
    return UniverseRecord(
        date="2025-01-10",
        ticker=ticker,
        in_universe=True,
        tradable=True,
        liquid=True,
        halted=False,
        data_quality_ok=True,
        reason=reason,
    )


def _bars(
    ticker: str,
    *,
    close: float,
    dollar_volume: float,
) -> list[OHLCVRecord]:
    """Return five recent business-day bars ending on the test as-of date."""
    return [
        _bar(ticker, as_of_date, close=close, dollar_volume=dollar_volume)
        for as_of_date in _business_dates("2025-01-06", 5)
    ]


def _bar(
    ticker: str,
    as_of_date: str,
    *,
    close: float,
    volume: int = 100_000,
    dollar_volume: float,
) -> OHLCVRecord:
    """Return one valid OHLCV record."""
    return OHLCVRecord(
        date=as_of_date,
        ticker=ticker,
        open=close,
        high=close,
        low=close,
        close=close,
        volume=volume,
        adj_close=close,
        dollar_volume=dollar_volume,
    )


def _business_dates(start: str, count: int) -> list[str]:
    """Return business dates from start inclusive."""
    current = date.fromisoformat(start)
    dates: list[str] = []
    while len(dates) < count:
        if current.weekday() < 5:
            dates.append(current.isoformat())
        current += timedelta(days=1)
    return dates
