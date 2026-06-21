"""Regular US equity trading-session calendar helpers."""
from __future__ import annotations

from datetime import date as Date
from datetime import timedelta


def calendar_dates(from_date: str, to_date: str) -> tuple[str, ...]:
    """Return calendar dates between two ISO dates, inclusive."""
    start = Date.fromisoformat(from_date)
    end = Date.fromisoformat(to_date)
    dates: list[str] = []
    current = start
    while current <= end:
        dates.append(current.isoformat())
        current += timedelta(days=1)
    return tuple(dates)


def trading_dates(from_date: str, to_date: str) -> tuple[str, ...]:
    """Return regular US equity trading sessions between two ISO dates, inclusive."""
    return tuple(
        date_text
        for date_text in calendar_dates(from_date, to_date)
        if is_us_equity_trading_session(Date.fromisoformat(date_text))
    )


def skipped_non_trading_dates(from_date: str, to_date: str) -> tuple[str, ...]:
    """Return requested dates that are not regular US equity trading sessions."""
    processed_dates = set(trading_dates(from_date, to_date))
    return tuple(
        date_text
        for date_text in calendar_dates(from_date, to_date)
        if date_text not in processed_dates
    )


def previous_trading_day(date_text: str) -> str:
    """Return the prior regular US equity trading session for one ISO date."""
    current = Date.fromisoformat(date_text) - timedelta(days=1)
    while not is_us_equity_trading_session(current):
        current -= timedelta(days=1)
    return current.isoformat()


def subtract_trading_days(date_text: str, count: int) -> str:
    """Return the ISO date that is `count` trading sessions before `date_text`."""
    if count < 0:
        raise ValueError("count must be non-negative")
    current = Date.fromisoformat(date_text)
    remaining = count
    while remaining > 0:
        current -= timedelta(days=1)
        if is_us_equity_trading_session(current):
            remaining -= 1
    return current.isoformat()


def is_us_equity_trading_session(day: Date) -> bool:
    """Return True when the date is a regular full US equity market session."""
    if day.weekday() >= 5:
        return False
    return day not in us_equity_market_holidays(day.year)


def us_equity_market_holidays(year: int) -> set[Date]:
    """Return regular full-day US equity market holidays for one year."""
    holidays = {
        _observed_fixed_holiday(year, 1, 1),
        _nth_weekday(year, 1, 0, 3),
        _nth_weekday(year, 2, 0, 3),
        _easter_sunday(year) - timedelta(days=2),
        _last_weekday(year, 5, 0),
        _observed_fixed_holiday(year, 7, 4),
        _nth_weekday(year, 9, 0, 1),
        _nth_weekday(year, 11, 3, 4),
        _observed_fixed_holiday(year, 12, 25),
    }
    if year >= 2022:
        holidays.add(_observed_fixed_holiday(year, 6, 19))
    next_new_year_observed = _observed_fixed_holiday(year + 1, 1, 1)
    if next_new_year_observed.year == year:
        holidays.add(next_new_year_observed)
    return holidays


def _observed_fixed_holiday(year: int, month: int, day: int) -> Date:
    """Return the weekday-observed date for one fixed-date market holiday."""
    holiday = Date(year, month, day)
    if holiday.weekday() == 5:
        return holiday - timedelta(days=1)
    if holiday.weekday() == 6:
        return holiday + timedelta(days=1)
    return holiday


def _nth_weekday(year: int, month: int, weekday: int, n: int) -> Date:
    """Return the nth weekday in a month, where Monday is 0."""
    current = Date(year, month, 1)
    while current.weekday() != weekday:
        current += timedelta(days=1)
    return current + timedelta(days=7 * (n - 1))


def _last_weekday(year: int, month: int, weekday: int) -> Date:
    """Return the last weekday in a month, where Monday is 0."""
    if month == 12:
        current = Date(year + 1, 1, 1) - timedelta(days=1)
    else:
        current = Date(year, month + 1, 1) - timedelta(days=1)
    while current.weekday() != weekday:
        current -= timedelta(days=1)
    return current


def _easter_sunday(year: int) -> Date:
    """Return Gregorian Easter Sunday for one year."""
    a = year % 19
    b = year // 100
    c = year % 100
    d = b // 4
    e = b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i = c // 4
    k = c % 4
    correction = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * correction) // 451
    month = (h + correction - 7 * m + 114) // 31
    day = ((h + correction - 7 * m + 114) % 31) + 1
    return Date(year, month, day)
