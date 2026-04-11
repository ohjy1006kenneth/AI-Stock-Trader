from __future__ import annotations

import math
from collections.abc import Mapping
from datetime import date as Date
from decimal import Decimal
from typing import Any

from core.contracts.schemas import OHLCVRecord

REQUIRED_OHLCV_FIELDS = (
    "date",
    "ticker",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "adj_close",
    "dollar_volume",
)


def build_ohlcv_record(row: Mapping[str, Any]) -> OHLCVRecord:
    """Build a validated OHLCVRecord from a vendor-neutral mapping."""
    _require_fields(row, REQUIRED_OHLCV_FIELDS)

    date = _coerce_date(row["date"])
    ticker = _coerce_ticker(row["ticker"])
    open_price = _coerce_positive_finite_float(row["open"], "open")
    high = _coerce_positive_finite_float(row["high"], "high")
    low = _coerce_positive_finite_float(row["low"], "low")
    close = _coerce_positive_finite_float(row["close"], "close")
    adj_close = _coerce_positive_finite_float(row["adj_close"], "adj_close")
    volume = _coerce_non_negative_int(row["volume"], "volume")
    dollar_volume = _coerce_non_negative_finite_float(row["dollar_volume"], "dollar_volume")

    _validate_ohlc_relationships(
        open_price=open_price,
        high=high,
        low=low,
        close=close,
    )

    return OHLCVRecord(
        date=date,
        ticker=ticker,
        open=open_price,
        high=high,
        low=low,
        close=close,
        volume=volume,
        adj_close=adj_close,
        dollar_volume=dollar_volume,
    )


def _require_fields(row: Mapping[str, Any], fields: tuple[str, ...]) -> None:
    """Require all fields to exist and be non-null."""
    missing = [field for field in fields if field not in row or row[field] is None]
    if missing:
        raise ValueError(f"Missing required OHLCV field(s): {', '.join(missing)}")


def _coerce_date(value: Any) -> str:
    """Coerce and validate a YYYY-MM-DD date string."""
    if not isinstance(value, str):
        raise TypeError("date must be a YYYY-MM-DD string")
    stripped = value.strip()
    if not _is_extended_iso_date(stripped):
        raise ValueError(f"date must be YYYY-MM-DD: {value}")
    try:
        return Date.fromisoformat(stripped).isoformat()
    except ValueError as exc:
        raise ValueError(f"date must be YYYY-MM-DD: {value}") from exc


def _coerce_ticker(value: Any) -> str:
    """Coerce ticker text to uppercase."""
    if not isinstance(value, str):
        raise TypeError("ticker must be a string")
    stripped = value.strip().upper()
    if not stripped:
        raise ValueError("ticker cannot be empty")
    return stripped


def _coerce_positive_finite_float(value: Any, field_name: str) -> float:
    """Coerce a strictly positive finite float."""
    number = _coerce_finite_float(value, field_name)
    if number <= 0.0:
        raise ValueError(f"{field_name} must be positive")
    return number


def _coerce_non_negative_finite_float(value: Any, field_name: str) -> float:
    """Coerce a non-negative finite float."""
    number = _coerce_finite_float(value, field_name)
    if number < 0.0:
        raise ValueError(f"{field_name} must be non-negative")
    return number


def _coerce_finite_float(value: Any, field_name: str) -> float:
    """Coerce a finite float."""
    if isinstance(value, bool) or not isinstance(value, (int, float, Decimal)):
        raise TypeError(f"{field_name} must be numeric")
    number = float(value)
    if not math.isfinite(number):
        raise ValueError(f"{field_name} must be finite")
    return number


def _coerce_non_negative_int(value: Any, field_name: str) -> int:
    """Coerce a non-negative integer."""
    if isinstance(value, bool) or not isinstance(value, (int, float, Decimal)):
        raise TypeError(f"{field_name} must be an integer")
    number = float(value)
    if not math.isfinite(number) or not number.is_integer():
        raise ValueError(f"{field_name} must be an integer")
    integer = int(number)
    if integer < 0:
        raise ValueError(f"{field_name} must be non-negative")
    return integer


def _validate_ohlc_relationships(
    *,
    open_price: float,
    high: float,
    low: float,
    close: float,
) -> None:
    """Validate OHLC price relationships."""
    if high < low:
        raise ValueError("high must be greater than or equal to low")
    if not low <= open_price <= high:
        raise ValueError("open must be inside the [low, high] range")
    if not low <= close <= high:
        raise ValueError("close must be inside the [low, high] range")


def _is_extended_iso_date(value: str) -> bool:
    """Return True for YYYY-MM-DD strings only."""
    return (
        len(value) == 10
        and value[4] == "-"
        and value[7] == "-"
        and value[:4].isdigit()
        and value[5:7].isdigit()
        and value[8:10].isdigit()
    )
