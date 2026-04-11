from __future__ import annotations

import math
from typing import Any

import pytest

from core.data.ohlcv import build_ohlcv_record


def _valid_row() -> dict[str, Any]:
    """Return a valid vendor-neutral OHLCV row."""
    return {
        "date": "2025-01-02",
        "ticker": " aapl ",
        "open": 100.0,
        "high": 105.0,
        "low": 99.0,
        "close": 104.0,
        "volume": 1234,
        "adj_close": 103.5,
        "dollar_volume": 128336.0,
    }


def test_build_ohlcv_record_happy_path() -> None:
    """A complete valid row becomes a schema-valid OHLCVRecord."""
    record = build_ohlcv_record(_valid_row())

    assert record.date == "2025-01-02"
    assert record.ticker == "AAPL"
    assert record.open == 100.0
    assert record.high == 105.0
    assert record.low == 99.0
    assert record.close == 104.0
    assert record.volume == 1234
    assert record.adj_close == 103.5
    assert record.dollar_volume == 128336.0


@pytest.mark.parametrize("field", ["date", "ticker"])
def test_build_ohlcv_record_rejects_missing_identity_fields(field: str) -> None:
    """Date and ticker are required identity fields."""
    row = _valid_row()
    row.pop(field)

    with pytest.raises(ValueError, match=field):
        build_ohlcv_record(row)


def test_build_ohlcv_record_rejects_invalid_date() -> None:
    """OHLCV dates must be ISO calendar dates."""
    row = _valid_row()
    row["date"] = "20250102"

    with pytest.raises(ValueError, match="YYYY-MM-DD"):
        build_ohlcv_record(row)


@pytest.mark.parametrize("field", ["open", "high", "low", "close", "adj_close"])
def test_build_ohlcv_record_rejects_missing_price_fields(field: str) -> None:
    """All required price fields must be present."""
    row = _valid_row()
    row.pop(field)

    with pytest.raises(ValueError, match=field):
        build_ohlcv_record(row)


@pytest.mark.parametrize("bad_value", [math.nan, math.inf, -math.inf])
def test_build_ohlcv_record_rejects_non_finite_prices(bad_value: float) -> None:
    """Price fields cannot be NaN or infinite."""
    row = _valid_row()
    row["close"] = bad_value

    with pytest.raises(ValueError, match="finite"):
        build_ohlcv_record(row)


@pytest.mark.parametrize("field", ["open", "high", "low", "close", "adj_close"])
def test_build_ohlcv_record_rejects_non_positive_prices(field: str) -> None:
    """Equity price fields must be positive."""
    row = _valid_row()
    row[field] = 0

    with pytest.raises(ValueError, match="positive"):
        build_ohlcv_record(row)


@pytest.mark.parametrize(
    ("updates", "message"),
    [
        ({"high": 98.0}, "high"),
        ({"open": 98.0}, "open"),
        ({"close": 106.0}, "close"),
    ],
)
def test_build_ohlcv_record_rejects_invalid_ohlc_relationships(
    updates: dict[str, float],
    message: str,
) -> None:
    """High/low/open/close relationships are validated before record creation."""
    row = _valid_row()
    row.update(updates)

    with pytest.raises(ValueError, match=message):
        build_ohlcv_record(row)


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("volume", -1, "non-negative"),
        ("volume", 1.5, "integer"),
        ("dollar_volume", -0.01, "non-negative"),
        ("dollar_volume", math.nan, "finite"),
    ],
)
def test_build_ohlcv_record_rejects_bad_volume_values(
    field: str,
    value: float,
    message: str,
) -> None:
    """Volume and dollar-volume fields must be non-negative and finite."""
    row = _valid_row()
    row[field] = value

    with pytest.raises(ValueError, match=message):
        build_ohlcv_record(row)
