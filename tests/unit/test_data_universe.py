from __future__ import annotations

from typing import Any

import pytest

from core.data.universe import build_universe_record


def _valid_row() -> dict[str, Any]:
    """Return a valid vendor-neutral universe row."""
    return {
        "date": "2025-01-02",
        "ticker": " aapl ",
        "in_universe": True,
    }


def test_build_universe_record_happy_path_uses_explicit_defaults() -> None:
    """A minimal row becomes a schema-valid UniverseRecord with explicit defaults."""
    record = build_universe_record(_valid_row())

    assert record.date == "2025-01-02"
    assert record.ticker == "AAPL"
    assert record.in_universe is True
    assert record.tradable is True
    assert record.liquid is True
    assert record.halted is False
    assert record.data_quality_ok is True
    assert record.reason is None


def test_build_universe_record_accepts_explicit_optional_fields() -> None:
    """Optional flags and reason text are preserved when supplied."""
    row = {
        **_valid_row(),
        "tradable": "false",
        "liquid": 0,
        "halted": "yes",
        "data_quality_ok": "n",
        "reason": " data issue ",
    }

    record = build_universe_record(row)

    assert record.tradable is False
    assert record.liquid is False
    assert record.halted is True
    assert record.data_quality_ok is False
    assert record.reason == "data issue"


@pytest.mark.parametrize("field", ["date", "ticker", "in_universe"])
def test_build_universe_record_rejects_missing_required_fields(field: str) -> None:
    """Date, ticker, and in_universe are required."""
    row = _valid_row()
    row.pop(field)

    with pytest.raises(ValueError, match=field):
        build_universe_record(row)


def test_build_universe_record_rejects_invalid_date() -> None:
    """Universe dates must be ISO calendar dates."""
    row = _valid_row()
    row["date"] = "20250102"

    with pytest.raises(ValueError, match="YYYY-MM-DD"):
        build_universe_record(row)


def test_build_universe_record_rejects_empty_ticker() -> None:
    """Ticker identity cannot be blank."""
    row = _valid_row()
    row["ticker"] = "   "

    with pytest.raises(ValueError, match="ticker"):
        build_universe_record(row)


@pytest.mark.parametrize("field", ["in_universe", "tradable", "liquid", "halted", "data_quality_ok"])
def test_build_universe_record_rejects_invalid_bool_values(field: str) -> None:
    """Boolean fields must use recognized boolean values."""
    row = _valid_row()
    row[field] = "maybe"

    with pytest.raises(ValueError, match=field):
        build_universe_record(row)


def test_build_universe_record_rejects_non_string_reason() -> None:
    """Reason metadata must be text when present."""
    row = _valid_row()
    row["reason"] = 123

    with pytest.raises(TypeError, match="reason"):
        build_universe_record(row)
