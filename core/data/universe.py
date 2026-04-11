from __future__ import annotations

from collections.abc import Mapping
from datetime import date as Date
from typing import Any

from core.contracts.schemas import UniverseRecord

REQUIRED_UNIVERSE_FIELDS = ("date", "ticker", "in_universe")
BOOLEAN_FIELDS = ("in_universe", "tradable", "liquid", "halted", "data_quality_ok")
DEFAULT_UNIVERSE_VALUES = {
    "tradable": True,
    "liquid": True,
    "halted": False,
    "data_quality_ok": True,
    "reason": None,
}
TRUE_VALUES = {"1", "true", "t", "yes", "y"}
FALSE_VALUES = {"0", "false", "f", "no", "n"}


def build_universe_record(row: Mapping[str, Any]) -> UniverseRecord:
    """Build a validated UniverseRecord from a vendor-neutral mapping."""
    _require_fields(row, REQUIRED_UNIVERSE_FIELDS)

    values: dict[str, Any] = {
        "date": _coerce_date(row["date"]),
        "ticker": _coerce_ticker(row["ticker"]),
        "in_universe": _coerce_bool(row["in_universe"], "in_universe"),
        **DEFAULT_UNIVERSE_VALUES,
    }

    for field in BOOLEAN_FIELDS:
        if field in row and row[field] is not None:
            values[field] = _coerce_bool(row[field], field)

    if "reason" in row:
        values["reason"] = _coerce_optional_reason(row["reason"])

    return UniverseRecord(**values)


def _require_fields(row: Mapping[str, Any], fields: tuple[str, ...]) -> None:
    """Require all fields to exist and be non-null."""
    missing = [field for field in fields if field not in row or row[field] is None]
    if missing:
        raise ValueError(f"Missing required universe field(s): {', '.join(missing)}")


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


def _coerce_bool(value: Any, field_name: str) -> bool:
    """Coerce common boolean representations."""
    if isinstance(value, bool):
        return value
    if isinstance(value, int) and value in {0, 1}:
        return bool(value)
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in TRUE_VALUES:
            return True
        if normalized in FALSE_VALUES:
            return False
    raise ValueError(f"{field_name} must be a boolean value")


def _coerce_optional_reason(value: Any) -> str | None:
    """Coerce an optional reason string."""
    if value is None:
        return None
    if not isinstance(value, str):
        raise TypeError("reason must be a string or None")
    stripped = value.strip()
    return stripped or None


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
