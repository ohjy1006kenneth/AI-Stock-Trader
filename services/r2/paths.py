from __future__ import annotations

from datetime import date as Date
from datetime import datetime
from pathlib import PurePosixPath


def build_r2_key(*parts: str) -> str:
    """Build a safe POSIX object key from validated path parts."""
    if not parts:
        raise ValueError("R2 key requires at least one path part")

    cleaned_parts = [_validate_key_part(part) for part in parts]
    key = PurePosixPath(*cleaned_parts).as_posix()
    path = PurePosixPath(key)
    if path.is_absolute() or ".." in path.parts:
        raise ValueError(f"Unsafe R2 key: {key}")
    return key


def raw_price_path(security_id: str) -> str:
    """Return the canonical raw OHLCV Parquet path for one stable security identifier."""
    safe_security_id = _validate_key_part(security_id)
    return build_r2_key("raw", "prices", f"{safe_security_id}.parquet")


def raw_news_path(as_of_date: str | Date | datetime) -> str:
    """Return the canonical raw news JSON Lines path for one date."""
    return build_r2_key("raw", "news", f"{_format_date(as_of_date)}.jsonl")


def raw_universe_path(as_of_date: str | Date | datetime) -> str:
    """Return the canonical raw universe eligibility-mask CSV path for one date."""
    return build_r2_key("raw", "universe", f"{_format_date(as_of_date)}.csv")


def raw_fundamentals_path(ticker: str) -> str:
    """Return the canonical raw fundamentals Parquet path for one ticker's full history."""
    safe_ticker = _validate_key_part(ticker)
    return build_r2_key("raw", "fundamentals", f"{safe_ticker}.parquet")


def raw_macro_path(observation_date: str | Date | datetime) -> str:
    """Return the canonical raw macro/rates Parquet path for one observation date."""
    return build_r2_key("raw", "macro", f"{_format_date(observation_date)}.parquet")


def raw_reference_path(name: str, extension: str = "json") -> str:
    """Return the canonical raw reference snapshot path."""
    safe_name = _validate_key_part(name)
    safe_extension = _validate_extension(extension)
    return build_r2_key("raw", "reference", f"{safe_name}.{safe_extension}")


def raw_security_master_path(as_of_date: str | Date | datetime) -> str:
    """Return the canonical raw security-master snapshot path for one date."""
    return build_r2_key("raw", "reference", "security_master", f"{_format_date(as_of_date)}.json")


def layer1_feature_path(as_of_date: str | Date | datetime, ticker: str) -> str:
    """Return the canonical Layer 1 feature-shard Parquet path for one date/ticker pair."""
    safe_ticker = _validate_key_part(ticker)
    return build_r2_key("features", "layer1", _format_date(as_of_date), f"{safe_ticker}.parquet")


def pipeline_manifest_path(stage: str, run_id: str) -> str:
    """Return the canonical pipeline manifest path for one stage/run pair."""
    safe_stage = _validate_key_part(stage)
    safe_run_id = _validate_key_part(run_id)
    return build_r2_key("artifacts", "manifests", safe_stage, f"{safe_run_id}.json")


def _format_date(value: str | Date | datetime) -> str:
    """Normalize a date-like value to YYYY-MM-DD."""
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, Date):
        return value.isoformat()
    if not isinstance(value, str):
        raise TypeError("Date value must be a string, date, or datetime")

    stripped = value.strip()
    if not _is_extended_iso_date(stripped):
        raise ValueError(f"Date must be YYYY-MM-DD: {value}")
    try:
        return Date.fromisoformat(stripped).isoformat()
    except ValueError as exc:
        raise ValueError(f"Date must be YYYY-MM-DD: {value}") from exc


def _validate_key_part(part: str) -> str:
    """Validate one object-key part and return its stripped value."""
    if not isinstance(part, str):
        raise TypeError("R2 key parts must be strings")

    stripped = part.strip()
    if not stripped:
        raise ValueError("R2 key parts cannot be empty")
    if stripped in {".", ".."}:
        raise ValueError(f"Unsafe R2 key part: {part}")
    if stripped.startswith("/") or "\\" in stripped or "/" in stripped:
        raise ValueError(f"Unsafe R2 key part: {part}")
    if "\x00" in stripped:
        raise ValueError("R2 key parts cannot contain null bytes")
    return stripped


def _validate_extension(extension: str) -> str:
    """Validate a file extension without a leading dot."""
    safe_extension = _validate_key_part(extension)
    if safe_extension.startswith(".") or "." in safe_extension:
        raise ValueError(f"File extension must not contain dots: {extension}")
    return safe_extension


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
