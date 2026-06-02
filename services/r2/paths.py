from __future__ import annotations

import re
from datetime import date as Date
from datetime import datetime
from pathlib import PurePosixPath

RAW_PRICE_PREFIX = "raw/prices/"
_CANONICAL_RAW_PRICE_KEY_RE = re.compile(r"^raw/prices/[^/_]+\.parquet$")
_CANONICAL_RAW_MACRO_KEY_RE = re.compile(r"^raw/macro/(?P<date>\d{4}-\d{2}-\d{2})\.parquet$")


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


def is_canonical_raw_price_key(key: str) -> bool:
    """Return True when a raw price key follows the one-file-per-symbol pattern."""
    if not isinstance(key, str):
        raise TypeError("key must be a string")
    return bool(_CANONICAL_RAW_PRICE_KEY_RE.fullmatch(key.strip()))


def is_legacy_raw_price_key(key: str) -> bool:
    """Return True when a raw price key is a legacy non-canonical Parquet filename."""
    if not isinstance(key, str):
        raise TypeError("key must be a string")
    stripped = key.strip()
    if not stripped.startswith(RAW_PRICE_PREFIX) or not stripped.endswith(".parquet"):
        return False
    return not is_canonical_raw_price_key(stripped)


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


def raw_macro_path(archive_date: str | Date | datetime) -> str:
    """Return the canonical raw macro/rates Parquet path for one archive date."""
    return build_r2_key("raw", "macro", f"{_format_date(archive_date)}.parquet")


def raw_order_book_path(provider: str, archive_date: str | Date | datetime) -> str:
    """Return the canonical raw order-book Parquet path for one provider/date pair."""
    safe_provider = _validate_key_part(provider).lower()
    return build_r2_key(
        "raw",
        "order_book",
        safe_provider,
        f"{_format_date(archive_date)}.parquet",
    )


def is_canonical_raw_macro_key(key: str) -> bool:
    """Return True when a raw macro key follows the one-file-per-day pattern."""
    if not isinstance(key, str):
        raise TypeError("key must be a string")
    return bool(_CANONICAL_RAW_MACRO_KEY_RE.fullmatch(key.strip()))


def raw_macro_date_from_key(key: str) -> str:
    """Return the archive date encoded in a canonical raw macro key."""
    if not isinstance(key, str):
        raise TypeError("key must be a string")
    match = _CANONICAL_RAW_MACRO_KEY_RE.fullmatch(key.strip())
    if match is None:
        raise ValueError(f"Not a canonical raw macro key: {key}")
    return _format_date(match.group("date"))


def raw_reference_path(name: str, extension: str = "json") -> str:
    """Return the canonical raw reference snapshot path."""
    safe_name = _validate_key_part(name)
    safe_extension = _validate_extension(extension)
    return build_r2_key("raw", "reference", f"{safe_name}.{safe_extension}")


def raw_security_master_path(as_of_date: str | Date | datetime) -> str:
    """Return the canonical raw security-master snapshot path for one date."""
    return build_r2_key("raw", "reference", "security_master", f"{_format_date(as_of_date)}.json")


def layer1_feature_path(as_of_date: str | Date | datetime, ticker: str) -> str:
    """Return the canonical date-first Layer 1 feature-shard path."""
    safe_ticker = _validate_key_part(ticker)
    return build_r2_key("features", _format_date(as_of_date), f"{safe_ticker}.parquet")


def layer1_ticker_history_path(ticker: str) -> str:
    """Return the legacy Layer 1 full-history Parquet path for one ticker."""
    safe_ticker = _validate_key_part(ticker)
    return build_r2_key("features", "layer1", f"{safe_ticker}.parquet")


def layer1_news_preprocessing_path(as_of_date: str | Date | datetime, run_id: str) -> str:
    """Return the canonical date-first Layer 1 preprocessed-news parquet path."""
    safe_run_id = _validate_key_part(run_id)
    return build_r2_key(
        "features",
        _format_date(as_of_date),
        "news_sentiment",
        f"{safe_run_id}.parquet",
    )


def layer1_text_embedding_path(as_of_date: str | Date | datetime, run_id: str) -> str:
    """Return the canonical date-first Layer 1 sentence-embedding cache path."""
    safe_run_id = _validate_key_part(run_id)
    return build_r2_key(
        "features",
        _format_date(as_of_date),
        "text_embeddings",
        f"{safe_run_id}.parquet",
    )


def layer1_topic_label_path(as_of_date: str | Date | datetime, run_id: str) -> str:
    """Return the canonical date-first Layer 1 sentence-topic label path."""
    safe_run_id = _validate_key_part(run_id)
    return build_r2_key(
        "features",
        _format_date(as_of_date),
        "topic_labels",
        f"{safe_run_id}.parquet",
    )


def layer1_topic_feature_path(as_of_date: str | Date | datetime, run_id: str) -> str:
    """Return the canonical date-first Layer 1 ticker-day topic feature path."""
    safe_run_id = _validate_key_part(run_id)
    return build_r2_key(
        "features",
        _format_date(as_of_date),
        "topic_features",
        f"{safe_run_id}.parquet",
    )


def layer1_sentiment_score_path(as_of_date: str | Date | datetime, run_id: str) -> str:
    """Return the canonical date-first Layer 1 scored FinBERT news path."""
    safe_run_id = _validate_key_part(run_id)
    return build_r2_key(
        "features",
        _format_date(as_of_date),
        "news_sentiment_scored",
        f"{safe_run_id}.parquet",
    )


def layer1_sentiment_feature_path(as_of_date: str | Date | datetime, run_id: str) -> str:
    """Return the canonical date-first Layer 1 ticker-day sentiment feature path."""
    safe_run_id = _validate_key_part(run_id)
    return build_r2_key(
        "features",
        _format_date(as_of_date),
        "sentiment_features",
        f"{safe_run_id}.parquet",
    )


def layer1_regime_path(as_of_date: str | Date | datetime, run_id: str) -> str:
    """Return the canonical date-first Layer 1.5 regime-feature path for one date/run."""
    safe_run_id = _validate_key_part(run_id)
    return build_r2_key(
        "features",
        _format_date(as_of_date),
        "regime",
        f"{safe_run_id}.parquet",
    )


def legacy_layer1_regime_path(run_id: str) -> str:
    """Return the legacy Layer 1.5 regime-feature path for compatibility reads."""
    safe_run_id = _validate_key_part(run_id)
    return build_r2_key("features", "layer1_5", "regime", f"{safe_run_id}.parquet")


def layer1_validation_report_path(run_id: str, from_date: str, to_date: str) -> str:
    """Return the canonical durable Layer 1 validation-report key for one run window."""
    safe_run_id = _validate_key_part(run_id)
    return build_r2_key(
        "artifacts",
        "reports",
        "integration",
        f"layer1_archive_validation_{safe_run_id}_{_format_date(from_date)}_to_{_format_date(to_date)}.json",
    )


def layer0_ohlcv_provenance_report_path(run_id: str) -> str:
    """Return the canonical Layer 0 OHLCV adjustment-provenance report key for one run."""
    safe_run_id = _validate_key_part(run_id)
    return build_r2_key(
        "artifacts",
        "reports",
        "integration",
        f"layer0_ohlcv_provenance_{safe_run_id}.json",
    )


def layer1_label_path(as_of_date: str | Date | datetime, ticker: str) -> str:
    """Return the canonical Layer 1 label-shard Parquet path for one date/ticker pair."""
    safe_ticker = _validate_key_part(ticker)
    return build_r2_key("labels", "layer1", _format_date(as_of_date), f"{safe_ticker}.parquet")


def layer2_model_path(model_version: str) -> str:
    """Return the canonical Layer 2 model artifact path for one version."""
    safe_version = _validate_key_part(model_version)
    return build_r2_key("models", "layer2", f"{safe_version}.pkl")


def layer2_model_manifest_path(model_version: str) -> str:
    """Return the canonical Layer 2 model manifest path for one version."""
    safe_version = _validate_key_part(model_version)
    return build_r2_key("models", "layer2", f"{safe_version}_manifest.json")


def backtest_report_path(report_id: str) -> str:
    """Return the canonical backtest report path for one run."""
    safe_id = _validate_key_part(report_id)
    return build_r2_key("artifacts", "reports", "backtests", f"{safe_id}.json")


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
