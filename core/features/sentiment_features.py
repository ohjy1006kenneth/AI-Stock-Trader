"""Layer 1 sentiment aggregation from scored FinBERT news rows."""
from __future__ import annotations

import importlib
import json
import math
import re
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

from core.contracts.schemas import NewsSentimentRecord

if TYPE_CHECKING:
    import pandas as pd

DEFAULT_SOURCE_CREDIBILITY_CONFIG_PATH = (
    Path(__file__).resolve().parents[2] / "config" / "source_credibility.json"
)

SENTIMENT_AGGREGATE_COLUMNS: tuple[str, ...] = (
    "date",
    "ticker",
    "headline",
    "source",
    "published_at",
    "sentiment_positive",
    "sentiment_negative",
    "sentiment_neutral",
    "sentiment_score",
    "relevance_score",
)

REQUIRED_SENTIMENT_COLUMNS: frozenset[str] = frozenset(
    {
        "date",
        "ticker",
        "source",
        "sentiment_positive",
        "sentiment_negative",
        "sentiment_neutral",
        "sentiment_score",
        "relevance_score",
    }
)


@dataclass(frozen=True)
class SourceCredibilityConfig:
    """Configurable source credibility weights for sentiment aggregation."""

    default_source_weight: float
    source_weights: Mapping[str, float]


def load_source_credibility_config(
    path: Path = DEFAULT_SOURCE_CREDIBILITY_CONFIG_PATH,
) -> SourceCredibilityConfig:
    """Load source credibility weights from the repository config file."""
    payload = json.loads(path.read_text())
    return SourceCredibilityConfig(
        default_source_weight=_validate_weight(
            payload.get("default_source_weight"),
            label="default_source_weight",
        ),
        source_weights=_normalize_source_weights(payload.get("source_weights", {})),
    )


def aggregate_sentiment_by_ticker_day(
    scored_news: pd.DataFrame,
    *,
    credibility_config: SourceCredibilityConfig | None = None,
) -> pd.DataFrame:
    """Return source-weighted ticker-day sentiment aggregates.

    Args:
        scored_news: Article-level rows with FinBERT probabilities and a
            relevance score. Rows must contain the `NewsSentimentRecord`
            sentiment fields plus `source`.
        credibility_config: Optional source-weight mapping. When omitted, the
            repository default config is loaded from `config/source_credibility.json`.

    Returns:
        DataFrame with columns matching `NewsSentimentRecord`. One row is
        emitted per `(date, ticker)` group. The sentiment fields are weighted by
        source credibility and article relevance.
    """
    pd = _require_pandas()
    config = _normalized_config(credibility_config or load_source_credibility_config())
    _validate_columns(scored_news)

    if len(scored_news) == 0:
        return _empty_frame(pd)

    frame = scored_news.copy()
    frame["date"] = frame["date"].map(_to_iso_date)
    if frame["date"].isna().any():
        raise ValueError("sentiment rows must contain date-like values")

    for column in (
        "sentiment_positive",
        "sentiment_negative",
        "sentiment_neutral",
        "sentiment_score",
        "relevance_score",
    ):
        frame[column] = frame[column].map(_to_float_or_none)

    required_numeric = (
        "sentiment_positive",
        "sentiment_negative",
        "sentiment_neutral",
        "sentiment_score",
    )
    if frame[list(required_numeric)].isna().any().any():
        raise ValueError("sentiment probability and score columns must be numeric")
    _validate_probability_columns(frame)

    frame["_source_weight"] = frame["source"].map(
        lambda value: _source_weight(value, config=config)
    )
    frame["_relevance_weight"] = frame["relevance_score"].map(_relevance_weight)
    frame["_effective_weight"] = frame["_source_weight"] * frame["_relevance_weight"]

    rows: list[dict[str, Any]] = []
    for (date_value, ticker), group in frame.groupby(["date", "ticker"], sort=True, dropna=False):
        if pd.isna(ticker):
            raise ValueError("sentiment rows must contain ticker values")
        rows.append(
            {
                "date": str(date_value),
                "ticker": str(ticker),
                "headline": None,
                "source": None,
                "published_at": None,
                "sentiment_positive": _weighted_average(
                    group["sentiment_positive"], group["_effective_weight"]
                ),
                "sentiment_negative": _weighted_average(
                    group["sentiment_negative"], group["_effective_weight"]
                ),
                "sentiment_neutral": _weighted_average(
                    group["sentiment_neutral"], group["_effective_weight"]
                ),
                "sentiment_score": _weighted_average(
                    group["sentiment_score"], group["_effective_weight"]
                ),
                "relevance_score": _weighted_average(
                    group["relevance_score"], group["_source_weight"]
                ),
            }
        )

    return pd.DataFrame(rows, columns=list(SENTIMENT_AGGREGATE_COLUMNS))


def sentiment_aggregates_to_records(aggregates: pd.DataFrame) -> list[NewsSentimentRecord]:
    """Convert sentiment aggregate rows into `NewsSentimentRecord` instances."""
    _validate_columns(aggregates, required=frozenset(SENTIMENT_AGGREGATE_COLUMNS))

    records: list[NewsSentimentRecord] = []
    for row in aggregates.to_dict(orient="records"):
        records.append(
            NewsSentimentRecord(
                date=str(row["date"]),
                ticker=str(row["ticker"]),
                headline=_normalize_optional_string(row.get("headline")),
                source=_normalize_optional_string(row.get("source")),
                published_at=row.get("published_at") or None,
                sentiment_positive=_normalize_optional_float(row.get("sentiment_positive")),
                sentiment_negative=_normalize_optional_float(row.get("sentiment_negative")),
                sentiment_neutral=_normalize_optional_float(row.get("sentiment_neutral")),
                sentiment_score=_normalize_optional_float(row.get("sentiment_score")),
                relevance_score=_normalize_optional_float(row.get("relevance_score")),
            )
        )
    return records


def _validate_columns(
    frame: pd.DataFrame,
    *,
    required: frozenset[str] = REQUIRED_SENTIMENT_COLUMNS,
) -> None:
    """Raise when sentiment rows are missing required columns."""
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ValueError(f"Sentiment frame missing required columns: {missing}")


def _validate_config(config: SourceCredibilityConfig) -> None:
    """Raise when source credibility config contains invalid weights."""
    _validate_weight(config.default_source_weight, label="default_source_weight")
    _normalize_source_weights(config.source_weights)


def _normalized_config(config: SourceCredibilityConfig) -> SourceCredibilityConfig:
    """Return a config copy with validated source keys and weights."""
    return SourceCredibilityConfig(
        default_source_weight=_validate_weight(
            config.default_source_weight,
            label="default_source_weight",
        ),
        source_weights=_normalize_source_weights(config.source_weights),
    )


def _normalize_source_weights(raw_weights: Mapping[str, Any]) -> dict[str, float]:
    """Return source weights keyed by normalized source names."""
    if not isinstance(raw_weights, Mapping):
        raise ValueError("source_weights must be a mapping")

    weights: dict[str, float] = {}
    for source, weight in raw_weights.items():
        normalized_source = _normalize_source(source)
        if not normalized_source:
            raise ValueError("source_weights keys must be non-empty source names")
        weights[normalized_source] = _validate_weight(
            weight,
            label=f"source_weights[{source!r}]",
        )
    return weights


def _validate_weight(value: Any, *, label: str) -> float:
    """Return a finite positive weight or raise ValueError."""
    numeric = _to_float_or_none(value)
    if numeric is None or numeric <= 0.0:
        raise ValueError(f"{label} must be a positive finite number")
    return numeric


def _source_weight(source: Any, *, config: SourceCredibilityConfig) -> float:
    """Return configured source weight for one raw source value."""
    normalized_source = _normalize_source(source)
    if not normalized_source:
        return config.default_source_weight
    return config.source_weights.get(normalized_source, config.default_source_weight)


def _relevance_weight(value: Any) -> float:
    """Return the non-negative article relevance multiplier."""
    numeric = _to_float_or_none(value)
    if numeric is None:
        return 1.0
    return max(numeric, 0.0)


def _weighted_average(values: pd.Series, weights: pd.Series) -> float | None:
    """Return the finite weighted average, or None when no positive weight exists."""
    total_weight = 0.0
    weighted_sum = 0.0
    for raw_value, raw_weight in zip(values.tolist(), weights.tolist(), strict=True):
        value = _to_float_or_none(raw_value)
        weight = _to_float_or_none(raw_weight)
        if value is None or weight is None or weight <= 0.0:
            continue
        weighted_sum += value * weight
        total_weight += weight
    if total_weight <= 0.0:
        return None
    return weighted_sum / total_weight


def _validate_probability_columns(frame: pd.DataFrame) -> None:
    """Raise when FinBERT probability columns fall outside [0, 1]."""
    for column in ("sentiment_positive", "sentiment_negative", "sentiment_neutral"):
        invalid = frame[column].map(lambda value: value < 0.0 or value > 1.0)
        if invalid.any():
            raise ValueError(f"{column} must contain probabilities in [0, 1]")


def _normalize_source(source: Any) -> str:
    """Return a stable lowercase source key for config lookup."""
    if source is None:
        return ""
    return re.sub(r"\s+", " ", str(source).strip().lower())


def _to_iso_date(value: Any) -> str | None:
    """Normalize date-like values to YYYY-MM-DD strings."""
    if value is None:
        return None
    try:
        timestamp = _require_pandas().to_datetime(value, utc=True)
    except (TypeError, ValueError):
        return None
    if timestamp is None or getattr(timestamp, "tzinfo", None) is None:
        return None
    return timestamp.date().isoformat()


def _to_float_or_none(value: Any) -> float | None:
    """Return a finite float value, or None for missing/non-finite input."""
    if value is None:
        return None
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(numeric) or math.isinf(numeric):
        return None
    return numeric


def _normalize_optional_float(value: Any) -> float | None:
    """Return a finite optional float suitable for Pydantic models."""
    return _to_float_or_none(value)


def _normalize_optional_string(value: Any) -> str | None:
    """Return a non-empty string or None."""
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _empty_frame(pd: Any) -> pd.DataFrame:
    """Return an empty sentiment aggregate frame with canonical columns."""
    return pd.DataFrame(columns=list(SENTIMENT_AGGREGATE_COLUMNS))


def _require_pandas() -> Any:
    """Import pandas/pyarrow lazily with a clear error when absent."""
    try:
        import pandas as pd

        importlib.import_module("pyarrow")
    except ModuleNotFoundError as exc:
        raise ModuleNotFoundError(
            "pandas and pyarrow are required for sentiment feature aggregation."
        ) from exc
    return pd
