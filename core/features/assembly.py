"""Layer 1 feature assembly and leakage guards."""
from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, time
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from core.contracts.schemas import FeatureRecord


@dataclass(frozen=True)
class Layer1FeatureInput:
    """One set of FeatureRecords with the timestamp when its data became available."""

    name: str
    records: Sequence[FeatureRecord]
    as_of_timestamp: datetime

    def __post_init__(self) -> None:
        """Validate input metadata."""
        if not self.name.strip():
            raise ValueError("feature input name cannot be empty")
        if self.as_of_timestamp.tzinfo is None:
            raise ValueError("as_of_timestamp must be timezone-aware")


def assemble_layer1_feature_records(
    inputs: Sequence[Layer1FeatureInput],
    *,
    market_timezone: str = "America/New_York",
    market_open: time = time(9, 30),
) -> list[FeatureRecord]:
    """Merge Layer 1 FeatureRecords by `(date, ticker)` with no-leakage validation."""
    timezone = _load_timezone(market_timezone)
    if market_open.tzinfo is not None:
        raise ValueError("market_open must be a naive local market time")

    assembled: dict[tuple[str, str], dict[str, Any]] = {}
    for feature_input in inputs:
        seen_keys: set[tuple[str, str]] = set()
        for record in feature_input.records:
            key = (record.date, record.ticker)
            if key in seen_keys:
                raise ValueError(
                    f"Duplicate FeatureRecord for {feature_input.name}: "
                    f"{record.date}/{record.ticker}"
                )
            seen_keys.add(key)
            _validate_no_leakage(
                record=record,
                as_of_timestamp=feature_input.as_of_timestamp,
                market_timezone=timezone,
                market_open=market_open,
                input_name=feature_input.name,
            )
            clean_features = _validated_features(record.features, input_name=feature_input.name)
            target = assembled.setdefault(key, {})
            _merge_features(target, clean_features, input_name=feature_input.name)

    return [
        FeatureRecord(date=date_value, ticker=ticker, features=features)
        for (date_value, ticker), features in sorted(assembled.items())
    ]


def validate_feature_availability(
    records: Sequence[FeatureRecord],
    *,
    as_of_timestamp: datetime,
    market_timezone: str = "America/New_York",
    market_open: time = time(9, 30),
    input_name: str = "features",
) -> None:
    """Validate that FeatureRecords were available before market open on their dates."""
    timezone = _load_timezone(market_timezone)
    if market_open.tzinfo is not None:
        raise ValueError("market_open must be a naive local market time")
    for record in records:
        _validate_no_leakage(
            record=record,
            as_of_timestamp=as_of_timestamp,
            market_timezone=timezone,
            market_open=market_open,
            input_name=input_name,
        )


def _validate_no_leakage(
    *,
    record: FeatureRecord,
    as_of_timestamp: datetime,
    market_timezone: ZoneInfo,
    market_open: time,
    input_name: str,
) -> None:
    """Raise when a feature value was not available before the target market open."""
    try:
        local_midnight = datetime.fromisoformat(record.date).replace(tzinfo=market_timezone)
    except ValueError as exc:
        raise ValueError(f"{input_name} record date must be YYYY-MM-DD") from exc
    market_open_at = datetime.combine(
        local_midnight.date(),
        market_open,
        tzinfo=market_timezone,
    )
    local_as_of = as_of_timestamp.astimezone(market_timezone)
    if local_as_of >= market_open_at:
        raise ValueError(
            f"{input_name} for {record.date}/{record.ticker} has as_of_timestamp "
            f"{local_as_of.isoformat()} at or after market open {market_open_at.isoformat()}"
        )


def _validated_features(features: Mapping[str, Any], *, input_name: str) -> dict[str, Any]:
    """Return schema-compatible features after rejecting non-finite numeric values."""
    clean: dict[str, Any] = {}
    for key, value in features.items():
        if not isinstance(key, str) or not key.strip():
            raise ValueError(f"{input_name} feature keys must be non-empty strings")
        if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
            raise ValueError(f"{input_name} feature {key!r} must be finite")
        clean[key] = value
    return clean


def _merge_features(
    target: dict[str, Any],
    incoming: Mapping[str, Any],
    *,
    input_name: str,
) -> None:
    """Merge one feature map into another and reject conflicting keys."""
    for key, value in incoming.items():
        if key in target and target[key] != value:
            raise ValueError(f"Conflicting feature {key!r} from {input_name}")
        target[key] = value


def _load_timezone(timezone_name: str) -> ZoneInfo:
    """Return a ZoneInfo object or raise a clear validation error."""
    try:
        return ZoneInfo(timezone_name)
    except ZoneInfoNotFoundError as exc:
        raise ValueError(f"Invalid market_timezone: {timezone_name}") from exc
