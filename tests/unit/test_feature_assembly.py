from __future__ import annotations

from datetime import UTC, datetime, time

import pytest

from core.contracts.schemas import FeatureRecord
from core.features.assembly import (
    Layer1FeatureInput,
    assemble_layer1_feature_records,
    validate_feature_availability,
)


def test_assemble_layer1_feature_records_merges_feature_branches() -> None:
    """Feature branches merge into one sorted FeatureRecord per date/ticker."""
    as_of = datetime(2024, 1, 2, 13, 0, tzinfo=UTC)
    market = Layer1FeatureInput(
        name="market",
        as_of_timestamp=as_of,
        records=[
            FeatureRecord(date="2024-01-02", ticker="MSFT", features={"returns_1d": 0.02}),
            FeatureRecord(date="2024-01-02", ticker="AAPL", features={"returns_1d": 0.01}),
        ],
    )
    sentiment = Layer1FeatureInput(
        name="sentiment",
        as_of_timestamp=as_of,
        records=[
            FeatureRecord(
                date="2024-01-02",
                ticker="AAPL",
                features={"nlp_sentiment_score": 0.7},
            )
        ],
    )

    assembled = assemble_layer1_feature_records([market, sentiment])

    assert assembled == [
        FeatureRecord(
            date="2024-01-02",
            ticker="AAPL",
            features={"returns_1d": 0.01, "nlp_sentiment_score": 0.7},
        ),
        FeatureRecord(date="2024-01-02", ticker="MSFT", features={"returns_1d": 0.02}),
    ]


def test_assemble_layer1_feature_records_returns_empty_for_no_inputs() -> None:
    """Empty feature inputs produce no assembled records."""
    assert assemble_layer1_feature_records([]) == []


def test_assemble_layer1_feature_records_rejects_market_open_boundary() -> None:
    """Features available at market open are rejected as lookahead-biased."""
    source = Layer1FeatureInput(
        name="market",
        as_of_timestamp=datetime(2024, 1, 2, 14, 30, tzinfo=UTC),
        records=[FeatureRecord(date="2024-01-02", ticker="AAPL", features={"x": 1.0})],
    )

    with pytest.raises(ValueError, match="at or after market open"):
        assemble_layer1_feature_records([source])


def test_validate_feature_availability_accepts_pre_open_timestamp() -> None:
    """Pre-open availability timestamps pass the leakage guard."""
    validate_feature_availability(
        [FeatureRecord(date="2024-01-02", ticker="AAPL", features={"x": 1.0})],
        as_of_timestamp=datetime(2024, 1, 2, 14, 29, 59, tzinfo=UTC),
    )


def test_assemble_layer1_feature_records_uses_configured_market_timezone() -> None:
    """Market-open boundary checks use the configured local market timezone."""
    source = Layer1FeatureInput(
        name="market",
        as_of_timestamp=datetime(2024, 1, 2, 8, 59, tzinfo=UTC),
        records=[FeatureRecord(date="2024-01-02", ticker="AAPL", features={"x": 1.0})],
    )

    assert assemble_layer1_feature_records([source], market_timezone="Europe/London") == [
        FeatureRecord(date="2024-01-02", ticker="AAPL", features={"x": 1.0})
    ]


def test_assemble_layer1_feature_records_rejects_naive_as_of_timestamp() -> None:
    """Source metadata must use timezone-aware timestamps."""
    with pytest.raises(ValueError, match="timezone-aware"):
        Layer1FeatureInput(
            name="market",
            as_of_timestamp=datetime(2024, 1, 2, 8, 0),
            records=[],
        )


def test_assemble_layer1_feature_records_rejects_conflicting_features() -> None:
    """Two branches cannot publish different values for the same feature key."""
    as_of = datetime(2024, 1, 2, 13, 0, tzinfo=UTC)
    first = Layer1FeatureInput(
        name="first",
        as_of_timestamp=as_of,
        records=[FeatureRecord(date="2024-01-02", ticker="AAPL", features={"x": 1.0})],
    )
    second = Layer1FeatureInput(
        name="second",
        as_of_timestamp=as_of,
        records=[FeatureRecord(date="2024-01-02", ticker="AAPL", features={"x": 2.0})],
    )

    with pytest.raises(ValueError, match="Conflicting feature"):
        assemble_layer1_feature_records([first, second])


def test_assemble_layer1_feature_records_rejects_nan_feature_values() -> None:
    """NaN feature values fail before they reach the final FeatureRecord."""
    source = Layer1FeatureInput(
        name="market",
        as_of_timestamp=datetime(2024, 1, 2, 13, 0, tzinfo=UTC),
        records=[FeatureRecord(date="2024-01-02", ticker="AAPL", features={"x": float("nan")})],
    )

    with pytest.raises(ValueError, match="finite"):
        assemble_layer1_feature_records([source])


def test_assemble_layer1_feature_records_rejects_tz_aware_market_open() -> None:
    """The market-open wall clock must be a local naive time."""
    source = Layer1FeatureInput(
        name="market",
        as_of_timestamp=datetime(2024, 1, 2, 13, 0, tzinfo=UTC),
        records=[],
    )

    with pytest.raises(ValueError, match="naive"):
        assemble_layer1_feature_records([source], market_open=time(9, 30, tzinfo=UTC))
