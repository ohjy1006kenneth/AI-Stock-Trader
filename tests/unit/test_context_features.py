from __future__ import annotations

import json

import pandas as pd
import pytest

from core.features.context_features import (
    CONTEXT_FEATURE_COLUMNS,
    compute_context_features,
    context_features_to_records,
)
from core.features.fundamentals_features import FUNDAMENTAL_FEATURE_COLUMNS
from core.features.macro_features import MACRO_FEATURE_COLUMNS


def _fundamentals_row(
    *,
    availability_date: str,
    earnings_date: str | None = None,
) -> dict[str, object]:
    """Build one normalized SimFin archive row for context tests."""
    return {
        "source": "simfin",
        "ticker": "AAPL",
        "report_date": "2024-03-31",
        "availability_date": availability_date,
        "retrieved_at": "2024-05-03T00:00:00",
        "fiscal_year": 2024,
        "fiscal_period": "Q1",
        "statement": "pl",
        "earnings_date": earnings_date,
        "raw_json": json.dumps({"revenue": 1_000.0, "netIncome": 100.0}),
    }


def _ohlcv_frame(dates: list[str]) -> pd.DataFrame:
    """Build a minimal OHLCV frame used by fundamentals feature computation."""
    prices = [100.0 + index for index in range(len(dates))]
    return pd.DataFrame({"date": dates, "adj_close": prices})


def _macro_row(
    *,
    series_id: str,
    observation_date: str,
    realtime_start: str,
    value: float | None,
) -> dict[str, object]:
    """Build one normalized FRED archive row for context tests."""
    return {
        "source": "fred",
        "series_id": series_id,
        "observation_date": observation_date,
        "realtime_start": realtime_start,
        "realtime_end": realtime_start,
        "retrieved_at": "2024-01-01T00:00:00+00:00",
        "value": value,
        "is_missing": value is None,
        "raw": {},
    }


def _empty_macro_frame() -> pd.DataFrame:
    """Return an empty macro frame with required archive columns."""
    return pd.DataFrame(
        columns=["series_id", "observation_date", "realtime_start", "value", "is_missing"]
    )


def test_compute_context_features_merges_fundamentals_earnings_and_macro() -> None:
    """Context frame includes ticker-specific and market-wide context features."""
    fundamentals = pd.DataFrame(
        [_fundamentals_row(availability_date="2024-05-03", earnings_date="2024-05-10")]
    )
    ohlcv = _ohlcv_frame(["2024-05-03", "2024-05-06", "2024-05-13"])
    macro = pd.DataFrame(
        [
            _macro_row(
                series_id="DGS10",
                observation_date="2024-05-03",
                realtime_start="2024-05-03",
                value=4.5,
            ),
            _macro_row(
                series_id="DGS2",
                observation_date="2024-05-03",
                realtime_start="2024-05-03",
                value=4.2,
            ),
        ]
    )

    features = compute_context_features(fundamentals, ohlcv, macro, "AAPL")

    assert list(features.columns) == ["date", "ticker", *CONTEXT_FEATURE_COLUMNS]
    assert features["date"].tolist() == ["2024-05-03", "2024-05-06", "2024-05-13"]
    assert features.loc[1, "net_profit_margin"] == pytest.approx(0.1)
    assert features.loc[1, "days_to_next_earnings"] == 4
    assert features.loc[1, "treasury_10y"] == pytest.approx(4.5)
    assert features.loc[1, "yield_curve_slope_10y_2y"] == pytest.approx(0.3)


def test_compute_context_features_empty_ohlcv_returns_canonical_empty_frame() -> None:
    """No target dates yields an empty frame with all context columns."""
    fundamentals = pd.DataFrame(columns=["report_date", "availability_date", "raw_json"])
    ohlcv = _ohlcv_frame([])

    features = compute_context_features(fundamentals, ohlcv, _empty_macro_frame(), "AAPL")

    assert len(features) == 0
    assert list(features.columns) == ["date", "ticker", *CONTEXT_FEATURE_COLUMNS]


def test_compute_context_features_rejects_missing_macro_columns() -> None:
    """Macro archive validation still fails closed through context composition."""
    fundamentals = pd.DataFrame(columns=["report_date", "availability_date", "raw_json"])
    ohlcv = _ohlcv_frame(["2024-05-06"])
    macro = pd.DataFrame([{"series_id": "DGS10", "value": 4.5}])

    with pytest.raises(ValueError, match="observation_date"):
        compute_context_features(fundamentals, ohlcv, macro, "AAPL")


def test_compute_context_features_validates_macro_when_ohlcv_is_empty() -> None:
    """Malformed macro archives fail fast even when no context rows will emit."""
    fundamentals = pd.DataFrame(columns=["report_date", "availability_date", "raw_json"])
    ohlcv = _ohlcv_frame([])
    macro = pd.DataFrame([{"series_id": "DGS10", "value": 4.5}])

    with pytest.raises(ValueError, match="observation_date"):
        compute_context_features(fundamentals, ohlcv, macro, "AAPL")


def test_compute_context_features_handles_nan_macro_values() -> None:
    """Non-finite macro observations are ignored instead of becoming feature values."""
    fundamentals = pd.DataFrame(columns=["report_date", "availability_date", "raw_json"])
    ohlcv = _ohlcv_frame(["2024-05-06"])
    macro = pd.DataFrame(
        [
            _macro_row(
                series_id="DGS10",
                observation_date="2024-05-03",
                realtime_start="2024-05-03",
                value=float("nan"),
            )
        ]
    )

    features = compute_context_features(fundamentals, ohlcv, macro, "AAPL")

    assert pd.isna(features.loc[0, "treasury_10y"])


def test_context_features_to_records_converts_nan_to_none() -> None:
    """Context FeatureRecord conversion keeps schema-valid primitives only."""
    fundamentals = pd.DataFrame(columns=["report_date", "availability_date", "raw_json"])
    ohlcv = _ohlcv_frame(["2024-05-06"])
    features = compute_context_features(fundamentals, ohlcv, _empty_macro_frame(), "AAPL")

    records = context_features_to_records(features)

    assert len(records) == 1
    assert records[0].date == "2024-05-06"
    assert records[0].ticker == "AAPL"
    for column in (*FUNDAMENTAL_FEATURE_COLUMNS, *MACRO_FEATURE_COLUMNS):
        assert column in records[0].features
    assert records[0].features["net_profit_margin"] is None
    assert records[0].features["treasury_10y"] is None
