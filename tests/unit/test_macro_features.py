from __future__ import annotations

import pandas as pd
import pytest

from core.features.macro_features import (
    MACRO_FEATURE_COLUMNS,
    compute_macro_features,
    macro_features_to_records,
)


def _macro_row(
    *,
    series_id: str,
    observation_date: str,
    realtime_start: str,
    value: float | None,
) -> dict[str, object]:
    """Build one normalized FRED macro archive row."""
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


def test_compute_macro_features_returns_columns_and_shape() -> None:
    """Feature frame contains canonical macro columns and one row per target date."""
    macro = pd.DataFrame(
        [
            _macro_row(
                series_id="FEDFUNDS",
                observation_date="2024-01-01",
                realtime_start="2024-01-02",
                value=5.33,
            )
        ]
    )

    features = compute_macro_features(macro, ["2024-01-03", "2024-01-04"])

    assert list(features.columns) == ["date", *MACRO_FEATURE_COLUMNS]
    assert features["date"].tolist() == ["2024-01-03", "2024-01-04"]
    assert features.loc[0, "fed_funds_rate"] == pytest.approx(5.33)


def test_compute_macro_features_empty_targets_return_empty_canonical_frame() -> None:
    """No target dates yields an empty frame with canonical columns."""
    macro = pd.DataFrame(
        columns=["series_id", "observation_date", "realtime_start", "value", "is_missing"]
    )

    features = compute_macro_features(macro, [])

    assert len(features) == 0
    assert list(features.columns) == ["date", *MACRO_FEATURE_COLUMNS]


def test_compute_macro_features_rejects_missing_columns() -> None:
    """Missing required FRED archive columns raise ValueError."""
    macro = pd.DataFrame([{"series_id": "DGS10", "value": 4.0}])

    with pytest.raises(ValueError, match="observation_date"):
        compute_macro_features(macro, ["2024-01-03"])


def test_daily_series_forward_fills_across_weekend_without_same_day_leakage() -> None:
    """Daily macro levels forward-fill from the last value known before the target date."""
    macro = pd.DataFrame(
        [
            _macro_row(
                series_id="DGS10",
                observation_date="2024-01-05",
                realtime_start="2024-01-05",
                value=4.0,
            ),
            _macro_row(
                series_id="DGS10",
                observation_date="2024-01-08",
                realtime_start="2024-01-08",
                value=4.1,
            ),
        ]
    )

    features = compute_macro_features(
        macro,
        ["2024-01-05", "2024-01-06", "2024-01-08", "2024-01-09"],
    )

    assert pd.isna(features.loc[0, "treasury_10y"])
    assert features.loc[1, "treasury_10y"] == pytest.approx(4.0)
    assert features.loc[2, "treasury_10y"] == pytest.approx(4.0)
    assert features.loc[3, "treasury_10y"] == pytest.approx(4.1)


def test_timestamp_targets_do_not_allow_same_day_leakage() -> None:
    """Datetime-like targets normalize to dates before leakage comparisons."""
    macro = pd.DataFrame(
        [
            _macro_row(
                series_id="DGS10",
                observation_date="2024-04-10",
                realtime_start="2024-04-10",
                value=4.42,
            )
        ]
    )

    features = compute_macro_features(
        macro,
        [pd.Timestamp("2024-04-10 00:00:00"), pd.Timestamp("2024-04-11 00:00:00")],
    )

    assert features["date"].tolist() == ["2024-04-10", "2024-04-11"]
    assert pd.isna(features.loc[0, "treasury_10y"])
    assert features.loc[1, "treasury_10y"] == pytest.approx(4.42)


def test_compute_macro_features_rejects_invalid_target_dates() -> None:
    """Invalid target dates fail closed instead of entering string comparisons."""
    macro = pd.DataFrame(
        [
            _macro_row(
                series_id="DGS10",
                observation_date="2024-04-10",
                realtime_start="2024-04-10",
                value=4.42,
            )
        ]
    )

    with pytest.raises(ValueError, match="target_dates"):
        compute_macro_features(macro, ["not-a-date"])


def test_lagged_monthly_series_uses_publication_date_not_observation_month() -> None:
    """Lagged releases appear only after their realtime_start date."""
    macro = pd.DataFrame(
        [
            _macro_row(
                series_id="CPIAUCSL",
                observation_date="2024-02-01",
                realtime_start="2024-03-12",
                value=310.0,
            ),
            _macro_row(
                series_id="CPIAUCSL",
                observation_date="2024-03-01",
                realtime_start="2024-04-10",
                value=312.0,
            ),
        ]
    )

    features = compute_macro_features(macro, ["2024-04-10", "2024-04-11"])

    assert features.loc[0, "cpi_level"] == pytest.approx(310.0)
    assert features.loc[1, "cpi_level"] == pytest.approx(312.0)


def test_revision_to_older_observation_does_not_displace_latest_level() -> None:
    """Later vintage updates to older observations do not replace newer known levels."""
    macro = pd.DataFrame(
        [
            _macro_row(
                series_id="DGS10",
                observation_date="2024-01-05",
                realtime_start="2024-01-05",
                value=4.0,
            ),
            _macro_row(
                series_id="DGS10",
                observation_date="2024-01-08",
                realtime_start="2024-01-08",
                value=4.1,
            ),
            _macro_row(
                series_id="DGS10",
                observation_date="2024-01-05",
                realtime_start="2024-01-10",
                value=4.05,
            ),
        ]
    )

    features = compute_macro_features(macro, ["2024-01-11"])

    assert features.loc[0, "treasury_10y"] == pytest.approx(4.1)


def test_macro_features_to_records_broadcasts_ticker_and_coerces_nan() -> None:
    """FeatureRecord conversion stamps the ticker and converts NaN values to None."""
    macro = pd.DataFrame(
        [
            _macro_row(
                series_id="FEDFUNDS",
                observation_date="2024-01-01",
                realtime_start="2024-01-02",
                value=5.33,
            )
        ]
    )
    features = compute_macro_features(macro, ["2024-01-03"])

    records = macro_features_to_records(features, ticker="AAPL")

    assert len(records) == 1
    assert records[0].date == "2024-01-03"
    assert records[0].ticker == "AAPL"
    assert records[0].features["fed_funds_rate"] == pytest.approx(5.33)
    assert records[0].features["treasury_10y"] is None
