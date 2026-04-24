from __future__ import annotations

import json

import pandas as pd
import pytest

from core.features.fundamentals_features import (
    FUNDAMENTAL_FEATURE_COLUMNS,
    compute_fundamentals_features,
    fundamentals_features_to_records,
)


def _fundamentals_row(
    *,
    report_date: str,
    availability_date: str,
    fiscal_year: int,
    fiscal_period: str,
    financials: dict,
    earnings_date: str | None = None,
) -> dict:
    """Build one fundamentals-archive row matching the Layer 0 serialized shape."""
    return {
        "source": "simfin",
        "ticker": "AAPL",
        "report_date": report_date,
        "availability_date": availability_date,
        "retrieved_at": "2024-01-01T00:00:00",
        "fiscal_year": fiscal_year,
        "fiscal_period": fiscal_period,
        "statement": "pl",
        "earnings_date": earnings_date,
        "raw_json": json.dumps(financials, sort_keys=True, separators=(",", ":")),
    }


def _ohlcv_frame(dates: list[str], prices: list[float]) -> pd.DataFrame:
    """Build a minimal OHLCV frame with date and adj_close."""
    return pd.DataFrame({"date": dates, "adj_close": prices})


def test_empty_fundamentals_yield_all_none_features() -> None:
    """With no fundamentals history every feature resolves to None."""
    fundamentals = pd.DataFrame(columns=["report_date", "availability_date", "raw_json"])
    ohlcv = _ohlcv_frame(["2024-01-02", "2024-01-03"], [150.0, 151.0])

    features = compute_fundamentals_features(fundamentals, ohlcv, "AAPL")

    assert len(features) == 2
    for column in FUNDAMENTAL_FEATURE_COLUMNS:
        assert features[column].isna().all()


def test_filing_is_not_used_on_or_before_its_availability_date() -> None:
    """A filing with availability_date=T cannot be used on dates <= T."""
    fundamentals = pd.DataFrame(
        [
            _fundamentals_row(
                report_date="2024-03-31",
                availability_date="2024-05-03",
                fiscal_year=2024,
                fiscal_period="Q1",
                financials={
                    "revenue": 1_000.0,
                    "netIncome": 100.0,
                    "totalAssets": 5_000.0,
                    "totalLiabilities": 2_000.0,
                },
            )
        ]
    )
    ohlcv = _ohlcv_frame(
        ["2024-05-02", "2024-05-03", "2024-05-06"],
        [150.0, 151.0, 152.0],
    )

    features = compute_fundamentals_features(fundamentals, ohlcv, "AAPL")

    assert pd.isna(features.loc[0, "net_profit_margin"])
    assert pd.isna(features.loc[1, "net_profit_margin"])
    assert features.loc[2, "net_profit_margin"] == pytest.approx(0.1)


def test_forward_fill_across_multiple_filings() -> None:
    """Each target date uses the most recent filing with availability_date < date."""
    fundamentals = pd.DataFrame(
        [
            _fundamentals_row(
                report_date="2023-12-31",
                availability_date="2024-02-01",
                fiscal_year=2023,
                fiscal_period="Q4",
                financials={"revenue": 500.0, "netIncome": 50.0},
            ),
            _fundamentals_row(
                report_date="2024-03-31",
                availability_date="2024-05-03",
                fiscal_year=2024,
                fiscal_period="Q1",
                financials={"revenue": 1_000.0, "netIncome": 200.0},
            ),
        ]
    )
    ohlcv = _ohlcv_frame(
        ["2024-02-05", "2024-05-04"],
        [150.0, 151.0],
    )

    features = compute_fundamentals_features(fundamentals, ohlcv, "AAPL")

    assert features.loc[0, "net_profit_margin"] == pytest.approx(50 / 500)
    assert features.loc[1, "net_profit_margin"] == pytest.approx(200 / 1000)


def test_valuation_ratios_use_adjusted_close_when_shares_present() -> None:
    """PE, PB, and PS use adj_close when the filing supplies per-share inputs."""
    fundamentals = pd.DataFrame(
        [
            _fundamentals_row(
                report_date="2023-12-31",
                availability_date="2024-02-01",
                fiscal_year=2023,
                fiscal_period="Q4",
                financials={
                    "revenue": 1_000.0,
                    "netIncome": 200.0,
                    "totalAssets": 5_000.0,
                    "totalLiabilities": 2_000.0,
                    "sharesBasic": 100.0,
                    "eps": 2.0,
                },
            )
        ]
    )
    ohlcv = _ohlcv_frame(["2024-02-05"], [50.0])

    features = compute_fundamentals_features(fundamentals, ohlcv, "AAPL")

    assert features.loc[0, "pe_ratio"] == pytest.approx(25.0)
    assert features.loc[0, "pb_ratio"] == pytest.approx(50.0 / ((5000.0 - 2000.0) / 100.0))
    assert features.loc[0, "ps_ratio"] == pytest.approx(50.0 / (1000.0 / 100.0))


def test_revenue_growth_uses_prior_year_same_period() -> None:
    """YoY revenue growth compares the matched fiscal period one year back."""
    fundamentals = pd.DataFrame(
        [
            _fundamentals_row(
                report_date="2023-03-31",
                availability_date="2023-05-03",
                fiscal_year=2023,
                fiscal_period="Q1",
                financials={"revenue": 800.0},
            ),
            _fundamentals_row(
                report_date="2024-03-31",
                availability_date="2024-05-03",
                fiscal_year=2024,
                fiscal_period="Q1",
                financials={"revenue": 1_000.0},
            ),
        ]
    )
    ohlcv = _ohlcv_frame(["2024-05-06"], [150.0])

    features = compute_fundamentals_features(fundamentals, ohlcv, "AAPL")

    assert features.loc[0, "revenue_growth_yoy"] == pytest.approx(0.25)


def test_earnings_calendar_days_to_next_and_pre_post_flags() -> None:
    """days_to_next_earnings and the flag pair honor configured windows."""
    fundamentals = pd.DataFrame(
        [
            _fundamentals_row(
                report_date="2023-12-31",
                availability_date="2024-02-01",
                fiscal_year=2023,
                fiscal_period="Q4",
                financials={"revenue": 500.0, "netIncome": 50.0},
                earnings_date="2024-02-01",
            ),
            _fundamentals_row(
                report_date="2024-03-31",
                availability_date="2024-05-03",
                fiscal_year=2024,
                fiscal_period="Q1",
                financials={"revenue": 1000.0, "netIncome": 200.0},
                earnings_date="2024-05-03",
            ),
        ]
    )
    ohlcv = _ohlcv_frame(
        ["2024-02-02", "2024-04-30", "2024-05-02", "2024-05-04"],
        [100.0, 101.0, 102.0, 103.0],
    )

    features = compute_fundamentals_features(fundamentals, ohlcv, "AAPL")

    # Day after earnings on 2024-02-01
    assert features.loc[0, "days_to_next_earnings"] == 91
    assert features.loc[0, "post_earnings_flag"] == 1
    assert features.loc[0, "pre_earnings_flag"] == 0

    # Three trading days before 2024-05-03 earnings
    assert features.loc[1, "days_to_next_earnings"] == 3
    assert features.loc[1, "pre_earnings_flag"] == 1
    assert features.loc[1, "post_earnings_flag"] == 0

    # Day before earnings
    assert features.loc[2, "days_to_next_earnings"] == 1
    assert features.loc[2, "pre_earnings_flag"] == 1

    # Day after the 2024-05-03 earnings event
    assert features.loc[3, "post_earnings_flag"] == 1


def test_fundamentals_missing_optional_fields_emit_none_ratios() -> None:
    """Ratios whose inputs are missing resolve to None without raising."""
    fundamentals = pd.DataFrame(
        [
            _fundamentals_row(
                report_date="2023-12-31",
                availability_date="2024-02-01",
                fiscal_year=2023,
                fiscal_period="Q4",
                financials={"totalAssets": 1000.0},
            )
        ]
    )
    ohlcv = _ohlcv_frame(["2024-02-05"], [100.0])

    features = compute_fundamentals_features(fundamentals, ohlcv, "AAPL")

    assert pd.isna(features.loc[0, "pe_ratio"])
    assert pd.isna(features.loc[0, "net_profit_margin"])
    assert pd.isna(features.loc[0, "gross_margin"])
    assert pd.isna(features.loc[0, "debt_to_equity"])


def test_multiple_statement_rows_for_same_period_are_merged() -> None:
    """pl, bs, and cf rows for one period combine into a single set of ratios."""
    fundamentals = pd.DataFrame(
        [
            _fundamentals_row(
                report_date="2024-03-31",
                availability_date="2024-05-03",
                fiscal_year=2024,
                fiscal_period="Q1",
                financials={"revenue": 1_000.0, "netIncome": 200.0},
            ),
            _fundamentals_row(
                report_date="2024-03-31",
                availability_date="2024-05-03",
                fiscal_year=2024,
                fiscal_period="Q1",
                financials={"totalAssets": 5_000.0, "totalLiabilities": 2_000.0},
            ),
        ]
    )
    ohlcv = _ohlcv_frame(["2024-05-06"], [150.0])

    features = compute_fundamentals_features(fundamentals, ohlcv, "AAPL")

    assert features.loc[0, "return_on_assets"] == pytest.approx(200 / 5_000)
    assert features.loc[0, "return_on_equity"] == pytest.approx(200 / 3_000)


def test_fundamentals_features_to_records_coerces_nan_to_none() -> None:
    """The record converter returns validated FeatureRecord rows with None for NaN."""
    fundamentals = pd.DataFrame(columns=["report_date", "availability_date", "raw_json"])
    ohlcv = _ohlcv_frame(["2024-02-05"], [100.0])

    features = compute_fundamentals_features(fundamentals, ohlcv, "AAPL")
    records = fundamentals_features_to_records(features)

    assert len(records) == 1
    assert records[0].ticker == "AAPL"
    assert records[0].date == "2024-02-05"
    for column in FUNDAMENTAL_FEATURE_COLUMNS:
        assert records[0].features[column] is None


def test_rejects_missing_required_columns() -> None:
    """Fundamentals frame lacking mandatory columns raises ValueError."""
    fundamentals = pd.DataFrame([{"report_date": "2024-03-31"}])
    ohlcv = _ohlcv_frame(["2024-05-06"], [150.0])

    with pytest.raises(ValueError, match="availability_date"):
        compute_fundamentals_features(fundamentals, ohlcv, "AAPL")
