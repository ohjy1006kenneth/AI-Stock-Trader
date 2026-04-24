"""Layer 1 fundamentals context features from the SimFin raw archive.

Features are forward-filled per ticker from the latest filing whose
`availability_date` is strictly before each target date — the point-in-time
convention that prevents a filing released on 2024-03-15 from appearing in
feature rows for dates 2024-03-14 or earlier.

Earnings-calendar features use the `earnings_date` column emitted by the
Layer 0 SimFin normalizer. Ratios that require price data (PE, PB, PS) read
adjusted closing prices from the supplied OHLCV frame; any ratio whose inputs
are missing on a given date resolves to `None` in the output record.
"""
from __future__ import annotations

import importlib
import json
import math
from collections.abc import Iterable, Mapping
from typing import TYPE_CHECKING, Any

from core.contracts.schemas import FeatureRecord

if TYPE_CHECKING:
    import pandas as pd

FUNDAMENTAL_FEATURE_COLUMNS: tuple[str, ...] = (
    "pe_ratio",
    "pb_ratio",
    "ps_ratio",
    "net_profit_margin",
    "return_on_equity",
    "return_on_assets",
    "gross_margin",
    "debt_to_equity",
    "interest_coverage",
    "revenue_growth_yoy",
    "earnings_surprise",
    "days_to_next_earnings",
    "pre_earnings_flag",
    "post_earnings_flag",
)

# Configurable earnings-calendar thresholds (business-day approximation).
PRE_EARNINGS_WINDOW_DAYS = 5
POST_EARNINGS_WINDOW_DAYS = 2

_REVENUE_KEYS = ("revenue", "Revenue", "sales", "Sales")
_NET_INCOME_KEYS = ("netIncome", "NetIncome", "net_income", "Net Income")
_GROSS_PROFIT_KEYS = ("grossProfit", "GrossProfit", "gross_profit", "Gross Profit")
_EPS_KEYS = ("eps", "EPS", "earningsPerShare", "Earnings Per Share", "epsBasic", "epsDiluted")
_EPS_ESTIMATE_KEYS = ("epsEstimate", "eps_estimate", "estimatedEps", "consensusEps")
_ASSETS_KEYS = ("totalAssets", "TotalAssets", "total_assets", "Total Assets")
_LIABILITIES_KEYS = (
    "totalLiabilities",
    "TotalLiabilities",
    "total_liabilities",
    "Total Liabilities",
)
_EQUITY_KEYS = (
    "totalEquity",
    "TotalEquity",
    "total_equity",
    "shareholdersEquity",
    "stockholdersEquity",
)
_DEBT_KEYS = ("totalDebt", "total_debt", "longTermDebt", "long_term_debt")
_EBIT_KEYS = ("ebit", "EBIT", "operatingIncome", "operating_income")
_INTEREST_EXPENSE_KEYS = ("interestExpense", "interest_expense", "interestExpenseNet")
_SHARES_KEYS = (
    "sharesBasic",
    "sharesDiluted",
    "commonSharesOutstanding",
    "shares_outstanding",
    "shares",
)
_BOOK_VALUE_PS_KEYS = ("bookValuePerShare", "book_value_per_share")


def compute_fundamentals_features(
    fundamentals: pd.DataFrame,
    ohlcv: pd.DataFrame,
    ticker: str,
) -> pd.DataFrame:
    """Return per-date fundamentals context features for one ticker.

    Args:
        fundamentals: Raw SimFin archive rows for this ticker as written by
            Layer 0, including `report_date`, `availability_date`, `raw_json`,
            `earnings_date`, and `fiscal_year` / `fiscal_period` columns.
        ohlcv: Adjusted OHLCV frame matching the OHLCVRecord contract. Only the
            `date` and `adj_close` columns are consulted, but the full contract
            is required for downstream feature alignment.
        ticker: Ticker symbol stamped on every output row.

    Returns:
        DataFrame with columns (`date`, `ticker`, *FUNDAMENTAL_FEATURE_COLUMNS*)
        and one row per trading day in the OHLCV frame. A row's feature values
        depend only on filings with `availability_date < row.date`.
    """
    pd = _require_pandas()

    if "date" not in ohlcv.columns or "adj_close" not in ohlcv.columns:
        raise ValueError("OHLCV frame must include date and adj_close columns")

    target_dates = (
        ohlcv[["date", "adj_close"]].sort_values("date").drop_duplicates("date").reset_index(drop=True)
    )

    if len(target_dates) == 0:
        return _empty_frame(pd)

    fiscal_periods = _collect_fiscal_periods(fundamentals)
    earnings_dates = _collect_earnings_dates(fundamentals)

    rows: list[dict[str, Any]] = []
    for _, row in target_dates.iterrows():
        as_of_date = row["date"]
        adj_close = _to_float(row.get("adj_close"))

        latest = _latest_period_before(fiscal_periods, as_of_date)
        prior_year = _prior_year_period(fiscal_periods, latest) if latest else None
        base_features = _ratios_from_period(latest, prior_year, adj_close)
        calendar_features = _earnings_calendar_features(earnings_dates, as_of_date)

        features = {**base_features, **calendar_features}
        features_row = {"date": as_of_date, "ticker": ticker}
        features_row.update({column: features.get(column) for column in FUNDAMENTAL_FEATURE_COLUMNS})
        rows.append(features_row)

    return pd.DataFrame(rows)[["date", "ticker", *FUNDAMENTAL_FEATURE_COLUMNS]]


def fundamentals_features_to_records(features: pd.DataFrame) -> list[FeatureRecord]:
    """Convert a fundamentals-features frame into FeatureRecord instances."""
    records: list[FeatureRecord] = []
    for row in features.to_dict(orient="records"):
        feature_values = {
            name: _normalize_feature_value(row.get(name)) for name in FUNDAMENTAL_FEATURE_COLUMNS
        }
        records.append(
            FeatureRecord(
                date=str(row["date"]),
                ticker=str(row["ticker"]),
                features=feature_values,
            )
        )
    return records


def _collect_fiscal_periods(fundamentals: pd.DataFrame) -> list[dict[str, Any]]:
    """Merge SimFin statement rows into per-fiscal-period fundamentals dictionaries."""
    if len(fundamentals) == 0:
        return []

    required = {"report_date", "availability_date", "raw_json"}
    missing = sorted(required - set(fundamentals.columns))
    if missing:
        raise ValueError(f"Fundamentals frame missing required columns: {missing}")

    grouped: dict[tuple[int | None, str | None, str | None], dict[str, Any]] = {}
    for _, row in fundamentals.iterrows():
        report_date = _string_or_none(row.get("report_date"))
        availability_date = _string_or_none(row.get("availability_date"))
        if availability_date is None:
            continue
        fiscal_year = _int_or_none(row.get("fiscal_year"))
        fiscal_period = _string_or_none(row.get("fiscal_period"))
        key = (fiscal_year, fiscal_period, report_date)
        bucket = grouped.setdefault(
            key,
            {
                "fiscal_year": fiscal_year,
                "fiscal_period": fiscal_period,
                "report_date": report_date,
                "availability_date": availability_date,
                "financials": {},
            },
        )
        if availability_date > bucket["availability_date"]:
            bucket["availability_date"] = availability_date
        raw = _decode_raw_json(row.get("raw_json"))
        if raw:
            bucket["financials"].update(raw)

    return sorted(grouped.values(), key=lambda item: item["availability_date"])


def _collect_earnings_dates(fundamentals: pd.DataFrame) -> list[str]:
    """Return sorted unique earnings dates present in the archive."""
    if len(fundamentals) == 0 or "earnings_date" not in fundamentals.columns:
        return []
    unique = {
        cleaned
        for cleaned in (_string_or_none(value) for value in fundamentals["earnings_date"].tolist())
        if cleaned is not None
    }
    return sorted(unique)


def _latest_period_before(
    fiscal_periods: Iterable[Mapping[str, Any]],
    as_of_date: str,
) -> Mapping[str, Any] | None:
    """Return the fiscal-period bucket with the greatest availability_date < as_of_date."""
    latest: Mapping[str, Any] | None = None
    for period in fiscal_periods:
        if period["availability_date"] < as_of_date:
            latest = period
        else:
            break
    return latest


def _prior_year_period(
    fiscal_periods: Iterable[Mapping[str, Any]],
    latest: Mapping[str, Any] | None,
) -> Mapping[str, Any] | None:
    """Return the fiscal period one year before the latest one, when available."""
    if latest is None:
        return None
    target_year = latest.get("fiscal_year")
    target_period = latest.get("fiscal_period")
    target_report = latest.get("report_date")
    if target_year is None:
        return None

    for period in fiscal_periods:
        if period.get("fiscal_year") == target_year - 1 and period.get("fiscal_period") == target_period:
            if period.get("report_date") and target_report and period["report_date"] < target_report:
                return period
            return period
    return None


def _ratios_from_period(
    latest: Mapping[str, Any] | None,
    prior: Mapping[str, Any] | None,
    adj_close: float | None,
) -> dict[str, float | int | bool | None]:
    """Compute valuation/profitability/leverage ratios from a merged fiscal period."""
    blank: dict[str, float | int | bool | None] = {
        name: None for name in FUNDAMENTAL_FEATURE_COLUMNS
    }
    if latest is None:
        return blank

    financials = latest.get("financials", {})
    revenue = _read_numeric(financials, _REVENUE_KEYS)
    net_income = _read_numeric(financials, _NET_INCOME_KEYS)
    gross_profit = _read_numeric(financials, _GROSS_PROFIT_KEYS)
    eps = _read_numeric(financials, _EPS_KEYS)
    eps_estimate = _read_numeric(financials, _EPS_ESTIMATE_KEYS)
    total_assets = _read_numeric(financials, _ASSETS_KEYS)
    total_liabilities = _read_numeric(financials, _LIABILITIES_KEYS)
    total_equity = _read_numeric(financials, _EQUITY_KEYS)
    if total_equity is None and total_assets is not None and total_liabilities is not None:
        total_equity = total_assets - total_liabilities
    total_debt = _read_numeric(financials, _DEBT_KEYS)
    ebit = _read_numeric(financials, _EBIT_KEYS)
    interest_expense = _read_numeric(financials, _INTEREST_EXPENSE_KEYS)
    shares = _read_numeric(financials, _SHARES_KEYS)
    book_value_ps = _read_numeric(financials, _BOOK_VALUE_PS_KEYS)

    blank["pe_ratio"] = _safe_divide(adj_close, eps)
    book_value_per_share = book_value_ps or _safe_divide(total_equity, shares)
    blank["pb_ratio"] = _safe_divide(adj_close, book_value_per_share)
    revenue_per_share = _safe_divide(revenue, shares)
    blank["ps_ratio"] = _safe_divide(adj_close, revenue_per_share)

    blank["net_profit_margin"] = _safe_divide(net_income, revenue)
    blank["return_on_equity"] = _safe_divide(net_income, total_equity)
    blank["return_on_assets"] = _safe_divide(net_income, total_assets)
    blank["gross_margin"] = _safe_divide(gross_profit, revenue)

    debt_numerator = total_debt if total_debt is not None else total_liabilities
    blank["debt_to_equity"] = _safe_divide(debt_numerator, total_equity)
    blank["interest_coverage"] = _safe_divide(ebit, interest_expense)

    prior_revenue: float | None = None
    if prior is not None:
        prior_revenue = _read_numeric(prior.get("financials", {}), _REVENUE_KEYS)
    if revenue is not None and prior_revenue is not None and prior_revenue != 0:
        blank["revenue_growth_yoy"] = (revenue - prior_revenue) / abs(prior_revenue)

    if eps is not None and eps_estimate is not None and eps_estimate != 0:
        blank["earnings_surprise"] = (eps - eps_estimate) / abs(eps_estimate)

    return blank


def _earnings_calendar_features(
    earnings_dates: list[str],
    as_of_date: str,
) -> dict[str, float | int | bool | None]:
    """Compute days-to-earnings and surrounding flags relative to `as_of_date`."""
    result: dict[str, float | int | bool | None] = {
        "days_to_next_earnings": None,
        "pre_earnings_flag": None,
        "post_earnings_flag": None,
    }
    if not earnings_dates:
        return result

    next_date: str | None = None
    last_passed: str | None = None
    for candidate in earnings_dates:
        if candidate >= as_of_date and next_date is None:
            next_date = candidate
        if candidate < as_of_date:
            last_passed = candidate

    if next_date is not None:
        delta = _calendar_day_delta(as_of_date, next_date)
        result["days_to_next_earnings"] = delta
        result["pre_earnings_flag"] = 1 if delta <= PRE_EARNINGS_WINDOW_DAYS else 0

    if last_passed is not None:
        delta_since = _calendar_day_delta(last_passed, as_of_date)
        result["post_earnings_flag"] = 1 if delta_since <= POST_EARNINGS_WINDOW_DAYS else 0

    return result


def _calendar_day_delta(start_iso: str, end_iso: str) -> int:
    """Return the inclusive calendar-day delta between two YYYY-MM-DD strings."""
    from datetime import date as Date

    start = Date.fromisoformat(start_iso)
    end = Date.fromisoformat(end_iso)
    return (end - start).days


def _decode_raw_json(raw_json: Any) -> dict[str, Any] | None:
    """Parse the archived `raw_json` string into a dictionary."""
    if raw_json is None:
        return None
    if isinstance(raw_json, Mapping):
        return dict(raw_json)
    if not isinstance(raw_json, str):
        return None
    raw_json = raw_json.strip()
    if not raw_json:
        return None
    try:
        parsed = json.loads(raw_json)
    except json.JSONDecodeError:
        return None
    return parsed if isinstance(parsed, dict) else None


def _read_numeric(financials: Mapping[str, Any], keys: tuple[str, ...]) -> float | None:
    """Return the first numeric value present under any of the candidate keys."""
    for key in keys:
        if key in financials:
            value = _to_float(financials[key])
            if value is not None:
                return value
    return None


def _safe_divide(numerator: float | None, denominator: float | None) -> float | None:
    """Return numerator/denominator when both are finite and the denominator is non-zero."""
    if numerator is None or denominator is None or denominator == 0:
        return None
    result = numerator / denominator
    if math.isnan(result) or math.isinf(result):
        return None
    return result


def _to_float(value: Any) -> float | None:
    """Coerce a scalar to a finite float or return None."""
    if value is None:
        return None
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(numeric) or math.isinf(numeric):
        return None
    return numeric


def _string_or_none(value: Any) -> str | None:
    """Coerce a scalar to a string or return None when missing/NaN."""
    if value is None:
        return None
    try:
        if isinstance(value, float) and math.isnan(value):
            return None
    except TypeError:
        pass
    text = str(value).strip()
    return text or None


def _int_or_none(value: Any) -> int | None:
    """Coerce a scalar to an int or return None when not representable."""
    numeric = _to_float(value)
    if numeric is None:
        return None
    if not numeric.is_integer():
        return None
    return int(numeric)


def _empty_frame(pd: Any) -> pd.DataFrame:
    """Return an empty feature frame with canonical columns."""
    return pd.DataFrame(columns=["date", "ticker", *FUNDAMENTAL_FEATURE_COLUMNS])


def _normalize_feature_value(value: Any) -> float | int | bool | None:
    """Convert a pandas/numpy scalar to a FeatureRecord-compatible primitive."""
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(numeric) or math.isinf(numeric):
        return None
    if numeric.is_integer() and isinstance(value, (int,)) and not isinstance(value, bool):
        return int(numeric)
    return numeric


def _require_pandas() -> Any:
    """Import pandas/pyarrow lazily with a clear error when absent."""
    try:
        import pandas as pd

        importlib.import_module("pyarrow")
    except ModuleNotFoundError as exc:
        raise ModuleNotFoundError(
            "pandas and pyarrow are required for fundamentals feature computation."
        ) from exc
    return pd
