from __future__ import annotations

from datetime import UTC, datetime

from core.contracts.schemas import OHLCVRecord


def historical_adjusted_provenance() -> dict[str, object]:
    """Return the canonical historical Alpaca adjustment policy fixture."""
    return {
        "policy_id": "alpaca_historical_1day_adjustment_all",
        "provider": "alpaca",
        "endpoint": "/v2/stocks/bars",
        "timeframe": "1Day",
        "feed": "sip",
        "request_adjustment": "all",
        "stored_ohlc_basis": "provider_adjusted",
        "normalized_adj_close_policy": "copy_close_to_adj_close",
        "corporate_actions_reflected": ["splits", "dividends"],
    }


def daily_raw_provenance(*, feed: str = "iex") -> dict[str, object]:
    """Return the canonical daily incremental Alpaca adjustment policy fixture."""
    return {
        "policy_id": "alpaca_live_1day_adjustment_raw",
        "provider": "alpaca",
        "endpoint": "/v2/stocks/bars",
        "timeframe": "1Day",
        "feed": feed,
        "request_adjustment": "raw",
        "stored_ohlc_basis": "raw",
        "normalized_adj_close_policy": "copy_close_to_adj_close",
        "corporate_actions_reflected": [],
    }


def build_provenance_report(
    *,
    run_id: str,
    mode: str,
    provenance: dict[str, object],
    observed_rows: int,
    split_like_discontinuity_count: int = 0,
) -> dict[str, object]:
    """Build a minimal Layer 0 OHLCV provenance report payload."""
    return {
        "run_id": run_id,
        "mode": mode,
        "generated_at": datetime.now(UTC).isoformat(),
        "price_adjustment_provenance": provenance,
        "archive_summary": {
            "archive_keys": ["raw/prices/AAPL.parquet"],
            "written_keys": ["raw/prices/AAPL.parquet"],
            "observed_rows": observed_rows,
            "close_equals_adj_close_rows": observed_rows,
            "close_diff_adj_close_rows": 0,
            "split_like_discontinuity_count": split_like_discontinuity_count,
            "split_like_discontinuity_samples": (
                [
                    {
                        "ticker": "AAPL",
                        "previous_date": "2024-01-02",
                        "current_date": "2024-01-03",
                        "price_ratio": 2.0,
                    }
                ]
                if split_like_discontinuity_count
                else []
            ),
        },
    }


def build_split_like_history(ticker: str) -> list[OHLCVRecord]:
    """Return a synthetic OHLCV history with one split-like adjacent close jump."""
    return [
        OHLCVRecord(
            date="2024-01-02",
            ticker=ticker,
            open=100.0,
            high=101.0,
            low=99.0,
            close=100.0,
            volume=1000,
            adj_close=100.0,
            dollar_volume=100_000.0,
        ),
        OHLCVRecord(
            date="2024-01-03",
            ticker=ticker,
            open=50.0,
            high=51.0,
            low=49.0,
            close=50.0,
            volume=1000,
            adj_close=50.0,
            dollar_volume=50_000.0,
        ),
    ]
