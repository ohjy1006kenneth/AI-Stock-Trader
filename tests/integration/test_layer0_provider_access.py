from __future__ import annotations

import os
import re
from collections.abc import Callable
from pathlib import Path
from typing import TypeVar

import pytest
from dotenv import load_dotenv

from services.alpaca.market_data import AlpacaMarketDataClient, AlpacaMarketDataConfig
from services.fred.macro_fetcher import FredClientConfig, FredMacroFetcher
from services.simfin.fundamentals_fetcher import SimFinClientConfig, SimFinFundamentalsFetcher
from services.tiingo.news_fetcher import TiingoNewsFetcher
from services.tiingo.ohlcv_fetcher import TiingoClientConfig, TiingoOHLCVFetcher

_REPO_ROOT = Path(__file__).resolve().parents[2]
_SECRET_QUERY_PATTERN = re.compile(
    r"(?P<prefix>[?&](?:token|api-key|api_key|apikey|apiKey)=)(?P<secret>[^&\s]+)",
    flags=re.IGNORECASE,
)
T = TypeVar("T")


def _load_local_env(name: str) -> None:
    """Load one local provider env file when present without overriding shell values."""
    load_dotenv(_REPO_ROOT / "config" / name, override=False)


def _run_live_check(name: str, callback: Callable[[], T]) -> T:
    """Run a live provider check while redacting credential-bearing error URLs."""
    try:
        return callback()
    except Exception as exc:  # pragma: no cover - exercised only during live failures
        message = _SECRET_QUERY_PATTERN.sub(r"\g<prefix><redacted>", str(exc))
        raise AssertionError(f"{name} live check failed: {type(exc).__name__}: {message}") from None


@pytest.mark.skipif(
    os.getenv("RUN_TIINGO_INTEGRATION") != "1",
    reason="Set RUN_TIINGO_INTEGRATION=1 to run live Tiingo checks.",
)
def test_tiingo_live_price_access() -> None:
    """Verify the configured Tiingo token can access EOD prices."""
    _load_local_env("tiingo.env")
    rows = _run_live_check(
        "Tiingo OHLCV",
        lambda: TiingoOHLCVFetcher(TiingoClientConfig.from_env()).fetch_price_rows(
            "AAPL", "2024-01-02", "2024-01-02"
        ),
    )

    assert rows


@pytest.mark.skipif(
    os.getenv("RUN_TIINGO_INTEGRATION") != "1",
    reason="Set RUN_TIINGO_INTEGRATION=1 to run live Tiingo checks.",
)
def test_tiingo_live_news_access() -> None:
    """Verify the configured Tiingo token can access raw news."""
    _load_local_env("tiingo.env")
    rows = _run_live_check(
        "Tiingo News",
        lambda: TiingoNewsFetcher(TiingoClientConfig.from_env()).fetch_news_day(
            tickers=["AAPL"], as_of_date="2024-01-02", limit=1
        ),
    )

    assert isinstance(rows, list)


@pytest.mark.skipif(
    os.getenv("RUN_SIMFIN_INTEGRATION") != "1",
    reason="Set RUN_SIMFIN_INTEGRATION=1 to run live SimFin checks.",
)
def test_simfin_live_access() -> None:
    """Verify the configured SimFin key can access as-reported statements."""
    _load_local_env("simfin.env")
    page = _run_live_check(
        "SimFin",
        lambda: SimFinFundamentalsFetcher(SimFinClientConfig.from_env()).fetch_statement_rows(
            tickers=["AAPL"],
            start_date="2024-01-01",
            end_date="2024-03-31",
            limit=1,
        ),
    )

    assert isinstance(page.rows, list)


@pytest.mark.skipif(
    os.getenv("RUN_FRED_INTEGRATION") != "1",
    reason="Set RUN_FRED_INTEGRATION=1 to run live FRED checks.",
)
def test_fred_live_access() -> None:
    """Verify the configured FRED key can access configured macro observations."""
    _load_local_env("fred.env")
    page = _run_live_check(
        "FRED",
        lambda: FredMacroFetcher(FredClientConfig.from_env()).fetch_series_page(
            series_id="DGS10",
            start_date="2024-01-01",
            end_date="2024-01-10",
            realtime_start="2024-01-01",
            realtime_end="2024-01-10",
            limit=10,
        ),
    )

    assert page.rows


@pytest.mark.skipif(
    os.getenv("RUN_ALPACA_INTEGRATION") != "1",
    reason="Set RUN_ALPACA_INTEGRATION=1 to run live Alpaca market-data checks.",
)
def test_alpaca_live_market_data_access() -> None:
    """Verify the configured Alpaca key can fetch normalized daily bars."""
    _load_local_env("alpaca.env")
    records = _run_live_check(
        "Alpaca market data",
        lambda: AlpacaMarketDataClient(AlpacaMarketDataConfig.from_env()).fetch_live_daily_bars(
            tickers=["AAPL"], as_of_date="2024-01-02", max_pages=1
        ),
    )

    assert records
