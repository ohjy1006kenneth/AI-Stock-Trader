from __future__ import annotations

from typing import Any

import pytest

from services.alpaca.market_data import AlpacaMarketDataConfig
from services.alpaca.ohlcv_fetcher import (
    AlpacaHistoricalOHLCVFetcher,
    AlpacaTickerSecurity,
    AlpacaTickerSecurityMaster,
)


class _FakeResponse:
    def __init__(self, payload: Any) -> None:
        self._payload = payload

    def json(self) -> Any:
        return self._payload

    def raise_for_status(self) -> None:
        return None


class _FakeSession:
    def __init__(self, responses: list[_FakeResponse]) -> None:
        self.responses = responses
        self.calls: list[dict[str, Any]] = []

    def get(self, url: str, **kwargs: Any) -> _FakeResponse:
        self.calls.append({"url": url, **kwargs})
        if not self.responses:
            raise AssertionError("unexpected Alpaca HTTP call")
        return self.responses.pop(0)


def _payload(*, next_page_token: str | None = None) -> dict[str, Any]:
    return {
        "bars": {
            "AAPL": [
                {
                    "t": "2024-01-02T05:00:00Z",
                    "o": 185.64,
                    "h": 188.44,
                    "l": 183.89,
                    "c": 186.86,
                    "v": 82488700,
                    "vw": 186.12,
                }
            ]
        },
        "next_page_token": next_page_token,
    }


def _client(session: _FakeSession) -> AlpacaHistoricalOHLCVFetcher:
    return AlpacaHistoricalOHLCVFetcher(
        AlpacaMarketDataConfig(
            api_key_id="test-key",
            api_secret_key="test-secret",
            base_url="https://example.alpaca.test",
            retry_sleep_seconds=0,
        ),
        session=session,  # type: ignore[arg-type]
        min_request_interval_seconds=0,
    )


def test_fetch_price_page_calls_alpaca_sip_adjusted_endpoint() -> None:
    """Historical fetcher requests split/dividend-adjusted daily SIP bars."""
    session = _FakeSession([_FakeResponse(_payload())])
    fetcher = _client(session)

    records, next_page_token = fetcher.fetch_price_page(
        ticker="aapl",
        from_date="2024-01-02",
        to_date="2024-01-03",
    )

    assert [record.ticker for record in records] == ["AAPL"]
    assert next_page_token is None
    assert session.calls == [
        {
            "url": "https://example.alpaca.test/v2/stocks/bars",
            "headers": {
                "APCA-API-KEY-ID": "test-key",
                "APCA-API-SECRET-KEY": "test-secret",
            },
            "params": {
                "symbols": "AAPL",
                "timeframe": "1Day",
                "start": "2024-01-02",
                "end": "2024-01-03",
                "adjustment": "all",
                "feed": "sip",
                "currency": "USD",
                "limit": 10000,
                "sort": "asc",
            },
            "timeout": 30,
        }
    ]


def test_fetch_records_paginates_and_rejects_duplicates() -> None:
    """Historical fetching follows page tokens and fails on duplicate ticker-date bars."""
    first_page = _FakeResponse(_payload(next_page_token="page-2"))
    second_page = _FakeResponse(_payload())
    session = _FakeSession([first_page, second_page])
    fetcher = _client(session)

    with pytest.raises(ValueError, match="Duplicate Alpaca historical bar"):
        fetcher.fetch_records("AAPL", "2024-01-02", "2024-01-03")

    assert session.calls[1]["params"]["page_token"] == "page-2"


def test_fetch_security_records_uses_resolved_ticker_identity() -> None:
    """Security-level fetching uses the resolved ticker from the security master."""
    session = _FakeSession([_FakeResponse(_payload())])
    fetcher = _client(session)
    security = AlpacaTickerSecurity(ticker="aapl")

    records = fetcher.fetch_security_records(security, "2024-01-02", "2024-01-02")

    assert [record.ticker for record in records] == ["AAPL"]
    assert session.calls[0]["params"]["symbols"] == "AAPL"


def test_ticker_security_master_returns_ticker_keyed_reference_rows() -> None:
    """Alpaca security master keys archives by normalized ticker symbol."""
    master = AlpacaTickerSecurityMaster()

    security = master.resolve_all("brk.b")[0]

    assert security.ticker == "BRK-B"
    assert security.security_id == "BRK-B"
    assert security.to_reference_row() == {
        "ticker": "BRK-B",
        "security_id": "BRK-B",
        "source": "alpaca_sip",
        "start_date": None,
        "end_date": None,
    }


def test_fetch_price_page_rejects_bad_date_window() -> None:
    """Historical requests fail fast when the date window is inverted."""
    fetcher = _client(_FakeSession([]))

    with pytest.raises(ValueError, match="from_date"):
        fetcher.fetch_price_page(
            ticker="AAPL",
            from_date="2024-01-03",
            to_date="2024-01-02",
        )


def test_fetch_price_page_rejects_non_object_payload() -> None:
    """Malformed Alpaca historical responses fail before normalization."""
    fetcher = _client(_FakeSession([_FakeResponse([])]))

    with pytest.raises(ValueError, match="JSON object"):
        fetcher.fetch_price_page(ticker="AAPL", from_date="2024-01-02", to_date="2024-01-02")


def test_fetch_price_page_rejects_nan_values() -> None:
    """NaN vendor values fail contract validation instead of entering storage."""
    payload = _payload()
    payload["bars"]["AAPL"][0]["h"] = float("nan")
    fetcher = _client(_FakeSession([_FakeResponse(payload)]))

    with pytest.raises(ValueError, match="finite"):
        fetcher.fetch_price_page(ticker="AAPL", from_date="2024-01-02", to_date="2024-01-02")
