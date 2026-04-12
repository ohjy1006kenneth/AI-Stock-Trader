from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

import services.alpaca.market_data as market_data_module
from services.alpaca.market_data import (
    AlpacaMarketDataClient,
    AlpacaMarketDataConfig,
    normalize_alpaca_bar_response,
)

FIXTURE_PATH = Path("data/sample/alpaca_market_bar_response.json")


@pytest.fixture(autouse=True)
def no_local_alpaca_env_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep unit tests isolated from a developer's local config/alpaca.env."""
    monkeypatch.setattr(
        market_data_module,
        "ALPACA_ENV_FILE",
        tmp_path / "does-not-exist-alpaca.env",
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


def _fixture_payload() -> dict[str, Any]:
    return json.loads(FIXTURE_PATH.read_text())


def test_client_config_from_env_reads_primary_alpaca_vars(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Alpaca config reads market-data credentials and optional feed settings."""
    monkeypatch.setenv("ALPACA_API_KEY_ID", "key-id")
    monkeypatch.setenv("ALPACA_API_SECRET_KEY", "secret-key")
    monkeypatch.setenv("ALPACA_DATA_BASE_URL", "https://example.alpaca.test")
    monkeypatch.setenv("ALPACA_DATA_FEED", "delayed_sip")

    config = AlpacaMarketDataConfig.from_env()

    assert config.api_key_id == "key-id"
    assert config.api_secret_key == "secret-key"
    assert config.base_url == "https://example.alpaca.test"
    assert config.feed == "delayed_sip"


def test_client_config_from_env_reads_legacy_apca_vars(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Alpaca config also accepts the official APCA-prefixed variable names."""
    monkeypatch.delenv("ALPACA_API_KEY_ID", raising=False)
    monkeypatch.delenv("ALPACA_API_SECRET_KEY", raising=False)
    monkeypatch.setenv("APCA_API_KEY_ID", "legacy-key-id")
    monkeypatch.setenv("APCA_API_SECRET_KEY", "legacy-secret-key")

    config = AlpacaMarketDataConfig.from_env()

    assert config.api_key_id == "legacy-key-id"
    assert config.api_secret_key == "legacy-secret-key"


def test_client_config_from_env_rejects_missing_credentials(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Alpaca config fails closed when credentials are absent."""
    monkeypatch.delenv("ALPACA_API_KEY_ID", raising=False)
    monkeypatch.delenv("ALPACA_API_SECRET_KEY", raising=False)
    monkeypatch.delenv("APCA_API_KEY_ID", raising=False)
    monkeypatch.delenv("APCA_API_SECRET_KEY", raising=False)

    with pytest.raises(ValueError, match="ALPACA_API_KEY_ID"):
        AlpacaMarketDataConfig.from_env()


def test_fetch_daily_bar_page_calls_alpaca_bars_endpoint() -> None:
    """Fetcher requests one 1Day bar slice and normalizes the response."""
    session = _FakeSession([_FakeResponse(_fixture_payload())])
    client = AlpacaMarketDataClient(
        AlpacaMarketDataConfig(
            api_key_id="test-key",
            api_secret_key="test-secret",
            base_url="https://example.alpaca.test",
            feed="iex",
        ),
        session=session,  # type: ignore[arg-type]
    )

    page = client.fetch_daily_bar_page(tickers=["aapl", "msft"], as_of_date="2024-01-02")

    assert [record.ticker for record in page.records] == ["AAPL", "MSFT"]
    assert page.next_page_token is None
    assert session.calls == [
        {
            "url": "https://example.alpaca.test/v2/stocks/bars",
            "headers": {
                "APCA-API-KEY-ID": "test-key",
                "APCA-API-SECRET-KEY": "test-secret",
            },
            "params": {
                "symbols": "AAPL,MSFT",
                "timeframe": "1Day",
                "start": "2024-01-02",
                "end": "2024-01-02",
                "adjustment": "raw",
                "asof": "2024-01-02",
                "feed": "iex",
                "currency": "USD",
                "limit": 10000,
                "sort": "asc",
            },
            "timeout": 30,
        }
    ]


def test_fetch_live_daily_bars_paginates_and_deduplicates() -> None:
    """Fetcher follows Alpaca page tokens for one-date live snapshots only."""
    first_payload = {
        "bars": {"AAPL": _fixture_payload()["bars"]["AAPL"]},
        "next_page_token": "page-2",
    }
    second_payload = {
        "bars": {"MSFT": _fixture_payload()["bars"]["MSFT"]},
        "next_page_token": None,
    }
    session = _FakeSession([_FakeResponse(first_payload), _FakeResponse(second_payload)])
    client = AlpacaMarketDataClient(
        AlpacaMarketDataConfig(
            api_key_id="test-key",
            api_secret_key="test-secret",
            base_url="https://example.alpaca.test",
            feed="iex",
        ),
        session=session,  # type: ignore[arg-type]
    )

    records = client.fetch_live_daily_bars(
        tickers=["AAPL", "MSFT"],
        as_of_date="2024-01-02",
    )

    assert [record.ticker for record in records] == ["AAPL", "MSFT"]
    assert session.calls[1]["params"]["page_token"] == "page-2"


def test_fetch_live_daily_bars_rejects_empty_ticker_list() -> None:
    """Fetcher fails fast when there is no eligible ticker slice to request."""
    client = AlpacaMarketDataClient(
        AlpacaMarketDataConfig(api_key_id="test-key", api_secret_key="test-secret")
    )

    with pytest.raises(ValueError, match="tickers"):
        client.fetch_live_daily_bars(tickers=[], as_of_date="2024-01-02")


def test_normalize_alpaca_bar_response_builds_schema_valid_records() -> None:
    """Alpaca bars normalize into OHLCVRecord rows compatible with Layer 0 storage."""
    records = normalize_alpaca_bar_response(
        _fixture_payload(),
        requested_tickers=["AAPL", "MSFT"],
        as_of_date="2024-01-02",
    )

    assert [record.date for record in records] == ["2024-01-02", "2024-01-02"]
    assert records[0].ticker == "AAPL"
    assert records[0].open == 185.64
    assert records[0].high == 188.44
    assert records[0].low == 183.89
    assert records[0].close == 186.86
    assert records[0].adj_close == 186.86
    assert records[0].volume == 82488700
    assert records[0].dollar_volume == pytest.approx(186.12 * 82488700)


def test_normalize_alpaca_bar_response_returns_empty_for_empty_response() -> None:
    """An empty Alpaca bar response remains an empty record list."""
    assert normalize_alpaca_bar_response({"bars": {}}, as_of_date="2024-01-02") == []
    assert normalize_alpaca_bar_response({}, as_of_date="2024-01-02") == []


def test_normalize_alpaca_bar_response_uses_payload_keys_when_tickers_omitted() -> None:
    """Lowercase payload keys still normalize when requested_tickers is omitted."""
    payload = _fixture_payload()
    payload["bars"] = {ticker.lower(): rows for ticker, rows in payload["bars"].items()}

    records = normalize_alpaca_bar_response(payload, as_of_date="2024-01-02")

    assert [record.ticker for record in records] == ["AAPL", "MSFT"]
    assert [record.date for record in records] == ["2024-01-02", "2024-01-02"]


def test_normalize_alpaca_bar_response_uses_close_when_vwap_missing() -> None:
    """Dollar volume falls back to close times volume when Alpaca omits VWAP."""
    payload = _fixture_payload()
    row = dict(payload["bars"]["AAPL"][0])
    del row["vw"]
    payload["bars"]["AAPL"] = [row]

    record = normalize_alpaca_bar_response(
        payload,
        requested_tickers=["AAPL"],
        as_of_date="2024-01-02",
    )[0]

    assert record.dollar_volume == pytest.approx(record.close * record.volume)


def test_normalize_alpaca_bar_response_rejects_missing_required_field() -> None:
    """Missing Alpaca OHLCV fields fail before reaching downstream storage."""
    payload = _fixture_payload()
    row = dict(payload["bars"]["AAPL"][0])
    del row["c"]
    payload["bars"]["AAPL"] = [row]

    with pytest.raises(ValueError, match="c"):
        normalize_alpaca_bar_response(
            payload,
            requested_tickers=["AAPL"],
            as_of_date="2024-01-02",
        )


def test_normalize_alpaca_bar_response_rejects_missing_timestamp_field() -> None:
    """Missing bar timestamps fail closed before live snapshot date normalization."""
    payload = _fixture_payload()
    row = dict(payload["bars"]["AAPL"][0])
    row["t"] = None
    payload["bars"]["AAPL"] = [row]

    with pytest.raises(ValueError, match="t"):
        normalize_alpaca_bar_response(
            payload,
            requested_tickers=["AAPL"],
            as_of_date="2024-01-02",
        )


def test_normalize_alpaca_bar_response_rejects_string_numeric_values() -> None:
    """Numeric strings are rejected instead of silently coercing vendor payloads."""
    payload = _fixture_payload()
    row = dict(payload["bars"]["AAPL"][0])
    row["o"] = "185.64"
    payload["bars"]["AAPL"] = [row]

    with pytest.raises(TypeError, match="o"):
        normalize_alpaca_bar_response(
            payload,
            requested_tickers=["AAPL"],
            as_of_date="2024-01-02",
        )


def test_normalize_alpaca_bar_response_rejects_nan_values() -> None:
    """NaN vendor values fail contract validation instead of entering storage."""
    payload = _fixture_payload()
    row = dict(payload["bars"]["AAPL"][0])
    row["h"] = float("nan")
    payload["bars"]["AAPL"] = [row]

    with pytest.raises(ValueError, match="finite"):
        normalize_alpaca_bar_response(
            payload,
            requested_tickers=["AAPL"],
            as_of_date="2024-01-02",
        )


def test_normalize_alpaca_bar_response_rejects_wrong_bar_date() -> None:
    """A one-date live request cannot silently normalize another trading date."""
    with pytest.raises(ValueError, match="does not match"):
        normalize_alpaca_bar_response(
            _fixture_payload(),
            requested_tickers=["AAPL"],
            as_of_date="2024-01-03",
        )


def test_normalize_alpaca_bar_response_rejects_duplicate_daily_bars() -> None:
    """Duplicate ticker-date bars fail closed for a live append."""
    row = _fixture_payload()["bars"]["AAPL"][0]
    payload = {"bars": {"AAPL": [row, row]}}

    with pytest.raises(ValueError, match="Duplicate"):
        normalize_alpaca_bar_response(
            payload,
            requested_tickers=["AAPL"],
            as_of_date="2024-01-02",
        )
