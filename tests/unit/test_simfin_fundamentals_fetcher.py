from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pytest
import requests

from app.lab.data_pipelines import backfill_simfin
from app.lab.data_pipelines.backfill_simfin import backfill_simfin_archive
from services.r2.paths import raw_fundamentals_path
from services.simfin.fundamentals_fetcher import (
    DEFAULT_SIMFIN_PERIODS,
    DEFAULT_SIMFIN_STATEMENTS,
    SimFinClientConfig,
    SimFinFundamentalsFetcher,
    normalize_simfin_fundamental_rows,
)

FIXTURE_PATH = Path("data/sample/simfin_fundamentals_response.json")


class _FakeResponse:
    def __init__(self, payload: Any, error: requests.RequestException | None = None) -> None:
        self._payload = payload
        self._error = error

    def json(self) -> Any:
        return self._payload

    def raise_for_status(self) -> None:
        if self._error is not None:
            raise self._error


class _FakeSession:
    def __init__(self, responses: list[_FakeResponse]) -> None:
        self._responses = list(responses)
        self.calls: list[dict[str, Any]] = []

    def get(self, url: str, **kwargs: Any) -> _FakeResponse:
        self.calls.append({"url": url, **kwargs})
        return self._responses.pop(0) if self._responses else _FakeResponse({"data": []})


class _FakeWriter:
    def __init__(self, existing: set[str] | None = None) -> None:
        self.existing = set(existing or set())
        self.objects: dict[str, bytes | str] = {}

    def put_object(self, key: str, data: bytes | str) -> None:
        self.objects[key] = data
        self.existing.add(key)

    def exists(self, key: str) -> bool:
        return key in self.existing


class _FakeFetcher:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self.rows = rows
        self.calls: list[dict[str, Any]] = []

    def fetch_all_fundamentals(self, **kwargs: Any) -> list[dict[str, Any]]:
        self.calls.append(kwargs)
        return self.rows


def _fixture_payload() -> dict[str, Any]:
    return json.loads(FIXTURE_PATH.read_text())


def _json_serializer(rows: list[dict[str, object]]) -> bytes:
    return json.dumps(rows, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _read_json(payload: bytes | str) -> list[dict[str, Any]]:
    text = payload.decode("utf-8") if isinstance(payload, bytes) else payload
    return json.loads(text)


def test_client_config_from_env_reads_key(monkeypatch: pytest.MonkeyPatch) -> None:
    """SimFin config reads API settings from environment variables."""
    monkeypatch.setenv("SIMFIN_API_KEY", "test-key")
    monkeypatch.setenv("SIMFIN_BASE_URL", "https://backend.simfin.com/api/v3")

    config = SimFinClientConfig.from_env()

    assert config.api_key == "test-key"
    assert config.base_url == "https://backend.simfin.com/api/v3"
    assert config.timeout_seconds == 30


def test_client_config_from_env_rejects_missing_key(monkeypatch: pytest.MonkeyPatch) -> None:
    """SimFin config fails closed when credentials are absent."""
    monkeypatch.setattr(
        "services.simfin.fundamentals_fetcher.SIMFIN_ENV_FILE",
        Path("/tmp/does-not-exist.env"),
    )
    monkeypatch.delenv("SIMFIN_API_KEY", raising=False)

    with pytest.raises(ValueError, match="SIMFIN_API_KEY"):
        SimFinClientConfig.from_env()


def test_fetch_statement_rows_calls_compact_endpoint_and_normalizes_rows() -> None:
    """Fetcher calls SimFin's compact statements endpoint with scoped params."""
    fixture = _fixture_payload()
    session = _FakeSession([_FakeResponse(fixture["page1"])])
    fetcher = SimFinFundamentalsFetcher(
        SimFinClientConfig(
            api_key="test-key",
            base_url="https://example.simfin.test/api/v3",
            retry_sleep_seconds=0,
        ),
        session=session,  # type: ignore[arg-type]
    )

    page = fetcher.fetch_statement_rows(
        tickers=["aapl", "msft"],
        start_date="2024-01-01",
        end_date="2024-12-31",
        statements=("pl", "bs"),
        periods=("q1", "fy"),
        limit=2,
        offset=0,
    )

    assert [row["ticker"] for row in page.rows] == ["AAPL", "MSFT"]
    assert page.rows[0]["availability_date"] == "2024-05-03"
    assert page.rows[0]["earnings_date"] == "2024-05-02"
    assert page.rows[0]["fiscal_year"] == 2024
    assert isinstance(page.rows[0]["fiscal_year"], int)
    assert session.calls == [
        {
            "url": "https://example.simfin.test/api/v3/companies/statements/compact",
            "params": {
                "ticker": "AAPL,MSFT",
                "statements": "pl,bs",
                "period": "q1,fy",
                "start": "2024-01-01",
                "end": "2024-12-31",
                "asreported": "true",
                "limit": 2,
                "offset": 0,
            },
            "headers": {
                "accept": "application/json",
                "Authorization": "api-key test-key",
            },
            "timeout": 30,
        }
    ]


def test_fetch_all_fundamentals_paginates_and_deduplicates() -> None:
    """Pagination continues until exhaustion and deduplicates repeated raw rows."""
    fixture = _fixture_payload()
    retrieved_at = datetime(2024, 8, 5, tzinfo=UTC)
    session = _FakeSession(
        [
            _FakeResponse(fixture["page1"]),
            _FakeResponse(fixture["page2"]),
            _FakeResponse(fixture["empty"]),
        ]
    )
    fetcher = SimFinFundamentalsFetcher(
        SimFinClientConfig(api_key="test-key", retry_sleep_seconds=0),
        session=session,  # type: ignore[arg-type]
    )

    rows = fetcher.fetch_all_fundamentals(
        tickers=["AAPL", "MSFT"],
        start_date="2024-01-01",
        end_date="2024-12-31",
        retrieved_at=retrieved_at,
        limit=2,
    )

    assert [(row["ticker"], row.get("statement")) for row in rows] == [
        ("AAPL", "pl"),
        ("MSFT", "bs"),
        ("AAPL", "cf"),
    ]
    assert [call["params"]["offset"] for call in session.calls] == [0, 2, 4]
    assert {row["retrieved_at"] for row in rows} == {"2024-08-05T00:00:00+00:00"}


def test_fetch_all_fundamentals_batches_large_ticker_sets() -> None:
    """Large ticker universes are split into smaller SimFin requests."""
    session = _FakeSession([_FakeResponse({"data": []}), _FakeResponse({"data": []})])
    fetcher = SimFinFundamentalsFetcher(
        SimFinClientConfig(api_key="test-key", retry_sleep_seconds=0),
        session=session,  # type: ignore[arg-type]
    )

    tickers = [f"TICKER{i}" for i in range(201)]
    rows = fetcher.fetch_all_fundamentals(
        tickers=tickers,
        start_date="2024-01-01",
        end_date="2024-12-31",
    )

    assert rows == []
    assert len(session.calls) == 2
    assert session.calls[0]["params"]["ticker"] == ",".join(tickers[:200])
    assert session.calls[1]["params"]["ticker"] == tickers[200]


def test_fetch_statement_rows_rejects_malformed_payload_item() -> None:
    """Malformed SimFin rows fail with actionable diagnostics."""
    session = _FakeSession([_FakeResponse({"data": [{"ticker": "AAPL"}, "bad-row"]})])
    fetcher = SimFinFundamentalsFetcher(
        SimFinClientConfig(api_key="test-key"),
        session=session,  # type: ignore[arg-type]
    )

    with pytest.raises(ValueError, match="row 1 must be an object, got str"):
        fetcher.fetch_statement_rows(
            tickers=["AAPL"],
            start_date="2024-01-01",
            end_date="2024-12-31",
        )


def test_fetch_statement_rows_retries_transient_errors() -> None:
    """Retryable provider errors are retried before succeeding."""
    response = requests.Response()
    response.status_code = 429
    error = requests.HTTPError("rate limited", response=response)
    fixture = _fixture_payload()
    session = _FakeSession([_FakeResponse({}, error), _FakeResponse(fixture["page1"])])
    fetcher = SimFinFundamentalsFetcher(
        SimFinClientConfig(api_key="test-key", retry_sleep_seconds=0),
        session=session,  # type: ignore[arg-type]
    )

    rows = fetcher.fetch_statement_rows(
        tickers=["AAPL"],
        start_date="2024-01-01",
        end_date="2024-12-31",
    )

    assert len(rows.rows) == 2
    assert len(session.calls) == 2


def test_fetch_statement_rows_accepts_nested_company_payload_shape() -> None:
    """Fetcher supports backend compact payloads nested by company and statement."""
    payload = [
        {
            "ticker": "AAPL",
            "currency": "USD",
            "statements": [
                {
                    "statement": "PL",
                    "columns": [
                        "Fiscal Period",
                        "Fiscal Year",
                        "Report Date",
                        "Publish Date",
                    ],
                    "data": [["Q2", 2024, "2024-03-31", "2024-05-03"]],
                }
            ],
        }
    ]
    session = _FakeSession([_FakeResponse(payload)])
    fetcher = SimFinFundamentalsFetcher(
        SimFinClientConfig(api_key="test-key", retry_sleep_seconds=0),
        session=session,  # type: ignore[arg-type]
    )

    page = fetcher.fetch_statement_rows(
        tickers=["AAPL"],
        start_date="2024-01-01",
        end_date="2024-12-31",
    )

    assert len(page.rows) == 1
    assert page.rows[0]["ticker"] == "AAPL"
    assert page.rows[0]["report_date"] == "2024-03-31"
    assert page.rows[0]["availability_date"] == "2024-05-03"
    assert page.rows[0]["statement"] == "pl"


def test_normalize_accepts_missing_optional_fields() -> None:
    """Optional earnings/currency metadata can be absent without losing raw data."""
    retrieved_at = datetime(2024, 5, 4, tzinfo=UTC)
    rows = normalize_simfin_fundamental_rows(
        [
            {
                "ticker": "brk.b",
                "reportDate": "2024-03-31",
                "publishDate": "2024-05-04",
                "revenue": 9000,
            }
        ],
        retrieved_at=retrieved_at,
    )

    assert rows == [
        {
            "source": "simfin",
            "ticker": "BRK-B",
            "report_date": "2024-03-31",
            "availability_date": "2024-05-04",
            "retrieved_at": "2024-05-04T00:00:00+00:00",
            "raw": {
                "ticker": "brk.b",
                "reportDate": "2024-03-31",
                "publishDate": "2024-05-04",
                "revenue": 9000,
            },
        }
    ]


def test_normalize_uses_report_date_when_availability_date_is_missing() -> None:
    """Compact SimFin rows can fall back to report date when publish metadata is absent."""
    rows = normalize_simfin_fundamental_rows(
        [{"ticker": "AAPL", "reportDate": "2024-03-31"}],
        retrieved_at=datetime(2024, 5, 4, tzinfo=UTC),
    )

    assert rows[0]["availability_date"] == "2024-03-31"


def test_normalize_rejects_rows_without_any_point_in_time_date() -> None:
    """Rows still need at least a report date to support point-in-time joins."""
    with pytest.raises(ValueError, match="report_date"):
        normalize_simfin_fundamental_rows(
            [{"ticker": "AAPL"}],
            retrieved_at=datetime(2024, 5, 4, tzinfo=UTC),
        )


def test_backfill_writes_raw_fundamentals_archive() -> None:
    """Backfill writes one deterministic raw fundamentals archive for the range."""
    retrieved_at = datetime(2024, 8, 3, tzinfo=UTC)
    rows = normalize_simfin_fundamental_rows(
        _fixture_payload()["page2"],
        retrieved_at=retrieved_at,
    )
    writer = _FakeWriter()
    fetcher = _FakeFetcher(rows)

    result = backfill_simfin_archive(
        from_date=date(2024, 1, 1),
        to_date=date(2024, 12, 31),
        fetcher=fetcher,
        writer=writer,
        tickers=["AAPL"],
        statements=DEFAULT_SIMFIN_STATEMENTS,
        periods=DEFAULT_SIMFIN_PERIODS,
        retrieved_at=retrieved_at,
        serializer=_json_serializer,
    )

    key = raw_fundamentals_path(date(2024, 1, 1), date(2024, 12, 31))
    assert result.output_key == key
    assert result.requested_tickers == 1
    assert result.written == 1
    assert result.total_rows == 2
    assert key in writer.objects
    stored_rows = _read_json(writer.objects[key])
    assert [row["ticker"] for row in stored_rows] == ["AAPL", "AAPL"]
    assert "raw" in stored_rows[0]
    assert fetcher.calls[0]["tickers"] == ["AAPL"]
    assert fetcher.calls[0]["retrieved_at"] == retrieved_at


def test_backfill_is_idempotent_for_existing_archive() -> None:
    """Existing SimFin archives are skipped unless overwrite is requested."""
    key = raw_fundamentals_path(date(2024, 1, 1), date(2024, 12, 31))
    writer = _FakeWriter(existing={key})
    fetcher = _FakeFetcher([])

    result = backfill_simfin_archive(
        from_date=date(2024, 1, 1),
        to_date=date(2024, 12, 31),
        fetcher=fetcher,
        writer=writer,
        tickers=["AAPL"],
        serializer=_json_serializer,
    )

    assert result.written == 0
    assert result.skipped == 1
    assert fetcher.calls == []


def test_parse_args_rejects_empty_tickers_flag(monkeypatch: pytest.MonkeyPatch) -> None:
    """CLI rejects '--tickers' without symbols instead of running an unscoped pull."""
    monkeypatch.setattr(
        "sys.argv",
        [
            "backfill_simfin.py",
            "--from-date",
            "2024-01-01",
            "--to-date",
            "2024-12-31",
            "--tickers",
        ],
    )

    with pytest.raises(SystemExit):
        backfill_simfin._parse_args()
