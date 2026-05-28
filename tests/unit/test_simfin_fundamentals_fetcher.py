from __future__ import annotations

import json
import os
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pytest
import requests

from app.lab.data_pipelines import backfill_simfin
from app.lab.data_pipelines.backfill_simfin import backfill_simfin_archive
from app.lab.data_pipelines.repair_simfin_coverage import (
    FundamentalsCoverageRecord,
    FundamentalsCoverageReport,
    affected_active_tickers,
    count_fundamentals_rows,
    diagnose_fundamentals_coverage,
    refetch_active_fundamentals_gaps,
    write_coverage_report,
)
from services.r2.paths import raw_fundamentals_path
from services.simfin.fundamentals_fetcher import (
    DEFAULT_SIMFIN_PERIODS,
    DEFAULT_SIMFIN_STATEMENTS,
    SimFinClientConfig,
    SimFinFundamentalsFetcher,
    normalize_simfin_fundamental_rows,
)

FIXTURE_PATH = Path("data/sample/simfin_fundamentals_response.json")
SEC_TEST_USER_AGENT = "AI Stock Trader ops@quanttradingresearch.test"


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

    def get_object(self, key: str) -> bytes:
        payload = self.objects[key]
        return payload if isinstance(payload, bytes) else payload.encode("utf-8")


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


def _set_sec_user_agent(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SEC_USER_AGENT", SEC_TEST_USER_AGENT)


def _sec_company_tickers_payload(*, ticker: str, cik: int, title: str) -> dict[str, Any]:
    return {"0": {"ticker": ticker, "cik_str": cik, "title": title}}


def _sec_companyfacts_payload(
    *,
    accession: str,
    report_date: str,
    filed_date: str,
    fiscal_year: int,
    fiscal_period: str,
    form: str = "10-Q",
) -> dict[str, Any]:
    return {
        "cik": 14693,
        "entityName": "BROWN FORMAN CORP",
        "facts": {
            "dei": {
                "EntityCommonStockSharesOutstanding": {
                    "units": {
                        "shares": [
                            {
                                "accn": accession,
                                "end": report_date,
                                "filed": filed_date,
                                "form": form,
                                "fp": fiscal_period,
                                "fy": fiscal_year,
                                "val": 217_000_000,
                            }
                        ]
                    }
                }
            },
            "us-gaap": {
                "RevenueFromContractWithCustomerExcludingAssessedTax": {
                    "units": {
                        "USD": [
                            {
                                "accn": accession,
                                "end": report_date,
                                "filed": filed_date,
                                "form": form,
                                "fp": fiscal_period,
                                "fy": fiscal_year,
                                "start": "2025-02-01",
                                "val": 1_051_200_000,
                            }
                        ]
                    }
                },
                "NetIncomeLoss": {
                    "units": {
                        "USD": [
                            {
                                "accn": accession,
                                "end": report_date,
                                "filed": filed_date,
                                "form": form,
                                "fp": fiscal_period,
                                "fy": fiscal_year,
                                "start": "2025-02-01",
                                "val": 266_700_000,
                            }
                        ]
                    }
                },
                "Assets": {
                    "units": {
                        "USD": [
                            {
                                "accn": accession,
                                "end": report_date,
                                "filed": filed_date,
                                "form": form,
                                "fp": fiscal_period,
                                "fy": fiscal_year,
                                "val": 7_933_500_000,
                            }
                        ]
                    }
                },
                "Liabilities": {
                    "units": {
                        "USD": [
                            {
                                "accn": accession,
                                "end": report_date,
                                "filed": filed_date,
                                "form": form,
                                "fp": fiscal_period,
                                "fy": fiscal_year,
                                "val": 4_712_800_000,
                            }
                        ]
                    }
                },
                "StockholdersEquity": {
                    "units": {
                        "USD": [
                            {
                                "accn": accession,
                                "end": report_date,
                                "filed": filed_date,
                                "form": form,
                                "fp": fiscal_period,
                                "fy": fiscal_year,
                                "val": 3_220_700_000,
                            }
                        ]
                    }
                },
                "EarningsPerShareBasic": {
                    "units": {
                        "USD/shares": [
                            {
                                "accn": accession,
                                "end": report_date,
                                "filed": filed_date,
                                "form": form,
                                "fp": fiscal_period,
                                "fy": fiscal_year,
                                "start": "2025-02-01",
                                "val": 1.23,
                            }
                        ]
                    }
                },
            },
        },
    }


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


def test_client_config_from_env_uses_env_file_for_blank_values(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Blank runtime env vars are repopulated from config/simfin.env."""
    env_file = tmp_path / "simfin.env"
    env_file.write_text(
        "\n".join(
            [
                "SIMFIN_API_KEY=file-key",
                "SIMFIN_BASE_URL=https://example.simfin.test/api/v3",
                "SEC_USER_AGENT=AI Stock Trader env-file@test.invalid",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr("services.simfin.fundamentals_fetcher.SIMFIN_ENV_FILE", env_file)
    monkeypatch.setenv("SIMFIN_API_KEY", "")
    monkeypatch.setenv("SIMFIN_BASE_URL", "")
    monkeypatch.setenv("SEC_USER_AGENT", "")

    config = SimFinClientConfig.from_env()

    assert config.api_key == "file-key"
    assert config.base_url == "https://example.simfin.test/api/v3"
    assert os.environ["SEC_USER_AGENT"] == "AI Stock Trader env-file@test.invalid"


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
        SimFinClientConfig(api_key="test-key", retry_sleep_seconds=0, rate_limit_sleep_seconds=0),
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


def test_fetch_all_fundamentals_batches_large_ticker_sets(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Large ticker universes are split into smaller SimFin requests."""
    _set_sec_user_agent(monkeypatch)
    session = _FakeSession([_FakeResponse({"data": []}), _FakeResponse({"data": []})])
    fetcher = SimFinFundamentalsFetcher(
        SimFinClientConfig(
            api_key="test-key",
            retry_sleep_seconds=0,
            min_request_interval_seconds=0,
        ),
        session=session,  # type: ignore[arg-type]
    )

    tickers = [f"TICKER{i}" for i in range(51)]
    rows = fetcher.fetch_all_fundamentals(
        tickers=tickers,
        start_date="2024-01-01",
        end_date="2024-12-31",
    )

    assert rows == []
    assert len(session.calls) == 3
    assert session.calls[0]["params"]["ticker"] == ",".join(tickers[:50])
    assert session.calls[1]["params"]["ticker"] == tickers[50]
    assert session.calls[2]["url"] == "https://www.sec.gov/files/company_tickers.json"


def test_fetch_all_fundamentals_splits_on_server_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Transient 5xx errors split ticker batches to isolate failures."""
    _set_sec_user_agent(monkeypatch)
    response = requests.Response()
    response.status_code = 500
    error = requests.HTTPError("server error", response=response)
    session = _FakeSession(
        [
            _FakeResponse({}, error),
            _FakeResponse({"data": []}),
            _FakeResponse({"data": []}),
        ]
    )
    fetcher = SimFinFundamentalsFetcher(
        SimFinClientConfig(
            api_key="test-key",
            retry_sleep_seconds=0,
            max_retries=0,
            split_cooldown_seconds=0,
        ),
        session=session,  # type: ignore[arg-type]
    )

    rows = fetcher.fetch_all_fundamentals(
        tickers=["AAPL", "MSFT"],
        start_date="2024-01-01",
        end_date="2024-12-31",
    )

    assert rows == []
    assert session.calls[0]["params"]["ticker"] == "AAPL,MSFT"
    assert session.calls[1]["params"]["ticker"] == "AAPL"
    assert session.calls[2]["params"]["ticker"] == "MSFT"


def test_fetch_all_fundamentals_splits_on_rate_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Rate-limit errors split ticker batches to reduce request pressure."""
    _set_sec_user_agent(monkeypatch)
    response = requests.Response()
    response.status_code = 429
    error = requests.HTTPError("rate limited", response=response)
    session = _FakeSession(
        [
            _FakeResponse({}, error),
            _FakeResponse({"data": []}),
            _FakeResponse({"data": []}),
        ]
    )
    fetcher = SimFinFundamentalsFetcher(
        SimFinClientConfig(
            api_key="test-key",
            retry_sleep_seconds=0,
            rate_limit_sleep_seconds=0,
            max_retries=0,
            split_cooldown_seconds=0,
        ),
        session=session,  # type: ignore[arg-type]
    )

    rows = fetcher.fetch_all_fundamentals(
        tickers=["AAPL", "MSFT"],
        start_date="2024-01-01",
        end_date="2024-12-31",
    )

    assert rows == []
    assert session.calls[0]["params"]["ticker"] == "AAPL,MSFT"
    assert session.calls[1]["params"]["ticker"] == "AAPL"
    assert session.calls[2]["params"]["ticker"] == "MSFT"


def test_fetch_all_fundamentals_skips_single_ticker_when_retries_exhausted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Single-ticker failures after split are logged and skipped, not re-raised."""
    _set_sec_user_agent(monkeypatch)
    server_response = requests.Response()
    server_response.status_code = 500
    server_error = requests.HTTPError("server error", response=server_response)
    rate_response = requests.Response()
    rate_response.status_code = 429
    rate_error = requests.HTTPError("rate limited", response=rate_response)
    session = _FakeSession(
        [
            _FakeResponse({}, server_error),  # AAPL,MSFT batch fails
            _FakeResponse({"data": []}),       # AAPL split succeeds (empty)
            _FakeResponse({}, rate_error),    # MSFT split exhausts retries -> skipped
        ]
    )
    fetcher = SimFinFundamentalsFetcher(
        SimFinClientConfig(
            api_key="test-key",
            retry_sleep_seconds=0,
            rate_limit_sleep_seconds=0,
            max_retries=0,
            split_cooldown_seconds=0,
        ),
        session=session,  # type: ignore[arg-type]
    )

    rows = fetcher.fetch_all_fundamentals(
        tickers=["AAPL", "MSFT"],
        start_date="2024-01-01",
        end_date="2024-12-31",
    )

    assert rows == []
    assert session.calls[2]["params"]["ticker"] == "MSFT"


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


def test_fetch_statement_rows_throttles_between_requests(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Fetcher enforces a minimum delay between SimFin requests."""
    fixture = _fixture_payload()
    session = _FakeSession([_FakeResponse(fixture["page1"]), _FakeResponse(fixture["page1"])])
    clock = {"now": 0.0}
    sleeps: list[float] = []

    def fake_monotonic() -> float:
        return clock["now"]

    def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)
        clock["now"] += seconds

    monkeypatch.setattr("services.simfin.fundamentals_fetcher.time.monotonic", fake_monotonic)
    monkeypatch.setattr("services.simfin.fundamentals_fetcher.time.sleep", fake_sleep)

    fetcher = SimFinFundamentalsFetcher(
        SimFinClientConfig(
            api_key="test-key",
            retry_sleep_seconds=0,
            min_request_interval_seconds=1.0,
        ),
        session=session,  # type: ignore[arg-type]
    )

    fetcher.fetch_statement_rows(
        tickers=["AAPL"],
        start_date="2024-01-01",
        end_date="2024-12-31",
    )

    clock["now"] += 0.25

    fetcher.fetch_statement_rows(
        tickers=["MSFT"],
        start_date="2024-01-01",
        end_date="2024-12-31",
    )

    assert sleeps == [pytest.approx(0.75)]


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


def test_fetch_statement_rows_uses_simfin_share_class_symbols() -> None:
    """SP500 class-share archive tickers are translated to SimFin vendor symbols."""
    payload = [
        {
            "ticker": "BRK.B",
            "currency": "USD",
            "statements": [
                {
                    "statement": "PL",
                    "columns": ["Fiscal Period", "Fiscal Year", "Report Date", "Publish Date"],
                    "data": [["Q1", 2024, "2024-03-31", "2024-05-03"]],
                }
            ],
        },
        {
            "ticker": "BF.B",
            "currency": "USD",
            "statements": [
                {
                    "statement": "BS",
                    "columns": ["Fiscal Period", "Fiscal Year", "Report Date", "Publish Date"],
                    "data": [["Q1", 2024, "2024-03-31", "2024-05-04"]],
                }
            ],
        },
    ]
    session = _FakeSession([_FakeResponse(payload)])
    fetcher = SimFinFundamentalsFetcher(
        SimFinClientConfig(api_key="test-key", retry_sleep_seconds=0),
        session=session,  # type: ignore[arg-type]
    )

    page = fetcher.fetch_statement_rows(
        tickers=["BRK-B", "BF-B"],
        start_date="2024-01-01",
        end_date="2024-12-31",
    )

    assert session.calls[0]["params"]["ticker"] == "BRK.B,BF.B"
    assert [row["ticker"] for row in page.rows] == ["BRK-B", "BF-B"]


def test_fetch_statement_rows_duplicates_company_fundamentals_for_requested_share_classes() -> None:
    """One provider company-symbol response can populate multiple requested archive tickers."""
    payload = [
        {
            "ticker": "GOOG",
            "currency": "USD",
            "statements": [
                {
                    "statement": "PL",
                    "columns": ["Fiscal Period", "Fiscal Year", "Report Date", "Publish Date"],
                    "data": [["Q1", 2024, "2024-03-31", "2024-05-03"]],
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
        tickers=["GOOG", "GOOGL"],
        start_date="2024-01-01",
        end_date="2024-12-31",
    )

    assert session.calls[0]["params"]["ticker"] == "GOOG"
    assert [row["ticker"] for row in page.rows] == ["GOOG", "GOOGL"]


def test_fetch_statement_rows_maps_provider_rename_aliases_back_to_canonical_ticker() -> None:
    """Vendor rename aliases are rewritten to the canonical archive ticker."""
    payload = [
        {
            "ticker": "FLT",
            "currency": "USD",
            "statements": [
                {
                    "statement": "PL",
                    "columns": ["Fiscal Period", "Fiscal Year", "Report Date", "Publish Date"],
                    "data": [["Q1", 2024, "2024-03-31", "2024-05-03"]],
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
        tickers=["CPAY"],
        start_date="2024-01-01",
        end_date="2024-12-31",
    )

    assert session.calls[0]["params"]["ticker"] == "FLT"
    assert [row["ticker"] for row in page.rows] == ["CPAY"]


def test_fetch_all_fundamentals_uses_sec_companyfacts_fallback_for_missing_ticker(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Public SEC company facts backfill tickers when SimFin returns no rows."""
    _set_sec_user_agent(monkeypatch)
    session = _FakeSession(
        [
            _FakeResponse({"data": []}),
            _FakeResponse(
                _sec_company_tickers_payload(
                    ticker="BF-B",
                    cik=14693,
                    title="BROWN FORMAN CORP",
                )
            ),
            _FakeResponse(
                _sec_companyfacts_payload(
                    accession="0000014693-26-000123",
                    report_date="2025-04-30",
                    filed_date="2026-03-04",
                    fiscal_year=2026,
                    fiscal_period="Q4",
                )
            ),
        ]
    )
    fetcher = SimFinFundamentalsFetcher(
        SimFinClientConfig(api_key="test-key", retry_sleep_seconds=0, rate_limit_sleep_seconds=0),
        session=session,  # type: ignore[arg-type]
    )

    rows = fetcher.fetch_all_fundamentals(
        tickers=["BF-B"],
        start_date="2026-03-01",
        end_date="2026-03-31",
        retrieved_at=datetime(2026, 3, 5, tzinfo=UTC),
        limit=100,
    )

    assert len(rows) == 1
    assert rows[0]["source"] == "sec_companyfacts"
    assert rows[0]["ticker"] == "BF-B"
    assert rows[0]["report_date"] == "2025-04-30"
    assert rows[0]["availability_date"] == "2026-03-04"
    assert rows[0]["fiscal_year"] == 2026
    assert rows[0]["fiscal_period"] == "Q4"
    assert rows[0]["statement"] == "sec_companyfacts"
    assert rows[0]["retrieved_at"] == "2026-03-05T00:00:00+00:00"
    assert rows[0]["raw"]["Revenue"] == 1_051_200_000
    assert rows[0]["raw"]["Net Income"] == 266_700_000
    assert rows[0]["raw"]["sharesBasic"] == 217_000_000
    assert rows[0]["raw"]["epsBasic"] == 1.23
    assert session.calls[0]["params"]["ticker"] == "BF.B"
    assert session.calls[1]["url"] == "https://www.sec.gov/files/company_tickers.json"
    assert session.calls[1]["headers"]["user-agent"] == SEC_TEST_USER_AGENT
    assert session.calls[2]["url"] == "https://data.sec.gov/api/xbrl/companyfacts/CIK0000014693.json"
    assert session.calls[2]["headers"]["user-agent"] == SEC_TEST_USER_AGENT


def test_fetch_all_fundamentals_requires_sec_user_agent_for_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """SEC fallback fails closed when no EDGAR user-agent is configured."""
    monkeypatch.setattr(
        "services.simfin.fundamentals_fetcher.SIMFIN_ENV_FILE",
        Path("/tmp/does-not-exist.env"),
    )
    monkeypatch.delenv("SEC_USER_AGENT", raising=False)
    session = _FakeSession([_FakeResponse({"data": []})])
    fetcher = SimFinFundamentalsFetcher(
        SimFinClientConfig(
            api_key="test-key",
            retry_sleep_seconds=0,
            rate_limit_sleep_seconds=0,
        ),
        session=session,  # type: ignore[arg-type]
    )

    with pytest.raises(ValueError, match="SEC_USER_AGENT"):
        fetcher.fetch_all_fundamentals(
            tickers=["BF-B"],
            start_date="2026-03-01",
            end_date="2026-03-31",
            retrieved_at=datetime(2026, 3, 5, tzinfo=UTC),
            limit=100,
        )


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

    key = raw_fundamentals_path("AAPL")
    assert result.output_keys == (key,)
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
    """Existing SimFin per-ticker archives are skipped unless overwrite is requested."""
    key = raw_fundamentals_path("AAPL")
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


def test_backfill_skips_writing_new_empty_archives() -> None:
    """A zero-row SimFin fetch does not create a brand-new empty parquet placeholder."""
    writer = _FakeWriter()
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
    assert result.empty == 1
    assert result.missing_tickers == ("AAPL",)
    assert writer.objects == {}


def test_diagnose_fundamentals_coverage_tags_active_and_delisted_gaps() -> None:
    """Coverage diagnostics list low-row archives and identify active constituents."""
    writer = _FakeWriter(
        existing={
            raw_fundamentals_path("AAPL"),
            raw_fundamentals_path("MSFT"),
            raw_fundamentals_path("OLD"),
        }
    )

    report = diagnose_fundamentals_coverage(
        reader=writer,
        historical_tickers=["AAPL", "MSFT", "OLD", "GOOGL"],
        active_tickers=["AAPL", "MSFT", "GOOGL"],
        min_rows=10,
        row_counter=lambda _reader, key: {
            raw_fundamentals_path("AAPL"): 129,
            raw_fundamentals_path("MSFT"): 3,
            raw_fundamentals_path("OLD"): 2,
        }.get(key, 0),
    )

    assert [(record.ticker, record.row_count, record.active, record.reason) for record in report.records] == [
        ("GOOGL", 0, True, "missing_archive"),
        ("MSFT", 3, True, "below_min_rows"),
        ("OLD", 2, False, "below_min_rows"),
    ]
    assert affected_active_tickers(report) == ["GOOGL", "MSFT"]


def test_count_fundamentals_rows_reads_parquet_payload() -> None:
    """Coverage row counting reads raw Parquet objects from the archive reader."""
    pd = pytest.importorskip("pandas")
    pytest.importorskip("pyarrow")
    key = raw_fundamentals_path("AAPL")
    payload = pd.DataFrame(
        [
            {"ticker": "AAPL", "report_date": "2024-03-31"},
            {"ticker": "AAPL", "report_date": "2024-06-30"},
        ]
    ).to_parquet(index=False)
    writer = _FakeWriter(existing={key})
    writer.objects[key] = payload

    assert count_fundamentals_rows(writer, key) == 2


def test_write_coverage_report_writes_sorted_json(tmp_path: Path) -> None:
    """Coverage diagnostics are persisted as deterministic JSON audit records."""
    report = FundamentalsCoverageReport(
        generated_at="2024-08-05T00:00:00+00:00",
        min_rows=10,
        historical_ticker_count=2,
        active_ticker_count=1,
        records=[
            FundamentalsCoverageRecord(
                ticker="MSFT",
                row_count=3,
                active=True,
                reason="below_min_rows",
            )
        ],
    )

    path = write_coverage_report(report, tmp_path)

    assert path.name == "simfin_coverage_gaps_min10.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload == {
        "active_ticker_count": 1,
        "generated_at": "2024-08-05T00:00:00+00:00",
        "historical_ticker_count": 2,
        "min_rows": 10,
        "records": [
            {
                "active": True,
                "reason": "below_min_rows",
                "row_count": 3,
                "ticker": "MSFT",
            }
        ],
    }
    assert path.read_text(encoding="utf-8").endswith("\n")


def test_refetch_active_fundamentals_gaps_targets_only_active_low_coverage_tickers() -> None:
    """Recovery refetch is scoped to active tickers below the coverage threshold."""
    writer = _FakeWriter()
    retrieved_at = datetime(2024, 5, 4, tzinfo=UTC)
    fetcher = _FakeFetcher(
        normalize_simfin_fundamental_rows(
            [
                {"ticker": "GOOGL", "reportDate": "2024-03-31", "publishDate": "2024-05-01"},
                {"ticker": "MSFT", "reportDate": "2024-03-31", "publishDate": "2024-05-02"},
            ],
            retrieved_at=retrieved_at,
        )
    )

    result = refetch_active_fundamentals_gaps(
        from_date=date(2024, 1, 1),
        to_date=date(2024, 12, 31),
        fetcher=fetcher,
        writer=writer,
        tickers=["GOOGL", "MSFT"],
        retrieved_at=retrieved_at,
        serializer=_json_serializer,
    )

    assert result.written == 2
    assert fetcher.calls[0]["tickers"] == ["GOOGL", "MSFT"]
    assert fetcher.calls[0]["retrieved_at"] == retrieved_at
    assert set(writer.objects) == {raw_fundamentals_path("GOOGL"), raw_fundamentals_path("MSFT")}


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
