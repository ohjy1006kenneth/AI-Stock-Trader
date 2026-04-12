from __future__ import annotations

import io
import json
import zipfile
from datetime import date
from pathlib import Path
from typing import Any

import pytest

import services.tiingo.ohlcv_fetcher as ohlcv_fetcher_module
from app.lab.data_pipelines import backfill_ohlcv as backfill_module
from app.lab.data_pipelines.backfill_ohlcv import backfill_ohlcv_archive
from core.contracts.schemas import OHLCVRecord
from services.r2.paths import raw_price_path, raw_security_master_path
from services.tiingo.ohlcv_fetcher import (
    TiingoClientConfig,
    TiingoOHLCVFetcher,
    normalize_tiingo_price_rows,
)
from services.tiingo.security_master import (
    TiingoSecurity,
    TiingoSecurityMaster,
    security_from_mapping,
)

FIXTURE_PATH = Path("data/sample/tiingo_ohlcv_response.json")


@pytest.fixture(autouse=True)
def no_local_tiingo_env_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep unit tests isolated from a developer's local config/tiingo.env."""
    monkeypatch.setattr(
        ohlcv_fetcher_module,
        "TIINGO_ENV_FILE",
        tmp_path / "does-not-exist-tiingo.env",
    )


class _FakeResponse:
    def __init__(self, payload: Any, content: bytes = b"") -> None:
        self._payload = payload
        self.content = content

    def json(self) -> Any:
        return self._payload

    def raise_for_status(self) -> None:
        return None


class _FakeSession:
    def __init__(self, response: _FakeResponse) -> None:
        self.response = response
        self.calls: list[dict[str, Any]] = []

    def get(self, url: str, **kwargs: Any) -> _FakeResponse:
        self.calls.append({"url": url, **kwargs})
        return self.response


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
    def __init__(self, records_by_ticker: dict[str, list[OHLCVRecord]]) -> None:
        self.records_by_ticker = records_by_ticker
        self.calls: list[tuple[str, str, str]] = []

    def fetch_security_records(
        self,
        security: TiingoSecurity,
        from_date: str,
        to_date: str,
    ) -> list[OHLCVRecord]:
        self.calls.append((security.ticker, from_date, to_date))
        return self.records_by_ticker.get(security.ticker, [])


def _fixture_payload() -> dict[str, Any]:
    return json.loads(FIXTURE_PATH.read_text())


def _supported_tickers_zip(csv_text: str) -> bytes:
    """Build an in-memory supported-tickers ZIP fixture."""
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, mode="w") as archive:
        archive.writestr("supported_tickers.csv", csv_text)
    return buffer.getvalue()


def test_client_config_from_env_reads_token(monkeypatch: pytest.MonkeyPatch) -> None:
    """Tiingo config reads its API token from the documented environment variable."""
    monkeypatch.setenv("TIINGO_API_TOKEN", "env-token")

    config = TiingoClientConfig.from_env()

    assert config.api_token == "env-token"
    assert config.base_url == "https://api.tiingo.com"
    assert config.timeout_seconds == 30


def test_client_config_from_env_rejects_missing_token(monkeypatch: pytest.MonkeyPatch) -> None:
    """Tiingo config fails closed when credentials are absent."""
    monkeypatch.delenv("TIINGO_API_TOKEN", raising=False)

    with pytest.raises(ValueError, match="TIINGO_API_TOKEN"):
        TiingoClientConfig.from_env()


def test_client_config_from_env_loads_local_config_file(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Tiingo config loads config/tiingo.env when shell variables are absent."""
    env_file = tmp_path / "tiingo.env"
    env_file.write_text("TIINGO_API_TOKEN=file-token\n", encoding="utf-8")
    monkeypatch.delenv("TIINGO_API_TOKEN", raising=False)
    monkeypatch.setattr(ohlcv_fetcher_module, "TIINGO_ENV_FILE", env_file)

    config = TiingoClientConfig.from_env()

    assert config.api_token == "file-token"


def test_fetch_price_rows_calls_tiingo_historical_endpoint() -> None:
    """Fetcher uses Tiingo's historical EOD endpoint and tokenized date params."""
    session = _FakeSession(_FakeResponse(_fixture_payload()["prices"]))
    fetcher = TiingoOHLCVFetcher(
        TiingoClientConfig(api_token="test-token", base_url="https://example.tiingo.test"),
        session=session,  # type: ignore[arg-type]
    )

    rows = fetcher.fetch_price_rows("AAPL", "2024-01-02", "2024-01-03")

    assert len(rows) == 2
    assert session.calls == [
        {
            "url": "https://example.tiingo.test/tiingo/daily/AAPL/prices",
            "params": {
                "startDate": "2024-01-02",
                "endDate": "2024-01-03",
                "token": "test-token",
            },
            "timeout": 30,
        }
    ]


def test_fetch_records_normalizes_symbol_for_endpoint_and_contract() -> None:
    """Fetcher normalizes ticker aliases before calling Tiingo and building records."""
    session = _FakeSession(_FakeResponse(_fixture_payload()["prices"]))
    fetcher = TiingoOHLCVFetcher(
        TiingoClientConfig(api_token="test-token", base_url="https://example.tiingo.test"),
        session=session,  # type: ignore[arg-type]
    )

    records = fetcher.fetch_records("brk.b", "2024-01-02", "2024-01-03")

    assert session.calls[0]["url"] == "https://example.tiingo.test/tiingo/daily/BRK-B/prices"
    assert {record.ticker for record in records} == {"BRK-B"}


def test_fetch_security_records_uses_resolved_tiingo_ticker() -> None:
    """Security-level fetching preserves the resolved Tiingo identity boundary."""
    session = _FakeSession(_FakeResponse(_fixture_payload()["prices"]))
    fetcher = TiingoOHLCVFetcher(
        TiingoClientConfig(api_token="test-token", base_url="https://example.tiingo.test"),
        session=session,  # type: ignore[arg-type]
    )
    security = TiingoSecurity(ticker="ABC", perma_ticker="PT_ABC_OLD")

    records = fetcher.fetch_security_records(security, "2024-01-02", "2024-01-03")

    assert session.calls[0]["url"] == "https://example.tiingo.test/tiingo/daily/ABC/prices"
    assert [record.ticker for record in records] == ["ABC", "ABC"]


def test_fetch_price_rows_rejects_non_list_payload() -> None:
    """Malformed Tiingo payloads fail fast instead of silently normalizing."""
    session = _FakeSession(_FakeResponse({"unexpected": "shape"}))
    fetcher = TiingoOHLCVFetcher(TiingoClientConfig(api_token="test-token"), session=session)  # type: ignore[arg-type]

    with pytest.raises(ValueError, match="JSON list"):
        fetcher.fetch_price_rows("AAPL", "2024-01-02", "2024-01-03")


def test_normalize_tiingo_price_rows_uses_adjusted_prices_and_contract_validation() -> None:
    """Tiingo rows normalize into schema-valid adjusted OHLCV records."""
    records = normalize_tiingo_price_rows("aapl", _fixture_payload()["prices"])

    assert [record.date for record in records] == ["2024-01-02", "2024-01-03"]
    assert records[0].ticker == "AAPL"
    assert records[0].open == 184.734
    assert records[0].high == 187.52
    assert records[0].low == 182.992
    assert records[0].close == 184.734
    assert records[0].adj_close == 184.734
    assert records[0].volume == 82488700
    assert records[0].dollar_volume == pytest.approx(184.734 * 82488700)


def test_normalize_tiingo_price_rows_returns_empty_for_empty_day() -> None:
    """An empty Tiingo response remains an empty record list."""
    assert normalize_tiingo_price_rows("AAPL", []) == []


def test_normalize_tiingo_price_rows_rejects_string_numeric_values() -> None:
    """Numeric strings must be parsed explicitly before contract construction."""
    row = dict(_fixture_payload()["prices"][0])
    row["adjClose"] = "184.734"

    with pytest.raises(TypeError, match="adjClose"):
        normalize_tiingo_price_rows("AAPL", [row])


def test_normalize_tiingo_price_rows_rejects_missing_required_price_fields() -> None:
    """Missing adjusted close fields fail before downstream feature code can use them."""
    row = dict(_fixture_payload()["prices"][0])
    del row["adjClose"]

    with pytest.raises(ValueError, match="adjClose"):
        normalize_tiingo_price_rows("AAPL", [row])


def test_normalize_tiingo_price_rows_rejects_nan_values() -> None:
    """NaN vendor values fail contract validation instead of entering storage."""
    row = dict(_fixture_payload()["prices"][0])
    row["adjClose"] = float("nan")

    with pytest.raises(ValueError, match="must be finite"):
        normalize_tiingo_price_rows("AAPL", [row])


def test_security_master_resolves_perma_ticker_security_id() -> None:
    """Security master uses permaTicker as the preferred stable archive key."""
    master = TiingoSecurityMaster.from_rows(_fixture_payload()["security_master"])

    security = master.resolve("aapl")

    assert security.ticker == "AAPL"
    assert security.perma_ticker == "PT_AAPL_001"
    assert security.security_id == "PT_AAPL_001"


def test_security_master_constructs_stable_id_when_perma_ticker_is_absent() -> None:
    """Ticker plus Tiingo date range prevents assuming ticker symbols are permanent."""
    master = TiingoSecurityMaster.from_rows(
        [
            {
                "ticker": "XYZ",
                "startDate": "2010-01-01",
                "endDate": "2012-12-31",
            }
        ]
    )

    assert master.resolve("XYZ").security_id == "XYZ_2010-01-01_2012-12-31"


def test_security_from_mapping_accepts_alternate_tiingo_fields() -> None:
    """Security parsing handles alternate Tiingo field names seen across exports."""
    security = security_from_mapping(
        {
            "symbol": "brk.b",
            "permaTickerId": 12345,
            "start_date": "1996-05-09",
            "end_date": "present",
            "exchangeCode": "NYSE",
            "asset_type": "Stock",
            "companyName": "Berkshire Hathaway Inc.",
        }
    )

    assert security.ticker == "BRK-B"
    assert security.perma_ticker == "12345"
    assert security.security_id == "12345"
    assert security.end_date is None


def test_security_master_resolve_many_preserves_reused_ticker_rows() -> None:
    """A reused ticker resolves to every historical security row, not just the latest one."""
    master = TiingoSecurityMaster.from_rows(
        [
            {
                "ticker": "XYZ",
                "permaTicker": "PT_XYZ_OLD",
                "startDate": "2010-01-01",
                "endDate": "2012-12-31",
            },
            {
                "ticker": "XYZ",
                "permaTicker": "PT_XYZ_NEW",
                "startDate": "2020-01-01",
                "endDate": None,
            },
        ]
    )

    assert [security.security_id for security in master.resolve_many(["XYZ"])] == [
        "PT_XYZ_NEW",
        "PT_XYZ_OLD",
    ]


def test_security_master_resolve_many_keeps_rows_with_shared_perma_ticker() -> None:
    """Ticker aliases sharing one permaTicker remain separate reference rows."""
    master = TiingoSecurityMaster.from_rows(
        [
            {
                "ticker": "OLD",
                "permaTicker": "PT_SHARED",
                "startDate": "2010-01-01",
                "endDate": "2012-12-31",
            },
            {
                "ticker": "NEW",
                "permaTicker": "PT_SHARED",
                "startDate": "2013-01-01",
                "endDate": None,
            },
        ]
    )

    assert [security.ticker for security in master.resolve_many(["OLD", "NEW"])] == [
        "OLD",
        "NEW",
    ]
    assert [security.security_id for security in master.resolve_many(["OLD", "NEW"])] == [
        "PT_SHARED",
        "PT_SHARED",
    ]


def test_security_master_parses_supported_tickers_zip() -> None:
    """Supported-ticker ZIP parsing preserves delisted/end-date metadata."""
    payload = _supported_tickers_zip(
        "ticker,permaTicker,startDate,endDate,exchange,assetType,name\n"
        "OLD,PT_OLD,2001-01-01,2010-12-31,NYSE,Stock,Old Co\n"
    )

    master = TiingoSecurityMaster.from_supported_tickers_zip(payload)
    security = master.resolve("OLD", as_of_date="2005-01-01")

    assert security.security_id == "PT_OLD"
    assert security.end_date == "2010-12-31"


def test_security_master_fetch_supported_tickers_uses_supplied_session() -> None:
    """Supported-ticker downloads are isolated behind an injectable HTTP session."""
    payload = _supported_tickers_zip(
        "ticker,permaTicker,startDate,endDate,exchange,assetType,name\n"
        "OLD,PT_OLD,2001-01-01,2010-12-31,NYSE,Stock,Old Co\n"
    )
    session = _FakeSession(_FakeResponse(payload={}, content=payload))

    master = TiingoSecurityMaster.fetch_supported_tickers(
        session=session,  # type: ignore[arg-type]
        url="https://example.tiingo.test/supported_tickers.zip",
        timeout_seconds=7,
    )

    assert session.calls == [
        {
            "url": "https://example.tiingo.test/supported_tickers.zip",
            "timeout": 7,
        }
    ]
    assert master.resolve("OLD").security_id == "PT_OLD"


def test_security_master_reference_rows_are_stable_and_serializable() -> None:
    """Reference-row output captures the symbol mapping needed to resolve archives."""
    master = TiingoSecurityMaster.from_rows(_fixture_payload()["security_master"])

    rows = master.to_reference_rows([master.resolve("ABC"), master.resolve("AAPL")])

    assert [row["security_id"] for row in rows] == ["PT_AAPL_001", "PT_ABC_OLD"]
    assert rows[0]["ticker"] == "AAPL"
    assert rows[1]["end_date"] == "2015-12-31"


def test_backfill_writes_price_archive_and_reference_mapping() -> None:
    """Backfill writes canonical R2 objects keyed by stable security identity."""
    records = normalize_tiingo_price_rows("AAPL", _fixture_payload()["prices"])
    master = TiingoSecurityMaster.from_rows(_fixture_payload()["security_master"])
    writer = _FakeWriter()
    fetcher = _FakeFetcher({"AAPL": records})

    result = backfill_ohlcv_archive(
        from_date=date(2024, 1, 2),
        to_date=date(2024, 1, 3),
        fetcher=fetcher,
        security_master=master,
        writer=writer,
        tickers=["AAPL"],
        record_serializer=_serialize_records_for_test,
    )

    price_key = raw_price_path("PT_AAPL_001")
    reference_key = raw_security_master_path("2024-01-03")
    assert result.requested == 1
    assert result.written == 1
    assert result.skipped == 0
    assert result.empty == 0
    assert result.reference_key == reference_key
    assert price_key in writer.objects
    assert reference_key in writer.objects

    archive_rows = json.loads(writer.objects[price_key])  # type: ignore[arg-type]
    assert [row["ticker"] for row in archive_rows] == ["AAPL", "AAPL"]
    assert [row["date"] for row in archive_rows] == ["2024-01-02", "2024-01-03"]

    reference = json.loads(writer.objects[reference_key])
    assert reference["source"] == "tiingo"
    assert reference["missing_tickers"] == []
    assert reference["securities"][0]["security_id"] == "PT_AAPL_001"


def test_backfill_combines_alias_rows_that_share_one_perma_ticker() -> None:
    """Backfill fetches every alias row before writing one stable-identity archive."""
    price_rows = _fixture_payload()["prices"]
    master = TiingoSecurityMaster.from_rows(
        [
            {
                "ticker": "OLD",
                "permaTicker": "PT_SHARED",
                "startDate": "2024-01-02",
                "endDate": "2024-01-02",
            },
            {
                "ticker": "NEW",
                "permaTicker": "PT_SHARED",
                "startDate": "2024-01-03",
                "endDate": None,
            },
        ]
    )
    writer = _FakeWriter()
    fetcher = _FakeFetcher(
        {
            "OLD": normalize_tiingo_price_rows("OLD", [price_rows[0]]),
            "NEW": normalize_tiingo_price_rows("NEW", [price_rows[1]]),
        }
    )

    result = backfill_ohlcv_archive(
        from_date=date(2024, 1, 2),
        to_date=date(2024, 1, 3),
        fetcher=fetcher,
        security_master=master,
        writer=writer,
        tickers=["OLD", "NEW"],
        record_serializer=_serialize_records_for_test,
    )

    price_key = raw_price_path("PT_SHARED")
    reference_key = raw_security_master_path("2024-01-03")
    archive_rows = json.loads(writer.objects[price_key])  # type: ignore[arg-type]
    reference_rows = json.loads(writer.objects[reference_key])["securities"]

    assert result.requested == 1
    assert result.written == 1
    assert fetcher.calls == [
        ("OLD", "2024-01-02", "2024-01-02"),
        ("NEW", "2024-01-03", "2024-01-03"),
    ]
    assert [row["ticker"] for row in archive_rows] == ["OLD", "NEW"]
    assert [row["ticker"] for row in reference_rows] == ["OLD", "NEW"]
    assert [row["security_id"] for row in reference_rows] == ["PT_SHARED", "PT_SHARED"]


def test_backfill_skips_missing_security_master_tickers() -> None:
    """Universe/security-master mismatches are reported without aborting the backfill."""
    records = normalize_tiingo_price_rows("AAPL", _fixture_payload()["prices"])
    master = TiingoSecurityMaster.from_rows(_fixture_payload()["security_master"])
    writer = _FakeWriter()
    fetcher = _FakeFetcher({"AAPL": records})

    result = backfill_ohlcv_archive(
        from_date=date(2024, 1, 2),
        to_date=date(2024, 1, 3),
        fetcher=fetcher,
        security_master=master,
        writer=writer,
        tickers=["AAPL", "MISSING"],
        record_serializer=_serialize_records_for_test,
    )

    reference = json.loads(writer.objects[result.reference_key])
    assert result.requested == 1
    assert result.written == 1
    assert result.missing_tickers == ("MISSING",)
    assert reference["missing_tickers"] == ["MISSING"]


def test_backfill_allows_empty_ticker_list_without_fetching_universe() -> None:
    """An explicit empty ticker scope writes an empty reference and makes no API calls."""
    master = TiingoSecurityMaster.from_rows(_fixture_payload()["security_master"])
    writer = _FakeWriter()
    fetcher = _FakeFetcher({})

    result = backfill_ohlcv_archive(
        from_date=date(2024, 1, 2),
        to_date=date(2024, 1, 3),
        fetcher=fetcher,
        security_master=master,
        writer=writer,
        tickers=[],
        record_serializer=_serialize_records_for_test,
    )

    reference_key = raw_security_master_path("2024-01-03")
    assert result.requested == 0
    assert result.written == 0
    assert result.skipped == 0
    assert result.empty == 0
    assert fetcher.calls == []
    assert json.loads(writer.objects[reference_key])["securities"] == []


def test_backfill_counts_empty_price_responses_without_writing_archive() -> None:
    """Empty Tiingo responses are counted and do not create empty price archives."""
    master = TiingoSecurityMaster.from_rows(_fixture_payload()["security_master"])
    writer = _FakeWriter()
    fetcher = _FakeFetcher({"AAPL": []})

    result = backfill_ohlcv_archive(
        from_date=date(2024, 1, 2),
        to_date=date(2024, 1, 3),
        fetcher=fetcher,
        security_master=master,
        writer=writer,
        tickers=["AAPL"],
        record_serializer=_serialize_records_for_test,
    )

    assert result.requested == 1
    assert result.written == 0
    assert result.empty == 1
    assert raw_price_path("PT_AAPL_001") not in writer.objects


def test_backfill_clamps_fetch_window_to_security_active_dates() -> None:
    """Backfill does not request dates outside a resolved security's Tiingo lifetime."""
    master = TiingoSecurityMaster.from_rows(
        [
            {
                "ticker": "ABC",
                "permaTicker": "PT_ABC_OLD",
                "startDate": "2010-01-01",
                "endDate": "2015-12-31",
            }
        ]
    )
    writer = _FakeWriter()
    fetcher = _FakeFetcher(
        {"ABC": normalize_tiingo_price_rows("ABC", _fixture_payload()["prices"])}
    )

    backfill_ohlcv_archive(
        from_date=date(2014, 1, 1),
        to_date=date(2020, 1, 1),
        fetcher=fetcher,
        security_master=master,
        writer=writer,
        tickers=["ABC"],
        record_serializer=_serialize_records_for_test,
    )

    assert fetcher.calls == [("ABC", "2014-01-01", "2015-12-31")]


def test_backfill_is_idempotent_for_existing_price_archive() -> None:
    """Existing R2 price archives are skipped unless overwrite is requested."""
    master = TiingoSecurityMaster.from_rows(_fixture_payload()["security_master"])
    price_key = raw_price_path("PT_AAPL_001")
    writer = _FakeWriter(existing={price_key})
    fetcher = _FakeFetcher(
        {"AAPL": normalize_tiingo_price_rows("AAPL", _fixture_payload()["prices"])}
    )

    result = backfill_ohlcv_archive(
        from_date=date(2024, 1, 2),
        to_date=date(2024, 1, 3),
        fetcher=fetcher,
        security_master=master,
        writer=writer,
        tickers=["AAPL"],
        record_serializer=_serialize_records_for_test,
    )

    assert result.requested == 1
    assert result.written == 0
    assert result.skipped == 1
    assert fetcher.calls == []
    assert price_key not in writer.objects


def test_parquet_serializer_reports_missing_pyarrow(monkeypatch: pytest.MonkeyPatch) -> None:
    """Parquet serialization fails with a clear dependency message when pyarrow is absent."""
    records = normalize_tiingo_price_rows("AAPL", _fixture_payload()["prices"])
    real_import_module = backfill_module.importlib.import_module

    def fake_import_module(name: str, package: str | None = None) -> object:
        if name == "pyarrow":
            raise ModuleNotFoundError("No module named 'pyarrow'")
        return real_import_module(name, package)

    monkeypatch.setattr(backfill_module.importlib, "import_module", fake_import_module)

    with pytest.raises(ModuleNotFoundError, match="pandas and pyarrow are required"):
        backfill_module._records_to_parquet_bytes(records)


def _serialize_records_for_test(records: list[OHLCVRecord]) -> bytes:
    """Serialize records as JSON bytes so unit tests do not require Parquet dependencies."""
    return json.dumps([record.model_dump() for record in records]).encode("utf-8")
