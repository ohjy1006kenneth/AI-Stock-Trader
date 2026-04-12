from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pytest
import requests

from app.lab.data_pipelines import backfill_fred
from app.lab.data_pipelines.backfill_fred import backfill_fred_archive
from services.fred.macro_fetcher import (
    DEFAULT_FRED_PAGE_LIMIT,
    FredClientConfig,
    FredMacroFetcher,
    load_fred_archive_config,
    normalize_fred_observations,
)
from services.r2.paths import raw_macro_path

FIXTURE_PATH = Path("data/sample/fred_series_response.json")


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
        return self._responses.pop(0) if self._responses else _FakeResponse({"observations": []})


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

    def fetch_all_macro_observations(self, **kwargs: Any) -> list[dict[str, Any]]:
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
    """FRED config reads API settings from environment variables."""
    monkeypatch.setenv("FRED_API_KEY", "test-key")
    monkeypatch.setenv("FRED_BASE_URL", "https://example.fred.test/fred")

    config = FredClientConfig.from_env()

    assert config.api_key == "test-key"
    assert config.base_url == "https://example.fred.test/fred"
    assert config.timeout_seconds == 30


def test_client_config_from_env_rejects_missing_key(monkeypatch: pytest.MonkeyPatch) -> None:
    """FRED config fails closed when credentials are absent."""
    monkeypatch.delenv("FRED_API_KEY", raising=False)

    with pytest.raises(ValueError, match="FRED_API_KEY"):
        FredClientConfig.from_env()


def test_load_fred_archive_config_reads_baseline_series() -> None:
    """Default FRED config provides the baseline regime/context series."""
    config = load_fred_archive_config()

    assert config.default_start_date == "2014-01-01"
    assert config.default_end_date == "latest"
    assert {"FEDFUNDS", "DGS10", "DGS2", "CPIAUCSL"}.issubset(config.series_ids)


def test_load_fred_archive_config_rejects_duplicate_series(tmp_path: Path) -> None:
    """Series selection is explicit and duplicate IDs fail fast."""
    config_path = tmp_path / "fred_series.json"
    config_path.write_text(
        json.dumps(
            {
                "default_start_date": "2024-01-01",
                "default_end_date": "2024-12-31",
                "series": [{"id": "FEDFUNDS"}, {"id": "fedfunds"}],
            }
        )
    )

    with pytest.raises(ValueError, match="Duplicate FRED series"):
        load_fred_archive_config(config_path)


def test_load_fred_archive_config_rejects_non_string_series_id(tmp_path: Path) -> None:
    """Configured FRED series IDs must be explicit strings."""
    config_path = tmp_path / "fred_series.json"
    config_path.write_text(
        json.dumps(
            {
                "default_start_date": "2024-01-01",
                "default_end_date": "2024-12-31",
                "series": [{"id": 123}],
            }
        )
    )

    with pytest.raises(TypeError, match="id must be a string"):
        load_fred_archive_config(config_path)


def test_fetch_series_page_calls_fred_endpoint_and_normalizes_rows() -> None:
    """Fetcher calls FRED's observations endpoint with scoped params."""
    fixture = _fixture_payload()
    session = _FakeSession([_FakeResponse(fixture["page1"])])
    fetcher = FredMacroFetcher(
        FredClientConfig(
            api_key="test-key",
            base_url="https://example.fred.test/fred",
            retry_sleep_seconds=0,
        ),
        session=session,  # type: ignore[arg-type]
    )

    page = fetcher.fetch_series_page(
        series_id="fedfunds",
        start_date="2024-01-01",
        end_date="2024-12-31",
        limit=2,
        offset=0,
    )

    assert [(row["series_id"], row["observation_date"], row["value"]) for row in page.rows] == [
        ("FEDFUNDS", "2024-01-01", 5.33),
        ("FEDFUNDS", "2024-01-02", None),
    ]
    assert page.rows[1]["is_missing"] is True
    assert session.calls == [
        {
            "url": "https://example.fred.test/fred/series/observations",
            "params": {
                "series_id": "FEDFUNDS",
                "observation_start": "2024-01-01",
                "observation_end": "2024-12-31",
                "realtime_start": "2024-01-01",
                "realtime_end": "2024-12-31",
                "sort_order": "asc",
                "file_type": "json",
                "output_type": 1,
                "limit": 2,
                "offset": 0,
                "api_key": "test-key",
            },
            "timeout": 30,
        }
    ]


def test_fetch_series_observations_paginates_and_deduplicates() -> None:
    """Pagination continues until exhaustion and deduplicates repeated rows."""
    fixture = _fixture_payload()
    session = _FakeSession([
        _FakeResponse(fixture["page1"]),
        _FakeResponse(fixture["page2"]),
        _FakeResponse(fixture["empty"]),
    ])
    fetcher = FredMacroFetcher(
        FredClientConfig(api_key="test-key", retry_sleep_seconds=0),
        session=session,  # type: ignore[arg-type]
    )

    rows = fetcher.fetch_series_observations(
        series_id="FEDFUNDS",
        start_date="2024-01-01",
        end_date="2024-12-31",
        limit=2,
    )

    assert [(row["observation_date"], row["value"]) for row in rows] == [
        ("2024-01-01", 5.33),
        ("2024-01-02", None),
        ("2024-01-03", 5.35),
    ]
    assert [call["params"]["offset"] for call in session.calls] == [0, 2, 4]


def test_fetch_series_page_accepts_explicit_realtime_window() -> None:
    """Historical pulls can request a bounded FRED realtime vintage window."""
    fixture = _fixture_payload()
    session = _FakeSession([_FakeResponse(fixture["page1"])])
    fetcher = FredMacroFetcher(
        FredClientConfig(api_key="test-key", retry_sleep_seconds=0),
        session=session,  # type: ignore[arg-type]
    )

    fetcher.fetch_series_page(
        series_id="CPIAUCSL",
        start_date="2024-01-01",
        end_date="2024-12-31",
        realtime_start="2024-02-01",
        realtime_end="2024-03-01",
    )

    assert session.calls[0]["params"]["realtime_start"] == "2024-02-01"
    assert session.calls[0]["params"]["realtime_end"] == "2024-03-01"


def test_fetch_series_page_rejects_invalid_realtime_window() -> None:
    """Realtime vintage windows must be ordered."""
    fetcher = FredMacroFetcher(FredClientConfig(api_key="test-key"))

    with pytest.raises(ValueError, match="realtime_start"):
        fetcher.fetch_series_page(
            series_id="CPIAUCSL",
            start_date="2024-01-01",
            end_date="2024-12-31",
            realtime_start="2024-03-01",
            realtime_end="2024-02-01",
        )


def test_fetch_series_page_accepts_empty_response() -> None:
    """Empty FRED responses normalize to an empty page."""
    fixture = _fixture_payload()
    session = _FakeSession([_FakeResponse(fixture["empty"])])
    fetcher = FredMacroFetcher(
        FredClientConfig(api_key="test-key"),
        session=session,  # type: ignore[arg-type]
    )

    page = fetcher.fetch_series_page(
        series_id="DGS10",
        start_date="2024-01-01",
        end_date="2024-12-31",
    )

    assert page.rows == []


def test_fetch_series_page_rejects_malformed_required_fields() -> None:
    """Malformed FRED observations fail with actionable diagnostics."""
    fixture = _fixture_payload()
    session = _FakeSession([_FakeResponse(fixture["malformed_missing_date"])])
    fetcher = FredMacroFetcher(
        FredClientConfig(api_key="test-key"),
        session=session,  # type: ignore[arg-type]
    )

    with pytest.raises(ValueError, match="observation_date"):
        fetcher.fetch_series_page(
            series_id="FEDFUNDS",
            start_date="2024-01-01",
            end_date="2024-12-31",
        )


def test_normalize_rejects_malformed_numeric_value() -> None:
    """FRED values must be numeric strings or the '.' missing-value sentinel."""
    with pytest.raises(ValueError, match="numeric"):
        normalize_fred_observations(
            [
                {
                    "realtime_start": "2024-01-01",
                    "realtime_end": "2024-12-31",
                    "date": "2024-01-01",
                    "value": "not-a-number",
                }
            ],
            series_id="FEDFUNDS",
            retrieved_at=datetime(2024, 1, 4, tzinfo=UTC),
        )


def test_fetch_series_page_retries_transient_errors() -> None:
    """Retryable provider errors are retried before succeeding."""
    response = requests.Response()
    response.status_code = 503
    error = requests.HTTPError("temporarily unavailable", response=response)
    fixture = _fixture_payload()
    session = _FakeSession([_FakeResponse({}, error), _FakeResponse(fixture["page1"])])
    fetcher = FredMacroFetcher(
        FredClientConfig(api_key="test-key", retry_sleep_seconds=0),
        session=session,  # type: ignore[arg-type]
    )

    page = fetcher.fetch_series_page(
        series_id="FEDFUNDS",
        start_date="2024-01-01",
        end_date="2024-12-31",
    )

    assert len(page.rows) == 2
    assert len(session.calls) == 2


def test_normalize_preserves_retrieval_and_realtime_dates() -> None:
    """Point-in-time macro archives preserve observation and as-of metadata."""
    rows = normalize_fred_observations(
        [
            {
                "realtime_start": "2024-01-01",
                "realtime_end": "2024-12-31",
                "date": "2024-01-02",
                "value": ".",
            }
        ],
        series_id="DGS10",
        retrieved_at=datetime(2024, 1, 4, tzinfo=UTC),
    )

    assert rows == [
        {
            "source": "fred",
            "series_id": "DGS10",
            "observation_date": "2024-01-02",
            "realtime_start": "2024-01-01",
            "realtime_end": "2024-12-31",
            "retrieved_at": "2024-01-04T00:00:00+00:00",
            "value": None,
            "is_missing": True,
            "raw": {
                "realtime_start": "2024-01-01",
                "realtime_end": "2024-12-31",
                "date": "2024-01-02",
                "value": ".",
            },
        }
    ]


def test_backfill_writes_raw_macro_archive() -> None:
    """Backfill writes one deterministic raw macro archive for the range."""
    rows = normalize_fred_observations(
        _fixture_payload()["page1"]["observations"],
        series_id="FEDFUNDS",
        retrieved_at=datetime(2024, 1, 4, tzinfo=UTC),
    )
    writer = _FakeWriter()
    fetcher = _FakeFetcher(rows)

    result = backfill_fred_archive(
        from_date=date(2024, 1, 1),
        to_date=date(2024, 12, 31),
        fetcher=fetcher,
        writer=writer,
        series_ids=["FEDFUNDS", "DGS10"],
        serializer=_json_serializer,
        limit=DEFAULT_FRED_PAGE_LIMIT,
    )

    key = raw_macro_path(date(2024, 1, 1), date(2024, 12, 31))
    assert result.output_key == key
    assert result.requested_series == 2
    assert result.written == 1
    assert result.total_rows == 2
    assert key in writer.objects
    stored_rows = _read_json(writer.objects[key])
    assert [row["observation_date"] for row in stored_rows] == ["2024-01-01", "2024-01-02"]
    assert "raw" in stored_rows[0]
    assert fetcher.calls[0]["series_ids"] == ("DGS10", "FEDFUNDS")
    assert fetcher.calls[0]["realtime_start"] == "2024-01-01"
    assert fetcher.calls[0]["realtime_end"] == "2024-12-31"


def test_backfill_is_idempotent_for_existing_archive() -> None:
    """Existing FRED archives are skipped unless overwrite is requested."""
    key = raw_macro_path(date(2024, 1, 1), date(2024, 12, 31))
    writer = _FakeWriter(existing={key})
    fetcher = _FakeFetcher([])

    result = backfill_fred_archive(
        from_date=date(2024, 1, 1),
        to_date=date(2024, 12, 31),
        fetcher=fetcher,
        writer=writer,
        series_ids=["FEDFUNDS"],
        serializer=_json_serializer,
    )

    assert result.written == 0
    assert result.skipped == 1
    assert fetcher.calls == []


def test_parse_args_rejects_empty_series_ids_flag(monkeypatch: pytest.MonkeyPatch) -> None:
    """CLI rejects '--series-ids' without symbols instead of running an unscoped pull."""
    monkeypatch.setattr(
        "sys.argv",
        [
            "backfill_fred.py",
            "--from-date",
            "2024-01-01",
            "--to-date",
            "2024-12-31",
            "--series-ids",
        ],
    )

    with pytest.raises(SystemExit):
        backfill_fred._parse_args()
