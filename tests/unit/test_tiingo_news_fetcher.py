from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

import pytest

from app.lab.data_pipelines import backfill_news
from app.lab.data_pipelines.backfill_news import backfill_news_archive
from services.r2.paths import raw_news_path
from services.tiingo.news_fetcher import TiingoNewsFetcher
from services.tiingo.ohlcv_fetcher import TiingoClientConfig

FIXTURE_PATH = Path("data/sample/tiingo_news_response.json")


class _FakeResponse:
    def __init__(self, payload: Any) -> None:
        self._payload = payload

    def json(self) -> Any:
        return self._payload

    def raise_for_status(self) -> None:
        return None


class _FakeSession:
    def __init__(self, payloads: list[Any]) -> None:
        self._payloads = list(payloads)
        self.calls: list[dict[str, Any]] = []

    def get(self, url: str, **kwargs: Any) -> _FakeResponse:
        self.calls.append({"url": url, **kwargs})
        payload = self._payloads.pop(0) if self._payloads else []
        return _FakeResponse(payload)


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
    def __init__(self, rows_by_date: dict[str, list[dict[str, Any]]]) -> None:
        self.rows_by_date = rows_by_date
        self.calls: list[tuple[str, list[str] | None, int]] = []

    def fetch_news_day(
        self,
        *,
        tickers: list[str] | None,
        as_of_date: str,
        limit: int,
    ) -> list[dict[str, Any]]:
        self.calls.append((as_of_date, tickers, limit))
        return self.rows_by_date.get(as_of_date, [])


def _fixture_payload() -> dict[str, Any]:
    return json.loads(FIXTURE_PATH.read_text())


def _read_jsonl(payload: bytes | str) -> list[dict[str, Any]]:
    text = payload.decode("utf-8") if isinstance(payload, bytes) else payload
    lines = [line for line in text.splitlines() if line.strip()]
    return [json.loads(line) for line in lines]


def test_fetch_news_rows_calls_tiingo_news_endpoint() -> None:
    """Fetcher uses Tiingo news endpoint with normalized tickers and pagination params."""
    session = _FakeSession([_fixture_payload()["page1"]])
    fetcher = TiingoNewsFetcher(
        TiingoClientConfig(api_token="test-token", base_url="https://example.tiingo.test"),
        session=session,  # type: ignore[arg-type]
    )

    page = fetcher.fetch_news_rows(
        tickers=["AAPL", "brk.b"],
        start_date="2024-01-02",
        end_date="2024-01-03",
        limit=2,
        offset=0,
    )

    assert len(page.articles) == 2
    assert session.calls == [
        {
            "url": "https://example.tiingo.test/tiingo/news",
            "params": {
                "startDate": "2024-01-02",
                "endDate": "2024-01-03",
                "limit": 2,
                "offset": 0,
                "token": "test-token",
                "tickers": "AAPL,BRK-B",
            },
            "timeout": 30,
        }
    ]


def test_fetch_news_rows_rejects_non_list_payload() -> None:
    """Malformed Tiingo news payloads fail fast instead of silently normalizing."""
    session = _FakeSession([{"unexpected": "shape"}])
    fetcher = TiingoNewsFetcher(
        TiingoClientConfig(api_token="test-token"),
        session=session,  # type: ignore[arg-type]
    )

    with pytest.raises(ValueError, match="JSON list"):
        fetcher.fetch_news_rows(
            tickers=["AAPL"],
            start_date="2024-01-02",
            end_date="2024-01-02",
            limit=100,
            offset=0,
        )


def test_fetch_news_rows_rejects_non_object_items() -> None:
    """Malformed Tiingo list items fail with indexed diagnostics."""
    session = _FakeSession([[{"id": 1}, "not-an-object"]])
    fetcher = TiingoNewsFetcher(
        TiingoClientConfig(api_token="test-token"),
        session=session,  # type: ignore[arg-type]
    )

    with pytest.raises(ValueError, match="item 1 must be an object, got str"):
        fetcher.fetch_news_rows(
            tickers=["AAPL"],
            start_date="2024-01-02",
            end_date="2024-01-02",
            limit=100,
            offset=0,
        )


def test_fetch_all_news_paginates_and_deduplicates() -> None:
    """Pagination continues until exhaustion and deduplicates repeated articles."""
    payload = _fixture_payload()
    session = _FakeSession([payload["page1"], payload["page2"], payload["empty"]])
    fetcher = TiingoNewsFetcher(
        TiingoClientConfig(api_token="test-token", base_url="https://example.tiingo.test"),
        session=session,  # type: ignore[arg-type]
    )

    articles = fetcher.fetch_all_news(
        tickers=["AAPL", "MSFT"],
        start_date="2024-01-02",
        end_date="2024-01-02",
        limit=2,
    )

    assert [article["id"] for article in articles] == [1001, 1002, 1003]
    assert [call["params"]["offset"] for call in session.calls] == [0, 2, 4]


def test_fetch_news_day_returns_empty_for_empty_day() -> None:
    """Empty Tiingo days return an empty article list."""
    session = _FakeSession([_fixture_payload()["empty"]])
    fetcher = TiingoNewsFetcher(
        TiingoClientConfig(api_token="test-token"),
        session=session,  # type: ignore[arg-type]
    )

    assert fetcher.fetch_news_day(tickers=["AAPL"], as_of_date="2024-01-02") == []


def test_backfill_writes_jsonl_per_day() -> None:
    """Backfill writes one JSONL file per day with raw article fields preserved."""
    payload = _fixture_payload()
    writer = _FakeWriter()
    fetcher = _FakeFetcher({
        "2024-01-02": payload["page1"],
        "2024-01-03": payload["empty"],
    })

    result = backfill_news_archive(
        from_date=date(2024, 1, 2),
        to_date=date(2024, 1, 3),
        fetcher=fetcher,
        writer=writer,
        tickers=["AAPL"],
        limit=2,
    )

    first_key = raw_news_path("2024-01-02")
    second_key = raw_news_path("2024-01-03")
    assert result.requested == 2
    assert result.written == 2
    assert result.empty == 1
    assert first_key in writer.objects
    assert second_key in writer.objects

    first_rows = _read_jsonl(writer.objects[first_key])
    assert [row["id"] for row in first_rows] == [1001, 1002]
    assert "body" in first_rows[0]

    second_rows = _read_jsonl(writer.objects[second_key])
    assert second_rows == []


def test_backfill_is_idempotent_for_existing_archive() -> None:
    """Existing JSONL archives are skipped unless overwrite is requested."""
    writer = _FakeWriter(existing={raw_news_path("2024-01-02")})
    fetcher = _FakeFetcher({
        "2024-01-02": _fixture_payload()["page1"],
    })

    result = backfill_news_archive(
        from_date=date(2024, 1, 2),
        to_date=date(2024, 1, 2),
        fetcher=fetcher,
        writer=writer,
        tickers=["AAPL"],
        limit=10,
    )

    assert result.requested == 1
    assert result.written == 0
    assert result.skipped == 1
    assert fetcher.calls == []


def test_backfill_rejects_non_json_serializable_articles() -> None:
    """Raw archives fail fast instead of coercing unsupported values to strings."""
    writer = _FakeWriter()
    fetcher = _FakeFetcher({
        "2024-01-02": [{"id": 1, "publishedDate": "2024-01-02", "bad": object()}],
    })

    with pytest.raises(TypeError, match="not JSON serializable"):
        backfill_news_archive(
            from_date=date(2024, 1, 2),
            to_date=date(2024, 1, 2),
            fetcher=fetcher,
            writer=writer,
            tickers=["AAPL"],
            limit=10,
        )


def test_parse_args_rejects_empty_tickers_flag(monkeypatch: pytest.MonkeyPatch) -> None:
    """CLI rejects '--tickers' without symbols instead of running an unscoped pull."""
    monkeypatch.setattr(
        "sys.argv",
        [
            "backfill_news.py",
            "--from-date",
            "2024-01-02",
            "--to-date",
            "2024-01-02",
            "--tickers",
        ],
    )

    with pytest.raises(SystemExit):
        backfill_news._parse_args()
