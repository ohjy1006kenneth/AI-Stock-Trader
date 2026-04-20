from __future__ import annotations

import pytest

from app.lab.data_pipelines import backfill_layer0
from services.wikipedia.sp500_universe import ChangeEvent


def test_wikipedia_universe_provider_reuses_parsed_tables(monkeypatch: pytest.MonkeyPatch) -> None:
    """Universe mask generation parses Wikipedia once for repeated constituent dates."""
    calls = {"fetch": 0, "current": 0, "events": 0, "reconstruct": 0}
    event = ChangeEvent(date="2010-01-01", added=frozenset(), removed=frozenset())

    def fake_fetch_html() -> str:
        calls["fetch"] += 1
        return "<html></html>"

    def fake_parse_current_tickers(html: str) -> set[str]:
        calls["current"] += 1
        assert html == "<html></html>"
        return {"AAPL"}

    def fake_parse_change_log(html: str) -> list[ChangeEvent]:
        calls["events"] += 1
        assert html == "<html></html>"
        return [event]

    def fake_reconstruct_at_date(
        current_tickers: set[str],
        events: list[ChangeEvent],
        as_of_date: str,
    ) -> list[str]:
        calls["reconstruct"] += 1
        assert current_tickers == {"AAPL"}
        assert events == [event]
        return sorted([*current_tickers, as_of_date])

    monkeypatch.setattr(backfill_layer0, "fetch_html", fake_fetch_html)
    monkeypatch.setattr(backfill_layer0, "parse_current_tickers", fake_parse_current_tickers)
    monkeypatch.setattr(backfill_layer0, "parse_change_log", fake_parse_change_log)
    monkeypatch.setattr(backfill_layer0, "reconstruct_at_date", fake_reconstruct_at_date)

    provider = backfill_layer0.WikipediaUniverseProvider()

    assert provider.get_constituents("2024-01-02") == ["2024-01-02", "AAPL"]
    assert provider.get_constituents("2024-01-03") == ["2024-01-03", "AAPL"]
    assert calls == {"fetch": 1, "current": 1, "events": 1, "reconstruct": 2}


def test_wikipedia_universe_provider_rejects_invalid_date() -> None:
    """Universe provider rejects malformed dates before fetching Wikipedia."""
    provider = backfill_layer0.WikipediaUniverseProvider()

    with pytest.raises(ValueError, match="as_of_date must be YYYY-MM-DD"):
        provider.get_constituents("20240102")
