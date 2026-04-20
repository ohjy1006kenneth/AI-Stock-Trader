from __future__ import annotations

import csv
import io
import json
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

import pytest

from core.contracts.schemas import OHLCVRecord, RunStatus
from core.data.layer0_pipeline import (
    DailyLayer0Config,
    HistoricalLayer0Config,
    build_universe_mask_records,
    run_daily_layer0_incremental,
    run_historical_layer0_backfill,
)
from core.data.quality import QualityFilterConfig
from services.r2.paths import (
    pipeline_manifest_path,
    raw_fundamentals_path,
    raw_macro_path,
    raw_news_path,
    raw_price_path,
    raw_security_master_path,
    raw_universe_path,
)


@dataclass(frozen=True)
class _Security:
    ticker: str
    security_id: str
    start_date: str | None = None
    end_date: str | None = None

    def to_reference_row(self) -> dict[str, str | None]:
        return {
            "ticker": self.ticker,
            "security_id": self.security_id,
            "perma_ticker": self.security_id,
            "start_date": self.start_date,
            "end_date": self.end_date,
        }


class _Writer:
    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}
        self.put_counts: dict[str, int] = {}

    def put_object(self, key: str, data: bytes | str) -> None:
        self.objects[key] = data if isinstance(data, bytes) else data.encode("utf-8")
        self.put_counts[key] = self.put_counts.get(key, 0) + 1

    def exists(self, key: str) -> bool:
        return key in self.objects

    def get_object(self, key: str) -> bytes:
        return self.objects[key]

    def list_keys(self, prefix: str) -> list[str]:
        return sorted(k for k in self.objects if k.startswith(prefix))


class _UniverseProvider:
    def __init__(self) -> None:
        self.constituent_calls: list[str] = []

    def get_constituents(self, as_of_date: str) -> list[str]:
        self.constituent_calls.append(as_of_date)
        return ["AAPL", "MSFT"]

    def get_historical_tickers(self, from_date: str, to_date: str) -> set[str]:
        return {"AAPL", "MSFT"}


class _SecurityMaster:
    def resolve_all(self, ticker: str) -> list[_Security]:
        if ticker == "AAPL":
            return [_Security(ticker="AAPL", security_id="perm-aapl", start_date="2020-01-01")]
        if ticker == "MSFT":
            return [_Security(ticker="MSFT", security_id="perm-msft", start_date="2020-01-01")]
        raise KeyError(ticker)


class _HistoricalPriceFetcher:
    def __init__(self) -> None:
        self.calls: list[dict[str, str]] = []

    def fetch_security_records(
        self,
        security: _Security,
        from_date: str,
        to_date: str,
    ) -> list[OHLCVRecord]:
        self.calls.append({"ticker": security.ticker, "from_date": from_date, "to_date": to_date})
        return [_bar(date_value=from_date, ticker=security.ticker)]


class _LivePriceFetcher:
    def __init__(self, records: list[OHLCVRecord] | None = None) -> None:
        self.records = records or [_bar(date_value="2024-01-02", ticker="AAPL")]
        self.calls: list[dict[str, Any]] = []

    def fetch_live_daily_bars(
        self, *, tickers: list[str] | tuple[str, ...], as_of_date: str
    ) -> list[OHLCVRecord]:
        self.calls.append({"tickers": tuple(tickers), "as_of_date": as_of_date})
        return self.records


class _NewsFetcher:
    def __init__(self, should_raise: bool = False) -> None:
        self.should_raise = should_raise
        self.calls: list[dict[str, Any]] = []

    def fetch_news_day(
        self,
        *,
        tickers: list[str] | None,
        as_of_date: str,
        limit: int,
    ) -> list[dict[str, object]]:
        self.calls.append({"tickers": tickers, "as_of_date": as_of_date, "limit": limit})
        if self.should_raise:
            raise RuntimeError(
                "403 Client Error for url: "
                "https://data.alpaca.markets/v1beta1/news?api-key=secret-key"
            )
        return [{"id": f"news-{as_of_date}", "created_at": as_of_date, "symbols": tickers or []}]


class _FundamentalsFetcher:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def fetch_all_fundamentals(
        self,
        *,
        tickers: list[str] | tuple[str, ...],
        start_date: str,
        end_date: str,
        statements: list[str] | tuple[str, ...],
        periods: list[str] | tuple[str, ...],
        retrieved_at: datetime | None,
        limit: int,
    ) -> list[dict[str, object]]:
        self.calls.append(
            {
                "tickers": tuple(tickers),
                "start_date": start_date,
                "end_date": end_date,
                "statements": tuple(statements),
                "periods": tuple(periods),
                "retrieved_at": retrieved_at,
                "limit": limit,
            }
        )
        return [{"ticker": tickers[0], "report_date": start_date, "raw": {"x": 1}}]


class _MacroFetcher:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def fetch_all_macro_observations(
        self,
        *,
        series_ids: list[str] | tuple[str, ...],
        start_date: str,
        end_date: str,
        realtime_start: str | None,
        realtime_end: str | None,
        limit: int,
    ) -> list[dict[str, object]]:
        self.calls.append(
            {
                "series_ids": tuple(series_ids),
                "start_date": start_date,
                "end_date": end_date,
                "realtime_start": realtime_start,
                "realtime_end": realtime_end,
                "limit": limit,
            }
        )
        return [{"series_id": series_ids[0], "observation_date": start_date, "value": "1.0"}]


def _bar(*, date_value: str, ticker: str, dollar_volume: float = 2_000_000.0) -> OHLCVRecord:
    return OHLCVRecord(
        date=date_value,
        ticker=ticker,
        open=10.0,
        high=11.0,
        low=9.0,
        close=10.5,
        volume=100_000,
        adj_close=10.5,
        dollar_volume=dollar_volume,
    )


def _bytes_serializer(rows: list[Any]) -> bytes:
    payload = [row.model_dump() if hasattr(row, "model_dump") else row for row in rows]
    return json.dumps(payload, sort_keys=True, default=str).encode("utf-8")


def _bytes_deserializer(data: bytes) -> list[OHLCVRecord]:
    return [OHLCVRecord(**row) for row in json.loads(data)]


def _manifest(writer: _Writer, run_id: str) -> dict[str, Any]:
    key = pipeline_manifest_path("layer0", run_id)
    return json.loads(writer.objects[key])


def test_build_universe_mask_records_applies_quality_filters() -> None:
    records = build_universe_mask_records(
        as_of_date=date(2024, 1, 2),
        tickers=["aapl", "msft"],
        ohlcv_window={"AAPL": [_bar(date_value="2024-01-02", ticker="AAPL")]},
        quality_config=QualityFilterConfig(rolling_window_days=1),
    )

    by_ticker = {record.ticker: record for record in records}
    assert by_ticker["AAPL"].data_quality_ok is True
    assert by_ticker["MSFT"].data_quality_ok is False
    assert by_ticker["MSFT"].reason == "missing_ohlcv_window"


def test_historical_layer0_backfill_writes_all_raw_archives_and_manifest() -> None:
    writer = _Writer()
    run_id = "test-historical"
    config = HistoricalLayer0Config(
        from_date=date(2024, 1, 2),
        to_date=date(2024, 1, 3),
        fred_series_ids=("DGS10",),
        run_id=run_id,
        quality_config=QualityFilterConfig(rolling_window_days=1),
    )

    result = run_historical_layer0_backfill(
        config=config,
        universe_provider=_UniverseProvider(),
        price_fetcher=_HistoricalPriceFetcher(),
        security_master=_SecurityMaster(),
        news_fetcher=_NewsFetcher(),
        fundamentals_fetcher=_FundamentalsFetcher(),
        macro_fetcher=_MacroFetcher(),
        writer=writer,
        price_serializer=_bytes_serializer,
        price_deserializer=_bytes_deserializer,
        news_serializer=_bytes_serializer,
        fundamentals_serializer=_bytes_serializer,
        macro_serializer=_bytes_serializer,
    )

    expected_keys = {
        raw_security_master_path(date(2024, 1, 3)),
        raw_price_path("perm-aapl"),
        raw_price_path("perm-msft"),
        raw_universe_path("2024-01-02"),
        raw_universe_path("2024-01-03"),
        raw_news_path("2024-01-02"),
        raw_news_path("2024-01-03"),
        raw_fundamentals_path("AAPL"),
        raw_fundamentals_path("MSFT"),
        raw_macro_path("2024-01-02"),
        pipeline_manifest_path("layer0", run_id),
    }
    assert expected_keys.issubset(writer.objects)
    assert result.status == RunStatus.COMPLETED
    manifest = _manifest(writer, run_id)
    assert manifest["status"] == "completed"
    assert set(manifest["metadata"]["input_families"]) == {
        "universe",
        "prices",
        "news",
        "fundamentals",
        "macro",
        "manifest",
    }

    universe_rows = list(
        csv.DictReader(io.StringIO(writer.objects[raw_universe_path("2024-01-02")].decode()))
    )
    assert [row["ticker"] for row in universe_rows] == ["AAPL", "MSFT"]


def test_historical_backfill_reads_existing_prices_from_store_for_quality_masks() -> None:
    writer = _Writer()
    aapl_key = raw_price_path("perm-aapl")
    msft_key = raw_price_path("perm-msft")
    writer.put_object(aapl_key, _bytes_serializer([_bar(date_value="2024-01-02", ticker="AAPL")]))
    writer.put_object(msft_key, _bytes_serializer([_bar(date_value="2024-01-02", ticker="MSFT")]))
    writer.put_counts[aapl_key] = 0
    writer.put_counts[msft_key] = 0
    price_fetcher = _HistoricalPriceFetcher()

    run_historical_layer0_backfill(
        config=HistoricalLayer0Config(
            from_date=date(2024, 1, 2),
            to_date=date(2024, 1, 2),
            fred_series_ids=("DGS10",),
            run_id="test-existing-price-quality",
            quality_config=QualityFilterConfig(rolling_window_days=1),
        ),
        universe_provider=_UniverseProvider(),
        price_fetcher=price_fetcher,
        security_master=_SecurityMaster(),
        news_fetcher=_NewsFetcher(),
        fundamentals_fetcher=_FundamentalsFetcher(),
        macro_fetcher=_MacroFetcher(),
        writer=writer,
        price_serializer=_bytes_serializer,
        price_deserializer=_bytes_deserializer,
        news_serializer=_bytes_serializer,
        fundamentals_serializer=_bytes_serializer,
        macro_serializer=_bytes_serializer,
    )

    assert {aapl_key: writer.put_counts[aapl_key], msft_key: writer.put_counts[msft_key]} == {
        aapl_key: 0,
        msft_key: 0,
    }
    assert price_fetcher.calls == []
    universe_rows = list(
        csv.DictReader(io.StringIO(writer.objects[raw_universe_path("2024-01-02")].decode()))
    )
    assert {row["ticker"]: row["data_quality_ok"] for row in universe_rows} == {
        "AAPL": "True",
        "MSFT": "True",
    }


def test_daily_layer0_incremental_uses_alpaca_shape_and_canonical_paths() -> None:
    writer = _Writer()
    run_id = "test-daily"
    config = DailyLayer0Config(
        as_of_date=date(2024, 1, 2),
        tickers=("AAPL",),
        fred_series_ids=("dgs10",),
        run_id=run_id,
        quality_config=QualityFilterConfig(rolling_window_days=1),
    )
    live_fetcher = _LivePriceFetcher()

    result = run_daily_layer0_incremental(
        config=config,
        live_price_fetcher=live_fetcher,
        news_fetcher=_NewsFetcher(),
        fundamentals_fetcher=_FundamentalsFetcher(),
        macro_fetcher=_MacroFetcher(),
        writer=writer,
        price_serializer=_bytes_serializer,
        news_serializer=_bytes_serializer,
        fundamentals_serializer=_bytes_serializer,
        macro_serializer=_bytes_serializer,
    )

    assert live_fetcher.calls == [{"tickers": ("AAPL",), "as_of_date": "2024-01-02"}]
    assert raw_price_path("AAPL") in writer.objects
    assert raw_news_path("2024-01-02") in writer.objects
    assert raw_fundamentals_path("AAPL") in writer.objects
    assert raw_macro_path("2024-01-02") in writer.objects
    assert raw_universe_path("2024-01-02") in writer.objects
    assert pipeline_manifest_path("layer0", run_id) in writer.objects
    assert result.status == RunStatus.COMPLETED

    price_payload = json.loads(writer.objects[raw_price_path("AAPL")])
    assert price_payload[0]["ticker"] == "AAPL"
    assert price_payload[0]["date"] == "2024-01-02"


def test_daily_layer0_incremental_canonicalizes_dot_tickers_across_outputs() -> None:
    writer = _Writer()
    live_fetcher = _LivePriceFetcher(records=[_bar(date_value="2024-01-02", ticker="BRK.B")])

    run_daily_layer0_incremental(
        config=DailyLayer0Config(
            as_of_date=date(2024, 1, 2),
            tickers=("brk.b",),
            fred_series_ids=("DGS10",),
            run_id="test-dot-ticker",
            quality_config=QualityFilterConfig(rolling_window_days=1),
        ),
        live_price_fetcher=live_fetcher,
        news_fetcher=_NewsFetcher(),
        fundamentals_fetcher=_FundamentalsFetcher(),
        macro_fetcher=_MacroFetcher(),
        writer=writer,
        price_serializer=_bytes_serializer,
        news_serializer=_bytes_serializer,
        fundamentals_serializer=_bytes_serializer,
        macro_serializer=_bytes_serializer,
    )

    assert live_fetcher.calls == [{"tickers": ("BRK-B",), "as_of_date": "2024-01-02"}]
    assert raw_price_path("BRK-B") in writer.objects
    price_payload = json.loads(writer.objects[raw_price_path("BRK-B")])
    assert price_payload[0]["ticker"] == "BRK-B"

    universe_rows = list(
        csv.DictReader(io.StringIO(writer.objects[raw_universe_path("2024-01-02")].decode()))
    )
    assert universe_rows[0]["ticker"] == "BRK-B"
    assert universe_rows[0]["data_quality_ok"] == "True"


def test_daily_layer0_incremental_is_idempotent_for_existing_raw_outputs() -> None:
    writer = _Writer()
    keys = [
        raw_price_path("AAPL"),
        raw_news_path("2024-01-02"),
        raw_fundamentals_path("AAPL"),
        raw_macro_path("2024-01-02"),
        raw_universe_path("2024-01-02"),
    ]
    for key in keys:
        writer.put_object(key, b"existing")
        writer.put_counts[key] = 0

    run_daily_layer0_incremental(
        config=DailyLayer0Config(
            as_of_date=date(2024, 1, 2),
            tickers=("AAPL",),
            fred_series_ids=("DGS10",),
            run_id="test-idempotent",
            quality_config=QualityFilterConfig(rolling_window_days=1),
        ),
        live_price_fetcher=_LivePriceFetcher(),
        news_fetcher=_NewsFetcher(),
        fundamentals_fetcher=_FundamentalsFetcher(),
        macro_fetcher=_MacroFetcher(),
        writer=writer,
        price_serializer=_bytes_serializer,
        news_serializer=_bytes_serializer,
        fundamentals_serializer=_bytes_serializer,
        macro_serializer=_bytes_serializer,
    )

    assert {key: writer.put_counts[key] for key in keys} == {key: 0 for key in keys}
    assert writer.objects[pipeline_manifest_path("layer0", "test-idempotent")]


def test_layer0_pipeline_writes_failure_manifest_before_reraising() -> None:
    writer = _Writer()
    run_id = "test-failure"

    with pytest.raises(RuntimeError, match="403 Client Error"):
        run_daily_layer0_incremental(
            config=DailyLayer0Config(
                as_of_date=date(2024, 1, 2),
                tickers=("AAPL",),
                fred_series_ids=("DGS10",),
                run_id=run_id,
                quality_config=QualityFilterConfig(rolling_window_days=1),
            ),
            live_price_fetcher=_LivePriceFetcher(),
            news_fetcher=_NewsFetcher(should_raise=True),
            fundamentals_fetcher=_FundamentalsFetcher(),
            macro_fetcher=_MacroFetcher(),
            writer=writer,
            price_serializer=_bytes_serializer,
            news_serializer=_bytes_serializer,
            fundamentals_serializer=_bytes_serializer,
            macro_serializer=_bytes_serializer,
        )

    manifest = _manifest(writer, run_id)
    assert manifest["status"] == "failed"
    assert manifest["metadata"]["error"] == {
        "type": "RuntimeError",
        "message": (
            "403 Client Error for url: https://data.alpaca.markets/v1beta1/news?api-key=<redacted>"
        ),
    }
    assert raw_price_path("AAPL") in manifest["metadata"]["output_keys"]


def test_historical_backfill_skips_quality_reads_when_universe_masks_exist() -> None:
    """When all universe masks already exist, skip R2 reads for quality_window."""
    writer = _Writer()
    aapl_key = raw_price_path("perm-aapl")
    msft_key = raw_price_path("perm-msft")
    writer.put_object(aapl_key, _bytes_serializer([_bar(date_value="2024-01-02", ticker="AAPL")]))
    writer.put_object(msft_key, _bytes_serializer([_bar(date_value="2024-01-02", ticker="MSFT")]))
    # Pre-populate universe mask so _all_universe_masks_exist returns True
    writer.put_object(raw_universe_path("2024-01-02"), b"placeholder")
    # Pre-populate security master
    writer.put_object(raw_security_master_path("2024-01-02"), b"placeholder")
    writer.put_counts.clear()

    get_calls: list[str] = []
    original_get = writer.get_object

    def tracking_get(key: str) -> bytes:
        get_calls.append(key)
        return original_get(key)

    writer.get_object = tracking_get  # type: ignore[assignment]

    price_fetcher = _HistoricalPriceFetcher()

    run_historical_layer0_backfill(
        config=HistoricalLayer0Config(
            from_date=date(2024, 1, 2),
            to_date=date(2024, 1, 2),
            fred_series_ids=("DGS10",),
            run_id="test-skip-quality-reads",
            quality_config=QualityFilterConfig(rolling_window_days=1),
        ),
        universe_provider=_UniverseProvider(),
        price_fetcher=price_fetcher,
        security_master=_SecurityMaster(),
        news_fetcher=_NewsFetcher(),
        fundamentals_fetcher=_FundamentalsFetcher(),
        macro_fetcher=_MacroFetcher(),
        writer=writer,
        price_serializer=_bytes_serializer,
        price_deserializer=_bytes_deserializer,
        news_serializer=_bytes_serializer,
        fundamentals_serializer=_bytes_serializer,
        macro_serializer=_bytes_serializer,
    )

    # No Alpaca fetches and no R2 reads for price parquets
    assert price_fetcher.calls == []
    price_get_calls = [c for c in get_calls if c.startswith("raw/prices/")]
    assert price_get_calls == []


def test_layer0_config_rejects_empty_inputs() -> None:
    with pytest.raises(ValueError, match="tickers"):
        DailyLayer0Config(
            as_of_date=date(2024, 1, 2),
            tickers=(),
            fred_series_ids=("DGS10",),
        )
    with pytest.raises(ValueError, match="fred_series_ids"):
        HistoricalLayer0Config(
            from_date=date(2024, 1, 2),
            to_date=date(2024, 1, 3),
            fred_series_ids=(),
        )
