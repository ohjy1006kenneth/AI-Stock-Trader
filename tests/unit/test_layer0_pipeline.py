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
        if ticker == "SPY":
            return [_Security(ticker="SPY", security_id="perm-spy", start_date="2020-01-01")]
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
        self.records = records
        self.calls: list[dict[str, Any]] = []

    def fetch_live_daily_bars(
        self, *, tickers: list[str] | tuple[str, ...], as_of_date: str
    ) -> list[OHLCVRecord]:
        self.calls.append({"tickers": tuple(tickers), "as_of_date": as_of_date})
        if self.records is not None:
            return self.records
        return [_bar(date_value=as_of_date, ticker=ticker) for ticker in tickers]


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
        return [{"ticker": ticker, "report_date": start_date, "raw": {"x": 1}} for ticker in tickers]


class _EmptyFundamentalsFetcher:
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
        return []


class _MacroFetcher:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []
        self.snapshot_calls: list[dict[str, Any]] = []

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

    def fetch_latest_available_macro_observations(
        self,
        *,
        series_ids: list[str] | tuple[str, ...],
        as_of_date: str,
        limit: int,
    ) -> list[dict[str, object]]:
        self.snapshot_calls.append(
            {
                "series_ids": tuple(series_ids),
                "as_of_date": as_of_date,
                "limit": limit,
            }
        )
        previous_day = (date.fromisoformat(as_of_date) - date.resolution).isoformat()
        return [
            {
                "series_id": series_ids[0],
                "observation_date": previous_day,
                "realtime_start": previous_day,
                "realtime_end": as_of_date,
                "retrieved_at": f"{as_of_date}T00:00:00+00:00",
                "value": 1.0,
                "is_missing": False,
                "raw": {"series_id": series_ids[0]},
            }
        ]


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


def _universe_serializer(rows: list[Any]) -> bytes:
    buffer = io.StringIO()
    writer = csv.DictWriter(
        buffer,
        fieldnames=[
            "date",
            "ticker",
            "in_universe",
            "tradable",
            "liquid",
            "halted",
            "data_quality_ok",
            "reason",
        ],
        lineterminator="\n",
    )
    writer.writeheader()
    for row in rows:
        writer.writerow(row.model_dump() if hasattr(row, "model_dump") else row)
    return buffer.getvalue().encode("utf-8")


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
        raw_price_path("perm-spy"),
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
    spy_key = raw_price_path("perm-spy")
    writer.put_object(aapl_key, _bytes_serializer([_bar(date_value="2024-01-02", ticker="AAPL")]))
    writer.put_object(msft_key, _bytes_serializer([_bar(date_value="2024-01-02", ticker="MSFT")]))
    writer.put_object(spy_key, _bytes_serializer([_bar(date_value="2024-01-02", ticker="SPY")]))
    writer.put_counts[aapl_key] = 0
    writer.put_counts[msft_key] = 0
    writer.put_counts[spy_key] = 0
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

    assert {
        aapl_key: writer.put_counts[aapl_key],
        msft_key: writer.put_counts[msft_key],
        spy_key: writer.put_counts[spy_key],
    } == {
        aapl_key: 0,
        msft_key: 0,
        spy_key: 0,
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
        price_deserializer=_bytes_deserializer,
        news_serializer=_bytes_serializer,
        fundamentals_serializer=_bytes_serializer,
        macro_serializer=_bytes_serializer,
    )

    assert live_fetcher.calls == [{"tickers": ("AAPL", "SPY"), "as_of_date": "2024-01-02"}]
    assert raw_price_path("AAPL") in writer.objects
    assert raw_price_path("SPY") in writer.objects
    assert raw_news_path("2024-01-02") in writer.objects
    assert raw_fundamentals_path("AAPL") in writer.objects
    assert raw_macro_path("2024-01-02") in writer.objects
    assert raw_universe_path("2024-01-02") in writer.objects
    assert pipeline_manifest_path("layer0", run_id) in writer.objects
    assert result.status == RunStatus.COMPLETED

    price_payload = json.loads(writer.objects[raw_price_path("AAPL")])
    assert price_payload[0]["ticker"] == "AAPL"
    assert price_payload[0]["date"] == "2024-01-02"


def test_daily_layer0_incremental_writes_run_date_macro_snapshot_from_latest_available_rows() -> None:
    """Daily Layer 0 keys macro shards by run date even when FRED lags one day."""
    writer = _Writer()
    macro_fetcher = _MacroFetcher()

    run_daily_layer0_incremental(
        config=DailyLayer0Config(
            as_of_date=date(2026, 5, 13),
            tickers=("AAPL",),
            fred_series_ids=("DGS10",),
            run_id="test-daily-macro-snapshot",
            quality_config=QualityFilterConfig(rolling_window_days=1),
        ),
        live_price_fetcher=_LivePriceFetcher(
            records=[
                _bar(date_value="2026-05-13", ticker="AAPL"),
                _bar(date_value="2026-05-13", ticker="SPY"),
            ]
        ),
        news_fetcher=_NewsFetcher(),
        fundamentals_fetcher=_FundamentalsFetcher(),
        macro_fetcher=macro_fetcher,
        writer=writer,
        price_serializer=_bytes_serializer,
        price_deserializer=_bytes_deserializer,
        news_serializer=_bytes_serializer,
        fundamentals_serializer=_bytes_serializer,
        macro_serializer=_bytes_serializer,
    )

    assert macro_fetcher.snapshot_calls == [
        {"series_ids": ("DGS10",), "as_of_date": "2026-05-13", "limit": 1000}
    ]
    payload = json.loads(writer.objects[raw_macro_path("2026-05-13")])
    assert payload == [
        {
            "is_missing": False,
            "observation_date": "2026-05-12",
            "raw": {"series_id": "DGS10"},
            "realtime_end": "2026-05-13",
            "realtime_start": "2026-05-12",
            "retrieved_at": "2026-05-13T00:00:00+00:00",
            "series_id": "DGS10",
            "value": 1.0,
        }
    ]


def test_daily_layer0_incremental_canonicalizes_dot_tickers_across_outputs() -> None:
    writer = _Writer()
    live_fetcher = _LivePriceFetcher(
        records=[
            _bar(date_value="2024-01-02", ticker="BRK.B"),
            _bar(date_value="2024-01-02", ticker="SPY"),
        ]
    )

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
        price_deserializer=_bytes_deserializer,
        news_serializer=_bytes_serializer,
        fundamentals_serializer=_bytes_serializer,
        macro_serializer=_bytes_serializer,
    )

    assert live_fetcher.calls == [{"tickers": ("BRK-B", "SPY"), "as_of_date": "2024-01-02"}]
    assert raw_price_path("BRK-B") in writer.objects
    assert raw_price_path("SPY") in writer.objects
    price_payload = json.loads(writer.objects[raw_price_path("BRK-B")])
    assert price_payload[0]["ticker"] == "BRK-B"

    universe_rows = list(
        csv.DictReader(io.StringIO(writer.objects[raw_universe_path("2024-01-02")].decode()))
    )
    assert universe_rows[0]["ticker"] == "BRK-B"
    assert universe_rows[0]["data_quality_ok"] == "True"


def test_daily_layer0_incremental_is_idempotent_for_existing_raw_outputs() -> None:
    writer = _Writer()
    price_key = raw_price_path("AAPL")
    benchmark_key = raw_price_path("SPY")
    universe_key = raw_universe_path("2024-01-02")
    keys = [
        price_key,
        benchmark_key,
        raw_news_path("2024-01-02"),
        raw_fundamentals_path("AAPL"),
        raw_macro_path("2024-01-02"),
        universe_key,
    ]
    writer.put_object(price_key, _bytes_serializer([_bar(date_value="2024-01-02", ticker="AAPL")]))
    writer.put_object(
        benchmark_key,
        _bytes_serializer([_bar(date_value="2024-01-02", ticker="SPY")]),
    )
    writer.put_object(raw_news_path("2024-01-02"), b"existing")
    writer.put_object(raw_fundamentals_path("AAPL"), b"existing")
    writer.put_object(raw_macro_path("2024-01-02"), b"existing")
    writer.put_object(
        universe_key,
        _universe_serializer(
            build_universe_mask_records(
                as_of_date=date(2024, 1, 2),
                tickers=("AAPL",),
                ohlcv_window={"AAPL": [_bar(date_value="2024-01-02", ticker="AAPL")]},
                quality_config=QualityFilterConfig(rolling_window_days=1),
            )
        ),
    )
    for key in keys:
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
        price_deserializer=_bytes_deserializer,
        news_serializer=_bytes_serializer,
        fundamentals_serializer=_bytes_serializer,
        macro_serializer=_bytes_serializer,
        universe_serializer=_universe_serializer,
    )

    assert {key: writer.put_counts[key] for key in keys} == {key: 0 for key in keys}
    assert writer.objects[pipeline_manifest_path("layer0", "test-idempotent")]


def test_daily_layer0_incremental_skips_new_empty_fundamentals_archives() -> None:
    """Missing fundamentals history is surfaced without creating a zero-row archive placeholder."""
    writer = _Writer()

    run_daily_layer0_incremental(
        config=DailyLayer0Config(
            as_of_date=date(2024, 1, 2),
            tickers=("AAPL",),
            fred_series_ids=("DGS10",),
            run_id="test-empty-fundamentals",
            quality_config=QualityFilterConfig(rolling_window_days=1),
        ),
        live_price_fetcher=_LivePriceFetcher(),
        news_fetcher=_NewsFetcher(),
        fundamentals_fetcher=_EmptyFundamentalsFetcher(),
        macro_fetcher=_MacroFetcher(),
        writer=writer,
        price_serializer=_bytes_serializer,
        price_deserializer=_bytes_deserializer,
        news_serializer=_bytes_serializer,
        fundamentals_serializer=_bytes_serializer,
        macro_serializer=_bytes_serializer,
        universe_serializer=_universe_serializer,
    )

    assert raw_fundamentals_path("AAPL") not in writer.objects
    manifest = _manifest(writer, "test-empty-fundamentals")
    assert manifest["metadata"]["fundamentals"]["empty"] == 1
    assert manifest["metadata"]["fundamentals"]["missing_tickers"] == ["AAPL"]


def test_daily_layer0_incremental_repairs_missing_target_date_and_rewrites_universe_mask() -> None:
    writer = _Writer()
    price_key = raw_price_path("AAPL")
    benchmark_key = raw_price_path("SPY")
    universe_key = raw_universe_path("2024-01-03")
    writer.put_object(price_key, _bytes_serializer([_bar(date_value="2024-01-02", ticker="AAPL")]))
    writer.put_object(
        benchmark_key,
        _bytes_serializer([_bar(date_value="2024-01-02", ticker="SPY")]),
    )
    writer.put_object(
        universe_key,
        _universe_serializer(
            [
                {
                    "date": "2024-01-03",
                    "ticker": "AAPL",
                    "in_universe": True,
                    "tradable": True,
                    "liquid": False,
                    "halted": False,
                    "data_quality_ok": False,
                    "reason": "missing_ohlcv_window",
                }
            ]
        ),
    )
    writer.put_counts[price_key] = 0
    writer.put_counts[benchmark_key] = 0
    writer.put_counts[universe_key] = 0

    run_daily_layer0_incremental(
        config=DailyLayer0Config(
            as_of_date=date(2024, 1, 3),
            tickers=("AAPL",),
            fred_series_ids=("DGS10",),
            run_id="test-repair-daily",
            quality_config=QualityFilterConfig(rolling_window_days=2),
        ),
        live_price_fetcher=_LivePriceFetcher(
            records=[
                _bar(date_value="2024-01-03", ticker="AAPL"),
                _bar(date_value="2024-01-03", ticker="SPY"),
            ]
        ),
        news_fetcher=_NewsFetcher(),
        fundamentals_fetcher=_FundamentalsFetcher(),
        macro_fetcher=_MacroFetcher(),
        writer=writer,
        price_serializer=_bytes_serializer,
        price_deserializer=_bytes_deserializer,
        news_serializer=_bytes_serializer,
        fundamentals_serializer=_bytes_serializer,
        macro_serializer=_bytes_serializer,
        universe_serializer=_universe_serializer,
    )

    price_payload = json.loads(writer.objects[price_key])
    assert [row["date"] for row in price_payload] == ["2024-01-02", "2024-01-03"]
    benchmark_payload = json.loads(writer.objects[benchmark_key])
    assert [row["date"] for row in benchmark_payload] == ["2024-01-02", "2024-01-03"]
    assert writer.put_counts[price_key] == 1
    assert writer.put_counts[benchmark_key] == 1
    assert writer.put_counts[universe_key] == 1
    universe_rows = list(csv.DictReader(io.StringIO(writer.objects[universe_key].decode("utf-8"))))
    assert universe_rows[0]["data_quality_ok"] == "True"


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
            price_deserializer=_bytes_deserializer,
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
    """When prices already cover the window, existing universe masks short-circuit recomputation."""
    writer = _Writer()
    aapl_key = raw_price_path("perm-aapl")
    msft_key = raw_price_path("perm-msft")
    spy_key = raw_price_path("perm-spy")
    writer.put_object(aapl_key, _bytes_serializer([_bar(date_value="2024-01-02", ticker="AAPL")]))
    writer.put_object(msft_key, _bytes_serializer([_bar(date_value="2024-01-02", ticker="MSFT")]))
    writer.put_object(spy_key, _bytes_serializer([_bar(date_value="2024-01-02", ticker="SPY")]))
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

    assert price_fetcher.calls == []
    assert raw_universe_path("2024-01-02") not in writer.put_counts
    assert any(call.startswith("raw/prices/") for call in get_calls)


def test_historical_backfill_repairs_existing_price_history_for_one_day_catch_up() -> None:
    writer = _Writer()
    stale_price_key = raw_price_path("perm-aapl")
    universe_key = raw_universe_path("2024-01-03")
    writer.put_object(
        stale_price_key,
        _bytes_serializer([_bar(date_value="2024-01-02", ticker="AAPL")]),
    )
    writer.put_object(
        universe_key,
        _universe_serializer(
            [
                {
                    "date": "2024-01-03",
                    "ticker": "AAPL",
                    "in_universe": True,
                    "tradable": True,
                    "liquid": False,
                    "halted": False,
                    "data_quality_ok": False,
                    "reason": "missing_ohlcv_window",
                }
            ]
        ),
    )
    writer.put_counts[stale_price_key] = 0
    writer.put_counts[universe_key] = 0

    class _SingleTickerUniverseProvider:
        def get_constituents(self, as_of_date: str) -> list[str]:
            return ["AAPL"]

        def get_historical_tickers(self, from_date: str, to_date: str) -> set[str]:
            return {"AAPL"}

    price_fetcher = _HistoricalPriceFetcher()

    run_historical_layer0_backfill(
        config=HistoricalLayer0Config(
            from_date=date(2024, 1, 3),
            to_date=date(2024, 1, 3),
            tickers=("AAPL",),
            fred_series_ids=("DGS10",),
            run_id="test-historical-repair",
            quality_config=QualityFilterConfig(rolling_window_days=2),
        ),
        universe_provider=_SingleTickerUniverseProvider(),
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
        universe_serializer=_universe_serializer,
    )

    assert price_fetcher.calls == [
        {"ticker": "AAPL", "from_date": "2024-01-03", "to_date": "2024-01-03"},
        {"ticker": "SPY", "from_date": "2024-01-03", "to_date": "2024-01-03"},
    ]
    price_payload = json.loads(writer.objects[stale_price_key])
    assert [row["date"] for row in price_payload] == ["2024-01-02", "2024-01-03"]
    assert writer.put_counts[stale_price_key] == 1
    assert writer.put_counts[universe_key] == 1
    universe_rows = list(csv.DictReader(io.StringIO(writer.objects[universe_key].decode("utf-8"))))
    assert universe_rows[0]["data_quality_ok"] == "True"


def test_historical_backfill_fetches_benchmark_prices_without_adding_benchmark_to_universe() -> None:
    writer = _Writer()
    price_fetcher = _HistoricalPriceFetcher()

    run_historical_layer0_backfill(
        config=HistoricalLayer0Config(
            from_date=date(2024, 1, 2),
            to_date=date(2024, 1, 2),
            fred_series_ids=("DGS10",),
            run_id="test-benchmark-historical",
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
        universe_serializer=_universe_serializer,
    )

    assert raw_price_path("perm-spy") in writer.objects
    assert {call["ticker"] for call in price_fetcher.calls} == {"AAPL", "MSFT", "SPY"}
    universe_rows = list(
        csv.DictReader(io.StringIO(writer.objects[raw_universe_path("2024-01-02")].decode()))
    )
    assert [row["ticker"] for row in universe_rows] == ["AAPL", "MSFT"]


def test_historical_backfill_repairs_stale_benchmark_archive_missing_target_date() -> None:
    writer = _Writer()
    benchmark_key = raw_price_path("perm-spy")
    writer.put_object(
        benchmark_key,
        _bytes_serializer([_bar(date_value="2024-01-02", ticker="SPY")]),
    )
    writer.put_counts[benchmark_key] = 0

    class _SpyOnlyUniverseProvider:
        def get_constituents(self, as_of_date: str) -> list[str]:
            return ["AAPL", "MSFT"]

        def get_historical_tickers(self, from_date: str, to_date: str) -> set[str]:
            return {"AAPL", "MSFT"}

    price_fetcher = _HistoricalPriceFetcher()

    run_historical_layer0_backfill(
        config=HistoricalLayer0Config(
            from_date=date(2024, 1, 3),
            to_date=date(2024, 1, 3),
            fred_series_ids=("DGS10",),
            run_id="test-benchmark-top-up",
            quality_config=QualityFilterConfig(rolling_window_days=2),
        ),
        universe_provider=_SpyOnlyUniverseProvider(),
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
        universe_serializer=_universe_serializer,
    )

    assert {"ticker": "SPY", "from_date": "2024-01-03", "to_date": "2024-01-03"} in price_fetcher.calls
    benchmark_payload = json.loads(writer.objects[benchmark_key])
    assert [row["date"] for row in benchmark_payload] == ["2024-01-02", "2024-01-03"]
    assert writer.put_counts[benchmark_key] == 1


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
