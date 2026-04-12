from __future__ import annotations

import csv
import importlib
import io
import json
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from typing import Protocol

from core.contracts.schemas import OHLCVRecord, PipelineManifestRecord, RunStatus, UniverseRecord
from core.data.quality import QualityFilterConfig, apply_quality_filters
from core.data.universe import build_universe_record
from services.r2.paths import (
    pipeline_manifest_path,
    raw_fundamentals_path,
    raw_macro_path,
    raw_news_path,
    raw_price_path,
    raw_security_master_path,
    raw_universe_path,
)


class ObjectWriter(Protocol):
    """Object-store methods required by the Layer 0 pipeline."""

    def put_object(self, key: str, data: bytes | str) -> None:
        """Write one object to storage."""

    def exists(self, key: str) -> bool:
        """Return True when an object key already exists."""


class HistoricalUniverseProvider(Protocol):
    """Point-in-time universe source used by historical Layer 0 backfills."""

    def get_constituents(self, as_of_date: str) -> list[str]:
        """Return index constituents as of one date."""

    def get_historical_tickers(self, from_date: str, to_date: str) -> set[str]:
        """Return all tickers that appeared in the index over a date range."""


class SecurityIdentity(Protocol):
    """Stable security identity row resolved from a ticker security master."""

    ticker: str
    security_id: str
    start_date: str | None
    end_date: str | None

    def to_reference_row(self) -> dict[str, str | None]:
        """Serialize a security identity for the reference archive."""


class SecurityMaster(Protocol):
    """Security-master methods required for historical Tiingo price archives."""

    def resolve_all(self, ticker: str) -> list[SecurityIdentity]:
        """Resolve one ticker to every historical security identity row."""


class HistoricalPriceFetcher(Protocol):
    """Historical OHLCV fetcher used by the backfill pipeline."""

    def fetch_security_records(
        self,
        security: SecurityIdentity,
        from_date: str,
        to_date: str,
    ) -> list[OHLCVRecord]:
        """Fetch validated OHLCV records for one security identity."""


class LivePriceFetcher(Protocol):
    """Live daily OHLCV fetcher used by the daily incremental pipeline."""

    def fetch_live_daily_bars(
        self,
        *,
        tickers: Sequence[str],
        as_of_date: str,
    ) -> list[OHLCVRecord]:
        """Fetch current daily bars normalized to OHLCVRecord."""


class NewsFetcher(Protocol):
    """Tiingo raw-news fetcher used by Layer 0 orchestration."""

    def fetch_news_day(
        self,
        *,
        tickers: list[str] | None,
        as_of_date: str,
        limit: int,
    ) -> list[dict[str, object]]:
        """Fetch raw news rows for one date."""


class FundamentalsFetcher(Protocol):
    """SimFin raw fundamentals fetcher used by Layer 0 orchestration."""

    def fetch_all_fundamentals(
        self,
        *,
        tickers: Sequence[str],
        start_date: str,
        end_date: str,
        statements: Sequence[str],
        periods: Sequence[str],
        retrieved_at: datetime | None,
        limit: int,
    ) -> list[dict[str, object]]:
        """Fetch raw fundamentals rows for a ticker/date range."""


class MacroFetcher(Protocol):
    """FRED raw macro/rates fetcher used by Layer 0 orchestration."""

    def fetch_all_macro_observations(
        self,
        *,
        series_ids: Sequence[str],
        start_date: str,
        end_date: str,
        realtime_start: str | None,
        realtime_end: str | None,
        limit: int,
    ) -> list[dict[str, object]]:
        """Fetch raw macro observations for configured series/date ranges."""


RecordSerializer = Callable[[list[OHLCVRecord]], bytes]
NewsSerializer = Callable[[list[dict[str, object]]], bytes]
RawRowSerializer = Callable[[list[dict[str, object]]], bytes]
UniverseSerializer = Callable[[list[UniverseRecord]], bytes]


@dataclass(frozen=True)
class HistoricalLayer0Config:
    """Configuration for a complete historical Layer 0 raw-data backfill."""

    from_date: date
    to_date: date
    tickers: Sequence[str] | None = None
    fred_series_ids: Sequence[str] = ()
    simfin_statements: Sequence[str] = ("pl", "bs", "cf", "derived")
    simfin_periods: Sequence[str] = ("q1", "q2", "q3", "q4", "fy")
    overwrite: bool = False
    news_limit: int = 100
    simfin_limit: int = 1000
    fred_limit: int = 1000
    quality_config: QualityFilterConfig = field(default_factory=QualityFilterConfig)
    quality_ohlcv_window: Mapping[str, Sequence[OHLCVRecord]] | None = None
    run_id: str | None = None

    def __post_init__(self) -> None:
        """Validate date windows, limits, and configured FRED series."""
        _validate_date_window(self.from_date, self.to_date)
        _validate_positive_limit(self.news_limit, "news_limit")
        _validate_positive_limit(self.simfin_limit, "simfin_limit")
        _validate_positive_limit(self.fred_limit, "fred_limit")
        if not self.fred_series_ids:
            raise ValueError("fred_series_ids must contain at least one series")
        if not self.simfin_statements:
            raise ValueError("simfin_statements must contain at least one statement")
        if not self.simfin_periods:
            raise ValueError("simfin_periods must contain at least one period")


@dataclass(frozen=True)
class DailyLayer0Config:
    """Configuration for one daily Layer 0 incremental ingest run."""

    as_of_date: date
    tickers: Sequence[str]
    fred_series_ids: Sequence[str]
    simfin_statements: Sequence[str] = ("pl", "bs", "cf", "derived")
    simfin_periods: Sequence[str] = ("q1", "q2", "q3", "q4", "fy")
    overwrite: bool = False
    news_limit: int = 100
    simfin_limit: int = 1000
    fred_limit: int = 1000
    quality_config: QualityFilterConfig = field(default_factory=QualityFilterConfig)
    quality_ohlcv_window: Mapping[str, Sequence[OHLCVRecord]] | None = None
    run_id: str | None = None

    def __post_init__(self) -> None:
        """Validate daily ticker and vendor-fetch settings."""
        if not _normalize_tickers(self.tickers):
            raise ValueError("tickers must contain at least one ticker")
        if not self.fred_series_ids:
            raise ValueError("fred_series_ids must contain at least one series")
        if not self.simfin_statements:
            raise ValueError("simfin_statements must contain at least one statement")
        if not self.simfin_periods:
            raise ValueError("simfin_periods must contain at least one period")
        _validate_positive_limit(self.news_limit, "news_limit")
        _validate_positive_limit(self.simfin_limit, "simfin_limit")
        _validate_positive_limit(self.fred_limit, "fred_limit")


@dataclass(frozen=True)
class Layer0PipelineResult:
    """Completion summary for one Layer 0 pipeline run."""

    run_id: str
    manifest_key: str
    status: RunStatus
    output_keys: tuple[str, ...]
    metadata: dict[str, object]


def run_historical_layer0_backfill(
    *,
    config: HistoricalLayer0Config,
    universe_provider: HistoricalUniverseProvider,
    price_fetcher: HistoricalPriceFetcher,
    security_master: SecurityMaster,
    news_fetcher: NewsFetcher,
    fundamentals_fetcher: FundamentalsFetcher,
    macro_fetcher: MacroFetcher,
    writer: ObjectWriter,
    price_serializer: RecordSerializer | None = None,
    news_serializer: NewsSerializer | None = None,
    fundamentals_serializer: RawRowSerializer | None = None,
    macro_serializer: RawRowSerializer | None = None,
    universe_serializer: UniverseSerializer | None = None,
) -> Layer0PipelineResult:
    """Run the complete historical Layer 0 ingest and write a success/failure manifest."""
    run_id = config.run_id or f"layer0-historical-{config.from_date}_to_{config.to_date}"
    started_at = datetime.now(UTC)
    output_keys: list[str] = []
    metadata: dict[str, object] = _base_metadata(
        mode="historical_backfill",
        from_date=config.from_date.isoformat(),
        to_date=config.to_date.isoformat(),
        fred_series_ids=_normalize_tokens(config.fred_series_ids),
    )

    try:
        tickers = _resolve_backfill_tickers(config=config, universe_provider=universe_provider)
        quality_window = _copy_ohlcv_window(config.quality_ohlcv_window)

        price_result = _backfill_historical_prices(
            config=config,
            tickers=tickers,
            price_fetcher=price_fetcher,
            security_master=security_master,
            writer=writer,
            serializer=price_serializer or _records_to_parquet_bytes,
            quality_window=quality_window,
        )
        output_keys.extend(price_result.output_keys)
        metadata["prices"] = price_result.metadata

        universe_result = _write_historical_universe_masks(
            config=config,
            universe_provider=universe_provider,
            writer=writer,
            ohlcv_window=quality_window,
            serializer=universe_serializer or _universe_to_csv_bytes,
        )
        output_keys.extend(universe_result.output_keys)
        metadata["universe"] = universe_result.metadata

        news_result = _backfill_news(
            from_date=config.from_date,
            to_date=config.to_date,
            tickers=tickers,
            limit=config.news_limit,
            overwrite=config.overwrite,
            fetcher=news_fetcher,
            writer=writer,
            serializer=news_serializer or _articles_to_jsonl_bytes,
        )
        output_keys.extend(news_result.output_keys)
        metadata["news"] = news_result.metadata

        fundamentals_key = raw_fundamentals_path(config.from_date, config.to_date)
        fundamentals_result = _write_fundamentals_archive(
            key=fundamentals_key,
            from_date=config.from_date,
            to_date=config.to_date,
            tickers=tickers,
            statements=config.simfin_statements,
            periods=config.simfin_periods,
            limit=config.simfin_limit,
            overwrite=config.overwrite,
            fetcher=fundamentals_fetcher,
            writer=writer,
            serializer=fundamentals_serializer or _raw_rows_to_parquet_bytes,
        )
        output_keys.extend(fundamentals_result.output_keys)
        metadata["fundamentals"] = fundamentals_result.metadata

        macro_key = raw_macro_path(config.from_date, config.to_date)
        macro_result = _write_macro_archive(
            key=macro_key,
            from_date=config.from_date,
            to_date=config.to_date,
            series_ids=config.fred_series_ids,
            limit=config.fred_limit,
            overwrite=config.overwrite,
            fetcher=macro_fetcher,
            writer=writer,
            serializer=macro_serializer or _raw_rows_to_parquet_bytes,
        )
        output_keys.extend(macro_result.output_keys)
        metadata["macro"] = macro_result.metadata

        manifest_key = _write_pipeline_manifest(
            writer=writer,
            run_id=run_id,
            status=RunStatus.COMPLETED,
            started_at=started_at,
            metadata=metadata | {"output_keys": sorted(set(output_keys))},
        )
        return Layer0PipelineResult(
            run_id=run_id,
            manifest_key=manifest_key,
            status=RunStatus.COMPLETED,
            output_keys=tuple(sorted(set(output_keys + [manifest_key]))),
            metadata=metadata,
        )
    except Exception as exc:
        manifest_key = _write_failure_manifest(
            writer=writer,
            run_id=run_id,
            started_at=started_at,
            metadata=metadata,
            output_keys=output_keys,
            exc=exc,
        )
        output_keys.append(manifest_key)
        raise


def run_daily_layer0_incremental(
    *,
    config: DailyLayer0Config,
    live_price_fetcher: LivePriceFetcher,
    news_fetcher: NewsFetcher,
    fundamentals_fetcher: FundamentalsFetcher,
    macro_fetcher: MacroFetcher,
    writer: ObjectWriter,
    price_serializer: RecordSerializer | None = None,
    news_serializer: NewsSerializer | None = None,
    fundamentals_serializer: RawRowSerializer | None = None,
    macro_serializer: RawRowSerializer | None = None,
    universe_serializer: UniverseSerializer | None = None,
) -> Layer0PipelineResult:
    """Run one daily Layer 0 ingest and write a success/failure manifest."""
    run_id = config.run_id or f"layer0-daily-{config.as_of_date}"
    started_at = datetime.now(UTC)
    as_of_date = config.as_of_date
    tickers = _normalize_tickers(config.tickers)
    output_keys: list[str] = []
    metadata: dict[str, object] = _base_metadata(
        mode="daily_incremental",
        from_date=as_of_date.isoformat(),
        to_date=as_of_date.isoformat(),
        fred_series_ids=_normalize_tokens(config.fred_series_ids),
    )

    try:
        daily_records = _canonicalize_ohlcv_records(
            live_price_fetcher.fetch_live_daily_bars(
                tickers=tickers,
                as_of_date=as_of_date.isoformat(),
            )
        )
        price_result = _write_daily_prices(
            records=daily_records,
            overwrite=config.overwrite,
            writer=writer,
            serializer=price_serializer or _records_to_parquet_bytes,
        )
        output_keys.extend(price_result.output_keys)
        metadata["prices"] = price_result.metadata

        quality_window = _copy_ohlcv_window(config.quality_ohlcv_window)
        for record in daily_records:
            quality_window.setdefault(_canonicalize_ticker(record.ticker), []).append(record)
        universe_records = build_universe_mask_records(
            as_of_date=as_of_date,
            tickers=tickers,
            ohlcv_window=quality_window,
            quality_config=config.quality_config,
        )
        universe_result = _write_universe_mask(
            as_of_date=as_of_date,
            records=universe_records,
            overwrite=config.overwrite,
            writer=writer,
            serializer=universe_serializer or _universe_to_csv_bytes,
        )
        output_keys.extend(universe_result.output_keys)
        metadata["universe"] = universe_result.metadata

        news_result = _backfill_news(
            from_date=as_of_date,
            to_date=as_of_date,
            tickers=list(tickers),
            limit=config.news_limit,
            overwrite=config.overwrite,
            fetcher=news_fetcher,
            writer=writer,
            serializer=news_serializer or _articles_to_jsonl_bytes,
        )
        output_keys.extend(news_result.output_keys)
        metadata["news"] = news_result.metadata

        fundamentals_key = raw_fundamentals_path(as_of_date, as_of_date)
        fundamentals_result = _write_fundamentals_archive(
            key=fundamentals_key,
            from_date=as_of_date,
            to_date=as_of_date,
            tickers=list(tickers),
            statements=config.simfin_statements,
            periods=config.simfin_periods,
            limit=config.simfin_limit,
            overwrite=config.overwrite,
            fetcher=fundamentals_fetcher,
            writer=writer,
            serializer=fundamentals_serializer or _raw_rows_to_parquet_bytes,
        )
        output_keys.extend(fundamentals_result.output_keys)
        metadata["fundamentals"] = fundamentals_result.metadata

        macro_key = raw_macro_path(as_of_date, as_of_date)
        macro_result = _write_macro_archive(
            key=macro_key,
            from_date=as_of_date,
            to_date=as_of_date,
            series_ids=config.fred_series_ids,
            limit=config.fred_limit,
            overwrite=config.overwrite,
            fetcher=macro_fetcher,
            writer=writer,
            serializer=macro_serializer or _raw_rows_to_parquet_bytes,
        )
        output_keys.extend(macro_result.output_keys)
        metadata["macro"] = macro_result.metadata

        manifest_key = _write_pipeline_manifest(
            writer=writer,
            run_id=run_id,
            status=RunStatus.COMPLETED,
            started_at=started_at,
            metadata=metadata | {"output_keys": sorted(set(output_keys))},
        )
        return Layer0PipelineResult(
            run_id=run_id,
            manifest_key=manifest_key,
            status=RunStatus.COMPLETED,
            output_keys=tuple(sorted(set(output_keys + [manifest_key]))),
            metadata=metadata,
        )
    except Exception as exc:
        manifest_key = _write_failure_manifest(
            writer=writer,
            run_id=run_id,
            started_at=started_at,
            metadata=metadata,
            output_keys=output_keys,
            exc=exc,
        )
        output_keys.append(manifest_key)
        raise


def build_universe_mask_records(
    *,
    as_of_date: date,
    tickers: Sequence[str],
    ohlcv_window: Mapping[str, Sequence[OHLCVRecord]],
    quality_config: QualityFilterConfig,
) -> list[UniverseRecord]:
    """Build schema-valid point-in-time universe rows with quality flags applied."""
    records = [
        build_universe_record(
            {"date": as_of_date.isoformat(), "ticker": ticker, "in_universe": True}
        )
        for ticker in _normalize_tickers(tickers)
    ]
    return apply_quality_filters(records, ohlcv_window, quality_config)


@dataclass(frozen=True)
class _WriteResult:
    output_keys: list[str]
    metadata: dict[str, object]


def _resolve_backfill_tickers(
    *,
    config: HistoricalLayer0Config,
    universe_provider: HistoricalUniverseProvider,
) -> list[str]:
    if config.tickers is not None:
        tickers = _normalize_tickers(config.tickers)
    else:
        tickers = _normalize_tickers(
            universe_provider.get_historical_tickers(
                config.from_date.isoformat(),
                config.to_date.isoformat(),
            )
        )
    if not tickers:
        raise ValueError("historical backfill ticker universe is empty")
    return list(tickers)


def _backfill_historical_prices(
    *,
    config: HistoricalLayer0Config,
    tickers: Sequence[str],
    price_fetcher: HistoricalPriceFetcher,
    security_master: SecurityMaster,
    writer: ObjectWriter,
    serializer: RecordSerializer,
    quality_window: dict[str, list[OHLCVRecord]],
) -> _WriteResult:
    securities, missing_tickers = _resolve_securities(
        security_master=security_master, tickers=tickers
    )
    active_securities = [
        security
        for security in securities
        if _security_overlaps_range(security, config.from_date, config.to_date)
    ]
    grouped = _group_securities_by_id(active_securities)
    output_keys: list[str] = []
    written = 0
    skipped = 0
    empty = 0

    reference_key = raw_security_master_path(config.to_date)
    if not writer.exists(reference_key) or config.overwrite:
        reference_payload = _security_reference_payload(
            from_date=config.from_date,
            to_date=config.to_date,
            securities=active_securities,
            missing_tickers=missing_tickers,
        )
        writer.put_object(reference_key, json.dumps(reference_payload, sort_keys=True))
        output_keys.append(reference_key)
    else:
        skipped += 1

    for security_id, security_rows in grouped.items():
        key = raw_price_path(security_id)
        should_write = config.overwrite or not writer.exists(key)
        if not should_write:
            skipped += 1

        records: list[OHLCVRecord] = []
        for security in security_rows:
            fetch_from, fetch_to = _security_fetch_range(security, config.from_date, config.to_date)
            fetched = _canonicalize_ohlcv_records(
                price_fetcher.fetch_security_records(
                    security=security,
                    from_date=fetch_from.isoformat(),
                    to_date=fetch_to.isoformat(),
                )
            )
            records.extend(fetched)
            for record in fetched:
                quality_window.setdefault(_canonicalize_ticker(record.ticker), []).append(record)

        if not records:
            empty += 1
            continue

        if should_write:
            writer.put_object(key, serializer(_sort_ohlcv_records(records)))
            output_keys.append(key)
            written += 1

    return _WriteResult(
        output_keys=output_keys,
        metadata={
            "requested_tickers": len(tickers),
            "resolved_securities": len(grouped),
            "written": written,
            "skipped": skipped,
            "empty": empty,
            "missing_tickers": list(missing_tickers),
            "reference_key": reference_key,
            "output_keys": output_keys,
        },
    )


def _write_historical_universe_masks(
    *,
    config: HistoricalLayer0Config,
    universe_provider: HistoricalUniverseProvider,
    writer: ObjectWriter,
    ohlcv_window: Mapping[str, Sequence[OHLCVRecord]],
    serializer: UniverseSerializer,
) -> _WriteResult:
    output_keys: list[str] = []
    written = 0
    skipped = 0
    total_records = 0
    days = _business_days(config.from_date, config.to_date)

    for current_date in days:
        if writer.exists(raw_universe_path(current_date)) and not config.overwrite:
            skipped += 1
            continue
        tickers = universe_provider.get_constituents(current_date.isoformat())
        records = build_universe_mask_records(
            as_of_date=current_date,
            tickers=tickers,
            ohlcv_window=ohlcv_window,
            quality_config=config.quality_config,
        )
        result = _write_universe_mask(
            as_of_date=current_date,
            records=records,
            overwrite=config.overwrite,
            writer=writer,
            serializer=serializer,
        )
        output_keys.extend(result.output_keys)
        total_records += len(records)
        written += int(result.metadata["written"])
        skipped += int(result.metadata["skipped"])

    return _WriteResult(
        output_keys=output_keys,
        metadata={
            "requested_days": len(days),
            "written": written,
            "skipped": skipped,
            "total_records": total_records,
            "output_keys": output_keys,
        },
    )


def _write_daily_prices(
    *,
    records: Sequence[OHLCVRecord],
    overwrite: bool,
    writer: ObjectWriter,
    serializer: RecordSerializer,
) -> _WriteResult:
    grouped: dict[str, list[OHLCVRecord]] = {}
    for record in records:
        grouped.setdefault(record.ticker.upper(), []).append(record)

    output_keys: list[str] = []
    written = 0
    skipped = 0
    for ticker, ticker_records in sorted(grouped.items()):
        key = raw_price_path(ticker)
        if writer.exists(key) and not overwrite:
            skipped += 1
            continue
        writer.put_object(key, serializer(_sort_ohlcv_records(ticker_records)))
        output_keys.append(key)
        written += 1

    return _WriteResult(
        output_keys=output_keys,
        metadata={
            "requested_records": len(records),
            "written": written,
            "skipped": skipped,
            "empty": 0 if records else 1,
            "output_keys": output_keys,
        },
    )


def _write_universe_mask(
    *,
    as_of_date: date,
    records: list[UniverseRecord],
    overwrite: bool,
    writer: ObjectWriter,
    serializer: UniverseSerializer,
) -> _WriteResult:
    key = raw_universe_path(as_of_date)
    if writer.exists(key) and not overwrite:
        return _WriteResult(output_keys=[], metadata={"written": 0, "skipped": 1, "key": key})
    writer.put_object(key, serializer(_sort_universe_records(records)))
    return _WriteResult(
        output_keys=[key],
        metadata={"written": 1, "skipped": 0, "rows": len(records), "key": key},
    )


def _backfill_news(
    *,
    from_date: date,
    to_date: date,
    tickers: list[str],
    limit: int,
    overwrite: bool,
    fetcher: NewsFetcher,
    writer: ObjectWriter,
    serializer: NewsSerializer,
) -> _WriteResult:
    output_keys: list[str] = []
    written = 0
    skipped = 0
    empty = 0
    total_articles = 0
    days = _date_range(from_date, to_date)

    for current_date in days:
        key = raw_news_path(current_date)
        if writer.exists(key) and not overwrite:
            skipped += 1
            continue
        articles = fetcher.fetch_news_day(
            tickers=tickers,
            as_of_date=current_date.isoformat(),
            limit=limit,
        )
        if not articles:
            empty += 1
        total_articles += len(articles)
        writer.put_object(key, serializer(_sort_articles(articles)))
        output_keys.append(key)
        written += 1

    return _WriteResult(
        output_keys=output_keys,
        metadata={
            "requested_days": len(days),
            "written": written,
            "skipped": skipped,
            "empty": empty,
            "total_articles": total_articles,
            "output_keys": output_keys,
        },
    )


def _write_fundamentals_archive(
    *,
    key: str,
    from_date: date,
    to_date: date,
    tickers: Sequence[str],
    statements: Sequence[str],
    periods: Sequence[str],
    limit: int,
    overwrite: bool,
    fetcher: FundamentalsFetcher,
    writer: ObjectWriter,
    serializer: RawRowSerializer,
) -> _WriteResult:
    if writer.exists(key) and not overwrite:
        return _WriteResult(
            output_keys=[],
            metadata={"requested_tickers": len(tickers), "written": 0, "skipped": 1, "key": key},
        )
    rows = fetcher.fetch_all_fundamentals(
        tickers=tickers,
        start_date=from_date.isoformat(),
        end_date=to_date.isoformat(),
        statements=statements,
        periods=periods,
        retrieved_at=datetime.now(UTC),
        limit=limit,
    )
    writer.put_object(key, serializer(_sort_raw_rows(rows, ("ticker", "report_date"))))
    return _WriteResult(
        output_keys=[key],
        metadata={
            "requested_tickers": len(tickers),
            "written": 1,
            "skipped": 0,
            "empty": 0 if rows else 1,
            "total_rows": len(rows),
            "key": key,
        },
    )


def _write_macro_archive(
    *,
    key: str,
    from_date: date,
    to_date: date,
    series_ids: Sequence[str],
    limit: int,
    overwrite: bool,
    fetcher: MacroFetcher,
    writer: ObjectWriter,
    serializer: RawRowSerializer,
) -> _WriteResult:
    if writer.exists(key) and not overwrite:
        return _WriteResult(
            output_keys=[],
            metadata={"requested_series": len(series_ids), "written": 0, "skipped": 1, "key": key},
        )
    normalized_series = _normalize_tokens(series_ids)
    rows = fetcher.fetch_all_macro_observations(
        series_ids=normalized_series,
        start_date=from_date.isoformat(),
        end_date=to_date.isoformat(),
        realtime_start=from_date.isoformat(),
        realtime_end=to_date.isoformat(),
        limit=limit,
    )
    writer.put_object(key, serializer(_sort_raw_rows(rows, ("series_id", "observation_date"))))
    return _WriteResult(
        output_keys=[key],
        metadata={
            "requested_series": len(normalized_series),
            "written": 1,
            "skipped": 0,
            "empty": 0 if rows else 1,
            "total_rows": len(rows),
            "key": key,
        },
    )


def _write_pipeline_manifest(
    *,
    writer: ObjectWriter,
    run_id: str,
    status: RunStatus,
    started_at: datetime,
    metadata: dict[str, object],
) -> str:
    manifest_key = pipeline_manifest_path("layer0", run_id)
    manifest = PipelineManifestRecord(
        run_id=run_id,
        stage="layer0",
        status=status,
        started_at=started_at,
        finished_at=datetime.now(UTC),
        input_path="external:layer0",
        output_path="raw/",
        metadata=metadata,
    )
    writer.put_object(manifest_key, manifest.model_dump_json())
    return manifest_key


def _write_failure_manifest(
    *,
    writer: ObjectWriter,
    run_id: str,
    started_at: datetime,
    metadata: dict[str, object],
    output_keys: Sequence[str],
    exc: Exception,
) -> str:
    failure_metadata = dict(metadata)
    failure_metadata["output_keys"] = sorted(set(output_keys))
    failure_metadata["error"] = {"type": type(exc).__name__, "message": str(exc)}
    return _write_pipeline_manifest(
        writer=writer,
        run_id=run_id,
        status=RunStatus.FAILED,
        started_at=started_at,
        metadata=failure_metadata,
    )


def _base_metadata(
    *,
    mode: str,
    from_date: str,
    to_date: str,
    fred_series_ids: tuple[str, ...],
) -> dict[str, object]:
    return {
        "mode": mode,
        "from_date": from_date,
        "to_date": to_date,
        "input_families": {
            "universe": "wikipedia_sp500_membership",
            "prices": "tiingo_historical_or_alpaca_daily_bars",
            "news": "tiingo_news",
            "fundamentals": "simfin_as_reported",
            "macro": "fred_macro_rates",
            "manifest": "pipeline_manifest",
        },
        "fred_series_ids": list(fred_series_ids),
    }


def _records_to_parquet_bytes(records: list[OHLCVRecord]) -> bytes:
    try:
        import pandas as pd

        importlib.import_module("pyarrow")
    except ModuleNotFoundError as exc:
        raise ModuleNotFoundError(
            "pandas and pyarrow are required to serialize OHLCV records to Parquet."
        ) from exc

    frame = pd.DataFrame([record.model_dump() for record in records])
    buffer = io.BytesIO()
    frame.to_parquet(buffer, index=False)
    return buffer.getvalue()


def _raw_rows_to_parquet_bytes(rows: list[dict[str, object]]) -> bytes:
    try:
        import pandas as pd

        importlib.import_module("pyarrow")
    except ModuleNotFoundError as exc:
        raise ModuleNotFoundError(
            "pandas and pyarrow are required to serialize raw Layer 0 rows to Parquet."
        ) from exc

    frame = pd.DataFrame([_parquet_ready_row(row) for row in rows])
    buffer = io.BytesIO()
    frame.to_parquet(buffer, index=False)
    return buffer.getvalue()


def _parquet_ready_row(row: dict[str, object]) -> dict[str, object]:
    output = dict(row)
    raw = output.pop("raw", None)
    if raw is not None:
        output["raw_json"] = json.dumps(raw, sort_keys=True, separators=(",", ":"))
    return output


def _articles_to_jsonl_bytes(articles: list[dict[str, object]]) -> bytes:
    if not articles:
        return b""
    lines = [json.dumps(article, sort_keys=True, separators=(",", ":")) for article in articles]
    return ("\n".join(lines) + "\n").encode("utf-8")


def _universe_to_csv_bytes(records: list[UniverseRecord]) -> bytes:
    buffer = io.StringIO()
    fieldnames = [
        "date",
        "ticker",
        "in_universe",
        "tradable",
        "liquid",
        "halted",
        "data_quality_ok",
        "reason",
    ]
    writer = csv.DictWriter(buffer, fieldnames=fieldnames, lineterminator="\n")
    writer.writeheader()
    for record in records:
        writer.writerow(record.model_dump())
    return buffer.getvalue().encode("utf-8")


def _copy_ohlcv_window(
    window: Mapping[str, Sequence[OHLCVRecord]] | None,
) -> dict[str, list[OHLCVRecord]]:
    if window is None:
        return {}
    return {_canonicalize_ticker(ticker): list(records) for ticker, records in window.items()}


def _resolve_securities(
    *,
    security_master: SecurityMaster,
    tickers: Sequence[str],
) -> tuple[list[SecurityIdentity], tuple[str, ...]]:
    resolved: list[SecurityIdentity] = []
    missing: list[str] = []
    seen_ids: set[tuple[str, str | None, str | None, str]] = set()
    for ticker in tickers:
        try:
            securities = security_master.resolve_all(ticker)
        except KeyError:
            missing.append(ticker)
            continue
        for security in securities:
            key = (security.ticker, security.start_date, security.end_date, security.security_id)
            if key not in seen_ids:
                seen_ids.add(key)
                resolved.append(security)
    return sorted(resolved, key=_security_sort_key), tuple(sorted(missing))


def _group_securities_by_id(
    securities: Iterable[SecurityIdentity],
) -> dict[str, list[SecurityIdentity]]:
    grouped: dict[str, list[SecurityIdentity]] = {}
    for security in securities:
        grouped.setdefault(security.security_id, []).append(security)
    return {
        security_id: sorted(rows, key=_security_sort_key)
        for security_id, rows in sorted(grouped.items())
    }


def _security_reference_payload(
    *,
    from_date: date,
    to_date: date,
    securities: Sequence[SecurityIdentity],
    missing_tickers: Sequence[str],
) -> dict[str, object]:
    return {
        "source": "tiingo",
        "from_date": from_date.isoformat(),
        "to_date": to_date.isoformat(),
        "generated_at": datetime.now(UTC).isoformat(),
        "missing_tickers": list(missing_tickers),
        "securities": [security.to_reference_row() for security in securities],
    }


def _security_overlaps_range(security: SecurityIdentity, from_date: date, to_date: date) -> bool:
    if security.end_date is not None and date.fromisoformat(security.end_date) < from_date:
        return False
    if security.start_date is not None and date.fromisoformat(security.start_date) > to_date:
        return False
    return True


def _security_fetch_range(
    security: SecurityIdentity, from_date: date, to_date: date
) -> tuple[date, date]:
    fetch_from = (
        max(from_date, date.fromisoformat(security.start_date))
        if security.start_date
        else from_date
    )
    fetch_to = min(to_date, date.fromisoformat(security.end_date)) if security.end_date else to_date
    return fetch_from, fetch_to


def _security_sort_key(security: SecurityIdentity) -> tuple[str, str, str, str]:
    return (
        security.security_id,
        security.start_date or "0000-00-00",
        security.end_date or "9999-99-99",
        security.ticker,
    )


def _normalize_tickers(tickers: Iterable[str]) -> tuple[str, ...]:
    normalized: set[str] = set()
    for ticker in tickers:
        if not isinstance(ticker, str):
            raise TypeError("tickers must be strings")
        cleaned = _canonicalize_ticker(ticker)
        if not cleaned:
            raise ValueError("tickers cannot contain empty values")
        normalized.add(cleaned)
    return tuple(sorted(normalized))


def _canonicalize_ticker(ticker: str) -> str:
    return ticker.strip().upper().replace(".", "-")


def _canonicalize_ohlcv_records(records: Sequence[OHLCVRecord]) -> list[OHLCVRecord]:
    return [
        record.model_copy(update={"ticker": _canonicalize_ticker(record.ticker)})
        for record in records
    ]


def _normalize_tokens(values: Sequence[str]) -> tuple[str, ...]:
    normalized: set[str] = set()
    for value in values:
        if not isinstance(value, str):
            raise TypeError("configured tokens must be strings")
        cleaned = value.strip().upper()
        if not cleaned:
            raise ValueError("configured tokens cannot contain empty values")
        normalized.add(cleaned)
    return tuple(sorted(normalized))


def _validate_date_window(from_date: date, to_date: date) -> None:
    if from_date > to_date:
        raise ValueError("from_date must be <= to_date")


def _validate_positive_limit(value: int, field_name: str) -> None:
    if value <= 0:
        raise ValueError(f"{field_name} must be positive")


def _date_range(start: date, end: date) -> list[date]:
    days: list[date] = []
    current = start
    while current <= end:
        days.append(current)
        current += timedelta(days=1)
    return days


def _business_days(start: date, end: date) -> list[date]:
    return [day for day in _date_range(start, end) if day.weekday() < 5]


def _sort_ohlcv_records(records: Sequence[OHLCVRecord]) -> list[OHLCVRecord]:
    return sorted(records, key=lambda record: (record.date, record.ticker))


def _sort_universe_records(records: Sequence[UniverseRecord]) -> list[UniverseRecord]:
    return sorted(records, key=lambda record: (record.date, record.ticker))


def _sort_articles(articles: Sequence[dict[str, object]]) -> list[dict[str, object]]:
    return sorted(
        articles,
        key=lambda article: (
            str(article.get("publishedDate") or article.get("published_at") or ""),
            str(article.get("id") or article.get("url") or ""),
        ),
    )


def _sort_raw_rows(
    rows: Sequence[dict[str, object]],
    keys: Sequence[str],
) -> list[dict[str, object]]:
    return sorted(rows, key=lambda row: tuple(str(row.get(key) or "") for key in keys))
