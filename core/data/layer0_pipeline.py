from __future__ import annotations

import csv
import importlib
import io
import json
import re
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from typing import Protocol

from loguru import logger

from core.contracts.schemas import OHLCVRecord, PipelineManifestRecord, RunStatus, UniverseRecord
from core.data.quality import (
    QualityFilterConfig,
    SharesOutstandingSnapshot,
    apply_prepared_quality_filters,
    apply_quality_filters,
    prepare_quality_windows,
)
from core.data.universe import build_universe_record
from services.r2.paths import (
    layer0_ohlcv_provenance_report_path,
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

    def get_object(self, key: str) -> bytes:
        """Read one object from storage."""

    def list_keys(self, prefix: str) -> list[str]:
        """List object keys beneath the given prefix."""


class _CachedExistenceWriter:
    """Wrapper that pre-fetches key existence via list_keys to avoid per-key HeadObject calls."""

    def __init__(self, writer: ObjectWriter, prefixes: Sequence[str]) -> None:
        self._writer = writer
        self._known_keys: set[str] = set()
        for prefix in prefixes:
            self._known_keys.update(writer.list_keys(prefix))

    def put_object(self, key: str, data: bytes | str) -> None:
        self._writer.put_object(key, data)
        self._known_keys.add(key)

    def exists(self, key: str) -> bool:
        return key in self._known_keys

    def get_object(self, key: str) -> bytes:
        return self._writer.get_object(key)

    def list_keys(self, prefix: str) -> list[str]:
        return self._writer.list_keys(prefix)


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
    """Security-master methods required for historical price archives."""

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


class PriceAdjustmentProvenanceProvider(Protocol):
    """Price fetchers that can describe their corporate-action adjustment policy."""

    def describe_adjustment_provenance(self) -> dict[str, object]:
        """Return machine-readable adjustment provenance for downstream manifests/reports."""


class NewsFetcher(Protocol):
    """Raw-news fetcher used by Layer 0 orchestration."""

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

    def fetch_latest_available_macro_observations(
        self,
        *,
        series_ids: Sequence[str],
        as_of_date: str,
        limit: int,
    ) -> list[dict[str, object]]:
        """Fetch the latest raw macro observation per series available as of one date."""


RecordSerializer = Callable[[list[OHLCVRecord]], bytes]
RecordDeserializer = Callable[[bytes], list[OHLCVRecord]]
NewsSerializer = Callable[[list[dict[str, object]]], bytes]
RawRowSerializer = Callable[[list[dict[str, object]]], bytes]
RawRowsDeserializer = Callable[[bytes], list[dict[str, object]]]
UniverseSerializer = Callable[[list[UniverseRecord]], bytes]
SENSITIVE_ERROR_PATTERN = re.compile(
    r"(?P<prefix>[?&](?:token|api-key|api_key|apikey|apiKey)=)(?P<secret>[^&\s]+)",
    flags=re.IGNORECASE,
)

_SHARES_OUTSTANDING_KEYS = (
    "sharesBasic",
    "sharesDiluted",
    "commonSharesOutstanding",
    "Common Shares Outstanding",
    "shares_outstanding",
)
_NET_INCOME_TO_COMMON_KEYS = (
    "Net Income Available to Common Shareholders",
    "netIncomeAvailableToCommonShareholders",
)
_BASIC_EPS_KEYS = ("Earnings Per Share, Basic", "earningsPerShareBasic", "epsBasic")
_REVENUE_KEYS = ("Revenue", "revenue")
_SALES_PER_SHARE_KEYS = ("Sales Per Share", "salesPerShare")
_FREE_CASH_FLOW_KEYS = ("Free Cash Flow", "freeCashFlow")
_FREE_CASH_FLOW_PER_SHARE_KEYS = ("Free Cash Flow Per Share", "freeCashFlowPerShare")
_EQUITY_KEYS = (
    "Equity Before Minority Interest",
    "Total Equity",
    "totalEquity",
    "shareholdersEquity",
    "stockholdersEquity",
)
_EQUITY_PER_SHARE_KEYS = ("Equity Per Share", "equityPerShare", "bookValuePerShare")


@dataclass(frozen=True)
class HistoricalLayer0Config:
    """Configuration for a complete historical Layer 0 raw-data backfill."""

    from_date: date
    to_date: date
    tickers: Sequence[str] | None = None
    benchmark_ticker: str = "SPY"
    fred_series_ids: Sequence[str] = ()
    simfin_statements: Sequence[str] = ("pl", "bs", "cf", "derived")
    simfin_periods: Sequence[str] = ("q1", "q2", "q3", "q4", "fy")
    overwrite: bool = False
    news_limit: int = 50
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
        if not self.benchmark_ticker.strip():
            raise ValueError("benchmark_ticker cannot be empty")
        if not self.fred_series_ids:
            raise ValueError("fred_series_ids must contain at least one series")
        if not self.simfin_statements:
            raise ValueError("simfin_statements must contain at least one statement")
        if not self.simfin_periods:
            raise ValueError("simfin_periods must contain at least one period")
        object.__setattr__(self, "benchmark_ticker", _canonicalize_ticker(self.benchmark_ticker))


@dataclass(frozen=True)
class DailyLayer0Config:
    """Configuration for one daily Layer 0 incremental ingest run."""

    as_of_date: date
    tickers: Sequence[str]
    fred_series_ids: Sequence[str]
    benchmark_ticker: str = "SPY"
    simfin_statements: Sequence[str] = ("pl", "bs", "cf", "derived")
    simfin_periods: Sequence[str] = ("q1", "q2", "q3", "q4", "fy")
    overwrite: bool = False
    news_limit: int = 50
    simfin_limit: int = 1000
    fred_limit: int = 1000
    quality_config: QualityFilterConfig = field(default_factory=QualityFilterConfig)
    quality_ohlcv_window: Mapping[str, Sequence[OHLCVRecord]] | None = None
    run_id: str | None = None

    def __post_init__(self) -> None:
        """Validate daily ticker and vendor-fetch settings."""
        if not _normalize_tickers(self.tickers):
            raise ValueError("tickers must contain at least one ticker")
        if not self.benchmark_ticker.strip():
            raise ValueError("benchmark_ticker cannot be empty")
        if not self.fred_series_ids:
            raise ValueError("fred_series_ids must contain at least one series")
        if not self.simfin_statements:
            raise ValueError("simfin_statements must contain at least one statement")
        if not self.simfin_periods:
            raise ValueError("simfin_periods must contain at least one period")
        _validate_positive_limit(self.news_limit, "news_limit")
        _validate_positive_limit(self.simfin_limit, "simfin_limit")
        _validate_positive_limit(self.fred_limit, "fred_limit")
        object.__setattr__(self, "benchmark_ticker", _canonicalize_ticker(self.benchmark_ticker))


@dataclass(frozen=True)
class Layer0PipelineResult:
    """Completion summary for one Layer 0 pipeline run."""

    run_id: str
    manifest_key: str
    status: RunStatus
    output_keys: tuple[str, ...]
    metadata: dict[str, object]


_SPLIT_LIKE_RATIO_THRESHOLD = 1.8
_SPLIT_LIKE_SAMPLE_LIMIT = 20


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
    price_deserializer: RecordDeserializer | None = None,
    news_serializer: NewsSerializer | None = None,
    fundamentals_serializer: RawRowSerializer | None = None,
    fundamentals_deserializer: RawRowsDeserializer | None = None,
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
    cached_writer: ObjectWriter = writer

    try:
        logger.info(
            "Layer 0 historical backfill starting: run_id={}, {}..{}",
            run_id,
            config.from_date.isoformat(),
            config.to_date.isoformat(),
        )
        tickers = _resolve_backfill_tickers(config=config, universe_provider=universe_provider)
        price_tickers = _scope_with_benchmark(
            tickers=tickers,
            benchmark_ticker=config.benchmark_ticker,
        )
        logger.info(
            "Resolved {} universe tickers and {} price tickers for backfill",
            len(tickers),
            len(price_tickers),
        )
        quality_window = _copy_ohlcv_window(config.quality_ohlcv_window)

        logger.info("Caching existence keys across R2 raw/ prefixes")
        cached_writer = _CachedExistenceWriter(
            writer,
            prefixes=[
                "raw/prices/",
                "raw/universe/",
                "raw/news/",
                "raw/fundamentals/",
                "raw/macro/",
                "raw/reference/",
                "artifacts/manifests/",
            ],
        )
        masks_complete = not config.overwrite and all(
            cached_writer.exists(raw_universe_path(day))
            for day in _business_days(config.from_date, config.to_date)
        )
        logger.info("Universe masks complete: {}", masks_complete)

        logger.info("Phase: prices")
        price_result = _backfill_historical_prices(
            config=config,
            tickers=price_tickers,
            price_fetcher=price_fetcher,
            security_master=security_master,
            writer=cached_writer,
            serializer=price_serializer or _records_to_parquet_bytes,
            deserializer=price_deserializer or _parquet_bytes_to_records,
            quality_window=quality_window,
            skip_quality_reads=masks_complete,
        )
        output_keys.extend(price_result.output_keys)
        metadata["prices"] = dict(price_result.metadata)
        provenance_report_key, adjustment_provenance = _write_ohlcv_provenance_report(
            writer=cached_writer,
            run_id=run_id,
            mode="historical_backfill",
            price_metadata=metadata["prices"],
            fetcher=price_fetcher,
        )
        metadata["prices"]["adjustment_provenance"] = adjustment_provenance
        metadata["prices"]["provenance_report_key"] = provenance_report_key
        output_keys.append(provenance_report_key)
        logger.info("Phase prices done: {}", price_result.metadata)

        logger.info("Phase: fundamentals (SimFin)")
        fundamentals_result = _write_fundamentals_archive(
            from_date=config.from_date,
            to_date=config.to_date,
            tickers=tickers,
            statements=config.simfin_statements,
            periods=config.simfin_periods,
            limit=config.simfin_limit,
            overwrite=config.overwrite,
            fetcher=fundamentals_fetcher,
            writer=cached_writer,
            serializer=fundamentals_serializer or _raw_rows_to_parquet_bytes,
        )
        output_keys.extend(fundamentals_result.output_keys)
        metadata["fundamentals"] = fundamentals_result.metadata
        logger.info("Phase fundamentals done: {}", fundamentals_result.metadata)

        logger.info("Phase: universe masks")
        business_days = _business_days(config.from_date, config.to_date)
        if masks_complete and int(price_result.metadata["written"]) == 0:
            universe_result = _WriteResult(
                output_keys=[],
                metadata={
                    "requested_days": len(business_days),
                    "written": 0,
                    "skipped": len(business_days),
                    "total_records": 0,
                    "output_keys": [],
                    "short_circuited": True,
                },
            )
        else:
            if masks_complete and not quality_window:
                _extend_quality_window_from_price_prefix(
                    writer=cached_writer,
                    prefix="raw/prices/",
                    deserializer=price_deserializer or _parquet_bytes_to_records,
                    quality_window=quality_window,
                )
            shares_outstanding_window = _load_shares_outstanding_window(
                writer=cached_writer,
                tickers=tickers,
                deserializer=fundamentals_deserializer or _parquet_bytes_to_raw_rows,
                min_market_cap=config.quality_config.min_market_cap,
            )
            universe_result = _write_historical_universe_masks(
                config=config,
                universe_provider=universe_provider,
                writer=cached_writer,
                ohlcv_window=quality_window,
                shares_outstanding_window=shares_outstanding_window,
                serializer=universe_serializer or _universe_to_csv_bytes,
            )
        output_keys.extend(universe_result.output_keys)
        metadata["universe"] = universe_result.metadata
        logger.info("Phase universe done: {}", universe_result.metadata)

        logger.info("Phase: news")
        news_result = _backfill_news(
            from_date=config.from_date,
            to_date=config.to_date,
            tickers=tickers,
            limit=config.news_limit,
            overwrite=config.overwrite,
            fetcher=news_fetcher,
            writer=cached_writer,
            serializer=news_serializer or _articles_to_jsonl_bytes,
        )
        output_keys.extend(news_result.output_keys)
        metadata["news"] = news_result.metadata
        logger.info("Phase news done: {}", news_result.metadata)

        logger.info("Phase: macro (FRED)")
        macro_result = _write_macro_archive(
            from_date=config.from_date,
            to_date=config.to_date,
            series_ids=config.fred_series_ids,
            limit=config.fred_limit,
            overwrite=config.overwrite,
            fetcher=macro_fetcher,
            writer=cached_writer,
            serializer=macro_serializer or _raw_rows_to_parquet_bytes,
        )
        output_keys.extend(macro_result.output_keys)
        metadata["macro"] = macro_result.metadata
        logger.info("Phase macro done: {}", macro_result.metadata)

        logger.info("Phase: manifest")
        manifest_key = _write_pipeline_manifest(
            writer=cached_writer,
            run_id=run_id,
            status=RunStatus.COMPLETED,
            started_at=started_at,
            metadata=metadata | {"output_keys": sorted(set(output_keys))},
        )
        logger.info("Layer 0 historical backfill complete: manifest={}", manifest_key)
        return Layer0PipelineResult(
            run_id=run_id,
            manifest_key=manifest_key,
            status=RunStatus.COMPLETED,
            output_keys=tuple(sorted(set(output_keys + [manifest_key]))),
            metadata=metadata,
        )
    except Exception as exc:
        logger.exception("Layer 0 historical backfill failed: {}", exc)
        manifest_key = _write_failure_manifest(
            writer=cached_writer,
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
    price_deserializer: RecordDeserializer | None = None,
    news_serializer: NewsSerializer | None = None,
    fundamentals_serializer: RawRowSerializer | None = None,
    fundamentals_deserializer: RawRowsDeserializer | None = None,
    macro_serializer: RawRowSerializer | None = None,
    universe_serializer: UniverseSerializer | None = None,
) -> Layer0PipelineResult:
    """Run one daily Layer 0 ingest and write a success/failure manifest."""
    run_id = config.run_id or f"layer0-daily-{config.as_of_date}"
    started_at = datetime.now(UTC)
    as_of_date = config.as_of_date
    tickers = _normalize_tickers(config.tickers)
    price_tickers = _scope_with_benchmark(
        tickers=tickers,
        benchmark_ticker=config.benchmark_ticker,
    )
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
                tickers=price_tickers,
                as_of_date=as_of_date.isoformat(),
            )
        )
        price_result = _write_daily_prices(
            records=daily_records,
            overwrite=config.overwrite,
            writer=writer,
            serializer=price_serializer or _records_to_parquet_bytes,
            deserializer=price_deserializer or _parquet_bytes_to_records,
        )
        output_keys.extend(price_result.output_keys)
        metadata["prices"] = dict(price_result.metadata)
        provenance_report_key, adjustment_provenance = _write_ohlcv_provenance_report(
            writer=writer,
            run_id=run_id,
            mode="daily_incremental",
            price_metadata=metadata["prices"],
            fetcher=live_price_fetcher,
        )
        metadata["prices"]["adjustment_provenance"] = adjustment_provenance
        metadata["prices"]["provenance_report_key"] = provenance_report_key
        output_keys.append(provenance_report_key)

        quality_window = _copy_ohlcv_window(config.quality_ohlcv_window)
        _extend_quality_window_from_price_archives(
            writer=writer,
            tickers=price_tickers,
            deserializer=price_deserializer or _parquet_bytes_to_records,
            quality_window=quality_window,
        )
        _require_target_date_price_coverage(
            as_of_date=as_of_date,
            tickers=(config.benchmark_ticker,),
            ohlcv_window=quality_window,
            reason="benchmark_ticker",
        )

        fundamentals_result = _write_fundamentals_archive(
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

        shares_outstanding_window = _load_shares_outstanding_window(
            writer=writer,
            tickers=tickers,
            deserializer=fundamentals_deserializer or _parquet_bytes_to_raw_rows,
            min_market_cap=config.quality_config.min_market_cap,
        )
        universe_records = build_universe_mask_records(
            as_of_date=as_of_date,
            tickers=tickers,
            ohlcv_window=quality_window,
            quality_config=config.quality_config,
            shares_outstanding_window=shares_outstanding_window,
        )
        _require_price_coverage_for_eligible_universe(
            records=universe_records,
            ohlcv_window=quality_window,
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

        macro_result = _write_daily_macro_snapshot(
            as_of_date=as_of_date,
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
    shares_outstanding_window: (
        Mapping[str, Sequence[SharesOutstandingSnapshot]] | None
    ) = None,
) -> list[UniverseRecord]:
    """Build schema-valid point-in-time universe rows with quality flags applied."""
    records = [
        build_universe_record(
            {"date": as_of_date.isoformat(), "ticker": ticker, "in_universe": True}
        )
        for ticker in _normalize_tickers(tickers)
    ]
    return apply_quality_filters(
        records,
        ohlcv_window,
        quality_config,
        shares_outstanding_window,
    )


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


def _scope_with_benchmark(*, tickers: Sequence[str], benchmark_ticker: str) -> list[str]:
    """Return the price-fetch scope with the required Layer 1 benchmark included."""
    return list(_normalize_tickers([*tickers, benchmark_ticker]))


def _backfill_historical_prices(
    *,
    config: HistoricalLayer0Config,
    tickers: Sequence[str],
    price_fetcher: HistoricalPriceFetcher,
    security_master: SecurityMaster,
    writer: ObjectWriter,
    serializer: RecordSerializer,
    deserializer: RecordDeserializer,
    quality_window: dict[str, list[OHLCVRecord]],
    skip_quality_reads: bool = False,
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
    archive_keys: list[str] = []
    written = 0
    skipped = 0
    empty = 0
    observed_rows = 0
    close_equals_adj_close_rows = 0
    close_diff_adj_close_rows = 0
    split_like_discontinuities: list[dict[str, object]] = []

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
        archive_keys.append(key)
        existing_records = (
            _canonicalize_ohlcv_records(_read_existing_records(writer, key, deserializer))
            if writer.exists(key)
            else []
        )
        records = list(existing_records)
        history_changed = config.overwrite or not writer.exists(key)
        for security in security_rows:
            fetch_from, fetch_to = _security_fetch_range(security, config.from_date, config.to_date)
            fetch_ranges = (
                [(fetch_from, fetch_to)]
                if config.overwrite or not existing_records
                else _missing_boundary_ranges(
                    existing_records,
                    from_date=fetch_from,
                    to_date=fetch_to,
                )
            )
            for range_from, range_to in fetch_ranges:
                fetched = _canonicalize_ohlcv_records(
                    price_fetcher.fetch_security_records(
                        security=security,
                        from_date=range_from.isoformat(),
                        to_date=range_to.isoformat(),
                    )
                )
                if not fetched:
                    continue
                records = _merge_ohlcv_histories(records, fetched)
                history_changed = True

        if not records:
            empty += 1
            continue

        observed_rows += len(records)
        equality_counts = _adj_close_equality_counts(records)
        close_equals_adj_close_rows += equality_counts["equal"]
        close_diff_adj_close_rows += equality_counts["different"]
        split_like_discontinuities.extend(
            _detect_split_like_discontinuities(records, ticker=security_id)
        )

        if history_changed:
            writer.put_object(key, serializer(_sort_ohlcv_records(records)))
            output_keys.append(key)
            written += 1
        else:
            skipped += 1
        if not skip_quality_reads:
            _extend_quality_window_with_records(quality_window, records)

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
            "archive_keys": archive_keys,
            "observed_rows": observed_rows,
            "close_equals_adj_close_rows": close_equals_adj_close_rows,
            "close_diff_adj_close_rows": close_diff_adj_close_rows,
            "split_like_discontinuity_count": len(split_like_discontinuities),
            "split_like_discontinuity_samples": split_like_discontinuities[
                :_SPLIT_LIKE_SAMPLE_LIMIT
            ],
            "output_keys": output_keys,
        },
    )


def _write_historical_universe_masks(
    *,
    config: HistoricalLayer0Config,
    universe_provider: HistoricalUniverseProvider,
    writer: ObjectWriter,
    ohlcv_window: Mapping[str, Sequence[OHLCVRecord]],
    shares_outstanding_window: Mapping[str, Sequence[SharesOutstandingSnapshot]] | None,
    serializer: UniverseSerializer,
) -> _WriteResult:
    output_keys: list[str] = []
    written = 0
    skipped = 0
    total_records = 0
    days = _business_days(config.from_date, config.to_date)
    quality_windows = prepare_quality_windows(ohlcv_window)

    for current_date in days:
        tickers = universe_provider.get_constituents(current_date.isoformat())
        records = apply_prepared_quality_filters(
            [
                build_universe_record(
                    {
                        "date": current_date.isoformat(),
                        "ticker": ticker,
                        "in_universe": True,
                    }
                )
                for ticker in _normalize_tickers(tickers)
            ],
            quality_windows,
            config.quality_config,
            shares_outstanding_window,
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
    deserializer: RecordDeserializer,
) -> _WriteResult:
    grouped: dict[str, list[OHLCVRecord]] = {}
    for record in records:
        grouped.setdefault(record.ticker.upper(), []).append(record)

    output_keys: list[str] = []
    archive_keys: list[str] = []
    written = 0
    skipped = 0
    observed_rows = 0
    close_equals_adj_close_rows = 0
    close_diff_adj_close_rows = 0
    split_like_discontinuities: list[dict[str, object]] = []
    for ticker, ticker_records in sorted(grouped.items()):
        key = raw_price_path(ticker)
        archive_keys.append(key)
        if not writer.exists(key):
            sorted_records = _sort_ohlcv_records(ticker_records)
            writer.put_object(key, serializer(sorted_records))
            output_keys.append(key)
            written += 1
            observed_rows += len(sorted_records)
            equality_counts = _adj_close_equality_counts(sorted_records)
            close_equals_adj_close_rows += equality_counts["equal"]
            close_diff_adj_close_rows += equality_counts["different"]
            split_like_discontinuities.extend(
                _detect_split_like_discontinuities(sorted_records, ticker=ticker)
            )
            continue

        existing_records = _canonicalize_ohlcv_records(
            _read_existing_records(writer, key, deserializer)
        )
        merged_records = _merge_ohlcv_histories(
            [] if overwrite else existing_records,
            ticker_records,
        )
        if _ohlcv_histories_equal(existing_records, merged_records):
            skipped += 1
            observed_rows += len(existing_records)
            equality_counts = _adj_close_equality_counts(existing_records)
            close_equals_adj_close_rows += equality_counts["equal"]
            close_diff_adj_close_rows += equality_counts["different"]
            split_like_discontinuities.extend(
                _detect_split_like_discontinuities(existing_records, ticker=ticker)
            )
            continue
        writer.put_object(key, serializer(_sort_ohlcv_records(merged_records)))
        output_keys.append(key)
        written += 1
        observed_rows += len(merged_records)
        equality_counts = _adj_close_equality_counts(merged_records)
        close_equals_adj_close_rows += equality_counts["equal"]
        close_diff_adj_close_rows += equality_counts["different"]
        split_like_discontinuities.extend(
            _detect_split_like_discontinuities(merged_records, ticker=ticker)
        )

    return _WriteResult(
        output_keys=output_keys,
        metadata={
            "requested_records": len(records),
            "written": written,
            "skipped": skipped,
            "empty": 0 if records else 1,
            "archive_keys": archive_keys,
            "observed_rows": observed_rows,
            "close_equals_adj_close_rows": close_equals_adj_close_rows,
            "close_diff_adj_close_rows": close_diff_adj_close_rows,
            "split_like_discontinuity_count": len(split_like_discontinuities),
            "split_like_discontinuity_samples": split_like_discontinuities[
                :_SPLIT_LIKE_SAMPLE_LIMIT
            ],
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
    payload = serializer(_sort_universe_records(records))
    if writer.exists(key) and not overwrite:
        existing_payload = writer.get_object(key)
        if existing_payload == payload:
            return _WriteResult(output_keys=[], metadata={"written": 0, "skipped": 1, "key": key})
    writer.put_object(key, payload)
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
    """Fetch and persist SimFin fundamentals per-ticker so partial progress survives failures."""
    normalized_tickers = [_canonicalize_ticker(ticker) for ticker in tickers]
    output_keys: list[str] = []
    written = 0
    skipped = 0
    empty = 0
    total_rows = 0
    missing_tickers: list[str] = []
    retrieved_at = datetime.now(UTC)
    batch_size = 50
    batches = [
        tuple(normalized_tickers[index : index + batch_size])
        for index in range(0, len(normalized_tickers), batch_size)
    ]

    for batch_index, batch in enumerate(batches, start=1):
        remaining = [
            ticker
            for ticker in batch
            if overwrite or not writer.exists(raw_fundamentals_path(ticker))
        ]
        if not remaining:
            skipped += len(batch)
            logger.info(
                "SimFin batch {}/{} fully cached — skipping",
                batch_index,
                len(batches),
            )
            continue

        logger.info(
            "SimFin batch {}/{}: fetching {} of {} tickers",
            batch_index,
            len(batches),
            len(remaining),
            len(batch),
        )
        rows = fetcher.fetch_all_fundamentals(
            tickers=remaining,
            start_date=from_date.isoformat(),
            end_date=to_date.isoformat(),
            statements=statements,
            periods=periods,
            retrieved_at=retrieved_at,
            limit=limit,
        )
        rows_by_ticker: dict[str, list[dict[str, object]]] = {ticker: [] for ticker in remaining}
        for row in rows:
            ticker = _canonicalize_ticker(str(row.get("ticker") or ""))
            if ticker in rows_by_ticker:
                rows_by_ticker[ticker].append(row)
        skipped += len(batch) - len(remaining)
        for ticker in remaining:
            ticker_rows = _sort_raw_rows(rows_by_ticker.get(ticker, []), ("report_date",))
            if not ticker_rows:
                empty += 1
                missing_tickers.append(ticker)
                logger.warning(
                    "SimFin returned no fundamentals rows for ticker {} in {}..{}; "
                    "skipping archive write",
                    ticker,
                    from_date.isoformat(),
                    to_date.isoformat(),
                )
                continue
            key = raw_fundamentals_path(ticker)
            writer.put_object(key, serializer(ticker_rows))
            output_keys.append(key)
            written += 1
            total_rows += len(ticker_rows)

    return _WriteResult(
        output_keys=output_keys,
        metadata={
            "requested_tickers": len(normalized_tickers),
            "written": written,
            "skipped": skipped,
            "empty": empty,
            "missing_tickers": missing_tickers,
            "total_rows": total_rows,
            "output_keys": output_keys,
        },
    )


def _write_macro_archive(
    *,
    from_date: date,
    to_date: date,
    series_ids: Sequence[str],
    limit: int,
    overwrite: bool,
    fetcher: MacroFetcher,
    writer: ObjectWriter,
    serializer: RawRowSerializer,
) -> _WriteResult:
    """Fetch and persist FRED macro observations per observation_date."""
    normalized_series = _normalize_tokens(series_ids)
    if not overwrite and _macro_archive_covers_range(writer, from_date, to_date):
        logger.info(
            "Macro archive already covers {}..{}; skipping FRED fetch",
            from_date.isoformat(),
            to_date.isoformat(),
        )
        return _WriteResult(
            output_keys=[],
            metadata={
                "requested_series": len(normalized_series),
                "written": 0,
                "skipped": 0,
                "empty": 0,
                "total_rows": 0,
                "output_keys": [],
                "short_circuited": True,
            },
        )
    rows = fetcher.fetch_all_macro_observations(
        series_ids=normalized_series,
        start_date=from_date.isoformat(),
        end_date=to_date.isoformat(),
        realtime_start=from_date.isoformat(),
        realtime_end=to_date.isoformat(),
        limit=limit,
    )
    rows_by_date: dict[str, list[dict[str, object]]] = {}
    for row in rows:
        observation_date = str(row.get("observation_date") or "").strip()
        if not observation_date:
            continue
        rows_by_date.setdefault(observation_date, []).append(row)

    output_keys: list[str] = []
    written = 0
    skipped = 0
    empty = 0
    total_rows = 0
    for observation_date in sorted(rows_by_date):
        date_rows = _sort_raw_rows(
            rows_by_date[observation_date],
            ("series_id", "realtime_start", "realtime_end"),
        )
        key = raw_macro_path(observation_date)
        if not overwrite and writer.exists(key):
            skipped += 1
            continue
        writer.put_object(key, serializer(date_rows))
        output_keys.append(key)
        written += 1
        total_rows += len(date_rows)
        if not date_rows:
            empty += 1

    return _WriteResult(
        output_keys=output_keys,
        metadata={
            "requested_series": len(normalized_series),
            "written": written,
            "skipped": skipped,
            "empty": empty,
            "total_rows": total_rows,
            "output_keys": output_keys,
        },
    )


def _write_daily_macro_snapshot(
    *,
    as_of_date: date,
    series_ids: Sequence[str],
    limit: int,
    overwrite: bool,
    fetcher: MacroFetcher,
    writer: ObjectWriter,
    serializer: RawRowSerializer,
) -> _WriteResult:
    """Fetch and persist one run-date macro snapshot using latest available observations."""
    normalized_series = _normalize_tokens(series_ids)
    key = raw_macro_path(as_of_date)
    if not overwrite and writer.exists(key):
        logger.info(
            "Daily macro snapshot already exists for {}; skipping FRED fetch",
            as_of_date.isoformat(),
        )
        return _WriteResult(
            output_keys=[],
            metadata={
                "requested_series": len(normalized_series),
                "written": 0,
                "skipped": 1,
                "empty": 0,
                "total_rows": 0,
                "output_keys": [],
                "snapshot_date": as_of_date.isoformat(),
            },
        )

    rows = _sort_raw_rows(
        fetcher.fetch_latest_available_macro_observations(
            series_ids=normalized_series,
            as_of_date=as_of_date.isoformat(),
            limit=limit,
        ),
        ("series_id", "observation_date", "realtime_start", "realtime_end"),
    )
    if not rows:
        return _WriteResult(
            output_keys=[],
            metadata={
                "requested_series": len(normalized_series),
                "written": 0,
                "skipped": 0,
                "empty": 1,
                "total_rows": 0,
                "output_keys": [],
                "snapshot_date": as_of_date.isoformat(),
            },
        )

    writer.put_object(key, serializer(rows))
    return _WriteResult(
        output_keys=[key],
        metadata={
            "requested_series": len(normalized_series),
            "written": 1,
            "skipped": 0,
            "empty": 0,
            "total_rows": len(rows),
            "output_keys": [key],
            "snapshot_date": as_of_date.isoformat(),
        },
    )


def _write_ohlcv_provenance_report(
    *,
    writer: ObjectWriter,
    run_id: str,
    mode: str,
    price_metadata: dict[str, object],
    fetcher: HistoricalPriceFetcher | LivePriceFetcher,
) -> tuple[str, dict[str, object]]:
    """Persist one provenance report and return its key plus the manifest summary."""
    provenance = _price_adjustment_provenance(fetcher)
    report_key = layer0_ohlcv_provenance_report_path(run_id)
    archive_keys = list(_string_list(price_metadata.get("archive_keys")))
    written_keys = [
        key for key in _string_list(price_metadata.get("output_keys")) if key in set(archive_keys)
    ]
    report = {
        "run_id": run_id,
        "mode": mode,
        "generated_at": datetime.now(UTC).isoformat(),
        "price_adjustment_provenance": provenance,
        "archive_summary": {
            "archive_keys": archive_keys,
            "written_keys": written_keys,
            "observed_rows": int(price_metadata.get("observed_rows", 0)),
            "close_equals_adj_close_rows": int(
                price_metadata.get("close_equals_adj_close_rows", 0)
            ),
            "close_diff_adj_close_rows": int(
                price_metadata.get("close_diff_adj_close_rows", 0)
            ),
            "split_like_discontinuity_count": int(
                price_metadata.get("split_like_discontinuity_count", 0)
            ),
            "split_like_discontinuity_samples": list(
                _mapping_list(price_metadata.get("split_like_discontinuity_samples"))
            ),
        },
    }
    writer.put_object(report_key, json.dumps(report, indent=2, sort_keys=True))
    adjustment_provenance = {
        "policy_id": provenance["policy_id"],
        "provider": provenance["provider"],
        "feed": provenance["feed"],
        "request_adjustment": provenance["request_adjustment"],
        "stored_ohlc_basis": provenance["stored_ohlc_basis"],
        "normalized_adj_close_policy": provenance["normalized_adj_close_policy"],
    }
    return report_key, adjustment_provenance


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
    failure_metadata["error"] = {
        "type": type(exc).__name__,
        "message": _sanitize_error_message(str(exc)),
    }
    return _write_pipeline_manifest(
        writer=writer,
        run_id=run_id,
        status=RunStatus.FAILED,
        started_at=started_at,
        metadata=failure_metadata,
    )


def _sanitize_error_message(message: str) -> str:
    return SENSITIVE_ERROR_PATTERN.sub(r"\g<prefix><redacted>", message)


def _price_adjustment_provenance(
    fetcher: HistoricalPriceFetcher | LivePriceFetcher,
) -> dict[str, object]:
    """Return machine-readable OHLCV adjustment provenance for a price fetcher."""
    describe = getattr(fetcher, "describe_adjustment_provenance", None)
    if callable(describe):
        provenance = describe()
        if not isinstance(provenance, Mapping):
            raise TypeError("describe_adjustment_provenance() must return a mapping")
        return dict(provenance)
    raise TypeError(
        f"{type(fetcher).__name__} must implement describe_adjustment_provenance() "
        "for Layer 0 OHLCV provenance tracking"
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
            "prices": "alpaca_sip_historical_and_daily_bars",
            "news": "alpaca_news",
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


def _parquet_bytes_to_records(data: bytes) -> list[OHLCVRecord]:
    """Deserialize a parquet archive back into OHLCVRecord objects."""
    try:
        import pandas as pd

        importlib.import_module("pyarrow")
    except ModuleNotFoundError as exc:
        raise ModuleNotFoundError(
            "pandas and pyarrow are required to deserialize OHLCV records from Parquet."
        ) from exc

    frame = pd.read_parquet(io.BytesIO(data))
    return [OHLCVRecord(**row) for row in frame.to_dict("records")]


def _parquet_bytes_to_raw_rows(data: bytes) -> list[dict[str, object]]:
    """Deserialize a parquet raw-archive payload back into row dictionaries."""
    try:
        import pandas as pd

        importlib.import_module("pyarrow")
    except ModuleNotFoundError as exc:
        raise ModuleNotFoundError(
            "pandas and pyarrow are required to deserialize raw Layer 0 rows from Parquet."
        ) from exc

    frame = pd.read_parquet(io.BytesIO(data))
    return [dict(row) for row in frame.to_dict("records")]


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


def _load_shares_outstanding_window(
    *,
    writer: ObjectWriter,
    tickers: Sequence[str],
    deserializer: RawRowsDeserializer,
    min_market_cap: float,
) -> dict[str, list[SharesOutstandingSnapshot]]:
    """Load point-in-time shares-outstanding snapshots when market-cap screening is enabled."""
    if min_market_cap <= 0.0:
        return {}

    shares_outstanding_window: dict[str, list[SharesOutstandingSnapshot]] = {}
    for ticker in _normalize_tickers(tickers):
        key = raw_fundamentals_path(ticker)
        if not writer.exists(key):
            continue
        rows = _read_existing_rows(writer, key, deserializer)
        snapshots = _extract_shares_outstanding_snapshots(rows)
        if snapshots:
            shares_outstanding_window[ticker] = snapshots
    return shares_outstanding_window


def _extract_shares_outstanding_snapshots(
    rows: Sequence[Mapping[str, object]],
) -> list[SharesOutstandingSnapshot]:
    """Return sorted point-in-time shares-outstanding snapshots from raw fundamentals rows."""
    # SimFin share-count inputs are treated as raw shares, not thousands. Verified on
    # 2026-05-14 against the live AAPL 2024-03-31 / 2024-05-03 compact snapshot:
    # Revenue 90,753,000,000 divided by Sales Per Share 5.89081 implies
    # ~15.406B shares, so the resulting market cap stays in plain USD.
    #
    # Do not accept a bare "shares" key here. The compact API exposes many share-related
    # metrics, and Layer 0 market-cap screening must only use explicitly named
    # shares-outstanding fields or a ratio derived from official SimFin per-share metrics.
    by_date: dict[str, tuple[str, SharesOutstandingSnapshot]] = {}
    grouped_rows: dict[tuple[str, str, str], list[tuple[str, Mapping[str, object]]]] = {}
    for row in rows:
        availability_date = str(row.get("availability_date") or "").strip()
        if not availability_date:
            continue
        raw = _decode_raw_row_payload(row.get("raw_json") or row.get("raw"))
        if not raw:
            continue
        report_date = str(row.get("report_date") or raw.get("Report Date") or "").strip()
        fiscal_year = str(
            row.get("fiscal_year") or raw.get("Fiscal Year") or raw.get("fiscalYear") or ""
        ).strip()
        fiscal_period = str(
            row.get("fiscal_period")
            or raw.get("Fiscal Period")
            or raw.get("fiscalPeriod")
            or ""
        ).strip()
        group_key = (report_date or availability_date, fiscal_year, fiscal_period)
        grouped_rows.setdefault(group_key, []).append((availability_date, raw))

    for (report_date, _, _), period_rows in grouped_rows.items():
        merged_payload: dict[str, object] = {}
        availability_date = ""
        for row_availability_date, raw in period_rows:
            merged_payload.update(raw)
            availability_date = max(availability_date, row_availability_date)
        shares_outstanding = _read_numeric(merged_payload, _SHARES_OUTSTANDING_KEYS)
        if shares_outstanding is None or shares_outstanding <= 0.0:
            shares_outstanding = _derive_shares_outstanding_from_simfin_metrics(merged_payload)
        if shares_outstanding is None or shares_outstanding <= 0.0:
            continue
        current = by_date.get(availability_date)
        snapshot = SharesOutstandingSnapshot(
            availability_date=availability_date,
            shares_outstanding=shares_outstanding,
        )
        if current is None or report_date >= current[0]:
            by_date[availability_date] = (report_date, snapshot)
    return [snapshot for _, snapshot in (by_date[as_of_date] for as_of_date in sorted(by_date))]


def _derive_shares_outstanding_from_simfin_metrics(
    raw: Mapping[str, object],
) -> float | None:
    """Infer a raw share count from SimFin numerator/per-share metric pairs."""
    ratio_pairs = (
        (_NET_INCOME_TO_COMMON_KEYS, _BASIC_EPS_KEYS),
        (_REVENUE_KEYS, _SALES_PER_SHARE_KEYS),
        (_FREE_CASH_FLOW_KEYS, _FREE_CASH_FLOW_PER_SHARE_KEYS),
        (_EQUITY_KEYS, _EQUITY_PER_SHARE_KEYS),
    )
    for numerator_keys, per_share_keys in ratio_pairs:
        numerator = _read_numeric(raw, numerator_keys)
        per_share_value = _read_numeric(raw, per_share_keys)
        shares_outstanding = _positive_finite_ratio(numerator, per_share_value)
        if shares_outstanding is not None:
            return shares_outstanding
    return None


def _require_price_coverage_for_eligible_universe(
    *,
    records: Sequence[UniverseRecord],
    ohlcv_window: Mapping[str, Sequence[OHLCVRecord]],
) -> None:
    missing: list[str] = []
    indexed_dates = {
        _canonicalize_ticker(ticker): {record.date for record in rows}
        for ticker, rows in ohlcv_window.items()
    }
    for record in records:
        if not (
            record.in_universe
            and record.tradable
            and record.liquid
            and record.data_quality_ok
            and not record.halted
        ):
            continue
        if record.date not in indexed_dates.get(_canonicalize_ticker(record.ticker), set()):
            missing.append(record.ticker)
    if missing:
        raise RuntimeError(
            "Layer 0 cannot mark the universe ready without raw target-date price coverage for: "
            + ", ".join(sorted(set(missing)))
        )


def _require_target_date_price_coverage(
    *,
    as_of_date: date,
    tickers: Sequence[str],
    ohlcv_window: Mapping[str, Sequence[OHLCVRecord]],
    reason: str,
) -> None:
    """Require target-date coverage for explicitly required non-universe tickers."""
    target_date = as_of_date.isoformat()
    indexed_dates = {
        _canonicalize_ticker(ticker): {record.date for record in rows}
        for ticker, rows in ohlcv_window.items()
    }
    missing = [
        ticker
        for ticker in _normalize_tickers(tickers)
        if target_date not in indexed_dates.get(_canonicalize_ticker(ticker), set())
    ]
    if missing:
        raise RuntimeError(
            f"Layer 0 missing raw target-date price coverage for {reason}: "
            + ", ".join(missing)
        )


def _extend_quality_window_from_price_archives(
    *,
    writer: ObjectWriter,
    tickers: Sequence[str],
    deserializer: RecordDeserializer,
    quality_window: dict[str, list[OHLCVRecord]],
) -> None:
    for ticker in tickers:
        key = raw_price_path(ticker)
        if not writer.exists(key):
            continue
        records = _canonicalize_ohlcv_records(_read_existing_records(writer, key, deserializer))
        _extend_quality_window_with_records(quality_window, records)


def _extend_quality_window_from_price_prefix(
    *,
    writer: ObjectWriter,
    prefix: str,
    deserializer: RecordDeserializer,
    quality_window: dict[str, list[OHLCVRecord]],
) -> None:
    for key in writer.list_keys(prefix):
        records = _canonicalize_ohlcv_records(_read_existing_records(writer, key, deserializer))
        _extend_quality_window_with_records(quality_window, records)


def _extend_quality_window_with_records(
    quality_window: dict[str, list[OHLCVRecord]],
    records: Sequence[OHLCVRecord],
) -> None:
    for record in records:
        quality_window.setdefault(_canonicalize_ticker(record.ticker), []).append(record)


def _merge_ohlcv_histories(
    existing_records: Sequence[OHLCVRecord],
    new_records: Sequence[OHLCVRecord],
) -> list[OHLCVRecord]:
    merged = {record.date: record for record in existing_records}
    for record in new_records:
        merged[record.date] = record
    return _sort_ohlcv_records(list(merged.values()))


def _ohlcv_histories_equal(
    left: Sequence[OHLCVRecord],
    right: Sequence[OHLCVRecord],
) -> bool:
    return [record.model_dump() for record in _sort_ohlcv_records(left)] == [
        record.model_dump() for record in _sort_ohlcv_records(right)
    ]


def _missing_boundary_ranges(
    existing_records: Sequence[OHLCVRecord],
    *,
    from_date: date,
    to_date: date,
) -> list[tuple[date, date]]:
    existing_dates = sorted(
        date.fromisoformat(record.date)
        for record in existing_records
        if from_date <= date.fromisoformat(record.date) <= to_date
    )
    if not existing_dates:
        return [(from_date, to_date)]

    ranges: list[tuple[date, date]] = []
    first_present = existing_dates[0]
    if first_present > from_date:
        prefix_end = _previous_business_day(first_present)
        if prefix_end >= from_date:
            ranges.append((from_date, prefix_end))

    last_present = existing_dates[-1]
    if last_present < to_date:
        suffix_start = _next_business_day(last_present)
        if suffix_start <= to_date:
            ranges.append((suffix_start, to_date))
    return ranges


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
    reference_rows = [security.to_reference_row() for security in securities]
    return {
        "source": _security_reference_source(reference_rows),
        "from_date": from_date.isoformat(),
        "to_date": to_date.isoformat(),
        "generated_at": datetime.now(UTC).isoformat(),
        "missing_tickers": list(missing_tickers),
        "securities": reference_rows,
    }


def _security_reference_source(reference_rows: Sequence[Mapping[str, object]]) -> str:
    sources = {
        source
        for row in reference_rows
        if isinstance(source := row.get("source"), str) and source.strip()
    }
    if len(sources) == 1:
        return next(iter(sources))
    if sources:
        return "mixed"
    return "unknown"


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


def _read_existing_records(
    writer: ObjectWriter,
    key: str,
    deserializer: RecordDeserializer,
    max_retries: int = 3,
) -> list[OHLCVRecord]:
    """Read and deserialize OHLCV records from storage with transient-error retries."""
    import time as _time

    last_error: Exception | None = None
    for attempt in range(max_retries):
        try:
            return deserializer(writer.get_object(key))
        except Exception as exc:
            last_error = exc
            if attempt < max_retries - 1:
                _time.sleep(2 ** attempt)
    raise last_error  # type: ignore[misc]


def _read_existing_rows(
    writer: ObjectWriter,
    key: str,
    deserializer: RawRowsDeserializer,
    max_retries: int = 3,
) -> list[dict[str, object]]:
    """Read and deserialize raw archive rows from storage with transient-error retries."""
    import time as _time

    last_error: Exception | None = None
    for attempt in range(max_retries):
        try:
            return deserializer(writer.get_object(key))
        except Exception as exc:
            last_error = exc
            if attempt < max_retries - 1:
                _time.sleep(2 ** attempt)
    raise last_error  # type: ignore[misc]


def _decode_raw_row_payload(raw_payload: object) -> dict[str, object] | None:
    """Return the archived vendor payload as a dictionary when available."""
    if raw_payload is None:
        return None
    if isinstance(raw_payload, Mapping):
        return dict(raw_payload)
    if not isinstance(raw_payload, str):
        return None
    stripped = raw_payload.strip()
    if not stripped:
        return None
    try:
        parsed = json.loads(stripped)
    except json.JSONDecodeError:
        return None
    if not isinstance(parsed, Mapping):
        return None
    return dict(parsed)


def _read_numeric(row: Mapping[str, object], keys: Sequence[str]) -> float | None:
    """Return the first finite numeric value found under any of the supplied keys."""
    for key in keys:
        if key not in row:
            continue
        value = row[key]
        if value is None or isinstance(value, bool):
            continue
        if isinstance(value, (int, float)):
            candidate = float(value)
        elif isinstance(value, str):
            stripped = value.strip()
            if not stripped:
                continue
            try:
                candidate = float(stripped.replace(",", ""))
            except ValueError:
                continue
        else:
            continue
        if candidate == candidate and candidate not in {float("inf"), float("-inf")}:
            return candidate
    return None


def _positive_finite_ratio(
    numerator: float | None,
    denominator: float | None,
) -> float | None:
    """Return a positive finite ratio when both inputs define one."""
    if numerator is None or denominator in (None, 0.0):
        return None
    candidate = numerator / denominator
    if candidate <= 0.0:
        return None
    if candidate != candidate or candidate in {float("inf"), float("-inf")}:
        return None
    return candidate


def _adj_close_equality_counts(records: Sequence[OHLCVRecord]) -> dict[str, int]:
    """Count rows whose normalized adj_close matches or differs from close."""
    equal = 0
    different = 0
    for record in records:
        if abs(record.close - record.adj_close) <= 1e-9:
            equal += 1
        else:
            different += 1
    return {"equal": equal, "different": different}


def _detect_split_like_discontinuities(
    records: Sequence[OHLCVRecord],
    *,
    ticker: str,
) -> list[dict[str, object]]:
    """Flag large adjacent close-ratio jumps for audit-only corporate-action review."""
    sorted_records = _sort_ohlcv_records(records)
    discontinuities: list[dict[str, object]] = []
    for previous, current in zip(sorted_records, sorted_records[1:], strict=False):
        prior_close = previous.adj_close
        current_close = current.adj_close
        if prior_close <= 0.0 or current_close <= 0.0:
            continue
        ratio = max(current_close / prior_close, prior_close / current_close)
        if ratio < _SPLIT_LIKE_RATIO_THRESHOLD:
            continue
        discontinuities.append(
            {
                "ticker": ticker,
                "previous_date": previous.date,
                "current_date": current.date,
                "price_ratio": round(ratio, 6),
            }
        )
    return discontinuities


def _string_list(value: object) -> tuple[str, ...]:
    """Return only string values from a generic list-like object."""
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        return ()
    return tuple(item for item in value if isinstance(item, str))


def _mapping_list(value: object) -> tuple[dict[str, object], ...]:
    """Return dictionary items from a generic list-like object."""
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        return ()
    return tuple(dict(item) for item in value if isinstance(item, Mapping))


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


def _previous_business_day(value: date) -> date:
    current = value - timedelta(days=1)
    while current.weekday() >= 5:
        current -= timedelta(days=1)
    return current


def _next_business_day(value: date) -> date:
    current = value + timedelta(days=1)
    while current.weekday() >= 5:
        current += timedelta(days=1)
    return current


def _macro_archive_covers_range(writer: ObjectWriter, from_date: date, to_date: date) -> bool:
    """Return True when every business day in the range has a raw/macro/{date}.parquet key."""
    expected = {raw_macro_path(day.isoformat()) for day in _business_days(from_date, to_date)}
    if not expected:
        return False
    present = set(writer.list_keys("raw/macro/"))
    return expected.issubset(present)


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
