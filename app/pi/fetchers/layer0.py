"""Daily Layer 0 incremental ingest for the Pi runtime."""

from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence
from datetime import date
from pathlib import Path

from loguru import logger

_REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(_REPO_ROOT))

from core.data.layer0_pipeline import (  # noqa: E402
    DailyLayer0Config,
    Layer0PipelineResult,
    run_daily_layer0_incremental,
)
from services.alpaca.market_data import AlpacaMarketDataClient, AlpacaMarketDataConfig  # noqa: E402
from services.alpaca.news import DEFAULT_ALPACA_NEWS_PAGE_LIMIT, AlpacaNewsClient  # noqa: E402
from services.fred.macro_fetcher import (  # noqa: E402
    DEFAULT_FRED_CONFIG_PATH,
    FredClientConfig,
    FredMacroFetcher,
    load_fred_archive_config,
)
from services.r2.writer import R2Writer  # noqa: E402
from services.simfin.fundamentals_fetcher import (  # noqa: E402
    SimFinClientConfig,
    SimFinFundamentalsFetcher,
)


def run_layer0_incremental(
    *,
    as_of_date: date,
    tickers: Sequence[str],
    benchmark_ticker: str = "SPY",
    series_ids: Sequence[str] | None = None,
    overwrite: bool = False,
    run_id: str | None = None,
    news_limit: int = DEFAULT_ALPACA_NEWS_PAGE_LIMIT,
    simfin_limit: int = 1000,
    fred_limit: int = 1000,
    config_path: Path = DEFAULT_FRED_CONFIG_PATH,
    writer: R2Writer | None = None,
) -> Layer0PipelineResult:
    """Run one daily Layer 0 incremental ingest with production dependencies."""
    archive_config = load_fred_archive_config(config_path)
    alpaca_config = AlpacaMarketDataConfig.from_env()
    config = DailyLayer0Config(
        as_of_date=as_of_date,
        tickers=tickers,
        benchmark_ticker=benchmark_ticker,
        fred_series_ids=series_ids or archive_config.series_ids,
        overwrite=overwrite,
        news_limit=news_limit,
        simfin_limit=simfin_limit,
        fred_limit=fred_limit,
        run_id=run_id,
    )
    return run_daily_layer0_incremental(
        config=config,
        live_price_fetcher=AlpacaMarketDataClient(alpaca_config),
        news_fetcher=AlpacaNewsClient(alpaca_config),
        fundamentals_fetcher=SimFinFundamentalsFetcher(SimFinClientConfig.from_env()),
        macro_fetcher=FredMacroFetcher(FredClientConfig.from_env()),
        writer=writer or R2Writer(),
    )


def main() -> int:
    """Run one daily Layer 0 incremental ingest from the command line."""
    args = _parse_args()
    try:
        as_of_date = date.fromisoformat(args.as_of_date)
    except ValueError as exc:
        logger.error("Invalid --as-of-date: {}", exc)
        return 1

    result = run_layer0_incremental(
        as_of_date=as_of_date,
        tickers=args.tickers,
        benchmark_ticker=args.benchmark_ticker,
        series_ids=args.series_ids,
        overwrite=args.overwrite,
        run_id=args.run_id,
        news_limit=args.news_limit,
        simfin_limit=args.simfin_limit,
        fred_limit=args.fred_limit,
        config_path=Path(args.config),
    )
    logger.info("Daily Layer 0 incremental ingest complete: {}", result)
    return 0


def _parse_args() -> argparse.Namespace:
    """Parse daily Layer 0 CLI arguments."""
    parser = argparse.ArgumentParser(description="Run daily Layer 0 raw ingest into R2.")
    parser.add_argument("--config", default=str(DEFAULT_FRED_CONFIG_PATH))
    parser.add_argument("--as-of-date", required=True, metavar="YYYY-MM-DD")
    parser.add_argument("--tickers", nargs="+", required=True)
    parser.add_argument(
        "--benchmark-ticker",
        default="SPY",
        help=(
            "Benchmark ticker whose raw price archive must also be refreshed for Layer 1 "
            "(default: SPY)."
        ),
    )
    parser.add_argument("--series-ids", nargs="*", default=None)
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--news-limit", type=int, default=DEFAULT_ALPACA_NEWS_PAGE_LIMIT)
    parser.add_argument("--simfin-limit", type=int, default=1000)
    parser.add_argument("--fred-limit", type=int, default=1000)
    args = parser.parse_args()
    if args.series_ids == []:
        parser.error("--series-ids requires at least one series when provided")
    return args


if __name__ == "__main__":
    sys.exit(main())
