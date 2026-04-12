"""Complete historical Layer 0 raw-data backfill orchestration."""

from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

from loguru import logger

_REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(_REPO_ROOT))

from core.data.layer0_pipeline import (  # noqa: E402
    HistoricalLayer0Config,
    run_historical_layer0_backfill,
)
from services.alpaca.market_data import AlpacaMarketDataConfig  # noqa: E402
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
from services.tiingo.ohlcv_fetcher import TiingoClientConfig, TiingoOHLCVFetcher  # noqa: E402
from services.tiingo.security_master import TiingoSecurityMaster  # noqa: E402
from services.wikipedia.sp500_universe import (  # noqa: E402
    get_all_historical_tickers,
    get_constituents,
)


class WikipediaUniverseProvider:
    """Adapter exposing Wikipedia S&P 500 membership to the Layer 0 pipeline."""

    def get_constituents(self, as_of_date: str) -> list[str]:
        """Return point-in-time S&P 500 constituents for one date."""
        return get_constituents(as_of_date)

    def get_historical_tickers(self, from_date: str, to_date: str) -> set[str]:
        """Return all S&P 500 tickers present at any point in the date range."""
        return get_all_historical_tickers(from_date, to_date)


def main() -> int:
    """Run the historical Layer 0 ingest from the command line."""
    args = _parse_args()
    archive_config = load_fred_archive_config(Path(args.config))
    try:
        from_date = date.fromisoformat(args.from_date)
        to_date = _resolve_to_date(args.to_date or archive_config.default_end_date)
    except ValueError as exc:
        logger.error("Invalid date argument: {}", exc)
        return 1

    if from_date > to_date:
        logger.error("--from-date must be <= --to-date")
        return 1

    tiingo_config = TiingoClientConfig.from_env()
    config = HistoricalLayer0Config(
        from_date=from_date,
        to_date=to_date,
        tickers=args.tickers,
        fred_series_ids=args.series_ids or archive_config.series_ids,
        overwrite=args.overwrite,
        news_limit=args.news_limit,
        simfin_limit=args.simfin_limit,
        fred_limit=args.fred_limit,
        run_id=args.run_id,
    )
    result = run_historical_layer0_backfill(
        config=config,
        universe_provider=WikipediaUniverseProvider(),
        price_fetcher=TiingoOHLCVFetcher(tiingo_config),
        security_master=TiingoSecurityMaster.fetch_supported_tickers(),
        news_fetcher=AlpacaNewsClient(AlpacaMarketDataConfig.from_env()),
        fundamentals_fetcher=SimFinFundamentalsFetcher(SimFinClientConfig.from_env()),
        macro_fetcher=FredMacroFetcher(FredClientConfig.from_env()),
        writer=R2Writer(),
    )
    logger.info("Historical Layer 0 backfill complete: {}", result)
    return 0


def _parse_args() -> argparse.Namespace:
    """Parse historical Layer 0 CLI arguments."""
    parser = argparse.ArgumentParser(description="Backfill all Layer 0 raw archives into R2.")
    parser.add_argument("--config", default=str(DEFAULT_FRED_CONFIG_PATH))
    parser.add_argument("--from-date", required=True, metavar="YYYY-MM-DD")
    parser.add_argument("--to-date", metavar="YYYY-MM-DD")
    parser.add_argument("--tickers", nargs="*", default=None)
    parser.add_argument("--series-ids", nargs="*", default=None)
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--news-limit", type=int, default=DEFAULT_ALPACA_NEWS_PAGE_LIMIT)
    parser.add_argument("--simfin-limit", type=int, default=1000)
    parser.add_argument("--fred-limit", type=int, default=1000)
    args = parser.parse_args()
    if args.tickers == []:
        parser.error("--tickers requires at least one ticker when provided")
    if args.series_ids == []:
        parser.error("--series-ids requires at least one series when provided")
    return args


def _resolve_to_date(value: str) -> date:
    """Resolve an explicit YYYY-MM-DD date or the latest sentinel."""
    if value.strip().lower() == "latest":
        return date.today()
    return date.fromisoformat(value)


if __name__ == "__main__":
    sys.exit(main())
