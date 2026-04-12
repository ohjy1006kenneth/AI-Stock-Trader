"""Historical Alpaca news backfill into the canonical R2 raw archive."""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Protocol

from loguru import logger

_REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(_REPO_ROOT))

from services.alpaca.market_data import AlpacaMarketDataConfig  # noqa: E402
from services.alpaca.news import DEFAULT_ALPACA_NEWS_PAGE_LIMIT, AlpacaNewsClient  # noqa: E402
from services.r2.paths import raw_news_path  # noqa: E402
from services.r2.writer import R2Writer  # noqa: E402
from services.wikipedia.sp500_universe import get_all_historical_tickers  # noqa: E402


class ObjectWriter(Protocol):
    """Subset of R2Writer used by the news backfill."""

    def put_object(self, key: str, data: bytes | str) -> None:
        """Write an object to storage."""

    def exists(self, key: str) -> bool:
        """Return True when an object already exists."""


class NewsFetcher(Protocol):
    """Subset of AlpacaNewsClient used by the news backfill."""

    def fetch_news_day(
        self,
        *,
        tickers: list[str] | None,
        as_of_date: str,
        limit: int,
    ) -> list[dict[str, object]]:
        """Fetch all raw Alpaca news rows for a single date."""


NewsSerializer = Callable[[list[dict[str, object]]], bytes]


@dataclass(frozen=True)
class BackfillResult:
    """Summary of an Alpaca news backfill run."""

    requested: int
    written: int
    skipped: int
    empty: int
    total_articles: int


def backfill_news_archive(
    from_date: date,
    to_date: date,
    *,
    fetcher: NewsFetcher,
    writer: ObjectWriter,
    tickers: list[str] | None = None,
    overwrite: bool = False,
    limit: int = DEFAULT_ALPACA_NEWS_PAGE_LIMIT,
    serializer: NewsSerializer | None = None,
) -> BackfillResult:
    """Backfill Alpaca raw news into R2 as JSON Lines per date."""
    if from_date > to_date:
        raise ValueError("from_date must be <= to_date")
    if limit <= 0:
        raise ValueError("limit must be positive")

    tickers_source = (
        sorted(set(get_all_historical_tickers(from_date.isoformat(), to_date.isoformat())))
        if tickers is None
        else sorted(set(tickers))
    )
    payload_serializer = serializer or _articles_to_jsonl_bytes

    written = 0
    skipped = 0
    empty = 0
    total_articles = 0

    for current_date in _date_range(from_date, to_date):
        key = raw_news_path(current_date)
        if writer.exists(key) and not overwrite:
            skipped += 1
            logger.info("Skipping existing Alpaca news archive {}", key)
            continue

        articles = fetcher.fetch_news_day(
            tickers=tickers_source,
            as_of_date=current_date.isoformat(),
            limit=limit,
        )
        if not articles:
            empty += 1
            logger.info("No Alpaca news rows for {}", current_date.isoformat())
        else:
            total_articles += len(articles)

        writer.put_object(key, payload_serializer(_sort_articles(articles)))
        written += 1
        logger.info("Wrote {} Alpaca news rows to {}", len(articles), key)

    return BackfillResult(
        requested=_count_days(from_date, to_date),
        written=written,
        skipped=skipped,
        empty=empty,
        total_articles=total_articles,
    )


def _articles_to_jsonl_bytes(articles: list[dict[str, object]]) -> bytes:
    """Serialize raw news articles as JSON Lines bytes."""
    if not articles:
        return b""
    lines = [json.dumps(article, sort_keys=True, separators=(",", ":")) for article in articles]
    return ("\n".join(lines) + "\n").encode("utf-8")


def _sort_articles(articles: list[dict[str, object]]) -> list[dict[str, object]]:
    """Sort raw news articles deterministically before writing."""
    return sorted(
        articles,
        key=lambda article: (
            str(
                article.get("created_at")
                or article.get("updated_at")
                or article.get("publishedDate")
                or article.get("published_at")
                or ""
            ),
            str(article.get("id") or article.get("url") or article.get("headline") or ""),
        ),
    )


def _date_range(start: date, end: date) -> Iterable[date]:
    """Yield each date in [start, end] inclusive."""
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def _count_days(start: date, end: date) -> int:
    """Count inclusive days between two dates."""
    return (end - start).days + 1


def _parse_args() -> argparse.Namespace:
    """Parse CLI arguments for the historical news backfill."""
    parser = argparse.ArgumentParser(description="Backfill Alpaca raw news into R2.")
    parser.add_argument("--from-date", required=True, metavar="YYYY-MM-DD")
    parser.add_argument("--to-date", required=True, metavar="YYYY-MM-DD")
    parser.add_argument(
        "--tickers",
        nargs="*",
        default=None,
        help="Optional ticker list. Defaults to all historical S&P 500 constituents.",
    )
    parser.add_argument("--overwrite", action="store_true", help="Rewrite existing R2 objects.")
    parser.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_ALPACA_NEWS_PAGE_LIMIT,
        help=f"Page size for Alpaca pagination (default: {DEFAULT_ALPACA_NEWS_PAGE_LIMIT}).",
    )
    args = parser.parse_args()
    if args.tickers == []:
        parser.error("--tickers requires at least one ticker when provided")
    return args


def main() -> int:
    """Run the Alpaca news backfill from the command line."""
    args = _parse_args()
    try:
        from_date = date.fromisoformat(args.from_date)
        to_date = date.fromisoformat(args.to_date)
    except ValueError as exc:
        logger.error("Invalid date argument: {}", exc)
        return 1

    if from_date > to_date:
        logger.error("--from-date must be <= --to-date")
        return 1

    writer = R2Writer()
    fetcher = AlpacaNewsClient(AlpacaMarketDataConfig.from_env())
    result = backfill_news_archive(
        from_date=from_date,
        to_date=to_date,
        fetcher=fetcher,
        writer=writer,
        tickers=args.tickers,
        overwrite=args.overwrite,
        limit=args.limit,
    )
    logger.info("Alpaca news backfill complete: {}", result)
    return 0


if __name__ == "__main__":
    sys.exit(main())
