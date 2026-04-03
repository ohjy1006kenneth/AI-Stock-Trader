from __future__ import annotations

from pathlib import Path
import sys

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import argparse
import csv
import json
import math
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from statistics import fmean, pvariance
from typing import Any

from cloud_training.data_pipelines.alpaca_news import fetch_news
from cloud_training.data_pipelines.finbert_sentiment import (
    FinBERTSentimentScorer,
    KeywordMockSentimentScorer,
    aggregate_ticker_day_sentiment_features,
)
from runtime.common.common import MARKET_DATA_DIR, now_iso, read_json

OUTPUT_DIR = ROOT_DIR / "data" / "processed" / "training"
SEQUENCE_LENGTH = 21
DEFAULT_NEWS_LOOKBACK_DAYS = 7
DEFAULT_MAX_TICKERS = 25
DEFAULT_MAX_SAMPLES_PER_TICKER = 0
DEFAULT_TICKER_SELECTION = "coverage"
MARKET_PROXY_TICKER = "SPY"
ALWAYS_INCLUDE_TICKERS = (MARKET_PROXY_TICKER,)
FUNDAMENTAL_SNAPSHOT_PATH = MARKET_DATA_DIR / "fundamental_snapshot.json"
SECTOR_UNKNOWN = "UNKNOWN"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build first predictive training dataset (OHLCV + Alpaca news, T+1 log return)")
    parser.add_argument("--max-tickers", type=int, default=DEFAULT_MAX_TICKERS, help="limit tickers for the first dataset pass")
    parser.add_argument("--tickers", default="", help="optional comma-separated explicit ticker list; bypasses selection scan when provided")
    parser.add_argument("--max-samples-per-ticker", type=int, default=DEFAULT_MAX_SAMPLES_PER_TICKER)
    parser.add_argument("--sequence-length", type=int, default=SEQUENCE_LENGTH)
    parser.add_argument("--news-lookback-days", type=int, default=DEFAULT_NEWS_LOOKBACK_DAYS)
    parser.add_argument("--output-prefix", default="predictive_dataset_v1")
    parser.add_argument(
        "--ticker-selection",
        choices=["alphabetical", "coverage"],
        default=DEFAULT_TICKER_SELECTION,
        help="how to choose target tickers when max-tickers truncates the universe",
    )
    parser.add_argument(
        "--coverage-lookback-days",
        type=int,
        default=90,
        help="lookback horizon used to rank tickers by recent Alpaca news coverage",
    )
    parser.add_argument(
        "--exclude-market-proxy-target",
        action="store_true",
        help="exclude the market proxy ticker from supervised target rows while still using it for market context",
    )
    parser.add_argument(
        "--sentiment-scorer",
        choices=["finbert", "mock"],
        default="finbert",
        help="article sentiment scorer; finbert is canonical, mock is only for tests/dev smoke runs",
    )
    parser.add_argument("--recency-halflife-hours", type=float, default=12.0)
    return parser.parse_args()


def load_price_history() -> tuple[dict[str, list[dict[str, Any]]], str | None]:
    payload = read_json(MARKET_DATA_DIR / "price_snapshot.json", {"items": []})
    out: dict[str, list[dict[str, Any]]] = {}
    for item in payload.get("items", []):
        ticker = item.get("ticker")
        history = item.get("history", [])
        if ticker and history:
            out[str(ticker)] = history
    return out, payload.get("source")


def load_fundamental_snapshot() -> tuple[dict[str, dict[str, Any]], str | None]:
    payload = read_json(FUNDAMENTAL_SNAPSHOT_PATH, {"items": []})
    out: dict[str, dict[str, Any]] = {}
    for item in payload.get("items", []):
        ticker = item.get("ticker")
        if ticker:
            out[str(ticker)] = item
    return out, payload.get("source")


def _normalize_tickers(available: list[str]) -> list[str]:
    return sorted(set(str(t).upper() for t in available if t))


def parse_explicit_tickers(raw: str) -> list[str]:
    return _normalize_tickers([part.strip() for part in str(raw or "").split(",") if part.strip()])


def _coverage_sort_key(item: tuple[str, dict[str, float]]) -> tuple[float, float, str]:
    ticker, stats = item
    return (
        float(stats.get("news_days", 0.0)),
        float(stats.get("news_items", 0.0)),
        ticker,
    )


def fetch_recent_news_coverage(
    tickers: list[str],
    *,
    lookback_days: int,
    end_dt: datetime | None = None,
    batch_size: int = 50,
) -> dict[str, dict[str, float]]:
    if not tickers:
        return {}
    end_dt = end_dt or datetime.now(timezone.utc)
    start_dt = end_dt - timedelta(days=max(int(lookback_days), 1))
    coverage: dict[str, dict[str, float]] = {
        ticker: {"news_items": 0.0, "news_days": 0.0, "sources": 0.0}
        for ticker in tickers
    }
    source_sets: dict[str, set[str]] = defaultdict(set)
    day_sets: dict[str, set[str]] = defaultdict(set)

    for start in range(0, len(tickers), max(int(batch_size), 1)):
        batch = tickers[start: start + max(int(batch_size), 1)]
        items = fetch_news(symbols=batch, start_iso=start_dt.isoformat(), end_iso=end_dt.isoformat(), limit=0)
        batch_members = set(batch)
        for item in items:
            symbols = [str(symbol).upper() for symbol in item.get("symbols") or [] if str(symbol).upper() in batch_members]
            if not symbols:
                continue
            item_day = str(item.get("date") or "")[:10]
            source = str(item.get("source") or "").strip().lower()
            for symbol in symbols:
                coverage[symbol]["news_items"] += 1.0
                if item_day:
                    day_sets[symbol].add(item_day)
                if source:
                    source_sets[symbol].add(source)

    for ticker in tickers:
        coverage[ticker]["news_days"] = float(len(day_sets[ticker]))
        coverage[ticker]["sources"] = float(len(source_sets[ticker]))
    return coverage


def select_tickers(
    available: list[str],
    max_tickers: int,
    *,
    strategy: str = DEFAULT_TICKER_SELECTION,
    coverage_lookback_days: int = 90,
    exclude_market_proxy_target: bool = False,
) -> tuple[list[str], dict[str, dict[str, float]]]:
    ordered = _normalize_tickers(available)
    if exclude_market_proxy_target:
        ordered = [ticker for ticker in ordered if ticker != MARKET_PROXY_TICKER]
    coverage_stats: dict[str, dict[str, float]] = {}

    if strategy == "coverage" and ordered:
        coverage_stats = fetch_recent_news_coverage(ordered, lookback_days=coverage_lookback_days)
        ranked = sorted(coverage_stats.items(), key=_coverage_sort_key, reverse=True)
        selected = [ticker for ticker, _ in ranked[:max_tickers]] if max_tickers > 0 else [ticker for ticker, _ in ranked]
    else:
        selected = ordered[:max_tickers] if max_tickers > 0 else ordered

    return selected, coverage_stats


def normalize_news_items(items: list[dict[str, Any]]) -> list[dict[str, str]]:
    normalized = []
    for item in items:
        normalized.append({
            "date": str(item.get("date") or ""),
            "timestamp": str(item.get("date") or ""),
            "headline": str(item.get("headline") or ""),
            "summary": str(item.get("summary") or ""),
            "source": str(item.get("source") or ""),
        })
    return normalized


def estimate_sample_start_date(history: list[dict[str, Any]], sequence_length: int, max_samples: int) -> str:
    if not history:
        raise ValueError("cannot_estimate_sample_start_date_without_history")
    if max_samples <= 0:
        return str(history[0]["date"])
    earliest_idx = max(sequence_length - 1, len(history) - 1 - max_samples)
    earliest_idx = min(max(earliest_idx, 0), len(history) - 1)
    return str(history[earliest_idx]["date"])


def fetch_ticker_news_map(ticker: str, start_date: str, end_date: str, lookback_days: int) -> dict[str, list[dict[str, str]]]:
    start_dt = datetime.fromisoformat(start_date + "T00:00:00+00:00") - timedelta(days=lookback_days)
    end_dt = datetime.fromisoformat(end_date + "T23:59:59+00:00")
    news_items = fetch_news(symbols=[ticker], start_iso=start_dt.isoformat(), end_iso=end_dt.isoformat(), limit=0)
    by_day: dict[str, list[dict[str, str]]] = defaultdict(list)
    for item in normalize_news_items(news_items):
        dt = item["date"]
        if not dt:
            continue
        by_day[dt[:10]].append(item)
    return dict(by_day)


def _parse_date(value: str) -> date:
    return datetime.fromisoformat(f"{value}T00:00:00+00:00").date()


def _collect_news_window(news_by_day: dict[str, list[dict[str, str]]], as_of_date: str, window_days: int) -> list[dict[str, str]]:
    if window_days <= 0:
        return []
    day = _parse_date(as_of_date)
    collected: list[dict[str, str]] = []
    for offset in range(window_days):
        key = (day - timedelta(days=offset)).isoformat()
        collected.extend(news_by_day.get(key, []))
    collected.sort(key=lambda item: str(item.get("timestamp") or item.get("date") or ""))
    return collected


def _compute_text_features(
    news_by_day: dict[str, list[dict[str, str]]],
    *,
    scorer: Any,
    as_of_date: str,
    recency_halflife_hours: float,
    rolling_window_days: int,
    aggregate_cache: dict[tuple[str, int], dict[str, Any]] | None = None,
) -> dict[str, Any]:
    same_day_news = list(news_by_day.get(as_of_date, []))
    rolling_days = max(int(rolling_window_days), 1)

    def _aggregate_for_window(window_days: int) -> dict[str, Any]:
        cache_key = (as_of_date, int(window_days))
        if aggregate_cache is not None and cache_key in aggregate_cache:
            return dict(aggregate_cache[cache_key])
        window_news = _collect_news_window(news_by_day, as_of_date, window_days)
        aggregated = aggregate_ticker_day_sentiment_features(
            window_news,
            scorer=scorer,
            as_of_date=as_of_date,
            recency_halflife_hours=recency_halflife_hours,
        )
        if aggregate_cache is not None:
            aggregate_cache[cache_key] = dict(aggregated)
        return dict(aggregated)

    features = _aggregate_for_window(rolling_days)
    features_3d = _aggregate_for_window(3)
    features_7d = features if rolling_days == 7 else _aggregate_for_window(7)
    trailing_7d_news = _collect_news_window(news_by_day, as_of_date, 7)

    source_count_7d = len({str(item.get("source") or "").strip().lower() for item in trailing_7d_news if str(item.get("source") or "").strip()})
    coverage_days_7d = sum(1 for offset in range(7) if news_by_day.get((_parse_date(as_of_date) - timedelta(days=offset)).isoformat()))
    days_since_last_news_7d = 8.0
    for offset in range(7):
        if news_by_day.get((_parse_date(as_of_date) - timedelta(days=offset)).isoformat()):
            days_since_last_news_7d = float(offset)
            break

    features.update({
        "same_day_news_count": float(len(same_day_news)),
        "rolling_news_window_days": float(rolling_days),
        "news_count_3d": float(features_3d["news_count"]),
        "news_count_7d": float(features_7d["news_count"]),
        "news_days_with_coverage_7d": float(coverage_days_7d),
        "news_source_count_7d": float(source_count_7d),
        "days_since_last_news_7d": float(days_since_last_news_7d),
        "finbert_sentiment_score_mean_3d": float(features_3d["finbert_sentiment_score_mean"]),
        "finbert_sentiment_score_mean_7d": float(features_7d["finbert_sentiment_score_mean"]),
        "finbert_sentiment_score_recency_weighted_3d": float(features_3d["finbert_sentiment_score_recency_weighted"]),
        "finbert_sentiment_score_recency_weighted_7d": float(features_7d["finbert_sentiment_score_recency_weighted"]),
        "sentiment_acceleration_3d_vs_7d": float(features_3d["finbert_sentiment_score_recency_weighted"] - features_7d["finbert_sentiment_score_recency_weighted"]),
        "news_count_surprise_3d_vs_7d": float(features_3d["news_count"] - (features_7d["news_count"] / 7.0 * 3.0 if features_7d["news_count"] > 0 else 0.0)),
    })
    return features


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value in (None, ""):
            return default
        return float(value)
    except Exception:
        return default


def _safe_log(value: Any, default: float = 0.0) -> float:
    value_f = _safe_float(value, 0.0)
    return math.log(value_f) if value_f > 0 else default


def _safe_ratio(numerator: float, denominator: float) -> float:
    if abs(denominator) < 1e-12:
        return 0.0
    return numerator / denominator


def _close_return(closes: list[float], lookback: int) -> float:
    if len(closes) <= lookback:
        return 0.0
    start = closes[-lookback - 1]
    end = closes[-1]
    if start <= 0 or end <= 0:
        return 0.0
    return math.log(end / start)


def _daily_log_returns(closes: list[float]) -> list[float]:
    out: list[float] = []
    for prev_close, close in zip(closes[:-1], closes[1:]):
        if prev_close <= 0 or close <= 0:
            out.append(0.0)
            continue
        out.append(math.log(close / prev_close))
    return out


def _realized_volatility(closes: list[float], window: int) -> float:
    returns = _daily_log_returns(closes)
    if len(returns) < window:
        return 0.0
    sample = returns[-window:]
    if len(sample) < 2:
        return 0.0
    return math.sqrt(pvariance(sample))


def _moving_average(closes: list[float], window: int) -> float:
    if len(closes) < window:
        return 0.0
    return fmean(closes[-window:])


def _max_drawdown(closes: list[float], window: int) -> float:
    if len(closes) < window:
        return 0.0
    sample = closes[-window:]
    peak = sample[0]
    worst = 0.0
    for close in sample:
        peak = max(peak, close)
        if peak > 0:
            worst = min(worst, (close / peak) - 1.0)
    return worst


def _atr_ratio(history: list[dict[str, Any]], window: int) -> float:
    if len(history) < window + 1:
        return 0.0
    trs: list[float] = []
    sample = history[-(window + 1):]
    for prev_row, row in zip(sample[:-1], sample[1:]):
        high = _safe_float(row.get("high"))
        low = _safe_float(row.get("low"))
        prev_close = _safe_float(prev_row.get("close"))
        close = _safe_float(row.get("close"))
        if close <= 0:
            continue
        tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
        trs.append(tr)
    if not trs:
        return 0.0
    return fmean(trs) / max(_safe_float(sample[-1].get("close")), 1e-12)


def _beta_and_corr(asset_returns: list[float], benchmark_returns: list[float], window: int) -> tuple[float, float]:
    if len(asset_returns) < window or len(benchmark_returns) < window:
        return 0.0, 0.0
    a = asset_returns[-window:]
    b = benchmark_returns[-window:]
    if len(a) != len(b) or len(a) < 2:
        return 0.0, 0.0
    mean_a = fmean(a)
    mean_b = fmean(b)
    cov = sum((x - mean_a) * (y - mean_b) for x, y in zip(a, b)) / len(a)
    var_b = pvariance(b) if len(b) > 1 else 0.0
    var_a = pvariance(a) if len(a) > 1 else 0.0
    beta = cov / var_b if var_b > 1e-12 else 0.0
    corr = cov / math.sqrt(var_a * var_b) if var_a > 1e-12 and var_b > 1e-12 else 0.0
    return beta, corr


def compute_market_features(history_window: list[dict[str, Any]], market_history_window: list[dict[str, Any]]) -> dict[str, float]:
    closes = [_safe_float(row.get("close")) for row in history_window]
    highs = [_safe_float(row.get("high")) for row in history_window]
    lows = [_safe_float(row.get("low")) for row in history_window]
    opens = [_safe_float(row.get("open")) for row in history_window]
    volumes = [_safe_float(row.get("volume")) for row in history_window]
    market_closes = [_safe_float(row.get("close")) for row in market_history_window]

    last_close = closes[-1]
    prev_close = closes[-2] if len(closes) >= 2 else last_close
    last_open = opens[-1]
    avg_volume_5 = fmean(volumes[-5:]) if len(volumes) >= 5 else fmean(volumes)
    avg_volume_21 = fmean(volumes[-21:]) if len(volumes) >= 21 else fmean(volumes)
    sma_5 = _moving_average(closes, 5)
    sma_10 = _moving_average(closes, 10)
    sma_21 = _moving_average(closes, 21)
    sma_63 = _moving_average(closes, 63)
    asset_returns = _daily_log_returns(closes)
    market_returns = _daily_log_returns(market_closes)
    beta_21, corr_21 = _beta_and_corr(asset_returns, market_returns, 21)

    return {
        "return_1d": math.log(last_close / prev_close) if last_close > 0 and prev_close > 0 else 0.0,
        "return_5d": _close_return(closes, 5),
        "return_10d": _close_return(closes, 10),
        "return_21d": _close_return(closes, 21),
        "return_63d": _close_return(closes, 63),
        "range_ratio_last": _safe_ratio(highs[-1] - lows[-1], last_close),
        "gap_ratio_last": _safe_ratio(last_open - prev_close, prev_close) if prev_close > 0 else 0.0,
        "intraday_return_last": _safe_ratio(last_close - last_open, last_open) if last_open > 0 else 0.0,
        "volume_ratio_last": (_safe_ratio(volumes[-1], avg_volume_21) - 1.0) if avg_volume_21 > 0 else 0.0,
        "volume_ratio_5d": (_safe_ratio(avg_volume_5, avg_volume_21) - 1.0) if avg_volume_21 > 0 else 0.0,
        "realized_vol_5d": _realized_volatility(closes, 5),
        "realized_vol_21d": _realized_volatility(closes, 21),
        "realized_vol_63d": _realized_volatility(closes, 63),
        "atr_ratio_14d": _atr_ratio(history_window, 14),
        "sma_5_over_21": (_safe_ratio(sma_5, sma_21) - 1.0) if sma_5 > 0 and sma_21 > 0 else 0.0,
        "sma_10_over_21": (_safe_ratio(sma_10, sma_21) - 1.0) if sma_10 > 0 and sma_21 > 0 else 0.0,
        "sma_21_over_63": (_safe_ratio(sma_21, sma_63) - 1.0) if sma_21 > 0 and sma_63 > 0 else 0.0,
        "drawdown_21d": _max_drawdown(closes, 21),
        "beta_to_spy_21d": beta_21,
        "corr_to_spy_21d": corr_21,
        "relative_strength_vs_spy_21d": _close_return(closes, 21) - _close_return(market_closes, 21),
    }


def compute_macro_features(market_history_window: list[dict[str, Any]]) -> dict[str, float]:
    closes = [_safe_float(row.get("close")) for row in market_history_window]
    return {
        "macro_spy_return_5d": _close_return(closes, 5),
        "macro_spy_return_21d": _close_return(closes, 21),
        "macro_spy_return_63d": _close_return(closes, 63),
        "macro_spy_realized_vol_21d": _realized_volatility(closes, 21),
        "macro_spy_realized_vol_63d": _realized_volatility(closes, 63),
        "macro_spy_drawdown_21d": _max_drawdown(closes, 21),
        "macro_spy_sma_21_over_63": (_safe_ratio(_moving_average(closes, 21), _moving_average(closes, 63)) - 1.0) if _moving_average(closes, 21) > 0 and _moving_average(closes, 63) > 0 else 0.0,
    }


def build_sector_date_features(
    prices: dict[str, list[dict[str, Any]]],
    fundamentals: dict[str, dict[str, Any]],
) -> dict[tuple[str, str], dict[str, float]]:
    sector_groups: dict[str, list[str]] = defaultdict(list)
    for ticker, item in fundamentals.items():
        sector = str(item.get("sector") or SECTOR_UNKNOWN)
        sector_groups[sector].append(ticker)

    feature_map: dict[tuple[str, str], dict[str, float]] = {}
    for sector, tickers in sector_groups.items():
        if not tickers:
            continue
        date_values: dict[str, list[tuple[str, float, float]]] = defaultdict(list)
        for ticker in tickers:
            history = prices.get(ticker) or []
            for idx in range(21, len(history)):
                window = history[: idx + 1]
                as_of_date = str(history[idx].get("date"))
                closes = [_safe_float(row.get("close")) for row in window]
                date_values[as_of_date].append((
                    ticker,
                    _close_return(closes, 21),
                    _realized_volatility(closes, 21),
                ))
        for as_of_date, values in date_values.items():
            mean_return = fmean(item[1] for item in values) if values else 0.0
            mean_vol = fmean(item[2] for item in values) if values else 0.0
            peer_count = max(len(values) - 1, 0)
            for ticker, ret_21, vol_21 in values:
                feature_map[(ticker, as_of_date)] = {
                    "sector_peer_return_21d_mean": mean_return,
                    "sector_peer_vol_21d_mean": mean_vol,
                    "sector_relative_return_21d": ret_21 - mean_return,
                    "sector_relative_vol_21d": vol_21 - mean_vol,
                    "sector_peer_count": float(peer_count),
                }
    return feature_map


def compute_context_features(
    ticker: str,
    as_of_date: str,
    fundamentals: dict[str, dict[str, Any]],
    sector_feature_map: dict[tuple[str, str], dict[str, float]],
) -> dict[str, Any]:
    item = fundamentals.get(ticker, {})
    market_cap = _safe_float(item.get("market_cap"))
    average_volume = _safe_float(item.get("average_volume"))
    free_cash_flow = _safe_float(item.get("free_cash_flow"))
    sector = str(item.get("sector") or SECTOR_UNKNOWN)
    industry = str(item.get("industry") or SECTOR_UNKNOWN)
    context = {
        "country": str(item.get("country") or ""),
        "quote_type": str(item.get("quote_type") or ""),
        "sector": sector,
        "industry": industry,
        "market_cap_log": _safe_log(market_cap),
        "average_volume_log": _safe_log(average_volume),
        "net_margin": _safe_float(item.get("net_margin")),
        "debt_to_equity": _safe_float(item.get("debt_to_equity")),
        "revenue_growth": _safe_float(item.get("revenue_growth")),
        "operating_margin": _safe_float(item.get("operating_margin")),
        "return_on_equity": _safe_float(item.get("return_on_equity")),
        "free_cash_flow_yield": _safe_ratio(free_cash_flow, market_cap) if market_cap > 0 else 0.0,
    }
    context.update(sector_feature_map.get((ticker, as_of_date), {
        "sector_peer_return_21d_mean": 0.0,
        "sector_peer_vol_21d_mean": 0.0,
        "sector_relative_return_21d": 0.0,
        "sector_relative_vol_21d": 0.0,
        "sector_peer_count": 0.0,
    }))
    return context


def build_samples_for_ticker(
    ticker: str,
    history: list[dict[str, Any]],
    news_by_day: dict[str, list[dict[str, str]]],
    sequence_length: int,
    max_samples: int,
    sentiment_scorer: Any,
    recency_halflife_hours: float,
    market_history: list[dict[str, Any]],
    fundamentals: dict[str, dict[str, Any]],
    sector_feature_map: dict[tuple[str, str], dict[str, float]],
) -> list[dict[str, Any]]:
    rows = [row for row in history if all(k in row for k in ("date", "open", "high", "low", "close", "volume"))]
    rows.sort(key=lambda x: x["date"])
    market_rows = [row for row in market_history if all(k in row for k in ("date", "open", "high", "low", "close", "volume"))]
    market_by_date = {str(row["date"]): idx for idx, row in enumerate(market_rows)}
    samples: list[dict[str, Any]] = []
    text_feature_cache: dict[tuple[str, int], dict[str, Any]] = {}
    # Need 64 closes to compute a true 63-day return (lookback + current bar),
    # and the 63-day realized volatility path also benefits from the extra bar.
    extended_window = max(sequence_length, 64)
    for idx in range(sequence_length - 1, len(rows) - 1):
        window = rows[idx - sequence_length + 1: idx + 1]
        today = rows[idx]
        tomorrow = rows[idx + 1]
        close_t = float(today["close"])
        close_tp1 = float(tomorrow["close"])
        if close_t <= 0 or close_tp1 <= 0:
            continue
        market_idx = market_by_date.get(str(today["date"]))
        if market_idx is None or market_idx + 1 < extended_window:
            continue
        market_window = market_rows[market_idx - extended_window + 1: market_idx + 1]
        history_window = rows[max(0, idx - extended_window + 1): idx + 1]
        if len(history_window) < extended_window:
            continue
        target_log_return = math.log(close_tp1 / close_t)
        raw_news = _collect_news_window(news_by_day, today["date"], max(sequence_length, 7))
        text_features = _compute_text_features(
            news_by_day,
            scorer=sentiment_scorer,
            as_of_date=today["date"],
            recency_halflife_hours=recency_halflife_hours,
            rolling_window_days=7,
            aggregate_cache=text_feature_cache,
        )
        market_features = compute_market_features(history_window, market_window)
        macro_features = compute_macro_features(market_window)
        context_features = compute_context_features(ticker, today["date"], fundamentals, sector_feature_map)
        sample = {
            "ticker": ticker,
            "as_of_date": today["date"],
            "target_date": tomorrow["date"],
            "sequence_length": sequence_length,
            "history": [
                {
                    "date": row["date"],
                    "open": float(row["open"]),
                    "high": float(row["high"]),
                    "low": float(row["low"]),
                    "close": float(row["close"]),
                    "volume": int(row["volume"]),
                }
                for row in window
            ],
            "news": raw_news,
            **text_features,
            **market_features,
            **macro_features,
            **context_features,
            "target_log_return_t_plus_1": target_log_return,
            "target_positive_return": 1 if target_log_return > 0 else 0,
        }
        samples.append(sample)
    if max_samples > 0:
        samples = samples[-max_samples:]
    return samples


def write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as f:
        for row in rows:
            f.write(json.dumps(row) + "\n")


def write_manifest(path: Path, manifest: dict[str, Any]) -> None:
    path.write_text(json.dumps(manifest, indent=2))


def write_summary_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "ticker",
        "samples",
        "avg_news_count",
        "avg_same_day_news_count",
        "avg_news_count_7d",
        "avg_news_source_count_7d",
        "avg_finbert_positive_prob_mean",
        "avg_finbert_negative_prob_mean",
        "avg_finbert_sentiment_score_recency_weighted",
        "avg_finbert_sentiment_score_recency_weighted_7d",
        "avg_return_21d",
        "avg_realized_vol_21d",
        "avg_beta_to_spy_21d",
    ]
    by_ticker: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        by_ticker[row["ticker"]].append(row)
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for ticker, items in sorted(by_ticker.items()):
            writer.writerow({
                "ticker": ticker,
                "samples": len(items),
                "avg_news_count": round(sum(x["news_count"] for x in items) / max(len(items), 1), 4),
                "avg_same_day_news_count": round(sum(x["same_day_news_count"] for x in items) / max(len(items), 1), 4),
                "avg_news_count_7d": round(sum(x["news_count_7d"] for x in items) / max(len(items), 1), 4),
                "avg_news_source_count_7d": round(sum(x["news_source_count_7d"] for x in items) / max(len(items), 1), 4),
                "avg_finbert_positive_prob_mean": round(sum(x["finbert_positive_prob_mean"] for x in items) / max(len(items), 1), 6),
                "avg_finbert_negative_prob_mean": round(sum(x["finbert_negative_prob_mean"] for x in items) / max(len(items), 1), 6),
                "avg_finbert_sentiment_score_recency_weighted": round(sum(x["finbert_sentiment_score_recency_weighted"] for x in items) / max(len(items), 1), 6),
                "avg_finbert_sentiment_score_recency_weighted_7d": round(sum(x["finbert_sentiment_score_recency_weighted_7d"] for x in items) / max(len(items), 1), 6),
                "avg_return_21d": round(sum(x["return_21d"] for x in items) / max(len(items), 1), 6),
                "avg_realized_vol_21d": round(sum(x["realized_vol_21d"] for x in items) / max(len(items), 1), 6),
                "avg_beta_to_spy_21d": round(sum(x["beta_to_spy_21d"] for x in items) / max(len(items), 1), 6),
            })


def build_sentiment_scorer(name: str) -> Any:
    if name == "mock":
        return KeywordMockSentimentScorer()
    if name == "finbert":
        return FinBERTSentimentScorer()
    raise SystemExit(f"unsupported_sentiment_scorer:{name}")


def main() -> None:
    args = parse_args()
    prices, history_source = load_price_history()
    fundamentals, fundamental_source = load_fundamental_snapshot()
    if MARKET_PROXY_TICKER not in prices:
        raise SystemExit("spy_market_proxy_missing_for_market_context_features")

    explicit_tickers = parse_explicit_tickers(args.tickers)
    if explicit_tickers:
        available_prices = {str(ticker).upper() for ticker in prices.keys()}
        missing = [ticker for ticker in explicit_tickers if ticker not in available_prices]
        if missing:
            raise SystemExit(f"explicit_tickers_missing_price_history:{','.join(missing)}")
        tickers = [ticker for ticker in explicit_tickers if not (args.exclude_market_proxy_target and ticker == MARKET_PROXY_TICKER)]
        selection_coverage = {}
    else:
        tickers, selection_coverage = select_tickers(
            list(prices.keys()),
            args.max_tickers,
            strategy=args.ticker_selection,
            coverage_lookback_days=args.coverage_lookback_days,
            exclude_market_proxy_target=args.exclude_market_proxy_target,
        )
    if not tickers:
        raise SystemExit("no_price_history_available_for_dataset_build")

    sentiment_scorer = build_sentiment_scorer(args.sentiment_scorer)
    sector_feature_map = build_sector_date_features(prices, fundamentals)
    all_rows: list[dict[str, Any]] = []
    ticker_stats: dict[str, dict[str, Any]] = {}
    for ticker in tickers:
        history = prices[ticker]
        start_date = estimate_sample_start_date(history, args.sequence_length, args.max_samples_per_ticker)
        end_date = history[-1]["date"]
        news_by_day = fetch_ticker_news_map(ticker, start_date, end_date, args.news_lookback_days)
        rows = build_samples_for_ticker(
            ticker=ticker,
            history=history,
            news_by_day=news_by_day,
            sequence_length=args.sequence_length,
            max_samples=args.max_samples_per_ticker,
            sentiment_scorer=sentiment_scorer,
            recency_halflife_hours=args.recency_halflife_hours,
            market_history=prices[MARKET_PROXY_TICKER],
            fundamentals=fundamentals,
            sector_feature_map=sector_feature_map,
        )
        all_rows.extend(rows)
        coverage = selection_coverage.get(ticker, {})
        ticker_stats[ticker] = {
            "history_rows": len(history),
            "news_days": len(news_by_day),
            "samples": len(rows),
            "has_fundamentals": ticker in fundamentals,
            "selection_news_items_lookback": int(coverage.get("news_items", 0.0)),
            "selection_news_days_lookback": int(coverage.get("news_days", 0.0)),
            "selection_sources_lookback": int(coverage.get("sources", 0.0)),
        }

    all_rows.sort(key=lambda row: (str(row.get("ticker") or ""), str(row.get("as_of_date") or "")))
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    prefix = f"{args.output_prefix}_{stamp}"
    jsonl_path = OUTPUT_DIR / f"{prefix}.jsonl"
    manifest_path = OUTPUT_DIR / f"{prefix}.manifest.json"
    summary_csv = OUTPUT_DIR / f"{prefix}.summary.csv"

    market_feature_fields = [
        "return_1d",
        "return_5d",
        "return_10d",
        "return_21d",
        "return_63d",
        "range_ratio_last",
        "gap_ratio_last",
        "intraday_return_last",
        "volume_ratio_last",
        "volume_ratio_5d",
        "realized_vol_5d",
        "realized_vol_21d",
        "realized_vol_63d",
        "atr_ratio_14d",
        "sma_5_over_21",
        "sma_10_over_21",
        "sma_21_over_63",
        "drawdown_21d",
        "beta_to_spy_21d",
        "corr_to_spy_21d",
        "relative_strength_vs_spy_21d",
    ]
    macro_feature_fields = [
        "macro_spy_return_5d",
        "macro_spy_return_21d",
        "macro_spy_return_63d",
        "macro_spy_realized_vol_21d",
        "macro_spy_realized_vol_63d",
        "macro_spy_drawdown_21d",
        "macro_spy_sma_21_over_63",
    ]
    context_feature_fields = [
        "sector",
        "industry",
        "market_cap_log",
        "average_volume_log",
        "net_margin",
        "debt_to_equity",
        "revenue_growth",
        "operating_margin",
        "return_on_equity",
        "free_cash_flow_yield",
        "sector_peer_return_21d_mean",
        "sector_peer_vol_21d_mean",
        "sector_relative_return_21d",
        "sector_relative_vol_21d",
        "sector_peer_count",
    ]

    write_jsonl(jsonl_path, all_rows)
    write_summary_csv(summary_csv, all_rows)
    write_manifest(manifest_path, {
        "generated_at": now_iso(),
        "dataset_name": prefix,
        "target": "next_day_log_return",
        "sequence_length": args.sequence_length,
        "news_source": "alpaca_news_api",
        "article_sentiment_model": args.sentiment_scorer,
        "article_sentiment_model_name": "ProsusAI/finbert" if args.sentiment_scorer == "finbert" else "keyword_mock_sentiment",
        "recency_halflife_hours": args.recency_halflife_hours,
        "history_source": history_source or "unknown",
        "fundamental_source": fundamental_source or "unknown",
        "market_proxy_ticker": MARKET_PROXY_TICKER,
        "always_include_tickers": list(ALWAYS_INCLUDE_TICKERS),
        "max_samples_per_ticker": args.max_samples_per_ticker,
        "ticker_selection": args.ticker_selection,
        "explicit_tickers": explicit_tickers,
        "coverage_lookback_days": args.coverage_lookback_days,
        "exclude_market_proxy_target": bool(args.exclude_market_proxy_target),
        "tickers_considered": tickers,
        "ticker_stats": ticker_stats,
        "rows": len(all_rows),
        "row_ordering": ["ticker", "as_of_date"],
        "jsonl_path": str(jsonl_path.relative_to(ROOT_DIR)),
        "summary_csv": str(summary_csv.relative_to(ROOT_DIR)),
        "text_feature_fields": [
            "news_count",
            "news_volume",
            "same_day_news_count",
            "rolling_news_window_days",
            "news_count_3d",
            "news_count_7d",
            "news_days_with_coverage_7d",
            "news_source_count_7d",
            "days_since_last_news_7d",
            "finbert_positive_prob_mean",
            "finbert_negative_prob_mean",
            "finbert_neutral_prob_mean",
            "finbert_sentiment_score_mean",
            "finbert_positive_prob_recency_weighted",
            "finbert_negative_prob_recency_weighted",
            "finbert_neutral_prob_recency_weighted",
            "finbert_sentiment_score_recency_weighted",
            "finbert_sentiment_score_mean_3d",
            "finbert_sentiment_score_mean_7d",
            "finbert_sentiment_score_recency_weighted_3d",
            "finbert_sentiment_score_recency_weighted_7d",
            "sentiment_acceleration_3d_vs_7d",
            "news_count_surprise_3d_vs_7d",
            "finbert_article_age_hours_min",
            "finbert_article_age_hours_max",
            "finbert_article_age_hours_mean",
            "finbert_recency_weight_sum",
        ],
        "market_feature_fields": market_feature_fields,
        "macro_feature_fields": macro_feature_fields,
        "context_feature_fields": context_feature_fields,
    })
    print(json.dumps({
        "status": "ok",
        "rows": len(all_rows),
        "tickers": len(tickers),
        "jsonl": str(jsonl_path.relative_to(ROOT_DIR)),
        "manifest": str(manifest_path.relative_to(ROOT_DIR)),
        "summary_csv": str(summary_csv.relative_to(ROOT_DIR)),
    }, indent=2))


if __name__ == "__main__":
    main()
