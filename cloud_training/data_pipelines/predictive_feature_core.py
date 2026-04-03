from __future__ import annotations

import math
from statistics import fmean, pvariance
from typing import Any

SECTOR_UNKNOWN = "UNKNOWN"


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value in (None, ""):
            return default
        return float(value)
    except Exception:
        return default


def safe_log(value: Any, default: float = 0.0) -> float:
    value_f = safe_float(value, 0.0)
    return math.log(value_f) if value_f > 0 else default


def safe_ratio(numerator: float, denominator: float) -> float:
    if abs(denominator) < 1e-12:
        return 0.0
    return numerator / denominator


def close_return(closes: list[float], lookback: int) -> float:
    if len(closes) <= lookback:
        return 0.0
    start = closes[-lookback - 1]
    end = closes[-1]
    if start <= 0 or end <= 0:
        return 0.0
    return math.log(end / start)


def daily_log_returns(closes: list[float]) -> list[float]:
    out: list[float] = []
    for prev_close, close in zip(closes[:-1], closes[1:]):
        if prev_close <= 0 or close <= 0:
            out.append(0.0)
            continue
        out.append(math.log(close / prev_close))
    return out


def realized_volatility(closes: list[float], window: int) -> float:
    returns = daily_log_returns(closes)
    if len(returns) < window:
        return 0.0
    sample = returns[-window:]
    if len(sample) < 2:
        return 0.0
    return math.sqrt(pvariance(sample))


def moving_average(closes: list[float], window: int) -> float:
    if len(closes) < window:
        return 0.0
    return fmean(closes[-window:])


def max_drawdown(closes: list[float], window: int) -> float:
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


def atr_ratio(history: list[dict[str, Any]], window: int) -> float:
    if len(history) < window + 1:
        return 0.0
    trs: list[float] = []
    sample = history[-(window + 1):]
    for prev_row, row in zip(sample[:-1], sample[1:]):
        high = safe_float(row.get("high"))
        low = safe_float(row.get("low"))
        prev_close = safe_float(prev_row.get("close"))
        close = safe_float(row.get("close"))
        if close <= 0:
            continue
        tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
        trs.append(tr)
    if not trs:
        return 0.0
    return fmean(trs) / max(safe_float(sample[-1].get("close")), 1e-12)


def beta_and_corr(asset_returns: list[float], benchmark_returns: list[float], window: int) -> tuple[float, float]:
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
    closes = [safe_float(row.get("close")) for row in history_window]
    highs = [safe_float(row.get("high")) for row in history_window]
    lows = [safe_float(row.get("low")) for row in history_window]
    opens = [safe_float(row.get("open")) for row in history_window]
    volumes = [safe_float(row.get("volume")) for row in history_window]
    market_closes = [safe_float(row.get("close")) for row in market_history_window]

    last_close = closes[-1]
    prev_close = closes[-2] if len(closes) >= 2 else last_close
    last_open = opens[-1]
    avg_volume_5 = fmean(volumes[-5:]) if len(volumes) >= 5 else fmean(volumes)
    avg_volume_21 = fmean(volumes[-21:]) if len(volumes) >= 21 else fmean(volumes)
    sma_5 = moving_average(closes, 5)
    sma_10 = moving_average(closes, 10)
    sma_21 = moving_average(closes, 21)
    sma_63 = moving_average(closes, 63)
    asset_returns = daily_log_returns(closes)
    market_returns = daily_log_returns(market_closes)
    beta_21, corr_21 = beta_and_corr(asset_returns, market_returns, 21)

    return {
        "return_1d": math.log(last_close / prev_close) if last_close > 0 and prev_close > 0 else 0.0,
        "return_5d": close_return(closes, 5),
        "return_10d": close_return(closes, 10),
        "return_21d": close_return(closes, 21),
        "return_63d": close_return(closes, 63),
        "range_ratio_last": safe_ratio(highs[-1] - lows[-1], last_close),
        "gap_ratio_last": safe_ratio(last_open - prev_close, prev_close) if prev_close > 0 else 0.0,
        "intraday_return_last": safe_ratio(last_close - last_open, last_open) if last_open > 0 else 0.0,
        "volume_ratio_last": (safe_ratio(volumes[-1], avg_volume_21) - 1.0) if avg_volume_21 > 0 else 0.0,
        "volume_ratio_5d": (safe_ratio(avg_volume_5, avg_volume_21) - 1.0) if avg_volume_21 > 0 else 0.0,
        "realized_vol_5d": realized_volatility(closes, 5),
        "realized_vol_21d": realized_volatility(closes, 21),
        "realized_vol_63d": realized_volatility(closes, 63),
        "atr_ratio_14d": atr_ratio(history_window, 14),
        "sma_5_over_21": (safe_ratio(sma_5, sma_21) - 1.0) if sma_5 > 0 and sma_21 > 0 else 0.0,
        "sma_10_over_21": (safe_ratio(sma_10, sma_21) - 1.0) if sma_10 > 0 and sma_21 > 0 else 0.0,
        "sma_21_over_63": (safe_ratio(sma_21, sma_63) - 1.0) if sma_21 > 0 and sma_63 > 0 else 0.0,
        "drawdown_21d": max_drawdown(closes, 21),
        "beta_to_spy_21d": beta_21,
        "corr_to_spy_21d": corr_21,
        "relative_strength_vs_spy_21d": close_return(closes, 21) - close_return(market_closes, 21),
    }


def compute_macro_features(market_history_window: list[dict[str, Any]]) -> dict[str, float]:
    closes = [safe_float(row.get("close")) for row in market_history_window]
    sma_21 = moving_average(closes, 21)
    sma_63 = moving_average(closes, 63)
    return {
        "macro_spy_return_5d": close_return(closes, 5),
        "macro_spy_return_21d": close_return(closes, 21),
        "macro_spy_return_63d": close_return(closes, 63),
        "macro_spy_realized_vol_21d": realized_volatility(closes, 21),
        "macro_spy_realized_vol_63d": realized_volatility(closes, 63),
        "macro_spy_drawdown_21d": max_drawdown(closes, 21),
        "macro_spy_sma_21_over_63": (safe_ratio(sma_21, sma_63) - 1.0) if sma_21 > 0 and sma_63 > 0 else 0.0,
    }


def compute_context_features(
    ticker: str,
    as_of_date: str,
    fundamentals: dict[str, dict[str, Any]],
    sector_feature_map: dict[tuple[str, str], dict[str, float]],
) -> dict[str, Any]:
    item = fundamentals.get(ticker, {})
    market_cap = safe_float(item.get("market_cap"))
    average_volume = safe_float(item.get("average_volume"))
    free_cash_flow = safe_float(item.get("free_cash_flow"))
    sector = str(item.get("sector") or SECTOR_UNKNOWN)
    industry = str(item.get("industry") or SECTOR_UNKNOWN)
    context = {
        "country": str(item.get("country") or ""),
        "quote_type": str(item.get("quote_type") or ""),
        "sector": sector,
        "industry": industry,
        "market_cap_log": safe_log(market_cap),
        "average_volume_log": safe_log(average_volume),
        "net_margin": safe_float(item.get("net_margin")),
        "debt_to_equity": safe_float(item.get("debt_to_equity")),
        "revenue_growth": safe_float(item.get("revenue_growth")),
        "operating_margin": safe_float(item.get("operating_margin")),
        "return_on_equity": safe_float(item.get("return_on_equity")),
        "free_cash_flow_yield": safe_ratio(free_cash_flow, market_cap) if market_cap > 0 else 0.0,
    }
    context.update(sector_feature_map.get((ticker, as_of_date), {
        "sector_peer_return_21d_mean": 0.0,
        "sector_peer_vol_21d_mean": 0.0,
        "sector_relative_return_21d": 0.0,
        "sector_relative_vol_21d": 0.0,
        "sector_peer_count": 0.0,
    }))
    return context
