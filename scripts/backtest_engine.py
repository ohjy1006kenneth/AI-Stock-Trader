from __future__ import annotations

import csv
import math
from collections import defaultdict
from datetime import datetime

from common import BACKTESTS_DIR, OUTPUTS_DIR, TRADING_DAYS_PER_YEAR, mean, now_iso, read_json, realized_volatility, rsi, safe_float, sma, stddev, trailing_return, write_json, write_text

ASSUMPTIONS = {
    "benchmark": "SPY",
    "transaction_cost_bps": 10,
    "slippage_bps": 5,
    "starting_cash": 100000.0,
    "rebalance_frequency": "monthly",
    "max_core_positions": 5,
    "max_swing_positions": 5,
    "core_target_weight": 0.12,
    "swing_target_weight": 0.06,
    "swing_trailing_stop_pct": 0.10,
    "swing_take_profit_pct": 0.15,
    "swing_max_holding_days": 20,
    "signal_decay_threshold": 0.35,
    "quality_thresholds": {
        "net_margin_min": 0.12,
        "debt_to_equity_max": 40.0,
        "revenue_growth_min": 0.05,
        "market_cap_min": 5_000_000_000,
        "average_volume_min": 1_000_000,
        "free_cash_flow_positive": True,
    },
    "bias_controls": [
        "signals use trailing history only",
        "monthly entries use information available on the rebalance date",
        "daily exits use same-day close approximation for deterministic simplicity",
        "survivorship bias remains unsolved in the static seed universe",
        "fundamentals are not point-in-time clean in V1",
    ],
}


def parse_date(s: str) -> datetime:
    return datetime.strptime(s, "%Y-%m-%d")


def percentile_ranks(pairs: list[tuple[str, float]], reverse: bool) -> dict[str, float]:
    ordered = sorted(pairs, key=lambda x: x[1], reverse=reverse)
    total = len(ordered)
    if total == 0:
        return {}
    if total == 1:
        return {ordered[0][0]: 1.0}
    return {ticker: 1.0 - ((idx - 1) / (total - 1)) for idx, (ticker, _) in enumerate(ordered, start=1)}


def cost_multiplier() -> float:
    return 1.0 + ((ASSUMPTIONS["transaction_cost_bps"] + ASSUMPTIONS["slippage_bps"]) / 10000.0)


def sell_multiplier() -> float:
    return 1.0 - ((ASSUMPTIONS["transaction_cost_bps"] + ASSUMPTIONS["slippage_bps"]) / 10000.0)


def build_price_maps() -> tuple[dict[str, dict[str, dict]], list[str]]:
    snapshot = read_json(OUTPUTS_DIR / "price_snapshot.json", {"items": []})
    prices_by_ticker: dict[str, dict[str, dict]] = {}
    all_dates: set[str] = set()
    for item in snapshot.get("items", []):
        rows = item.get("history", [])
        date_map = {row["date"]: row for row in rows}
        prices_by_ticker[item["ticker"]] = date_map
        all_dates.update(date_map.keys())
    return prices_by_ticker, sorted(all_dates)


def build_benchmark_series(dates: list[str]) -> dict[str, float]:
    import yfinance as yf

    hist = yf.Ticker(ASSUMPTIONS["benchmark"]).history(period="18mo", interval="1d", auto_adjust=False)
    benchmark = {}
    if hist is not None and not hist.empty:
        for idx, row in hist.iterrows():
            benchmark[str(idx.date())] = float(row.get("Close", 0.0))
    return {d: benchmark.get(d) for d in dates}


def build_fundamental_map() -> dict[str, dict]:
    fundamentals = read_json(OUTPUTS_DIR / "fundamental_snapshot.json", {"items": []})
    return {item["ticker"]: item for item in fundamentals.get("items", [])}


def is_quality_pass(item: dict) -> bool:
    if not item:
        return False
    return (
        item.get("country") == "United States"
        and str(item.get("quote_type", "")).upper() in {"EQUITY", "COMMON STOCK"}
        and (safe_float(item.get("net_margin")) or -1) > ASSUMPTIONS["quality_thresholds"]["net_margin_min"]
        and (safe_float(item.get("debt_to_equity")) or 999) < ASSUMPTIONS["quality_thresholds"]["debt_to_equity_max"]
        and (safe_float(item.get("revenue_growth")) or -1) > ASSUMPTIONS["quality_thresholds"]["revenue_growth_min"]
        and (safe_float(item.get("market_cap")) or 0) >= ASSUMPTIONS["quality_thresholds"]["market_cap_min"]
        and (safe_float(item.get("average_volume")) or 0) >= ASSUMPTIONS["quality_thresholds"]["average_volume_min"]
        and (safe_float(item.get("free_cash_flow")) or -1) > 0
    )


def history_until(prices_by_ticker: dict[str, dict[str, dict]], ticker: str, current_date: str) -> list[dict]:
    rows = [row for d, row in prices_by_ticker.get(ticker, {}).items() if d <= current_date]
    rows.sort(key=lambda x: x["date"])
    return rows


def compute_signal_set(prices_by_ticker: dict[str, dict[str, dict]], fundamentals_by_ticker: dict[str, dict], current_date: str) -> list[dict]:
    metrics = []
    for ticker in prices_by_ticker.keys():
        rows = history_until(prices_by_ticker, ticker, current_date)
        if len(rows) < 253:
            continue
        adj = [float(r["adj_close"]) for r in rows if r.get("adj_close") is not None]
        close = [float(r["close"]) for r in rows if r.get("close") is not None]
        last_close = close[-1] if close else None
        sma200 = sma(close, 200)
        metrics.append({
            "ticker": ticker,
            "current_close": last_close,
            "momentum_12_1": trailing_return(adj, 252, 21),
            "realized_vol_30d": realized_volatility(adj, 30),
            "rsi_14": rsi(close, 14),
            "trend_filter_pass": bool(last_close is not None and sma200 is not None and last_close > sma200),
            "quality_pass": is_quality_pass(fundamentals_by_ticker.get(ticker, {})),
        })
    mom_ranks = percentile_ranks([(m["ticker"], m["momentum_12_1"]) for m in metrics if m["momentum_12_1"] is not None], True)
    vol_ranks = percentile_ranks([(m["ticker"], m["realized_vol_30d"]) for m in metrics if m["realized_vol_30d"] is not None], False)
    out = []
    for m in metrics:
        mr = mom_ranks.get(m["ticker"])
        vr = vol_ranks.get(m["ticker"])
        if mr is None or vr is None:
            alpha = None
        else:
            alpha = (0.5 * mr) + (0.2 * vr) + (0.15 * (1.0 if m["trend_filter_pass"] else 0.0)) + (0.15 * (1.0 if m["quality_pass"] else 0.0))
        out.append({**m, "alpha_score": alpha})
    out.sort(key=lambda x: (-999 if x["alpha_score"] is None else -x["alpha_score"], x["ticker"]))
    return out


def is_rebalance_day(dates: list[str], idx: int) -> bool:
    if idx == 0:
        return True
    cur = parse_date(dates[idx])
    prev = parse_date(dates[idx - 1])
    return cur.month != prev.month


def mark_positions(positions: list[dict], prices_by_ticker: dict[str, dict[str, dict]], current_date: str) -> tuple[float, float]:
    total_mv = 0.0
    total_unrealized = 0.0
    for pos in positions:
        row = prices_by_ticker.get(pos["ticker"], {}).get(current_date)
        if not row:
            continue
        close = float(row["close"])
        pos["last_price"] = close
        pos["market_value"] = round(close * pos["shares"], 2)
        pos["unrealized_pnl"] = round((close - pos["avg_cost"]) * pos["shares"], 2)
        pos["peak_price"] = max(float(pos.get("peak_price", close)), close)
        pos["holding_days"] = int(pos.get("holding_days", 0)) + 1
        total_mv += pos["market_value"]
        total_unrealized += pos["unrealized_pnl"]
    return round(total_mv, 2), round(total_unrealized, 2)


def sell_position(position: dict, price: float, current_date: str, reason: str, cash: float, trade_rows: list[dict]) -> tuple[float, float]:
    proceeds = round(position["shares"] * price * sell_multiplier(), 2)
    realized = round((price * sell_multiplier() - position["avg_cost"]) * position["shares"], 2)
    trade_rows.append({
        "date": current_date,
        "ticker": position["ticker"],
        "action": "SELL",
        "sleeve": position["sleeve"],
        "shares": position["shares"],
        "price": round(price * sell_multiplier(), 4),
        "notional": proceeds,
        "reason_code": reason,
    })
    return round(cash + proceeds, 2), realized


def buy_position(ticker: str, sleeve: str, shares: int, raw_price: float, current_date: str, thesis: str, cash: float, trade_rows: list[dict]) -> tuple[float, dict]:
    buy_px = round(raw_price * cost_multiplier(), 4)
    notional = round(shares * buy_px, 2)
    trade_rows.append({
        "date": current_date,
        "ticker": ticker,
        "action": "BUY",
        "sleeve": sleeve,
        "shares": shares,
        "price": buy_px,
        "notional": notional,
        "reason_code": thesis,
    })
    position = {
        "ticker": ticker,
        "sleeve": sleeve,
        "shares": shares,
        "avg_cost": buy_px,
        "last_price": raw_price,
        "market_value": round(shares * raw_price, 2),
        "unrealized_pnl": round((raw_price - buy_px) * shares, 2),
        "entry_date": current_date,
        "thesis_summary": thesis,
        "stop_rule": {"type": "trailing_stop_pct", "value": ASSUMPTIONS["swing_trailing_stop_pct"]} if sleeve == "SWING" else None,
        "take_profit_rule": {"type": "take_profit_pct", "value": ASSUMPTIONS["swing_take_profit_pct"]} if sleeve == "SWING" else None,
        "max_holding_period_days": ASSUMPTIONS["swing_max_holding_days"] if sleeve == "SWING" else None,
        "holding_days": 0,
        "peak_price": raw_price,
    }
    return round(cash - notional, 2), position


def compute_metrics(equity_rows: list[dict], trade_rows: list[dict]) -> dict:
    if len(equity_rows) < 2:
        return {
            "cagr": None,
            "annualized_volatility": None,
            "sharpe_ratio": None,
            "max_drawdown": None,
            "win_rate": None,
            "number_of_trades": 0,
            "average_holding_period": None,
            "turnover": None,
            "benchmark_relative_comparison": None,
        }
    equities = [row["portfolio_equity"] for row in equity_rows]
    bench = [row["benchmark_equity"] for row in equity_rows if row.get("benchmark_equity") is not None]
    daily_returns = []
    for idx in range(1, len(equities)):
        if equities[idx - 1] > 0:
            daily_returns.append((equities[idx] / equities[idx - 1]) - 1.0)
    vol = stddev(daily_returns) if daily_returns else None
    ann_vol = (vol * math.sqrt(TRADING_DAYS_PER_YEAR)) if vol is not None else None
    avg_ret = mean(daily_returns) if daily_returns else None
    sharpe = ((avg_ret * TRADING_DAYS_PER_YEAR) / ann_vol) if (avg_ret is not None and ann_vol not in (None, 0)) else None
    years = len(equity_rows) / TRADING_DAYS_PER_YEAR
    cagr = ((equities[-1] / equities[0]) ** (1 / years) - 1.0) if years > 0 and equities[0] > 0 else None
    peak = equities[0]
    max_dd = 0.0
    for eq in equities:
        peak = max(peak, eq)
        dd = (eq / peak) - 1.0 if peak > 0 else 0.0
        max_dd = min(max_dd, dd)
    sell_rows = [r for r in trade_rows if r["action"] == "SELL"]
    buy_rows = [r for r in trade_rows if r["action"] == "BUY"]
    paired = min(len(buy_rows), len(sell_rows))
    win_rate = None
    avg_holding = None
    if paired > 0:
        # approximate using sell notional > buy notional per ticker sequence is too noisy; leave honest simple estimate unavailable without pair ledger
        win_rate = None
    turnover = (sum(r["notional"] for r in trade_rows) / mean(equities)) if mean(equities) else None
    benchmark_relative = None
    if len(bench) == len(equities) and bench[0] and bench[-1]:
        strategy_total = (equities[-1] / equities[0]) - 1.0
        benchmark_total = (bench[-1] / bench[0]) - 1.0
        benchmark_relative = strategy_total - benchmark_total
    return {
        "cagr": cagr,
        "annualized_volatility": ann_vol,
        "sharpe_ratio": sharpe,
        "max_drawdown": max_dd,
        "win_rate": win_rate,
        "number_of_trades": len(trade_rows),
        "average_holding_period": avg_holding,
        "turnover": turnover,
        "benchmark_relative_comparison": benchmark_relative,
    }


def yearly_breakdown(equity_rows: list[dict]) -> list[dict]:
    by_year: dict[str, list[dict]] = defaultdict(list)
    for row in equity_rows:
        by_year[row["date"][:4]].append(row)
    out = []
    for year, rows in sorted(by_year.items()):
        start = rows[0]["portfolio_equity"]
        end = rows[-1]["portfolio_equity"]
        bench_start = rows[0].get("benchmark_equity")
        bench_end = rows[-1].get("benchmark_equity")
        out.append({
            "year": year,
            "portfolio_return": ((end / start) - 1.0) if start else None,
            "benchmark_return": ((bench_end / bench_start) - 1.0) if bench_start and bench_end else None,
        })
    return out


def main() -> None:
    prices_by_ticker, dates = build_price_maps()
    if not dates:
        raise SystemExit("No price history available in outputs/price_snapshot.json")
    fundamentals_by_ticker = build_fundamental_map()
    benchmark_map = build_benchmark_series(dates)

    start_idx = 252
    cash = ASSUMPTIONS["starting_cash"]
    realized_pnl = 0.0
    positions: list[dict] = []
    trade_rows: list[dict] = []
    equity_rows: list[dict] = []
    benchmark_start = None

    for idx in range(start_idx, len(dates)):
        current_date = dates[idx]

        # Daily marks and exit handling first
        total_mv, unrealized = mark_positions(positions, prices_by_ticker, current_date)
        signal_map = {item["ticker"]: item for item in compute_signal_set(prices_by_ticker, fundamentals_by_ticker, current_date)}

        kept_positions = []
        for pos in positions:
            current_price = pos.get("last_price")
            if current_price is None:
                kept_positions.append(pos)
                continue
            exit_reason = None
            if pos["sleeve"] == "SWING":
                peak_price = float(pos.get("peak_price", current_price))
                if current_price <= peak_price * (1.0 - ASSUMPTIONS["swing_trailing_stop_pct"]):
                    exit_reason = "trailing_stop_hit"
                elif current_price >= float(pos["avg_cost"]) * (1.0 + ASSUMPTIONS["swing_take_profit_pct"]):
                    exit_reason = "take_profit_hit"
                elif int(pos.get("holding_days", 0)) >= ASSUMPTIONS["swing_max_holding_days"]:
                    exit_reason = "max_holding_period"
                else:
                    sig = signal_map.get(pos["ticker"])
                    if sig and sig.get("alpha_score") is not None and sig["alpha_score"] < ASSUMPTIONS["signal_decay_threshold"]:
                        exit_reason = "signal_decay"
            elif pos["sleeve"] == "CORE":
                sig = signal_map.get(pos["ticker"])
                if sig and not sig.get("quality_pass", True):
                    exit_reason = "quality_decay_placeholder"
            if exit_reason:
                cash, realized = sell_position(pos, current_price, current_date, exit_reason, cash, trade_rows)
                realized_pnl += realized
            else:
                kept_positions.append(pos)
        positions = kept_positions

        # Rebalance monthly for entries
        if is_rebalance_day(dates, idx):
            signals = list(signal_map.values())
            current_equity = cash + sum(float(p.get("market_value", 0.0)) for p in positions)
            core_count = sum(1 for p in positions if p["sleeve"] == "CORE")
            swing_count = sum(1 for p in positions if p["sleeve"] == "SWING")
            held = {p["ticker"] for p in positions}

            for sig in signals:
                ticker = sig["ticker"]
                if ticker in held:
                    continue
                if sig.get("current_close") is None or sig.get("alpha_score") is None:
                    continue
                if sig.get("quality_pass") and sig["alpha_score"] >= 0.65 and sig.get("trend_filter_pass") and core_count < ASSUMPTIONS["max_core_positions"]:
                    target_notional = current_equity * ASSUMPTIONS["core_target_weight"]
                    shares = int(target_notional // float(sig["current_close"]))
                    if shares > 0 and cash >= shares * float(sig["current_close"]) * cost_multiplier():
                        cash, pos = buy_position(ticker, "CORE", shares, float(sig["current_close"]), current_date, "quality_plus_alpha", cash, trade_rows)
                        positions.append(pos)
                        core_count += 1
                        held.add(ticker)
                elif sig["alpha_score"] >= 0.80 and sig.get("trend_filter_pass") and swing_count < ASSUMPTIONS["max_swing_positions"]:
                    target_notional = current_equity * ASSUMPTIONS["swing_target_weight"]
                    shares = int(target_notional // float(sig["current_close"]))
                    if shares > 0 and cash >= shares * float(sig["current_close"]) * cost_multiplier():
                        cash, pos = buy_position(ticker, "SWING", shares, float(sig["current_close"]), current_date, "strong_alpha_tactical", cash, trade_rows)
                        positions.append(pos)
                        swing_count += 1
                        held.add(ticker)

        total_mv, unrealized = mark_positions(positions, prices_by_ticker, current_date)
        portfolio_equity = round(cash + total_mv, 2)
        benchmark_price = benchmark_map.get(current_date)
        if benchmark_price is not None and benchmark_start is None:
            benchmark_start = benchmark_price
        benchmark_equity = round(ASSUMPTIONS["starting_cash"] * (benchmark_price / benchmark_start), 2) if benchmark_price and benchmark_start else None
        equity_rows.append({
            "date": current_date,
            "portfolio_equity": portfolio_equity,
            "benchmark_equity": benchmark_equity,
            "cash": round(cash, 2),
            "positions": len(positions),
            "realized_pnl": round(realized_pnl, 2),
            "unrealized_pnl": round(unrealized, 2),
        })

    metrics_payload = {
        "strategy_version": "v1",
        "benchmark": ASSUMPTIONS["benchmark"],
        "generated_at": now_iso(),
        "metrics": compute_metrics(equity_rows, trade_rows),
        "yearly_breakdown": yearly_breakdown(equity_rows),
        "assumptions": ASSUMPTIONS,
        "limitations": [
            "survivorship bias is not solved",
            "yfinance remains fallback data",
            "fundamentals are not point-in-time clean",
            "same-day close execution is a simplifying assumption",
            "win-rate and pair-level analytics are incomplete in V1",
        ],
        "status": "deterministic_v1_backtest",
    }

    with (BACKTESTS_DIR / "trade_log.csv").open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["date", "ticker", "action", "sleeve", "shares", "price", "notional", "reason_code"])
        writer.writeheader()
        writer.writerows(trade_rows)

    with (BACKTESTS_DIR / "equity_curve.csv").open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["date", "portfolio_equity", "benchmark_equity", "cash", "positions", "realized_pnl", "unrealized_pnl"])
        writer.writeheader()
        writer.writerows(equity_rows)

    write_json(BACKTESTS_DIR / "metrics.json", metrics_payload)

    m = metrics_payload["metrics"]
    verdict = "weak_or_inconclusive"
    if (m.get("cagr") is not None and m.get("benchmark_relative_comparison") is not None and m.get("max_drawdown") is not None and m["cagr"] > 0 and m["benchmark_relative_comparison"] > 0 and m["max_drawdown"] > -0.30):
        verdict = "cautiously_promising_but_untrusted"

    write_text(
        BACKTESTS_DIR / "backtest_report.md",
        "# Backtest Report\n\n"
        f"Generated at: {metrics_payload['generated_at']}\n\n"
        "## Simulation Rules\n"
        f"- Rebalance frequency: {ASSUMPTIONS['rebalance_frequency']}\n"
        f"- CORE entries: quality pass + alpha >= 0.65 + trend filter\n"
        f"- SWING entries: alpha >= 0.80 + trend filter\n"
        f"- SWING exits: 10% trailing stop, 15% take-profit, 20-day max hold, signal decay < {ASSUMPTIONS['signal_decay_threshold']}\n"
        "- CORE exits: quality decay placeholder based on current fundamental screen\n"
        "- Daily marking with same-day close approximation\n\n"
        "## Assumptions\n"
        f"- Transaction cost: {ASSUMPTIONS['transaction_cost_bps']} bps per side\n"
        f"- Slippage: {ASSUMPTIONS['slippage_bps']} bps per side\n"
        f"- Starting cash: {ASSUMPTIONS['starting_cash']}\n"
        f"- Benchmark: {ASSUMPTIONS['benchmark']}\n\n"
        "## Results Snapshot\n"
        f"- CAGR: {m.get('cagr')}\n"
        f"- Annualized volatility: {m.get('annualized_volatility')}\n"
        f"- Sharpe ratio: {m.get('sharpe_ratio')}\n"
        f"- Max drawdown: {m.get('max_drawdown')}\n"
        f"- Number of trades: {m.get('number_of_trades')}\n"
        f"- Turnover: {m.get('turnover')}\n"
        f"- Benchmark-relative comparison: {m.get('benchmark_relative_comparison')}\n\n"
        "## Limitations\n"
        "- Survivorship bias is still not solved.\n"
        "- yfinance is still fallback data.\n"
        "- Fundamentals are not point-in-time clean.\n"
        "- Same-day close execution is unrealistic for live implementation.\n"
        "- This is a research backtest, not evidence of tradable profitability.\n\n"
        "## Bottom Line\n"
        f"- V1 verdict: {verdict}\n"
        "- Treat any positive result as provisional until bias controls and data quality improve.\n"
    )
    print(f"Backtest complete: {len(trade_rows)} trades, {len(equity_rows)} equity points")


if __name__ == "__main__":
    main()
