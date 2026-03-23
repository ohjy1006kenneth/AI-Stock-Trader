from __future__ import annotations

from common import DATA_DIR, LEDGER_DIR, OUTPUTS_DIR, now_iso, read_json, write_json


CORE_WEIGHT = 0.12
SWING_WEIGHT = 0.06
SWING_MAX_HOLD_DAYS = 20
SWING_TRAILING_STOP = 0.10
SWING_TAKE_PROFIT = 0.15


def earnings_blocked(fundamental_item: dict) -> bool:
    return fundamental_item.get("earnings_timestamp") is not None


def current_sleeve_weights(portfolio: dict) -> tuple[float, float]:
    total_equity = float(portfolio.get("total_equity", 0.0) or 0.0)
    if total_equity <= 0:
        return 0.0, 0.0
    core_value = 0.0
    swing_value = 0.0
    for pos in portfolio.get("open_positions", []):
        mv = float(pos.get("market_value", 0.0) or 0.0)
        if pos.get("sleeve") == "CORE":
            core_value += mv
        elif pos.get("sleeve") == "SWING":
            swing_value += mv
    return core_value / total_equity, swing_value / total_equity


def main() -> None:
    alpha = read_json(OUTPUTS_DIR / "alpha_rankings.json", {"items": []})
    qualified = read_json(OUTPUTS_DIR / "qualified_universe.json", {"items": []})
    price_history = read_json(DATA_DIR / "price_history.json", {"items": []})
    fundamentals = read_json(DATA_DIR / "fundamental_data.json", {"items": []})
    portfolio = read_json(LEDGER_DIR / "mock_portfolio.json", {})

    qualified_by_ticker = {item["ticker"]: item for item in qualified.get("items", [])}
    prices_by_ticker = {item["ticker"]: item for item in price_history.get("items", [])}
    fundamentals_by_ticker = {item["ticker"]: item for item in fundamentals.get("items", [])}
    open_tickers = {pos["ticker"] for pos in portfolio.get("open_positions", [])}
    core_weight, swing_weight = current_sleeve_weights(portfolio)
    risk_limits = portfolio.get("risk_limits", {})

    decisions = []
    for item in alpha.get("items", []):
        ticker = item["ticker"]
        if ticker in open_tickers:
            continue
        latest_rows = prices_by_ticker.get(ticker, {}).get("rows", [])
        if not latest_rows:
            continue
        price = latest_rows[-1]["close"]
        alpha_score = item.get("alpha_score")
        factors = item.get("factors", {})
        rsi_value = factors.get("rsi_14")
        trend_ok = factors.get("trend_filter_pass", False)
        quality_item = qualified_by_ticker.get(ticker)
        fundamental_item = fundamentals_by_ticker.get(ticker, {})

        if quality_item and alpha_score is not None and alpha_score >= 0.65 and trend_ok and core_weight < risk_limits.get("max_core_allocation", 0.7):
            decisions.append({
                "ticker": ticker,
                "action": "BUY",
                "sleeve": "CORE",
                "target_weight": CORE_WEIGHT,
                "entry_price": price,
                "reason": "passed quality screen and met approved alpha support",
                "risk_rules": {
                    "sell_on_fundamental_decay": True,
                    "trend_review_only": True
                }
            })
            core_weight += CORE_WEIGHT
            continue

        if alpha_score is not None and alpha_score >= 0.75 and trend_ok and swing_weight < risk_limits.get("max_swing_allocation", 0.3) and not earnings_blocked(fundamental_item):
            decisions.append({
                "ticker": ticker,
                "action": "BUY",
                "sleeve": "SWING",
                "target_weight": SWING_WEIGHT,
                "entry_price": price,
                "reason": "strong approved alpha support with tactical trend confirmation",
                "risk_rules": {
                    "trailing_stop_pct": SWING_TRAILING_STOP,
                    "take_profit_pct": SWING_TAKE_PROFIT,
                    "max_holding_days": SWING_MAX_HOLD_DAYS
                },
                "context": {
                    "rsi_14": rsi_value
                }
            })
            swing_weight += SWING_WEIGHT

    payload = {
        "generated_at": now_iso(),
        "portfolio_id": portfolio.get("portfolio_id"),
        "decisions": decisions,
        "notes": {
            "core_logic": "quality + alpha + risk limits",
            "swing_logic": "alpha + trend + liquidity/event sanity",
            "status": "deterministic_v1"
        }
    }
    write_json(OUTPUTS_DIR / "strategist_decisions.json", payload)
    print(f"Strategist produced {len(decisions)} candidate decisions")


if __name__ == "__main__":
    main()
