from __future__ import annotations

from common import LEDGER_DIR, OUTPUTS_DIR, now_iso, read_json, write_json


def position_market_value(qty: int, price: float) -> float:
    return round(qty * price, 2)


def main() -> None:
    portfolio = read_json(LEDGER_DIR / "mock_portfolio.json", {})
    decisions = read_json(OUTPUTS_DIR / "strategist_decisions.json", {"decisions": []}).get("decisions", [])
    cash = float(portfolio.get("cash", 0.0))
    open_positions = portfolio.get("open_positions", [])
    total_equity = float(portfolio.get("total_equity", cash))
    trade_history = portfolio.get("trade_history", [])

    existing = {pos["ticker"]: pos for pos in open_positions}

    for decision in decisions:
        if decision.get("action") != "BUY":
            continue
        ticker = decision["ticker"]
        if ticker in existing:
            continue
        target_weight = float(decision.get("target_weight", 0.0))
        entry_price = float(decision.get("entry_price", 0.0))
        if entry_price <= 0:
            continue
        target_notional = total_equity * target_weight
        qty = int(target_notional // entry_price)
        if qty <= 0:
            continue
        notional = round(qty * entry_price, 2)
        if notional > cash:
            continue
        cash = round(cash - notional, 2)
        position = {
            "ticker": ticker,
            "sleeve": decision.get("sleeve"),
            "qty": qty,
            "avg_cost": entry_price,
            "last_price": entry_price,
            "market_value": position_market_value(qty, entry_price),
            "unrealized_pnl": 0.0,
            "opened_at": now_iso(),
            "holding_days": 0,
            "risk_rules": decision.get("risk_rules", {}),
            "entry_reason": decision.get("reason")
        }
        open_positions.append(position)
        trade_history.append({
            "timestamp": now_iso(),
            "ticker": ticker,
            "side": "BUY",
            "sleeve": decision.get("sleeve"),
            "qty": qty,
            "price": entry_price,
            "notional": notional,
            "reason": decision.get("reason")
        })
        existing[ticker] = position

    marked_equity = cash
    unrealized_pnl = 0.0
    for position in open_positions:
        marked_equity += float(position.get("market_value", 0.0))
        unrealized_pnl += float(position.get("unrealized_pnl", 0.0))

    portfolio["cash"] = cash
    portfolio["open_positions"] = open_positions
    portfolio["trade_history"] = trade_history
    portfolio["total_equity"] = round(marked_equity, 2)
    portfolio["unrealized_pnl"] = round(unrealized_pnl, 2)
    portfolio["last_updated"] = now_iso()

    write_json(LEDGER_DIR / "mock_portfolio.json", portfolio)
    print(f"Applied decisions. Open positions: {len(open_positions)}")


if __name__ == "__main__":
    main()
