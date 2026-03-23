from __future__ import annotations

from common import DATA_DIR, LEDGER_DIR, now_iso, read_json, write_json


def main() -> None:
    portfolio = read_json(LEDGER_DIR / "mock_portfolio.json", {})
    price_history = read_json(DATA_DIR / "price_history.json", {"items": []})
    prices_by_ticker = {item["ticker"]: item.get("rows", []) for item in price_history.get("items", [])}

    cash = float(portfolio.get("cash", 0.0))
    total_equity = cash
    unrealized_pnl = 0.0
    for position in portfolio.get("open_positions", []):
        rows = prices_by_ticker.get(position["ticker"], [])
        if not rows:
            continue
        last_price = float(rows[-1]["close"])
        qty = int(position.get("qty", 0))
        market_value = round(qty * last_price, 2)
        pnl = round((last_price - float(position.get("avg_cost", 0.0))) * qty, 2)
        position["last_price"] = last_price
        position["market_value"] = market_value
        position["unrealized_pnl"] = pnl
        total_equity += market_value
        unrealized_pnl += pnl

    portfolio["total_equity"] = round(total_equity, 2)
    portfolio["unrealized_pnl"] = round(unrealized_pnl, 2)
    portfolio["last_updated"] = now_iso()
    portfolio.setdefault("daily_marks", []).append({
        "timestamp": now_iso(),
        "total_equity": portfolio["total_equity"],
        "cash": cash,
        "unrealized_pnl": portfolio["unrealized_pnl"]
    })
    write_json(LEDGER_DIR / "mock_portfolio.json", portfolio)
    print("Portfolio marked to market")


if __name__ == "__main__":
    main()
