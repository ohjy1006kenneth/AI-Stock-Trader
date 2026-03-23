from __future__ import annotations

from common import CONTEXT_DIR, DATA_DIR, fetch_history, load_json, utc_now_iso, write_markdown


def market_price(ticker: str, fallback: float) -> float:
    try:
        hist = fetch_history(ticker, period="5d")
        return float(hist["Close"].iloc[-1])
    except Exception:
        return fallback


def main() -> None:
    portfolio = load_json(DATA_DIR / "portfolio.json", {"cash": 0.0, "positions": {}})
    positions = portfolio.get("positions", {})
    cash = float(portfolio.get("cash", 0.0))
    total_value = cash
    unrealized = 0.0
    lines = ["# Daily Portfolio Summary", "", f"Generated at: {utc_now_iso()}", "", f"- Cash: ${cash:,.2f}"]
    for ticker, pos in positions.items():
        last = market_price(ticker, float(pos["avg_cost"]))
        market_value = last * int(pos["qty"])
        pnl = (last - float(pos["avg_cost"])) * int(pos["qty"])
        total_value += market_value
        unrealized += pnl
        lines.extend([
            f"## {ticker}",
            f"- Qty: {pos['qty']}",
            f"- Avg cost: ${float(pos['avg_cost']):,.2f}",
            f"- Last price: ${last:,.2f}",
            f"- Market value: ${market_value:,.2f}",
            f"- Unrealized PnL: ${pnl:,.2f}",
            f"- Holding type: {pos['holding_type']}",
            "",
        ])
    lines.insert(4, f"- Current Portfolio Value: ${total_value:,.2f}")
    lines.insert(5, f"- Unrealized PnL: ${unrealized:,.2f}")
    write_markdown(CONTEXT_DIR / "daily_summary.md", "\n".join(lines) + "\n")
    print("\n".join(lines))


if __name__ == "__main__":
    main()
