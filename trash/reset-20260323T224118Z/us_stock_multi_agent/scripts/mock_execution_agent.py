from __future__ import annotations

from common import CONTEXT_DIR, DATA_DIR, load_json, save_json, utc_now_iso, write_markdown


def main() -> None:
    portfolio = load_json(DATA_DIR / "portfolio.json", {"cash": 100000.0, "positions": {}})
    decisions = load_json(DATA_DIR / "risk_decisions.json", {"items": []}).get("items", [])
    trade_log = load_json(DATA_DIR / "trade_log.json", [])
    report_lines = ["# Execution Report", "", f"Generated at: {utc_now_iso()}", ""]

    for order in decisions:
        if order["action"] != "BUY":
            continue
        cash = float(portfolio.get("cash", 0.0))
        allocation = cash * float(order["size_pct"])
        price = float(order["entry_price"])
        qty = int(allocation // price)
        if qty <= 0:
            report_lines.append(f"- Skipped {order['ticker']}: insufficient cash for position sizing")
            continue
        cost = round(qty * price, 2)
        portfolio["cash"] = round(cash - cost, 2)
        portfolio.setdefault("positions", {})[order["ticker"]] = {
            "ticker": order["ticker"],
            "qty": qty,
            "avg_cost": price,
            "holding_type": order["holding_type"],
            "trailing_stop_pct": order["trailing_stop_pct"],
            "opened_at": utc_now_iso(),
            "reasoning": order["reasoning"],
        }
        trade = {
            "timestamp": utc_now_iso(),
            "ticker": order["ticker"],
            "side": "BUY",
            "price": price,
            "qty": qty,
            "notional": cost,
            "reasoning": order["reasoning"],
            "mode": "paper",
        }
        trade_log.append(trade)
        report_lines.append(f"- BUY {order['ticker']} x{qty} @ {price} | {order['holding_type']} | {order['reasoning']}")

    portfolio["last_updated"] = utc_now_iso()
    save_json(DATA_DIR / "portfolio.json", portfolio)
    save_json(DATA_DIR / "trade_log.json", trade_log)
    if len(report_lines) == 4:
        report_lines.append("No mock trades executed.")
    write_markdown(CONTEXT_DIR / "execution_report.md", "\n".join(report_lines) + "\n")


if __name__ == "__main__":
    main()
