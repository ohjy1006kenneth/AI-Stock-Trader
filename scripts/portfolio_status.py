from __future__ import annotations

import argparse
import json
from typing import Any

from common import LEDGER_DIR, OUTPUTS_DIR, read_json


def latest_prices() -> tuple[dict[str, float], bool]:
    snapshot = read_json(OUTPUTS_DIR / "price_snapshot.json", {"items": []})
    prices = {}
    for item in snapshot.get("items", []):
        ticker = item.get("ticker")
        close = item.get("close")
        if ticker and close is not None:
            prices[ticker] = float(close)
    return prices, len(prices) > 0


def build_status() -> dict[str, Any]:
    portfolio = read_json(LEDGER_DIR / "mock_portfolio.json", {})
    prices, has_snapshot_prices = latest_prices()

    starting_cash = float(portfolio.get("starting_cash", 0.0) or 0.0)
    current_cash = float(portfolio.get("cash", 0.0) or 0.0)
    realized_pnl = float(portfolio.get("realized_pnl", 0.0) or 0.0)
    positions = portfolio.get("positions", [])

    position_rows = []
    unrealized_total = 0.0
    market_value_total = 0.0
    using_fallback_for_any = False

    for pos in positions:
        ticker = pos.get("ticker")
        shares = int(pos.get("shares", 0) or 0)
        avg_cost = float(pos.get("avg_cost", 0.0) or 0.0)
        stored_last = float(pos.get("last_price", 0.0) or 0.0)
        last_price = float(prices.get(ticker, stored_last))
        used_fallback = ticker not in prices
        using_fallback_for_any = using_fallback_for_any or used_fallback
        market_value = round(shares * last_price, 2)
        unrealized_pnl = round((last_price - avg_cost) * shares, 2)
        unrealized_return_pct = round(((last_price / avg_cost) - 1.0) * 100.0, 2) if avg_cost > 0 else None
        unrealized_total += unrealized_pnl
        market_value_total += market_value
        position_rows.append({
            "ticker": ticker,
            "sleeve": pos.get("sleeve"),
            "shares": shares,
            "average_cost": avg_cost,
            "last_price": last_price,
            "market_value": market_value,
            "unrealized_pnl": unrealized_pnl,
            "unrealized_return_pct": unrealized_return_pct,
            "entry_date": pos.get("entry_date"),
            "price_source": "snapshot_close" if not used_fallback else "stored_last_price",
        })

    total_equity = round(current_cash + market_value_total, 2)
    total_pnl = round(realized_pnl + unrealized_total, 2)
    total_return_pct = round((total_pnl / starting_cash) * 100.0, 2) if starting_cash > 0 else None

    return {
        "account_summary": {
            "starting_cash": starting_cash,
            "current_cash": current_cash,
            "total_equity": total_equity,
            "realized_pnl": round(realized_pnl, 2),
            "unrealized_pnl": round(unrealized_total, 2),
            "total_pnl": total_pnl,
            "total_return_pct": total_return_pct,
            "pricing_mode": "latest_price_snapshot" if has_snapshot_prices and not using_fallback_for_any else ("mixed_fallback" if has_snapshot_prices else "stored_last_price_fallback"),
            "portfolio_id": portfolio.get("portfolio_id"),
            "last_updated": portfolio.get("last_updated"),
        },
        "open_positions": position_rows,
    }


def print_summary(summary: dict[str, Any]) -> None:
    print("ACCOUNT SUMMARY")
    print(f"- starting cash: ${summary['starting_cash']:,.2f}")
    print(f"- current cash: ${summary['current_cash']:,.2f}")
    print(f"- total equity: ${summary['total_equity']:,.2f}")
    print(f"- realized PnL: ${summary['realized_pnl']:,.2f}")
    print(f"- unrealized PnL: ${summary['unrealized_pnl']:,.2f}")
    print(f"- total PnL: ${summary['total_pnl']:,.2f}")
    ret = summary['total_return_pct']
    print(f"- total return %: {ret:.2f}%" if ret is not None else "- total return %: n/a")
    mode = summary.get("pricing_mode")
    if mode == "stored_last_price_fallback":
        print("- pricing note: using stored last_price fallback for all positions")
    elif mode == "mixed_fallback":
        print("- pricing note: using mixed pricing (snapshot where available, stored last_price fallback otherwise)")


def print_positions(positions: list[dict[str, Any]]) -> None:
    print("OPEN POSITIONS")
    if not positions:
        print("- No open positions")
        return
    for pos in positions:
        print(f"- {pos['ticker']} | {pos['sleeve']} | shares={pos['shares']} | avg_cost=${pos['average_cost']:,.2f} | last_price=${pos['last_price']:,.2f} | market_value=${pos['market_value']:,.2f} | unrealized_pnl=${pos['unrealized_pnl']:,.2f} | unrealized_return={pos['unrealized_return_pct']:.2f}% | entry_date={pos['entry_date']} | price_source={pos['price_source']}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Read-only mock portfolio status")
    parser.add_argument("--json", action="store_true", dest="as_json", help="print machine-readable JSON")
    parser.add_argument("--summary", action="store_true", help="print only account summary")
    parser.add_argument("--positions", action="store_true", help="print only open positions")
    args = parser.parse_args()

    status = build_status()
    if args.as_json:
        print(json.dumps(status, indent=2))
        return
    if args.summary:
        print_summary(status["account_summary"])
        return
    if args.positions:
        print_positions(status["open_positions"])
        return
    print_summary(status["account_summary"])
    print()
    print_positions(status["open_positions"])


if __name__ == "__main__":
    main()
