from __future__ import annotations

from common import DATA_DIR, LEDGER_DIR, OUTPUTS_DIR, now_iso, pct_change, read_json, write_json


MEANINGFUL_MOVE_PCT = 0.05


def main() -> None:
    portfolio = read_json(LEDGER_DIR / "mock_portfolio.json", {"open_positions": []})
    price_history = read_json(DATA_DIR / "price_history.json", {"items": []})
    rankings = read_json(OUTPUTS_DIR / "alpha_rankings.json", {"items": []})
    prices_by_ticker = {item["ticker"]: item.get("rows", []) for item in price_history.get("items", [])}
    rankings_by_ticker = {item["ticker"]: item for item in rankings.get("items", [])}

    events = []
    for position in portfolio.get("open_positions", []):
        ticker = position["ticker"]
        rows = prices_by_ticker.get(ticker, [])
        if len(rows) < 2:
            continue
        latest_close = float(rows[-1]["close"])
        prev_close = float(rows[-2]["close"])
        move = pct_change(latest_close, prev_close)
        entry_price = float(position.get("avg_cost", 0.0))
        if entry_price <= 0:
            continue
        if position.get("sleeve") == "SWING":
            trailing_stop = float(position.get("risk_rules", {}).get("trailing_stop_pct", 0.10))
            if latest_close <= entry_price * (1.0 - trailing_stop):
                events.append({"ticker": ticker, "event_type": "trailing_stop_hit", "price": latest_close})
            take_profit = position.get("risk_rules", {}).get("take_profit_pct")
            if take_profit is not None and latest_close >= entry_price * (1.0 + float(take_profit)):
                events.append({"ticker": ticker, "event_type": "take_profit_hit", "price": latest_close})
        if move is not None and abs(move) >= MEANINGFUL_MOVE_PCT:
            events.append({"ticker": ticker, "event_type": "meaningful_price_move", "price": latest_close, "move_pct": round(move, 4)})
        ranking = rankings_by_ticker.get(ticker)
        if ranking and ranking.get("alpha_score") is not None and ranking["alpha_score"] < 0.35:
            events.append({"ticker": ticker, "event_type": "signal_decay", "alpha_score": ranking["alpha_score"]})

    payload = {
        "generated_at": now_iso(),
        "events": events,
        "rules": {
            "escalate_on": [
                "trailing_stop_hit",
                "take_profit_hit",
                "meaningful_price_move",
                "signal_decay",
                "scheduled_review_due",
                "data_integrity_issue",
                "new_factor_model_version_available"
            ]
        }
    }
    write_json(OUTPUTS_DIR / "sentry_events.json", payload)
    print(f"Sentry produced {len(events)} events")


if __name__ == "__main__":
    main()
