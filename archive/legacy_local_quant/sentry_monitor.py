from __future__ import annotations

from pathlib import Path
import sys

ROOT_DIR = Path(__file__).resolve()
for _ in range(5):
    if (ROOT_DIR / ".gitignore").exists():
        break
    ROOT_DIR = ROOT_DIR.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from runtime.common.common import LEDGER_DIR, MARKET_DATA_DIR, STRATEGY_DATA_DIR, gen_id, now_iso, read_json, write_json

MEANINGFUL_MOVE_PCT = 0.05


def main() -> None:
    snapshot = read_json(MARKET_DATA_DIR / "price_snapshot.json", {"items": []})
    rankings = read_json(STRATEGY_DATA_DIR / "alpha_rankings.json", {"items": []})
    portfolio = read_json(LEDGER_DIR / "mock_portfolio.json", {"positions": []})

    px = {item["ticker"]: item for item in snapshot.get("items", [])}
    rank = {item["ticker"]: item for item in rankings.get("items", [])}

    events = []
    for pos in portfolio.get("positions", []):
        ticker = pos["ticker"]
        item = px.get(ticker)
        if not item:
            continue
        history = item.get("history", [])
        if len(history) >= 2:
            prev_close = history[-2]["close"]
            cur_close = history[-1]["close"]
            if prev_close and abs((cur_close / prev_close) - 1.0) >= MEANINGFUL_MOVE_PCT:
                events.append({"event_id": gen_id("evt"), "timestamp": now_iso(), "ticker": ticker, "event_type": "meaningful_price_move", "details": {"move_pct": round((cur_close / prev_close) - 1.0, 4)}})
        if pos.get("sleeve") == "SWING":
            stop = (pos.get("stop_rule") or {}).get("value")
            if stop is not None and item.get("close") is not None and item["close"] <= pos["avg_cost"] * (1.0 - float(stop)):
                events.append({"event_id": gen_id("evt"), "timestamp": now_iso(), "ticker": ticker, "event_type": "trailing_stop_hit", "details": {}})
            take = (pos.get("take_profit_rule") or {}).get("value")
            if take is not None and item.get("close") is not None and item["close"] >= pos["avg_cost"] * (1.0 + float(take)):
                events.append({"event_id": gen_id("evt"), "timestamp": now_iso(), "ticker": ticker, "event_type": "take_profit_hit", "details": {}})
            if pos.get("max_holding_period_days") is not None and int(pos.get("holding_days", 0)) >= int(pos["max_holding_period_days"]):
                events.append({"event_id": gen_id("evt"), "timestamp": now_iso(), "ticker": ticker, "event_type": "scheduled_review_due", "details": {}})
        alpha_item = rank.get(ticker)
        if alpha_item and alpha_item.get("alpha_score") is not None and alpha_item["alpha_score"] < 0.35:
            events.append({"event_id": gen_id("evt"), "timestamp": now_iso(), "ticker": ticker, "event_type": "signal_decay", "details": {"alpha_score": alpha_item["alpha_score"]}})

    write_json(STRATEGY_DATA_DIR / "sentry_events.json", {"generated_at": now_iso(), "events": events})
    print(f"Sentry events written: {len(events)}")


if __name__ == "__main__":
    main()
