from __future__ import annotations

from common import OUTPUTS_DIR, LEDGER_DIR, gen_id, now_iso, read_json, write_json


def build_buy_decision(ticker: str, sleeve: str, price: float, reason_code: str, thesis: str, trigger_source: str, confidence: str, max_days: int | None = None) -> dict:
    return {
        "decision_id": gen_id("dec"),
        "timestamp": now_iso(),
        "ticker": ticker,
        "action": "BUY",
        "sleeve": sleeve,
        "target_shares": None,
        "target_notional": 12000 if sleeve == "CORE" else 6000,
        "max_allowed_price_for_entry": round(price * 1.01, 4) if price else None,
        "reason_code": reason_code,
        "thesis_summary": thesis,
        "trigger_source": trigger_source,
        "confidence_label": confidence,
        "stop_loss_rule": None if sleeve == "CORE" else {"type": "trailing_stop_pct", "value": 0.10},
        "take_profit_rule": None if sleeve == "CORE" else {"type": "take_profit_pct", "value": 0.15},
        "max_holding_period_days": max_days,
        "requires_executor_validation": True,
    }


def main() -> None:
    rankings = read_json(OUTPUTS_DIR / "alpha_rankings.json", {"items": []})
    qualified = read_json(OUTPUTS_DIR / "qualified_universe.json", {"items": []})
    sentry = read_json(OUTPUTS_DIR / "sentry_events.json", {"events": []})
    portfolio = read_json(LEDGER_DIR / "mock_portfolio.json", {"positions": []})

    quality_set = {item["ticker"] for item in qualified.get("items", [])}
    open_tickers = {item["ticker"] for item in portfolio.get("positions", [])}
    exits = {e["ticker"] for e in sentry.get("events", []) if e.get("event_type") in {"trailing_stop_hit", "take_profit_hit", "signal_decay", "scheduled_review_due"}}

    decisions = []
    for item in rankings.get("items", []):
        ticker = item["ticker"]
        score = item.get("alpha_score")
        factors = item.get("factors", {})
        price = factors.get("current_close")
        if ticker in open_tickers and ticker in exits:
            decisions.append({
                "decision_id": gen_id("dec"),
                "timestamp": now_iso(),
                "ticker": ticker,
                "action": "SELL",
                "sleeve": next((p.get("sleeve") for p in portfolio.get("positions", []) if p.get("ticker") == ticker), "SWING"),
                "target_shares": None,
                "target_notional": None,
                "max_allowed_price_for_entry": None,
                "reason_code": "sentry_exit_event",
                "thesis_summary": "Exit driven by deterministic sentry event.",
                "trigger_source": "sentry_monitor",
                "confidence_label": "HIGH",
                "stop_loss_rule": None,
                "take_profit_rule": None,
                "max_holding_period_days": None,
                "requires_executor_validation": True,
            })
            continue
        if ticker in open_tickers or score is None or price is None:
            continue
        if ticker in quality_set and score >= 0.65 and factors.get("trend_filter_pass"):
            decisions.append(build_buy_decision(ticker, "CORE", price, "quality_plus_alpha", "Passes quality screen with acceptable approved alpha support.", "alpha_rankings", "MEDIUM"))
        elif score >= 0.80 and factors.get("trend_filter_pass"):
            decisions.append(build_buy_decision(ticker, "SWING", price, "strong_alpha_tactical", "Strong approved alpha signal for tactical swing sleeve.", "alpha_rankings", "MEDIUM", max_days=20))
        else:
            decisions.append({
                "decision_id": gen_id("dec"),
                "timestamp": now_iso(),
                "ticker": ticker,
                "action": "REVIEW",
                "sleeve": "SWING",
                "target_shares": None,
                "target_notional": None,
                "max_allowed_price_for_entry": None,
                "reason_code": "no_action_threshold_not_met",
                "thesis_summary": "No action. Keep under review until thresholds are met.",
                "trigger_source": "alpha_rankings",
                "confidence_label": "LOW",
                "stop_loss_rule": None,
                "take_profit_rule": None,
                "max_holding_period_days": None,
                "requires_executor_validation": True,
            })

    write_json(OUTPUTS_DIR / "strategist_decisions.json", {
        "generated_at": now_iso(),
        "decision_schema_version": "v1",
        "decisions": decisions,
    })
    print(f"Strategist decisions written: {len(decisions)}")


if __name__ == "__main__":
    main()
