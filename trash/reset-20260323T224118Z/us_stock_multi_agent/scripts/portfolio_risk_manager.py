from __future__ import annotations

from common import CONTEXT_DIR, DATA_DIR, load_json, save_json, utc_now_iso, write_markdown


def classify(signal: dict) -> dict | None:
    fundamental = float(signal.get("fundamental_score", 0))
    signal_type = signal.get("signal", "WAIT")
    rsi_value = float(signal.get("rsi14", 50)) if signal.get("rsi14") not in (None, "n/a") else 50.0
    price = signal.get("price")
    if not price or signal_type == "WAIT":
        return None

    volatility_flag = signal_type == "BREAKOUT_SWING" and signal.get("volume_spike", False)
    if fundamental >= 70 and signal_type == "OVERSOLD_LONG":
        return {
            "ticker": signal["ticker"],
            "action": "BUY",
            "holding_type": "LONG_TERM_CORE",
            "size_pct": 0.10,
            "entry_price": price,
            "trailing_stop_pct": 15,
            "reasoning": f"High fundamental score ({fundamental}) with oversold long-term setup; RSI={rsi_value}",
        }
    if fundamental >= 55 and volatility_flag:
        return {
            "ticker": signal["ticker"],
            "action": "BUY",
            "holding_type": "SHORT_TERM_SWING",
            "size_pct": 0.05,
            "entry_price": price,
            "trailing_stop_pct": 5,
            "reasoning": f"Momentum breakout with acceptable fundamentals ({fundamental}) and volume confirmation",
        }
    return None


def main() -> None:
    technicals = load_json(DATA_DIR / "technical_signals.json", {"items": []})
    decisions = [decision for signal in technicals.get("items", []) if (decision := classify(signal))]
    payload = {"generated_at": utc_now_iso(), "items": decisions}
    save_json(DATA_DIR / "risk_decisions.json", payload)

    lines = ["# Risk Decisions", "", f"Generated at: {payload['generated_at']}", ""]
    if not decisions:
        lines.append("No trade decisions passed current rules.\n")
    for d in decisions:
        lines.extend([
            f"## {d['ticker']} - {d['action']}",
            f"- Holding type: {d['holding_type']}",
            f"- Position size: {round(d['size_pct'] * 100, 2)}%",
            f"- Entry price: {d['entry_price']}",
            f"- Trailing stop: {d['trailing_stop_pct']}%",
            f"- Reasoning: {d['reasoning']}",
            "",
        ])
    write_markdown(CONTEXT_DIR / "risk_decisions.md", "\n".join(lines) + "\n")


if __name__ == "__main__":
    main()
