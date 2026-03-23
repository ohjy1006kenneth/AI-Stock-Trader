from __future__ import annotations

from common import LEDGER_DIR, OUTPUTS_DIR, REPORTS_DIR, now_iso, read_json, write_text


def main() -> None:
    portfolio = read_json(LEDGER_DIR / "mock_portfolio.json", {})
    decisions = read_json(OUTPUTS_DIR / "strategist_decisions.json", {"decisions": []})
    events = read_json(OUTPUTS_DIR / "sentry_events.json", {"events": []})
    text = f'''# Daily Summary\n\nGenerated at: {now_iso()}\n\n- Portfolio value: {portfolio.get("total_equity")}\n- Cash: {portfolio.get("cash")}\n- Open positions: {len(portfolio.get("open_positions", []))}\n- Decisions today: {len(decisions.get("decisions", []))}\n- Sentry events: {len(events.get("events", []))}\n\nStatus: placeholder summary. Use Daily Reporter agent for polished final wording.\n'''
    write_text(REPORTS_DIR / "daily_summary.md", text)
    print("Daily summary placeholder written")


if __name__ == "__main__":
    main()
