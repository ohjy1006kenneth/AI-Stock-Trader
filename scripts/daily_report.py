from __future__ import annotations

from common import LEDGER_DIR, OUTPUTS_DIR, REPORTS_DIR, now_iso, read_json, write_text


def summarize_positions(positions: list[dict], sleeve: str) -> list[str]:
    lines = []
    for pos in positions:
        if pos.get("sleeve") != sleeve:
            continue
        lines.append(
            f"- {pos['ticker']}: qty={pos['qty']}, avg_cost={pos['avg_cost']}, last_price={pos.get('last_price')}, unrealized_pnl={pos.get('unrealized_pnl')}"
        )
    return lines or ["- None"]


def main() -> None:
    portfolio = read_json(LEDGER_DIR / "mock_portfolio.json", {})
    decisions = read_json(OUTPUTS_DIR / "strategist_decisions.json", {"decisions": []})
    events = read_json(OUTPUTS_DIR / "sentry_events.json", {"events": []})
    positions = portfolio.get("open_positions", [])

    entries_today = [t for t in portfolio.get("trade_history", []) if t.get("side") == "BUY"]
    stop_events = [e for e in events.get("events", []) if e.get("event_type") == "trailing_stop_hit"]
    take_profit_events = [e for e in events.get("events", []) if e.get("event_type") == "take_profit_hit"]

    lines = [
        "# Daily Summary",
        "",
        f"Generated at: {now_iso()}",
        "",
        "## Portfolio Snapshot",
        f"- Current portfolio value: {portfolio.get('total_equity')}",
        f"- Current cash: {portfolio.get('cash')}",
        f"- Realized PnL: {portfolio.get('realized_pnl')}",
        f"- Unrealized PnL: {portfolio.get('unrealized_pnl')}",
        "",
        "## Open CORE Positions",
        *summarize_positions(positions, "CORE"),
        "",
        "## Open SWING Positions",
        *summarize_positions(positions, "SWING"),
        "",
        "## Activity Today",
        f"- Entries: {len(entries_today)}",
        f"- Exits: 0",
        f"- Stop-loss events: {len(stop_events)}",
        f"- Take-profit events: {len(take_profit_events)}",
        "",
        "## Signal / Watchlist Changes",
        f"- Strategist decisions generated: {len(decisions.get('decisions', []))}",
        f"- Sentry events generated: {len(events.get('events', []))}",
        "- Upcoming watchlist: review top ranked names and any unresolved sentry events",
        "",
        "## Data Quality",
        "- Warnings: none embedded in deterministic report",
        "- Fallback data used: yes, yfinance convenience data for V1 prototyping",
        "",
        "## Plan For Tomorrow",
        "- Re-run ingestion, refresh rankings, mark portfolio, and review sentry escalations before changing mock holdings",
    ]
    write_text(REPORTS_DIR / "daily_summary.md", "\n".join(lines) + "\n")
    print("Daily summary written")


if __name__ == "__main__":
    main()
