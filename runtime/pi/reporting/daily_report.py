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

from runtime.common.common import DAILY_REPORTS_DIR, EXECUTION_DATA_DIR, LEDGER_DIR, STRATEGY_DATA_DIR, today_iso, read_json, write_text


def fmt_positions(positions: list[dict], sleeve: str) -> list[str]:
    lines = []
    for p in positions:
        if p.get("sleeve") != sleeve:
            continue
        lines.append(f"- {p['ticker']}: shares={p['shares']}, avg_cost={p['avg_cost']}, last_price={p.get('last_price')}, unrealized_pnl={p.get('unrealized_pnl')}")
    return lines or ["- None"]


def main() -> None:
    portfolio = read_json(LEDGER_DIR / "mock_portfolio.json", {})
    execution = read_json(EXECUTION_DATA_DIR / "execution_log.json", {"items": []})
    sentry = read_json(STRATEGY_DATA_DIR / "sentry_events.json", {"events": []})
    rankings = read_json(STRATEGY_DATA_DIR / "alpha_rankings.json", {"items": []})
    positions = portfolio.get("positions", [])
    execution_mode = execution.get("execution_mode") or portfolio.get("execution_mode") or "local_simulated"
    entries = [x for x in execution.get("items", []) if x.get("requested_action") == "BUY" and x.get("execution_status") == "EXECUTED"]
    exits = [x for x in execution.get("items", []) if x.get("requested_action") == "SELL" and x.get("execution_status") == "EXECUTED"]
    broker_orders_sent = sum(1 for x in execution.get("items", []) if x.get("broker_order_id"))
    broker_fills = sum(1 for x in execution.get("items", []) if x.get("broker_status") == "filled")
    stop_take = [x for x in sentry.get("events", []) if x.get("event_type") in {"trailing_stop_hit", "take_profit_hit"}]
    watchlist = [x["ticker"] for x in rankings.get("items", [])[:5]]
    lines = [
        "# Daily Summary",
        "",
        f"Date: {today_iso()}",
        "",
        "## Portfolio Snapshot",
        f"- Current portfolio value: {portfolio.get('total_equity')}",
        f"- Current cash: {portfolio.get('cash')}",
        "",
        "## Open CORE Positions",
        *fmt_positions(positions, "CORE"),
        "",
        "## Open SWING Positions",
        *fmt_positions(positions, "SWING"),
        "",
        "## Activity Today",
        f"- Execution mode: {execution_mode}",
        f"- Entries today: {len(entries)}",
        f"- Exits today: {len(exits)}",
        f"- Broker orders sent: {broker_orders_sent}",
        f"- Broker fills observed: {broker_fills}",
        f"- Stop-loss / take-profit events: {len(stop_take)}",
        "",
        "## Signals And Watchlist",
        f"- Notable signal changes: {len(sentry.get('events', []))} sentry events recorded",
        f"- Watchlist for tomorrow: {', '.join(watchlist) if watchlist else 'None'}",
        "",
        "## Data Quality",
        "- Data-quality warnings: not embedded in this deterministic report; check runtime diagnostics if needed.",
        "- Fallback data usage notes: yfinance convenience data used in V1",
        "",
        "## Plan For Tomorrow",
        "- Re-run deterministic pipeline, inspect rejected executions, and review top-ranked names skeptically before any mock changes.",
    ]
    write_text(DAILY_REPORTS_DIR / f"daily_summary_{today_iso()}.md", "\n".join(lines) + "\n")
    print("Daily report written")


if __name__ == "__main__":
    main()
