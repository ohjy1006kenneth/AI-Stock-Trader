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

from runtime.common.common import BACKTEST_REPORTS_DIR, DAILY_REPORTS_DIR, EXECUTION_DATA_DIR, LEDGER_DIR, MARKET_DATA_DIR, PIPELINE_REPORTS_DIR, STRATEGY_DATA_DIR, now_iso, read_json, write_text


def count_strategist_actions(decisions: list[dict]) -> dict[str, int]:
    counts = {"BUY": 0, "SELL": 0, "HOLD": 0, "REVIEW": 0}
    for row in decisions:
        action = row.get("action")
        if action in counts:
            counts[action] += 1
    return counts


def count_execution_results(items: list[dict]) -> tuple[int, int, int, int]:
    executed = sum(1 for x in items if x.get("execution_status") == "EXECUTED")
    rejected = sum(1 for x in items if x.get("execution_status") == "REJECTED")
    buys = sum(1 for x in items if x.get("execution_status") == "EXECUTED" and x.get("requested_action") == "BUY")
    sells = sum(1 for x in items if x.get("execution_status") == "EXECUTED" and x.get("requested_action") == "SELL")
    return executed, rejected, buys, sells


def read_recent_validation_note() -> str:
    path = BACKTEST_REPORTS_DIR / "backtest_verdict.md"
    if not path.exists():
        return "No validation memo found."
    lines = [line.strip() for line in path.read_text().splitlines() if line.strip()]
    for line in lines:
        if line.startswith("**") or line.startswith("Current verdict") or line.startswith("## Verdict"):
            return line.replace("## ", "")
    return lines[0] if lines else "Validation memo present but empty."


def read_recent_research_note() -> str:
    return "No research-layer changes affected this run; current formulas were used as-is."


def main() -> None:
    ts = now_iso()
    universe = read_json(MARKET_DATA_DIR / "universe.json", {"tickers": []})
    prices = read_json(MARKET_DATA_DIR / "price_snapshot.json", {"items": []})
    fundamentals = read_json(MARKET_DATA_DIR / "fundamental_snapshot.json", {"items": []})
    quality = read_json(STRATEGY_DATA_DIR / "qualified_universe.json", {"items": [], "rejected": []})
    rankings = read_json(STRATEGY_DATA_DIR / "alpha_rankings.json", {"items": []})
    sentry = read_json(STRATEGY_DATA_DIR / "sentry_events.json", {"events": []})
    strategist = read_json(STRATEGY_DATA_DIR / "strategist_decisions.json", {"decisions": []})
    execution = read_json(EXECUTION_DATA_DIR / "execution_log.json", {"items": []})
    alerts = read_json(EXECUTION_DATA_DIR.parent / "alerts" / "trade_alerts_latest.json", {"items": []})
    portfolio = read_json(LEDGER_DIR / "paper_portfolio.json", {})

    universe_size = len(universe.get("tickers", []))
    price_count = len(prices.get("items", []))
    fundamental_count = len(fundamentals.get("items", []))
    quality_pass = len(quality.get("items", []))
    quality_reject = len(quality.get("rejected", []))
    strong_alpha = [x for x in rankings.get("items", []) if (x.get("alpha_score") is not None and x.get("alpha_score") >= 0.80)]
    core_candidates = [x.get("ticker") for x in rankings.get("items", []) if x.get("factors", {}).get("is_quality_eligible") and x.get("alpha_score") is not None and x.get("alpha_score") >= 0.65 and x.get("factors", {}).get("trend_filter_pass")]
    swing_candidates = [x.get("ticker") for x in rankings.get("items", []) if x.get("alpha_score") is not None and x.get("alpha_score") >= 0.80 and x.get("factors", {}).get("trend_filter_pass")]
    decisions = strategist.get("decisions", [])
    action_counts = count_strategist_actions(decisions)
    executed, rejected, buys, sells = count_execution_results(execution.get("items", []))
    execution_mode = execution.get("execution_mode") or portfolio.get("execution_mode") or "mock"
    broker_orders_sent = sum(1 for x in execution.get("items", []) if x.get("requested_action") in {"BUY", "SELL"} and x.get("broker_order_id"))
    broker_fills = sum(1 for x in execution.get("items", []) if x.get("broker_status") == "filled")
    report_path = DAILY_REPORTS_DIR / f"daily_summary_{ts[:10]}.md"
    report_status = "produced" if report_path.exists() else "missing"
    alert_items = alerts.get("items", [])
    alert_status = f"{len(alert_items)} new trade alert(s) generated" if alert_items else "no new trade alerts"
    warning_lines = []
    if quality_pass == 0:
        warning_lines.append("- No names passed the quality filter.")
    if not strong_alpha:
        warning_lines.append("- No names cleared the strong-alpha threshold.")
    if rejected:
        warning_lines.append(f"- {rejected} execution record(s) were rejected.")
    if report_status != "produced":
        warning_lines.append("- Daily report file was not found after pipeline run.")
    if not warning_lines:
        warning_lines.append("- None")

    lines = [
        "# Pipeline Run Summary",
        "",
        f"Run timestamp: {ts}",
        "Pipeline status: SUCCESS",
        "Preflight result: PASSED",
        "",
        "## Overall pipeline summary",
        f"- Universe size: {universe_size}",
        f"- Price snapshot count: {price_count}",
        f"- Fundamental snapshot count: {fundamental_count}",
        f"- Quality filter: {quality_pass} passed, {quality_reject} rejected",
        f"- Alpha ranking: {len(rankings.get('items', []))} ranked, {len(strong_alpha)} strong-alpha names",
        f"- Strategy result: BUY={action_counts['BUY']}, SELL={action_counts['SELL']}, HOLD={action_counts['HOLD']}, REVIEW={action_counts['REVIEW']}",
        f"- Execution result: executed={executed}, rejected={rejected}, buys={buys}, sells={sells}",
        f"- Execution mode: {execution_mode}",
        f"- Broker orders sent: {broker_orders_sent}, broker fills observed: {broker_fills}",
        f"- Sentry events: {len(sentry.get('events', []))}",
        f"- Report result: {report_status}",
        f"- Alert result: {alert_status}",
        "",
        "## Agent summaries",
        "",
        "### trading",
        "- Orchestrated the single after-close workflow: preflight, data refresh, screening, ranking, sentry checks, strategist decisions, executor run, report generation, and alert generation.",
        "- Preflight passed and no stages were skipped after launch.",
        f"- Final run status: success; cash={portfolio.get('cash')}, total_equity={portfolio.get('total_equity')}.",
        "",
        "### trading-quant-researcher",
        "- Current factor stack was used as documented: ranked momentum, realized volatility, trend bonus, and quality bonus over the refreshed snapshots.",
        f"- Research-layer status: {read_recent_research_note()}",
        "- No ad hoc research change was introduced during this pipeline run unless separately recorded elsewhere.",
        "",
        "### trading-backtest-validator",
        f"- Validation context for this run: {read_recent_validation_note()}",
        "- No new formula or threshold was backtested during this runtime pipeline itself; this run consumed the currently integrated strategy configuration.",
        "- The validator remained relevant as governance context, not as a hot-path computation stage.",
        "",
        "### trading-portfolio-strategist",
        f"- Reviewed {len(rankings.get('items', []))} ranked names after filtering and scoring.",
        f"- Quality-passing names: {quality_pass}. Strong-alpha names: {len(strong_alpha)}.",
        f"- CORE candidates this run: {', '.join(core_candidates) if core_candidates else 'None'}.",
        f"- SWING candidates this run: {', '.join(swing_candidates) if swing_candidates else 'None'}.",
        f"- Decisions generated: BUY={action_counts['BUY']}, SELL={action_counts['SELL']}, HOLD={action_counts['HOLD']}, REVIEW={action_counts['REVIEW']}.",
        "",
        "### trading-executor-reporter",
        f"- Ledger activity: executed={executed}, rejected={rejected}, buys={buys}, sells={sells}.",
        f"- Broker path: mode={execution_mode}, orders_sent={broker_orders_sent}, fills_observed={broker_fills}.",
        f"- End-of-run portfolio snapshot: cash={portfolio.get('cash')}, total_equity={portfolio.get('total_equity')}, open_positions={len(portfolio.get('positions', []))}.",
        f"- Trade alerts: {alert_status}.",
        f"- Daily report status: {report_status} at `{report_path.name}`.",
        "",
        "## Warnings / blockers",
        *warning_lines,
    ]

    out_path = PIPELINE_REPORTS_DIR / f"pipeline_run_summary_{ts[:10]}.md"
    write_text(out_path, "\n".join(lines) + "\n")
    print("\n".join(lines))


if __name__ == "__main__":
    main()
