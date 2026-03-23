from __future__ import annotations

from common import CONTEXT_DIR, DATA_DIR, save_json, write_markdown, utc_now_iso


def main() -> None:
    save_json(DATA_DIR / "trade_log.json", [])
    save_json(DATA_DIR / "portfolio.json", {
        "cash": 100000.0,
        "positions": {},
        "last_updated": utc_now_iso(),
    })
    save_json(DATA_DIR / "watchlist.json", {"generated_at": None, "items": []})
    save_json(DATA_DIR / "technical_signals.json", {"generated_at": None, "items": []})
    save_json(DATA_DIR / "risk_decisions.json", {"generated_at": None, "items": []})
    save_json(DATA_DIR / "triggers.json", {"generated_at": None, "items": []})
    save_json(DATA_DIR / "model_routing.json", {
        "macro_scout": {"model": "github-copilot/claude-3-haiku", "thinking": "low"},
        "technical_analyst": {"model": "github-copilot/claude-3-haiku", "thinking": "low"},
        "portfolio_risk_manager": {"model": "openai-codex/gpt-5.4", "thinking": "low"},
        "mock_execution_agent": {"model": "openai-codex/gpt-5.4", "thinking": "low"},
        "final_execution_analysis": {"model": "openai-codex/gpt-5.4", "thinking": "medium"}
    })
    write_markdown(CONTEXT_DIR / "trigger_events.md", "# Trigger Events\n\nPending first Sentry run.\n")
    write_markdown(CONTEXT_DIR / "macro_watchlist.md", "# Macro Watchlist\n\nPending first Macro Scout run.\n")
    write_markdown(CONTEXT_DIR / "technical_signals.md", "# Technical Signals\n\nPending first Technical Analyst run.\n")
    write_markdown(CONTEXT_DIR / "risk_decisions.md", "# Risk Decisions\n\nPending first Risk Manager run.\n")
    write_markdown(CONTEXT_DIR / "execution_report.md", "# Execution Report\n\nPending first Mock Execution run.\n")


if __name__ == "__main__":
    main()
