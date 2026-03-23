# US Stock Multi-Agent Mock Investment System

US-stock-only, paper-trading investment stack for OpenClaw.

This version is now designed for **true OpenClaw multi-agent orchestration** with an **event-driven trigger system**.

## Layers

1. Sentry → non-LLM trigger scanner running every 15 minutes
2. Macro Scout → builds or refreshes fundamentally screened watchlist context
3. Technical Analyst → finds entry setups on triggered watchlist names
4. Portfolio Risk Manager → classifies trades and sizes positions
5. Mock Execution Agent → simulates fills and tracks portfolio state

## Guardrails

- US equities only
- No live trading
- Trades logged locally to JSON
- No forced daily liquidation
- Flexible holds based on technical and fundamental triggers

## Workspace layout

- `agents/us_stock_multi_agent/prompts/` → system prompts for each agent
- `agents/us_stock_multi_agent/scripts/` → Python agents
- `agents/us_stock_multi_agent/context/` → Markdown handoff files
- `agents/us_stock_multi_agent/data/` → watchlists, trades, portfolio JSON
- `agents/us_stock_multi_agent/logs/` → runtime logs
- `agents/us_stock_multi_agent/cron/` → cron command references

## Typical flow

1. Run `macro_scout.py` weekly to refresh the watchlist
2. `sentry.py` checks the watchlist every 15 minutes during market hours
3. If no trigger fires, stop there
4. If a trigger fires, the OpenClaw orchestrator wakes the 4 agents
5. Review `context/macro_watchlist.md`
6. Review `context/technical_signals.md`
7. Review `context/risk_decisions.md`
8. `mock_execution_agent.py` simulates trades
9. `reporting.py` writes the summary output

## Quick start

```bash
cd /home/node/.openclaw/workspace-trading
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python3 agents/us_stock_multi_agent/scripts/bootstrap_data.py
python3 agents/us_stock_multi_agent/scripts/macro_scout.py
python3 agents/us_stock_multi_agent/scripts/sentry.py
python3 agents/us_stock_multi_agent/scripts/orchestrator.py
python3 agents/us_stock_multi_agent/scripts/technical_analyst.py
python3 agents/us_stock_multi_agent/scripts/portfolio_risk_manager.py
python3 agents/us_stock_multi_agent/scripts/mock_execution_agent.py
python3 agents/us_stock_multi_agent/scripts/reporting.py
```

## Scheduling notes

Use `America/New_York` for cron jobs.

- Macro Scout refresh: Monday 9:00 AM New York time
- Sentry: every 15 minutes during US market hours
- LLM agents: only on trigger
- Final summary: after market close

See:
- `agents/us_stock_multi_agent/cron/openclaw_cron_commands.md`
- `agents/us_stock_multi_agent/OPENCLAW_MULTI_AGENT_PLAN.md`
