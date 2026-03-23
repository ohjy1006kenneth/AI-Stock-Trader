# US Stock Multi-Agent Mock Investment System

US-stock-only, paper-trading investment stack for OpenClaw.

## Layers

1. Macro Scout → builds a fundamentally screened watchlist
2. Technical Analyst → finds entry setups on the watchlist
3. Portfolio Risk Manager → classifies trades and sizes positions
4. Mock Execution Agent → simulates fills and tracks portfolio state

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

1. Run `macro_scout.py`
2. Review `context/macro_watchlist.md`
3. Run `technical_analyst.py`
4. Run `portfolio_risk_manager.py`
5. Run `mock_execution_agent.py`
6. Run `reporting.py` for summary output

## Quick start

```bash
cd /home/node/.openclaw/workspace-trading
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python3 agents/us_stock_multi_agent/scripts/bootstrap_data.py
python3 agents/us_stock_multi_agent/scripts/macro_scout.py
python3 agents/us_stock_multi_agent/scripts/technical_analyst.py
python3 agents/us_stock_multi_agent/scripts/portfolio_risk_manager.py
python3 agents/us_stock_multi_agent/scripts/mock_execution_agent.py
python3 agents/us_stock_multi_agent/scripts/reporting.py
```

## Scheduling notes

Use `America/New_York` for cron jobs.

- Macro Scout: Monday 9:00 AM New York time
- Technical stack: hourly during market hours

See `agents/us_stock_multi_agent/cron/openclaw_cron_commands.md`.
