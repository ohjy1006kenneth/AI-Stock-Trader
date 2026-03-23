# OpenClaw Cron Commands

Use `America/New_York` to align with US market hours and DST.

## 1) Weekly Macro Scout

Runs every Monday at 9:00 AM New York time.

```bash
openclaw cron add \
  --name "US Macro Scout" \
  --cron "0 9 * * 1" \
  --tz "America/New_York" \
  --session isolated \
  --message "Run python3 /home/node/.openclaw/workspace-trading/agents/us_stock_multi_agent/scripts/macro_scout.py and summarize the updated watchlist from context/macro_watchlist.md. Do not place live trades." \
  --delivery none
```

## 2) Hourly Technical / Risk / Mock Execution Pipeline

Runs hourly from 10:00 AM to 4:00 PM New York time on weekdays.

Note: standard cron cannot express 9:30 exactly plus then hourly, so split it into:
- 9:30 AM kickoff
- 10 AM through 4 PM hourly

### 2a) 9:30 AM market-open kickoff

```bash
openclaw cron add \
  --name "US Market Open Pipeline" \
  --cron "30 9 * * 1-5" \
  --tz "America/New_York" \
  --session isolated \
  --message "Run python3 /home/node/.openclaw/workspace-trading/agents/us_stock_multi_agent/scripts/technical_analyst.py && python3 /home/node/.openclaw/workspace-trading/agents/us_stock_multi_agent/scripts/portfolio_risk_manager.py && python3 /home/node/.openclaw/workspace-trading/agents/us_stock_multi_agent/scripts/mock_execution_agent.py && python3 /home/node/.openclaw/workspace-trading/agents/us_stock_multi_agent/scripts/reporting.py. Keep it paper-trading only." \
  --delivery none
```

### 2b) 10:00 AM to 4:00 PM hourly

```bash
openclaw cron add \
  --name "US Hourly Trading Pipeline" \
  --cron "0 10-16 * * 1-5" \
  --tz "America/New_York" \
  --session isolated \
  --message "Run python3 /home/node/.openclaw/workspace-trading/agents/us_stock_multi_agent/scripts/technical_analyst.py && python3 /home/node/.openclaw/workspace-trading/agents/us_stock_multi_agent/scripts/portfolio_risk_manager.py && python3 /home/node/.openclaw/workspace-trading/agents/us_stock_multi_agent/scripts/mock_execution_agent.py && python3 /home/node/.openclaw/workspace-trading/agents/us_stock_multi_agent/scripts/reporting.py. Keep it paper-trading only." \
  --delivery none
```

## 3) Daily Telegram Portfolio Summary

Example at 4:05 PM New York time.

```bash
openclaw cron add \
  --name "US Daily Portfolio Summary" \
  --cron "5 16 * * 1-5" \
  --tz "America/New_York" \
  --session isolated \
  --message "Run python3 /home/node/.openclaw/workspace-trading/agents/us_stock_multi_agent/scripts/reporting.py and send the contents of /home/node/.openclaw/workspace-trading/agents/us_stock_multi_agent/context/daily_summary.md as a concise Telegram portfolio summary. Paper trading only." \
  --announce
```

## Verification

```bash
openclaw cron status
openclaw cron list
openclaw cron runs --limit 20
```
