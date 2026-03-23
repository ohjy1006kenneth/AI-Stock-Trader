# OpenClaw Cron Commands

Use `America/New_York` to align with US market hours and DST.

## 1) Weekly Macro Watchlist Refresh

Runs every Monday at 9:00 AM New York time.

```bash
openclaw cron add \
  --name "US Macro Scout Refresh" \
  --cron "0 9 * * 1" \
  --tz "America/New_York" \
  --session isolated \
  --message "Run python3 /home/node/.openclaw/workspace-trading/agents/us_stock_multi_agent/scripts/macro_scout.py and refresh the watchlist markdown. Paper trading only." \
  --delivery none
```

## 2) 15-Minute Non-LLM Sentry During US Market Hours

This is the cheap trigger layer.

Runs at:
- 9:30, 9:45
- every 15 minutes from 10:00 to 15:45
- 16:00
- weekdays only

```bash
openclaw cron add \
  --name "US Trigger Sentry" \
  --cron "30,45 9 * * 1-5" \
  --tz "America/New_York" \
  --session isolated \
  --message "Run python3 /home/node/.openclaw/workspace-trading/agents/us_stock_multi_agent/scripts/sentry.py. If no triggers exist in data/triggers.json, say no triggers and stop. Do not wake analysis agents." \
  --delivery none

openclaw cron add \
  --name "US Trigger Sentry Intraday" \
  --cron "0,15,30,45 10-15 * * 1-5" \
  --tz "America/New_York" \
  --session isolated \
  --message "Run python3 /home/node/.openclaw/workspace-trading/agents/us_stock_multi_agent/scripts/sentry.py. If no triggers exist in data/triggers.json, say no triggers and stop. Do not wake analysis agents." \
  --delivery none

openclaw cron add \
  --name "US Trigger Sentry Close" \
  --cron "0 16 * * 1-5" \
  --tz "America/New_York" \
  --session isolated \
  --message "Run python3 /home/node/.openclaw/workspace-trading/agents/us_stock_multi_agent/scripts/sentry.py. If no triggers exist in data/triggers.json, say no triggers and stop. Do not wake analysis agents." \
  --delivery none
```

## 3) Triggered Multi-Agent Analysis Run

This is the true OpenClaw orchestration step. Only run this if triggers were created by the sentry.

```bash
openclaw cron add \
  --name "US Triggered Orchestrator" \
  --cron "31,46 9 * * 1-5" \
  --tz "America/New_York" \
  --session isolated \
  --message "Act as the orchestrator. Read /home/node/.openclaw/workspace-trading/agents/us_stock_multi_agent/data/triggers.json. If it is empty, stop. If triggers exist, wake the 4 specialist agents in order using cheap models for Macro Scout and Technical Analyst, then stronger model for final risk/execution analysis. Persist outputs to Markdown files in context/. Never place live trades." \
  --delivery none

openclaw cron add \
  --name "US Triggered Orchestrator Intraday" \
  --cron "1,16,31,46 10-15 * * 1-5" \
  --tz "America/New_York" \
  --session isolated \
  --message "Act as the orchestrator. Read /home/node/.openclaw/workspace-trading/agents/us_stock_multi_agent/data/triggers.json. If it is empty, stop. If triggers exist, wake the 4 specialist agents in order using cheap models for Macro Scout and Technical Analyst, then stronger model for final risk/execution analysis. Persist outputs to Markdown files in context/. Never place live trades." \
  --delivery none

openclaw cron add \
  --name "US Triggered Orchestrator Close" \
  --cron "1 16 * * 1-5" \
  --tz "America/New_York" \
  --session isolated \
  --message "Act as the orchestrator. Read /home/node/.openclaw/workspace-trading/agents/us_stock_multi_agent/data/triggers.json. If it is empty, stop. If triggers exist, wake the 4 specialist agents in order using cheap models for Macro Scout and Technical Analyst, then stronger model for final risk/execution analysis. Persist outputs to Markdown files in context/. Never place live trades." \
  --delivery none
```

## 4) Daily Telegram Portfolio Summary

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
