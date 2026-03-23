# Agent Workflow

## Event-driven sequence

1. `sentry.py` runs every 15 minutes during market hours.
2. If no trigger fires, no AI agents are woken.
3. If a trigger fires:
   - Macro Scout updates fundamental context
   - Technical Analyst checks setup quality
   - Portfolio Risk Manager decides category, size, stop logic
   - Mock Execution Agent simulates the trade and logs it
4. End-of-day summary is prepared separately.

## Context files

- `trigger_events.md`
- `macro_watchlist.md`
- `technical_signals.md`
- `risk_decisions.md`
- `execution_report.md`
- `daily_summary.md`

## Guardrails

- US stocks only
- paper trading only
- no daily forced liquidation
- all actions must be inspectable from local files
