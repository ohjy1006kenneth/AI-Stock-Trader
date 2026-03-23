# True OpenClaw Multi-Agent Plan

This version is event-driven, not polling-heavy.

## Architecture

### Layer 1 — Sentry (non-LLM)

- Script: `scripts/sentry.py`
- Schedule: every 15 minutes during US market hours
- Cost: minimal, no LLM tokens
- Input: `data/watchlist.json`
- Output: `data/triggers.json` and `context/trigger_events.md`

The sentry checks for trigger conditions such as:
- daily drop <= -3%
- cross above/below 200-day EMA
- breakout-style volume spikes

### Layer 2 — Trigger Gate

If no triggers fire:
- do nothing
- do not wake any LLM agents

If triggers fire:
- wake the 4 specialized OpenClaw agents
- only analyze the triggered tickers

### Layer 3 — Model Routing

- Macro Scout → cheap model
- Technical Analyst → cheap model
- Portfolio Risk Manager → stronger model if needed
- Mock Execution Agent / final execution analysis → big model

Routing config lives in:
- `data/model_routing.json`

## The 4 Specialized Agents

### 1) Macro Scout

Role:
- refresh fundamental context for triggered tickers
- confirm moat characteristics and analyst sentiment

Inputs:
- `context/trigger_events.md`
- `data/watchlist.json`

Outputs:
- `context/macro_watchlist.md`

Recommended OpenClaw spawn:
- runtime: `subagent`
- model: cheap

### 2) Technical Analyst

Role:
- analyze RSI, EMA50/EMA200, volume spike behavior
- determine `OVERSOLD_LONG`, `BREAKOUT_SWING`, or `WAIT`

Inputs:
- `context/trigger_events.md`
- `context/macro_watchlist.md`

Outputs:
- `context/technical_signals.md`

### 3) Portfolio Risk Manager

Role:
- classify `LONG_TERM_CORE` vs `SHORT_TERM_SWING`
- set position size and stop rules

Inputs:
- `context/macro_watchlist.md`
- `context/technical_signals.md`

Outputs:
- `context/risk_decisions.md`

### 4) Mock Execution Agent

Role:
- simulate trades only
- update portfolio and trade log
- prepare execution report and daily summary

Inputs:
- `context/risk_decisions.md`

Outputs:
- `data/trade_log.json`
- `data/portfolio.json`
- `context/execution_report.md`
- `context/daily_summary.md`

## Orchestrator Behavior

The main OpenClaw assistant acts as orchestrator:
1. check `data/triggers.json`
2. if empty, stop
3. if non-empty, wake agents in sequence
4. pass file-based context between them
5. keep paper-trading guardrails

## Suggested OpenClaw orchestration flow

1. Cron launches isolated run every 15 minutes.
2. That run executes `sentry.py`.
3. If triggers exist, the orchestrator wakes subagents.
4. The orchestrator reviews outputs.
5. Final execution analysis uses the stronger model.
6. Mock execution writes logs locally.

## Why this is cheaper

- No hourly full-agent turns when nothing happened
- Cheap model for scouting/technical filtering
- Strong model only for high-value final judgment
- Most time intervals end with no LLM work at all
