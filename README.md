# US Stock Mock Research & Portfolio System

Architecture-first scaffold for a deterministic US stock research and mock portfolio system in OpenClaw.

## Scope
- US stocks only
- Mock portfolio only
- No broker connectivity
- No live trading
- No silent strategy drift
- Deterministic hot path
- LLMs only for research, validation, explanation, reporting, and controlled maintenance

## Trading Agent Layout
- `trading` -> project/runtime workspace owner
- `trading-orchestrator` -> supervisor/dispatcher
- `trading-data-guardian`
- `trading-scholar`
- `trading-backtest-validator`
- `trading-code-maintainer`
- `trading-strategist`
- `trading-executor`
- `trading-daily-reporter`

## Build Plan
Phase 1 in this commit:
- architecture
- prompts
- schemas
- starter files only

Phase 2:
- deterministic script logic

## Convenience Commands
- `python3 scripts/portfolio_status.py` -> quick mock portfolio inspection
