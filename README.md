# Trading

A deterministic paper-trading trading workspace with OpenClaw orchestration.

## Active architecture
- `trading` is the orchestrator the user talks to and the canonical project owner.
- Specialists sit under `trading`:
  1. `trading-quant-researcher`
  2. `trading-backtest-validator`
  3. `trading-portfolio-strategist`
  4. `trading-executor-reporter`

## What is preserved
- deterministic Python hot path
- strict executor-only ledger mutation boundary
- CORE / SWING distinction
- paper trading only
- runtime wrappers, preflight checks, and reporting

## Project layout

### Research
- `research/` for formula definitions, factor notes, and trusted-source references

### Backtest
- `backtests/engine/` for deterministic historical simulation
- `backtests/outputs/` for generated validation artifacts

### Runtime data + execution
- `runtime/pi/data/` for runtime data collection steps
- `strategy/` for screening, ranking, sentry, and decision-policy logic
- `runtime/pi/execution/` for paper execution and status
- `runtime/pi/reporting/` for reports and alerts
- `runtime/pi/wrappers/` for repo-relative runtime entrypoints
- `data/runtime/` for generated runtime artifacts
- `ledger/` for local paper portfolio state
- `reports/` for human-readable summaries and diagnostics

## Deterministic file flow
1. Runtime data steps refresh market and fundamental artifacts.
2. Strategy logic writes approved screening, ranking, sentry, and decision artifacts.
3. The executor reads decisions and is the only ledger mutator.
4. Reporting reads final artifacts and summarizes the mock system state.

## Notes
- Cloud is intended for training, heavy backtesting, validation prep, and artifact export.
- Raspberry Pi is intended for runtime: fresh data, runtime features, artifact loading, inference, decision conversion, paper execution, and reporting.
- Older multi-agent scaffolding has been archived under `archive/`.
