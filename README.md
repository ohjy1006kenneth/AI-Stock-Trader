# Trading

A deterministic mock-money quant trading workspace.

## Active architecture
- `trading` is the only orchestrator the user talks to.
- `trading` is also the canonical workspace and runtime owner.
- Four specialist agents sit under `trading`:
  1. `trading-quant-researcher`
  2. `trading-backtest-validator`
  3. `trading-portfolio-strategist`
  4. `trading-executor-reporter`

## What is preserved
- deterministic Python hot path
- strict ledger mutation boundary
- CORE / SWING distinction
- mock-only execution
- runtime wrappers, preflight checks, and reporting

## Project layout

### Research
- `research/` for formula definitions, factor notes, and trusted-source references

### Backtest
- `backtests/` for metrics and generated validation artifacts
- `scripts/backtest_engine.py` for deterministic historical simulation

### Strategy
- `scripts/build_universe.py`
- `scripts/fetch_price_data.py`
- `scripts/fetch_fundamental_data.py`
- `scripts/quality_filter.py`
- `scripts/calculate_alpha_score.py`
- `scripts/portfolio_strategist.py`
- `scripts/sentry_monitor.py`
- `config/portfolio_rules.md`

### Execution / Reporting
- `scripts/mock_portfolio_executor.py`
- `scripts/portfolio_status.py`
- runtime wrapper scripts in `scripts/`
- `ledger/`
- `outputs/`
- `reports/`

## Deterministic file flow
1. Strategy/data scripts write outputs.
2. Strategist writes decisions only.
3. Executor reads decisions and is the only ledger mutator.
4. Reporting reads final artifacts and summarizes the mock system state.

## Notes
Older multi-agent scaffolding has been archived under `archive/` inside this repo and under `/home/node/.openclaw/archived-trading-workspaces/` for retired external role workspaces.
