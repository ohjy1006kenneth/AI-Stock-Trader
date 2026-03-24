# Simplified Trading Architecture

## Canonical runtime owner
- `trading`
- This is the only orchestrator the user talks to.
- This is the only canonical workspace and runtime owner.
- It supervises the specialist roles directly.
- There is no longer a separate `trading-orchestrator` project role in the active architecture.

## Hard constraints
- Keep the system mock-money only.
- Keep repeated calculations in deterministic Python.
- Do not put LLM reasoning into the numeric hot path.
- Preserve the deterministic ledger boundary.
- The executor remains the only ledger mutator.
- Specialist role workspaces must not become second sources of truth.

## Final specialist model
Exactly four specialist roles sit under `trading`:

1. **Quant Researcher**
2. **Backtest Validator**
3. **Portfolio Strategist**
4. **Executor / Reporter**

## Role mapping from the old scaffold

### Active roles
- `trading` -> orchestrator + canonical runtime owner
- `trading-scholar` -> **Quant Researcher**
- `trading-backtest-validator` -> **Backtest Validator**
- `trading-strategist` -> **Portfolio Strategist**
- `trading-executor` -> **Executor / Reporter**

### Folded responsibilities
The following old conceptual roles are no longer active standalone roles in the project design:
- `trading-orchestrator`
- `trading-data-guardian`
- `trading-code-maintainer`
- `trading-daily-reporter`

Their useful responsibilities are absorbed as follows:
- data sanity / schema vigilance -> Quant Researcher + `trading` runtime supervision
- deterministic code maintenance -> `trading` + Quant Researcher workflow, using deterministic Python changes in the canonical repo
- daily reporting -> Executor / Reporter
- orchestration -> `trading`

## Simplified workflow
1. **Quant Researcher** defines formulas, curates the formula registry, and proposes factor/rule changes.
2. **Backtest Validator** tests realism, historical usefulness, and overfitting risk before promotion.
3. **Portfolio Strategist** converts approved deterministic outputs into CORE/SWING decisions.
4. **Executor / Reporter** applies decisions to the mock ledger and produces human-readable reporting.

`trading` supervises all four directly.

## Active project structure
The active project should be thought of in four layers:

### 1) Research
Purpose:
- factor ideas, formula definitions, source notes, and promotion rationale

Primary files:
- `research/formula_registry.json`
- `research/factor_notes.md`
- `research/trusted_sources.md`

### 2) Backtest
Purpose:
- historical simulation, metrics, validation artifacts

Primary files:
- `scripts/backtest_engine.py`
- `backtests/backtest_report.md`
- `backtests/metrics.json`
- `backtests/equity_curve.csv`
- `backtests/trade_log.csv`
- `reports/backtest_verdict.md`

### 3) Strategy
Purpose:
- universe construction, data snapshots, quality screen, alpha ranking, strategist decision logic, and strategy rules

Primary files:
- `scripts/build_universe.py`
- `scripts/fetch_price_data.py`
- `scripts/fetch_fundamental_data.py`
- `scripts/quality_filter.py`
- `scripts/calculate_alpha_score.py`
- `scripts/portfolio_strategist.py`
- `scripts/sentry_monitor.py`
- `config/portfolio_rules.md`
- `outputs/universe.json`
- `outputs/price_snapshot.json`
- `outputs/fundamental_snapshot.json`
- `outputs/qualified_universe.json`
- `outputs/alpha_rankings.json`
- `outputs/sentry_events.json`
- `outputs/strategist_decisions.json`

### 4) Execution / Reporting
Purpose:
- deterministic mock execution, portfolio inspection, alerts, summaries, runtime wrappers

Primary files:
- `scripts/mock_portfolio_executor.py`
- `scripts/portfolio_status.py`
- `scripts/trade_alerts.py`
- `scripts/daily_report.py`
- `scripts/preflight_check.py`
- `scripts/run_pipeline.sh`
- `scripts/run_preflight_alert.sh`
- `scripts/run_trade_alerts.sh`
- `scripts/run_daily_summary.sh`
- `ledger/mock_portfolio.json`
- `outputs/execution_log.json`
- `outputs/trade_alerts_latest.json`
- `reports/current_pipeline_explanation.md`
- `reports/quality_filter_diagnosis.md`
- `reports/daily_summary_TEMPLATE.md`

## Deterministic file flow contract
1. Data and strategy scripts write canonical outputs.
2. The strategist reads approved outputs and writes decisions only.
3. The executor reads strategist decisions and the ledger.
4. The executor is the only component allowed to mutate `ledger/mock_portfolio.json`.
5. Reporting reads final deterministic artifacts and produces summaries.

No other component may mutate the ledger.

## Promotion workflow
1. Researcher proposes or revises a formula.
2. Formula is documented in the registry with fields, lag rules, and caveats.
3. Deterministic Python implementation is updated.
4. Backtest engine is run on historical data.
5. Backtest Validator decides whether the idea is weak, provisional, or promotion-worthy.
6. Only promoted logic influences strategist decisions.

## Model posture
- `trading`: orchestrator / runtime owner, current strong model acceptable
- `trading-scholar`: strong reasoning model
- `trading-backtest-validator`: strong reasoning model
- `trading-strategist`: strong reasoning model
- `trading-executor`: deterministic Python in hot path; LLM only for debugging or explanation
