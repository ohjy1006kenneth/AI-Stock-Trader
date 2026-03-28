# Agent Ownership Rules

This file defines the active ownership model for the trading system.

## Global rules
- `trading` is the only canonical runtime and artifact-owning workspace.
- Specialists work inside their domains but must not become second sources of truth.
- Repeated calculations stay in deterministic Python.
- LLMs do not go into the numeric hot path.
- `trading-executor-reporter` remains the only runtime ledger mutator through `runtime/pi/execution/mock_portfolio_executor.py`.
- Specialists should author code in their own specialty whenever possible.
- `trading` coordinates, integrates, and resolves cross-cutting design or implementation conflicts.
- `trading-executor-reporter` is intentionally lightweight and should not be the default author for complex cross-cutting code.

## Active agent ownership

### 1) trading
**Role:** orchestrator + canonical runtime owner

**Responsibilities:**
- coordinate specialist work
- integrate approved changes into the canonical workspace
- preserve source of truth and deterministic file flow
- own runtime wiring, cron wrappers, and cross-cutting glue
- take over implementation when a task spans multiple specialist domains
- take over execution/reporting changes when they become architectural or nontrivially cross-cutting

**Reads:**
- entire canonical workspace as needed for integration

**Writes:**
- any canonical file when integration or cross-cutting changes are required
- runtime/config/integration docs and wrapper scripts

**Must not outsource away:**
- final integration decisions
- source-of-truth artifact ownership

**Conceptual script ownership:**
- `runtime/pi/wrappers/run_pipeline.sh`
- `runtime/pi/wrappers/run_preflight_alert.sh`
- `runtime/pi/wrappers/run_trade_alerts.sh`
- `runtime/pi/wrappers/run_daily_summary.sh`
- `runtime/pi/preflight/preflight_check.py`
- cross-cutting integration glue and orchestration-facing config

---

### 2) trading-quant-researcher
**Role:** formula research, factor design, and research documentation

**Responsibilities:**
- own factor definitions and research notes
- improve formula registry structure and factor documentation
- author factor computation logic where appropriate
- propose research-facing utilities for factor exploration or diagnostics
- surface source/field caveats that affect factor validity

**Reads:**
- `research/`
- `runtime/pi/data/`
- `strategy/calculate_alpha_score.py`
- `data/runtime/market/`
- `strategy/portfolio_rules.md`

**Writes:**
- `research/formula_registry.json`
- `research/factor_notes.md`
- `research/trusted_sources.md`
- factor-related Python logic, primarily `strategy/calculate_alpha_score.py`
- future research helpers under `research/` or training-side folders

**Must not touch directly:**
- `ledger/mock_portfolio.json`
- `data/runtime/execution/execution_log.json`
- `runtime/pi/execution/mock_portfolio_executor.py` except for narrow consultation comments or review

---

### 3) trading-backtest-validator
**Role:** backtesting, validation, and anti-overfitting

**Responsibilities:**
- own backtest methodology and validation logic
- improve metrics, evaluation workflow, and validation utilities
- author out-of-sample or robustness-testing support
- interpret backtest results conservatively
- gate promotion from research idea to strategist-usable rule

**Reads:**
- `backtests/engine/backtest_engine.py`
- `research/formula_registry.json`
- `research/factor_notes.md`
- `backtests/`
- `strategy/portfolio_rules.md`
- relevant strategy scripts where needed for validation context

**Writes:**
- `backtests/engine/backtest_engine.py`
- validation utilities under `backtests/`
- `backtests/outputs/metrics.json`
- `backtests/outputs/backtest_report.md`
- `backtests/outputs/equity_curve.csv`
- `backtests/outputs/trade_log.csv`
- `reports/backtests/backtest_verdict.md`

**Must not touch directly:**
- `ledger/mock_portfolio.json`
- `data/runtime/execution/execution_log.json`
- runtime wrapper scripts unless the change is clearly backtest-methodology related and coordinated by `trading`

---

### 4) trading-portfolio-strategist
**Role:** portfolio logic, entry/exit rules, CORE/SWING definitions, and decision logic

**Responsibilities:**
- own quality-filter logic
- own alpha-to-decision mapping
- own CORE/SWING eligibility rules and decision schemas
- author strategist output logic and portfolio rule interpretation
- define how approved research becomes decision behavior

**Reads:**
- `strategy/quality_filter.py`
- `strategy/portfolio_strategist.py`
- `strategy/sentry_monitor.py`
- `strategy/portfolio_rules.md`
- `data/runtime/strategy/alpha_rankings.json`
- `data/runtime/strategy/qualified_universe.json`
- `data/runtime/strategy/sentry_events.json`
- `data/runtime/market/price_snapshot.json`
- `data/runtime/market/fundamental_snapshot.json`
- `ledger/mock_portfolio.json` (read-only for position awareness)
- `reports/backtests/backtest_verdict.md`

**Writes:**
- `strategy/quality_filter.py`
- `strategy/portfolio_strategist.py`
- `strategy/sentry_monitor.py`
- `strategy/portfolio_rules.md`
- `data/runtime/strategy/strategist_decisions.json`

**Must not touch directly:**
- `ledger/mock_portfolio.json`
- `data/runtime/execution/execution_log.json`
- `runtime/pi/execution/mock_portfolio_executor.py` except for narrow schema coordination reviewed by `trading`

---

### 5) trading-executor-reporter
**Role:** execution, reporting, and runtime status only

**Responsibilities:**
- own mock execution logic
- own ledger update helpers and execution logging behavior
- own status/report formatting and alert/report utilities
- stay narrow and efficient

**Reads:**
- `runtime/pi/execution/mock_portfolio_executor.py`
- `runtime/pi/reporting/daily_report.py`
- `runtime/pi/execution/portfolio_status.py`
- `runtime/pi/reporting/trade_alerts.py`
- `ledger/mock_portfolio.json`
- `data/runtime/strategy/strategist_decisions.json`
- `data/runtime/market/price_snapshot.json`
- `data/runtime/execution/execution_log.json`
- `data/runtime/strategy/sentry_events.json`
- `data/runtime/strategy/alpha_rankings.json`
- `reports/templates/daily_summary_template.md`

**Writes:**
- `runtime/pi/execution/mock_portfolio_executor.py`
- `runtime/pi/reporting/daily_report.py`
- `runtime/pi/execution/portfolio_status.py`
- `runtime/pi/reporting/trade_alerts.py`
- `ledger/mock_portfolio.json` (runtime mutation only via executor)
- `data/runtime/execution/execution_log.json`
- `data/runtime/alerts/trade_alerts_latest.json`
- `data/runtime/alerts/trade_alerts_latest.txt`
- `reports/daily/daily_summary_YYYY-MM-DD.md`

**Must not touch directly:**
- `research/formula_registry.json`
- `research/factor_notes.md`
- `strategy/calculate_alpha_score.py`
- `backtests/engine/backtest_engine.py`
- `strategy/quality_filter.py`
- `strategy/portfolio_strategist.py`
- broad architectural config except narrow execution/reporting notes

## Boundary summary
- `trading` = orchestrator + runtime integration owner
- `trading-quant-researcher` = `research/` + factor logic
- `trading-backtest-validator` = `backtests/` + validation outputs
- `trading-portfolio-strategist` = `strategy/` + strategist runtime artifacts
- `trading-executor-reporter` = `runtime/pi/execution/`, `runtime/pi/reporting/`, and ledger mutation path

## Non-negotiable rule
- No agent may bypass the deterministic ledger boundary.
- `runtime/pi/execution/mock_portfolio_executor.py` remains the only portfolio-state mutator.
