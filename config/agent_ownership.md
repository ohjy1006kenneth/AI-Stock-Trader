# Agent Ownership Rules

This file defines the active ownership model for the trading system.

## Global rules
- `trading` is the only canonical runtime and artifact-owning workspace.
- Specialists work inside their domains but must not become second sources of truth.
- Repeated calculations stay in deterministic Python.
- LLMs do not go into the numeric hot path.
- `trading-executor-reporter` remains the only runtime ledger mutator through `scripts/mock_portfolio_executor.py`.
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
- `scripts/run_pipeline.sh`
- `scripts/run_preflight_alert.sh`
- `scripts/run_trade_alerts.sh`
- `scripts/run_daily_summary.sh`
- `scripts/preflight_check.py`
- cross-cutting integration glue and orchestration-facing docs

**When trading should take over:**
- a change spans research + backtest + strategy + execution boundaries
- multiple specialists disagree or produce conflicting changes
- execution/reporting work is too complex for the lightweight executor/reporter
- runtime, cron, or integration behavior must change

---

### 2) trading-quant-researcher
**Role:** formula research, factor design, and research documentation

**Default model:** `openai-codex/gpt-5.4`

**Responsibilities:**
- own factor definitions and research notes
- improve formula registry structure and factor documentation
- author factor computation logic where appropriate
- propose research-facing utilities for factor exploration or diagnostics
- surface source/field caveats that affect factor validity

**Reads:**
- `research/`
- `scripts/calculate_alpha_score.py`
- `scripts/build_universe.py`
- `scripts/fetch_price_data.py`
- `scripts/fetch_fundamental_data.py`
- `outputs/price_snapshot.json`
- `outputs/fundamental_snapshot.json`
- `config/portfolio_rules.md`

**Writes:**
- `research/formula_registry.json`
- `research/factor_notes.md`
- `research/trusted_sources.md`
- `reports/research_update.md`
- factor-related Python logic, primarily `scripts/calculate_alpha_score.py`
- research-facing helper scripts if added later

**Must not touch directly:**
- `ledger/mock_portfolio.json`
- `outputs/execution_log.json`
- `scripts/mock_portfolio_executor.py` except for narrow consultation comments or review

**Conceptual script ownership:**
- `scripts/calculate_alpha_score.py`
- research-facing utilities if added later

**When trading should override or integrate:**
- a factor change also requires broad strategy-rule or runtime integration work
- a research code change ripples into backtest, strategist, and execution layers simultaneously

---

### 3) trading-backtest-validator
**Role:** backtesting, validation, and anti-overfitting

**Default model:** `openai-codex/gpt-5.4`

**Responsibilities:**
- own backtest methodology and validation logic
- improve metrics, evaluation workflow, and validation utilities
- author out-of-sample or robustness-testing support
- interpret backtest results conservatively
- gate promotion from research idea to strategist-usable rule

**Reads:**
- `scripts/backtest_engine.py`
- `research/formula_registry.json`
- `research/factor_notes.md`
- `backtests/`
- `reports/research_update.md`
- `config/portfolio_rules.md`
- relevant strategy scripts where needed for validation context

**Writes:**
- `scripts/backtest_engine.py`
- validation utilities under `scripts/` or `backtests/` if added later
- `backtests/metrics.json`
- `backtests/backtest_report.md`
- `backtests/equity_curve.csv`
- `backtests/trade_log.csv`
- `reports/backtest_verdict.md`

**Must not touch directly:**
- `ledger/mock_portfolio.json`
- `outputs/execution_log.json`
- runtime wrapper scripts unless the change is clearly backtest-methodology related and coordinated by `trading`

**Conceptual script ownership:**
- `scripts/backtest_engine.py`
- validation/metrics helper code added later

**When trading should override or integrate:**
- validation changes require cross-cutting runtime or strategist integration
- backtest methodology changes must be merged with broader system-wide updates

---

### 4) trading-portfolio-strategist
**Role:** portfolio logic, entry/exit rules, CORE/SWING definitions, and decision logic

**Default model:** `openai-codex/gpt-5.4`

**Responsibilities:**
- own quality-filter logic
- own alpha-to-decision mapping
- own CORE/SWING eligibility rules and decision schemas
- author strategist output logic and portfolio rule interpretation
- define how approved research becomes decision behavior

**Reads:**
- `scripts/quality_filter.py`
- `scripts/portfolio_strategist.py`
- `scripts/sentry_monitor.py`
- `config/portfolio_rules.md`
- `outputs/alpha_rankings.json`
- `outputs/qualified_universe.json`
- `outputs/sentry_events.json`
- `outputs/price_snapshot.json`
- `outputs/fundamental_snapshot.json`
- `ledger/mock_portfolio.json` (read-only for position awareness)
- `reports/backtest_verdict.md`

**Writes:**
- `scripts/quality_filter.py`
- `scripts/portfolio_strategist.py`
- `config/portfolio_rules.md`
- `reports/strategist_note.md`
- `outputs/strategist_decisions.json`

**Must not touch directly:**
- `ledger/mock_portfolio.json`
- `outputs/execution_log.json`
- `scripts/mock_portfolio_executor.py` except for narrow schema coordination reviewed by `trading`

**Conceptual script ownership:**
- `scripts/quality_filter.py`
- `scripts/portfolio_strategist.py`
- decision-schema logic tightly tied to strategist outputs

**When trading should override or integrate:**
- strategy-rule changes require simultaneous research, backtest, and runtime integration
- strategist changes affect executor contracts or broader orchestration logic

---

### 5) trading-executor-reporter
**Role:** execution, reporting, and runtime status only

**Default model:** `github-copilot/gpt-4o`

**Responsibilities:**
- own mock execution logic
- own ledger update helpers and execution logging behavior
- own status/report formatting and alert/report utilities
- stay narrow and efficient

**Reads:**
- `scripts/mock_portfolio_executor.py`
- `scripts/daily_report.py`
- `scripts/portfolio_status.py`
- `scripts/trade_alerts.py`
- `ledger/mock_portfolio.json`
- `outputs/strategist_decisions.json`
- `outputs/price_snapshot.json`
- `outputs/execution_log.json`
- `outputs/sentry_events.json`
- `outputs/alpha_rankings.json`
- `reports/daily_summary_TEMPLATE.md`

**Writes:**
- `scripts/mock_portfolio_executor.py`
- `scripts/daily_report.py`
- `scripts/portfolio_status.py`
- `scripts/trade_alerts.py`
- `ledger/mock_portfolio.json` (runtime mutation only via executor)
- `outputs/execution_log.json`
- `outputs/trade_alerts_latest.json`
- `outputs/trade_alerts_latest.txt`
- `reports/daily_summary_YYYY-MM-DD.md`
- execution/report formatting helpers if added later

**Must not touch directly:**
- `research/formula_registry.json`
- `research/factor_notes.md`
- `scripts/calculate_alpha_score.py`
- `scripts/backtest_engine.py`
- `scripts/quality_filter.py`
- `scripts/portfolio_strategist.py`
- broad architectural docs except narrow execution/reporting notes

**Conceptual script ownership:**
- `scripts/mock_portfolio_executor.py`
- `scripts/daily_report.py`
- `scripts/portfolio_status.py`
- `scripts/trade_alerts.py`

**When trading should override or integrate:**
- execution/reporting work is cross-cutting, architectural, or materially complex
- a change reaches into research, backtest, or strategist logic
- runtime and integration constraints dominate over local execution concerns

## File-to-agent mapping

### Research / factor side
- `research/formula_registry.json` -> `trading-quant-researcher`
- `research/factor_notes.md` -> `trading-quant-researcher`
- `scripts/calculate_alpha_score.py` -> `trading-quant-researcher`

### Backtest / validation side
- `scripts/backtest_engine.py` -> `trading-backtest-validator`
- `backtests/metrics.json` -> `trading-backtest-validator`
- `backtests/backtest_report.md` -> `trading-backtest-validator`
- `reports/backtest_verdict.md` -> `trading-backtest-validator`

### Strategy side
- `scripts/quality_filter.py` -> `trading-portfolio-strategist`
- `scripts/portfolio_strategist.py` -> `trading-portfolio-strategist`
- `outputs/strategist_decisions.json` -> `trading-portfolio-strategist`
- `reports/strategist_note.md` -> `trading-portfolio-strategist`

### Execution / reporting side
- `scripts/mock_portfolio_executor.py` -> `trading-executor-reporter`
- `scripts/daily_report.py` -> `trading-executor-reporter`
- `scripts/portfolio_status.py` -> `trading-executor-reporter`
- `scripts/trade_alerts.py` -> `trading-executor-reporter`
- `ledger/mock_portfolio.json` -> runtime-owned by `trading-executor-reporter`
- `outputs/execution_log.json` -> `trading-executor-reporter`
- `reports/daily_summary_YYYY-MM-DD.md` -> `trading-executor-reporter`

### Orchestrator / integration side
- `config/architecture.md` -> `trading`
- `config/pipeline_timing.md` -> `trading`
- `config/commands.md` -> `trading`
- `config/delegation_map.json` -> archived / inactive legacy artifact
- integration glue and cross-cutting coordination logic -> `trading`

## Code-writing policy
- Specialists should write the code inside their specialty.
- `trading` coordinates, reviews integration points, and preserves the canonical source of truth.
- `trading-executor-reporter` is lightweight and should not be the default author of complex system-wide code.
- If execution/reporting changes are simple and local, `trading-executor-reporter` may own them directly.
- If execution/reporting changes are complex, architectural, or cross-cutting, `trading` should implement or integrate them instead.
- No specialist may bypass the deterministic ledger boundary.
- No specialist may create a second source of truth outside the canonical trading workspace.
