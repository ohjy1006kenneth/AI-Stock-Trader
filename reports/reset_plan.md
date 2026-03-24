# Trading Project Controlled Reset

## Goal
Simplify the trading project from an over-scaffolded multi-agent layout into a smaller quant workflow:
- `trading` = orchestrator + canonical runtime owner
- Quant Researcher
- Backtest Validator
- Portfolio Strategist
- Executor / Reporter

The reset preserves working infrastructure while removing or archiving redundant scaffolding.

## Keep / archive / rewrite table

| Item | Action | Reason |
|---|---|---|
| `scripts/mock_portfolio_executor.py` | Keep | Strong deterministic ledger boundary |
| `ledger/mock_portfolio.json` | Keep | Canonical mock ledger |
| `scripts/portfolio_status.py` | Keep | Useful operational visibility |
| `scripts/run_pipeline.sh` and wrappers | Keep | Working runtime infrastructure |
| `scripts/preflight_check.py` | Keep | Useful runtime guardrail |
| `scripts/quality_filter.py` | Keep and refine | CORE filter is part of quant core |
| `scripts/calculate_alpha_score.py` | Keep and refine | Alpha logic belongs in quant core |
| `scripts/backtest_engine.py` | Keep and refine | Base for serious backtesting |
| `research/formula_registry.json` | Keep and refine | Canonical formula registry |
| `research/factor_notes.md` | Keep and refine | Human-readable research layer |
| `config/portfolio_rules.md` | Keep and refine | Canonical strategy rule doc |
| `config/architecture.md` | Rewrite | Needed to match simplified 4-role design |
| `config/agent_registry.md` | Archive | Old multi-agent scaffold |
| `config/delegation_map.json` | Archive | Too tied to old many-role structure |
| `config/persistent_agents.md` | Archive | Old specialist inventory no longer matches active design |
| `prompts/data_guardian.md` | Archive | Folded into simpler design |
| `prompts/code_maintainer.md` | Archive | Folded into simpler design |
| `prompts/daily_reporter.md` | Archive | Folded into Executor / Reporter |
| external workspace `trading-orchestrator` | Archive | `trading` is now the orchestrator |
| external workspace `trading-data-guardian` | Archive | responsibility folded into simpler roles |
| external workspace `trading-code-maintainer` | Archive | responsibility folded into simpler roles |
| external workspace `trading-daily-reporter` | Archive | responsibility folded into Executor / Reporter |
| `BOOTSTRAP.md` | Archive | first-run scaffolding, no longer useful |
| `reports/daily_summary_2026-03-24.md` | Archive | old generated artifact not needed in live root |
| `scripts/__pycache__/` | Delete | dead generated cache |

## Final simplified agent architecture
- `trading` = orchestrator + canonical runtime owner
- `trading-quant-researcher` = Quant Researcher
- `trading-backtest-validator` = Backtest Validator
- `trading-portfolio-strategist` = Portfolio Strategist
- `trading-executor-reporter` = Executor / Reporter

## Final cleaned file structure

### Research
- `research/formula_registry.json`
- `research/factor_notes.md`
- `research/trusted_sources.md`

### Backtest
- `scripts/backtest_engine.py`
- `backtests/metrics.json`
- `backtests/backtest_report.md`
- `backtests/equity_curve.csv`
- `backtests/trade_log.csv`
- `reports/backtest_verdict.md`

### Strategy
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

### Execution / Reporting
- `scripts/mock_portfolio_executor.py`
- `scripts/portfolio_status.py`
- `scripts/trade_alerts.py`
- `scripts/daily_report.py`
- `scripts/preflight_check.py`
- runtime wrapper scripts
- `ledger/mock_portfolio.json`
- `outputs/execution_log.json`
- `outputs/trade_alerts_latest.json`
- `reports/current_pipeline_explanation.md`
- `reports/quality_filter_diagnosis.md`
- `reports/daily_summary_TEMPLATE.md`

## What was actually archived or removed

### Archived inside canonical repo
- `archive/20260324_simplify/BOOTSTRAP.md`
- `archive/20260324_simplify/config/agent_registry.md`
- `archive/20260324_simplify/config/delegation_map.json`
- `archive/20260324_simplify/config/persistent_agents.md`
- `archive/20260324_simplify/prompts/code_maintainer.md`
- `archive/20260324_simplify/prompts/daily_reporter.md`
- `archive/20260324_simplify/prompts/data_guardian.md`
- `archive/20260324_simplify/reports/daily_summary_2026-03-24.md`

### Archived outside canonical repo
- `/home/node/.openclaw/archived-trading-workspaces/20260324_simplify/workspace-trading-orchestrator`
- `/home/node/.openclaw/archived-trading-workspaces/20260324_simplify/workspace-trading-data-guardian`
- `/home/node/.openclaw/archived-trading-workspaces/20260324_simplify/workspace-trading-code-maintainer`
- `/home/node/.openclaw/archived-trading-workspaces/20260324_simplify/workspace-trading-daily-reporter`

### Removed
- `scripts/__pycache__/` bytecode cache

## Preserved parts of the old system
- deterministic executor boundary
- mock ledger
- portfolio status tool
- runtime wrappers and preflight flow
- improved quality filter
- improved formula registry
- backtest engine as base infrastructure
- deterministic file flow
- CORE / SWING structure

## Plain-English workflow
1. The Quant Researcher documents factors and rule ideas.
2. Deterministic Python calculates the signals and screens.
3. The Backtest Validator decides whether those ideas look useful or overfit.
4. The Portfolio Strategist converts approved outputs into CORE/SWING actions.
5. The Executor / Reporter updates the mock ledger and reports what happened.
6. `trading` supervises everything directly.
