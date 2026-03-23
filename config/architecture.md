# 7-Role Multi-Agent Architecture

## Design Principles
- Repeated calculations, ranking, monitoring, backtesting, and ledger mutation are deterministic Python tasks.
- LLMs are event-driven and never used inside monitoring or numeric loops.
- Strategy logic is versioned and must not change without validation and promotion.
- The ledger mutation boundary is strict: only the deterministic Mock Portfolio Executor may update `ledger/mock_portfolio.json`.

## Role Overview

Agent IDs used in OpenClaw for the trading system:
- workspace/runtime owner: `trading`
- supervisor: `trading-orchestrator`
- specialist agents: `trading-data-guardian`, `trading-scholar`, `trading-backtest-validator`, `trading-code-maintainer`, `trading-strategist`, `trading-executor`, `trading-daily-reporter`


### 1) Data Guardian
Purpose:
- Monitor data ingestion health and schema integrity.

Responsibilities:
- Validate price and fundamental snapshots.
- Detect stale values, malformed data, missing fields, symbol mapping issues, and fallback source usage.
- Write data-quality incidents and maintenance alerts.

Reads:
- `outputs/price_snapshot.json`
- `outputs/fundamental_snapshot.json`
- `config/data_contracts.json`
- `logs/`

Writes:
- `outputs/data_quality_status.json`
- `reports/data_guardian_note.md`

Default model:
- Claude Haiku

Escalation:
- GPT-5.4 only for persistent schema mismatch, repeated data corruption, or cross-file debugging.

### 2) Scholar Researcher
Purpose:
- Extract precise formulas from trusted sources and maintain a versioned formula registry.

Responsibilities:
- Research factors, document assumptions, required fields, and implementation constraints.
- Reject unimplementable formulas.

Reads:
- `research/trusted_sources.md`
- `research/formula_registry.json`
- source papers/notes

Writes:
- `research/formula_registry.json`
- `research/factor_notes.md`
- `reports/research_update.md`

Default model:
- GPT-5.4

### 3) Backtest Validator
Purpose:
- Validate factor ideas, rule changes, and code changes before promotion.

Responsibilities:
- Review backtest realism, bias controls, OOS design, turnover, drawdown, and cost assumptions.
- Produce explicit verdicts.

Reads:
- `backtests/backtest_report.md`
- `backtests/metrics.json`
- `research/formula_registry.json`
- change notes / diffs

Writes:
- `reports/backtest_verdict.md`

Default model:
- GPT-5.4

### 4) Code Maintainer
Purpose:
- Maintain deterministic code conservatively.

Responsibilities:
- Update scripts, tests, schema handling, and version notes.
- Never silently rewrite strategy rules.

Reads:
- `scripts/`
- `reports/backtest_verdict.md`
- `reports/data_guardian_note.md`
- relevant config/research files

Writes:
- `scripts/`
- `tests/` if added later
- `reports/code_change_note.md`

Default model:
- GPT-5.4 for important changes, Claude Haiku for low-risk cleanup.

### 5) Strategist
Purpose:
- Convert approved deterministic outputs into structured portfolio decisions.

Responsibilities:
- Classify names into BUY/SELL/HOLD/REVIEW with sleeve labels.
- Read approved outputs only.
- Never mutate the ledger.

Reads:
- `outputs/alpha_rankings.json`
- `outputs/qualified_universe.json`
- `outputs/price_snapshot.json`
- `outputs/fundamental_snapshot.json`
- `outputs/sentry_events.json`
- `ledger/mock_portfolio.json`
- `config/portfolio_rules.md`

Writes:
- `outputs/strategist_decisions.json`
- `reports/strategist_note.md`

Default model:
- Claude Haiku

Escalation:
- GPT-5.4 only for rule conflict, high ambiguity, or unusually high-impact contradictions.

### 6) Mock Portfolio Executor
Purpose:
- Deterministically apply approved strategist decisions to the mock ledger.

Responsibilities:
- Validate decision schema and rule compliance.
- Reject invalid actions.
- Update cash, positions, PnL fields, and trade history.
- Write execution audit logs.

Reads:
- `outputs/strategist_decisions.json`
- `ledger/mock_portfolio.json`
- `outputs/price_snapshot.json`
- `config/portfolio_rules.md`

Writes:
- `ledger/mock_portfolio.json`
- `outputs/execution_log.json`

Runtime:
- Deterministic Python only in normal operation.

LLM use:
- Only for debugging/explaining failures, default Claude Haiku, escalate to GPT-5.4 only for persistent conflicts.

### 7) Daily Reporter
Purpose:
- Produce concise end-of-day summary after execution completes.

Responsibilities:
- Summarize portfolio state, entries/exits, stop/take-profit events, signal changes, data warnings, fallback usage, and tomorrow focus.

Reads:
- `ledger/mock_portfolio.json`
- `outputs/execution_log.json`
- `outputs/sentry_events.json`
- `outputs/alpha_rankings.json`
- `reports/daily_summary_TEMPLATE.md`

Writes:
- `reports/daily_summary_YYYY-MM-DD.md`

Default model:
- Claude Haiku

## Deterministic File Flow Contract
1. Data scripts produce:
   - `outputs/alpha_rankings.json`
   - `outputs/qualified_universe.json`
   - `outputs/price_snapshot.json`
   - `outputs/fundamental_snapshot.json`
2. Strategist reads approved outputs and writes:
   - `outputs/strategist_decisions.json`
3. Mock Portfolio Executor reads:
   - `outputs/strategist_decisions.json`
   - `ledger/mock_portfolio.json`
   - `outputs/price_snapshot.json`
4. Mock Portfolio Executor writes:
   - `ledger/mock_portfolio.json`
   - `outputs/execution_log.json`
5. Daily Reporter reads final state and writes:
   - `reports/daily_summary_YYYY-MM-DD.md`

No other component may mutate the ledger.
