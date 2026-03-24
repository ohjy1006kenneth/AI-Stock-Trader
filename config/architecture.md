# Trading Architecture

## Design principles to preserve
- Repeated calculations, ranking, monitoring, backtesting, and ledger mutation remain deterministic Python tasks.
- LLMs are event-driven and never used inside monitoring loops or numeric hot paths.
- Strategy logic is versioned and should change only through documented research, backtesting, and promotion.
- The ledger mutation boundary is strict: only the deterministic Mock Portfolio Executor may update `ledger/mock_portfolio.json`.
- `trading` remains the only canonical artifact-owning workspace.
- `trading-orchestrator` supervises and dispatches; it is not the general artifact store.
- `trading-*` specialist agents are role workspaces only and must never become a second source of truth.

## Keep vs refactor

### Infrastructure to preserve
These are the stable bones of the system and should only be changed if clearly broken.

- deterministic executor boundary
- `scripts/mock_portfolio_executor.py`
- `ledger/mock_portfolio.json`
- `scripts/portfolio_status.py`
- cron/runtime wrappers and preflight scripts
- reporting structure in `reports/`
- delegation map and role architecture docs
- deterministic file flow contract
- strategist -> executor mutation boundary
- canonical `trading` workspace ownership

### Quant core to refactor selectively
These are the parts that should evolve without deleting the runtime.

- `scripts/quality_filter.py`
- `scripts/calculate_alpha_score.py`
- `scripts/backtest_engine.py`
- `research/formula_registry.json`
- `research/factor_notes.md`
- CORE/SWING thresholds and rule definitions
- research -> validation -> promotion workflow
- factor documentation and promotion criteria

### Optional cleanup
Safe, non-urgent cleanup that should not drive architecture decisions.

- stale report phrasing
- duplicated notes across docs
- placeholder registry metadata
- low-value prompt wording
- non-canonical artifacts that can be regenerated

## Runtime role map
Agent IDs used in OpenClaw for the trading system:
- workspace/runtime owner: `trading`
- supervisor: `trading-orchestrator`
- specialist agents: `trading-data-guardian`, `trading-scholar`, `trading-backtest-validator`, `trading-code-maintainer`, `trading-strategist`, `trading-executor`, `trading-daily-reporter`

## Simplified conceptual quant workflow
The system should be understood as four main quant roles, even though several specialist workspaces still exist underneath.

### 1) Quant Researcher
Primary conceptual job:
- define, document, and refine formulas and rule candidates

Mapped agents:
- `trading-scholar`
- `trading-data-guardian` for data sanity and source caveats
- `trading-code-maintainer` when deterministic implementation is needed

What this role owns:
- factor definitions
- formula registry
- source caveats
- candidate rule changes

### 2) Backtest Validator
Primary conceptual job:
- decide whether a formula or rule deserves trust

Mapped agents:
- `trading-backtest-validator`

What this role owns:
- realism review
- in-sample vs out-of-sample discipline
- bias control review
- promotion gating

### 3) Portfolio Strategist
Primary conceptual job:
- translate approved deterministic research outputs into actual portfolio decisions

Mapped agents:
- `trading-strategist`
- `trading-orchestrator` when cross-role coordination or conflict resolution is needed

What this role owns:
- BUY/SELL/HOLD/REVIEW decisions
- CORE vs SWING interpretation
- decision notes

### 4) Executor / Reporter
Primary conceptual job:
- apply decisions deterministically and communicate the result

Mapped agents:
- `trading-executor`
- `trading-daily-reporter`
- runtime owner `trading`

What this role owns:
- ledger mutation
- execution logging
- daily summaries
- cron/runtime operation

## Plain-English workflow
1. A factor or rule idea is proposed and documented by the Quant Researcher.
2. Deterministic code implements that idea without putting LLMs into the hot path.
3. The Backtest Validator checks whether the change is realistic, biased, overfit, or still provisional.
4. Only approved deterministic outputs flow into the Portfolio Strategist.
5. The Portfolio Strategist writes decisions but never mutates the ledger.
6. The Executor / Reporter layer applies valid decisions, updates the mock ledger, and publishes summaries.

This keeps the runtime stable while letting the quant brain mature.

## Agent detail and model posture

### Data Guardian
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
- `github-copilot/gpt-4.1`

Escalation:
- `openai-codex/gpt-5.4` only for persistent schema mismatch, repeated corruption, or cross-file debugging.

### Scholar Researcher
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
- `openai-codex/gpt-5.4`

### Backtest Validator
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
- `openai-codex/gpt-5.4`

### Code Maintainer
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
- `openai-codex/gpt-5.4` for important changes, lighter models acceptable for low-risk cleanup.

### Strategist
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
- `openai-codex/gpt-5.4`

Why upgraded:
- strategist sits at the boundary between research outputs and actual portfolio intent
- stronger reasoning is justified here even though the hot path remains deterministic Python
- ledger mutation still remains outside the strategist

### Mock Portfolio Executor
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
- deterministic Python only in normal operation

LLM use:
- only for debugging or explaining failures; operational hot path stays non-LLM

### Daily Reporter
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
- `github-copilot/gpt-4.1`

## Deterministic file flow contract
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

## Research -> promotion workflow
1. Propose factor or rule in the research layer.
2. Document exact formula, fields, lag rules, and caveats in `research/formula_registry.json` and `research/factor_notes.md`.
3. Implement deterministic Python logic.
4. Backtest on historical data with explicit assumptions and costs.
5. Validate in-sample vs out-of-sample behavior and bias controls.
6. Reject, revise, or promote.
7. Only promoted logic influences production decision rules.
