# Multi-Agent Architecture

## Design Principles
- Deterministic Python owns repeated calculations and monitoring.
- LLMs are event-driven and never used in numeric loops.
- Strategy behavior is versioned and must not change silently.
- New factors require explicit research, backtest validation, and approval.

## System Components

### Deterministic Layer
Scripts produce stable, inspectable outputs:
- `scripts/build_universe.py`
- `scripts/fetch_price_data.py`
- `scripts/fetch_fundamental_data.py`
- `scripts/quality_filter.py`
- `scripts/calculate_alpha_score.py`
- `scripts/portfolio_strategist.py`
- `scripts/backtest_engine.py`
- `scripts/sentry_monitor.py`
- `scripts/apply_mock_decisions.py`
- `scripts/mark_to_market.py`
- `scripts/daily_report.py`

### Agent Layer

#### 1) Data Guardian
- Purpose: data health, schema validation, broken pipeline detection
- Default model: GitHub Copilot Claude Haiku
- Reads: `logs/`, `data/`, `outputs/`, `config/data_contracts.json`
- Writes: `reports/data_guardian_note.md`, `outputs/data_health.json`
- Wake events:
  - missing fields
  - stale data
  - parse failures
  - symbol mapping issues
- Escalate to GPT-5.4 when schema breakage is complex or code changes are needed

#### 2) Scholar Researcher
- Purpose: extract exact factor definitions from trusted sources
- Default model: GPT-5.4
- Reads: `research/trusted_sources.md`, papers/notes, current registry
- Writes: `research/formula_registry.json`, `research/factor_notes.md`, `reports/research_update.md`
- Wake events:
  - explicit research request
  - new paper / source evaluation
  - factor registry update request
- Never runs in monitoring loops

#### 3) Backtest Validator
- Purpose: validate ideas and changes before portfolio impact
- Default model: GPT-5.4
- Reads: `backtests/metrics.json`, `backtests/backtest_report.md`, code diffs, registry
- Writes: `reports/backtest_verdict.md`
- Wake events:
  - proposed factor update
  - rule change request
  - suspicious backtest result
- Must approve before production strategy changes

#### 4) Code Maintainer
- Purpose: safe code updates and tests
- Default model: GPT-5.4 for important changes, Haiku for small cleanup
- Reads: `scripts/`, tests, validator verdicts, maintenance notes
- Writes: code, tests, `reports/code_change_note.md`
- Wake events:
  - API breakage
  - approved strategy change
  - test failures
- Cannot promote strategy changes without Backtest Validator approval

#### 5) Strategist
- Purpose: convert approved deterministic outputs into mock portfolio actions
- Default model: Claude Haiku
- Reads: `outputs/alpha_rankings.json`, `outputs/qualified_universe.json`, `ledger/mock_portfolio.json`, `outputs/sentry_events.json`, `config/portfolio_rules.md`, approved factor registry
- Writes: `outputs/strategist_decisions.json`, `reports/strategist_note.md`
- Wake events:
  - sentry escalation
  - scheduled portfolio review
  - rule conflict
- Escalate to GPT-5.4 on ambiguous, conflicting, or high-impact decisions

#### 6) Mock Portfolio Executor
- Purpose: apply strategist decisions deterministically to the ledger
- Runtime: deterministic Python by default; no LLM in normal operation
- Reads: `outputs/strategist_decisions.json`, `ledger/mock_portfolio.json`, `data/price_history.json`, `config/portfolio_rules.md`
- Writes: `outputs/execution_log.json`, `ledger/mock_portfolio.json`
- Wake events:
  - new strategist decisions available
  - scheduled mock execution cycle
  - rule validation required
- LLM use: only for debugging execution failures, defaulting to Claude Haiku and escalating to GPT-5.4 only for persistent conflicts or schema issues

#### 7) Daily Reporter
- Purpose: concise factual EOD summary
- Default model: Claude Haiku
- Reads: ledger, outputs, logs, report template
- Writes: `reports/daily_summary.md`
- Wake events:
  - end of day
  - explicit status request
- Escalate only for deep explanation requests

## Event Flow
1. Deterministic scripts update universe, prices, fundamentals, quality screen, alpha scores.
2. `sentry_monitor.py` checks practical triggers.
3. If no issue/event, no LLM wake-up is required.
4. If event exists:
   - Data Guardian for data-quality issues
   - Strategist for structured portfolio action review
   - Mock Portfolio Executor applies only the approved structured decisions deterministically
5. Research and code changes follow explicit proposal -> validation -> promotion flow.
