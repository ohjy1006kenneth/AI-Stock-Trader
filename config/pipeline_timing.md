# Pipeline Timing

## Goals
- Fully mock-only automation
- Low token cost
- Deterministic hot path
- Alerts for executed mock BUY/SELL actions
- Daily summary every weekday at 7:00 AM America/Chicago
- Preflight gate before the main pipeline

## Final production-safe cron flow

Delegation targets in the conceptual system:
- runtime data/schema triage and factor/formula work -> `trading-quant-researcher`
- backtest/rule validation -> `trading-backtest-validator`
- structured decision generation -> `trading-portfolio-strategist`
- mock ledger mutation validation/execution and summary wording -> `trading-executor-reporter`

The cron-triggered hot path still runs inside `trading`, which remains the canonical artifact-owning runtime workspace.

### 1) Main trading pipeline
- Weekdays
- 6:10 PM America/Chicago
- Script: `scripts/run_pipeline.sh`
- Purpose:
  - run preflight
  - run the deterministic after-close pipeline
  - generate and dispatch mock BUY/SELL alerts only after the executor completes successfully
- Behavior:
  - runs `scripts/preflight_check.py` first
  - aborts immediately if preflight fails and returns the failure text
  - runs `scripts/trade_alerts.py` at the end of the pipeline
  - emits alert text only when new executed BUY/SELL records exist
  - otherwise returns `PIPELINE OK`

### 2) Daily summary
- Weekdays
- 7:00 AM America/Chicago
- Script: `scripts/run_daily_summary.sh`
- Runtime agent: `trading`
- Purpose:
  - send morning mock portfolio summary based on deterministic artifacts
- Note:
  - the cron must execute in `trading`, because the canonical scripts and reports live in the trading workspace

## Deterministic hot path
The main pipeline runs:
- `scripts/build_universe.py`
- `scripts/fetch_price_data.py`
- `scripts/fetch_fundamental_data.py`
- `scripts/quality_filter.py`
- `scripts/calculate_alpha_score.py`
- `scripts/sentry_monitor.py`
- `scripts/portfolio_strategist.py`
- `scripts/mock_portfolio_executor.py`
- `scripts/daily_report.py`
- `scripts/trade_alerts.py`

No LLM is used for repeated math, monitoring, or ledger mutation.

## Runtime note
- All cron jobs must execute using the project virtual environment at `.venv/bin/python`.
- Never depend on ambient system Python for project jobs.
- Never hardcode container-only or host-only absolute paths if a repo-relative path can be used.
- Wrapper scripts resolve repo root dynamically and then execute Python via `$ROOT_DIR/.venv/bin/python`.

## Delivery target
- channel: `telegram`
- target: `-1003845783711:topic:7`
- timezone: `America/Chicago`
