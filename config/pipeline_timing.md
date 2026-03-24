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
- runtime data and schema issues -> `trading-data-guardian`
- factor/formula work -> `trading-scholar`
- backtest/rule validation -> `trading-backtest-validator`
- code maintenance and runtime fixes -> `trading-code-maintainer`
- structured decision generation -> `trading-strategist`
- mock ledger mutation validation/execution -> `trading-executor`
- summary wording -> `trading-daily-reporter`

The cron-triggered hot path still runs inside `trading`, which remains the canonical artifact-owning runtime workspace.

### 1) Preflight alert
- Weekdays
- 6:09 PM America/Chicago
- Script: `scripts/run_preflight_alert.sh`
- Purpose:
  - verify runtime before the main pipeline
  - check Python package availability
  - check required files/folders
  - check JSON readability
  - check Telegram target configuration
- Delivery:
  - sends an alert only if preflight fails

### 2) Main trading pipeline
- Weekdays
- 6:10 PM America/Chicago
- Script: `scripts/run_pipeline.sh`
- Purpose:
  - run the deterministic after-close pipeline
- Behavior:
  - runs `scripts/preflight_check.py` first
  - aborts immediately if preflight fails
  - does not announce normal success output

### 3) Trade alert dispatch
- Weekdays
- 6:11 PM America/Chicago
- Script: `scripts/run_trade_alerts.sh`
- Purpose:
  - read deterministic execution results
  - send only new executed mock BUY/SELL alerts

### 4) Daily summary
- Weekdays
- 7:00 AM America/Chicago
- Script: `scripts/run_daily_summary.sh`
- Purpose:
  - send morning mock portfolio summary based on deterministic artifacts

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
