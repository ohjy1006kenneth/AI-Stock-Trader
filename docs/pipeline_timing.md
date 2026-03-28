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
- Script: `runtime/pi/wrappers/run_pipeline.sh`
- Purpose:
  - run the single end-to-end after-close workflow
  - include preflight, deterministic trading stages, daily report generation, pipeline run summary generation, and trade alert generation
- Behavior:
  - runs the runtime preflight step first
  - aborts immediately if preflight fails
  - if preflight passes, runs universe build, data fetch, quality filter, alpha scoring, sentry checks, strategist decision generation, executor mutation, daily report generation, trade alert generation, and pipeline summary generation in one flow
  - writes a human-readable run summary to `reports/pipeline_run_summary_YYYY-MM-DD.md`
  - prints the run summary to terminal output
  - prints trade alert text only when new executed BUY/SELL records exist

### 2) Daily summary
- Weekdays
- 7:00 AM America/Chicago
- Script: `runtime/pi/wrappers/run_daily_summary.sh`
- Runtime agent: `trading`
- Purpose:
  - send morning mock portfolio summary based on deterministic artifacts
- Note:
  - the cron must execute in `trading`, because the canonical scripts and reports live in the trading workspace

## Deterministic hot path
The main pipeline runs:
- runtime data fetch/build steps
- strategy screening, ranking, and sentry steps
- strategist decision generation
- paper execution
- daily reporting
- trade alert generation

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
