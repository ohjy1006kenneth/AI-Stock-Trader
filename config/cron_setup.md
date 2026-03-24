# Cron Setup

## Delivery target
- Channel: `telegram`
- Destination: `-1003845783711:topic:7`
- Timezone: `America/Chicago`

## Production-safe jobs
1. `trading-pipeline-after-close`
   - `10 18 * * 1-5`
   - runs preflight + deterministic pipeline + end-of-pipeline trade alerts
2. `trading-daily-summary-7am`
   - `0 7 * * 1-5`
   - runs in `trading` because the canonical scripts and reports live there

## Verification commands
```bash
openclaw cron status
openclaw cron list
openclaw cron runs --limit 20
```

## Notes
- `trading` owns cron-triggered execution and canonical runtime artifacts.
- `trading` is the supervisor/dispatcher and canonical runtime owner.
- Specialist delegation targets are `trading-quant-researcher`, `trading-backtest-validator`, `trading-portfolio-strategist`, and `trading-executor-reporter`.
- All cron jobs must execute using the project virtual environment at `.venv/bin/python`.
- Never depend on ambient system Python for project jobs.
- Never hardcode container-only or host-only absolute paths if a repo-relative path can be used.
- The preflight alert job announces only on failure.
- The main pipeline job is gated by `scripts/preflight_check.py`.
- The main pipeline job also generates and dispatches trade alerts after the executor completes successfully.
- The separate trade-alert cron job is no longer needed.
- The daily summary job announces the latest dated summary report.
