# Cron Setup

## Delivery target
- Channel: `telegram`
- Destination: `-1003845783711:topic:7`
- Timezone: `America/Chicago`

## Production-safe jobs
1. `trading-preflight-alert`
   - `9 18 * * 1-5`
2. `trading-pipeline-after-close`
   - `10 18 * * 1-5`
3. `trading-mock-trade-alerts`
   - `11 18 * * 1-5`
4. `trading-daily-summary-7am`
   - `0 7 * * 1-5`

## Verification commands
```bash
openclaw cron status
openclaw cron list
openclaw cron runs --limit 20
```

## Notes
- `trading` owns cron-triggered execution and canonical runtime artifacts.
- `trading-orchestrator` is the conceptual supervisor/dispatcher.
- Specialist delegation targets are `trading-data-guardian`, `trading-scholar`, `trading-backtest-validator`, `trading-code-maintainer`, `trading-strategist`, `trading-executor`, and `trading-daily-reporter`.
- The preflight alert job announces only on failure.
- The main pipeline job is gated by `scripts/preflight_check.py`.
- The trade alert job announces only when new executed BUY/SELL records exist.
- The daily summary job announces the latest dated summary report.
