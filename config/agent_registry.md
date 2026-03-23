# Trading Agent Registry

## Workspace owner
- `trading`
- Owns scripts, outputs, ledger, reports, and cron-triggered execution.

## Supervisor
- `trading-orchestrator`
- Delegates to the specialized trading agents and coordinates workflows.
- Does not act as the general artifact store.

## Specialized agents
- `trading-data-guardian`
- `trading-scholar`
- `trading-backtest-validator`
- `trading-code-maintainer`
- `trading-strategist`
- `trading-executor`
- `trading-daily-reporter`
