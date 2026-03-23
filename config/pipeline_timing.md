# Pipeline Timing

## Goals
- Fully mock-only automation
- Low token cost
- Deterministic hot path
- Alerts for executed mock BUY/SELL actions
- Daily summary every weekday at 7:00 AM America/Chicago

## Schedules

### 1) Main trading pipeline
- Weekdays
- 6:10 PM America/Chicago
- Purpose: run the deterministic after-close pipeline

### 2) Trade alert dispatch
- Weekdays
- 6:11 PM America/Chicago
- Purpose: read deterministic execution log and send any new BUY/SELL alerts

### 3) Daily summary
- Weekdays
- 7:00 AM America/Chicago
- Purpose: send concise morning account summary based on ledger and latest outputs

## Deterministic hot path
The scheduled pipeline runs:
- `scripts/build_universe.py`
- `scripts/fetch_price_data.py`
- `scripts/fetch_fundamental_data.py`
- `scripts/quality_filter.py`
- `scripts/calculate_alpha_score.py`
- `scripts/sentry_monitor.py`
- `scripts/portfolio_strategist.py`
- `scripts/mock_portfolio_executor.py`
- `scripts/daily_report.py`

No LLM is used for repeated math, monitoring, or ledger mutation.

## Alerting
Trade alerts are generated deterministically by:
- `scripts/trade_alerts.py`

The script reads:
- `outputs/execution_log.json`
- `ledger/mock_portfolio.json`

And writes:
- `outputs/trade_alerts_latest.json`
- `outputs/trade_alerts_latest.txt`
- `data/alert_state.json`

## Delivery target
Current configured chat target for cron delivery:
- channel: `telegram`
- target: `-1003845783711:topic:7`
