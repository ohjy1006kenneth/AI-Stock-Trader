# Commands

## Portfolio status

Quick read-only inspection of the mock portfolio:

```bash
.venv/bin/python runtime/pi/execution/portfolio_status.py
```

Options:

```bash
.venv/bin/python runtime/pi/execution/portfolio_status.py --json
.venv/bin/python runtime/pi/execution/portfolio_status.py --summary
.venv/bin/python runtime/pi/execution/portfolio_status.py --positions
```

Reads:
- `ledger/mock_portfolio.json`
- `data/runtime/market/price_snapshot.json`

Behavior:
- prefers latest snapshot prices for mark-to-market display
- falls back to stored `last_price` values if snapshot pricing is unavailable
- never mutates the ledger

## Automation verification

```bash
openclaw cron status
openclaw cron list
openclaw cron runs --limit 20
```

## Runtime setup verification

```bash
.venv/bin/python runtime/pi/preflight/preflight_check.py
./runtime/pi/wrappers/run_preflight_alert.sh
```

Rule:
- All cron jobs must execute using the project virtual environment at `.venv/bin/python`.
- Never depend on ambient system Python for project jobs.
- Never hardcode container-only or host-only absolute paths if a repo-relative path can be used.

## Useful inspection commands

```bash
.venv/bin/python runtime/pi/execution/portfolio_status.py
cat data/runtime/execution/execution_log.json
ls -1t reports/daily/daily_summary_*.md | head -n 1
ls -1t reports/pipeline/pipeline_run_summary_*.md | head -n 1
openclaw cron list
```
