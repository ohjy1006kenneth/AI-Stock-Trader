# Commands

## Portfolio status

Quick read-only inspection of the mock portfolio:

```bash
python3 scripts/portfolio_status.py
```

Options:

```bash
python3 scripts/portfolio_status.py --json
python3 scripts/portfolio_status.py --summary
python3 scripts/portfolio_status.py --positions
```

Reads:
- `ledger/mock_portfolio.json`
- `outputs/price_snapshot.json`

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
.venv/bin/python scripts/preflight_check.py
./scripts/run_preflight_alert.sh
```

Rule:
- All cron jobs must execute using the project virtual environment at `.venv/bin/python`.
- Never depend on ambient system Python for project jobs.
- Never hardcode container-only or host-only absolute paths if a repo-relative path can be used.

## Useful inspection commands

```bash
python3 scripts/portfolio_status.py
cat outputs/execution_log.json
ls -1t reports/daily_summary_*.md | head -n 1
openclaw cron list
```
