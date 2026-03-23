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
```
