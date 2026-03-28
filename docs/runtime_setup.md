# Runtime Setup

This project should run from a dedicated virtual environment to avoid host-level Python drift.

## Recommended setup

From the workspace root:

```bash
cd /home/node/.openclaw/workspace-trading
python3 -m venv .venv
. .venv/bin/activate
python -m ensurepip --upgrade || true
python -m pip install --upgrade pip wheel setuptools
python -m pip install -r requirements.txt
```

## Verify runtime

```bash
.venv/bin/python --version
.venv/bin/python -m pip show yfinance
.venv/bin/python scripts/preflight_check.py
```

## Manual pipeline run

Preferred one-shot test:

```bash
./scripts/run_pipeline.sh
```

Individual Python steps if needed:

```bash
.venv/bin/python scripts/build_universe.py
.venv/bin/python scripts/fetch_price_data.py
.venv/bin/python scripts/fetch_fundamental_data.py
.venv/bin/python scripts/quality_filter.py
.venv/bin/python scripts/calculate_alpha_score.py
.venv/bin/python scripts/sentry_monitor.py
.venv/bin/python scripts/portfolio_strategist.py
.venv/bin/python scripts/mock_portfolio_executor.py
.venv/bin/python scripts/daily_report.py
.venv/bin/python scripts/trade_alerts.py
```

## Notes
- All cron jobs must execute using the project virtual environment at `.venv/bin/python`.
- Never depend on ambient system Python for project jobs.
- Never hardcode container-only or host-only absolute paths if a repo-relative path can be used.
- Wrapper scripts should resolve their own `SCRIPT_DIR`, derive `ROOT_DIR`, and execute Python through `"$ROOT_DIR/.venv/bin/python"`.
- `yfinance` is required for V1 runtime.
- If preflight fails, the main pipeline must not run.
