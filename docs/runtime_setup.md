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

That baseline install is enough for the lightweight Pi/runtime pieces.

### Issue #12 / cloud training acceptance runtime

If you plan to run the Issue #12 dataset/train/export pipeline from the repo checkout, install the dedicated stack instead of relying on the root runtime requirements alone:

```bash
.venv/bin/python -m pip install -r requirements/issue12_cloud.txt
```

Why this matters:
- `requirements.txt` only covers the lightweight baseline runtime dependency set.
- the Issue #12 smoke/acceptance path also needs cloud-training + bundle/inference deps such as `numpy`, `pandas`, `scikit-learn`, and `xgboost`.
- using `requirements/issue12_cloud.txt` avoids the common "missing numpy" style failure when running `.venv/bin/python -m cloud_training.training.run_issue12_cloud_pipeline ...` from a fresh environment.

## Verify runtime

Baseline runtime check:

```bash
.venv/bin/python --version
.venv/bin/python -m pip show yfinance
```

Issue #12 acceptance check:

```bash
.venv/bin/python -c "import numpy, pandas, sklearn, xgboost; print('issue12 deps ok')"
.venv/bin/python -m unittest tests.test_issue12_cloud_pipeline -v
```

## S&P 500 snapshot refresh

Refresh the runtime S&P 500 snapshot explicitly when you want to update membership:

```bash
.venv/bin/python runtime/pi/data/refresh_sp500_constituents.py
```

This uses:
- Wikipedia as the S&P 500 membership source
- Alpaca active tradable U.S. equities as the tradability filter
- `config/sp500_constituents.json` as the runtime source of truth

## Manual pipeline run

Preferred one-shot test:

```bash
./runtime/pi/wrappers/run_pipeline.sh
```

Individual Python steps if needed:

```bash
.venv/bin/python runtime/pi/data/build_universe.py
.venv/bin/python runtime/pi/data/fetch_price_data.py
.venv/bin/python runtime/pi/data/fetch_fundamental_data.py
.venv/bin/python strategy/quality_filter.py
.venv/bin/python strategy/calculate_alpha_score.py
.venv/bin/python strategy/sentry_monitor.py
.venv/bin/python strategy/portfolio_strategist.py
.venv/bin/python runtime/pi/execution/paper_portfolio_executor.py
.venv/bin/python runtime/pi/reporting/daily_report.py
.venv/bin/python runtime/pi/reporting/trade_alerts.py
.venv/bin/python runtime/pi/reporting/pipeline_run_summary.py
```

## Notes
- All cron jobs must execute using the project virtual environment at `.venv/bin/python`.
- Never depend on ambient system Python for project jobs.
- Never hardcode container-only or host-only absolute paths if a repo-relative path can be used.
- Wrapper scripts should resolve their own `SCRIPT_DIR`, derive `ROOT_DIR`, and execute Python through `"$ROOT_DIR/.venv/bin/python"`.
- `yfinance` is required for V1 runtime.
- If preflight fails, the main pipeline must not run.
