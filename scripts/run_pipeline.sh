#!/usr/bin/env bash
set -euo pipefail
cd /home/node/.openclaw/workspace-trading
PYTHON_BIN="/home/node/.openclaw/workspace-trading/.venv/bin/python"

if [ ! -x "$PYTHON_BIN" ]; then
  echo "PREFLIGHT FAILED"
  echo ""
  echo "Errors:"
  echo "- missing_virtualenv_python:$PYTHON_BIN"
  exit 1
fi

"$PYTHON_BIN" scripts/preflight_check.py
"$PYTHON_BIN" scripts/build_universe.py
"$PYTHON_BIN" scripts/fetch_price_data.py
"$PYTHON_BIN" scripts/fetch_fundamental_data.py
"$PYTHON_BIN" scripts/quality_filter.py
"$PYTHON_BIN" scripts/calculate_alpha_score.py
"$PYTHON_BIN" scripts/sentry_monitor.py
"$PYTHON_BIN" scripts/portfolio_strategist.py
"$PYTHON_BIN" scripts/mock_portfolio_executor.py
"$PYTHON_BIN" scripts/daily_report.py
"$PYTHON_BIN" scripts/trade_alerts.py

echo "PIPELINE OK"
