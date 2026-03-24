#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd -- "$SCRIPT_DIR/.." && pwd)"
VENV_PY="$ROOT_DIR/.venv/bin/python"

echo "[runtime] root=$ROOT_DIR"
echo "[runtime] python=$VENV_PY"

if [ ! -x "$VENV_PY" ]; then
  echo "PREFLIGHT FAILED"
  echo ""
  echo "Errors:"
  echo "- missing_virtualenv_python:$VENV_PY"
  exit 1
fi

cd "$ROOT_DIR"
"$VENV_PY" scripts/preflight_check.py
"$VENV_PY" scripts/build_universe.py
"$VENV_PY" scripts/fetch_price_data.py
"$VENV_PY" scripts/fetch_fundamental_data.py
"$VENV_PY" scripts/quality_filter.py
"$VENV_PY" scripts/calculate_alpha_score.py
"$VENV_PY" scripts/sentry_monitor.py
"$VENV_PY" scripts/portfolio_strategist.py
"$VENV_PY" scripts/mock_portfolio_executor.py
"$VENV_PY" scripts/daily_report.py
"$VENV_PY" scripts/trade_alerts.py

echo "PIPELINE OK"
