#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd -- "$SCRIPT_DIR/.." && pwd)"
VENV_PY="$ROOT_DIR/.venv/bin/python"

echo "[runtime] root=$ROOT_DIR" >&2
echo "[runtime] python=$VENV_PY" >&2

if [ ! -x "$VENV_PY" ]; then
  echo "PREFLIGHT FAILED"
  echo ""
  echo "Errors:"
  echo "- missing_virtualenv_python:$VENV_PY"
  exit 1
fi

cd "$ROOT_DIR"
"$VENV_PY" scripts/preflight_check.py >&2
"$VENV_PY" scripts/build_universe.py >&2
"$VENV_PY" scripts/fetch_price_data.py >&2
"$VENV_PY" scripts/fetch_fundamental_data.py >&2
"$VENV_PY" scripts/quality_filter.py >&2
"$VENV_PY" scripts/calculate_alpha_score.py >&2
"$VENV_PY" scripts/sentry_monitor.py >&2
"$VENV_PY" scripts/portfolio_strategist.py >&2
"$VENV_PY" scripts/mock_portfolio_executor.py >&2
"$VENV_PY" scripts/daily_report.py >&2

ALERT_OUT=$("$VENV_PY" scripts/trade_alerts.py)
if [ -n "$ALERT_OUT" ] && [ "$ALERT_OUT" != "No new trade alerts" ]; then
  printf "%s\n" "$ALERT_OUT"
else
  echo "PIPELINE OK"
fi
