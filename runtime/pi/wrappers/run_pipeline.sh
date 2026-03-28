#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd -- "$SCRIPT_DIR/../../.." && pwd)"
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
if ! "$VENV_PY" runtime/pi/preflight/preflight_check.py >&2; then
  echo "PIPELINE FAILED"
  echo ""
  echo "Summary:"
  echo "- preflight: failed"
  echo "- later stages: skipped"
  exit 1
fi
"$VENV_PY" runtime/pi/data/build_universe.py >&2
"$VENV_PY" runtime/pi/data/fetch_price_data.py >&2
"$VENV_PY" runtime/pi/data/fetch_fundamental_data.py >&2
"$VENV_PY" strategy/quality_filter.py >&2
"$VENV_PY" strategy/calculate_alpha_score.py >&2
"$VENV_PY" strategy/sentry_monitor.py >&2
"$VENV_PY" strategy/portfolio_strategist.py >&2
"$VENV_PY" runtime/pi/execution/mock_portfolio_executor.py >&2
"$VENV_PY" runtime/pi/reporting/daily_report.py >&2
"$VENV_PY" runtime/pi/reporting/trade_alerts.py >/tmp/trading_trade_alerts_latest.txt
"$VENV_PY" runtime/pi/reporting/pipeline_run_summary.py

ALERT_OUT=$(cat /tmp/trading_trade_alerts_latest.txt)
if [ -n "$ALERT_OUT" ] && [ "$ALERT_OUT" != "No new trade alerts" ]; then
  echo ""
  printf "%s\n" "$ALERT_OUT"
fi
