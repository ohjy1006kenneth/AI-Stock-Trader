#!/usr/bin/env bash
set -euo pipefail
cd /home/node/.openclaw/workspace-trading
PYTHON_BIN="/home/node/.openclaw/workspace-trading/.venv/bin/python"

if [ ! -x "$PYTHON_BIN" ]; then
  echo "Daily summary unavailable: missing virtualenv python at $PYTHON_BIN"
  exit 1
fi

"$PYTHON_BIN" scripts/daily_report.py
LATEST_REPORT=$(ls -1t reports/daily_summary_*.md 2>/dev/null | head -n 1 || true)
if [ -z "$LATEST_REPORT" ]; then
  echo "Daily summary unavailable: no dated report found"
  exit 1
fi
cat "$LATEST_REPORT"
