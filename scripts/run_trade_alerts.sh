#!/usr/bin/env bash
set -euo pipefail
cd /home/node/.openclaw/workspace-trading
PYTHON_BIN="/home/node/.openclaw/workspace-trading/.venv/bin/python"

if [ ! -x "$PYTHON_BIN" ]; then
  echo "No new trade alerts"
  exit 0
fi

OUT=$($PYTHON_BIN scripts/trade_alerts.py)
if [ "$OUT" = "No new trade alerts" ]; then
  exit 0
fi
printf "%s\n" "$OUT"
