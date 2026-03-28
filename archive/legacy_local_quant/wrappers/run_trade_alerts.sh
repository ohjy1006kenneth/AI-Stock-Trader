#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd -- "$SCRIPT_DIR/../../.." && pwd)"
VENV_PY="$ROOT_DIR/.venv/bin/python"

echo "[runtime] root=$ROOT_DIR"
echo "[runtime] python=$VENV_PY"

if [ ! -x "$VENV_PY" ]; then
  echo "No new trade alerts"
  exit 0
fi

cd "$ROOT_DIR"
OUT=$("$VENV_PY" runtime/pi/reporting/trade_alerts.py)
if [ "$OUT" = "No new trade alerts" ]; then
  exit 0
fi
printf "%s\n" "$OUT"
