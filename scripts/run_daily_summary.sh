#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd -- "$SCRIPT_DIR/.." && pwd)"
VENV_PY="$ROOT_DIR/.venv/bin/python"

echo "[runtime] root=$ROOT_DIR"
echo "[runtime] python=$VENV_PY"

if [ ! -x "$VENV_PY" ]; then
  echo "Daily summary unavailable: missing virtualenv python at $VENV_PY"
  exit 1
fi

cd "$ROOT_DIR"
"$VENV_PY" scripts/daily_report.py
LATEST_REPORT=$(ls -1t reports/daily_summary_*.md 2>/dev/null | head -n 1 || true)
if [ -z "$LATEST_REPORT" ]; then
  echo "Daily summary unavailable: no dated report found"
  exit 1
fi
cat "$LATEST_REPORT"
