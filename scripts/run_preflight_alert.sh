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

if "$PYTHON_BIN" scripts/preflight_check.py; then
  exit 0
else
  cat outputs/preflight_status.txt
  exit 1
fi
