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
if "$VENV_PY" scripts/preflight_check.py; then
  exit 0
else
  cat outputs/preflight_status.txt
  exit 1
fi
