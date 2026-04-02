#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd -- "$SCRIPT_DIR/.." && pwd)"
VENV_PY="$ROOT_DIR/.venv/bin/python"

if [ -f "$ROOT_DIR/config/alpaca.env" ]; then
  set -a
  . "$ROOT_DIR/config/alpaca.env"
  set +a
fi

cd "$ROOT_DIR"

if [ ! -x "$VENV_PY" ]; then
  echo "EDGE PIPELINE FAILED"
  echo "- missing_virtualenv_python:$VENV_PY"
  exit 1
fi

"$VENV_PY" pi_edge/preflight_check.py >&2
"$VENV_PY" pi_edge/fetchers/refresh_sp500_constituents.py >&2
"$VENV_PY" pi_edge/fetchers/build_universe.py >&2
"$VENV_PY" pi_edge/fetchers/fetch_price_data.py >&2
"$VENV_PY" pi_edge/fetchers/fetch_fundamental_data.py >&2

HF_STATUS=$("$VENV_PY" - <<'PY'
from pi_edge.network.hf_api_client import require_hf_config
try:
    require_hf_config()
    print("OK")
except Exception as exc:
    print(f"ERROR:{exc}")
PY
)

if [[ "$HF_STATUS" == ERROR:* ]]; then
  echo "EDGE PIPELINE BLOCKED"
  echo "- data refresh: completed"
  echo "- inference: not configured"
  echo "- reason: ${HF_STATUS#ERROR:}"
  echo "- next step: set HF_INFERENCE_URL in config/alpaca.env or another loaded local env file"
  echo "- optional hardening: also set HF_MODEL_REPO_ID or HF_MODEL_REPO_READY_MANIFEST_URL for canonical ready-manifest validation"
  exit 1
fi

"$VENV_PY" pi_edge/execution/paper_portfolio_executor.py >&2
"$VENV_PY" pi_edge/reporting/trade_alerts.py >&2
"$VENV_PY" pi_edge/reporting/daily_report.py >&2
"$VENV_PY" pi_edge/reporting/pipeline_run_summary.py >&2

echo "EDGE PIPELINE SUCCESS"
echo "- data refresh: completed"
echo "- hugging_face_endpoint: configured"
echo "- oracle_call: completed"
echo "- paper_execution: completed"
echo "- reporting: completed"
