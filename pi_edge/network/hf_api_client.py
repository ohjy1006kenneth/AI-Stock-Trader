from __future__ import annotations

from pathlib import Path
import sys

ROOT_DIR = Path(__file__).resolve()
for _ in range(5):
    if (ROOT_DIR / ".gitignore").exists():
        break
    ROOT_DIR = ROOT_DIR.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import json
import uuid
import urllib.error
import urllib.request
from typing import Any

from runtime.common.common import CONFIG_DIR, env_str, load_local_env_file

REQUEST_SCHEMA_PATH = CONFIG_DIR / "cloud_oracle_request.schema.json"
RESPONSE_SCHEMA_PATH = CONFIG_DIR / "cloud_oracle_response.schema.json"


def require_hf_config() -> tuple[str, str]:
    load_local_env_file(CONFIG_DIR / "alpaca.env")
    endpoint = env_str("HF_INFERENCE_URL")
    token = env_str("HF_API_TOKEN")
    if not endpoint:
        raise RuntimeError("missing_hf_inference_url")
    if not token:
        raise RuntimeError("missing_hf_api_token")
    return endpoint, token


def _load_schema(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text())


def _require(cond: bool, message: str) -> None:
    if not cond:
        raise RuntimeError(message)


def validate_request_payload(payload: dict[str, Any]) -> None:
    schema = _load_schema(REQUEST_SCHEMA_PATH)
    _require(isinstance(payload, dict), "request_payload_must_be_object")
    _require("portfolio" in payload and "universe" in payload, "request_missing_top_level_fields")
    portfolio = payload["portfolio"]
    universe = payload["universe"]
    _require(isinstance(portfolio, dict), "portfolio_must_be_object")
    _require(isinstance(universe, list) and len(universe) > 0, "universe_must_be_nonempty_array")
    _require("cash" in portfolio and "positions" in portfolio, "portfolio_missing_fields")
    _require(float(portfolio["cash"]) >= 0, "portfolio_cash_must_be_nonnegative")
    _require(isinstance(portfolio["positions"], list), "portfolio_positions_must_be_array")
    for pos in portfolio["positions"]:
        _require(all(k in pos for k in ("ticker", "qty", "entry_price")), "position_missing_required_fields")
        _require(int(pos["qty"]) >= 0, "position_qty_must_be_nonnegative")
        _require(float(pos["entry_price"]) >= 0, "position_entry_price_must_be_nonnegative")
    for item in universe:
        _require(all(k in item for k in ("ticker", "history", "news")), "universe_item_missing_required_fields")
        _require(isinstance(item["history"], list) and len(item["history"]) > 0, "history_must_be_nonempty_array")
        _require(isinstance(item["news"], list), "news_must_be_array")
        for bar in item["history"]:
            _require(all(k in bar for k in ("date", "open", "high", "low", "close", "volume")), "history_bar_missing_required_fields")
        for news_item in item["news"]:
            _require(all(k in news_item for k in ("date", "headline", "summary")), "news_item_missing_required_fields")
    _ = schema


def validate_response_payload(payload: dict[str, Any], request_universe: list[str], current_positions: list[str]) -> None:
    schema = _load_schema(RESPONSE_SCHEMA_PATH)
    _require(isinstance(payload, dict), "response_payload_must_be_object")
    _require(all(k in payload for k in ("model_version", "generated_at", "request_id", "predictions")), "response_missing_top_level_fields")
    preds = payload["predictions"]
    _require(isinstance(preds, list) and len(preds) > 0, "predictions_must_be_nonempty_array")
    seen = set()
    total_weight = 0.0
    allowed = set(request_universe) | set(current_positions)
    for row in preds:
        _require(all(k in row for k in ("ticker", "target_weight", "confidence", "signal_type")), "prediction_missing_required_fields")
        ticker = str(row["ticker"])
        _require(ticker in allowed, f"prediction_contains_unknown_ticker:{ticker}")
        _require(ticker not in seen, f"duplicate_prediction_ticker:{ticker}")
        seen.add(ticker)
        weight = float(row["target_weight"])
        confidence = float(row["confidence"])
        _require(0.0 <= weight <= 0.2, f"invalid_target_weight:{ticker}")
        _require(0.0 <= confidence <= 1.0, f"invalid_confidence:{ticker}")
        _require(str(row["signal_type"]) == "long", f"invalid_signal_type:{ticker}")
        total_weight += weight
    _require(total_weight <= 1.0 + 1e-9, "invalid_total_target_weight_exceeds_one")
    _ = schema


def call_oracle(payload: dict[str, Any]) -> dict[str, Any]:
    endpoint, token = require_hf_config()
    validate_request_payload(payload)
    request_id = str(uuid.uuid4())
    req = urllib.request.Request(
        endpoint,
        data=json.dumps(payload).encode("utf-8"),
        method="POST",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "X-Request-Id": request_id,
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=60) as response:
            body = response.read().decode("utf-8")
            parsed = json.loads(body) if body else {}
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8") if exc.fp else ""
        raise RuntimeError(f"hf_http_{exc.code}:{body}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"hf_network_error:{exc}") from exc

    universe_tickers = [item["ticker"] for item in payload.get("universe", [])]
    current_positions = [item["ticker"] for item in payload.get("portfolio", {}).get("positions", [])]
    validate_response_payload(parsed, universe_tickers, current_positions)
    return parsed
