from __future__ import annotations

from pathlib import Path
import math
import sys

ROOT_DIR = Path(__file__).resolve()
for _ in range(5):
    if (ROOT_DIR / ".gitignore").exists():
        break
    ROOT_DIR = ROOT_DIR.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from typing import Any

from cloud_inference.contracts import validate_response_payload
from runtime.common.common import latest_price_from_snapshot, safe_float


def translate_oracle_response_to_execution_intents(*, oracle_response: dict[str, Any], broker_snapshot: dict[str, Any], price_snapshot: dict[str, Any]) -> list[dict[str, Any]]:
    broker_positions = {str(pos.get("symbol")): pos for pos in broker_snapshot.get("positions", []) if pos.get("symbol")}
    universe_tickers = [str(item.get("ticker")) for item in price_snapshot.get("items", []) if item.get("ticker")]
    validate_response_payload(oracle_response, universe_tickers, list(broker_positions.keys()))
    predicted_tickers = {str(row["ticker"]) for row in oracle_response.get("predictions", [])}
    missing_held = sorted(set(broker_positions.keys()) - predicted_tickers)
    if missing_held:
        raise ValueError(f"missing_predictions_for_held_positions:{','.join(missing_held)}")

    account = broker_snapshot.get("account", {})
    total_equity = float(safe_float(account.get("equity"), account.get("cash")) or 0.0)
    if total_equity <= 0:
        raise ValueError("invalid_total_equity")

    actions: list[dict[str, Any]] = []
    for row in oracle_response.get("predictions", []):
        ticker = str(row["ticker"])
        price = latest_price_from_snapshot(price_snapshot, ticker)
        if price is None or price <= 0:
            raise ValueError(f"missing_execution_price:{ticker}")
        current_qty = int(float((broker_positions.get(ticker) or {}).get("qty", 0) or 0))
        target_weight = float(row["target_weight"])
        target_notional = total_equity * target_weight
        target_shares = int(math.floor(target_notional / price)) if target_weight > 0 else 0
        share_delta = target_shares - current_qty
        if share_delta > 0:
            action = "BUY"
            order_qty = share_delta
        elif share_delta < 0:
            action = "SELL"
            order_qty = abs(share_delta)
        else:
            action = "HOLD"
            order_qty = 0
        actions.append({
            "ticker": ticker,
            "action": action,
            "order_qty": order_qty,
            "current_shares": current_qty,
            "target_shares": target_shares,
            "share_delta": share_delta,
            "target_weight": round(target_weight, 6),
            "target_notional": round(target_notional, 2),
            "current_price": round(float(price), 6),
            "confidence": float(row["confidence"]),
            "signal_type": row["signal_type"],
        })
    return actions
