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

from typing import Any

from pi_edge.execution.alpaca_paper import AlpacaPaperClient, build_broker_snapshot
from pi_edge.execution.decision_adapter import translate_oracle_response_to_execution_intents
from pi_edge.execution.risk_engine import apply_hard_risk_constraints
from pi_edge.network.hf_api_client import call_oracle
from runtime.common.common import (
    EXECUTION_DATA_DIR,
    LEDGER_DIR,
    MARKET_DATA_DIR,
    env_str,
    gen_id,
    load_execution_config,
    market_is_open_now,
    now_iso,
    read_json,
    safe_float,
    validate_required_fields,
    write_json,
)

EXEC_REQUIRED = [
    "execution_id", "linked_decision_id", "timestamp", "ticker", "requested_action", "execution_status",
    "rejection_reason", "execution_price", "shares_filled", "cash_before", "cash_after",
    "position_before", "position_after", "realized_pnl_change", "notes"
]
FINAL_BROKER_STATUSES = {"filled", "canceled", "expired", "rejected", "done_for_day"}
FILLED_BROKER_STATUSES = {"filled"}
REJECTED_BROKER_STATUSES = {"rejected", "canceled", "expired"}
ORACLE_HISTORY_BARS = 21


def make_log(**kwargs):
    log = {key: kwargs.get(key) for key in EXEC_REQUIRED}
    missing = validate_required_fields(log, EXEC_REQUIRED)
    if missing:
        raise ValueError(f"execution log missing fields: {missing}")
    return log


def append_extra(log: dict[str, Any], **kwargs: Any) -> dict[str, Any]:
    log.update(kwargs)
    return log


def reject(items, ticker, requested_action, reason, cash_before, cash_after, position_before, position_after, execution_price=None, **extra):
    items.append(append_extra(make_log(
        execution_id=gen_id("exe"), linked_decision_id=extra.get("linked_decision_id"), timestamp=now_iso(), ticker=ticker,
        requested_action=requested_action, execution_status="REJECTED", rejection_reason=reason, execution_price=execution_price,
        shares_filled=0, cash_before=cash_before, cash_after=cash_after, position_before=position_before, position_after=position_after,
        realized_pnl_change=0.0, notes=extra.get("notes") or reason
    ), **extra))


def normalize_broker_positions(raw_positions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    items = []
    for pos in raw_positions:
        qty = int(float(pos.get("qty") or 0))
        avg_cost = safe_float(pos.get("avg_entry_price"), 0.0) or 0.0
        last_price = safe_float(pos.get("current_price"), avg_cost) or avg_cost
        market_value = safe_float(pos.get("market_value"), qty * last_price) or (qty * last_price)
        unrealized = safe_float(pos.get("unrealized_pl"), (last_price - avg_cost) * qty) or ((last_price - avg_cost) * qty)
        items.append({
            "ticker": pos.get("symbol"),
            "sleeve": pos.get("asset_class") or "PAPER",
            "shares": qty,
            "avg_cost": round(avg_cost, 4),
            "last_price": round(last_price, 4),
            "market_value": round(market_value, 2),
            "unrealized_pnl": round(unrealized, 2),
            "entry_date": None,
            "thesis_summary": "broker_synced_position",
            "stop_rule": None,
            "take_profit_rule": None,
            "max_holding_period_days": None,
            "holding_days": None,
            "broker_position": True,
        })
    return items


def sync_ledger_from_broker(portfolio: dict, snapshot: dict[str, Any]) -> dict:
    account = snapshot.get("account", {})
    positions = snapshot.get("positions", [])
    portfolio["execution_mode"] = "paper"
    portfolio["broker"] = "alpaca"
    portfolio["cash"] = round(safe_float(account.get("cash"), portfolio.get("cash", 0.0)) or 0.0, 2)
    portfolio["total_equity"] = round(safe_float(account.get("equity"), portfolio.get("total_equity", 0.0)) or 0.0, 2)
    portfolio["buying_power"] = round(safe_float(account.get("buying_power"), 0.0) or 0.0, 2)
    portfolio["unrealized_pnl"] = round(safe_float(account.get("unrealized_pl"), portfolio.get("unrealized_pnl", 0.0)) or 0.0, 2)
    portfolio["realized_pnl"] = round(safe_float(account.get("realized_pl"), portfolio.get("realized_pnl", 0.0)) or float(portfolio.get("realized_pnl", 0.0)), 2)
    portfolio["positions"] = normalize_broker_positions(positions)
    portfolio["broker_account"] = {
        "id": account.get("id"),
        "status": account.get("status"),
        "currency": account.get("currency"),
        "cash": account.get("cash"),
        "buying_power": account.get("buying_power"),
        "equity": account.get("equity"),
        "pattern_day_trader": account.get("pattern_day_trader"),
        "trading_blocked": account.get("trading_blocked"),
        "last_synced": now_iso(),
    }
    portfolio["last_updated"] = now_iso()
    return portfolio


def ensure_paper_credentials() -> None:
    if not env_str("ALPACA_API_KEY") or not env_str("ALPACA_API_SECRET"):
        raise SystemExit("missing_alpaca_credentials")


def _trim_history(history: list[dict[str, Any]]) -> list[dict[str, Any]]:
    trimmed = history[-ORACLE_HISTORY_BARS:]
    return [
        {
            "date": row["date"],
            "open": row["open"],
            "high": row["high"],
            "low": row["low"],
            "close": row["close"],
            "volume": row["volume"],
        }
        for row in trimmed
        if {"date", "open", "high", "low", "close", "volume"}.issubset(row.keys())
    ]


def build_oracle_payload(price_snapshot: dict[str, Any], broker_snapshot: dict[str, Any]) -> dict[str, Any]:
    account = broker_snapshot.get("account", {})
    broker_positions = broker_snapshot.get("positions", [])
    universe = []
    for item in price_snapshot.get("items", []):
        ticker = item.get("ticker")
        history = _trim_history(item.get("history", []))
        if not ticker or not history:
            continue
        universe.append({
            "ticker": ticker,
            "history": history,
            "news": [],
        })
    if not universe:
        raise ValueError("oracle_request_universe_empty")

    portfolio_positions = []
    for pos in broker_positions:
        ticker = pos.get("symbol")
        if not ticker:
            continue
        portfolio_positions.append({
            "ticker": ticker,
            "qty": int(float(pos.get("qty") or 0)),
            "entry_price": float(pos.get("avg_entry_price") or 0.0),
        })

    return {
        "portfolio": {
            "cash": float(account.get("cash") or 0.0),
            "positions": portfolio_positions,
        },
        "universe": universe,
    }


def compute_rebalance_actions(*, predictions_payload: dict[str, Any], broker_snapshot: dict[str, Any], price_snapshot: dict[str, Any]) -> list[dict[str, Any]]:
    return translate_oracle_response_to_execution_intents(
        oracle_response=predictions_payload,
        broker_snapshot=broker_snapshot,
        price_snapshot=price_snapshot,
    )


def main() -> None:
    config = load_execution_config()
    if config.get("paper_trading_only") is not True:
        raise SystemExit("paper_trading_only_must_remain_true")
    if config.get("broker") != "alpaca":
        raise SystemExit("unsupported_broker")
    ensure_paper_credentials()

    price_snapshot = read_json(MARKET_DATA_DIR / "price_snapshot.json", {"items": []})
    portfolio = read_json(LEDGER_DIR / "paper_portfolio.json", {})
    logs: list[dict[str, Any]] = []

    client = AlpacaPaperClient()
    broker_snapshot = build_broker_snapshot(client)
    account = broker_snapshot.get("account", {})
    cash = float(safe_float(account.get("cash"), portfolio.get("cash", 0.0)) or 0.0)
    open_orders = client.list_orders(status="open") if config.get("duplicate_order_prevention", {}).get("check_open_broker_orders", True) else []
    broker_positions = {p.get("symbol"): p for p in broker_snapshot.get("positions", []) if p.get("symbol")}

    oracle_payload = build_oracle_payload(price_snapshot, broker_snapshot)
    oracle_response = call_oracle(oracle_payload)
    risk_checked_response, risk_summary = apply_hard_risk_constraints(
        oracle_response=oracle_response,
        request_universe=[str(item.get("ticker")) for item in oracle_payload.get("universe", []) if item.get("ticker")],
        current_positions=[str(pos.get("symbol")) for pos in broker_snapshot.get("positions", []) if pos.get("symbol")],
        execution_config=config,
    )
    rebalance_actions = compute_rebalance_actions(
        predictions_payload=risk_checked_response,
        broker_snapshot=broker_snapshot,
        price_snapshot=price_snapshot,
    )

    market_cfg = config.get("market_hours", {})
    market_open = market_is_open_now(market_cfg.get("timezone", "America/New_York")) if market_cfg.get("enforce", True) else True
    allow_queue = market_cfg.get("allow_queued_orders_outside_market_hours", True)

    for action_row in rebalance_actions:
        ticker = action_row["ticker"]
        action = action_row["action"]
        price = action_row["current_price"]
        position_before = broker_positions.get(ticker)
        if action == "HOLD":
            logs.append(append_extra(make_log(
                execution_id=gen_id("exe"), linked_decision_id=risk_checked_response.get("request_id"), timestamp=now_iso(), ticker=ticker,
                requested_action=action, execution_status="EXECUTED", rejection_reason=None, execution_price=price,
                shares_filled=0, cash_before=cash, cash_after=cash, position_before=position_before, position_after=position_before,
                realized_pnl_change=0.0, notes="rebalance_already_at_target"
            ), execution_mode="paper", broker_name="alpaca", broker_status="noop", rebalance=action_row, oracle_request_id=risk_checked_response.get("request_id"), oracle_model_version=risk_checked_response.get("model_version"), risk_summary=risk_summary))
            continue
        if not market_open and not allow_queue:
            reject(logs, ticker, action, "market_closed_submission_blocked", cash, cash, position_before, position_before, price,
                   execution_mode="paper", rebalance=action_row, oracle_request_id=risk_checked_response.get("request_id"), oracle_model_version=risk_checked_response.get("model_version"), risk_summary=risk_summary)
            continue
        if action_row["order_qty"] <= 0:
            reject(logs, ticker, action, "non_positive_share_request", cash, cash, position_before, position_before, price,
                   execution_mode="paper", rebalance=action_row, oracle_request_id=risk_checked_response.get("request_id"), oracle_model_version=risk_checked_response.get("model_version"), risk_summary=risk_summary)
            continue

        if config.get("duplicate_order_prevention", {}).get("check_open_broker_orders", True):
            duplicate_open = next((o for o in open_orders if o.get("symbol") == ticker and o.get("side", "").upper() == action), None)
            if duplicate_open is not None:
                reject(logs, ticker, action, "duplicate_open_broker_order", cash, cash, position_before, position_before, price,
                       execution_mode="paper", broker_order_id=duplicate_open.get("id"), broker_status=duplicate_open.get("status"), rebalance=action_row,
                       oracle_request_id=risk_checked_response.get("request_id"), oracle_model_version=risk_checked_response.get("model_version"), risk_summary=risk_summary)
                continue

        side = "buy" if action == "BUY" else "sell"
        broker_resp = client.submit_order(
            symbol=ticker,
            side=side,
            qty=int(action_row["order_qty"]),
            order_type=config.get("order_defaults", {}).get(f"{side}_order_type", "market"),
            time_in_force=config.get("order_defaults", {}).get("time_in_force", "day"),
            client_order_id=f"{risk_checked_response.get('request_id', 'oracle')}-{ticker}",
            extended_hours=bool(config.get("order_defaults", {}).get("extended_hours", False)),
        )
        broker_status = str(broker_resp.get("status") or "submitted")
        filled_qty = int(float(broker_resp.get("filled_qty") or 0))
        fill_price = safe_float(broker_resp.get("filled_avg_price"), price) or price
        if broker_status in FILLED_BROKER_STATUSES:
            execution_status = "EXECUTED"
            rejection_reason = None
        elif broker_status in REJECTED_BROKER_STATUSES:
            execution_status = "REJECTED"
            rejection_reason = broker_resp.get("reject_reason") or broker_status
        else:
            execution_status = "PENDING"
            rejection_reason = None

        logs.append(append_extra(make_log(
            execution_id=gen_id("exe"), linked_decision_id=risk_checked_response.get("request_id"), timestamp=now_iso(), ticker=ticker,
            requested_action=action, execution_status=execution_status,
            rejection_reason=rejection_reason, execution_price=fill_price,
            shares_filled=filled_qty, cash_before=cash, cash_after=cash, position_before=position_before, position_after=None,
            realized_pnl_change=0.0, notes="oracle_target_weight_rebalance"
        ), execution_mode="paper", broker_name="alpaca", broker_order_id=broker_resp.get("id"), broker_client_order_id=broker_resp.get("client_order_id"), broker_status=broker_status, broker_response=broker_resp, queued_for_next_session=(not market_open), rebalance=action_row, oracle_request_id=risk_checked_response.get("request_id"), oracle_model_version=risk_checked_response.get("model_version"), risk_summary=risk_summary))
        if broker_status in FINAL_BROKER_STATUSES and broker_status != "rejected":
            open_orders = [o for o in open_orders if o.get("id") != broker_resp.get("id")]
        else:
            open_orders.append(broker_resp)

    synced_snapshot = build_broker_snapshot(client)
    portfolio = sync_ledger_from_broker(portfolio, synced_snapshot)
    positions_count = len(synced_snapshot.get("positions", []))
    for row in logs:
        row.setdefault("cash_after", portfolio.get("cash"))
        row.setdefault("position_after", None)
        row["synced_positions_count"] = positions_count
        row["synced_total_equity"] = portfolio.get("total_equity")

    write_json(LEDGER_DIR / "paper_portfolio.json", portfolio)
    write_json(EXECUTION_DATA_DIR / "execution_log.json", {
        "generated_at": now_iso(),
        "execution_schema_version": "v5",
        "execution_mode": "paper",
        "oracle_request": oracle_payload,
        "oracle_response_raw": oracle_response,
        "oracle_response": risk_checked_response,
        "risk_summary": risk_summary,
        "rebalance_actions": rebalance_actions,
        "items": logs,
    })
    print(f"Executor finished: {len(logs)} execution log records (mode=paper, oracle_rebalance=true)")


if __name__ == "__main__":
    main()
