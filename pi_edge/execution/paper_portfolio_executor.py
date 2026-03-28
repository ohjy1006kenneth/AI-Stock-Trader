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

from runtime.common.common import EXECUTION_DATA_DIR, LEDGER_DIR, MARKET_DATA_DIR, STRATEGY_DATA_DIR, env_str, gen_id, latest_price_from_snapshot, load_execution_config, market_is_open_now, now_iso, read_json, safe_float, validate_required_fields, write_json
from runtime.pi.execution.alpaca_paper import AlpacaPaperClient, build_broker_snapshot

DECISION_REQUIRED = [
    "decision_id", "timestamp", "ticker", "action", "sleeve", "reason_code",
    "thesis_summary", "trigger_source", "confidence_label", "requires_executor_validation"
]
EXEC_REQUIRED = [
    "execution_id", "linked_decision_id", "timestamp", "ticker", "requested_action", "execution_status",
    "rejection_reason", "execution_price", "shares_filled", "cash_before", "cash_after",
    "position_before", "position_after", "realized_pnl_change", "notes"
]
VALID_ACTIONS = {"BUY", "SELL", "HOLD", "REVIEW"}
VALID_SLEEVES = {"CORE", "SWING"}
FINAL_BROKER_STATUSES = {"filled", "canceled", "expired", "rejected", "done_for_day"}


def make_log(**kwargs):
    log = {key: kwargs.get(key) for key in EXEC_REQUIRED}
    missing = validate_required_fields(log, EXEC_REQUIRED)
    if missing:
        raise ValueError(f"execution log missing fields: {missing}")
    return log


def append_extra(log: dict[str, Any], **kwargs: Any) -> dict[str, Any]:
    log.update(kwargs)
    return log


def reject(items, decision, reason, cash_before, cash_after, position_before, position_after, execution_price=None, **extra):
    items.append(append_extra(make_log(
        execution_id=gen_id("exe"), linked_decision_id=decision.get("decision_id"), timestamp=now_iso(), ticker=decision.get("ticker"),
        requested_action=decision.get("action"), execution_status="REJECTED", rejection_reason=reason, execution_price=execution_price,
        shares_filled=0, cash_before=cash_before, cash_after=cash_after, position_before=position_before, position_after=position_after,
        realized_pnl_change=0.0, notes=decision.get("reason_code")
    ), **extra))


def load_portfolio_rules(portfolio: dict, config: dict) -> dict:
    rules = dict(config.get("risk_limits", {}))
    rules.update(portfolio.get("portfolio_rules", {}))
    return {
        "max_total_positions": rules.get("max_total_positions", 15),
        "max_core_allocation": rules.get("max_core_allocation", 0.7),
        "max_swing_allocation": rules.get("max_swing_allocation", 0.3),
        "max_position_weight": rules.get("max_position_weight", 0.12),
        "cash_buffer_rule": rules.get("cash_buffer_rule", 0.05),
    }


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


def build_target_shares(decision: dict, price: float) -> int:
    if decision.get("target_shares"):
        return int(decision["target_shares"])
    target_notional = safe_float(decision.get("target_notional"), 0.0) or 0.0
    if price <= 0:
        return 0
    return int(target_notional // price)


def ensure_paper_credentials() -> None:
    if not env_str("ALPACA_API_KEY") or not env_str("ALPACA_API_SECRET"):
        raise SystemExit("missing_alpaca_credentials")


def main() -> None:
    config = load_execution_config()
    if config.get("paper_trading_only") is not True:
        raise SystemExit("paper_trading_only_must_remain_true")
    if config.get("broker") != "alpaca":
        raise SystemExit("unsupported_broker")
    ensure_paper_credentials()

    decisions_payload = read_json(STRATEGY_DATA_DIR / "strategist_decisions.json", {"decisions": []})
    price_snapshot = read_json(MARKET_DATA_DIR / "price_snapshot.json", {"items": []})
    portfolio = read_json(LEDGER_DIR / "paper_portfolio.json", {})
    decisions = decisions_payload.get("decisions", [])
    logs: list[dict[str, Any]] = []

    rules = load_portfolio_rules(portfolio, config)
    client = AlpacaPaperClient()
    broker_snapshot = build_broker_snapshot(client)
    open_orders = client.list_orders(status="open") if config.get("duplicate_order_prevention", {}).get("check_open_broker_orders", True) else []
    broker_positions = {p.get("symbol"): p for p in broker_snapshot.get("positions", [])}
    account = broker_snapshot.get("account", {})
    positions = normalize_broker_positions(broker_snapshot.get("positions", []))
    cash = float(safe_float(account.get("cash"), portfolio.get("cash", 0.0)) or 0.0)
    total_equity = float(safe_float(account.get("equity"), portfolio.get("total_equity", cash)) or cash)

    by_ticker = {}
    for d in decisions:
        missing = validate_required_fields(d, DECISION_REQUIRED)
        if missing:
            reject(logs, d, f"malformed_decision_schema:{','.join(missing)}", cash, cash, None, None, execution_mode="paper")
            continue
        if d.get("action") not in VALID_ACTIONS:
            reject(logs, d, "invalid_action", cash, cash, None, None, execution_mode="paper")
            continue
        if d.get("sleeve") not in VALID_SLEEVES:
            reject(logs, d, "invalid_sleeve", cash, cash, None, None, execution_mode="paper")
            continue
        by_ticker.setdefault(d["ticker"], []).append(d["action"])

    conflicted = {ticker for ticker, actions in by_ticker.items() if "BUY" in actions and "SELL" in actions}

    def sleeve_value(sleeve: str) -> float:
        return sum(float(p.get("market_value", 0.0)) for p in positions if p.get("sleeve") == sleeve)

    market_cfg = config.get("market_hours", {})
    market_open = market_is_open_now(market_cfg.get("timezone", "America/New_York")) if market_cfg.get("enforce", True) else True
    allow_queue = market_cfg.get("allow_queued_orders_outside_market_hours", True)

    for d in decisions:
        ticker = d.get("ticker")
        action = d.get("action")
        price = latest_price_from_snapshot(price_snapshot, ticker)
        position_before = next((p for p in positions if p.get("ticker") == ticker), None)
        if ticker in conflicted:
            reject(logs, d, "conflicting_buy_sell_instructions", cash, cash, position_before, position_before, price, execution_mode="paper")
            continue
        if price is None:
            reject(logs, d, "missing_execution_price", cash, cash, position_before, position_before, price, execution_mode="paper")
            continue
        if action in {"HOLD", "REVIEW"}:
            logs.append(append_extra(make_log(
                execution_id=gen_id("exe"), linked_decision_id=d["decision_id"], timestamp=now_iso(), ticker=ticker,
                requested_action=action, execution_status="EXECUTED", rejection_reason=None, execution_price=price,
                shares_filled=0, cash_before=cash, cash_after=cash, position_before=position_before, position_after=position_before,
                realized_pnl_change=0.0, notes=d.get("reason_code")
            ), execution_mode="paper", broker_name="alpaca", broker_status="noop"))
            continue
        if not market_open and not allow_queue:
            reject(logs, d, "market_closed_submission_blocked", cash, cash, position_before, position_before, price, execution_mode="paper")
            continue

        target_shares = build_target_shares(d, price)
        if action == "BUY":
            if config.get("duplicate_order_prevention", {}).get("check_existing_positions", True) and (position_before is not None or ticker in broker_positions):
                reject(logs, d, "duplicate_existing_position", cash, cash, position_before, position_before, price, execution_mode="paper")
                continue
            if len(positions) >= int(rules["max_total_positions"]):
                reject(logs, d, "max_total_positions_exceeded", cash, cash, None, None, price, execution_mode="paper")
                continue
            if target_shares <= 0:
                reject(logs, d, "non_positive_share_request", cash, cash, None, None, price, execution_mode="paper")
                continue
            notional = round(target_shares * price, 2)
            if notional > cash * (1.0 - float(rules["cash_buffer_rule"])):
                reject(logs, d, "insufficient_cash_buffer", cash, cash, None, None, price, execution_mode="paper")
                continue
            if (notional / max(total_equity, 1.0)) > float(rules["max_position_weight"]):
                reject(logs, d, "position_weight_exceeded", cash, cash, None, None, price, execution_mode="paper")
                continue
            if d["sleeve"] == "CORE" and ((sleeve_value("CORE") + notional) / max(total_equity, 1.0)) > float(rules["max_core_allocation"]):
                reject(logs, d, "core_allocation_exceeded", cash, cash, None, None, price, execution_mode="paper")
                continue
            if d["sleeve"] == "SWING" and ((sleeve_value("SWING") + notional) / max(total_equity, 1.0)) > float(rules["max_swing_allocation"]):
                reject(logs, d, "swing_allocation_exceeded", cash, cash, None, None, price, execution_mode="paper")
                continue
        elif action == "SELL":
            if target_shares <= 0 and position_before is not None:
                target_shares = int(position_before.get("shares", 0) or 0)
            if position_before is None and ticker not in broker_positions:
                reject(logs, d, "selling_nonexistent_position", cash, cash, None, None, price, execution_mode="paper")
                continue
            if target_shares <= 0:
                target_shares = int(float((broker_positions.get(ticker) or {}).get("qty", 0) or 0))
            if target_shares <= 0:
                reject(logs, d, "non_positive_share_request", cash, cash, position_before, position_before, price, execution_mode="paper")
                continue

        if config.get("duplicate_order_prevention", {}).get("check_open_broker_orders", True):
            duplicate_open = next((o for o in open_orders if o.get("symbol") == ticker and o.get("side", "").upper() == action), None)
            if duplicate_open is not None:
                reject(logs, d, "duplicate_open_broker_order", cash, cash, position_before, position_before, price,
                       execution_mode="paper", broker_order_id=duplicate_open.get("id"), broker_status=duplicate_open.get("status"))
                continue

        side = "buy" if action == "BUY" else "sell"
        broker_resp = client.submit_order(
            symbol=ticker,
            side=side,
            qty=target_shares,
            order_type=config.get("order_defaults", {}).get(f"{side}_order_type", "market"),
            time_in_force=config.get("order_defaults", {}).get("time_in_force", "day"),
            client_order_id=d["decision_id"],
            extended_hours=bool(config.get("order_defaults", {}).get("extended_hours", False)),
        )
        broker_status = str(broker_resp.get("status") or "submitted")
        filled_qty = int(float(broker_resp.get("filled_qty") or 0))
        fill_price = safe_float(broker_resp.get("filled_avg_price"), price) or price
        logs.append(append_extra(make_log(
            execution_id=gen_id("exe"), linked_decision_id=d["decision_id"], timestamp=now_iso(), ticker=ticker,
            requested_action=action, execution_status=("EXECUTED" if broker_status not in {"rejected"} else "REJECTED"),
            rejection_reason=(None if broker_status not in {"rejected"} else broker_resp.get("reject_reason")), execution_price=fill_price,
            shares_filled=filled_qty, cash_before=cash, cash_after=cash, position_before=position_before, position_after=None,
            realized_pnl_change=0.0, notes=d.get("reason_code")
        ), execution_mode="paper", broker_name="alpaca", broker_order_id=broker_resp.get("id"), broker_client_order_id=broker_resp.get("client_order_id"), broker_status=broker_status, broker_response=broker_resp, queued_for_next_session=(not market_open)))
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
        "execution_schema_version": "v3",
        "execution_mode": "paper",
        "items": logs,
    })
    print(f"Executor finished: {len(logs)} execution log records (mode=paper)")


if __name__ == "__main__":
    main()
