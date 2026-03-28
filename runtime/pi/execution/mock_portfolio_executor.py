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

from runtime.common.common import EXECUTION_DATA_DIR, LEDGER_DIR, MARKET_DATA_DIR, STRATEGY_DATA_DIR, gen_id, latest_price_from_snapshot, now_iso, read_json, validate_required_fields, write_json

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


def make_log(**kwargs):
    log = {key: kwargs.get(key) for key in EXEC_REQUIRED}
    missing = validate_required_fields(log, EXEC_REQUIRED)
    if missing:
        raise ValueError(f"execution log missing fields: {missing}")
    return log


def reject(items, decision, reason, cash_before, cash_after, position_before, position_after, execution_price=None):
    items.append(make_log(
        execution_id=gen_id("exe"), linked_decision_id=decision.get("decision_id"), timestamp=now_iso(), ticker=decision.get("ticker"),
        requested_action=decision.get("action"), execution_status="REJECTED", rejection_reason=reason, execution_price=execution_price,
        shares_filled=0, cash_before=cash_before, cash_after=cash_after, position_before=position_before, position_after=position_after,
        realized_pnl_change=0.0, notes=decision.get("reason_code")
    ))


def main() -> None:
    decisions_payload = read_json(STRATEGY_DATA_DIR / "strategist_decisions.json", {"decisions": []})
    price_snapshot = read_json(MARKET_DATA_DIR / "price_snapshot.json", {"items": []})
    portfolio = read_json(LEDGER_DIR / "mock_portfolio.json", {})
    decisions = decisions_payload.get("decisions", [])
    logs = []

    by_ticker = {}
    for d in decisions:
        missing = validate_required_fields(d, DECISION_REQUIRED)
        if missing:
            reject(logs, d, f"malformed_decision_schema:{','.join(missing)}", portfolio.get("cash"), portfolio.get("cash"), None, None)
            continue
        if d.get("action") not in VALID_ACTIONS:
            reject(logs, d, "invalid_action", portfolio.get("cash"), portfolio.get("cash"), None, None)
            continue
        if d.get("sleeve") not in VALID_SLEEVES:
            reject(logs, d, "invalid_sleeve", portfolio.get("cash"), portfolio.get("cash"), None, None)
            continue
        by_ticker.setdefault(d["ticker"], []).append(d["action"])

    conflicted = {ticker for ticker, actions in by_ticker.items() if "BUY" in actions and "SELL" in actions}
    positions = portfolio.get("positions", [])
    cash = float(portfolio.get("cash", 0))
    max_total_positions = portfolio.get("portfolio_rules", {}).get("max_total_positions") or 15
    max_core_alloc = portfolio.get("portfolio_rules", {}).get("max_core_allocation") or 0.7
    max_swing_alloc = portfolio.get("portfolio_rules", {}).get("max_swing_allocation") or 0.3
    max_position_weight = portfolio.get("portfolio_rules", {}).get("max_position_weight") or 0.12

    def sleeve_value(sleeve):
        return sum(float(p.get("market_value", 0.0)) for p in positions if p.get("sleeve") == sleeve)

    total_equity = float(portfolio.get("total_equity", cash)) or cash

    for d in decisions:
        ticker = d.get("ticker")
        action = d.get("action")
        price = latest_price_from_snapshot(price_snapshot, ticker)
        position_before = next((p for p in positions if p.get("ticker") == ticker), None)
        if ticker in conflicted:
            reject(logs, d, "conflicting_buy_sell_instructions", cash, cash, position_before, position_before, price)
            continue
        if price is None:
            reject(logs, d, "missing_execution_price", cash, cash, position_before, position_before, price)
            continue
        if action in {"HOLD", "REVIEW"}:
            logs.append(make_log(execution_id=gen_id("exe"), linked_decision_id=d["decision_id"], timestamp=now_iso(), ticker=ticker,
                                 requested_action=action, execution_status="EXECUTED", rejection_reason=None, execution_price=price,
                                 shares_filled=0, cash_before=cash, cash_after=cash, position_before=position_before, position_after=position_before,
                                 realized_pnl_change=0.0, notes=d.get("reason_code")))
            continue
        if action == "BUY":
            if position_before is not None:
                reject(logs, d, "duplicate_existing_position", cash, cash, position_before, position_before, price)
                continue
            if len(positions) >= max_total_positions:
                reject(logs, d, "max_total_positions_exceeded", cash, cash, None, None, price)
                continue
            target_notional = d.get("target_notional") or 0
            shares = d.get("target_shares") or int(float(target_notional) // price)
            if shares <= 0:
                reject(logs, d, "non_positive_share_request", cash, cash, None, None, price)
                continue
            notional = round(shares * price, 2)
            if notional > cash:
                reject(logs, d, "insufficient_cash", cash, cash, None, None, price)
                continue
            if (notional / max(total_equity, 1.0)) > max_position_weight:
                reject(logs, d, "position_weight_exceeded", cash, cash, None, None, price)
                continue
            if d["sleeve"] == "CORE" and ((sleeve_value("CORE") + notional) / max(total_equity, 1.0)) > max_core_alloc:
                reject(logs, d, "core_allocation_exceeded", cash, cash, None, None, price)
                continue
            if d["sleeve"] == "SWING" and ((sleeve_value("SWING") + notional) / max(total_equity, 1.0)) > max_swing_alloc:
                reject(logs, d, "swing_allocation_exceeded", cash, cash, None, None, price)
                continue
            cash_before = cash
            cash = round(cash - notional, 2)
            pos = {
                "ticker": ticker,
                "sleeve": d["sleeve"],
                "shares": shares,
                "avg_cost": price,
                "last_price": price,
                "market_value": round(shares * price, 2),
                "unrealized_pnl": 0.0,
                "entry_date": now_iso(),
                "thesis_summary": d["thesis_summary"],
                "stop_rule": d.get("stop_loss_rule"),
                "take_profit_rule": d.get("take_profit_rule"),
                "max_holding_period_days": d.get("max_holding_period_days"),
                "holding_days": 0,
            }
            positions.append(pos)
            portfolio.setdefault("trade_history", []).append({"timestamp": now_iso(), "ticker": ticker, "action": "BUY", "shares": shares, "price": price, "notional": notional, "reason_code": d["reason_code"], "sleeve": d["sleeve"]})
            logs.append(make_log(execution_id=gen_id("exe"), linked_decision_id=d["decision_id"], timestamp=now_iso(), ticker=ticker,
                                 requested_action="BUY", execution_status="EXECUTED", rejection_reason=None, execution_price=price,
                                 shares_filled=shares, cash_before=cash_before, cash_after=cash, position_before=None, position_after=pos,
                                 realized_pnl_change=0.0, notes=d.get("reason_code")))
        elif action == "SELL":
            if position_before is None:
                reject(logs, d, "selling_nonexistent_position", cash, cash, None, None, price)
                continue
            shares = int(position_before.get("shares", 0))
            notional = round(shares * price, 2)
            realized = round((price - float(position_before.get("avg_cost", 0.0))) * shares, 2)
            cash_before = cash
            cash = round(cash + notional, 2)
            portfolio["realized_pnl"] = round(float(portfolio.get("realized_pnl", 0.0)) + realized, 2)
            positions = [p for p in positions if p.get("ticker") != ticker]
            portfolio.setdefault("trade_history", []).append({"timestamp": now_iso(), "ticker": ticker, "action": "SELL", "shares": shares, "price": price, "notional": notional, "reason_code": d["reason_code"], "realized_pnl": realized, "sleeve": position_before.get("sleeve")})
            logs.append(make_log(execution_id=gen_id("exe"), linked_decision_id=d["decision_id"], timestamp=now_iso(), ticker=ticker,
                                 requested_action="SELL", execution_status="EXECUTED", rejection_reason=None, execution_price=price,
                                 shares_filled=shares, cash_before=cash_before, cash_after=cash, position_before=position_before, position_after=None,
                                 realized_pnl_change=realized, notes=d.get("reason_code")))

    portfolio["positions"] = positions
    total_equity = cash
    unrealized = 0.0
    for p in positions:
        price = latest_price_from_snapshot(price_snapshot, p["ticker"])
        if price is None:
            continue
        p["last_price"] = price
        p["market_value"] = round(int(p["shares"]) * price, 2)
        p["unrealized_pnl"] = round((price - float(p["avg_cost"])) * int(p["shares"]), 2)
        total_equity += p["market_value"]
        unrealized += p["unrealized_pnl"]
    portfolio["cash"] = cash
    portfolio["total_equity"] = round(total_equity, 2)
    portfolio["unrealized_pnl"] = round(unrealized, 2)
    portfolio["last_updated"] = now_iso()
    write_json(LEDGER_DIR / "mock_portfolio.json", portfolio)
    write_json(EXECUTION_DATA_DIR / "execution_log.json", {"generated_at": now_iso(), "execution_schema_version": "v1", "items": logs})
    print(f"Executor finished: {len(logs)} execution log records")


if __name__ == "__main__":
    main()
