from __future__ import annotations

from common import DATA_DIR, LEDGER_DIR, OUTPUTS_DIR, now_iso, read_json, write_json

VALID_ACTIONS = {"BUY", "SELL", "HOLD", "UPDATE_SLEEVE"}
VALID_SLEEVES = {"CORE", "SWING"}


def latest_price_for_ticker(price_history: dict, ticker: str) -> float | None:
    for item in price_history.get("items", []):
        if item.get("ticker") == ticker:
            rows = item.get("rows", [])
            if rows:
                return float(rows[-1]["close"])
    return None


def portfolio_allocations(portfolio: dict) -> tuple[float, float]:
    total_equity = float(portfolio.get("total_equity", 0.0) or 0.0)
    if total_equity <= 0:
        return 0.0, 0.0
    core_value = 0.0
    swing_value = 0.0
    for pos in portfolio.get("open_positions", []):
        mv = float(pos.get("market_value", 0.0) or 0.0)
        if pos.get("sleeve") == "CORE":
            core_value += mv
        elif pos.get("sleeve") == "SWING":
            swing_value += mv
    return core_value / total_equity, swing_value / total_equity


def reject(logs: list[dict], decision: dict, status: str, reason_code: str, execution_price: float | None = None) -> None:
    logs.append({
        "timestamp": now_iso(),
        "ticker": decision.get("ticker"),
        "action": decision.get("action"),
        "status": status,
        "reason_code": reason_code,
        "execution_price": execution_price,
        "decision": decision,
    })


def execute_buy(portfolio: dict, decision: dict, price: float, execution_logs: list[dict]) -> None:
    cash = float(portfolio.get("cash", 0.0))
    total_equity = float(portfolio.get("total_equity", cash))
    open_positions = portfolio.get("open_positions", [])
    risk_limits = portfolio.get("risk_limits", {})
    ticker = decision["ticker"]
    sleeve = decision.get("sleeve")
    target_weight = float(decision.get("target_weight", 0.0))

    if sleeve not in VALID_SLEEVES:
        reject(execution_logs, decision, "REJECTED", "invalid_sleeve_label", price)
        return
    if any(pos.get("ticker") == ticker for pos in open_positions):
        reject(execution_logs, decision, "REJECTED", "duplicate_existing_position", price)
        return
    if len(open_positions) >= int(risk_limits.get("max_total_positions", 15)):
        reject(execution_logs, decision, "REJECTED", "max_total_positions_exceeded", price)
        return

    core_alloc, swing_alloc = portfolio_allocations(portfolio)
    if sleeve == "CORE" and target_weight > float(risk_limits.get("max_core_weight", 0.12)):
        reject(execution_logs, decision, "REJECTED", "core_position_weight_exceeded", price)
        return
    if sleeve == "SWING" and target_weight > float(risk_limits.get("max_swing_weight", 0.06)):
        reject(execution_logs, decision, "REJECTED", "swing_position_weight_exceeded", price)
        return
    if sleeve == "CORE" and core_alloc + target_weight > float(risk_limits.get("max_core_allocation", 0.70)):
        reject(execution_logs, decision, "REJECTED", "core_sleeve_allocation_exceeded", price)
        return
    if sleeve == "SWING" and swing_alloc + target_weight > float(risk_limits.get("max_swing_allocation", 0.30)):
        reject(execution_logs, decision, "REJECTED", "swing_sleeve_allocation_exceeded", price)
        return

    target_notional = total_equity * target_weight
    qty = int(target_notional // price)
    if qty <= 0:
        reject(execution_logs, decision, "REJECTED", "non_positive_quantity", price)
        return
    notional = round(qty * price, 2)
    remaining_cash = round(cash - notional, 2)
    min_cash_buffer = float(risk_limits.get("min_cash_buffer", 0.05)) * total_equity
    if notional > cash:
        reject(execution_logs, decision, "REJECTED", "insufficient_cash", price)
        return
    if remaining_cash < min_cash_buffer:
        reject(execution_logs, decision, "REJECTED", "cash_buffer_breach", price)
        return

    portfolio["cash"] = remaining_cash
    position = {
        "ticker": ticker,
        "sleeve": sleeve,
        "qty": qty,
        "avg_cost": price,
        "last_price": price,
        "market_value": round(qty * price, 2),
        "unrealized_pnl": 0.0,
        "opened_at": now_iso(),
        "holding_days": 0,
        "risk_rules": decision.get("risk_rules", {}),
        "entry_reason": decision.get("reason"),
    }
    portfolio.setdefault("open_positions", []).append(position)
    portfolio.setdefault("trade_history", []).append({
        "timestamp": now_iso(),
        "ticker": ticker,
        "side": "BUY",
        "sleeve": sleeve,
        "qty": qty,
        "price": price,
        "notional": notional,
        "reason": decision.get("reason"),
    })
    execution_logs.append({
        "timestamp": now_iso(),
        "ticker": ticker,
        "action": "BUY",
        "status": "EXECUTED",
        "reason_code": decision.get("reason"),
        "execution_price": price,
        "qty": qty,
        "notional": notional,
    })


def execute_sell(portfolio: dict, decision: dict, price: float, execution_logs: list[dict]) -> None:
    ticker = decision["ticker"]
    open_positions = portfolio.get("open_positions", [])
    target = next((pos for pos in open_positions if pos.get("ticker") == ticker), None)
    if target is None:
        reject(execution_logs, decision, "REJECTED", "position_not_found", price)
        return

    qty = int(target.get("qty", 0))
    if qty <= 0:
        reject(execution_logs, decision, "REJECTED", "invalid_position_quantity", price)
        return

    notional = round(qty * price, 2)
    avg_cost = float(target.get("avg_cost", 0.0))
    realized_pnl = round((price - avg_cost) * qty, 2)
    portfolio["cash"] = round(float(portfolio.get("cash", 0.0)) + notional, 2)
    portfolio["realized_pnl"] = round(float(portfolio.get("realized_pnl", 0.0)) + realized_pnl, 2)
    portfolio["open_positions"] = [pos for pos in open_positions if pos.get("ticker") != ticker]
    portfolio.setdefault("trade_history", []).append({
        "timestamp": now_iso(),
        "ticker": ticker,
        "side": "SELL",
        "sleeve": target.get("sleeve"),
        "qty": qty,
        "price": price,
        "notional": notional,
        "reason": decision.get("reason"),
        "realized_pnl": realized_pnl,
    })
    execution_logs.append({
        "timestamp": now_iso(),
        "ticker": ticker,
        "action": "SELL",
        "status": "EXECUTED",
        "reason_code": decision.get("reason"),
        "execution_price": price,
        "qty": qty,
        "notional": notional,
        "realized_pnl": realized_pnl,
    })


def execute_hold(decision: dict, price: float, execution_logs: list[dict]) -> None:
    execution_logs.append({
        "timestamp": now_iso(),
        "ticker": decision.get("ticker"),
        "action": "HOLD",
        "status": "EXECUTED",
        "reason_code": decision.get("reason"),
        "execution_price": price,
    })


def execute_update_sleeve(portfolio: dict, decision: dict, price: float, execution_logs: list[dict]) -> None:
    ticker = decision.get("ticker")
    new_sleeve = decision.get("sleeve")
    if new_sleeve not in VALID_SLEEVES:
        reject(execution_logs, decision, "REJECTED", "invalid_sleeve_label", price)
        return
    target = next((pos for pos in portfolio.get("open_positions", []) if pos.get("ticker") == ticker), None)
    if target is None:
        reject(execution_logs, decision, "REJECTED", "position_not_found", price)
        return
    old_sleeve = target.get("sleeve")
    target["sleeve"] = new_sleeve
    execution_logs.append({
        "timestamp": now_iso(),
        "ticker": ticker,
        "action": "UPDATE_SLEEVE",
        "status": "EXECUTED",
        "reason_code": decision.get("reason"),
        "execution_price": price,
        "old_sleeve": old_sleeve,
        "new_sleeve": new_sleeve,
    })


def main() -> None:
    portfolio = read_json(LEDGER_DIR / "mock_portfolio.json", {})
    decisions_payload = read_json(OUTPUTS_DIR / "strategist_decisions.json", {"decisions": []})
    price_history = read_json(DATA_DIR / "price_history.json", {"items": []})
    execution_logs: list[dict] = []

    decisions = decisions_payload.get("decisions", [])
    actions_by_ticker: dict[str, set[str]] = {}
    for decision in decisions:
        ticker = decision.get("ticker")
        action = decision.get("action")
        if not ticker or action not in VALID_ACTIONS:
            reject(execution_logs, decision, "REJECTED", "invalid_decision_schema")
            continue
        actions_by_ticker.setdefault(ticker, set()).add(action)

    conflicted_tickers = {
        ticker for ticker, actions in actions_by_ticker.items()
        if "BUY" in actions and "SELL" in actions
    }

    for decision in decisions:
        ticker = decision.get("ticker")
        action = decision.get("action")
        if ticker in conflicted_tickers:
            reject(execution_logs, decision, "PARTIALLY_REJECTED", "conflicting_buy_sell_instructions")
            continue
        if action not in VALID_ACTIONS:
            reject(execution_logs, decision, "REJECTED", "invalid_action")
            continue

        execution_price = latest_price_for_ticker(price_history, ticker)
        if execution_price is None:
            reject(execution_logs, decision, "REJECTED", "missing_execution_price")
            continue

        if action == "BUY":
            execute_buy(portfolio, decision, execution_price, execution_logs)
        elif action == "SELL":
            execute_sell(portfolio, decision, execution_price, execution_logs)
        elif action == "HOLD":
            execute_hold(decision, execution_price, execution_logs)
        elif action == "UPDATE_SLEEVE":
            execute_update_sleeve(portfolio, decision, execution_price, execution_logs)

    total_equity = float(portfolio.get("cash", 0.0))
    unrealized_pnl = 0.0
    for position in portfolio.get("open_positions", []):
        price = latest_price_for_ticker(price_history, position["ticker"])
        if price is None:
            continue
        qty = int(position.get("qty", 0))
        market_value = round(qty * price, 2)
        pnl = round((price - float(position.get("avg_cost", 0.0))) * qty, 2)
        position["last_price"] = price
        position["market_value"] = market_value
        position["unrealized_pnl"] = pnl
        total_equity += market_value
        unrealized_pnl += pnl

    portfolio["total_equity"] = round(total_equity, 2)
    portfolio["unrealized_pnl"] = round(unrealized_pnl, 2)
    portfolio["last_updated"] = now_iso()

    write_json(LEDGER_DIR / "mock_portfolio.json", portfolio)
    write_json(OUTPUTS_DIR / "execution_log.json", {
        "generated_at": now_iso(),
        "items": execution_logs,
    })
    print(f"Executor processed {len(decisions)} decisions; logged {len(execution_logs)} outcomes")


if __name__ == "__main__":
    main()
