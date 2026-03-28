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

from runtime.common.common import ALERTS_DATA_DIR, EXECUTION_DATA_DIR, LEDGER_DIR, now_iso, read_json, write_json, write_text

STATE_PATH = ALERTS_DATA_DIR / "alert_state.json"
ALERT_REPORT_PATH = ALERTS_DATA_DIR / "trade_alerts_latest.json"
ALERT_TEXT_PATH = ALERTS_DATA_DIR / "trade_alerts_latest.txt"


def main() -> None:
    execution = read_json(EXECUTION_DATA_DIR / "execution_log.json", {"items": []})
    portfolio = read_json(LEDGER_DIR / "mock_portfolio.json", {})
    state = read_json(STATE_PATH, {"alerted_execution_ids": []})
    alerted = set(state.get("alerted_execution_ids", []))

    items = []
    text_lines = []
    for row in execution.get("items", []):
        if row.get("execution_status") != "EXECUTED":
            continue
        if row.get("requested_action") not in {"BUY", "SELL"}:
            continue
        execution_id = row.get("execution_id")
        if execution_id in alerted:
            continue
        trade = next((t for t in reversed(portfolio.get("trade_history", [])) if t.get("ticker") == row.get("ticker") and t.get("action") == row.get("requested_action")), None)
        sleeve = trade.get("sleeve") if trade else None
        shares = row.get("shares_filled")
        execution_price = row.get("execution_price")
        reason_code = row.get("notes")
        cash_after = row.get("cash_after")
        total_equity = portfolio.get("total_equity")
        broker_status = row.get("broker_status")
        payload = {
            "timestamp": row.get("timestamp"),
            "ticker": row.get("ticker"),
            "action": row.get("requested_action"),
            "sleeve": sleeve,
            "shares": shares,
            "execution_price": execution_price,
            "reason_code": reason_code,
            "remaining_cash": cash_after,
            "total_equity_after": total_equity,
            "execution_id": execution_id,
            "execution_mode": row.get("execution_mode"),
            "broker_name": row.get("broker_name"),
            "broker_order_id": row.get("broker_order_id"),
            "broker_status": broker_status,
        }
        items.append(payload)
        text_lines.extend([
            "PAPER EXECUTION ALERT" if payload.get("execution_mode") == "paper" else "MOCK EXECUTION ALERT",
            f"- timestamp: {payload['timestamp']}",
            f"- ticker: {payload['ticker']}",
            f"- action: {payload['action']}",
            f"- sleeve: {payload['sleeve']}",
            f"- shares: {payload['shares']}",
            f"- execution price: {payload['execution_price']}",
            f"- broker: {payload.get('broker_name')}",
            f"- broker status: {payload.get('broker_status')}",
            f"- broker order id: {payload.get('broker_order_id')}",
            f"- reason code: {payload['reason_code']}",
            f"- remaining cash: {payload['remaining_cash']}",
            f"- total equity after: {payload['total_equity_after']}",
            "",
        ])
        alerted.add(execution_id)

    write_json(ALERT_REPORT_PATH, {"generated_at": now_iso(), "execution_mode": execution.get("execution_mode"), "items": items})
    write_text(ALERT_TEXT_PATH, "\n".join(text_lines).strip() + ("\n" if text_lines else ""))
    write_json(STATE_PATH, {"alerted_execution_ids": sorted(alerted)})
    print("No new trade alerts" if not items else "\n".join(text_lines).strip())


if __name__ == "__main__":
    main()
