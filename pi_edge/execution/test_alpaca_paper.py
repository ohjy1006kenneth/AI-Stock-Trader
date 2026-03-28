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

from runtime.pi.execution.alpaca_paper import AlpacaPaperClient, build_broker_snapshot


def main() -> None:
    client = AlpacaPaperClient()
    snapshot = build_broker_snapshot(client)
    account = snapshot["account"]
    positions = snapshot["positions"]
    print(json.dumps({
        "status": "ok",
        "account_id": account.get("id"),
        "account_status": account.get("status"),
        "currency": account.get("currency"),
        "cash": account.get("cash"),
        "buying_power": account.get("buying_power"),
        "equity": account.get("equity"),
        "positions_count": len(positions),
    }, indent=2))


if __name__ == "__main__":
    main()
