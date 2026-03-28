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
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

from runtime.common.common import CONFIG_DIR, env_str, load_local_env_file


class AlpacaPaperClient:
    def __init__(self) -> None:
        load_local_env_file(CONFIG_DIR / "alpaca.env")
        self.api_key = env_str("ALPACA_API_KEY")
        self.api_secret = env_str("ALPACA_API_SECRET")
        self.base_url = env_str("ALPACA_BASE_URL", "https://paper-api.alpaca.markets")
        if not self.api_key or not self.api_secret:
            raise RuntimeError("missing_alpaca_credentials")

    def _headers(self) -> dict[str, str]:
        return {
            "APCA-API-KEY-ID": self.api_key,
            "APCA-API-SECRET-KEY": self.api_secret,
            "Content-Type": "application/json",
        }

    def _request(self, method: str, path: str, payload: dict[str, Any] | None = None, params: dict[str, Any] | None = None) -> dict[str, Any]:
        url = f"{self.base_url}{path}"
        if params:
            query = urllib.parse.urlencode({k: v for k, v in params.items() if v is not None})
            if query:
                url = f"{url}?{query}"
        data = None if payload is None else json.dumps(payload).encode("utf-8")
        request = urllib.request.Request(url, data=data, method=method, headers=self._headers())
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                body = response.read().decode("utf-8")
                return json.loads(body) if body else {}
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8") if exc.fp else ""
            detail = body
            try:
                parsed = json.loads(body) if body else {}
                detail = parsed.get("message") or parsed.get("code") or body
            except Exception:
                pass
            raise RuntimeError(f"alpaca_http_{exc.code}:{detail}") from exc
        except urllib.error.URLError as exc:
            raise RuntimeError(f"alpaca_network_error:{exc}") from exc

    def get_account(self) -> dict[str, Any]:
        return self._request("GET", "/v2/account")

    def get_positions(self) -> list[dict[str, Any]]:
        return self._request("GET", "/v2/positions")

    def list_orders(self, status: str = "open", limit: int = 100) -> list[dict[str, Any]]:
        return self._request("GET", "/v2/orders", params={"status": status, "limit": limit, "direction": "desc"})

    def list_assets(self, status: str = "active", asset_class: str = "us_equity") -> list[dict[str, Any]]:
        return self._request("GET", "/v2/assets", params={"status": status, "asset_class": asset_class})

    def submit_order(self, *, symbol: str, side: str, qty: int, order_type: str = "market", time_in_force: str = "day", client_order_id: str | None = None, extended_hours: bool = False) -> dict[str, Any]:
        payload = {
            "symbol": symbol,
            "side": side.lower(),
            "type": order_type,
            "time_in_force": time_in_force,
            "qty": str(qty),
            "extended_hours": bool(extended_hours),
        }
        if client_order_id:
            payload["client_order_id"] = client_order_id
        return self._request("POST", "/v2/orders", payload=payload)


def build_broker_snapshot(client: AlpacaPaperClient) -> dict[str, Any]:
    account = client.get_account()
    positions = client.get_positions()
    return {
        "account": account,
        "positions": positions,
    }
