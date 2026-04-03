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

from datetime import datetime, timedelta, timezone
import json
import urllib.parse
import urllib.request
from typing import Any

from runtime.common.common import CONFIG_DIR, MARKET_DATA_DIR, gen_id, load_local_env_file, env_str, now_iso, read_json, write_json

DEFAULT_HISTORY_MONTHS = 36
BATCH_SIZE = 50
ALWAYS_INCLUDE_TICKERS = ("SPY",)


def _alpaca_symbol(symbol: str) -> str:
    return str(symbol).strip().upper().replace("-", ".")


def _rfc3339_z(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _history_window() -> tuple[str, str]:
    end_dt = datetime.now(timezone.utc)
    start_dt = end_dt - timedelta(days=DEFAULT_HISTORY_MONTHS * 31)
    return _rfc3339_z(start_dt), _rfc3339_z(end_dt)


class AlpacaHistoricalDataClient:
    def __init__(self) -> None:
        load_local_env_file(CONFIG_DIR / "alpaca.env")
        self.api_key = env_str("ALPACA_API_KEY")
        self.api_secret = env_str("ALPACA_API_SECRET")
        self.base_url = env_str("ALPACA_DATA_BASE_URL", "https://data.alpaca.markets")
        self.feed = env_str("ALPACA_DATA_FEED", "iex")
        if not self.api_key or not self.api_secret:
            raise RuntimeError("missing_alpaca_credentials")

    def _headers(self) -> dict[str, str]:
        return {
            "APCA-API-KEY-ID": self.api_key,
            "APCA-API-SECRET-KEY": self.api_secret,
            "Content-Type": "application/json",
        }

    def _request(self, path: str, params: dict[str, Any]) -> dict[str, Any]:
        query = urllib.parse.urlencode({k: v for k, v in params.items() if v is not None})
        url = f"{self.base_url}{path}?{query}"
        request = urllib.request.Request(url, method="GET", headers=self._headers())
        with urllib.request.urlopen(request, timeout=60) as response:
            body = response.read().decode("utf-8")
            return json.loads(body) if body else {}

    def get_daily_bars(self, tickers: list[str], *, start: str, end: str) -> dict[str, list[dict[str, Any]]]:
        out: dict[str, list[dict[str, Any]]] = {ticker: [] for ticker in tickers}
        symbol_map = {_alpaca_symbol(ticker): ticker for ticker in tickers}
        page_token: str | None = None
        while True:
            payload = self._request(
                "/v2/stocks/bars",
                {
                    "symbols": ",".join(symbol_map.keys()),
                    "timeframe": "1Day",
                    "start": start,
                    "end": end,
                    "adjustment": "raw",
                    "feed": self.feed,
                    "limit": 10000,
                    "sort": "asc",
                    "page_token": page_token,
                },
            )
            bars = payload.get("bars", {})
            for response_symbol, rows in bars.items():
                ticker = symbol_map.get(str(response_symbol).upper(), str(response_symbol).upper())
                normalized: list[dict[str, Any]] = []
                for row in rows:
                    ts = str(row.get("t") or "")
                    normalized.append({
                        "date": ts[:10],
                        "open": float(row.get("o", 0.0) or 0.0),
                        "high": float(row.get("h", 0.0) or 0.0),
                        "low": float(row.get("l", 0.0) or 0.0),
                        "close": float(row.get("c", 0.0) or 0.0),
                        "adj_close": float(row.get("c", 0.0) or 0.0),
                        "volume": int(float(row.get("v", 0.0) or 0.0)),
                    })
                out[ticker].extend(normalized)
            page_token = payload.get("next_page_token")
            if not page_token:
                break
        return out


def _chunked(items: list[str], size: int) -> list[list[str]]:
    return [items[idx: idx + size] for idx in range(0, len(items), size)]


def _load_universe_tickers() -> list[str]:
    universe = read_json(MARKET_DATA_DIR / "universe.json", {"tickers": []})
    ordered: list[str] = []
    seen: set[str] = set()
    for ticker in list(universe.get("tickers", [])) + list(ALWAYS_INCLUDE_TICKERS):
        symbol = str(ticker).strip().upper()
        if symbol and symbol not in seen:
            ordered.append(symbol)
            seen.add(symbol)
    return ordered


def main() -> None:
    tickers = _load_universe_tickers()
    if not tickers:
        raise SystemExit("no_universe_tickers_available")

    client = AlpacaHistoricalDataClient()
    start, end = _history_window()
    history_map: dict[str, list[dict[str, Any]]] = {ticker: [] for ticker in tickers}
    for batch in _chunked(tickers, BATCH_SIZE):
        batch_history = client.get_daily_bars(batch, start=start, end=end)
        history_map.update(batch_history)

    items = []
    for ticker in tickers:
        rows = history_map.get(ticker, [])
        latest = rows[-1] if rows else {}
        items.append({
            "snapshot_id": gen_id("px"),
            "ticker": ticker,
            "timestamp": now_iso(),
            "open": latest.get("open"),
            "high": latest.get("high"),
            "low": latest.get("low"),
            "close": latest.get("close"),
            "adj_close": latest.get("adj_close"),
            "volume": latest.get("volume"),
            "history": rows,
        })

    write_json(MARKET_DATA_DIR / "price_snapshot.json", {
        "generated_at": now_iso(),
        "source": "alpaca_historical_api",
        "history_months": DEFAULT_HISTORY_MONTHS,
        "always_include_tickers": list(ALWAYS_INCLUDE_TICKERS),
        "fallback_source_used": False,
        "items": items,
    })
    print(f"Price snapshot written from Alpaca: {len(items)} tickers, lookback_months={DEFAULT_HISTORY_MONTHS}")


if __name__ == "__main__":
    main()
