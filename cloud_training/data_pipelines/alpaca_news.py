from __future__ import annotations

from pathlib import Path
import sys

ROOT_DIR = Path(__file__).resolve()
for _ in range(6):
    if (ROOT_DIR / ".gitignore").exists():
        break
    ROOT_DIR = ROOT_DIR.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import json
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from typing import Any

from runtime.common.common import CONFIG_DIR, env_str, load_local_env_file, now_iso

NEWS_CACHE_DIR = ROOT_DIR / "data" / "raw" / "alpaca_news"


def _alpaca_headers() -> dict[str, str]:
    load_local_env_file(CONFIG_DIR / "alpaca.env")
    api_key = env_str("ALPACA_API_KEY")
    api_secret = env_str("ALPACA_API_SECRET")
    if not api_key or not api_secret:
        raise RuntimeError("missing_alpaca_credentials")
    return {
        "APCA-API-KEY-ID": api_key,
        "APCA-API-SECRET-KEY": api_secret,
        "Content-Type": "application/json",
    }


def _news_base_url() -> str:
    load_local_env_file(CONFIG_DIR / "alpaca.env")
    return env_str("ALPACA_DATA_URL", "https://data.alpaca.markets")


def fetch_news(*, symbols: list[str], start_iso: str, end_iso: str, limit: int = 50) -> list[dict[str, Any]]:
    base = _news_base_url()
    query = urllib.parse.urlencode({
        "symbols": ",".join(symbols),
        "start": start_iso,
        "end": end_iso,
        "limit": limit,
        "sort": "asc",
        "include_content": "false",
    })
    url = f"{base}/v1beta1/news?{query}"
    req = urllib.request.Request(url, headers=_alpaca_headers(), method="GET")
    with urllib.request.urlopen(req, timeout=60) as response:
        payload = json.loads(response.read().decode("utf-8"))
    news_items = payload.get("news", [])
    normalized = []
    for item in news_items:
        normalized.append({
            "id": item.get("id"),
            "date": item.get("created_at") or item.get("updated_at"),
            "headline": item.get("headline") or "",
            "summary": item.get("summary") or "",
            "symbols": item.get("symbols") or [],
            "source": item.get("source"),
        })
    return normalized


def cache_news_batch(*, symbols: list[str], start_iso: str, end_iso: str, limit: int = 50) -> Path:
    NEWS_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    safe_name = "_".join(symbols[:5]) + ("_more" if len(symbols) > 5 else "")
    path = NEWS_CACHE_DIR / f"news_{safe_name}_{stamp}.json"
    payload = {
        "generated_at": now_iso(),
        "symbols": symbols,
        "start": start_iso,
        "end": end_iso,
        "items": fetch_news(symbols=symbols, start_iso=start_iso, end_iso=end_iso, limit=limit),
    }
    path.write_text(json.dumps(payload, indent=2))
    return path
