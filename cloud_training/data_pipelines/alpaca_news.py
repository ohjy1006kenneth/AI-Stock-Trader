from __future__ import annotations

from pathlib import Path
import sys

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import json
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from typing import Any

from runtime.common.common import CONFIG_DIR, env_str, load_local_env_file, now_iso

NEWS_CACHE_DIR = ROOT_DIR / "data" / "raw" / "alpaca_news"
MAX_NEWS_HTTP_RETRIES = 5


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


def _retry_after_seconds(error: Exception, attempt: int) -> float:
    headers = getattr(error, "headers", None)
    retry_after = headers.get("Retry-After") if headers is not None else None
    if retry_after:
        try:
            return max(float(retry_after), 1.0)
        except (TypeError, ValueError):
            pass
    return min(2.0 ** max(attempt - 1, 0), 30.0)


def _fetch_news_page(url: str, headers: dict[str, str]) -> dict[str, Any]:
    req = urllib.request.Request(url, headers=headers, method="GET")
    last_error: Exception | None = None
    for attempt in range(1, MAX_NEWS_HTTP_RETRIES + 1):
        try:
            with urllib.request.urlopen(req, timeout=60) as response:
                return json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as error:
            if error.code != 429 or attempt >= MAX_NEWS_HTTP_RETRIES:
                raise
            last_error = error
            time.sleep(_retry_after_seconds(error, attempt))
        except urllib.error.URLError as error:
            if attempt >= MAX_NEWS_HTTP_RETRIES:
                raise
            last_error = error
            time.sleep(min(2.0 ** max(attempt - 1, 0), 15.0))
    if last_error is not None:
        raise last_error
    raise RuntimeError("unexpected_news_fetch_retry_state")


def fetch_news(*, symbols: list[str], start_iso: str, end_iso: str, limit: int = 50) -> list[dict[str, Any]]:
    base = _news_base_url()
    headers = _alpaca_headers()
    remaining = max(int(limit), 0)
    page_size = 50 if remaining == 0 else min(50, remaining)
    next_page_token: str | None = None
    news_items: list[dict[str, Any]] = []

    while True:
        query_params = {
            "symbols": ",".join(symbols),
            "start": start_iso,
            "end": end_iso,
            "limit": page_size,
            "sort": "asc",
            "include_content": "false",
        }
        if next_page_token:
            query_params["page_token"] = next_page_token
        query = urllib.parse.urlencode(query_params)
        url = f"{base}/v1beta1/news?{query}"
        payload = _fetch_news_page(url, headers)

        batch = payload.get("news", [])
        news_items.extend(batch)
        next_page_token = payload.get("next_page_token")
        if not next_page_token or not batch:
            break
        if remaining > 0:
            remaining -= len(batch)
            if remaining <= 0:
                break
            page_size = min(50, remaining)
        time.sleep(0.25)

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
