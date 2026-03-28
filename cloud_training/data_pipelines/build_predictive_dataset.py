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

import argparse
import csv
import json
import math
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any

from cloud_training.data_pipelines.alpaca_news import fetch_news
from runtime.common.common import MARKET_DATA_DIR, now_iso, read_json

OUTPUT_DIR = ROOT_DIR / "data" / "processed" / "training"
SEQUENCE_LENGTH = 21
DEFAULT_NEWS_LOOKBACK_DAYS = 1
DEFAULT_MAX_TICKERS = 25
DEFAULT_MAX_SAMPLES_PER_TICKER = 252


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build first predictive training dataset (OHLCV + Alpaca news, T+1 log return)")
    parser.add_argument("--max-tickers", type=int, default=DEFAULT_MAX_TICKERS, help="limit tickers for the first dataset pass")
    parser.add_argument("--max-samples-per-ticker", type=int, default=DEFAULT_MAX_SAMPLES_PER_TICKER)
    parser.add_argument("--sequence-length", type=int, default=SEQUENCE_LENGTH)
    parser.add_argument("--news-lookback-days", type=int, default=DEFAULT_NEWS_LOOKBACK_DAYS)
    parser.add_argument("--output-prefix", default="predictive_dataset_v1")
    return parser.parse_args()


def load_price_history() -> dict[str, list[dict[str, Any]]]:
    payload = read_json(MARKET_DATA_DIR / "price_snapshot.json", {"items": []})
    out: dict[str, list[dict[str, Any]]] = {}
    for item in payload.get("items", []):
        ticker = item.get("ticker")
        history = item.get("history", [])
        if ticker and history:
            out[ticker] = history
    return out


def normalize_news_items(items: list[dict[str, Any]]) -> list[dict[str, str]]:
    normalized = []
    for item in items:
        normalized.append({
            "date": str(item.get("date") or ""),
            "headline": str(item.get("headline") or ""),
            "summary": str(item.get("summary") or ""),
        })
    return normalized


def fetch_ticker_news_map(ticker: str, start_date: str, end_date: str, lookback_days: int) -> dict[str, list[dict[str, str]]]:
    start_dt = datetime.fromisoformat(start_date + "T00:00:00+00:00") - timedelta(days=lookback_days)
    end_dt = datetime.fromisoformat(end_date + "T23:59:59+00:00")
    news_items = fetch_news(symbols=[ticker], start_iso=start_dt.isoformat(), end_iso=end_dt.isoformat(), limit=1000)
    by_day: dict[str, list[dict[str, str]]] = defaultdict(list)
    for item in normalize_news_items(news_items):
        dt = item["date"]
        if not dt:
            continue
        by_day[dt[:10]].append(item)
    return dict(by_day)


def build_samples_for_ticker(ticker: str, history: list[dict[str, Any]], news_by_day: dict[str, list[dict[str, str]]], sequence_length: int, max_samples: int) -> list[dict[str, Any]]:
    rows = [row for row in history if all(k in row for k in ("date", "open", "high", "low", "close", "volume"))]
    rows.sort(key=lambda x: x["date"])
    samples: list[dict[str, Any]] = []
    for idx in range(sequence_length - 1, len(rows) - 1):
        window = rows[idx - sequence_length + 1: idx + 1]
        today = rows[idx]
        tomorrow = rows[idx + 1]
        close_t = float(today["close"])
        close_tp1 = float(tomorrow["close"])
        if close_t <= 0 or close_tp1 <= 0:
            continue
        target_log_return = math.log(close_tp1 / close_t)
        sample = {
            "ticker": ticker,
            "as_of_date": today["date"],
            "target_date": tomorrow["date"],
            "sequence_length": sequence_length,
            "history": [
                {
                    "date": row["date"],
                    "open": float(row["open"]),
                    "high": float(row["high"]),
                    "low": float(row["low"]),
                    "close": float(row["close"]),
                    "volume": int(row["volume"]),
                }
                for row in window
            ],
            "news": news_by_day.get(today["date"], []),
            "news_count": len(news_by_day.get(today["date"], [])),
            "target_log_return_t_plus_1": target_log_return,
            "target_positive_return": 1 if target_log_return > 0 else 0,
        }
        samples.append(sample)
    if max_samples > 0:
        samples = samples[-max_samples:]
    return samples


def write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as f:
        for row in rows:
            f.write(json.dumps(row) + "\n")


def write_manifest(path: Path, manifest: dict[str, Any]) -> None:
    path.write_text(json.dumps(manifest, indent=2))


def write_summary_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = ["ticker", "samples", "avg_news_count"]
    by_ticker: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        by_ticker[row["ticker"]].append(row)
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for ticker, items in sorted(by_ticker.items()):
            writer.writerow({
                "ticker": ticker,
                "samples": len(items),
                "avg_news_count": round(sum(x["news_count"] for x in items) / max(len(items), 1), 4),
            })


def main() -> None:
    args = parse_args()
    prices = load_price_history()
    tickers = sorted(prices.keys())[: args.max_tickers]
    if not tickers:
        raise SystemExit("no_price_history_available_for_dataset_build")

    all_rows: list[dict[str, Any]] = []
    ticker_stats: dict[str, dict[str, Any]] = {}
    for ticker in tickers:
        history = prices[ticker]
        start_date = history[0]["date"]
        end_date = history[-1]["date"]
        news_by_day = fetch_ticker_news_map(ticker, start_date, end_date, args.news_lookback_days)
        rows = build_samples_for_ticker(
            ticker=ticker,
            history=history,
            news_by_day=news_by_day,
            sequence_length=args.sequence_length,
            max_samples=args.max_samples_per_ticker,
        )
        all_rows.extend(rows)
        ticker_stats[ticker] = {
            "history_rows": len(history),
            "news_days": len(news_by_day),
            "samples": len(rows),
        }

    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    prefix = f"{args.output_prefix}_{stamp}"
    jsonl_path = OUTPUT_DIR / f"{prefix}.jsonl"
    manifest_path = OUTPUT_DIR / f"{prefix}.manifest.json"
    summary_csv = OUTPUT_DIR / f"{prefix}.summary.csv"

    write_jsonl(jsonl_path, all_rows)
    write_summary_csv(summary_csv, all_rows)
    write_manifest(manifest_path, {
        "generated_at": now_iso(),
        "dataset_name": prefix,
        "target": "next_day_log_return",
        "sequence_length": args.sequence_length,
        "news_source": "alpaca_news_api",
        "history_source": "price_snapshot_json",
        "tickers_considered": tickers,
        "ticker_stats": ticker_stats,
        "rows": len(all_rows),
        "jsonl_path": str(jsonl_path.relative_to(ROOT_DIR)),
        "summary_csv": str(summary_csv.relative_to(ROOT_DIR)),
    })
    print(json.dumps({
        "status": "ok",
        "rows": len(all_rows),
        "tickers": len(tickers),
        "jsonl": str(jsonl_path.relative_to(ROOT_DIR)),
        "manifest": str(manifest_path.relative_to(ROOT_DIR)),
        "summary_csv": str(summary_csv.relative_to(ROOT_DIR)),
    }, indent=2))


if __name__ == "__main__":
    main()
