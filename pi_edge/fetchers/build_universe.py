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

from runtime.common.common import CONFIG_DIR, MARKET_DATA_DIR, now_iso, read_json, write_json

SNAPSHOT_PATH = CONFIG_DIR / "sp500_constituents.json"


def normalize_ticker(symbol: str) -> str:
    return symbol.strip().upper().replace(".", "-")


def dedupe_keep_order(symbols: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for symbol in symbols:
        if symbol and symbol not in seen:
            seen.add(symbol)
            out.append(symbol)
    return out


def load_snapshot_tickers() -> tuple[list[str], dict]:
    payload = read_json(SNAPSHOT_PATH, {})
    if not payload:
        raise SystemExit("Missing S&P 500 snapshot: run .venv/bin/python pi_edge/fetchers/refresh_sp500_constituents.py")
    tickers = dedupe_keep_order([normalize_ticker(x) for x in payload.get("tickers", [])])
    if len(tickers) < 400:
        raise SystemExit("Invalid S&P 500 snapshot: snapshot too small or malformed; refresh it with pi_edge/fetchers/refresh_sp500_constituents.py")
    return tickers, payload


def main() -> None:
    tickers, payload = load_snapshot_tickers()
    counts = payload.get("counts", {})
    write_json(MARKET_DATA_DIR / "universe.json", {
        "generated_at": now_iso(),
        "market": "US_STOCKS",
        "universe_version": "sp500_snapshot_v1",
        "selection_method": "snapshot_sp500_intersect_alpaca_tradable_us_equities",
        "membership_source": {
            "runtime_source_of_truth": "config/sp500_constituents.json",
            "refresh_membership_source": payload.get("source", {}).get("membership", {}),
            "refresh_tradability_filter": payload.get("source", {}).get("tradability_filter", {}),
        },
        "snapshot_generated_at": payload.get("generated_at"),
        "snapshot_counts": counts,
        "tickers": tickers,
        "notes": "Runtime universe is built deterministically from the local S&P 500 snapshot. Refresh the snapshot explicitly before relying on membership updates."
    })
    print(f"Universe built: {len(tickers)} tickers from snapshot")


if __name__ == "__main__":
    main()
