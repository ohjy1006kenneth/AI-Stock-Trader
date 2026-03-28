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

from runtime.common.common import MARKET_DATA_DIR, gen_id, now_iso, read_json, write_json

PRICE_PERIOD = "18mo"


def fetch_one(ticker: str) -> dict:
    import yfinance as yf

    hist = yf.Ticker(ticker).history(period=PRICE_PERIOD, interval="1d", auto_adjust=False)
    rows = []
    if hist is not None and not hist.empty:
        for idx, row in hist.iterrows():
            rows.append({
                "date": str(idx.date()),
                "open": float(row.get("Open", 0.0)),
                "high": float(row.get("High", 0.0)),
                "low": float(row.get("Low", 0.0)),
                "close": float(row.get("Close", 0.0)),
                "adj_close": float(row.get("Close", 0.0)),
                "volume": int(float(row.get("Volume", 0.0))),
            })
    latest = rows[-1] if rows else {}
    return {
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
    }


def main() -> None:
    universe = read_json(MARKET_DATA_DIR / "universe.json", {"tickers": []})
    items = [fetch_one(t) for t in universe.get("tickers", [])]
    write_json(MARKET_DATA_DIR / "price_snapshot.json", {
        "generated_at": now_iso(),
        "source": "yfinance",
        "fallback_source_used": True,
        "items": items,
    })
    print(f"Price snapshot written: {len(items)} tickers")


if __name__ == "__main__":
    main()
