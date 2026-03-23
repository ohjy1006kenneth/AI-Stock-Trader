from __future__ import annotations

from common import DATA_DIR, OUTPUTS_DIR, now_iso, read_json, write_json


PRICE_PERIOD = "18mo"


def fetch_history_for_ticker(ticker: str) -> dict:
    import yfinance as yf

    history = yf.Ticker(ticker).history(period=PRICE_PERIOD, interval="1d", auto_adjust=False)
    if history is None or history.empty:
        return {"ticker": ticker, "rows": [], "error": "no_price_history"}

    rows = []
    for date_index, row in history.iterrows():
        rows.append({
            "date": str(date_index.date()),
            "open": round(float(row.get("Open", 0.0)), 6),
            "high": round(float(row.get("High", 0.0)), 6),
            "low": round(float(row.get("Low", 0.0)), 6),
            "close": round(float(row.get("Close", 0.0)), 6),
            "adj_close": round(float(row.get("Close", 0.0)), 6),
            "volume": int(float(row.get("Volume", 0.0))),
        })
    return {"ticker": ticker, "rows": rows}


def main() -> None:
    universe = read_json(OUTPUTS_DIR / "universe.json", {"tickers": []})
    items = [fetch_history_for_ticker(ticker) for ticker in universe.get("tickers", [])]
    payload = {
        "generated_at": now_iso(),
        "source": "yfinance",
        "period": PRICE_PERIOD,
        "items": items,
    }
    write_json(DATA_DIR / "price_history.json", payload)
    print(f"Fetched price history for {len(items)} tickers")


if __name__ == "__main__":
    main()
