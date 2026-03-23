from __future__ import annotations

from common import OUTPUTS_DIR, now_iso, read_json, write_json


def fetch_one(ticker: str) -> dict:
    import yfinance as yf

    info = yf.Ticker(ticker).info or {}
    return {
        "ticker": ticker,
        "timestamp": now_iso(),
        "country": info.get("country"),
        "quote_type": info.get("quoteType"),
        "sector": info.get("sector"),
        "industry": info.get("industry"),
        "market_cap": info.get("marketCap"),
        "average_volume": info.get("averageVolume"),
        "net_margin": info.get("profitMargins"),
        "debt_to_equity": info.get("debtToEquity"),
        "free_cash_flow": info.get("freeCashflow"),
        "revenue_growth": info.get("revenueGrowth"),
        "operating_margin": info.get("operatingMargins"),
        "return_on_equity": info.get("returnOnEquity"),
        "earnings_timestamp": info.get("earningsTimestamp"),
    }


def main() -> None:
    universe = read_json(OUTPUTS_DIR / "universe.json", {"tickers": []})
    items = [fetch_one(t) for t in universe.get("tickers", [])]
    write_json(OUTPUTS_DIR / "fundamental_snapshot.json", {
        "generated_at": now_iso(),
        "source": "yfinance",
        "fallback_source_used": True,
        "items": items,
        "notes": "Convenience prototype fundamentals only; SEC/XBRL should replace or validate critical production fields."
    })
    print(f"Fundamental snapshot written: {len(items)} tickers")


if __name__ == "__main__":
    main()
