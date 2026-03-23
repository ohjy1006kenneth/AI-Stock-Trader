from __future__ import annotations

from common import DATA_DIR, OUTPUTS_DIR, now_iso, read_json, write_json


def fetch_fundamentals_for_ticker(ticker: str) -> dict:
    import yfinance as yf

    info = yf.Ticker(ticker).info or {}
    return {
        "ticker": ticker,
        "as_of_date": now_iso(),
        "source": "yfinance",
        "revenue_growth": info.get("revenueGrowth"),
        "net_margin": info.get("profitMargins"),
        "operating_margin": info.get("operatingMargins"),
        "debt_to_equity": info.get("debtToEquity"),
        "free_cash_flow": info.get("freeCashflow"),
        "return_on_equity": info.get("returnOnEquity"),
        "market_cap": info.get("marketCap"),
        "average_volume": info.get("averageVolume"),
        "sector": info.get("sector"),
        "industry": info.get("industry"),
        "earnings_timestamp": info.get("earningsTimestamp"),
        "quote_type": info.get("quoteType"),
        "country": info.get("country")
    }


def main() -> None:
    universe = read_json(OUTPUTS_DIR / "universe.json", {"tickers": []})
    items = [fetch_fundamentals_for_ticker(ticker) for ticker in universe.get("tickers", [])]
    payload = {
        "generated_at": now_iso(),
        "source": "yfinance",
        "items": items,
        "notes": "Convenience fundamentals for prototyping. SEC/XBRL should replace or validate critical production fields later."
    }
    write_json(DATA_DIR / "fundamental_data.json", payload)
    print(f"Fetched fundamentals for {len(items)} tickers")


if __name__ == "__main__":
    main()
