from __future__ import annotations

from common import DATA_DIR, OUTPUTS_DIR, now_iso, read_json, write_json


def main() -> None:
    universe = read_json(OUTPUTS_DIR / "universe.json", {"tickers": []})
    payload = {
        "generated_at": now_iso(),
        "source": "yfinance_placeholder",
        "tickers": universe.get("tickers", []),
        "rows": []
    }
    write_json(DATA_DIR / "price_data.json", payload)
    print("Price data placeholder written")


if __name__ == "__main__":
    main()
