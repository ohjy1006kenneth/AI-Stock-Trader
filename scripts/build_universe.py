from __future__ import annotations

from common import OUTPUTS_DIR, now_iso, write_json

DEFAULT_UNIVERSE = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "META", "NVDA", "LLY", "V", "MA", "COST",
    "ADBE", "CRM", "UNH", "ISRG", "ABBV", "SPGI", "MSCI", "ROP", "TT", "ETN"
]


def main() -> None:
    payload = {
        "generated_at": now_iso(),
        "market": "US_STOCKS",
        "selection_method": "static_seed_universe_v1",
        "tickers": DEFAULT_UNIVERSE,
        "notes": "Seed universe only. Replace with a rules-based universe builder later."
    }
    write_json(OUTPUTS_DIR / "universe.json", payload)
    print(f"Universe initialized with {len(DEFAULT_UNIVERSE)} tickers")


if __name__ == "__main__":
    main()
