from __future__ import annotations

from common import OUTPUTS_DIR, now_iso, write_json

SEED_UNIVERSE = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "META", "NVDA", "LLY", "V", "MA", "COST",
    "ADBE", "CRM", "ISRG", "ABBV", "SPGI", "MSCI", "ROP", "TT", "ETN", "UNH"
]


def main() -> None:
    write_json(OUTPUTS_DIR / "universe.json", {
        "generated_at": now_iso(),
        "market": "US_STOCKS",
        "universe_version": "v1",
        "selection_method": "static_seed_universe_v1",
        "tickers": SEED_UNIVERSE,
        "notes": "Conservative starter universe for research and mock portfolio development."
    })
    print(f"Universe built: {len(SEED_UNIVERSE)} tickers")


if __name__ == "__main__":
    main()
