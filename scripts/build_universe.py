from __future__ import annotations

from common import OUTPUTS_DIR, now_iso, write_json

DEFAULT_UNIVERSE = ["AAPL", "MSFT", "GOOGL", "AMZN", "META", "NVDA", "LLY", "V", "MA", "COST"]


def main() -> None:
    payload = {
        "generated_at": now_iso(),
        "market": "US_STOCKS",
        "selection_method": "static_seed_universe_v1",
        "tickers": DEFAULT_UNIVERSE,
    }
    write_json(OUTPUTS_DIR / "universe.json", payload)
    print("Universe initialized")


if __name__ == "__main__":
    main()
