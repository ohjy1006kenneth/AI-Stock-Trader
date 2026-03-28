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

from runtime.common.common import MARKET_DATA_DIR, now_iso, write_json

SEED_UNIVERSE = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "META", "NVDA", "LLY", "V", "MA", "COST",
    "ADBE", "CRM", "ISRG", "ABBV", "SPGI", "MSCI", "ROP", "TT", "ETN", "UNH"
]


def main() -> None:
    write_json(MARKET_DATA_DIR / "universe.json", {
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
