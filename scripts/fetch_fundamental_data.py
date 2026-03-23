from __future__ import annotations

from common import DATA_DIR, OUTPUTS_DIR, now_iso, read_json, write_json


def main() -> None:
    universe = read_json(OUTPUTS_DIR / "universe.json", {"tickers": []})
    payload = {
        "generated_at": now_iso(),
        "source": "sec_or_placeholder",
        "tickers": universe.get("tickers", []),
        "items": []
    }
    write_json(DATA_DIR / "fundamental_data.json", payload)
    print("Fundamental data placeholder written")


if __name__ == "__main__":
    main()
