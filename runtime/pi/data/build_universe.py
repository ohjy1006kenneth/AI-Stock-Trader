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

import urllib.request
from html.parser import HTMLParser

from runtime.common.common import CONFIG_DIR, MARKET_DATA_DIR, now_iso, read_json, write_json

SP500_FALLBACK_PATH = CONFIG_DIR / "sp500_constituents.json"
SP500_WIKIPEDIA_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"


class SP500WikipediaParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.in_constituents = False
        self.table_depth = 0
        self.in_cell = False
        self.current_cell: list[str] = []
        self.current_row: list[str] = []
        self.rows: list[list[str]] = []

    def handle_starttag(self, tag: str, attrs) -> None:
        attr_map = dict(attrs)
        if tag == "table" and attr_map.get("id") == "constituents" and not self.in_constituents:
            self.in_constituents = True
            self.table_depth = 1
            return
        if not self.in_constituents:
            return
        if tag == "table":
            self.table_depth += 1
        elif tag in {"td", "th"}:
            self.in_cell = True
            self.current_cell = []
        elif tag == "tr":
            self.current_row = []

    def handle_endtag(self, tag: str) -> None:
        if not self.in_constituents:
            return
        if tag == "table":
            self.table_depth -= 1
            if self.table_depth == 0:
                self.in_constituents = False
        elif tag in {"td", "th"} and self.in_cell:
            self.in_cell = False
            self.current_row.append("".join(self.current_cell).strip())
        elif tag == "tr" and self.current_row:
            self.rows.append(self.current_row)

    def handle_data(self, data: str) -> None:
        if self.in_constituents and self.in_cell:
            self.current_cell.append(data)


def normalize_ticker(symbol: str) -> str:
    return symbol.strip().upper().replace(".", "-")


def dedupe_keep_order(symbols: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for symbol in symbols:
        if symbol and symbol not in seen:
            seen.add(symbol)
            out.append(symbol)
    return out


def fetch_sp500_from_wikipedia() -> list[str]:
    request = urllib.request.Request(SP500_WIKIPEDIA_URL, headers={"User-Agent": "Mozilla/5.0"})
    html = urllib.request.urlopen(request, timeout=30).read().decode("utf-8", errors="ignore")
    parser = SP500WikipediaParser()
    parser.feed(html)
    if not parser.rows:
        raise RuntimeError("sp500_constituents_table_not_found")
    symbols = [normalize_ticker(row[0]) for row in parser.rows[1:] if row]
    symbols = dedupe_keep_order(symbols)
    if len(symbols) < 500:
        raise RuntimeError(f"sp500_symbol_count_too_small:{len(symbols)}")
    return symbols


def load_fallback_sp500() -> list[str]:
    payload = read_json(SP500_FALLBACK_PATH, {"tickers": []})
    symbols = dedupe_keep_order([normalize_ticker(x) for x in payload.get("tickers", [])])
    if len(symbols) < 500:
        raise RuntimeError(f"fallback_sp500_symbol_count_too_small:{len(symbols)}")
    return symbols


def main() -> None:
    selection_method = "sp500_fallback_snapshot"
    try:
        tickers = fetch_sp500_from_wikipedia()
        selection_method = "sp500_wikipedia_live_with_fallback_snapshot_available"
    except Exception as exc:
        tickers = load_fallback_sp500()
        selection_method = f"sp500_fallback_snapshot_due_to:{type(exc).__name__}"

    write_json(MARKET_DATA_DIR / "universe.json", {
        "generated_at": now_iso(),
        "market": "US_STOCKS",
        "universe_version": "sp500_v1",
        "selection_method": selection_method,
        "membership_source": {
            "primary": SP500_WIKIPEDIA_URL,
            "fallback": "config/sp500_constituents.json"
        },
        "tickers": tickers,
        "notes": "S&P 500 membership universe. Live Wikipedia parse is attempted first; tracked fallback snapshot is used if live retrieval fails or looks incomplete. Tickers are normalized for yfinance compatibility by converting '.' to '-'."
    })
    print(f"Universe built: {len(tickers)} tickers")


if __name__ == "__main__":
    main()
