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

from runtime.common.common import CONFIG_DIR, now_iso, write_json
from pi_edge.execution.alpaca_paper import AlpacaPaperClient

SNAPSHOT_PATH = CONFIG_DIR / "sp500_constituents.json"
WIKIPEDIA_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"


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


def fetch_wikipedia_sp500() -> list[str]:
    request = urllib.request.Request(WIKIPEDIA_URL, headers={"User-Agent": "Mozilla/5.0"})
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


def fetch_alpaca_tradable_equities() -> list[dict]:
    client = AlpacaPaperClient()
    assets = client.list_assets(status="active", asset_class="us_equity")
    filtered = []
    for asset in assets:
        if not asset.get("tradable"):
            continue
        if not asset.get("active", True):
            continue
        if asset.get("class") != "us_equity":
            continue
        symbol = normalize_ticker(str(asset.get("symbol", "")))
        if not symbol:
            continue
        filtered.append({
            "symbol": symbol,
            "name": asset.get("name"),
            "exchange": asset.get("exchange"),
            "tradable": bool(asset.get("tradable")),
            "marginable": bool(asset.get("marginable")),
            "shortable": bool(asset.get("shortable")),
            "easy_to_borrow": bool(asset.get("easy_to_borrow")),
            "fractionable": bool(asset.get("fractionable")),
        })
    filtered.sort(key=lambda x: x["symbol"])
    return filtered


def main() -> None:
    wikipedia_symbols = fetch_wikipedia_sp500()
    alpaca_assets = fetch_alpaca_tradable_equities()
    alpaca_symbols = {row["symbol"] for row in alpaca_assets}

    final_tickers = [symbol for symbol in wikipedia_symbols if symbol in alpaca_symbols]
    dropped = []
    for symbol in wikipedia_symbols:
        if symbol not in alpaca_symbols:
            dropped.append({"ticker": symbol, "reason": "not_active_tradable_alpaca_us_equity"})

    payload = {
        "generated_at": now_iso(),
        "source": {
            "membership": {
                "name": "wikipedia_sp500_constituents",
                "url": WIKIPEDIA_URL,
            },
            "tradability_filter": {
                "name": "alpaca_assets",
                "status": "active",
                "asset_class": "us_equity",
                "tradable": True,
            },
        },
        "counts": {
            "wikipedia_constituents": len(wikipedia_symbols),
            "alpaca_active_tradable_us_equities": len(alpaca_assets),
            "final_intersection": len(final_tickers),
            "dropped_from_wikipedia_after_alpaca_filter": len(dropped),
        },
        "normalization": {
            "rule": "uppercase_and_replace_dot_with_dash",
            "examples": ["BRK.B -> BRK-B", "BF.B -> BF-B"],
        },
        "tickers": final_tickers,
        "dropped": dropped,
    }
    write_json(SNAPSHOT_PATH, payload)
    print(f"S&P 500 snapshot refreshed: wikipedia={len(wikipedia_symbols)}, alpaca_tradable={len(alpaca_assets)}, final={len(final_tickers)}")


if __name__ == "__main__":
    main()
