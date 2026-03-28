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

from runtime.common.common import MARKET_DATA_DIR, STRATEGY_DATA_DIR, now_iso, read_json, safe_float, write_json

THRESHOLDS = {
    "net_margin_min": 0.12,
    "debt_to_equity_max": 40.0,
    "revenue_growth_min": 0.05,
    "average_volume_min": 1_000_000,
    "market_cap_min": 5_000_000_000,
}


def evaluate(item: dict) -> dict:
    failures = []
    score = 0
    net_margin = safe_float(item.get("net_margin"))
    debt_to_equity = safe_float(item.get("debt_to_equity"))
    free_cash_flow = safe_float(item.get("free_cash_flow"))
    revenue_growth = safe_float(item.get("revenue_growth"))
    average_volume = safe_float(item.get("average_volume"))
    market_cap = safe_float(item.get("market_cap"))

    if item.get("country") != "United States":
        failures.append("not_us_stock")
    if str(item.get("quote_type", "")).upper() not in {"EQUITY", "COMMON STOCK"}:
        failures.append("unsupported_quote_type")
    if net_margin is None or net_margin <= THRESHOLDS["net_margin_min"]:
        failures.append("net_margin_below_threshold")
    else:
        score += 1
    if debt_to_equity is None or debt_to_equity >= THRESHOLDS["debt_to_equity_max"]:
        failures.append("debt_to_equity_above_threshold")
    else:
        score += 1
    if free_cash_flow is None or free_cash_flow <= 0:
        failures.append("free_cash_flow_not_positive")
    else:
        score += 1
    if revenue_growth is None or revenue_growth <= THRESHOLDS["revenue_growth_min"]:
        failures.append("revenue_growth_below_threshold")
    else:
        score += 1
    if average_volume is None or average_volume < THRESHOLDS["average_volume_min"]:
        failures.append("liquidity_below_threshold")
    else:
        score += 1
    if market_cap is None or market_cap < THRESHOLDS["market_cap_min"]:
        failures.append("market_cap_below_threshold")
    else:
        score += 1

    return {**item, "quality_score": round(score / 6.0, 4), "quality_pass": len(failures) == 0, "failures": failures}


def main() -> None:
    fundamentals = read_json(MARKET_DATA_DIR / "fundamental_snapshot.json", {"items": []})
    evaluated = [evaluate(item) for item in fundamentals.get("items", [])]
    write_json(STRATEGY_DATA_DIR / "qualified_universe.json", {
        "generated_at": now_iso(),
        "screen_version": "v1",
        "thresholds": THRESHOLDS,
        "items": [x for x in evaluated if x["quality_pass"]],
        "rejected": [x for x in evaluated if not x["quality_pass"]],
    })
    print(f"Qualified universe written: {sum(1 for x in evaluated if x['quality_pass'])} passed")


if __name__ == "__main__":
    main()
