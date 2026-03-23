from __future__ import annotations

from common import DATA_DIR, OUTPUTS_DIR, now_iso, read_json, write_json


QUALITY_THRESHOLDS = {
    "min_revenue_growth": 0.10,
    "min_net_margin": 0.10,
    "min_operating_margin": 0.10,
    "min_return_on_equity": 0.12,
    "max_debt_to_equity": 2.0,
    "min_market_cap": 5_000_000_000,
    "min_average_volume": 1_000_000,
}


def passes_quality_rules(item: dict) -> tuple[bool, list[str], float]:
    failures = []
    score = 0.0

    revenue_growth = item.get("revenue_growth")
    net_margin = item.get("net_margin")
    operating_margin = item.get("operating_margin")
    roe = item.get("return_on_equity")
    debt_to_equity = item.get("debt_to_equity")
    market_cap = item.get("market_cap")
    average_volume = item.get("average_volume")
    country = item.get("country")
    quote_type = str(item.get("quote_type", "")).upper()

    if country != "United States":
        failures.append("not_us_stock")
    if quote_type not in {"EQUITY", "COMMON STOCK"}:
        failures.append("unsupported_quote_type")
    if revenue_growth is None or revenue_growth < QUALITY_THRESHOLDS["min_revenue_growth"]:
        failures.append("revenue_growth_below_threshold")
    else:
        score += 1.0
    if net_margin is None or net_margin < QUALITY_THRESHOLDS["min_net_margin"]:
        failures.append("net_margin_below_threshold")
    else:
        score += 1.0
    if operating_margin is None or operating_margin < QUALITY_THRESHOLDS["min_operating_margin"]:
        failures.append("operating_margin_below_threshold")
    else:
        score += 1.0
    if roe is None or roe < QUALITY_THRESHOLDS["min_return_on_equity"]:
        failures.append("roe_below_threshold")
    else:
        score += 1.0
    if debt_to_equity is None or debt_to_equity > QUALITY_THRESHOLDS["max_debt_to_equity"]:
        failures.append("debt_to_equity_above_threshold")
    else:
        score += 1.0
    if market_cap is None or market_cap < QUALITY_THRESHOLDS["min_market_cap"]:
        failures.append("market_cap_below_threshold")
    else:
        score += 1.0
    if average_volume is None or average_volume < QUALITY_THRESHOLDS["min_average_volume"]:
        failures.append("liquidity_below_threshold")
    else:
        score += 1.0

    return len(failures) == 0, failures, score / 7.0


def main() -> None:
    fundamentals = read_json(DATA_DIR / "fundamental_data.json", {"items": []})
    qualified = []
    rejected = []
    for item in fundamentals.get("items", []):
        passed, failures, quality_score = passes_quality_rules(item)
        enriched = {
            **item,
            "quality_score": round(quality_score, 4),
            "quality_thresholds_version": "v1",
            "failures": failures,
        }
        if passed:
            qualified.append(enriched)
        else:
            rejected.append(enriched)

    payload = {
        "generated_at": now_iso(),
        "quality_model_version": "v1",
        "thresholds": QUALITY_THRESHOLDS,
        "items": qualified,
        "rejected": rejected,
    }
    write_json(OUTPUTS_DIR / "qualified_universe.json", payload)
    print(f"Qualified {len(qualified)} tickers; rejected {len(rejected)}")


if __name__ == "__main__":
    main()
