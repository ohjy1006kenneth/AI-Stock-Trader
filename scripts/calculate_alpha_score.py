from __future__ import annotations

from common import DATA_DIR, OUTPUTS_DIR, RESEARCH_DIR, annualized_realized_vol, now_iso, read_json, rsi_14, simple_moving_average, trailing_return, write_json


def build_factor_map(price_item: dict) -> dict:
    rows = price_item.get("rows", [])
    adj_close = [row["adj_close"] for row in rows]
    close = [row["close"] for row in rows]
    current_close = close[-1] if close else None
    sma200 = simple_moving_average(adj_close, 200)
    return {
        "momentum_12_1": trailing_return(adj_close, 252, 21),
        "realized_vol_30d": annualized_realized_vol(adj_close, 30),
        "rsi_14": rsi_14(close, 14),
        "sma_200": sma200,
        "trend_filter_pass": bool(current_close is not None and sma200 is not None and current_close > sma200),
        "current_close": current_close,
    }


def cross_sectional_ranks(values: list[tuple[str, float]], reverse: bool = True) -> dict[str, float]:
    ordered = sorted(values, key=lambda x: x[1], reverse=reverse)
    total = len(ordered)
    ranks = {}
    for idx, (ticker, _) in enumerate(ordered, start=1):
        ranks[ticker] = 1.0 if total == 1 else 1.0 - ((idx - 1) / (total - 1))
    return ranks


def main() -> None:
    registry = read_json(RESEARCH_DIR / "formula_registry.json", {"factors": []})
    prices = read_json(DATA_DIR / "price_history.json", {"items": []})
    fundamentals = read_json(DATA_DIR / "fundamental_data.json", {"items": []})
    fundamentals_by_ticker = {item["ticker"]: item for item in fundamentals.get("items", [])}

    factor_map = {item["ticker"]: build_factor_map(item) for item in prices.get("items", []) if item.get("rows")}
    momentum_values = [(ticker, values["momentum_12_1"]) for ticker, values in factor_map.items() if values["momentum_12_1"] is not None]
    vol_values = [(ticker, values["realized_vol_30d"]) for ticker, values in factor_map.items() if values["realized_vol_30d"] is not None]
    momentum_ranks = cross_sectional_ranks(momentum_values, reverse=True) if momentum_values else {}
    vol_ranks = cross_sectional_ranks(vol_values, reverse=False) if vol_values else {}

    items = []
    for ticker, values in factor_map.items():
        fundamentals_item = fundamentals_by_ticker.get(ticker, {})
        quality_proxy = 1.0 if (fundamentals_item.get("profitMargins") or fundamentals_item.get("net_margin") or 0) else 0.0
        momentum_rank = momentum_ranks.get(ticker)
        low_vol_rank = vol_ranks.get(ticker)
        trend_bonus = 1.0 if values["trend_filter_pass"] else 0.0
        if momentum_rank is None or low_vol_rank is None:
            alpha_score = None
        else:
            alpha_score = (0.45 * momentum_rank) + (0.20 * low_vol_rank) + (0.20 * trend_bonus) + (0.15 * quality_proxy)
        items.append({
            "ticker": ticker,
            "as_of": now_iso(),
            "factors": values,
            "alpha_score": round(alpha_score, 6) if alpha_score is not None else None,
            "alpha_model_version": "v1",
        })

    items.sort(key=lambda x: (-1 if x["alpha_score"] is None else -x["alpha_score"]))
    payload = {
        "generated_at": now_iso(),
        "model_version": "v1",
        "approved_factors_used": [
            f["factor_name"] for f in registry.get("factors", []) if f.get("approved_for_production")
        ],
        "items": items,
        "scoring_notes": {
            "formula": "0.45*momentum_rank + 0.20*low_vol_rank + 0.20*trend_bonus + 0.15*quality_proxy",
            "purpose": "conservative ranking scaffold for research, not a profitability claim"
        }
    }
    write_json(OUTPUTS_DIR / "alpha_rankings.json", payload)
    print(f"Calculated alpha scores for {len(items)} tickers")


if __name__ == "__main__":
    main()
