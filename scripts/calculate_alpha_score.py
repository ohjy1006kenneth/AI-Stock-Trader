from __future__ import annotations

from common import OUTPUTS_DIR, now_iso, read_json, realized_volatility, rsi, sma, trailing_return, write_json


def percentile_ranks(pairs: list[tuple[str, float]], reverse: bool) -> dict[str, float]:
    ordered = sorted(pairs, key=lambda x: x[1], reverse=reverse)
    total = len(ordered)
    if total == 0:
        return {}
    if total == 1:
        return {ordered[0][0]: 1.0}
    return {ticker: 1.0 - ((idx - 1) / (total - 1)) for idx, (ticker, _) in enumerate(ordered, start=1)}


def main() -> None:
    prices = read_json(OUTPUTS_DIR / "price_snapshot.json", {"items": []})
    qualified = read_json(OUTPUTS_DIR / "qualified_universe.json", {"items": []})
    qualified_tickers = {item["ticker"] for item in qualified.get("items", [])}

    metrics = []
    for item in prices.get("items", []):
        history = item.get("history", [])
        adj = [row["adj_close"] for row in history if row.get("adj_close") is not None]
        close = [row["close"] for row in history if row.get("close") is not None]
        last_close = close[-1] if close else None
        sma_200 = sma(close, 200)
        metrics.append({
            "ticker": item["ticker"],
            "momentum_12_1": trailing_return(adj, 252, 21),
            "realized_vol_30d": realized_volatility(adj, 30),
            "rsi_14": rsi(close, 14),
            "trend_filter_pass": bool(last_close is not None and sma_200 is not None and last_close > sma_200),
            "current_close": last_close,
            "is_quality_eligible": item["ticker"] in qualified_tickers,
        })

    momentum_ranks = percentile_ranks([(m["ticker"], m["momentum_12_1"]) for m in metrics if m["momentum_12_1"] is not None], True)
    vol_ranks = percentile_ranks([(m["ticker"], m["realized_vol_30d"]) for m in metrics if m["realized_vol_30d"] is not None], False)

    items = []
    for m in metrics:
        momentum_rank = momentum_ranks.get(m["ticker"])
        vol_rank = vol_ranks.get(m["ticker"])
        trend_bonus = 1.0 if m["trend_filter_pass"] else 0.0
        quality_bonus = 1.0 if m["is_quality_eligible"] else 0.0
        alpha_score = None
        if momentum_rank is not None and vol_rank is not None:
            alpha_score = round((0.5 * momentum_rank) + (0.2 * vol_rank) + (0.15 * trend_bonus) + (0.15 * quality_bonus), 6)
        items.append({
            "ticker": m["ticker"],
            "timestamp": now_iso(),
            "alpha_model_version": "v1",
            "alpha_score": alpha_score,
            "factors": m,
        })
    items.sort(key=lambda x: (-1 if x["alpha_score"] is None else -x["alpha_score"], x["ticker"]))
    write_json(OUTPUTS_DIR / "alpha_rankings.json", {
        "generated_at": now_iso(),
        "model_version": "v1",
        "items": items,
        "notes": "Deterministic V1 ranking scaffold. Not a profitability claim. RSI is contextual, not a sole driver."
    })
    print(f"Alpha rankings written: {len(items)} tickers")


if __name__ == "__main__":
    main()
