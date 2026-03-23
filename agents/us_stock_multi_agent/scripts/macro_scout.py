from __future__ import annotations

from common import UNIVERSE, CONTEXT_DIR, DATA_DIR, analyst_sentiment_score, fetch_info, safe_num, save_json, utc_now_iso, write_markdown, is_us_stock


def score_candidate(info):
    revenue_growth = safe_num(info.get("revenueGrowth")) * 100
    net_margin = safe_num(info.get("profitMargins")) * 100
    sentiment = analyst_sentiment_score(info)
    moat_score = 0
    moat_score += 40 if revenue_growth > 15 else max(0, revenue_growth * 2)
    moat_score += 40 if net_margin > 20 else max(0, net_margin * 2)
    moat_score += sentiment * 20
    return revenue_growth, net_margin, sentiment, round(min(moat_score, 100), 2)


def main() -> None:
    selected = []
    rejected = []
    for sector, tickers in UNIVERSE.items():
        for ticker in tickers:
            try:
                info = fetch_info(ticker)
                if not is_us_stock(info):
                    rejected.append({"ticker": ticker, "sector": sector, "reason": "Non-US or unsupported listing"})
                    continue
                revenue_growth, net_margin, sentiment, score = score_candidate(info)
                item = {
                    "ticker": ticker,
                    "sector": sector,
                    "company": info.get("longName", ticker),
                    "revenue_growth_pct": round(revenue_growth, 2),
                    "net_margin_pct": round(net_margin, 2),
                    "analyst_sentiment_score": round(sentiment, 2),
                    "fundamental_score": score,
                    "source": "Yahoo Finance via yfinance",
                }
                if revenue_growth > 15 and net_margin > 20 and sentiment >= 0.45:
                    selected.append(item)
                else:
                    rejected.append({**item, "reason": "Failed moat thresholds"})
            except Exception as exc:
                rejected.append({"ticker": ticker, "sector": sector, "reason": f"Data error: {exc}"})
    selected.sort(key=lambda x: x["fundamental_score"], reverse=True)
    payload = {"generated_at": utc_now_iso(), "items": selected, "rejected": rejected}
    save_json(DATA_DIR / "watchlist.json", payload)

    lines = ["# Macro Watchlist", "", f"Generated at: {payload['generated_at']}", "", "## Selected Targets", ""]
    if selected:
        for idx, item in enumerate(selected, start=1):
            lines.extend([
                f"### {idx}. {item['ticker']} - {item['company']}",
                f"- Sector: {item['sector']}",
                f"- Revenue growth: {item['revenue_growth_pct']}%",
                f"- Net margin: {item['net_margin_pct']}%",
                f"- Analyst sentiment score: {item['analyst_sentiment_score']}",
                f"- Fundamental score: {item['fundamental_score']}",
                f"- Source: {item['source']}",
                "",
            ])
    else:
        lines.append("No companies passed the screen.\n")

    lines.extend(["## Rejected Names", ""])
    for item in rejected:
        lines.append(f"- {item.get('ticker')}: {item.get('reason')}")
    write_markdown(CONTEXT_DIR / "macro_watchlist.md", "\n".join(lines) + "\n")


if __name__ == "__main__":
    main()
