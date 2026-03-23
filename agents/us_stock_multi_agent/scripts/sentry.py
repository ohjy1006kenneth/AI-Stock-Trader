from __future__ import annotations

from pathlib import Path

from common import CONTEXT_DIR, DATA_DIR, ema, fetch_history, load_json, save_json, utc_now_iso, write_markdown

THRESHOLDS = {
    "daily_drop_pct": -3.0,
    "volume_spike_multiple": 1.8,
}


def build_trigger(ticker: str) -> dict | None:
    hist = fetch_history(ticker, period="6mo")
    close = hist["Close"]
    volume = hist["Volume"]
    latest = float(close.iloc[-1])
    prev_close = float(close.iloc[-2])
    day_change_pct = ((latest / prev_close) - 1.0) * 100
    ema200_series = ema(close, 200)
    ema50_series = ema(close, 50)
    ema200_prev = float(ema200_series.iloc[-2])
    ema200_now = float(ema200_series.iloc[-1])
    ema50_now = float(ema50_series.iloc[-1])
    avg20_vol = float(volume.tail(20).mean())
    latest_vol = float(volume.iloc[-1])

    reasons = []
    event_types = []

    if day_change_pct <= THRESHOLDS["daily_drop_pct"]:
        event_types.append("DAILY_DROP")
        reasons.append(f"{ticker} dropped {day_change_pct:.2f}% on the day")

    crossed_above_ema200 = prev_close <= ema200_prev and latest > ema200_now
    crossed_below_ema200 = prev_close >= ema200_prev and latest < ema200_now
    if crossed_above_ema200:
        event_types.append("CROSS_ABOVE_200DMA")
        reasons.append(f"{ticker} crossed above 200-day EMA")
    elif crossed_below_ema200:
        event_types.append("CROSS_BELOW_200DMA")
        reasons.append(f"{ticker} crossed below 200-day EMA")

    if latest_vol > avg20_vol * THRESHOLDS["volume_spike_multiple"] and latest > ema50_now:
        event_types.append("BREAKOUT_VOLUME")
        reasons.append(f"{ticker} printed breakout-style volume ({latest_vol:.0f} vs avg {avg20_vol:.0f})")

    if not event_types:
        return None

    return {
        "ticker": ticker,
        "timestamp": utc_now_iso(),
        "price": round(latest, 2),
        "day_change_pct": round(day_change_pct, 2),
        "ema50": round(ema50_now, 2),
        "ema200": round(ema200_now, 2),
        "event_types": event_types,
        "reasons": reasons,
    }


def main() -> None:
    watchlist = load_json(DATA_DIR / "watchlist.json", {"items": []})
    tickers = [item["ticker"] for item in watchlist.get("items", [])]
    triggers = []
    for ticker in tickers:
        try:
            event = build_trigger(ticker)
            if event:
                triggers.append(event)
        except Exception as exc:
            triggers.append({
                "ticker": ticker,
                "timestamp": utc_now_iso(),
                "event_types": ["ERROR"],
                "reasons": [str(exc)],
            })

    payload = {"generated_at": utc_now_iso(), "items": triggers}
    save_json(DATA_DIR / "triggers.json", payload)

    lines = ["# Trigger Events", "", f"Generated at: {payload['generated_at']}", ""]
    if not triggers:
        lines.append("No triggers fired.")
    else:
        for item in triggers:
            lines.extend([
                f"## {item['ticker']} | {', '.join(item['event_types'])}",
                f"- Timestamp: {item['timestamp']}",
                f"- Price: {item.get('price', 'n/a')}",
                f"- Day change: {item.get('day_change_pct', 'n/a')}%",
                f"- EMA50 / EMA200: {item.get('ema50', 'n/a')} / {item.get('ema200', 'n/a')}",
                "- Reasons:",
                *[f"  - {reason}" for reason in item.get('reasons', [])],
                "",
            ])
    write_markdown(CONTEXT_DIR / "trigger_events.md", "\n".join(lines) + "\n")


if __name__ == "__main__":
    main()
