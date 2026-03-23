from __future__ import annotations

from common import CONTEXT_DIR, DATA_DIR, ema, fetch_history, load_json, rsi, save_json, utc_now_iso, write_markdown


def main() -> None:
    watchlist = load_json(DATA_DIR / "watchlist.json", {"items": []})
    signals = []
    for item in watchlist.get("items", []):
        ticker = item["ticker"]
        try:
            hist = fetch_history(ticker)
            close = hist["Close"]
            volume = hist["Volume"]
            rsi_series = rsi(close)
            ema50 = ema(close, 50)
            ema200 = ema(close, 200)
            latest_close = float(close.iloc[-1])
            latest_rsi = float(rsi_series.iloc[-1])
            latest_ema50 = float(ema50.iloc[-1])
            latest_ema200 = float(ema200.iloc[-1])
            avg20_volume = float(volume.tail(20).mean())
            latest_volume = float(volume.iloc[-1])
            volume_spike = latest_volume > avg20_volume * 1.5
            oversold = latest_rsi < 35 and latest_close > latest_ema200 * 0.9
            breakout = latest_close > latest_ema50 > latest_ema200 and volume_spike
            classification = "WAIT"
            if oversold:
                classification = "OVERSOLD_LONG"
            elif breakout:
                classification = "BREAKOUT_SWING"
            signals.append({
                **item,
                "price": round(latest_close, 2),
                "rsi14": round(latest_rsi, 2),
                "ema50": round(latest_ema50, 2),
                "ema200": round(latest_ema200, 2),
                "volume_spike": volume_spike,
                "signal": classification,
            })
        except Exception as exc:
            signals.append({**item, "signal": "WAIT", "error": str(exc)})
    payload = {"generated_at": utc_now_iso(), "items": signals}
    save_json(DATA_DIR / "technical_signals.json", payload)

    lines = ["# Technical Signals", "", f"Generated at: {payload['generated_at']}", ""]
    for s in signals:
        lines.extend([
            f"## {s['ticker']} - {s['signal']}",
            f"- Price: {s.get('price', 'n/a')}",
            f"- RSI(14): {s.get('rsi14', 'n/a')}",
            f"- EMA50 / EMA200: {s.get('ema50', 'n/a')} / {s.get('ema200', 'n/a')}",
            f"- Volume spike: {s.get('volume_spike', False)}",
            f"- Fundamental score: {s.get('fundamental_score', 'n/a')}",
            f"- Note: {s.get('error', 'OK')}",
            "",
        ])
    write_markdown(CONTEXT_DIR / "technical_signals.md", "\n".join(lines) + "\n")


if __name__ == "__main__":
    main()
