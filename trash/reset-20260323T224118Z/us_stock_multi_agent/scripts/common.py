from __future__ import annotations

import json
import math
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pandas as pd

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
CONTEXT_DIR = BASE_DIR / "context"
LOG_DIR = BASE_DIR / "logs"

for directory in (DATA_DIR, CONTEXT_DIR, LOG_DIR):
    directory.mkdir(parents=True, exist_ok=True)

UNIVERSE = {
    "Technology": ["MSFT", "GOOGL", "META", "NVDA", "ADBE", "CRM"],
    "Consumer": ["AMZN", "COST", "SBUX", "NKE", "MCD"],
    "Healthcare": ["LLY", "UNH", "ISRG", "ABBV", "VRTX"],
    "Financials": ["V", "MA", "SPGI", "MSCI", "BRK-B"],
    "Industrials": ["GE", "PH", "ROP", "TT", "ETN"],
}


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    return json.loads(path.read_text())


def save_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=False))


def write_markdown(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content)


def fetch_info(ticker: str) -> Dict[str, Any]:
    import yfinance as yf

    t = yf.Ticker(ticker)
    info = t.info or {}
    fast = t.fast_info or {}
    return {**info, "lastPrice": fast.get("lastPrice")}


def fetch_history(ticker: str, period: str = "1y", interval: str = "1d") -> "pd.DataFrame":
    import pandas as pd
    import yfinance as yf

    hist = yf.download(ticker, period=period, interval=interval, auto_adjust=False, progress=False)
    if hist is None or hist.empty:
        raise ValueError(f"No history for {ticker}")
    if isinstance(hist.columns, pd.MultiIndex):
        hist.columns = hist.columns.get_level_values(0)
    return hist.dropna()


def ema(series: "pd.Series", span: int) -> "pd.Series":
    return series.ewm(span=span, adjust=False).mean()


def rsi(series: "pd.Series", period: int = 14) -> "pd.Series":
    import numpy as np

    delta = series.diff()
    gain = delta.clip(lower=0).rolling(period).mean()
    loss = (-delta.clip(upper=0)).rolling(period).mean()
    rs = gain / loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs))


def safe_num(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or (isinstance(value, float) and math.isnan(value)):
            return default
        return float(value)
    except Exception:
        return default


def analyst_sentiment_score(info: Dict[str, Any]) -> float:
    recommendation = str(info.get("recommendationKey", "")).lower()
    mapping = {
        "strong_buy": 1.0,
        "buy": 0.8,
        "outperform": 0.7,
        "overweight": 0.65,
        "hold": 0.45,
        "neutral": 0.4,
        "underperform": 0.2,
        "sell": 0.0,
    }
    return mapping.get(recommendation, 0.35)


def is_us_stock(info: Dict[str, Any]) -> bool:
    country = str(info.get("country", "")).upper()
    quote_type = str(info.get("quoteType", "")).upper()
    exchange = str(info.get("exchange", "")).upper()
    return country == "UNITED STATES" and quote_type in {"EQUITY", "COMMON STOCK"} and exchange not in {"PNK", "OTC"}
