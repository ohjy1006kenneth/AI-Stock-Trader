from __future__ import annotations

import json
import math
import uuid
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
CONFIG_DIR = ROOT / "config"
DATA_DIR = ROOT / "data"
RUNTIME_DATA_DIR = DATA_DIR / "runtime"
MARKET_DATA_DIR = RUNTIME_DATA_DIR / "market"
STRATEGY_DATA_DIR = RUNTIME_DATA_DIR / "strategy"
EXECUTION_DATA_DIR = RUNTIME_DATA_DIR / "execution"
DIAGNOSTICS_DATA_DIR = RUNTIME_DATA_DIR / "diagnostics"
ALERTS_DATA_DIR = RUNTIME_DATA_DIR / "alerts"
RESEARCH_DIR = ROOT / "research"
BACKTESTS_DIR = ROOT / "backtests"
REPORTS_DIR = ROOT / "reports"
DAILY_REPORTS_DIR = REPORTS_DIR / "daily"
PIPELINE_REPORTS_DIR = REPORTS_DIR / "pipeline"
DIAGNOSTIC_REPORTS_DIR = REPORTS_DIR / "diagnostics"
BACKTEST_REPORTS_DIR = REPORTS_DIR / "backtests"
TEMPLATES_DIR = REPORTS_DIR / "templates"
LEDGER_DIR = ROOT / "ledger"
LOGS_DIR = ROOT / "logs"
STRATEGY_DIR = ROOT / "strategy"
DOCS_DIR = ROOT / "docs"
REQUIREMENTS_DIR = ROOT / "requirements"

TRADING_DAYS_PER_YEAR = 252


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def today_iso() -> str:
    return date.today().isoformat()


def gen_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:12]}"


def read_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    return json.loads(path.read_text())


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=False))


def write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content)


def safe_float(value: Any, default: float | None = None) -> float | None:
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


def mean(values: list[float]) -> float | None:
    return sum(values) / len(values) if values else None


def stddev(values: list[float]) -> float | None:
    if len(values) < 2:
        return None
    m = mean(values)
    if m is None:
        return None
    return math.sqrt(sum((v - m) ** 2 for v in values) / (len(values) - 1))


def trailing_return(series: list[float], start_offset: int, end_offset: int) -> float | None:
    if len(series) <= max(start_offset, end_offset):
        return None
    start_price = series[-start_offset - 1]
    end_price = series[-end_offset - 1]
    if start_price <= 0:
        return None
    return (end_price / start_price) - 1.0


def realized_volatility(close_series: list[float], window: int = 30) -> float | None:
    if len(close_series) < window + 1:
        return None
    sample = close_series[-(window + 1):]
    rets: list[float] = []
    for idx in range(1, len(sample)):
        prev_close = sample[idx - 1]
        cur_close = sample[idx]
        if prev_close <= 0 or cur_close <= 0:
            return None
        rets.append(math.log(cur_close / prev_close))
    st = stddev(rets)
    return None if st is None else st * math.sqrt(TRADING_DAYS_PER_YEAR)


def sma(series: list[float], window: int) -> float | None:
    if len(series) < window:
        return None
    return mean(series[-window:])


def rsi(close_series: list[float], period: int = 14) -> float | None:
    if len(close_series) < period + 1:
        return None
    gains: list[float] = []
    losses: list[float] = []
    recent = close_series[-(period + 1):]
    for idx in range(1, len(recent)):
        delta = recent[idx] - recent[idx - 1]
        gains.append(max(delta, 0.0))
        losses.append(max(-delta, 0.0))
    avg_gain = mean(gains)
    avg_loss = mean(losses)
    if avg_gain is None or avg_loss is None:
        return None
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))


def validate_required_fields(obj: dict, required_fields: list[str]) -> list[str]:
    return [field for field in required_fields if field not in obj]


def load_contracts() -> dict:
    return read_json(CONFIG_DIR / "data_contracts.json", {})


def latest_price_from_snapshot(snapshot: dict, ticker: str) -> float | None:
    for item in snapshot.get("items", []):
        if item.get("ticker") == ticker:
            return safe_float(item.get("close"))
    return None
