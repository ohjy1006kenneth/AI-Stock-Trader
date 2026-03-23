from __future__ import annotations

import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

ROOT = Path(__file__).resolve().parent.parent
CONFIG_DIR = ROOT / "config"
DATA_DIR = ROOT / "data"
RESEARCH_DIR = ROOT / "research"
BACKTESTS_DIR = ROOT / "backtests"
OUTPUTS_DIR = ROOT / "outputs"
REPORTS_DIR = ROOT / "reports"
LEDGER_DIR = ROOT / "ledger"
LOGS_DIR = ROOT / "logs"

TRADING_DAYS_PER_YEAR = 252


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def read_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    return json.loads(path.read_text())


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2))


def write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content)


def pct_change(new_value: float, old_value: float) -> float | None:
    if old_value == 0:
        return None
    return (new_value / old_value) - 1.0


def mean(values: Iterable[float]) -> float:
    values = list(values)
    return sum(values) / len(values) if values else 0.0


def stdev(values: Iterable[float]) -> float:
    values = list(values)
    if len(values) < 2:
        return 0.0
    avg = mean(values)
    variance = sum((v - avg) ** 2 for v in values) / (len(values) - 1)
    return math.sqrt(variance)


def trailing_return(series: list[float], start_offset: int, end_offset: int) -> float | None:
    if len(series) <= start_offset or len(series) <= end_offset:
        return None
    start_price = series[-start_offset - 1]
    end_price = series[-end_offset - 1]
    if start_price <= 0:
        return None
    return (end_price / start_price) - 1.0


def annualized_realized_vol(close_series: list[float], window: int = 30) -> float | None:
    if len(close_series) < window + 1:
        return None
    recent = close_series[-(window + 1):]
    log_returns = []
    for idx in range(1, len(recent)):
        prev_close = recent[idx - 1]
        next_close = recent[idx]
        if prev_close <= 0 or next_close <= 0:
            return None
        log_returns.append(math.log(next_close / prev_close))
    return stdev(log_returns) * math.sqrt(TRADING_DAYS_PER_YEAR)


def simple_moving_average(series: list[float], window: int) -> float | None:
    if len(series) < window:
        return None
    segment = series[-window:]
    return mean(segment)


def rsi_14(close_series: list[float], period: int = 14) -> float | None:
    if len(close_series) < period + 1:
        return None
    gains = []
    losses = []
    recent = close_series[-(period + 1):]
    for idx in range(1, len(recent)):
        delta = recent[idx] - recent[idx - 1]
        gains.append(max(delta, 0.0))
        losses.append(abs(min(delta, 0.0)))
    avg_gain = mean(gains)
    avg_loss = mean(losses)
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))
