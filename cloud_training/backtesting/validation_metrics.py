from __future__ import annotations

from pathlib import Path
import sys

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import argparse
import json
import math
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from statistics import fmean, pvariance
from typing import Any

from cloud_training.model_architecture.hybrid_model import HybridSignalEnsemble, extract_feature_row, load_artifact

DATASET_OUTPUT_DIR = ROOT_DIR / "data" / "processed" / "training"
MODEL_OUTPUT_DIR = ROOT_DIR / "data" / "processed" / "models"
BACKTEST_OUTPUT_DIR = ROOT_DIR / "reports" / "backtests"
MARKET_SNAPSHOT_PATH = ROOT_DIR / "data" / "runtime" / "market" / "price_snapshot.json"

ANNUALIZATION_DAYS = 252
DEFAULT_TRAIN_YEARS = 2
DEFAULT_TEST_MONTHS = 6
PROMOTION_THRESHOLDS = {
    "min_sharpe": 1.2,
    "max_drawdown": 0.12,
    "min_backtest_days": 252,
    "must_beat_spy": True,
}


@dataclass
class PredictionRow:
    ticker: str
    as_of_date: str
    target_date: str
    actual_return: float
    actual_positive: int
    signal_probability: float
    confidence: float
    predictive_variance: float
    raw_weight: float
    target_weight: float = 0.0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run first walk-forward validation scaffold and promotion checks")
    parser.add_argument("--dataset", help="dataset jsonl path; defaults to latest under data/processed/training")
    parser.add_argument("--artifact", help="model artifact path; defaults to latest under data/processed/models")
    parser.add_argument("--output-prefix", default="validation_metrics_v1")
    parser.add_argument("--train-years", type=int, default=DEFAULT_TRAIN_YEARS)
    parser.add_argument("--test-months", type=int, default=DEFAULT_TEST_MONTHS)
    parser.add_argument("--max-weight-per-ticker", type=float, default=0.20)
    return parser.parse_args()


def resolve_latest_file(directory: Path, suffix: str) -> Path:
    candidates = sorted(directory.glob(suffix))
    if not candidates:
        raise SystemExit(f"no_matching_files:{directory}:{suffix}")
    return candidates[-1]


def resolve_input_path(explicit_path: str | None, *, directory: Path, suffix: str) -> Path:
    if explicit_path:
        path = Path(explicit_path)
        if not path.is_absolute():
            path = ROOT_DIR / path
        if not path.exists():
            raise SystemExit(f"file_not_found:{path}")
        return path
    return resolve_latest_file(directory, suffix)


def load_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open() as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    if not rows:
        raise SystemExit(f"empty_jsonl:{path}")
    return rows


def _clip_probability(value: float) -> float:
    return min(max(value, 0.0), 1.0)


def load_artifact_from_payload(payload: dict[str, Any]) -> HybridSignalEnsemble:
    return HybridSignalEnsemble.from_artifact(payload)


def build_predictions(samples: list[dict[str, Any]], artifact_path: Path) -> list[PredictionRow]:
    with artifact_path.open() as f:
        artifact_payload = json.load(f)
    model_payload = artifact_payload.get("artifact", artifact_payload)
    model = load_artifact(str(artifact_path)) if "scaler" in artifact_payload else load_artifact_from_payload(model_payload)
    rows: list[PredictionRow] = []
    for sample in samples:
        output = model.predict(extract_feature_row(sample))
        signal_probability = _clip_probability(float(output["signal_probability"]))
        confidence = _clip_probability(float(output["confidence"]))
        raw_weight = max(0.0, (signal_probability - 0.5) * 2.0) * confidence
        rows.append(PredictionRow(
            ticker=str(sample["ticker"]),
            as_of_date=str(sample["as_of_date"]),
            target_date=str(sample["target_date"]),
            actual_return=float(sample["target_log_return_t_plus_1"]),
            actual_positive=int(sample.get("target_positive_return", 0)),
            signal_probability=signal_probability,
            confidence=confidence,
            predictive_variance=float(output["predictive_variance"]),
            raw_weight=raw_weight,
        ))
    return rows


def allocate_daily_weights(predictions: list[PredictionRow], *, max_weight_per_ticker: float) -> dict[str, list[PredictionRow]]:
    by_day: dict[str, list[PredictionRow]] = defaultdict(list)
    for row in predictions:
        by_day[row.target_date].append(row)

    for target_date, day_rows in by_day.items():
        total_raw = sum(row.raw_weight for row in day_rows)
        if total_raw <= 0:
            for row in day_rows:
                row.target_weight = 0.0
            continue
        scaled = [min(max_weight_per_ticker, row.raw_weight / total_raw) for row in day_rows]
        scaled_sum = sum(scaled)
        if scaled_sum > 1.0 and scaled_sum > 0:
            scaled = [value / scaled_sum for value in scaled]
        for row, weight in zip(day_rows, scaled):
            row.target_weight = weight
    return dict(sorted(by_day.items()))


def _annualized_sharpe(returns: list[float]) -> float:
    if len(returns) < 2:
        return 0.0
    mean_return = fmean(returns)
    variance = pvariance(returns)
    if variance <= 1e-12:
        return 0.0
    return (mean_return / math.sqrt(variance)) * math.sqrt(ANNUALIZATION_DAYS)


def _max_drawdown(returns: list[float]) -> float:
    equity = 1.0
    peak = 1.0
    max_dd = 0.0
    for daily_return in returns:
        equity *= math.exp(daily_return)
        peak = max(peak, equity)
        if peak > 0:
            max_dd = max(max_dd, 1.0 - (equity / peak))
    return max_dd


def _hit_rate(returns: list[float]) -> float:
    if not returns:
        return 0.0
    hits = sum(1 for value in returns if value > 0)
    return hits / len(returns)


def _directional_hit_rate(predictions: list[PredictionRow]) -> float:
    if not predictions:
        return 0.0
    hits = 0
    for row in predictions:
        predicted_positive = 1 if row.signal_probability >= 0.5 else 0
        if predicted_positive == row.actual_positive:
            hits += 1
    return hits / len(predictions)


def _brier_score(predictions: list[PredictionRow]) -> float:
    if not predictions:
        return 0.0
    return sum((row.signal_probability - row.actual_positive) ** 2 for row in predictions) / len(predictions)


def _calibration_buckets(predictions: list[PredictionRow], buckets: int = 10) -> list[dict[str, float | int]]:
    grouped: dict[int, list[PredictionRow]] = defaultdict(list)
    for row in predictions:
        bucket = min(buckets - 1, int(row.signal_probability * buckets))
        grouped[bucket].append(row)
    summary: list[dict[str, float | int]] = []
    for bucket in range(buckets):
        rows = grouped.get(bucket, [])
        if not rows:
            summary.append({"bucket": bucket, "count": 0, "avg_probability": 0.0, "observed_positive_rate": 0.0})
            continue
        summary.append({
            "bucket": bucket,
            "count": len(rows),
            "avg_probability": sum(row.signal_probability for row in rows) / len(rows),
            "observed_positive_rate": sum(row.actual_positive for row in rows) / len(rows),
        })
    return summary


def _turnover(weight_history: list[dict[str, float]]) -> float:
    if len(weight_history) < 2:
        return 0.0
    changes: list[float] = []
    previous = weight_history[0]
    for current in weight_history[1:]:
        tickers = set(previous) | set(current)
        gross_change = sum(abs(current.get(ticker, 0.0) - previous.get(ticker, 0.0)) for ticker in tickers)
        changes.append(gross_change / 2.0)
        previous = current
    return sum(changes) / len(changes) if changes else 0.0


def load_benchmark_returns() -> dict[str, float]:
    if not MARKET_SNAPSHOT_PATH.exists():
        return {}
    with MARKET_SNAPSHOT_PATH.open() as f:
        payload = json.load(f)
    spy_item = next((item for item in payload.get("items", []) if item.get("ticker") == "SPY"), None)
    if not spy_item:
        return {}
    history = sorted(spy_item.get("history", []), key=lambda row: row.get("date", ""))
    out: dict[str, float] = {}
    for prev_row, row in zip(history[:-1], history[1:]):
        prev_close = float(prev_row.get("close", 0.0) or 0.0)
        close = float(row.get("close", 0.0) or 0.0)
        if prev_close > 0 and close > 0:
            out[str(row["date"])] = math.log(close / prev_close)
    return out


def evaluate_portfolio(by_day: dict[str, list[PredictionRow]], benchmark_returns: dict[str, float]) -> dict[str, Any]:
    strategy_returns: list[float] = []
    matched_spy_returns: list[float] = []
    excess_returns: list[float] = []
    weight_history: list[dict[str, float]] = []
    daily_rows: list[dict[str, Any]] = []

    for target_date, rows in by_day.items():
        weights = {row.ticker: row.target_weight for row in rows if row.target_weight > 0}
        weight_history.append(weights)
        strategy_return = sum(row.target_weight * row.actual_return for row in rows)
        spy_return = benchmark_returns.get(target_date)
        strategy_returns.append(strategy_return)
        if spy_return is not None:
            matched_spy_returns.append(spy_return)
            excess_returns.append(strategy_return - spy_return)
        daily_rows.append({
            "target_date": target_date,
            "strategy_return": strategy_return,
            "spy_return": spy_return,
            "excess_return_vs_spy": (strategy_return - spy_return) if spy_return is not None else None,
            "gross_exposure": sum(weights.values()),
            "positions": len(weights),
        })

    total_return = math.exp(sum(strategy_returns)) - 1.0 if strategy_returns else 0.0
    spy_total_return = math.exp(sum(matched_spy_returns)) - 1.0 if matched_spy_returns else None
    excess_total_return = (total_return - spy_total_return) if spy_total_return is not None else None
    return {
        "daily_rows": daily_rows,
        "trading_days": len(strategy_returns),
        "benchmark_days": len(matched_spy_returns),
        "benchmark_available": len(matched_spy_returns) > 0,
        "total_return": total_return,
        "spy_total_return": spy_total_return,
        "excess_return_vs_spy": excess_total_return,
        "sharpe": _annualized_sharpe(strategy_returns),
        "max_drawdown": _max_drawdown(strategy_returns),
        "turnover": _turnover(weight_history),
        "daily_hit_rate": _hit_rate(strategy_returns),
        "benchmark_hit_rate": _hit_rate(excess_returns) if excess_returns else None,
        "spy_sharpe": _annualized_sharpe(matched_spy_returns) if matched_spy_returns else None,
    }


def build_walk_forward_summary(dates: list[str], *, train_years: int, test_months: int) -> dict[str, Any]:
    train_days = train_years * 252
    test_days = max(1, int(test_months * 21))
    windows = []
    start = 0
    while start + train_days + test_days <= len(dates):
        train_slice = dates[start:start + train_days]
        test_slice = dates[start + train_days:start + train_days + test_days]
        windows.append({
            "train_start": train_slice[0],
            "train_end": train_slice[-1],
            "test_start": test_slice[0],
            "test_end": test_slice[-1],
            "train_days": len(train_slice),
            "test_days": len(test_slice),
        })
        start += test_days
    required_days = train_days + test_days
    return {
        "protocol": "walk_forward_scaffold",
        "train_years": train_years,
        "test_months": test_months,
        "required_days_for_first_window": required_days,
        "available_days": len(dates),
        "window_count": len(windows),
        "sufficient_history": len(dates) >= required_days,
        "windows": windows,
    }


def evaluate_promotion(portfolio_metrics: dict[str, Any], walk_forward: dict[str, Any]) -> dict[str, Any]:
    benchmark_available = bool(portfolio_metrics["benchmark_available"])
    excess_return = portfolio_metrics["excess_return_vs_spy"]
    checks = {
        "meets_min_backtest_days": portfolio_metrics["trading_days"] >= PROMOTION_THRESHOLDS["min_backtest_days"],
        "meets_sharpe_threshold": portfolio_metrics["sharpe"] > PROMOTION_THRESHOLDS["min_sharpe"],
        "meets_drawdown_threshold": portfolio_metrics["max_drawdown"] < PROMOTION_THRESHOLDS["max_drawdown"],
        "has_spy_benchmark": benchmark_available,
        "beats_spy": (excess_return is not None and excess_return > 0.0) if PROMOTION_THRESHOLDS["must_beat_spy"] else True,
        "has_walk_forward_window": walk_forward["window_count"] > 0,
    }
    return {
        "thresholds": PROMOTION_THRESHOLDS,
        "checks": checks,
        "promote": all(checks.values()),
    }


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2))


def main() -> None:
    args = parse_args()
    dataset_path = resolve_input_path(args.dataset, directory=DATASET_OUTPUT_DIR, suffix="*.jsonl")
    artifact_path = resolve_input_path(args.artifact, directory=MODEL_OUTPUT_DIR, suffix="*.artifact.json")
    samples = load_jsonl(dataset_path)
    predictions = build_predictions(samples, artifact_path)
    by_day = allocate_daily_weights(predictions, max_weight_per_ticker=args.max_weight_per_ticker)
    benchmark_returns = load_benchmark_returns()
    portfolio_metrics = evaluate_portfolio(by_day, benchmark_returns)
    prediction_metrics = {
        "sample_count": len(predictions),
        "directional_hit_rate": _directional_hit_rate(predictions),
        "brier_score": _brier_score(predictions),
        "calibration": _calibration_buckets(predictions),
    }
    walk_forward = build_walk_forward_summary(sorted(by_day.keys()), train_years=args.train_years, test_months=args.test_months)
    promotion = evaluate_promotion(portfolio_metrics, walk_forward)

    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output_name = f"{args.output_prefix}_{stamp}.json"
    output_path = BACKTEST_OUTPUT_DIR / output_name
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "dataset_path": str(dataset_path.relative_to(ROOT_DIR)),
        "artifact_path": str(artifact_path.relative_to(ROOT_DIR)),
        "walk_forward": walk_forward,
        "portfolio_metrics": portfolio_metrics,
        "prediction_metrics": prediction_metrics,
        "promotion": promotion,
    }
    write_json(output_path, payload)
    print(json.dumps({
        "status": "ok",
        "report": str(output_path.relative_to(ROOT_DIR)),
        "trading_days": portfolio_metrics["trading_days"],
        "sharpe": portfolio_metrics["sharpe"],
        "max_drawdown": portfolio_metrics["max_drawdown"],
        "beats_spy": promotion["checks"]["beats_spy"],
        "promote": promotion["promote"],
    }, indent=2))


if __name__ == "__main__":
    main()
