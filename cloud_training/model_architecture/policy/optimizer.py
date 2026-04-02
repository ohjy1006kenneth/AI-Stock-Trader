from __future__ import annotations

from typing import Any

from .contracts import (
    MAX_WEIGHT_PER_TICKER,
    POLICY_SIGNAL_TYPE_LONG,
    build_policy_output_payload,
    validate_policy_observation_payload,
)


def _clamp(value: float, lower: float, upper: float) -> float:
    return max(lower, min(upper, value))


def build_policy_observation(*, as_of_date: str, scored_universe: list[dict[str, Any]], portfolio: dict[str, Any]) -> dict[str, Any]:
    positions = portfolio.get("positions", []) if isinstance(portfolio, dict) else []
    cash = float(portfolio.get("cash", 0.0) or 0.0) if isinstance(portfolio, dict) else 0.0

    normalized_positions: list[dict[str, Any]] = []
    total_market_value = 0.0
    for position in positions:
        qty = int(float(position.get("qty", 0) or 0))
        market_value = float(position.get("market_value", 0.0) or 0.0)
        total_market_value += max(market_value, 0.0)
        normalized_positions.append({
            "ticker": str(position.get("ticker") or ""),
            "qty": qty,
            "market_value": max(market_value, 0.0),
            "unrealized_pnl": float(position.get("unrealized_pnl", 0.0) or 0.0),
        })

    equity = float(portfolio.get("equity", cash + total_market_value) or 0.0) if isinstance(portfolio, dict) else (cash + total_market_value)
    equity = max(equity, cash + total_market_value, 0.0)

    position_map = {row["ticker"]: row for row in normalized_positions if row["ticker"]}
    portfolio_rows = []
    for row in normalized_positions:
        ticker = row["ticker"]
        portfolio_rows.append({
            "ticker": ticker,
            "current_weight": round(_clamp((row["market_value"] / equity) if equity > 0 else 0.0, 0.0, 1.0), 6),
            "qty": row["qty"],
            "market_value": round(row["market_value"], 6),
            "unrealized_pnl": round(row["unrealized_pnl"], 6),
        })

    candidates = []
    seen: set[str] = set()
    for item in scored_universe:
        ticker = str(item.get("ticker") or "")
        if not ticker or ticker in seen:
            continue
        seen.add(ticker)
        score = item.get("score", {}) if isinstance(item.get("score"), dict) else {}
        position = position_map.get(ticker, {})
        current_market_value = float(position.get("market_value", 0.0) or 0.0)
        candidates.append({
            "ticker": ticker,
            "signal": float(score.get("signal", 0.5) or 0.5),
            "confidence": float(score.get("confidence", 0.0) or 0.0),
            "embeddings": list(score.get("embeddings", [])) if isinstance(score.get("embeddings"), list) else [],
            "current_weight": round(_clamp((current_market_value / equity) if equity > 0 else 0.0, 0.0, 1.0), 6),
            "current_qty": int(float(position.get("qty", 0) or 0)),
            "unrealized_pnl": float(position.get("unrealized_pnl", 0.0) or 0.0),
        })

    observation = {
        "as_of_date": as_of_date,
        "portfolio": {
            "cash": round(cash, 6),
            "equity": round(equity, 6),
            "positions": portfolio_rows,
        },
        "candidates": candidates,
    }
    validate_policy_observation_payload(observation)
    return observation


def run_constrained_long_only_policy(
    observation: dict[str, Any],
    *,
    max_weight_per_ticker: float = MAX_WEIGHT_PER_TICKER,
    signal_floor: float = 0.5,
    conviction_scale: float = 0.35,
    hold_retention_fraction: float = 0.25,
    min_active_weight: float = 0.005,
) -> dict[str, Any]:
    validate_policy_observation_payload(observation)

    candidates = observation["candidates"]
    raw_predictions: list[dict[str, Any]] = []
    total_weight = 0.0
    for candidate in candidates:
        ticker = str(candidate["ticker"])
        signal = _clamp(float(candidate["signal"]), 0.0, 1.0)
        confidence = _clamp(float(candidate["confidence"]), 0.0, 1.0)
        current_weight = _clamp(float(candidate["current_weight"]), 0.0, 1.0)

        positive_edge = max(signal - signal_floor, 0.0)
        conviction = positive_edge * confidence
        desired_weight = max_weight_per_ticker * min(conviction / max(conviction_scale, 1e-9), 1.0)

        if current_weight > 0.0:
            retention_floor = min(current_weight, max_weight_per_ticker) * hold_retention_fraction * (1.0 - confidence)
            desired_weight = max(desired_weight, retention_floor)

        if desired_weight < min_active_weight:
            desired_weight = 0.0

        desired_weight = _clamp(desired_weight, 0.0, max_weight_per_ticker)
        total_weight += desired_weight
        raw_predictions.append({
            "ticker": ticker,
            "target_weight": desired_weight,
            "confidence": round(confidence, 6),
            "signal_type": POLICY_SIGNAL_TYPE_LONG,
        })

    if total_weight > 1.0 + 1e-9:
        scale = 1.0 / total_weight
        for row in raw_predictions:
            row["target_weight"] = float(row["target_weight"]) * scale

    predictions = [
        {
            **row,
            "target_weight": round(float(row["target_weight"]), 6),
        }
        for row in sorted(raw_predictions, key=lambda item: item["ticker"])
    ]
    return build_policy_output_payload(predictions)


def build_policy_predictions(*, as_of_date: str, scored_universe: list[dict[str, Any]], portfolio: dict[str, Any]) -> dict[str, Any]:
    observation = build_policy_observation(
        as_of_date=as_of_date,
        scored_universe=scored_universe,
        portfolio=portfolio,
    )
    return run_constrained_long_only_policy(observation)
