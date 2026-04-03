from __future__ import annotations

from typing import Any

from cloud_training.data_pipelines.predictive_feature_core import (
    compute_context_features,
    compute_macro_features,
    compute_market_features,
)
from cloud_training.model_architecture.hybrid_model import FEATURE_NAMES


def _coerce_float_features(payload: dict[str, Any] | None) -> dict[str, float]:
    out: dict[str, float] = {}
    for key, value in (payload or {}).items():
        try:
            out[str(key)] = float(value)
        except Exception:
            continue
    return out


def _infer_market_history(item: dict[str, Any], universe: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if isinstance(item.get("market_history"), list) and item["market_history"]:
        return item["market_history"]
    for candidate in universe:
        if str(candidate.get("ticker") or "").upper() == "SPY" and isinstance(candidate.get("history"), list):
            return candidate["history"]
    return item.get("history", [])


def build_predictive_sample(item: dict[str, Any], universe: list[dict[str, Any]]) -> dict[str, Any]:
    history = item["history"]
    news = item.get("news", [])
    as_of_date = history[-1]["date"]
    market_history = _infer_market_history(item, universe)
    trailing_window = history[-63:] if len(history) >= 63 else history
    market_window = market_history[-63:] if len(market_history) >= 63 else market_history

    sample: dict[str, Any] = {
        "ticker": item["ticker"],
        "as_of_date": as_of_date,
        "history": history,
        "news": news,
        "news_count": len(news),
        "news_volume": float(len(news)),
    }

    if len(trailing_window) >= 21 and len(market_window) >= 21:
        sample.update(compute_market_features(trailing_window, market_window))
        sample.update(compute_macro_features(market_window))

    context_payload = item.get("context") if isinstance(item.get("context"), dict) else {}
    if context_payload:
        fundamentals = {str(item["ticker"]): context_payload}
        sector_feature_map: dict[tuple[str, str], dict[str, float]] = {}
        sample.update(compute_context_features(str(item["ticker"]), as_of_date, fundamentals, sector_feature_map))

    sample.update(_coerce_float_features(item.get("precomputed_features")))

    for feature_name in FEATURE_NAMES:
        sample.setdefault(feature_name, 0.0)

    return sample
