from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT_DIR = Path(__file__).resolve().parents[1]
CONFIG_DIR = ROOT_DIR / "config"
REQUEST_SCHEMA_PATH = CONFIG_DIR / "cloud_oracle_request.schema.json"
RESPONSE_SCHEMA_PATH = CONFIG_DIR / "cloud_oracle_response.schema.json"


def _load_schema(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text())


def _require(cond: bool, message: str) -> None:
    if not cond:
        raise ValueError(message)


def validate_request_payload(payload: dict[str, Any]) -> None:
    _ = _load_schema(REQUEST_SCHEMA_PATH)
    _require(isinstance(payload, dict), "request_payload_must_be_object")
    _require(set(payload.keys()) == {"portfolio", "universe"}, "request_invalid_top_level_fields")

    portfolio = payload["portfolio"]
    universe = payload["universe"]
    _require(isinstance(portfolio, dict), "portfolio_must_be_object")
    _require(set(portfolio.keys()) == {"cash", "positions"}, "portfolio_invalid_fields")
    _require(isinstance(universe, list) and len(universe) > 0, "universe_must_be_nonempty_array")
    _require(float(portfolio["cash"]) >= 0, "portfolio_cash_must_be_nonnegative")
    _require(isinstance(portfolio["positions"], list), "portfolio_positions_must_be_array")

    seen_positions: set[str] = set()
    for pos in portfolio["positions"]:
        _require(isinstance(pos, dict), "position_must_be_object")
        _require(set(pos.keys()) == {"ticker", "qty", "entry_price"}, "position_invalid_fields")
        ticker = str(pos["ticker"])
        _require(ticker not in seen_positions, f"duplicate_portfolio_position:{ticker}")
        seen_positions.add(ticker)
        _require(ticker, "position_ticker_must_be_nonempty")
        _require(int(pos["qty"]) >= 0, "position_qty_must_be_nonnegative")
        _require(float(pos["entry_price"]) >= 0, "position_entry_price_must_be_nonnegative")

    seen_universe: set[str] = set()
    for item in universe:
        _require(isinstance(item, dict), "universe_item_must_be_object")
        allowed_item_fields = {"ticker", "history", "news", "market_history", "context", "precomputed_features"}
        _require(set(item.keys()) <= allowed_item_fields and {"ticker", "history", "news"} <= set(item.keys()), "universe_item_invalid_fields")
        ticker = str(item["ticker"])
        _require(ticker not in seen_universe, f"duplicate_universe_ticker:{ticker}")
        seen_universe.add(ticker)
        _require(ticker, "universe_ticker_must_be_nonempty")
        _require(isinstance(item["history"], list) and len(item["history"]) > 0, "history_must_be_nonempty_array")
        _require(isinstance(item["news"], list), "news_must_be_array")
        for bar in item["history"]:
            _require(isinstance(bar, dict), "history_bar_must_be_object")
            _require(set(bar.keys()) == {"date", "open", "high", "low", "close", "volume"}, "history_bar_invalid_fields")
            _require(int(bar["volume"]) >= 0, "history_bar_volume_must_be_nonnegative")
        for news_item in item["news"]:
            _require(isinstance(news_item, dict), "news_item_must_be_object")
            _require(set(news_item.keys()) == {"date", "headline", "summary"}, "news_item_invalid_fields")
        if "market_history" in item:
            _require(isinstance(item["market_history"], list), "market_history_must_be_array")
            for bar in item["market_history"]:
                _require(isinstance(bar, dict), "market_history_bar_must_be_object")
                _require(set(bar.keys()) == {"date", "open", "high", "low", "close", "volume"}, "market_history_bar_invalid_fields")
                _require(int(bar["volume"]) >= 0, "market_history_bar_volume_must_be_nonnegative")
        if "context" in item:
            _require(isinstance(item["context"], dict), "context_must_be_object")
        if "precomputed_features" in item:
            _require(isinstance(item["precomputed_features"], dict), "precomputed_features_must_be_object")
            for feature_name, feature_value in item["precomputed_features"].items():
                _require(isinstance(feature_name, str) and feature_name, "precomputed_feature_name_must_be_nonempty")
                _require(isinstance(feature_value, (int, float)), f"precomputed_feature_value_must_be_numeric:{feature_name}")


def validate_response_payload(payload: dict[str, Any], request_universe: list[str], current_positions: list[str]) -> None:
    _ = _load_schema(RESPONSE_SCHEMA_PATH)
    _require(isinstance(payload, dict), "response_payload_must_be_object")
    _require(set(payload.keys()) == {"model_version", "generated_at", "request_id", "predictions"}, "response_invalid_top_level_fields")
    _require(isinstance(payload["model_version"], str) and payload["model_version"], "model_version_must_be_nonempty")
    _require(isinstance(payload["generated_at"], str) and payload["generated_at"], "generated_at_must_be_nonempty")
    _require(isinstance(payload["request_id"], str) and payload["request_id"], "request_id_must_be_nonempty")
    preds = payload["predictions"]
    _require(isinstance(preds, list) and len(preds) > 0, "predictions_must_be_nonempty_array")

    seen = set()
    total_weight = 0.0
    allowed = set(request_universe) | set(current_positions)
    for row in preds:
        _require(isinstance(row, dict), "prediction_must_be_object")
        _require(set(row.keys()) == {"ticker", "target_weight", "confidence", "signal_type"}, "prediction_invalid_fields")
        ticker = str(row["ticker"])
        _require(ticker in allowed, f"prediction_contains_unknown_ticker:{ticker}")
        _require(ticker not in seen, f"duplicate_prediction_ticker:{ticker}")
        seen.add(ticker)
        weight = float(row["target_weight"])
        confidence = float(row["confidence"])
        _require(0.0 <= weight <= 0.2, f"invalid_target_weight:{ticker}")
        _require(0.0 <= confidence <= 1.0, f"invalid_confidence:{ticker}")
        _require(str(row["signal_type"]) == "long", f"invalid_signal_type:{ticker}")
        total_weight += weight
    _require(total_weight <= 1.0 + 1e-9, "invalid_total_target_weight_exceeds_one")


def unwrap_hf_request(data: dict[str, Any]) -> tuple[dict[str, Any], str | None]:
    _require(isinstance(data, dict), "hf_request_must_be_object")
    payload = data.get("inputs", data)
    _require(isinstance(payload, dict), "inputs_must_be_a_dict")
    request_id = data.get("request_id")
    if request_id is not None:
        _require(isinstance(request_id, str) and request_id, "request_id_must_be_nonempty")
    validate_request_payload(payload)
    return payload, request_id


def build_prediction_row(ticker: str, score: dict[str, Any]) -> dict[str, Any]:
    signal = float(score["signal"])
    confidence = float(score["confidence"])
    positive_edge = max(signal - 0.5, 0.0)
    raw_weight = min(0.2, positive_edge * 0.4 * confidence)
    return {
        "ticker": ticker,
        "target_weight": round(raw_weight, 6),
        "confidence": round(confidence, 6),
        "signal_type": "long",
    }


def normalize_prediction_weights(predictions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    total_weight = sum(float(row["target_weight"]) for row in predictions)
    if total_weight <= 1.0 + 1e-9:
        return predictions
    scale = 1.0 / total_weight
    normalized = []
    for row in predictions:
        normalized.append({**row, "target_weight": round(float(row["target_weight"]) * scale, 6)})
    return normalized


def build_response_payload(*, model_version: str, request_id: str, scored_universe: list[dict[str, Any]], current_positions: list[str]) -> dict[str, Any]:
    predictions = [build_prediction_row(item["ticker"], item["score"]) for item in scored_universe]
    position_set = set(current_positions)
    predicted_tickers = {row["ticker"] for row in predictions}
    for ticker in sorted(position_set - predicted_tickers):
        predictions.append({
            "ticker": ticker,
            "target_weight": 0.0,
            "confidence": 0.0,
            "signal_type": "long",
        })
    predictions = normalize_prediction_weights(predictions)
    payload = {
        "model_version": model_version,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "request_id": request_id,
        "predictions": sorted(predictions, key=lambda row: row["ticker"]),
    }
    validate_response_payload(payload, [item["ticker"] for item in scored_universe], current_positions)
    return payload
