from __future__ import annotations

from typing import Any

POLICY_ACTION_ADJUST_TO_TARGET_WEIGHTS = "ADJUST_TO_TARGET_WEIGHTS"
POLICY_SIGNAL_TYPE_LONG = "long"
MAX_WEIGHT_PER_TICKER = 0.20


def _require(cond: bool, message: str) -> None:
    if not cond:
        raise ValueError(message)


def validate_policy_observation_payload(payload: dict[str, Any]) -> None:
    _require(isinstance(payload, dict), "policy_observation_must_be_object")
    _require(
        set(payload.keys()) == {"as_of_date", "portfolio", "candidates"},
        "policy_observation_invalid_top_level_fields",
    )
    _require(isinstance(payload["as_of_date"], str) and payload["as_of_date"], "policy_as_of_date_must_be_nonempty")

    portfolio = payload["portfolio"]
    _require(isinstance(portfolio, dict), "policy_portfolio_must_be_object")
    _require(
        set(portfolio.keys()) == {"cash", "equity", "positions"},
        "policy_portfolio_invalid_fields",
    )
    _require(float(portfolio["cash"]) >= 0.0, "policy_cash_must_be_nonnegative")
    _require(float(portfolio["equity"]) >= 0.0, "policy_equity_must_be_nonnegative")
    positions = portfolio["positions"]
    _require(isinstance(positions, list), "policy_positions_must_be_array")

    seen_positions: set[str] = set()
    for position in positions:
        _require(isinstance(position, dict), "policy_position_must_be_object")
        _require(
            set(position.keys()) == {"ticker", "current_weight", "qty", "market_value", "unrealized_pnl"},
            "policy_position_invalid_fields",
        )
        ticker = str(position["ticker"])
        _require(ticker, "policy_position_ticker_must_be_nonempty")
        _require(ticker not in seen_positions, f"duplicate_policy_position:{ticker}")
        seen_positions.add(ticker)
        _require(float(position["current_weight"]) >= 0.0, f"invalid_position_current_weight:{ticker}")
        _require(int(position["qty"]) >= 0, f"invalid_position_qty:{ticker}")
        _require(float(position["market_value"]) >= 0.0, f"invalid_position_market_value:{ticker}")
        _require(isinstance(position["unrealized_pnl"], (int, float)), f"invalid_position_unrealized_pnl:{ticker}")

    candidates = payload["candidates"]
    _require(isinstance(candidates, list) and len(candidates) > 0, "policy_candidates_must_be_nonempty_array")
    seen_candidates: set[str] = set()
    total_current_weight = 0.0
    for candidate in candidates:
        _require(isinstance(candidate, dict), "policy_candidate_must_be_object")
        _require(
            set(candidate.keys()) == {
                "ticker",
                "signal",
                "confidence",
                "embeddings",
                "current_weight",
                "current_qty",
                "unrealized_pnl",
            },
            "policy_candidate_invalid_fields",
        )
        ticker = str(candidate["ticker"])
        _require(ticker, "policy_candidate_ticker_must_be_nonempty")
        _require(ticker not in seen_candidates, f"duplicate_policy_candidate:{ticker}")
        seen_candidates.add(ticker)
        signal = float(candidate["signal"])
        confidence = float(candidate["confidence"])
        current_weight = float(candidate["current_weight"])
        _require(0.0 <= signal <= 1.0, f"invalid_candidate_signal:{ticker}")
        _require(0.0 <= confidence <= 1.0, f"invalid_candidate_confidence:{ticker}")
        _require(0.0 <= current_weight <= 1.0, f"invalid_candidate_current_weight:{ticker}")
        _require(int(candidate["current_qty"]) >= 0, f"invalid_candidate_current_qty:{ticker}")
        _require(isinstance(candidate["unrealized_pnl"], (int, float)), f"invalid_candidate_unrealized_pnl:{ticker}")
        embeddings = candidate["embeddings"]
        _require(isinstance(embeddings, list), f"candidate_embeddings_must_be_array:{ticker}")
        for idx, value in enumerate(embeddings):
            _require(isinstance(value, (int, float)), f"candidate_embedding_value_must_be_numeric:{ticker}:{idx}")
        total_current_weight += current_weight

    _require(total_current_weight <= 1.0 + 1e-9, "invalid_total_current_weight_exceeds_one")


def validate_policy_output_payload(payload: dict[str, Any], *, allowed_tickers: list[str]) -> None:
    _require(isinstance(payload, dict), "policy_output_must_be_object")
    _require(
        set(payload.keys()) == {"action", "predictions"},
        "policy_output_invalid_top_level_fields",
    )
    _require(payload["action"] == POLICY_ACTION_ADJUST_TO_TARGET_WEIGHTS, "invalid_policy_action")
    predictions = payload["predictions"]
    _require(isinstance(predictions, list) and len(predictions) > 0, "policy_predictions_must_be_nonempty_array")

    seen: set[str] = set()
    allowed = set(allowed_tickers)
    total_weight = 0.0
    for row in predictions:
        _require(isinstance(row, dict), "policy_prediction_must_be_object")
        _require(
            set(row.keys()) == {"ticker", "target_weight", "confidence", "signal_type"},
            "policy_prediction_invalid_fields",
        )
        ticker = str(row["ticker"])
        _require(ticker in allowed, f"policy_prediction_contains_unknown_ticker:{ticker}")
        _require(ticker not in seen, f"duplicate_policy_prediction:{ticker}")
        seen.add(ticker)
        target_weight = float(row["target_weight"])
        confidence = float(row["confidence"])
        _require(0.0 <= target_weight <= MAX_WEIGHT_PER_TICKER, f"invalid_policy_target_weight:{ticker}")
        _require(0.0 <= confidence <= 1.0, f"invalid_policy_confidence:{ticker}")
        _require(str(row["signal_type"]) == POLICY_SIGNAL_TYPE_LONG, f"invalid_policy_signal_type:{ticker}")
        total_weight += target_weight
    _require(total_weight <= 1.0 + 1e-9, "invalid_policy_total_target_weight_exceeds_one")


def build_policy_output_payload(predictions: list[dict[str, Any]]) -> dict[str, Any]:
    payload = {
        "action": POLICY_ACTION_ADJUST_TO_TARGET_WEIGHTS,
        "predictions": predictions,
    }
    validate_policy_output_payload(payload, allowed_tickers=[str(row["ticker"]) for row in predictions])
    return payload
