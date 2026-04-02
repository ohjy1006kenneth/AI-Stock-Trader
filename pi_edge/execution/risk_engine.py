from __future__ import annotations

from typing import Any

from cloud_inference.contracts import validate_response_payload

DEFAULT_MAX_WEIGHT_PER_TICKER = 0.20


def _clamp(value: float, lower: float, upper: float) -> float:
    return max(lower, min(upper, value))


def _round_weight(value: float) -> float:
    return round(max(value, 0.0), 6)


def apply_hard_risk_constraints(
    *,
    oracle_response: dict[str, Any],
    request_universe: list[str],
    current_positions: list[str],
    execution_config: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    validate_response_payload(oracle_response, request_universe, current_positions)

    risk_limits = execution_config.get("risk_limits", {}) if isinstance(execution_config, dict) else {}
    configured_max_weight = float(risk_limits.get("max_position_weight", DEFAULT_MAX_WEIGHT_PER_TICKER) or DEFAULT_MAX_WEIGHT_PER_TICKER)
    max_weight_per_ticker = _clamp(configured_max_weight, 0.0, DEFAULT_MAX_WEIGHT_PER_TICKER)
    configured_cash_buffer = float(risk_limits.get("cash_buffer_rule", 0.0) or 0.0)
    cash_buffer = _clamp(configured_cash_buffer, 0.0, 1.0)
    investable_limit = max(0.0, 1.0 - cash_buffer)
    max_total_positions = int(risk_limits.get("max_total_positions", 0) or 0)

    current_position_set = {str(ticker) for ticker in current_positions}
    adjusted_rows: list[dict[str, Any]] = []
    clipped_by_weight: list[str] = []
    for row in oracle_response.get("predictions", []):
        ticker = str(row["ticker"])
        original_weight = float(row["target_weight"])
        clipped_weight = _clamp(original_weight, 0.0, max_weight_per_ticker)
        if clipped_weight + 1e-12 < original_weight:
            clipped_by_weight.append(ticker)
        adjusted_rows.append({
            **row,
            "target_weight": clipped_weight,
        })

    zeroed_by_position_limit: list[str] = []
    if max_total_positions > 0:
        positive_rows = [row for row in adjusted_rows if float(row["target_weight"]) > 0.0]
        positive_rows_sorted = sorted(
            positive_rows,
            key=lambda row: (
                0 if str(row["ticker"]) in current_position_set else 1,
                -float(row["target_weight"]),
                -float(row["confidence"]),
                str(row["ticker"]),
            ),
        )
        keep_tickers = {str(row["ticker"]) for row in positive_rows_sorted[:max_total_positions]}
        for row in adjusted_rows:
            if float(row["target_weight"]) > 0.0 and str(row["ticker"]) not in keep_tickers:
                row["target_weight"] = 0.0
                zeroed_by_position_limit.append(str(row["ticker"]))

    total_weight_before_scale = sum(float(row["target_weight"]) for row in adjusted_rows)
    scaled_for_cash_buffer = False
    scale_factor = 1.0
    if total_weight_before_scale > investable_limit + 1e-9:
        scale_factor = 0.0 if total_weight_before_scale <= 0.0 else investable_limit / total_weight_before_scale
        scaled_for_cash_buffer = True
        for row in adjusted_rows:
            row["target_weight"] = float(row["target_weight"]) * scale_factor

    adjusted_rows = [
        {
            **row,
            "target_weight": _round_weight(float(row["target_weight"])),
            "confidence": round(float(row["confidence"]), 6),
        }
        for row in adjusted_rows
    ]

    rounded_total_weight = sum(float(row["target_weight"]) for row in adjusted_rows)
    overflow = rounded_total_weight - investable_limit
    if overflow > 1e-9:
        positive_rows_desc = sorted(
            [row for row in adjusted_rows if float(row["target_weight"]) > 0.0],
            key=lambda row: (-float(row["target_weight"]), str(row["ticker"])),
        )
        for row in positive_rows_desc:
            if overflow <= 0.0:
                break
            reduction = min(float(row["target_weight"]), overflow)
            row["target_weight"] = _round_weight(float(row["target_weight"]) - reduction)
            overflow = round(overflow - reduction, 6)

    adjusted_response = {
        **oracle_response,
        "predictions": sorted(adjusted_rows, key=lambda row: str(row["ticker"])),
    }
    validate_response_payload(adjusted_response, request_universe, current_positions)

    summary = {
        "applied": True,
        "max_weight_per_ticker": round(max_weight_per_ticker, 6),
        "cash_buffer": round(cash_buffer, 6),
        "investable_limit": round(investable_limit, 6),
        "max_total_positions": max_total_positions,
        "clipped_by_weight": sorted(clipped_by_weight),
        "zeroed_by_position_limit": sorted(zeroed_by_position_limit),
        "scaled_for_cash_buffer": scaled_for_cash_buffer,
        "scale_factor": round(scale_factor, 6),
        "final_total_target_weight": round(sum(float(row["target_weight"]) for row in adjusted_response["predictions"]), 6),
        "final_active_positions": sum(1 for row in adjusted_response["predictions"] if float(row["target_weight"]) > 0.0),
    }
    return adjusted_response, summary
