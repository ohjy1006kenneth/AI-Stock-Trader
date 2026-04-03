from __future__ import annotations

import unittest

from cloud_inference.contracts import validate_response_payload
from cloud_training.model_architecture.policy.contracts import (
    POLICY_ACTION_ADJUST_TO_TARGET_WEIGHTS,
    build_policy_output_payload,
    validate_policy_observation_payload,
    validate_policy_output_payload,
)


class TestIssue6PolicyContract(unittest.TestCase):
    def test_policy_observation_accepts_predictive_state_plus_portfolio_context(self) -> None:
        payload = {
            "as_of_date": "2026-03-28",
            "portfolio": {
                "cash": 25000.0,
                "equity": 100000.0,
                "positions": [
                    {
                        "ticker": "AAPL",
                        "current_weight": 0.10,
                        "qty": 50,
                        "market_value": 10000.0,
                        "unrealized_pnl": 250.0,
                    }
                ],
            },
            "candidates": [
                {
                    "ticker": "AAPL",
                    "signal": 0.82,
                    "confidence": 0.74,
                    "embeddings": [0.12, -0.45],
                    "current_weight": 0.10,
                    "current_qty": 50,
                    "unrealized_pnl": 250.0,
                },
                {
                    "ticker": "MSFT",
                    "signal": 0.61,
                    "confidence": 0.55,
                    "embeddings": [0.02, 0.11],
                    "current_weight": 0.00,
                    "current_qty": 0,
                    "unrealized_pnl": 0.0,
                },
            ],
        }
        validate_policy_observation_payload(payload)

    def test_policy_output_matches_oracle_prediction_row_shape(self) -> None:
        predictions = [
            {"ticker": "AAPL", "target_weight": 0.15, "confidence": 0.88, "signal_type": "long"},
            {"ticker": "MSFT", "target_weight": 0.05, "confidence": 0.65, "signal_type": "long"},
        ]
        policy_payload = build_policy_output_payload(predictions)
        self.assertEqual(policy_payload["action"], POLICY_ACTION_ADJUST_TO_TARGET_WEIGHTS)
        validate_policy_output_payload(policy_payload, allowed_tickers=["AAPL", "MSFT"])

        oracle_payload = {
            "model_version": "policy-scaffold-v1",
            "generated_at": "2026-03-28T18:45:00+00:00",
            "request_id": "req-issue6-policy",
            "predictions": policy_payload["predictions"],
        }
        validate_response_payload(oracle_payload, ["AAPL", "MSFT"], [])

    def test_policy_output_rejects_unknown_ticker_and_overweight_books(self) -> None:
        with self.assertRaisesRegex(ValueError, "unknown_ticker"):
            validate_policy_output_payload(
                {
                    "action": POLICY_ACTION_ADJUST_TO_TARGET_WEIGHTS,
                    "predictions": [
                        {"ticker": "NVDA", "target_weight": 0.10, "confidence": 0.70, "signal_type": "long"}
                    ],
                },
                allowed_tickers=["AAPL", "MSFT"],
            )

        with self.assertRaisesRegex(ValueError, "invalid_policy_target_weight|invalid_policy_total_target_weight_exceeds_one"):
            validate_policy_output_payload(
                {
                    "action": POLICY_ACTION_ADJUST_TO_TARGET_WEIGHTS,
                    "predictions": [
                        {"ticker": "AAPL", "target_weight": 0.60, "confidence": 0.90, "signal_type": "long"},
                        {"ticker": "MSFT", "target_weight": 0.50, "confidence": 0.80, "signal_type": "long"},
                    ],
                },
                allowed_tickers=["AAPL", "MSFT"],
            )


if __name__ == "__main__":
    unittest.main()
