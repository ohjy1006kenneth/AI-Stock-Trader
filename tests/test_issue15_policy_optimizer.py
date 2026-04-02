from __future__ import annotations

import unittest

from cloud_inference.contracts import validate_response_payload
from cloud_training.model_architecture.policy import (
    POLICY_ACTION_ADJUST_TO_TARGET_WEIGHTS,
    build_policy_observation,
    build_policy_predictions,
    run_constrained_long_only_policy,
)


class TestIssue15PolicyOptimizer(unittest.TestCase):
    def test_build_policy_observation_enriches_scores_with_portfolio_state(self) -> None:
        observation = build_policy_observation(
            as_of_date="2026-04-02",
            scored_universe=[
                {
                    "ticker": "AAPL",
                    "score": {"signal": 0.82, "confidence": 0.74, "embeddings": [0.1, -0.2]},
                },
                {
                    "ticker": "MSFT",
                    "score": {"signal": 0.61, "confidence": 0.55, "embeddings": [0.3]},
                },
            ],
            portfolio={
                "cash": 40000.0,
                "positions": [
                    {"ticker": "AAPL", "qty": 50, "market_value": 10000.0, "unrealized_pnl": 250.0},
                ],
            },
        )

        self.assertEqual(observation["portfolio"]["equity"], 50000.0)
        self.assertEqual(observation["portfolio"]["positions"][0]["current_weight"], 0.2)
        by_ticker = {row["ticker"]: row for row in observation["candidates"]}
        self.assertEqual(by_ticker["AAPL"]["current_qty"], 50)
        self.assertEqual(by_ticker["AAPL"]["current_weight"], 0.2)
        self.assertEqual(by_ticker["MSFT"]["current_weight"], 0.0)

    def test_optimizer_respects_caps_and_total_budget(self) -> None:
        observation = {
            "as_of_date": "2026-04-02",
            "portfolio": {"cash": 100000.0, "equity": 100000.0, "positions": []},
            "candidates": [
                {
                    "ticker": f"T{idx}",
                    "signal": 0.99,
                    "confidence": 1.0,
                    "embeddings": [],
                    "current_weight": 0.0,
                    "current_qty": 0,
                    "unrealized_pnl": 0.0,
                }
                for idx in range(1, 9)
            ],
        }

        payload = run_constrained_long_only_policy(observation)
        self.assertEqual(payload["action"], POLICY_ACTION_ADJUST_TO_TARGET_WEIGHTS)
        weights = [row["target_weight"] for row in payload["predictions"]]
        self.assertTrue(all(0.0 <= weight <= 0.2 for weight in weights))
        self.assertAlmostEqual(sum(weights), 1.0, places=6)

    def test_optimizer_can_keep_small_residual_weight_in_existing_position(self) -> None:
        observation = {
            "as_of_date": "2026-04-02",
            "portfolio": {
                "cash": 9000.0,
                "equity": 10000.0,
                "positions": [
                    {"ticker": "AAPL", "current_weight": 0.1, "qty": 10, "market_value": 1000.0, "unrealized_pnl": -50.0},
                ],
            },
            "candidates": [
                {
                    "ticker": "AAPL",
                    "signal": 0.52,
                    "confidence": 0.2,
                    "embeddings": [],
                    "current_weight": 0.1,
                    "current_qty": 10,
                    "unrealized_pnl": -50.0,
                },
                {
                    "ticker": "MSFT",
                    "signal": 0.51,
                    "confidence": 0.1,
                    "embeddings": [],
                    "current_weight": 0.0,
                    "current_qty": 0,
                    "unrealized_pnl": 0.0,
                },
            ],
        }

        payload = run_constrained_long_only_policy(observation)
        by_ticker = {row["ticker"]: row for row in payload["predictions"]}
        self.assertGreater(by_ticker["AAPL"]["target_weight"], 0.0)
        self.assertEqual(by_ticker["MSFT"]["target_weight"], 0.0)

    def test_policy_predictions_match_oracle_response_contract(self) -> None:
        policy_payload = build_policy_predictions(
            as_of_date="2026-04-02",
            scored_universe=[
                {"ticker": "AAPL", "score": {"signal": 0.85, "confidence": 0.9, "embeddings": [0.1]}},
                {"ticker": "MSFT", "score": {"signal": 0.55, "confidence": 0.4, "embeddings": [0.2]}},
            ],
            portfolio={
                "cash": 1000.0,
                "positions": [
                    {"ticker": "AAPL", "qty": 1, "market_value": 100.0, "unrealized_pnl": 5.0},
                ],
            },
        )

        oracle_payload = {
            "model_version": "issue15-policy-v1",
            "generated_at": "2026-04-02T20:40:00+00:00",
            "request_id": "req-issue15",
            "predictions": policy_payload["predictions"],
        }
        validate_response_payload(oracle_payload, ["AAPL", "MSFT"], ["AAPL"])


if __name__ == "__main__":
    unittest.main()
