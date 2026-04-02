from __future__ import annotations

import unittest

from pi_edge.execution.risk_engine import apply_hard_risk_constraints


class TestIssue16RiskEngine(unittest.TestCase):
    def test_caps_position_weights_enforces_cash_buffer_and_limits_active_positions(self) -> None:
        oracle_response = {
            "model_version": "issue16-policy-v1",
            "generated_at": "2026-04-02T20:45:00+00:00",
            "request_id": "req-issue16-risk",
            "predictions": [
                {"ticker": "AAPL", "target_weight": 0.20, "confidence": 0.90, "signal_type": "long"},
                {"ticker": "MSFT", "target_weight": 0.18, "confidence": 0.80, "signal_type": "long"},
                {"ticker": "NVDA", "target_weight": 0.16, "confidence": 0.70, "signal_type": "long"},
                {"ticker": "AMZN", "target_weight": 0.14, "confidence": 0.60, "signal_type": "long"},
            ],
        }
        execution_config = {
            "risk_limits": {
                "max_total_positions": 3,
                "max_position_weight": 0.12,
                "cash_buffer_rule": 0.05,
            }
        }

        adjusted, summary = apply_hard_risk_constraints(
            oracle_response=oracle_response,
            request_universe=["AAPL", "MSFT", "NVDA", "AMZN"],
            current_positions=[],
            execution_config=execution_config,
        )

        by_ticker = {row["ticker"]: row for row in adjusted["predictions"]}
        self.assertEqual(by_ticker["AMZN"]["target_weight"], 0.0)
        self.assertAlmostEqual(by_ticker["AAPL"]["target_weight"], 0.12, places=6)
        self.assertAlmostEqual(by_ticker["MSFT"]["target_weight"], 0.12, places=6)
        self.assertAlmostEqual(by_ticker["NVDA"]["target_weight"], 0.12, places=6)
        self.assertAlmostEqual(sum(row["target_weight"] for row in adjusted["predictions"]), 0.36, places=6)
        self.assertEqual(summary["final_active_positions"], 3)
        self.assertEqual(summary["clipped_by_weight"], ["AAPL", "AMZN", "MSFT", "NVDA"])
        self.assertEqual(summary["zeroed_by_position_limit"], ["AMZN"])
        self.assertFalse(summary["scaled_for_cash_buffer"])

    def test_scales_to_respect_cash_buffer_after_weight_capping(self) -> None:
        oracle_response = {
            "model_version": "issue16-policy-v1",
            "generated_at": "2026-04-02T20:45:00+00:00",
            "request_id": "req-issue16-scale",
            "predictions": [
                {"ticker": f"T{idx}", "target_weight": 0.12, "confidence": 0.95, "signal_type": "long"}
                for idx in range(1, 9)
            ],
        }
        execution_config = {
            "risk_limits": {
                "max_total_positions": 8,
                "max_position_weight": 0.12,
                "cash_buffer_rule": 0.05,
            }
        }

        adjusted, summary = apply_hard_risk_constraints(
            oracle_response=oracle_response,
            request_universe=[f"T{idx}" for idx in range(1, 9)],
            current_positions=[],
            execution_config=execution_config,
        )

        weights = [row["target_weight"] for row in adjusted["predictions"]]
        self.assertTrue(all(0.0 <= weight <= 0.12 for weight in weights))
        self.assertAlmostEqual(sum(weights), 0.95, places=6)
        self.assertTrue(summary["scaled_for_cash_buffer"])
        self.assertAlmostEqual(summary["final_total_target_weight"], 0.95, places=6)

    def test_current_holdings_are_prioritized_when_position_limit_binds(self) -> None:
        oracle_response = {
            "model_version": "issue16-policy-v1",
            "generated_at": "2026-04-02T20:45:00+00:00",
            "request_id": "req-issue16-held",
            "predictions": [
                {"ticker": "AAPL", "target_weight": 0.08, "confidence": 0.20, "signal_type": "long"},
                {"ticker": "MSFT", "target_weight": 0.12, "confidence": 0.95, "signal_type": "long"},
                {"ticker": "NVDA", "target_weight": 0.11, "confidence": 0.90, "signal_type": "long"},
            ],
        }
        execution_config = {
            "risk_limits": {
                "max_total_positions": 2,
                "max_position_weight": 0.12,
                "cash_buffer_rule": 0.0,
            }
        }

        adjusted, summary = apply_hard_risk_constraints(
            oracle_response=oracle_response,
            request_universe=["AAPL", "MSFT", "NVDA"],
            current_positions=["AAPL"],
            execution_config=execution_config,
        )

        by_ticker = {row["ticker"]: row for row in adjusted["predictions"]}
        self.assertGreater(by_ticker["AAPL"]["target_weight"], 0.0)
        self.assertGreater(by_ticker["MSFT"]["target_weight"], 0.0)
        self.assertEqual(by_ticker["NVDA"]["target_weight"], 0.0)
        self.assertEqual(summary["zeroed_by_position_limit"], ["NVDA"])


if __name__ == "__main__":
    unittest.main()
