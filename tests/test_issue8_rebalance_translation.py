from __future__ import annotations

import unittest

from pi_edge.execution.paper_portfolio_executor import build_oracle_payload, compute_rebalance_actions


class TestIssue8RebalanceTranslation(unittest.TestCase):
    def test_build_oracle_payload_trims_to_recent_21_bars_and_maps_positions(self) -> None:
        history = [
            {
                "date": f"2026-03-{day:02d}",
                "open": float(day),
                "high": float(day) + 1.0,
                "low": float(day) - 1.0,
                "close": float(day) + 0.5,
                "volume": 1000 + day,
                "adj_close": float(day) + 0.5,
            }
            for day in range(1, 31)
        ]
        price_snapshot = {
            "items": [
                {"ticker": "AAPL", "history": history},
            ]
        }
        broker_snapshot = {
            "account": {"cash": "1250.5"},
            "positions": [
                {"symbol": "MSFT", "qty": "3", "avg_entry_price": "250.0"},
            ],
        }

        payload = build_oracle_payload(price_snapshot, broker_snapshot)
        self.assertEqual(payload["portfolio"]["cash"], 1250.5)
        self.assertEqual(payload["portfolio"]["positions"], [{"ticker": "MSFT", "qty": 3, "entry_price": 250.0}])
        self.assertEqual(len(payload["universe"]), 1)
        self.assertEqual(len(payload["universe"][0]["history"]), 21)
        self.assertEqual(payload["universe"][0]["history"][0]["date"], "2026-03-10")
        self.assertEqual(payload["universe"][0]["news"], [])

    def test_compute_rebalance_actions_generates_buy_sell_and_hold_deltas(self) -> None:
        price_snapshot = {
            "items": [
                {"ticker": "AAPL", "close": 100.0},
                {"ticker": "MSFT", "close": 50.0},
                {"ticker": "GOOGL", "close": 20.0},
                {"ticker": "AMZN", "close": 50.0},
            ]
        }
        broker_snapshot = {
            "account": {"equity": "1000", "cash": "400"},
            "positions": [
                {"symbol": "AAPL", "qty": "1", "avg_entry_price": "90.0"},
                {"symbol": "GOOGL", "qty": "10", "avg_entry_price": "25.0"},
                {"symbol": "AMZN", "qty": "5", "avg_entry_price": "55.0"},
            ],
        }
        predictions_payload = {
            "model_version": "test-bundle-v1",
            "generated_at": "2026-03-29T23:15:00+00:00",
            "request_id": "req-issue8",
            "predictions": [
                {"ticker": "AAPL", "target_weight": 0.2, "confidence": 0.9, "signal_type": "long"},
                {"ticker": "MSFT", "target_weight": 0.1, "confidence": 0.8, "signal_type": "long"},
                {"ticker": "GOOGL", "target_weight": 0.2, "confidence": 0.0, "signal_type": "long"},
                {"ticker": "AMZN", "target_weight": 0.1, "confidence": 0.4, "signal_type": "long"},
            ],
        }

        actions = compute_rebalance_actions(
            predictions_payload=predictions_payload,
            broker_snapshot=broker_snapshot,
            price_snapshot=price_snapshot,
        )
        by_ticker = {row["ticker"]: row for row in actions}

        self.assertEqual(by_ticker["AAPL"]["target_shares"], 2)
        self.assertEqual(by_ticker["AAPL"]["share_delta"], 1)
        self.assertEqual(by_ticker["AAPL"]["action"], "BUY")
        self.assertEqual(by_ticker["AAPL"]["order_qty"], 1)

        self.assertEqual(by_ticker["MSFT"]["target_shares"], 2)
        self.assertEqual(by_ticker["MSFT"]["action"], "BUY")
        self.assertEqual(by_ticker["MSFT"]["order_qty"], 2)

        self.assertEqual(by_ticker["GOOGL"]["target_shares"], 10)
        self.assertEqual(by_ticker["GOOGL"]["share_delta"], 0)
        self.assertEqual(by_ticker["GOOGL"]["action"], "HOLD")
        self.assertEqual(by_ticker["GOOGL"]["order_qty"], 0)

        self.assertEqual(by_ticker["AMZN"]["target_shares"], 2)
        self.assertEqual(by_ticker["AMZN"]["share_delta"], -3)
        self.assertEqual(by_ticker["AMZN"]["action"], "SELL")
        self.assertEqual(by_ticker["AMZN"]["order_qty"], 3)

    def test_compute_rebalance_actions_requires_position_tickers_in_validated_response(self) -> None:
        price_snapshot = {
            "items": [
                {"ticker": "AAPL", "close": 100.0},
            ]
        }
        broker_snapshot = {
            "account": {"equity": "1000", "cash": "400"},
            "positions": [
                {"symbol": "MSFT", "qty": "2", "avg_entry_price": "250.0"},
            ],
        }
        invalid_predictions_payload = {
            "model_version": "test-bundle-v1",
            "generated_at": "2026-03-29T23:15:00+00:00",
            "request_id": "req-issue8-missing-held",
            "predictions": [
                {"ticker": "AAPL", "target_weight": 0.1, "confidence": 0.8, "signal_type": "long"},
            ],
        }

        with self.assertRaisesRegex(ValueError, "missing_predictions_for_held_positions:MSFT"):
            compute_rebalance_actions(
                predictions_payload=invalid_predictions_payload,
                broker_snapshot=broker_snapshot,
                price_snapshot=price_snapshot,
            )


if __name__ == "__main__":
    unittest.main()
