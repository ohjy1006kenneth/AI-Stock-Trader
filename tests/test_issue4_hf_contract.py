from __future__ import annotations

import json
import unittest
from pathlib import Path

from cloud_inference.contracts import validate_request_payload, validate_response_payload
from cloud_inference.feature_adapter import build_predictive_sample
from cloud_inference.handler import EndpointHandler

ROOT_DIR = Path(__file__).resolve().parents[1]
DATASET_PATH = ROOT_DIR / "data" / "processed" / "training" / "issue1_real_dataset_20260328T174106Z.jsonl"


def _load_samples(count: int = 2) -> list[dict]:
    samples: list[dict] = []
    seen_tickers: set[str] = set()
    with DATASET_PATH.open() as f:
        for line in f:
            if not line.strip():
                continue
            sample = json.loads(line)
            ticker = sample["ticker"]
            if ticker in seen_tickers:
                continue
            seen_tickers.add(ticker)
            samples.append(sample)
            if len(samples) >= count:
                break
    return samples


class TestIssue4HFContract(unittest.TestCase):
    def test_request_validation_rejects_duplicate_universe_tickers(self) -> None:
        sample = _load_samples(1)[0]
        payload = {
            "portfolio": {"cash": 1000, "positions": []},
            "universe": [
                {"ticker": sample["ticker"], "history": sample["history"], "news": []},
                {"ticker": sample["ticker"], "history": sample["history"], "news": []},
            ],
        }
        with self.assertRaisesRegex(ValueError, "duplicate_universe_ticker"):
            validate_request_payload(payload)

    def test_handler_returns_predictions_for_universe_and_zeroes_missing_position_targets(self) -> None:
        sample_a, sample_b = _load_samples(2)
        held_ticker = "MSFT"
        payload = {
            "inputs": {
                "portfolio": {
                    "cash": 5000,
                    "positions": [
                        {"ticker": held_ticker, "qty": 3, "entry_price": 250.0},
                    ],
                },
                "universe": [
                    {"ticker": sample_a["ticker"], "history": sample_a["history"], "news": sample_a.get("news", [])},
                    {"ticker": sample_b["ticker"], "history": sample_b["history"], "news": sample_b.get("news", [])},
                ],
            },
            "request_id": "req-issue4-contract",
        }
        response = EndpointHandler()(payload)
        self.assertEqual(response["request_id"], "req-issue4-contract")
        tickers = [row["ticker"] for row in response["predictions"]]
        self.assertIn(sample_a["ticker"], tickers)
        self.assertIn(sample_b["ticker"], tickers)
        self.assertIn(held_ticker, tickers)
        held_row = next(row for row in response["predictions"] if row["ticker"] == held_ticker)
        self.assertEqual(held_row["target_weight"], 0.0)
        self.assertEqual(held_row["confidence"], 0.0)
        validate_response_payload(response, [sample_a["ticker"], sample_b["ticker"]], [held_ticker])

    def test_request_validation_accepts_optional_feature_fields(self) -> None:
        sample = _load_samples(1)[0]
        payload = {
            "portfolio": {"cash": 1000, "positions": []},
            "universe": [
                {
                    "ticker": sample["ticker"],
                    "history": sample["history"],
                    "news": sample.get("news", []),
                    "market_history": sample["history"],
                    "context": {"sector": "Tech", "industry": "Software", "market_cap": 1000000},
                    "precomputed_features": {"return_1d": 0.1, "news_count": 2.0},
                }
            ],
        }
        validate_request_payload(payload)

    def test_feature_adapter_prefers_precomputed_features_when_present(self) -> None:
        sample = _load_samples(1)[0]
        universe = [{
            "ticker": sample["ticker"],
            "history": sample["history"],
            "news": sample.get("news", []),
            "precomputed_features": {"return_1d": 9.99, "finbert_positive_prob_mean": 0.77},
            "context": {"sector": "Tech", "industry": "Software", "market_cap": 1000000},
        }]
        built = build_predictive_sample(universe[0], universe)
        self.assertEqual(built["return_1d"], 9.99)
        self.assertEqual(built["finbert_positive_prob_mean"], 0.77)
        self.assertEqual(built["ticker"], sample["ticker"])

    def test_response_validation_rejects_overweight_portfolio(self) -> None:
        payload = {
            "model_version": "bundle-v1",
            "generated_at": "2026-03-28T18:00:00+00:00",
            "request_id": "req-overweight",
            "predictions": [
                {"ticker": "AAPL", "target_weight": 0.6, "confidence": 0.9, "signal_type": "long"},
                {"ticker": "MSFT", "target_weight": 0.5, "confidence": 0.8, "signal_type": "long"},
            ],
        }
        with self.assertRaisesRegex(ValueError, "invalid_target_weight|invalid_total_target_weight_exceeds_one"):
            validate_response_payload(payload, ["AAPL", "MSFT"], [])


if __name__ == "__main__":
    unittest.main()
