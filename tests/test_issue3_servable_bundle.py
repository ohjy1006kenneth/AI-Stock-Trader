from __future__ import annotations

import json
import unittest
from pathlib import Path

from cloud_inference.artifact_loader import load_bundle
from cloud_inference.handler import EndpointHandler

ROOT_DIR = Path(__file__).resolve().parents[1]
DATASET_PATH = ROOT_DIR / "data" / "processed" / "training" / "issue1_real_dataset_20260328T174106Z.jsonl"


def _load_first_sample() -> dict:
    with DATASET_PATH.open() as f:
        return json.loads(next(line for line in f if line.strip()))


class TestIssue3ServableBundle(unittest.TestCase):
    def test_bundle_loads_and_predicts(self) -> None:
        bundle = load_bundle()
        sample = _load_first_sample()
        prediction = bundle.predict_from_sample(sample)
        self.assertEqual(bundle.bundle_version, "predictive_artifact_bundle_v1")
        self.assertIn("preferred_serving_mode", bundle.inference_contract)
        self.assertGreaterEqual(prediction["signal"], 0.0)
        self.assertLessEqual(prediction["signal"], 1.0)
        self.assertGreaterEqual(prediction["confidence"], 0.0)
        self.assertLessEqual(prediction["confidence"], 1.0)
        self.assertEqual(len(prediction["embeddings"]), len(bundle.feature_names))

    def test_handler_loads_bundle_and_scores_hf_contract_inputs(self) -> None:
        handler = EndpointHandler()
        sample = _load_first_sample()
        response = handler({
            "inputs": {
                "portfolio": {"cash": 10000, "positions": []},
                "universe": [
                    {
                        "ticker": sample["ticker"],
                        "history": sample["history"],
                        "news": sample.get("news", []),
                    }
                ],
            },
            "request_id": "req-test-1",
        })
        self.assertEqual(response["request_id"], "req-test-1")
        self.assertEqual(response["model_version"], load_bundle().artifact_name)
        self.assertEqual(len(response["predictions"]), 1)
        prediction = response["predictions"][0]
        self.assertEqual(prediction["ticker"], sample["ticker"])
        self.assertGreaterEqual(prediction["target_weight"], 0.0)
        self.assertLessEqual(prediction["target_weight"], 0.2)
        self.assertGreaterEqual(prediction["confidence"], 0.0)
        self.assertLessEqual(prediction["confidence"], 1.0)


if __name__ == "__main__":
    unittest.main()
