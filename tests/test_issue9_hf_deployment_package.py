from __future__ import annotations

import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path

from cloud_inference.artifact_loader import resolve_bundle_path
from cloud_inference.build_hf_deployment import build_hf_deployment

ROOT_DIR = Path(__file__).resolve().parents[1]
DATASET_PATH = ROOT_DIR / "data" / "processed" / "training" / "issue1_real_dataset_20260328T174106Z.jsonl"


def _load_first_sample() -> dict:
    with DATASET_PATH.open() as f:
        return json.loads(next(line for line in f if line.strip()))


class TestIssue9HFDeploymentPackage(unittest.TestCase):
    def test_build_outputs_minimal_hf_runtime_and_serves_predictions(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            output_dir = build_hf_deployment(Path(tmpdir) / "hf_oracle")
            self.assertTrue((output_dir / "handler.py").exists())
            self.assertTrue((output_dir / "requirements.txt").exists())
            self.assertTrue((output_dir / "bundle_pin.json").exists())
            self.assertTrue((output_dir / "cloud_inference" / "handler.py").exists())
            self.assertTrue((output_dir / "cloud_inference" / "feature_adapter.py").exists())
            self.assertTrue((output_dir / "cloud_training" / "model_architecture" / "hybrid_model.py").exists())
            self.assertTrue(list((output_dir / "artifacts" / "bundles").glob("*.bundle.json")))
            self.assertTrue((output_dir / "artifacts" / "bundles" / "manifest.json").exists())

            manifest = json.loads((output_dir / "artifacts" / "bundles" / "manifest.json").read_text())
            pin = json.loads((output_dir / "bundle_pin.json").read_text())
            pinned_bundle_path = resolve_bundle_path(str(output_dir))
            self.assertTrue(pinned_bundle_path.exists())
            self.assertEqual(pinned_bundle_path.parent, output_dir / "artifacts" / "bundles")
            self.assertEqual(pinned_bundle_path.name, manifest["default_bundle"])
            self.assertEqual(pin["bundle_name"], pinned_bundle_path.name)

            sys.path.insert(0, str(output_dir))
            try:
                spec = importlib.util.spec_from_file_location("hf_handler", output_dir / "handler.py")
                assert spec and spec.loader
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)
                handler = module.EndpointHandler(model_dir=str(output_dir))
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
                    "request_id": "req-issue9",
                })
            finally:
                sys.path.pop(0)

            self.assertEqual(response["request_id"], "req-issue9")
            self.assertEqual(len(response["predictions"]), 1)
            self.assertEqual(response["predictions"][0]["ticker"], sample["ticker"])


if __name__ == "__main__":
    unittest.main()
