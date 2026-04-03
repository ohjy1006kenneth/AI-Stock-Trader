from __future__ import annotations

import json
import subprocess
import sys
import unittest
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
REPORT_DIR = ROOT_DIR / "reports" / "pipeline"


class TestIssue21CloudPipeline(unittest.TestCase):
    def test_existing_probe_dataset_runs_train_validate_report(self) -> None:
        before_reports = {path.name for path in REPORT_DIR.glob("issue21_cloud_pipeline_*.json")}
        proc = subprocess.run(
            [
                sys.executable,
                "-m",
                "cloud_training.training.run_issue21_cloud_pipeline",
                "--dataset",
                "data/processed/training/issue21_dataset_quality_medium_probe_20260402T211906Z.jsonl",
                "--allow-smoke-dataset",
                "--epochs",
                "30",
                "--output-prefix",
                "issue21_test_candidate",
                "--validation-output-prefix",
                "issue21_test_validation",
            ],
            cwd=ROOT_DIR,
            text=True,
            capture_output=True,
            check=True,
        )
        payload = json.loads(proc.stdout)
        self.assertEqual(payload["status"], "ok")
        self.assertEqual(payload["dataset_sentiment_model"], "mock")
        self.assertTrue((ROOT_DIR / payload["dataset_path"]).exists())
        self.assertTrue((ROOT_DIR / payload["artifact_path"]).exists())
        self.assertTrue((ROOT_DIR / payload["metrics_path"]).exists())
        self.assertTrue((ROOT_DIR / payload["diagnostics_path"]).exists())
        self.assertTrue((ROOT_DIR / payload["validation_report_path"]).exists())

        after_reports = {path.name for path in REPORT_DIR.glob("issue21_cloud_pipeline_*.json")}
        new_reports = sorted(after_reports - before_reports)
        self.assertTrue(new_reports)
        report = json.loads((REPORT_DIR / new_reports[-1]).read_text())
        self.assertEqual(report["status"], "ok")
        self.assertEqual(report["issue"], 21)
        self.assertEqual(report["dataset_mode"], "existing_dataset")
        self.assertEqual(report["candidate_summary"]["dataset_sentiment_model"], "mock")
        self.assertIn("validate", report["steps"])
        self.assertTrue(report["validation_report_path"].endswith(".json"))


if __name__ == "__main__":
    unittest.main()
