from __future__ import annotations

import json
import subprocess
import sys
import unittest
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
REPORT_DIR = ROOT_DIR / "reports" / "pipeline"


class TestIssue12CloudPipeline(unittest.TestCase):
    def test_smoke_pipeline_produces_train_export_report(self) -> None:
        before_reports = {path.name for path in REPORT_DIR.glob("issue12_cloud_pipeline_*.json")}
        proc = subprocess.run(
            [
                sys.executable,
                "-m",
                "cloud_training.training.run_issue12_cloud_pipeline",
                "--smoke-synthetic-dataset",
                "--allow-smoke-dataset",
                "--smoke-rows",
                "360",
                "--epochs",
                "15",
                "--export-bundle",
                "--output-prefix",
                "issue12_smoke_test_model",
                "--bundle-output-prefix",
                "issue12_smoke_test_bundle",
            ],
            cwd=ROOT_DIR,
            text=True,
            capture_output=True,
            check=True,
        )
        payload = json.loads(proc.stdout)
        self.assertEqual(payload["status"], "ok")
        self.assertEqual(payload["dataset_mode"], "synthetic_smoke_dataset")
        self.assertTrue((ROOT_DIR / payload["dataset_path"]).exists())
        self.assertTrue((ROOT_DIR / payload["artifact_path"]).exists())
        self.assertTrue((ROOT_DIR / payload["metrics_path"]).exists())
        self.assertTrue((ROOT_DIR / payload["diagnostics_path"]).exists())
        self.assertTrue((ROOT_DIR / payload["bundle_path"]).exists())

        after_reports = {path.name for path in REPORT_DIR.glob("issue12_cloud_pipeline_*.json")}
        new_reports = sorted(after_reports - before_reports)
        self.assertTrue(new_reports)
        report = json.loads((REPORT_DIR / new_reports[-1]).read_text())
        self.assertEqual(report["status"], "ok")
        self.assertEqual(report["issue"], 12)
        self.assertEqual(report["dataset_mode"], "synthetic_smoke_dataset")
        self.assertEqual(report["downstream_issue14_handoff"]["refresh_unit"], "single bundle json")
        self.assertTrue(report["bundle_path"].endswith(".bundle.json"))


if __name__ == "__main__":
    unittest.main()
