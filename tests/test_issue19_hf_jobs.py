from __future__ import annotations

import json
import subprocess
import sys
import unittest
from pathlib import Path

from cloud_training.hf_jobs import ROOT_DIR, build_job_request, read_job_status
from cloud_training.hf_space_app import status_payload

JOBS_DIR = ROOT_DIR / "reports" / "pipeline" / "jobs"


class TestIssue19HFJobs(unittest.TestCase):
    def test_runner_persists_successful_job_status_and_result(self) -> None:
        job = build_job_request(
            job_type="test_success",
            request_label="test success",
            command=[sys.executable, "-c", "import json; print(json.dumps({'status':'ok','report':'reports/pipeline/fake.json'}))"],
        )
        subprocess.run(
            [sys.executable, "-m", "cloud_training.training.hf_job_runner", "--job-id", job["job_id"]],
            cwd=ROOT_DIR,
            text=True,
            capture_output=True,
            check=True,
        )
        updated = read_job_status(job["job_id"])
        self.assertEqual(updated["contract_version"], "hf_space_job/v1")
        self.assertEqual(updated["status"], "succeeded")
        self.assertIsNotNone(updated["started_at"])
        self.assertIsNotNone(updated["finished_at"])
        self.assertEqual(updated["returncode"], 0)
        self.assertEqual(updated["result"]["status"], "ok")
        log_path = ROOT_DIR / updated["log_path"]
        self.assertTrue(log_path.exists())
        self.assertIn(job["job_id"], log_path.read_text())

    def test_runner_persists_failed_job_status(self) -> None:
        job = build_job_request(
            job_type="test_failure",
            request_label="test failure",
            command=[sys.executable, "-c", "import sys; sys.stderr.write('boom\\n'); raise SystemExit(3)"],
        )
        proc = subprocess.run(
            [sys.executable, "-m", "cloud_training.training.hf_job_runner", "--job-id", job["job_id"]],
            cwd=ROOT_DIR,
            text=True,
            capture_output=True,
        )
        self.assertEqual(proc.returncode, 3)
        updated = read_job_status(job["job_id"])
        self.assertEqual(updated["status"], "failed")
        self.assertEqual(updated["returncode"], 3)
        self.assertIn("boom", updated["error"]["message"])

    def test_space_status_exposes_latest_jobs(self) -> None:
        payload = json.loads(status_payload())
        self.assertIn("latest_jobs", payload)
        self.assertIsInstance(payload["latest_jobs"], list)
        index_path = JOBS_DIR / "index.json"
        self.assertTrue(index_path.exists())


if __name__ == "__main__":
    unittest.main()
