from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from cloud_inference.artifact_loader import resolve_bundle_path
from cloud_inference.build_hf_deployment import build_hf_deployment
from cloud_training.model_repo import stage_model_repo_contract
from cloud_training.publish_hf_bundle import main as publish_main

ROOT_DIR = Path(__file__).resolve().parents[1]
BUNDLE_PATH = sorted((ROOT_DIR / "artifacts" / "bundles").glob("*.bundle.json"))[-1]


class FakeApi:
    def __init__(self, token: str | None = None):
        self.token = token
        self.calls: list[dict[str, str]] = []

    def upload_file(self, *, path_or_fileobj: str, path_in_repo: str, repo_id: str, repo_type: str) -> None:
        self.calls.append({
            "path_or_fileobj": path_or_fileobj,
            "path_in_repo": path_in_repo,
            "repo_id": repo_id,
            "repo_type": repo_type,
        })


class TestIssue19ModelRepoContract(unittest.TestCase):
    def test_stage_model_repo_contract_writes_canonical_manifests(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            staged = stage_model_repo_contract(
                BUNDLE_PATH,
                repo_id="FunkMonk87/ai-stock-trader-oracle",
                output_dir=Path(tmpdir),
                approved=True,
            )
            stage_dir = Path(tmpdir)
            self.assertTrue((stage_dir / staged["bundle_repo_path"]).exists())
            self.assertTrue((stage_dir / staged["bundle_manifest_repo_path"]).exists())
            self.assertTrue((stage_dir / staged["root_manifest_repo_path"]).exists())
            self.assertTrue((stage_dir / staged["approved_manifest_repo_path"]).exists())
            self.assertTrue((stage_dir / staged["endpoint_ready_repo_path"]).exists())

            root_manifest = json.loads((stage_dir / staged["root_manifest_repo_path"]).read_text())
            approved_manifest = json.loads((stage_dir / staged["approved_manifest_repo_path"]).read_text())
            endpoint_ready = json.loads((stage_dir / staged["endpoint_ready_repo_path"]).read_text())

            self.assertEqual(root_manifest["manifest_version"], "model_repo_manifest_v1")
            self.assertEqual(approved_manifest["channel"], "approved")
            self.assertEqual(endpoint_ready["endpoint"], "cloud_oracle")
            self.assertEqual(endpoint_ready["approved_bundle"]["repo_path"], staged["bundle_repo_path"])

    def test_publish_bundle_uploads_bundle_and_manifests(self) -> None:
        fake_api = FakeApi()
        with patch("cloud_training.publish_hf_bundle.HfApi", return_value=fake_api), \
             patch("sys.argv", [
                 "publish_hf_bundle.py",
                 "--repo-id", "FunkMonk87/ai-stock-trader-oracle",
                 "--bundle", str(BUNDLE_PATH),
                 "--approve-for-oracle",
             ]):
            publish_main()

        uploaded = {call["path_in_repo"] for call in fake_api.calls}
        self.assertIn(f"bundles/{BUNDLE_PATH.name}", uploaded)
        self.assertIn("manifest.json", uploaded)
        self.assertIn("channels/approved/manifest.json", uploaded)
        self.assertIn("endpoints/oracle/ready.json", uploaded)
        self.assertIn("manifests/bundles/" + BUNDLE_PATH.name.replace(".bundle.json", ".manifest.json"), uploaded)

    def test_build_hf_deployment_can_consume_model_repo_endpoint_ready_contract(self) -> None:
        with tempfile.TemporaryDirectory() as repo_tmpdir, tempfile.TemporaryDirectory() as deploy_tmpdir:
            repo_dir = Path(repo_tmpdir)
            stage_model_repo_contract(
                BUNDLE_PATH,
                repo_id="FunkMonk87/ai-stock-trader-oracle",
                output_dir=repo_dir,
                approved=True,
            )
            output_dir = build_hf_deployment(Path(deploy_tmpdir) / "hf_oracle", model_repo_dir=repo_dir)
            pin = json.loads((output_dir / "bundle_pin.json").read_text())
            manifest = json.loads((output_dir / "artifacts" / "bundles" / "manifest.json").read_text())
            resolved = resolve_bundle_path(str(output_dir))
            self.assertEqual(pin["bundle_name"], BUNDLE_PATH.name)
            self.assertEqual(manifest["default_bundle"], BUNDLE_PATH.name)
            self.assertEqual(resolved.name, BUNDLE_PATH.name)


if __name__ == "__main__":
    unittest.main()
