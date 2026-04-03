from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from cloud_training.upload_issue12_snapshot_handoff import upload_handoff


class _FakeApi:
    def __init__(self, token: str | None = None) -> None:
        self.token = token
        self.calls: list[dict[str, str]] = []

    def upload_file(self, *, path_or_fileobj: str, path_in_repo: str, repo_id: str, repo_type: str) -> None:
        self.calls.append({
            "path_or_fileobj": path_or_fileobj,
            "path_in_repo": path_in_repo,
            "repo_id": repo_id,
            "repo_type": repo_type,
        })


class TestIssue12SnapshotHandoffUpload(unittest.TestCase):
    def test_upload_handoff_pushes_staged_snapshots_and_manifest_into_space_repo(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            handoff_dir = Path(tmpdir) / "issue12_snapshot_handoff"
            market_dir = handoff_dir / "data" / "runtime" / "market"
            market_dir.mkdir(parents=True, exist_ok=True)
            (market_dir / "price_snapshot.json").write_text(json.dumps({"items": [{"ticker": "SPY"}]}))
            (market_dir / "fundamental_snapshot.json").write_text(json.dumps({"items": [{"ticker": "SPY"}]}))
            (handoff_dir / "manifest.json").write_text(json.dumps({
                "staged_files": {
                    "price_snapshot": "data/runtime/market/price_snapshot.json",
                    "fundamental_snapshot": "data/runtime/market/fundamental_snapshot.json",
                }
            }))

            fake_api = _FakeApi(token="stub-token")
            with patch("cloud_training.upload_issue12_snapshot_handoff.HfApi", return_value=fake_api):
                uploaded = upload_handoff(
                    repo_id="FunkMonk87/AI-Stock-Trader-Lab",
                    handoff_dir=handoff_dir,
                    token="stub-token",
                )

        self.assertEqual(
            uploaded,
            [
                "data/runtime/market/price_snapshot.json",
                "data/runtime/market/fundamental_snapshot.json",
                "issue12_snapshot_handoff.manifest.json",
            ],
        )
        self.assertEqual(len(fake_api.calls), 3)
        self.assertTrue(all(call["repo_type"] == "space" for call in fake_api.calls))
        self.assertEqual(fake_api.calls[0]["path_in_repo"], "data/runtime/market/price_snapshot.json")
        self.assertEqual(fake_api.calls[1]["path_in_repo"], "data/runtime/market/fundamental_snapshot.json")
        self.assertEqual(fake_api.calls[2]["path_in_repo"], "issue12_snapshot_handoff.manifest.json")


if __name__ == "__main__":
    unittest.main()
