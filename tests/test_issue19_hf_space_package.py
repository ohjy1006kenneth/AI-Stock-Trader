from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from cloud_training.build_hf_space import build_hf_space


class TestIssue19HFSpacePackage(unittest.TestCase):
    def test_build_hf_space_includes_model_repo_contract_module(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            output_dir = build_hf_space(Path(tmpdir) / "hf_space")
            self.assertTrue((output_dir / "cloud_training" / "model_repo.py").exists())
            self.assertTrue((output_dir / "cloud_training" / "publish_hf_bundle.py").exists())
            self.assertTrue((output_dir / "cloud_training" / "training" / "run_issue21_cloud_pipeline.py").exists())


if __name__ == "__main__":
    unittest.main()
