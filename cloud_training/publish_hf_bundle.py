from __future__ import annotations

import argparse
import json
import tempfile
from pathlib import Path

from huggingface_hub import HfApi

from cloud_training.model_repo import stage_model_repo_contract

ROOT_DIR = Path(__file__).resolve().parents[1]
DEFAULT_BUNDLE_DIR = ROOT_DIR / "artifacts" / "bundles"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Publish a servable predictive bundle to an HF model/artifact repo")
    parser.add_argument("--repo-id", required=True, help="target model repo id")
    parser.add_argument("--bundle", help="bundle path; defaults to latest bundle under artifacts/bundles")
    parser.add_argument("--token", help="HF token; otherwise use environment / local auth")
    parser.add_argument("--approve-for-oracle", action="store_true", help="also write approved + endpoint-ready manifests for downstream inference deployment consumption")
    return parser.parse_args()


def resolve_bundle(explicit: str | None) -> Path:
    if explicit:
        path = Path(explicit)
        if not path.is_absolute():
            path = ROOT_DIR / path
        if not path.exists():
            raise SystemExit(f"bundle_not_found:{path}")
        return path
    candidates = sorted(DEFAULT_BUNDLE_DIR.glob("*.bundle.json"))
    if not candidates:
        raise SystemExit(f"no_bundle_found_under:{DEFAULT_BUNDLE_DIR}")
    return candidates[-1]


def main() -> None:
    args = parse_args()
    bundle_path = resolve_bundle(args.bundle)
    api = HfApi(token=args.token)

    with tempfile.TemporaryDirectory() as tmpdir:
        staged_paths = stage_model_repo_contract(
            bundle_path,
            repo_id=args.repo_id,
            output_dir=Path(tmpdir),
            approved=bool(args.approve_for_oracle),
        )
        uploads = [
            staged_paths["bundle_repo_path"],
            staged_paths["bundle_manifest_repo_path"],
            staged_paths["root_manifest_repo_path"],
        ]
        if staged_paths["approved_manifest_repo_path"]:
            uploads.append(staged_paths["approved_manifest_repo_path"])
        if staged_paths["endpoint_ready_repo_path"]:
            uploads.append(staged_paths["endpoint_ready_repo_path"])

        for repo_path in uploads:
            api.upload_file(
                path_or_fileobj=str(Path(tmpdir) / repo_path),
                path_in_repo=repo_path,
                repo_id=args.repo_id,
                repo_type="model",
            )

    print(json.dumps({
        "status": "ok",
        "repo_id": args.repo_id,
        "bundle": staged_paths["bundle_repo_path"],
        "bundle_manifest": staged_paths["bundle_manifest_repo_path"],
        "root_manifest": staged_paths["root_manifest_repo_path"],
        "approved_manifest": staged_paths["approved_manifest_repo_path"],
        "endpoint_ready_manifest": staged_paths["endpoint_ready_repo_path"],
    }, indent=2))


if __name__ == "__main__":
    main()
