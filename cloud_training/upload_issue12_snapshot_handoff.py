from __future__ import annotations

import argparse
import json
from pathlib import Path

from huggingface_hub import HfApi

ROOT_DIR = Path(__file__).resolve().parents[1]
DEFAULT_HANDOFF_DIR = ROOT_DIR / "artifacts" / "deployments" / "issue12_snapshot_handoff"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Upload the staged Issue #12 market snapshot handoff into a Hugging Face Space repo"
    )
    parser.add_argument("--repo-id", required=True, help="target space repo id, e.g. FunkMonk87/AI-Stock-Trader-Lab")
    parser.add_argument(
        "--handoff-dir",
        default=str(DEFAULT_HANDOFF_DIR),
        help="directory produced by prepare_issue12_snapshot_handoff.py",
    )
    parser.add_argument("--token", help="HF token; otherwise use environment / local auth")
    parser.add_argument(
        "--repo-subdir",
        default=".",
        help="optional destination prefix inside the Space repo; default uploads to repo root",
    )
    return parser.parse_args()


def _normalize_handoff_dir(handoff_dir: str | Path) -> Path:
    path = Path(handoff_dir)
    if not path.is_absolute():
        path = ROOT_DIR / path
    if not path.exists():
        raise SystemExit(f"handoff_dir_not_found:{path}")
    return path


def load_manifest(handoff_dir: Path) -> dict:
    manifest_path = handoff_dir / "manifest.json"
    if not manifest_path.exists():
        raise SystemExit(f"handoff_manifest_not_found:{manifest_path}")
    return json.loads(manifest_path.read_text())


def _repo_target(repo_subdir: str, relative_path: str) -> str:
    prefix = repo_subdir.strip().strip("/")
    if not prefix or prefix == ".":
        return relative_path
    return f"{prefix}/{relative_path}"


def upload_handoff(*, repo_id: str, handoff_dir: str | Path, token: str | None = None, repo_subdir: str = ".") -> list[str]:
    handoff_path = _normalize_handoff_dir(handoff_dir)
    manifest = load_manifest(handoff_path)
    staged_files = manifest.get("staged_files") or {}
    if not isinstance(staged_files, dict) or not staged_files:
        raise SystemExit("handoff_manifest_missing_staged_files")

    api = HfApi(token=token)
    uploaded_paths: list[str] = []
    for _, relative_path in staged_files.items():
        source_path = handoff_path / relative_path
        if not source_path.exists():
            raise SystemExit(f"staged_file_missing:{source_path}")
        target_path = _repo_target(repo_subdir, relative_path)
        api.upload_file(
            path_or_fileobj=str(source_path),
            path_in_repo=target_path,
            repo_id=repo_id,
            repo_type="space",
        )
        uploaded_paths.append(target_path)

    manifest_repo_path = _repo_target(repo_subdir, "issue12_snapshot_handoff.manifest.json")
    api.upload_file(
        path_or_fileobj=str(handoff_path / "manifest.json"),
        path_in_repo=manifest_repo_path,
        repo_id=repo_id,
        repo_type="space",
    )
    uploaded_paths.append(manifest_repo_path)
    return uploaded_paths


def main() -> None:
    args = parse_args()
    uploaded_paths = upload_handoff(
        repo_id=args.repo_id,
        handoff_dir=args.handoff_dir,
        token=args.token,
        repo_subdir=args.repo_subdir,
    )
    print(json.dumps({
        "status": "ok",
        "repo_id": args.repo_id,
        "uploaded_paths": uploaded_paths,
    }, indent=2))


if __name__ == "__main__":
    main()
