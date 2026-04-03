from __future__ import annotations

import argparse
from pathlib import Path

from huggingface_hub import HfApi

ROOT_DIR = Path(__file__).resolve().parents[1]
DEFAULT_SPACE_DIR = ROOT_DIR / "artifacts" / "deployments" / "huggingface_space_lab"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Upload the built HF Space training package to a Space repo")
    parser.add_argument("--repo-id", required=True, help="target space repo id, e.g. FunkMonk87/AI-Stock-Trader-Lab")
    parser.add_argument("--source-dir", default=str(DEFAULT_SPACE_DIR), help="built package directory")
    parser.add_argument("--token", help="HF token; otherwise use environment / local auth")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    source_dir = Path(args.source_dir)
    if not source_dir.is_absolute():
        source_dir = ROOT_DIR / source_dir
    if not source_dir.exists():
        raise SystemExit(f"space_package_not_found:{source_dir}")

    api = HfApi(token=args.token)
    api.create_repo(
        repo_id=args.repo_id,
        repo_type="space",
        space_sdk="gradio",
        exist_ok=True,
    )
    api.upload_folder(
        repo_id=args.repo_id,
        repo_type="space",
        folder_path=str(source_dir),
    )
    print(f"uploaded_space_package:{args.repo_id}:{source_dir}")


if __name__ == "__main__":
    main()
