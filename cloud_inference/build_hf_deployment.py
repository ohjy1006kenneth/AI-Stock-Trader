from __future__ import annotations

import argparse
import json
import shutil
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT_DIR = ROOT_DIR / "artifacts" / "deployments" / "huggingface_oracle"
BUNDLE_DIR = ROOT_DIR / "artifacts" / "bundles"

DEPLOYMENT_FILES: dict[str, str] = {
    "cloud_inference/__init__.py": "cloud_inference/__init__.py",
    "cloud_inference/artifact_loader.py": "cloud_inference/artifact_loader.py",
    "cloud_inference/contracts.py": "cloud_inference/contracts.py",
    "cloud_inference/feature_adapter.py": "cloud_inference/feature_adapter.py",
    "cloud_inference/handler.py": "cloud_inference/handler.py",
    "cloud_training/__init__.py": "cloud_training/__init__.py",
    "cloud_training/data_pipelines/predictive_feature_core.py": "cloud_training/data_pipelines/predictive_feature_core.py",
    "cloud_training/model_architecture/__init__.py": "cloud_training/model_architecture/__init__.py",
    "cloud_training/model_architecture/hybrid_model.py": "cloud_training/model_architecture/hybrid_model.py",
    "config/cloud_oracle_request.schema.json": "config/cloud_oracle_request.schema.json",
    "config/cloud_oracle_response.schema.json": "config/cloud_oracle_response.schema.json",
}

ROOT_HANDLER = '''from cloud_inference.handler import EndpointHandler\n'''

ROOT_REQUIREMENTS = '''# Custom HF Oracle runtime for the calibrated XGBoost predictive bundle.\nnumpy==2.4.4\nxgboost==3.2.0\n'''

README = '''# Hugging Face Cloud Oracle deployment package\n\nThis directory is the minimal custom-runtime package for the trading Cloud Oracle.\n\n## Runtime shape\n- `handler.py` at package root exposes `EndpointHandler` for Hugging Face Inference Endpoints\n- `cloud_inference/` contains the custom request unwrap + response build logic\n- `cloud_training/model_architecture/hybrid_model.py` contains the calibrated XGBoost artifact-backed prediction code used at inference time\n- `artifacts/bundles/*.bundle.json` contains the bundled servable model artifact\n- `artifacts/bundles/manifest.json` records the packaged default bundle and shipped bundle set\n- `bundle_pin.json` pins the packaged default bundle so endpoint refreshes do not silently drift to a different file by lexicographic sort order\n- `config/cloud_oracle_*.schema.json` ships the locked request/response schemas\n\n## Deploy\n1. Build this package from the repo root:\n   - `.venv/bin/python cloud_inference/build_hf_deployment.py`\n   - or from an approved model-repo snapshot: `.venv/bin/python cloud_inference/build_hf_deployment.py --model-repo-dir <snapshot-dir>`\n2. Point Hugging Face Inference Endpoints at this directory contents (or upload/sync them into the deployment repo).\n3. Ensure runtime dependencies install from `requirements.txt` (`numpy`, `xgboost`).\n4. Use the default handler entrypoint discovered from root `handler.py`.\n5. Optional: set `PREDICTIVE_BUNDLE_PATH=artifacts/bundles/<bundle-name>.bundle.json` to override the packaged pin with a specific bundle.\n\n## Bundle refresh / pin behavior\n- The default build copies the currently available local bundles into `artifacts/bundles/`.\n- If `--model-repo-dir` is provided, the build instead reads `endpoints/oracle/ready.json` and packages the approved bundle referenced there.\n- The build writes an explicit packaged pin (`bundle_pin.json`) to the approved/default bundle chosen at build time.\n- At runtime, directory-based bundle resolution now prefers that packaged pin, then the bundle manifest default, and only falls back to \"latest by filename\" if no pin/manifest is present.\n- This makes endpoint packaging reproducible across redeploys and safer for staged bundle refreshes.\n\n## Supported request transport\nThe deployed endpoint accepts either:\n- `{\"inputs\": {\"portfolio\": ..., \"universe\": ...}, \"request_id\": \"...\"}`\n- or the bare `{\"portfolio\": ..., \"universe\": ...}` payload\n\nThe Pi edge currently sends the wrapped `inputs` form.\n'''


def _build_bundle_manifest(bundle_paths: list[Path], default_bundle: Path) -> dict[str, object]:
    return {
        "manifest_version": "hf_oracle_bundle_manifest_v1",
        "default_bundle": default_bundle.name,
        "bundles": [bundle_path.name for bundle_path in bundle_paths],
    }


def _build_bundle_pin(default_bundle: Path) -> dict[str, object]:
    return {
        "pin_version": "hf_oracle_bundle_pin_v1",
        "bundle_path": f"artifacts/bundles/{default_bundle.name}",
        "bundle_name": default_bundle.name,
    }


def _resolve_model_repo_ready_bundle(model_repo_dir: Path) -> tuple[list[Path], Path]:
    ready_path = model_repo_dir / "endpoints" / "oracle" / "ready.json"
    if not ready_path.exists():
        raise FileNotFoundError(f"model_repo_endpoint_ready_manifest_not_found:{ready_path}")
    payload = json.loads(ready_path.read_text())
    repo_bundle_path = payload.get("approved_bundle", {}).get("repo_path")
    if not isinstance(repo_bundle_path, str) or not repo_bundle_path:
        raise FileNotFoundError(f"model_repo_ready_manifest_missing_repo_path:{ready_path}")
    bundle_path = model_repo_dir / repo_bundle_path
    if not bundle_path.exists():
        raise FileNotFoundError(f"model_repo_approved_bundle_not_found:{bundle_path}")
    return [bundle_path], bundle_path


def build_hf_deployment(output_dir: Path = DEFAULT_OUTPUT_DIR, *, model_repo_dir: Path | None = None) -> Path:
    if output_dir.exists():
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    for src_rel, dst_rel in DEPLOYMENT_FILES.items():
        src = ROOT_DIR / src_rel
        dst = output_dir / dst_rel
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dst)

    if model_repo_dir is not None:
        bundle_paths, default_bundle = _resolve_model_repo_ready_bundle(model_repo_dir)
    else:
        bundle_paths = sorted(BUNDLE_DIR.glob("*.bundle.json"))
        if not bundle_paths:
            raise FileNotFoundError(f"no_bundle_found_under:{BUNDLE_DIR}")
        default_bundle = bundle_paths[-1]
    bundle_output_dir = output_dir / "artifacts" / "bundles"
    bundle_output_dir.mkdir(parents=True, exist_ok=True)
    for bundle_path in bundle_paths:
        shutil.copy2(bundle_path, bundle_output_dir / bundle_path.name)

    (bundle_output_dir / "manifest.json").write_text(json.dumps(_build_bundle_manifest(bundle_paths, default_bundle), indent=2) + "\n")
    (output_dir / "bundle_pin.json").write_text(json.dumps(_build_bundle_pin(default_bundle), indent=2) + "\n")
    (output_dir / "handler.py").write_text(ROOT_HANDLER)
    (output_dir / "requirements.txt").write_text(ROOT_REQUIREMENTS)
    (output_dir / "README.md").write_text(README)
    return output_dir


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build the HF Oracle deployment package")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR), help="destination directory for the packaged deployment")
    parser.add_argument("--model-repo-dir", help="optional local snapshot of the canonical model repo; if provided, package its approved endpoint-ready bundle")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    built = build_hf_deployment(
        Path(args.output_dir),
        model_repo_dir=Path(args.model_repo_dir) if args.model_repo_dir else None,
    )
    print(built)
