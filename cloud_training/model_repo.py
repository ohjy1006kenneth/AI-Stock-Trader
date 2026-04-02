from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT_DIR = Path(__file__).resolve().parents[1]
MODEL_REPO_BUNDLE_PREFIX = "bundles"
MODEL_REPO_BUNDLE_MANIFEST_PREFIX = "manifests/bundles"
MODEL_REPO_APPROVED_MANIFEST = "channels/approved/manifest.json"
MODEL_REPO_ENDPOINT_READY_MANIFEST = "endpoints/oracle/ready.json"
MODEL_REPO_ROOT_MANIFEST = "manifest.json"


@dataclass(frozen=True)
class ModelRepoLayout:
    bundle_repo_path: str
    bundle_manifest_repo_path: str
    approved_manifest_repo_path: str
    endpoint_ready_repo_path: str
    root_manifest_repo_path: str


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def load_bundle_payload(bundle_path: Path) -> dict[str, Any]:
    return json.loads(bundle_path.read_text())


def infer_layout(bundle_path: Path) -> ModelRepoLayout:
    bundle_name = bundle_path.name
    base_name = bundle_name[:-len('.bundle.json')] if bundle_name.endswith('.bundle.json') else bundle_name
    return ModelRepoLayout(
        bundle_repo_path=f"{MODEL_REPO_BUNDLE_PREFIX}/{bundle_name}",
        bundle_manifest_repo_path=f"{MODEL_REPO_BUNDLE_MANIFEST_PREFIX}/{base_name}.manifest.json",
        approved_manifest_repo_path=MODEL_REPO_APPROVED_MANIFEST,
        endpoint_ready_repo_path=MODEL_REPO_ENDPOINT_READY_MANIFEST,
        root_manifest_repo_path=MODEL_REPO_ROOT_MANIFEST,
    )


def build_bundle_manifest(bundle_path: Path, *, repo_id: str, approved: bool) -> dict[str, Any]:
    payload = load_bundle_payload(bundle_path)
    layout = infer_layout(bundle_path)
    return {
        "manifest_version": "model_repo_bundle_manifest_v1",
        "generated_at": utc_now(),
        "repo_id": repo_id,
        "bundle": {
            "artifact_name": payload["artifact_name"],
            "bundle_version": payload["bundle_version"],
            "repo_path": layout.bundle_repo_path,
            "file_name": bundle_path.name,
            "sha256": sha256_file(bundle_path),
            "size_bytes": bundle_path.stat().st_size,
        },
        "source": {
            "source_issue": payload.get("source_issue"),
            "source_training_artifact": payload.get("source_training_artifact"),
            "source_metrics_artifact": payload.get("source_metrics_artifact"),
        },
        "inference_contract": payload.get("inference_contract", {}),
        "servable_artifact_contract": payload.get("servable_artifact_contract", {}),
        "training_metadata": payload.get("training_metadata", {}),
        "promotion": {
            "approved_for_oracle": approved,
        },
    }


def build_approved_manifest(bundle_path: Path, *, repo_id: str) -> dict[str, Any]:
    payload = load_bundle_payload(bundle_path)
    layout = infer_layout(bundle_path)
    return {
        "manifest_version": "model_repo_approved_channel_v1",
        "generated_at": utc_now(),
        "repo_id": repo_id,
        "channel": "approved",
        "approved_bundle": {
            "artifact_name": payload["artifact_name"],
            "bundle_version": payload["bundle_version"],
            "repo_path": layout.bundle_repo_path,
            "bundle_manifest_path": layout.bundle_manifest_repo_path,
        },
        "promotion_boundary": {
            "source_of_truth": "human_or_pm_review_after_validation",
            "oracle_consumption_allowed": True,
        },
    }


def build_endpoint_ready_manifest(bundle_path: Path, *, repo_id: str) -> dict[str, Any]:
    payload = load_bundle_payload(bundle_path)
    layout = infer_layout(bundle_path)
    return {
        "manifest_version": "oracle_endpoint_ready_v1",
        "generated_at": utc_now(),
        "repo_id": repo_id,
        "endpoint": "cloud_oracle",
        "approved_bundle": {
            "artifact_name": payload["artifact_name"],
            "repo_path": layout.bundle_repo_path,
            "bundle_manifest_path": layout.bundle_manifest_repo_path,
            "approved_manifest_path": layout.approved_manifest_repo_path,
        },
        "deployment_contract": {
            "builder": "cloud_inference.build_hf_deployment.build_hf_deployment",
            "entrypoint": "cloud_inference.handler.EndpointHandler",
            "bundle_loader": "cloud_inference.artifact_loader.load_bundle",
            "expected_runtime_layout": "huggingface_inference_endpoint_custom_handler",
        },
    }


def build_root_manifest(bundle_path: Path, *, repo_id: str, approved: bool) -> dict[str, Any]:
    payload = load_bundle_payload(bundle_path)
    layout = infer_layout(bundle_path)
    return {
        "manifest_version": "model_repo_manifest_v1",
        "generated_at": utc_now(),
        "repo_id": repo_id,
        "layout": {
            "bundles_prefix": MODEL_REPO_BUNDLE_PREFIX,
            "bundle_manifests_prefix": MODEL_REPO_BUNDLE_MANIFEST_PREFIX,
            "approved_manifest": layout.approved_manifest_repo_path,
            "endpoint_ready_manifest": layout.endpoint_ready_repo_path,
        },
        "latest_bundle": {
            "artifact_name": payload["artifact_name"],
            "repo_path": layout.bundle_repo_path,
            "bundle_manifest_path": layout.bundle_manifest_repo_path,
        },
        "approved_bundle": layout.bundle_repo_path if approved else None,
    }


def stage_model_repo_contract(bundle_path: Path, *, repo_id: str, output_dir: Path, approved: bool = False) -> dict[str, Any]:
    layout = infer_layout(bundle_path)
    output_dir.mkdir(parents=True, exist_ok=True)
    staged_bundle = output_dir / layout.bundle_repo_path
    staged_bundle.parent.mkdir(parents=True, exist_ok=True)
    staged_bundle.write_bytes(bundle_path.read_bytes())

    bundle_manifest = build_bundle_manifest(bundle_path, repo_id=repo_id, approved=approved)
    bundle_manifest_path = output_dir / layout.bundle_manifest_repo_path
    bundle_manifest_path.parent.mkdir(parents=True, exist_ok=True)
    bundle_manifest_path.write_text(json.dumps(bundle_manifest, indent=2) + "\n")

    root_manifest = build_root_manifest(bundle_path, repo_id=repo_id, approved=approved)
    root_manifest_path = output_dir / layout.root_manifest_repo_path
    root_manifest_path.write_text(json.dumps(root_manifest, indent=2) + "\n")

    approved_manifest_path = None
    endpoint_ready_path = None
    if approved:
        approved_manifest = build_approved_manifest(bundle_path, repo_id=repo_id)
        approved_manifest_path = output_dir / layout.approved_manifest_repo_path
        approved_manifest_path.parent.mkdir(parents=True, exist_ok=True)
        approved_manifest_path.write_text(json.dumps(approved_manifest, indent=2) + "\n")

        endpoint_ready = build_endpoint_ready_manifest(bundle_path, repo_id=repo_id)
        endpoint_ready_path = output_dir / layout.endpoint_ready_repo_path
        endpoint_ready_path.parent.mkdir(parents=True, exist_ok=True)
        endpoint_ready_path.write_text(json.dumps(endpoint_ready, indent=2) + "\n")

    return {
        "bundle_repo_path": layout.bundle_repo_path,
        "bundle_manifest_repo_path": layout.bundle_manifest_repo_path,
        "root_manifest_repo_path": layout.root_manifest_repo_path,
        "approved_manifest_repo_path": layout.approved_manifest_repo_path if approved_manifest_path else None,
        "endpoint_ready_repo_path": layout.endpoint_ready_repo_path if endpoint_ready_path else None,
    }
