from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from cloud_training.model_architecture.hybrid_model import HybridSignalEnsemble

ROOT_DIR = Path(__file__).resolve().parents[1]
DEFAULT_BUNDLE_DIR = ROOT_DIR / "artifacts" / "bundles"
PACKAGE_BUNDLE_PIN_FILE = "bundle_pin.json"
BUNDLE_MANIFEST_FILE = "manifest.json"


class PredictiveArtifactBundle:
    def __init__(self, path: Path, payload: dict[str, Any]):
        self.path = path
        self.payload = payload
        self.bundle_version = payload["bundle_version"]
        self.artifact_name = payload["artifact_name"]
        self.model = HybridSignalEnsemble.from_artifact(payload["model_artifact"])
        self.feature_names = list(payload["feature_names"])
        self.inference_contract = {
            "preferred_serving_mode": "precomputed_features_from_cloud_feature_pipeline",
            "fallback_serving_mode": "derive_available_features_and_zero_fill_missing_fields",
            **payload["inference_contract"],
        }
        self.training_metadata = payload.get("training_metadata", {})

    def predict_from_sample(self, sample: dict[str, Any]) -> dict[str, Any]:
        features = [float(sample.get(name, 0.0) or 0.0) for name in self.feature_names]
        prediction = self.model.predict(features)
        return {
            "artifact_name": self.payload["artifact_name"],
            "model_type": self.payload["model_type"],
            "ticker": sample.get("ticker"),
            "as_of_date": sample.get("as_of_date"),
            "signal": prediction["signal_probability"],
            "confidence": prediction["confidence"],
            "variance": prediction["predictive_variance"],
            "embeddings": features,
        }


def _candidate_bundle_dirs(path: Path) -> list[Path]:
    candidates: list[Path] = []
    if path.is_dir():
        candidates.append(path)
        nested = path / "artifacts" / "bundles"
        if nested.is_dir():
            candidates.append(nested)
    return candidates


def _resolve_bundle_from_pin_file(path: Path) -> Path | None:
    if not path.is_file():
        return None
    payload = json.loads(path.read_text())
    bundle_rel = payload.get("bundle_path")
    if not isinstance(bundle_rel, str) or not bundle_rel:
        raise FileNotFoundError(f"bundle_pin_missing_bundle_path:{path}")
    bundle_path = (path.parent / bundle_rel).resolve()
    if not bundle_path.exists():
        raise FileNotFoundError(f"bundle_pin_target_missing:{bundle_path}")
    return bundle_path


def _resolve_bundle_from_manifest(path: Path) -> Path | None:
    if not path.is_file():
        return None
    payload = json.loads(path.read_text())
    bundle_name = payload.get("default_bundle")
    if not isinstance(bundle_name, str) or not bundle_name:
        raise FileNotFoundError(f"bundle_manifest_missing_default_bundle:{path}")
    bundle_path = path.parent / bundle_name
    if not bundle_path.exists():
        raise FileNotFoundError(f"bundle_manifest_target_missing:{bundle_path}")
    return bundle_path


def _resolve_bundle_from_dir(path: Path) -> Path:
    for candidate_dir in _candidate_bundle_dirs(path):
        pin_path = candidate_dir.parent / PACKAGE_BUNDLE_PIN_FILE
        pinned = _resolve_bundle_from_pin_file(pin_path)
        if pinned is not None:
            return pinned
        manifest_path = candidate_dir / BUNDLE_MANIFEST_FILE
        manifested = _resolve_bundle_from_manifest(manifest_path)
        if manifested is not None:
            return manifested

    direct_candidates = sorted(path.glob("*.bundle.json")) if path.is_dir() else []
    nested_candidates = sorted((path / "artifacts" / "bundles").glob("*.bundle.json")) if path.is_dir() else []
    candidates = direct_candidates + nested_candidates
    if not candidates:
        raise FileNotFoundError(f"no_bundle_found_under:{path}")
    return candidates[-1]


def resolve_bundle_path(explicit_path: str | None = None) -> Path:
    if explicit_path:
        path = Path(explicit_path)
        if not path.is_absolute():
            path = ROOT_DIR / path
        if not path.exists():
            raise FileNotFoundError(f"bundle_not_found:{path}")
        if path.is_dir():
            return _resolve_bundle_from_dir(path)
        return path

    return _resolve_bundle_from_dir(DEFAULT_BUNDLE_DIR)


def load_bundle(explicit_path: str | None = None) -> PredictiveArtifactBundle:
    path = resolve_bundle_path(explicit_path)
    payload = json.loads(path.read_text())
    return PredictiveArtifactBundle(path=path, payload=payload)
