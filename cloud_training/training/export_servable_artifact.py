from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
import re
import sys

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

MODEL_OUTPUT_DIR = ROOT_DIR / "data" / "processed" / "models"
BUNDLE_OUTPUT_DIR = ROOT_DIR / "artifacts" / "bundles"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export a first servable predictive artifact bundle")
    parser.add_argument("--source-artifact", help="path to validated model artifact json; defaults to the latest artifact under data/processed/models")
    parser.add_argument("--source-metrics", help="path to matching metrics json; defaults to sibling metrics file")
    parser.add_argument("--output-prefix", default="predictive_signal_bundle_v1")
    parser.add_argument("--source-issue", type=int, help="optional GitHub issue number to stamp into bundle metadata; otherwise inferred from artifact name when possible")
    return parser.parse_args()


def resolve_source_artifact(explicit_path: str | None) -> Path:
    if explicit_path:
        path = Path(explicit_path)
        if not path.is_absolute():
            path = ROOT_DIR / path
        if not path.exists():
            raise SystemExit(f"source_artifact_not_found:{path}")
        return path

    candidates = sorted(MODEL_OUTPUT_DIR.glob("*.artifact.json"), key=lambda path: path.stat().st_mtime)
    if not candidates:
        raise SystemExit("no_artifact_found_under_data_processed_models")
    return candidates[-1]


def resolve_source_metrics(artifact_path: Path, explicit_metrics: str | None) -> Path:
    if explicit_metrics:
        path = Path(explicit_metrics)
        if not path.is_absolute():
            path = ROOT_DIR / path
        if not path.exists():
            raise SystemExit(f"source_metrics_not_found:{path}")
        return path
    metrics_path = artifact_path.with_name(artifact_path.name.replace(".artifact.json", ".metrics.json"))
    if not metrics_path.exists():
        raise SystemExit(f"matching_metrics_not_found:{metrics_path}")
    return metrics_path


def infer_source_issue(artifact_path: Path, explicit_issue: int | None) -> int | None:
    if explicit_issue is not None:
        return explicit_issue
    match = re.search(r"issue(\d+)", artifact_path.name)
    if match:
        return int(match.group(1))
    return None


def main() -> None:
    args = parse_args()
    artifact_path = resolve_source_artifact(args.source_artifact)
    metrics_path = resolve_source_metrics(artifact_path, args.source_metrics)

    artifact_payload = json.loads(artifact_path.read_text())
    metrics_payload = json.loads(metrics_path.read_text())
    source_issue = infer_source_issue(artifact_path, args.source_issue)

    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    bundle_name = f"{args.output_prefix}_{stamp}"
    output_path = BUNDLE_OUTPUT_DIR / f"{bundle_name}.bundle.json"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    bundle_payload = {
        "bundle_version": "predictive_artifact_bundle_v1",
        "artifact_name": bundle_name,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source_issue": source_issue,
        "source_training_artifact": str(artifact_path.relative_to(ROOT_DIR)),
        "source_metrics_artifact": str(metrics_path.relative_to(ROOT_DIR)),
        "model_type": artifact_payload["artifact"]["model_type"],
        "target": artifact_payload["target"],
        "feature_names": artifact_payload["artifact"]["feature_names"],
        "output_schema": {
            "signal": "probability_of_positive_next_day_return_in_[0,1]",
            "confidence": "distance_from_decision_boundary_in_[0,1]",
            "variance": "bernoulli_predictive_variance",
            "embeddings": "ordered_numeric_feature_vector_used_for_prediction"
        },
        "inference_contract": {
            "transport": "huggingface_custom_handler_v1",
            "request_top_level_fields": ["portfolio", "universe"],
            "universe_required_fields": ["ticker", "history", "news"],
            "universe_optional_fields": ["market_history", "context", "precomputed_features"],
            "preferred_serving_mode": "precomputed_features_from_cloud_feature_pipeline",
            "fallback_serving_mode": "derive_market_and_macro_features_at_inference_and_zero_fill_missing_text_context_features",
            "precomputed_feature_names": artifact_payload["artifact"]["feature_names"],
            "history_row_fields": ["date", "open", "high", "low", "close", "volume"],
            "minimum_history_length": 21,
            "recommended_history_length": 63,
            "response_fields": ["artifact_name", "model_type", "ticker", "as_of_date", "signal", "confidence", "variance", "embeddings"]
        },
        "servable_artifact_contract": {
            "bundle_path": str(output_path.relative_to(ROOT_DIR)),
            "source_training_artifact": str(artifact_path.relative_to(ROOT_DIR)),
            "source_metrics_artifact": str(metrics_path.relative_to(ROOT_DIR)),
            "model_loader": "cloud_inference.artifact_loader.load_bundle",
            "oracle_entrypoint": "cloud_inference.handler.EndpointHandler",
            "oracle_refresh_expectations": {
                "publish_unit": "single .bundle.json file containing model_artifact + contract metadata",
                "required_files_for_oracle_package": [
                    "handler.py",
                    "requirements.txt",
                    "cloud_inference/artifact_loader.py",
                    "cloud_inference/contracts.py",
                    "cloud_inference/feature_adapter.py",
                    "cloud_inference/handler.py",
                    "cloud_training/model_architecture/hybrid_model.py",
                    "config/cloud_oracle_request.schema.json",
                    "config/cloud_oracle_response.schema.json",
                    str(output_path.relative_to(ROOT_DIR))
                ]
            }
        },
        "training_metadata": {
            "dataset_path": artifact_payload["dataset_path"],
            "train_accuracy": metrics_payload.get("train_accuracy"),
            "train_loss": metrics_payload.get("train_loss"),
            "samples": metrics_payload.get("samples"),
            "ensemble_size": metrics_payload.get("ensemble_size")
        },
        "model_artifact": artifact_payload["artifact"]
    }

    output_path.write_text(json.dumps(bundle_payload, indent=2))
    print(json.dumps({
        "status": "ok",
        "bundle": str(output_path.relative_to(ROOT_DIR)),
        "source_artifact": str(artifact_path.relative_to(ROOT_DIR)),
        "source_metrics": str(metrics_path.relative_to(ROOT_DIR))
    }, indent=2))


if __name__ == "__main__":
    main()
