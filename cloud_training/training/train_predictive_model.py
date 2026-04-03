from __future__ import annotations

from pathlib import Path
import sys

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import argparse
import json
from datetime import datetime, timezone
from typing import Any

from cloud_training.model_architecture.hybrid_model import extract_feature_row, train_hybrid_signal_ensemble

MODEL_OUTPUT_DIR = ROOT_DIR / "data" / "processed" / "models"
DATASET_OUTPUT_DIR = ROOT_DIR / "data" / "processed" / "training"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Train first predictive signal model scaffold")
    parser.add_argument("--dataset", help="path to dataset jsonl; defaults to latest non-smoke dataset under data/processed/training")
    parser.add_argument("--output-prefix", default="predictive_model_v1")
    parser.add_argument("--ensemble-size", type=int, default=1)
    parser.add_argument("--epochs", type=int, default=300)
    parser.add_argument("--learning-rate", type=float, default=0.05)
    parser.add_argument("--l2", type=float, default=1e-3)
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--limit", type=int, default=0, help="optional limit for quick smoke training")
    parser.add_argument("--allow-smoke-dataset", action="store_true", help="allow training on smoke/synthetic/mock datasets; intended only for plumbing validation")
    return parser.parse_args()


def _resolve_path(path_str: str) -> Path:
    path = Path(path_str)
    return path if path.is_absolute() else ROOT_DIR / path


def _manifest_path_for_dataset(dataset_path: Path) -> Path:
    return dataset_path.with_suffix('.manifest.json')


def _read_manifest(dataset_path: Path) -> dict[str, Any]:
    manifest_path = _manifest_path_for_dataset(dataset_path)
    if not manifest_path.exists():
        return {}
    try:
        return json.loads(manifest_path.read_text())
    except Exception:
        return {}


def _dataset_looks_smoke(dataset_path: Path, manifest: dict[str, Any]) -> bool:
    name_tokens = f"{dataset_path.name} {manifest.get('dataset_name', '')}".lower()
    if any(token in name_tokens for token in ("smoke", "synthetic", "mock", "test")):
        return True
    sentiment_model = str(manifest.get("article_sentiment_model") or "").lower()
    article_model_name = str(manifest.get("article_sentiment_model_name") or "").lower()
    if sentiment_model in {"mock"} or "keyword_mock" in article_model_name:
        return True
    return False


def resolve_dataset_path(explicit_path: str | None, *, allow_smoke_dataset: bool = False) -> Path:
    if explicit_path:
        path = _resolve_path(explicit_path)
        if not path.exists():
            raise SystemExit(f"dataset_not_found:{path}")
        manifest = _read_manifest(path)
        if _dataset_looks_smoke(path, manifest) and not allow_smoke_dataset:
            raise SystemExit(
                f"refusing_smoke_dataset:{path.relative_to(ROOT_DIR)}\n"
                "pass --allow-smoke-dataset only for plumbing validation"
            )
        return path

    candidates = sorted(DATASET_OUTPUT_DIR.glob("*.jsonl"))
    if not candidates:
        raise SystemExit("no_dataset_jsonl_found_under_data_processed_training")
    non_smoke = [path for path in candidates if not _dataset_looks_smoke(path, _read_manifest(path))]
    selected = non_smoke[-1] if non_smoke else candidates[-1]
    if _dataset_looks_smoke(selected, _read_manifest(selected)) and not allow_smoke_dataset:
        raise SystemExit(
            "latest_dataset_is_smoke_only_and_no_non_smoke_candidate_found\n"
            "build or pass a non-smoke dataset, or use --allow-smoke-dataset only for plumbing validation"
        )
    return selected


def load_jsonl(path: Path, limit: int = 0) -> list[dict[str, Any]]:
    rows = []
    with path.open() as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
            if limit > 0 and len(rows) >= limit:
                break
    if not rows:
        raise SystemExit(f"empty_dataset:{path}")
    return rows


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2))


def main() -> None:
    args = parse_args()
    dataset_path = resolve_dataset_path(args.dataset, allow_smoke_dataset=args.allow_smoke_dataset)
    samples = load_jsonl(dataset_path, limit=args.limit)
    model, metrics = train_hybrid_signal_ensemble(
        samples,
        ensemble_size=args.ensemble_size,
        learning_rate=args.learning_rate,
        epochs=args.epochs,
        l2=args.l2,
        seed=args.seed,
    )

    latest_predictions = []
    for sample in samples[-10:]:
        output = model.predict(extract_feature_row(sample))
        latest_predictions.append({
            "ticker": sample.get("ticker"),
            "as_of_date": sample.get("as_of_date"),
            "target_date": sample.get("target_date"),
            **output,
        })

    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    prefix = f"{args.output_prefix}_{stamp}"
    artifact_path = MODEL_OUTPUT_DIR / f"{prefix}.artifact.json"
    metrics_path = MODEL_OUTPUT_DIR / f"{prefix}.metrics.json"
    diagnostics_path = MODEL_OUTPUT_DIR / f"{prefix}.diagnostics.json"

    artifact_payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "dataset_path": str(dataset_path.relative_to(ROOT_DIR)),
        "artifact_name": prefix,
        "target": "next_day_log_return_positive_probability",
        "training_entrypoint": "cloud_training.training.train_predictive_model",
        "outputs": {
            "signal_probability": "calibrated_probability_of_positive_next_day_return_in_[0,1]",
            "confidence": "probability_distance_from_decision_boundary_in_[0,1]",
            "predictive_variance": "bernoulli_signal_variance",
            "raw_margin": "xgboost_margin_before_platt_calibration"
        },
        "artifact": model.to_artifact(),
    }
    diagnostics_payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "artifact_name": prefix,
        "dataset_path": str(dataset_path.relative_to(ROOT_DIR)),
        "diagnostics_schema_version": "xgboost_predictive_diagnostics_v1",
        **metrics["diagnostics"],
    }
    metrics_payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "artifact_name": prefix,
        "dataset_path": str(dataset_path.relative_to(ROOT_DIR)),
        "metrics_schema_version": "xgboost_predictive_metrics_v1",
        "model_type": metrics["model_type"],
        "train_accuracy": metrics["train_accuracy"],
        "train_loss": metrics["train_loss"],
        "samples": metrics["samples"],
        "ensemble_size": metrics["ensemble_size"],
        "feature_names": metrics["feature_names"],
        "split_metrics": metrics["split_metrics"],
        "diagnostics_path": str(diagnostics_path.relative_to(ROOT_DIR)),
        "latest_predictions": latest_predictions,
    }

    write_json(artifact_path, artifact_payload)
    write_json(metrics_path, metrics_payload)
    write_json(diagnostics_path, diagnostics_payload)
    print(json.dumps({
        "status": "ok",
        "artifact": str(artifact_path.relative_to(ROOT_DIR)),
        "metrics": str(metrics_path.relative_to(ROOT_DIR)),
        "diagnostics": str(diagnostics_path.relative_to(ROOT_DIR)),
        "train_accuracy": metrics["train_accuracy"],
        "train_loss": metrics["train_loss"],
        "samples": metrics["samples"],
    }, indent=2))


if __name__ == "__main__":
    main()
