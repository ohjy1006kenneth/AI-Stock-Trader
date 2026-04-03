from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT_DIR = Path(__file__).resolve().parents[2]
REPORT_DIR = ROOT_DIR / "reports" / "pipeline"
TRAINING_DIR = ROOT_DIR / "data" / "processed" / "training"
MODEL_DIR = ROOT_DIR / "data" / "processed" / "models"
BUNDLE_DIR = ROOT_DIR / "artifacts" / "bundles"
PRICE_SNAPSHOT_PATH = ROOT_DIR / "data" / "runtime" / "market" / "price_snapshot.json"
FUNDAMENTAL_SNAPSHOT_PATH = ROOT_DIR / "data" / "runtime" / "market" / "fundamental_snapshot.json"
LOCAL_ALPACA_ENV_PATH = ROOT_DIR / "config" / "alpaca.env"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Issue #12 cloud-safe XGBoost dataset/train/export/publish pipeline")
    parser.add_argument("--dataset", help="existing dataset jsonl path; if omitted can build or synthesize one")
    parser.add_argument("--build-dataset", action="store_true", help="build dataset via canonical predictive dataset pipeline")
    parser.add_argument("--dataset-max-tickers", type=int, default=0)
    parser.add_argument("--dataset-max-samples-per-ticker", type=int, default=0)
    parser.add_argument("--dataset-sentiment-scorer", choices=["finbert", "mock"], default="finbert")
    parser.add_argument("--allow-smoke-dataset", action="store_true", help="allow smoke/synthetic/mock datasets; only for plumbing validation, not real candidate runs")
    parser.add_argument("--dataset-output-prefix", default="issue12_aligned_offline")
    parser.add_argument("--smoke-synthetic-dataset", action="store_true", help="generate a tiny synthetic-but-trainable dataset locally for plumbing validation")
    parser.add_argument("--smoke-rows", type=int, default=360)
    parser.add_argument("--output-prefix", default="issue12_xgb_baseline")
    parser.add_argument("--epochs", type=int, default=250)
    parser.add_argument("--ensemble-size", type=int, default=1)
    parser.add_argument("--learning-rate", type=float, default=0.05)
    parser.add_argument("--l2", type=float, default=1e-3)
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--export-bundle", action="store_true")
    parser.add_argument("--bundle-output-prefix", default="predictive_signal_bundle_v1")
    parser.add_argument("--source-issue", type=int, default=12)
    parser.add_argument("--publish-repo-id", help="optional HF model repo id to publish latest bundle")
    parser.add_argument("--publish-path-in-repo", default="bundles")
    return parser.parse_args()


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def ensure_dirs() -> None:
    for path in [REPORT_DIR, TRAINING_DIR, MODEL_DIR, BUNDLE_DIR]:
        path.mkdir(parents=True, exist_ok=True)


def _latest(directory: Path, suffix: str) -> Path | None:
    candidates = sorted(directory.glob(suffix), key=lambda path: path.stat().st_mtime)
    return candidates[-1] if candidates else None


def _run(command: list[str]) -> dict[str, Any]:
    proc = subprocess.run(
        command,
        cwd=ROOT_DIR,
        text=True,
        capture_output=True,
        env=None,
    )
    return {
        "command": command,
        "returncode": proc.returncode,
        "stdout": proc.stdout,
        "stderr": proc.stderr,
    }


def _require_ok(step: str, result: dict[str, Any]) -> None:
    if result["returncode"] != 0:
        raise SystemExit(json.dumps({"status": "failed", "step": step, **result}, indent=2))


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


def _assert_dataset_allowed(dataset_path: Path, *, allow_smoke_dataset: bool) -> None:
    manifest = _read_manifest(dataset_path)
    if _dataset_looks_smoke(dataset_path, manifest) and not allow_smoke_dataset:
        raise SystemExit(
            f"refusing_smoke_dataset:{dataset_path.relative_to(ROOT_DIR)}\n"
            "pass --allow-smoke-dataset only for plumbing validation"
        )


def _extract_json_from_stdout(stdout: str) -> dict[str, Any]:
    lines = [line for line in stdout.strip().splitlines() if line.strip()]
    if not lines:
        return {}
    try:
        return json.loads("\n".join(lines))
    except json.JSONDecodeError:
        for idx in range(len(lines)):
            candidate = "\n".join(lines[idx:])
            try:
                return json.loads(candidate)
            except json.JSONDecodeError:
                continue
    return {}


def _has_alpaca_credentials() -> bool:
    if os.getenv("ALPACA_API_KEY") and os.getenv("ALPACA_API_SECRET"):
        return True
    if not LOCAL_ALPACA_ENV_PATH.exists():
        return False
    env_lines = LOCAL_ALPACA_ENV_PATH.read_text().splitlines()
    keys = {line.split("=", 1)[0].strip() for line in env_lines if "=" in line and not line.strip().startswith("#")}
    return "ALPACA_API_KEY" in keys and "ALPACA_API_SECRET" in keys


def validate_build_dataset_inputs(args: argparse.Namespace) -> None:
    missing: list[str] = []
    if not PRICE_SNAPSHOT_PATH.exists():
        missing.append(f"missing_price_snapshot:{PRICE_SNAPSHOT_PATH.relative_to(ROOT_DIR)}")
    if not FUNDAMENTAL_SNAPSHOT_PATH.exists():
        missing.append(f"missing_fundamental_snapshot:{FUNDAMENTAL_SNAPSHOT_PATH.relative_to(ROOT_DIR)}")
    if not _has_alpaca_credentials():
        missing.append("missing_alpaca_credentials: set ALPACA_API_KEY and ALPACA_API_SECRET via env or config/alpaca.env")
    if missing:
        raise SystemExit("issue12_build_dataset_prereqs_failed:\n- " + "\n- ".join(missing))



def build_dataset(args: argparse.Namespace) -> tuple[Path, dict[str, Any]]:
    validate_build_dataset_inputs(args)
    command = [sys.executable, "-m", "cloud_training.data_pipelines.build_predictive_dataset", "--output-prefix", args.dataset_output_prefix, "--sentiment-scorer", args.dataset_sentiment_scorer]
    if args.dataset_max_tickers > 0:
        command.extend(["--max-tickers", str(args.dataset_max_tickers)])
    if args.dataset_max_samples_per_ticker > 0:
        command.extend(["--max-samples-per-ticker", str(args.dataset_max_samples_per_ticker)])
    result = _run(command)
    _require_ok("build_dataset", result)
    payload = _extract_json_from_stdout(result["stdout"])
    dataset_rel = payload.get("jsonl")
    if not dataset_rel:
        latest = _latest(TRAINING_DIR, "*.jsonl")
        if latest is None:
            raise SystemExit("dataset_build_completed_but_no_jsonl_found")
        return latest, result
    return _resolve_path(dataset_rel), result


def build_smoke_dataset(args: argparse.Namespace) -> tuple[Path, dict[str, Any]]:
    command = [
        sys.executable,
        "-m",
        "cloud_training.training.synthetic_issue12_dataset",
        "--rows",
        str(args.smoke_rows),
        "--output-prefix",
        f"{args.output_prefix}_smoke",
    ]
    result = _run(command)
    _require_ok("build_smoke_dataset", result)
    payload = _extract_json_from_stdout(result["stdout"])
    dataset_rel = payload.get("jsonl")
    if not dataset_rel:
        raise SystemExit("smoke_dataset_builder_did_not_return_jsonl")
    return _resolve_path(dataset_rel), result


def train_model(args: argparse.Namespace, dataset_path: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    command = [
        sys.executable,
        "-m",
        "cloud_training.training.train_predictive_model",
        "--dataset",
        str(dataset_path),
        "--output-prefix",
        args.output_prefix,
        "--ensemble-size",
        str(args.ensemble_size),
        "--epochs",
        str(args.epochs),
        "--learning-rate",
        str(args.learning_rate),
        "--l2",
        str(args.l2),
        "--seed",
        str(args.seed),
    ]
    if args.allow_smoke_dataset:
        command.append("--allow-smoke-dataset")
    if args.limit > 0:
        command.extend(["--limit", str(args.limit)])
    result = _run(command)
    _require_ok("train_model", result)
    payload = _extract_json_from_stdout(result["stdout"])
    return payload, result


def export_bundle(args: argparse.Namespace, artifact_path: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    command = [
        sys.executable,
        "-m",
        "cloud_training.training.export_servable_artifact",
        "--source-artifact",
        str(artifact_path),
        "--output-prefix",
        args.bundle_output_prefix,
        "--source-issue",
        str(args.source_issue),
    ]
    result = _run(command)
    _require_ok("export_bundle", result)
    payload = _extract_json_from_stdout(result["stdout"])
    return payload, result


def publish_bundle(args: argparse.Namespace, bundle_path: Path) -> dict[str, Any]:
    command = [
        sys.executable,
        "-m",
        "cloud_training.publish_hf_bundle",
        "--repo-id",
        args.publish_repo_id,
        "--bundle",
        str(bundle_path),
        "--path-in-repo",
        args.publish_path_in_repo,
    ]
    result = _run(command)
    _require_ok("publish_bundle", result)
    return result


def resolve_dataset(args: argparse.Namespace) -> tuple[Path, str, dict[str, Any] | None]:
    if args.dataset:
        dataset_path = _resolve_path(args.dataset)
        _assert_dataset_allowed(dataset_path, allow_smoke_dataset=args.allow_smoke_dataset)
        return dataset_path, "existing_dataset", None
    if args.smoke_synthetic_dataset:
        dataset_path, result = build_smoke_dataset(args)
        _assert_dataset_allowed(dataset_path, allow_smoke_dataset=args.allow_smoke_dataset)
        return dataset_path, "synthetic_smoke_dataset", result
    if args.build_dataset:
        dataset_path, result = build_dataset(args)
        _assert_dataset_allowed(dataset_path, allow_smoke_dataset=args.allow_smoke_dataset)
        return dataset_path, "built_predictive_dataset", result
    candidates = sorted(TRAINING_DIR.glob("*.jsonl"), key=lambda path: path.stat().st_mtime)
    if not candidates:
        raise SystemExit("no_dataset_provided_and_no_training_dataset_found")
    non_smoke = [path for path in candidates if not _dataset_looks_smoke(path, _read_manifest(path))]
    dataset_path = non_smoke[-1] if non_smoke else candidates[-1]
    _assert_dataset_allowed(dataset_path, allow_smoke_dataset=args.allow_smoke_dataset)
    return dataset_path, "latest_existing_dataset", None


def main() -> None:
    args = parse_args()
    ensure_dirs()
    dataset_path, dataset_mode, dataset_step = resolve_dataset(args)
    if not dataset_path.exists():
        raise SystemExit(f"dataset_not_found:{dataset_path}")

    train_payload, train_step = train_model(args, dataset_path)
    artifact_path = _resolve_path(train_payload["artifact"])
    metrics_path = _resolve_path(train_payload["metrics"])
    diagnostics_path = _resolve_path(train_payload["diagnostics"])

    bundle_payload = None
    bundle_step = None
    bundle_path = None
    publish_step = None
    if args.export_bundle or args.publish_repo_id:
        bundle_payload, bundle_step = export_bundle(args, artifact_path)
        bundle_path = _resolve_path(bundle_payload["bundle"])
    if args.publish_repo_id:
        if bundle_path is None:
            raise SystemExit("publish_requested_but_bundle_not_created")
        publish_step = publish_bundle(args, bundle_path)

    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    report_path = REPORT_DIR / f"issue12_cloud_pipeline_{stamp}.json"
    report = {
        "status": "ok",
        "generated_at": utc_now(),
        "issue": 12,
        "dataset_mode": dataset_mode,
        "dataset_path": str(dataset_path.relative_to(ROOT_DIR)),
        "artifact_path": str(artifact_path.relative_to(ROOT_DIR)),
        "metrics_path": str(metrics_path.relative_to(ROOT_DIR)),
        "diagnostics_path": str(diagnostics_path.relative_to(ROOT_DIR)),
        "bundle_path": str(bundle_path.relative_to(ROOT_DIR)) if bundle_path else None,
        "publish_repo_id": args.publish_repo_id,
        "publish_path_in_repo": args.publish_path_in_repo if args.publish_repo_id else None,
        "training_entrypoint": "python -m cloud_training.training.train_predictive_model",
        "cloud_pipeline_entrypoint": "python -m cloud_training.training.run_issue12_cloud_pipeline",
        "downstream_issue14_handoff": {
            "refresh_unit": "single bundle json",
            "artifact_to_pin": str(bundle_path.relative_to(ROOT_DIR)) if bundle_path else None,
            "oracle_refresh_input": str(bundle_path.relative_to(ROOT_DIR)) if bundle_path else None,
            "expected_supporting_outputs": [
                str(metrics_path.relative_to(ROOT_DIR)),
                str(diagnostics_path.relative_to(ROOT_DIR)),
            ],
            "serving_publish_expectation": "Issue #14 should refresh/pin the exported bundle as the atomic model artifact for oracle deployment.",
        },
        "steps": {
            "dataset": dataset_step,
            "train": train_step,
            "export_bundle": bundle_step,
            "publish_bundle": publish_step,
        },
    }
    report_path.write_text(json.dumps(report, indent=2))
    print(json.dumps({
        "status": "ok",
        "report": str(report_path.relative_to(ROOT_DIR)),
        "dataset_path": report["dataset_path"],
        "artifact_path": report["artifact_path"],
        "metrics_path": report["metrics_path"],
        "diagnostics_path": report["diagnostics_path"],
        "bundle_path": report["bundle_path"],
        "dataset_mode": dataset_mode,
    }, indent=2))


if __name__ == "__main__":
    main()
