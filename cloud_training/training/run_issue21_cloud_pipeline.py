from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT_DIR = Path(__file__).resolve().parents[2]
REPORT_DIR = ROOT_DIR / "reports" / "pipeline"
TRAINING_DIR = ROOT_DIR / "data" / "processed" / "training"
MODEL_DIR = ROOT_DIR / "data" / "processed" / "models"
BACKTEST_DIR = ROOT_DIR / "reports" / "backtests"
BUNDLE_DIR = ROOT_DIR / "artifacts" / "bundles"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Issue #21 cloud-side candidate pipeline: dataset/train/validate/export"
    )
    parser.add_argument("--dataset", help="existing dataset jsonl path; recommended for cloud-built real FinBERT datasets")
    parser.add_argument("--build-dataset", action="store_true", help="build dataset via canonical predictive dataset pipeline")
    parser.add_argument("--dataset-max-tickers", type=int, default=0)
    parser.add_argument("--dataset-max-samples-per-ticker", type=int, default=0)
    parser.add_argument("--dataset-tickers", default="", help="optional explicit comma-separated ticker list")
    parser.add_argument("--dataset-output-prefix", default="issue21_real_candidate")
    parser.add_argument("--dataset-sentiment-scorer", choices=["finbert", "mock"], default="finbert")
    parser.add_argument("--dataset-news-lookback-days", type=int, default=7)
    parser.add_argument("--dataset-coverage-lookback-days", type=int, default=90)
    parser.add_argument("--dataset-ticker-selection", choices=["alphabetical", "coverage"], default="coverage")
    parser.add_argument("--exclude-market-proxy-target", action="store_true")
    parser.add_argument("--allow-smoke-dataset", action="store_true", help="only for plumbing validation; not for promotable candidates")

    parser.add_argument("--output-prefix", default="issue21_validation_candidate")
    parser.add_argument("--epochs", type=int, default=300)
    parser.add_argument("--ensemble-size", type=int, default=1)
    parser.add_argument("--learning-rate", type=float, default=0.05)
    parser.add_argument("--l2", type=float, default=1e-3)
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--limit", type=int, default=0)

    parser.add_argument("--validation-output-prefix", default="issue21_validation")
    parser.add_argument("--train-years", type=int, default=2)
    parser.add_argument("--test-months", type=int, default=6)
    parser.add_argument("--max-weight-per-ticker", type=float, default=0.20)

    parser.add_argument("--export-bundle", action="store_true")
    parser.add_argument("--bundle-output-prefix", default="issue21_predictive_bundle")
    parser.add_argument("--source-issue", type=int, default=21)
    return parser.parse_args()


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def ensure_dirs() -> None:
    for path in [REPORT_DIR, TRAINING_DIR, MODEL_DIR, BACKTEST_DIR, BUNDLE_DIR]:
        path.mkdir(parents=True, exist_ok=True)


def _run(command: list[str]) -> dict[str, Any]:
    proc = subprocess.run(
        command,
        cwd=ROOT_DIR,
        text=True,
        capture_output=True,
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


def _manifest_path_for_dataset(dataset_path: Path) -> Path:
    return dataset_path.with_suffix(".manifest.json")


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


def _assert_dataset_allowed(dataset_path: Path, *, allow_smoke_dataset: bool) -> dict[str, Any]:
    manifest = _read_manifest(dataset_path)
    if _dataset_looks_smoke(dataset_path, manifest) and not allow_smoke_dataset:
        raise SystemExit(
            f"refusing_smoke_dataset:{dataset_path.relative_to(ROOT_DIR)}\n"
            "pass --allow-smoke-dataset only for plumbing validation"
        )
    return manifest


def _latest_non_smoke_dataset() -> Path:
    candidates = sorted(TRAINING_DIR.glob("*.jsonl"), key=lambda path: path.stat().st_mtime)
    if not candidates:
        raise SystemExit("no_dataset_provided_and_no_training_dataset_found")
    non_smoke = [path for path in candidates if not _dataset_looks_smoke(path, _read_manifest(path))]
    return non_smoke[-1] if non_smoke else candidates[-1]


def build_dataset(args: argparse.Namespace) -> tuple[Path, dict[str, Any]]:
    command = [
        sys.executable,
        "-m",
        "cloud_training.data_pipelines.build_predictive_dataset",
        "--output-prefix",
        args.dataset_output_prefix,
        "--sentiment-scorer",
        args.dataset_sentiment_scorer,
        "--news-lookback-days",
        str(args.dataset_news_lookback_days),
        "--coverage-lookback-days",
        str(args.dataset_coverage_lookback_days),
        "--ticker-selection",
        args.dataset_ticker_selection,
    ]
    if args.dataset_max_tickers > 0:
        command.extend(["--max-tickers", str(args.dataset_max_tickers)])
    if args.dataset_max_samples_per_ticker > 0:
        command.extend(["--max-samples-per-ticker", str(args.dataset_max_samples_per_ticker)])
    if args.dataset_tickers.strip():
        command.extend(["--tickers", args.dataset_tickers.strip()])
    if args.exclude_market_proxy_target:
        command.append("--exclude-market-proxy-target")
    result = _run(command)
    _require_ok("build_dataset", result)
    payload = _extract_json_from_stdout(result["stdout"])
    dataset_rel = payload.get("jsonl")
    if not dataset_rel:
        raise SystemExit("dataset_build_completed_but_no_jsonl_reported")
    return _resolve_path(dataset_rel), result


def resolve_dataset(args: argparse.Namespace) -> tuple[Path, str, dict[str, Any] | None, dict[str, Any]]:
    if args.dataset:
        dataset_path = _resolve_path(args.dataset)
        if not dataset_path.exists():
            raise SystemExit(f"dataset_not_found:{dataset_path}")
        manifest = _assert_dataset_allowed(dataset_path, allow_smoke_dataset=args.allow_smoke_dataset)
        return dataset_path, "existing_dataset", None, manifest
    if args.build_dataset:
        dataset_path, result = build_dataset(args)
        manifest = _assert_dataset_allowed(dataset_path, allow_smoke_dataset=args.allow_smoke_dataset)
        return dataset_path, "built_predictive_dataset", result, manifest
    dataset_path = _latest_non_smoke_dataset()
    manifest = _assert_dataset_allowed(dataset_path, allow_smoke_dataset=args.allow_smoke_dataset)
    return dataset_path, "latest_existing_dataset", None, manifest


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
    return _extract_json_from_stdout(result["stdout"]), result


def validate_model(args: argparse.Namespace, dataset_path: Path, artifact_path: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    command = [
        sys.executable,
        "-m",
        "cloud_training.backtesting.validation_metrics",
        "--dataset",
        str(dataset_path),
        "--artifact",
        str(artifact_path),
        "--output-prefix",
        args.validation_output_prefix,
        "--train-years",
        str(args.train_years),
        "--test-months",
        str(args.test_months),
        "--max-weight-per-ticker",
        str(args.max_weight_per_ticker),
    ]
    result = _run(command)
    _require_ok("validate_model", result)
    return _extract_json_from_stdout(result["stdout"]), result


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
    return _extract_json_from_stdout(result["stdout"]), result


def main() -> None:
    args = parse_args()
    ensure_dirs()
    dataset_path, dataset_mode, dataset_step, dataset_manifest = resolve_dataset(args)

    train_payload, train_step = train_model(args, dataset_path)
    artifact_path = _resolve_path(train_payload["artifact"])
    metrics_path = _resolve_path(train_payload["metrics"])
    diagnostics_path = _resolve_path(train_payload["diagnostics"])

    validation_payload, validation_step = validate_model(args, dataset_path, artifact_path)
    validation_report_path = _resolve_path(validation_payload["report"])

    bundle_payload = None
    bundle_step = None
    bundle_path = None
    if args.export_bundle:
        bundle_payload, bundle_step = export_bundle(args, artifact_path)
        bundle_path = _resolve_path(bundle_payload["bundle"])

    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    report_path = REPORT_DIR / f"issue21_cloud_pipeline_{stamp}.json"
    report = {
        "status": "ok",
        "generated_at": utc_now(),
        "issue": 21,
        "dataset_mode": dataset_mode,
        "dataset_path": str(dataset_path.relative_to(ROOT_DIR)),
        "dataset_manifest": dataset_manifest,
        "artifact_path": str(artifact_path.relative_to(ROOT_DIR)),
        "metrics_path": str(metrics_path.relative_to(ROOT_DIR)),
        "diagnostics_path": str(diagnostics_path.relative_to(ROOT_DIR)),
        "validation_report_path": str(validation_report_path.relative_to(ROOT_DIR)),
        "bundle_path": str(bundle_path.relative_to(ROOT_DIR)) if bundle_path else None,
        "training_entrypoint": "python -m cloud_training.training.train_predictive_model",
        "validation_entrypoint": "python -m cloud_training.backtesting.validation_metrics",
        "cloud_pipeline_entrypoint": "python -m cloud_training.training.run_issue21_cloud_pipeline",
        "candidate_summary": {
            "dataset_sentiment_model": dataset_manifest.get("article_sentiment_model"),
            "dataset_rows": dataset_manifest.get("rows"),
            "tickers_considered": dataset_manifest.get("tickers_considered", []),
            "promote": validation_payload.get("promote"),
            "beats_spy": validation_payload.get("beats_spy"),
            "sharpe": validation_payload.get("sharpe"),
            "max_drawdown": validation_payload.get("max_drawdown"),
            "trading_days": validation_payload.get("trading_days"),
        },
        "steps": {
            "dataset": dataset_step,
            "train": train_step,
            "validate": validation_step,
            "export_bundle": bundle_step,
        },
    }
    report_path.write_text(json.dumps(report, indent=2))
    print(json.dumps({
        "status": "ok",
        "report": str(report_path.relative_to(ROOT_DIR)),
        "dataset_mode": dataset_mode,
        "dataset_path": report["dataset_path"],
        "artifact_path": report["artifact_path"],
        "metrics_path": report["metrics_path"],
        "diagnostics_path": report["diagnostics_path"],
        "validation_report_path": report["validation_report_path"],
        "bundle_path": report["bundle_path"],
        **report["candidate_summary"],
    }, indent=2))


if __name__ == "__main__":
    main()
