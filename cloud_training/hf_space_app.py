from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

try:
    import gradio as gr
except ModuleNotFoundError:  # pragma: no cover - allows non-UI tests without gradio installed
    gr = None

from cloud_training.hf_jobs import ROOT_DIR, ensure_job_dirs, build_job_request, latest_jobs, read_job_status, submit_job

LOG_DIR = ROOT_DIR / "reports" / "pipeline"
LOG_PATH = LOG_DIR / "hf_space_lab_latest.log"
TRAINING_DATA_DIR = ROOT_DIR / "data" / "processed" / "training"
MODEL_DATA_DIR = ROOT_DIR / "data" / "processed" / "models"
BUNDLE_DIR = ROOT_DIR / "artifacts" / "bundles"


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def ensure_runtime_dirs() -> None:
    for path in [LOG_DIR, TRAINING_DATA_DIR, MODEL_DATA_DIR, BUNDLE_DIR]:
        path.mkdir(parents=True, exist_ok=True)
    ensure_job_dirs()


def tail_paths(paths: Iterable[Path], suffix: str) -> list[str]:
    return [str(path.relative_to(ROOT_DIR)) for path in sorted(paths.glob(suffix))[-10:]]


def status_payload() -> str:
    ensure_runtime_dirs()
    payload = {
        "generated_at": utc_now(),
        "python": sys.version.split()[0],
        "cwd": str(ROOT_DIR),
        "hf_space_id": os.getenv("SPACE_ID"),
        "hf_artifact_repo_id": os.getenv("HF_ARTIFACT_REPO_ID"),
        "latest_training_datasets": tail_paths(TRAINING_DATA_DIR, "*.jsonl"),
        "latest_model_artifacts": tail_paths(MODEL_DATA_DIR, "*.artifact.json"),
        "latest_model_metrics": tail_paths(MODEL_DATA_DIR, "*.metrics.json"),
        "latest_bundles": tail_paths(BUNDLE_DIR, "*.bundle.json"),
        "latest_jobs": latest_jobs(limit=10),
    }
    return json.dumps(payload, indent=2)


def _job_command_issue12_pipeline(smoke_mode: bool, dataset_limit: int, train_limit: int, ensemble_size: int, epochs: int) -> list[str]:
    command = [
        sys.executable,
        "-m",
        "cloud_training.training.run_issue12_cloud_pipeline",
        "--export-bundle",
        "--ensemble-size",
        str(ensemble_size),
        "--epochs",
        str(epochs),
    ]
    if smoke_mode:
        command.extend(["--smoke-synthetic-dataset", "--allow-smoke-dataset", "--smoke-rows", "360"])
    else:
        command.extend(["--build-dataset"])
        if dataset_limit > 0:
            command.extend(["--dataset-max-tickers", str(dataset_limit)])
    if train_limit > 0:
        command.extend(["--limit", str(train_limit)])
    return command


def _job_command_publish_latest_bundle() -> tuple[list[str] | None, str | None]:
    repo_id = os.getenv("HF_ARTIFACT_REPO_ID", "").strip()
    if not repo_id:
        return None, "missing_env:HF_ARTIFACT_REPO_ID"
    return [
        sys.executable,
        "-m",
        "cloud_training.publish_hf_bundle",
        "--repo-id",
        repo_id,
    ], None


def enqueue_job(job_type: str, request_label: str, command: list[str], meta: dict | None = None) -> tuple[str, str, str]:
    job_request = build_job_request(job_type=job_type, request_label=request_label, command=command, meta=meta)
    submitted = submit_job(job_request)
    LOG_PATH.write_text(json.dumps({"last_submitted_job": submitted["job_id"], "submitted_at": utc_now()}, indent=2))
    return submitted["job_id"], status_payload(), json.dumps(submitted, indent=2)


def enqueue_issue12_pipeline(smoke_mode: bool, dataset_limit: int, train_limit: int, ensemble_size: int, epochs: int) -> tuple[str, str, str]:
    command = _job_command_issue12_pipeline(smoke_mode, int(dataset_limit), int(train_limit), int(ensemble_size), int(epochs))
    label = "Issue #12 smoke pipeline" if smoke_mode else "Issue #12 real-data pipeline"
    return enqueue_job(
        "issue12_cloud_pipeline",
        label,
        command,
        meta={
            "smoke_mode": bool(smoke_mode),
            "dataset_limit": int(dataset_limit),
            "train_limit": int(train_limit),
            "ensemble_size": int(ensemble_size),
            "epochs": int(epochs),
        },
    )


def enqueue_publish_latest_bundle() -> tuple[str, str, str]:
    command, error = _job_command_publish_latest_bundle()
    if error:
        return error, status_payload(), "Set HF_ARTIFACT_REPO_ID in the Space settings before publishing bundles."
    return enqueue_job("publish_latest_bundle", "Publish latest bundle to HF artifact repo", command)


def inspect_job(job_id: str) -> tuple[str, str, str]:
    job_id = (job_id or "").strip()
    if not job_id:
        jobs = latest_jobs(limit=1)
        if not jobs:
            return "no_jobs", status_payload(), ""
        job_id = jobs[-1]["job_id"]
    try:
        payload = read_job_status(job_id)
    except FileNotFoundError:
        return f"job_not_found:{job_id}", status_payload(), ""
    log_path = ROOT_DIR / payload["log_path"]
    log_text = log_path.read_text() if log_path.exists() else json.dumps(payload, indent=2)
    return payload["status"], status_payload(), log_text


if gr is not None:
    with gr.Blocks(title="AI Stock Trader Lab") as demo:
        gr.Markdown(
            "# AI Stock Trader Lab\n"
            "Hugging Face Space runtime for Milestone 2 cloud-side training control-plane actions.\n\n"
            "This Space no longer acts as the long-running trainer itself. It submits durable file-backed jobs that execute the repo's existing job-style entrypoints, then lets you inspect status, results, and logs."
        )

        with gr.Row():
            dataset_limit = gr.Number(label="Dataset max tickers (0 = default script behavior)", value=0, precision=0)
            train_limit = gr.Number(label="Training sample limit (0 = full)", value=0, precision=0)
            ensemble_size = gr.Number(label="Ensemble size", value=3, precision=0)
            epochs = gr.Number(label="Epochs", value=250, precision=0)
            smoke_mode = gr.Checkbox(label="Issue #12 smoke mode (synthetic dataset)", value=False)

        with gr.Row():
            pipeline_button = gr.Button("Submit Issue #12 dataset→train→export job", variant="primary")
            publish_bundle_button = gr.Button("Submit publish-latest-bundle job", variant="secondary")
            refresh_button = gr.Button("Refresh status")

        selected_job_id = gr.Textbox(label="Job id to inspect (blank = latest)", value="")
        inspect_job_button = gr.Button("Inspect job", variant="secondary")

        status_box = gr.Code(label="Space status", language="json", value=status_payload())
        command_status = gr.Textbox(label="Job status / submission result", value="idle")
        log_box = gr.Textbox(label="Selected job log or status payload", lines=24)

        pipeline_button.click(enqueue_issue12_pipeline, inputs=[smoke_mode, dataset_limit, train_limit, ensemble_size, epochs], outputs=[command_status, status_box, log_box])
        publish_bundle_button.click(enqueue_publish_latest_bundle, inputs=[], outputs=[command_status, status_box, log_box])
        inspect_job_button.click(inspect_job, inputs=[selected_job_id], outputs=[command_status, status_box, log_box])
        refresh_button.click(lambda: ("refreshed", status_payload(), LOG_PATH.read_text() if LOG_PATH.exists() else ""), inputs=[], outputs=[command_status, status_box, log_box])
else:  # pragma: no cover - only used in non-Space environments without gradio
    demo = None


if __name__ == "__main__":
    ensure_runtime_dirs()
    if demo is None:
        raise SystemExit("gradio_not_installed")
    demo.launch(server_name="0.0.0.0", server_port=int(os.getenv("PORT", "7860")))
