from __future__ import annotations

import json
import os
import subprocess
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

ROOT_DIR = Path(__file__).resolve().parents[1]
JOBS_DIR = ROOT_DIR / "reports" / "pipeline" / "jobs"
JOB_INDEX_PATH = JOBS_DIR / "index.json"


TERMINAL_STATUSES = {"succeeded", "failed"}
ACTIVE_STATUSES = {"queued", "running"}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def ensure_job_dirs() -> None:
    JOBS_DIR.mkdir(parents=True, exist_ok=True)


def _job_dir(job_id: str) -> Path:
    return JOBS_DIR / job_id


def _job_status_path(job_id: str) -> Path:
    return _job_dir(job_id) / "status.json"


def _job_log_path(job_id: str) -> Path:
    return _job_dir(job_id) / "run.log"


def _job_command_path(job_id: str) -> Path:
    return _job_dir(job_id) / "command.json"


def _job_index() -> list[dict[str, Any]]:
    ensure_job_dirs()
    if not JOB_INDEX_PATH.exists():
        return []
    try:
        payload = json.loads(JOB_INDEX_PATH.read_text())
    except json.JSONDecodeError:
        return []
    jobs = payload.get("jobs")
    return jobs if isinstance(jobs, list) else []


def _write_job_index(jobs: Iterable[dict[str, Any]]) -> None:
    JOB_INDEX_PATH.write_text(json.dumps({"generated_at": utc_now(), "jobs": list(jobs)}, indent=2))


def append_job_index(status_payload: dict[str, Any]) -> None:
    jobs = [job for job in _job_index() if job.get("job_id") != status_payload.get("job_id")]
    summary = {
        "job_id": status_payload["job_id"],
        "job_type": status_payload.get("job_type"),
        "status": status_payload.get("status"),
        "requested_at": status_payload.get("requested_at"),
        "started_at": status_payload.get("started_at"),
        "finished_at": status_payload.get("finished_at"),
        "request_label": status_payload.get("request_label"),
        "status_path": status_payload.get("status_path"),
        "log_path": status_payload.get("log_path"),
    }
    jobs.append(summary)
    jobs = sorted(jobs, key=lambda row: row.get("requested_at") or "")[-50:]
    _write_job_index(jobs)


def read_job_status(job_id: str) -> dict[str, Any]:
    path = _job_status_path(job_id)
    if not path.exists():
        raise FileNotFoundError(f"job_not_found:{job_id}")
    return json.loads(path.read_text())


def latest_jobs(limit: int = 10) -> list[dict[str, Any]]:
    jobs = sorted(_job_index(), key=lambda row: row.get("requested_at") or "")
    return jobs[-limit:]


def build_job_request(*, job_type: str, request_label: str, command: list[str], command_cwd: Path | None = None, meta: dict[str, Any] | None = None) -> dict[str, Any]:
    ensure_job_dirs()
    job_id = f"job-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{uuid.uuid4().hex[:8]}"
    job_dir = _job_dir(job_id)
    job_dir.mkdir(parents=True, exist_ok=False)
    status_path = _job_status_path(job_id)
    log_path = _job_log_path(job_id)
    command_path = _job_command_path(job_id)
    payload = {
        "contract_version": "hf_space_job/v1",
        "job_id": job_id,
        "job_type": job_type,
        "request_label": request_label,
        "status": "queued",
        "requested_at": utc_now(),
        "started_at": None,
        "finished_at": None,
        "returncode": None,
        "command": command,
        "command_cwd": str((command_cwd or ROOT_DIR).resolve()),
        "runner_entrypoint": "python -m cloud_training.training.hf_job_runner",
        "status_path": str(status_path.relative_to(ROOT_DIR)),
        "log_path": str(log_path.relative_to(ROOT_DIR)),
        "command_path": str(command_path.relative_to(ROOT_DIR)),
        "result": None,
        "error": None,
        "meta": meta or {},
    }
    status_path.write_text(json.dumps(payload, indent=2))
    command_path.write_text(json.dumps({"command": command, "cwd": payload["command_cwd"]}, indent=2))
    if not log_path.exists():
        log_path.write_text("")
    append_job_index(payload)
    return payload


def write_job_status(job_id: str, **updates: Any) -> dict[str, Any]:
    payload = read_job_status(job_id)
    payload.update(updates)
    _job_status_path(job_id).write_text(json.dumps(payload, indent=2))
    append_job_index(payload)
    return payload


def submit_job(job_request: dict[str, Any]) -> dict[str, Any]:
    runner_command = [
        sys.executable,
        "-m",
        "cloud_training.training.hf_job_runner",
        "--job-id",
        job_request["job_id"],
    ]
    kwargs: dict[str, Any] = {
        "cwd": ROOT_DIR,
        "stdin": subprocess.DEVNULL,
        "stdout": subprocess.DEVNULL,
        "stderr": subprocess.DEVNULL,
        "start_new_session": True,
        "env": os.environ.copy(),
    }
    subprocess.Popen(runner_command, **kwargs)
    return read_job_status(job_request["job_id"])
