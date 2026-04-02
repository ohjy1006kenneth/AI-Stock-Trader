from __future__ import annotations

import argparse
import json
import subprocess
from pathlib import Path
from typing import Any

from cloud_training.hf_jobs import ROOT_DIR, read_job_status, utc_now, write_job_status


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Execute one file-backed HF Space job and persist durable status/results")
    parser.add_argument("--job-id", required=True)
    return parser.parse_args()


def _extract_json_from_text(text: str) -> dict[str, Any] | None:
    lines = [line for line in text.strip().splitlines() if line.strip()]
    if not lines:
        return None
    for idx in range(len(lines)):
        candidate = "\n".join(lines[idx:])
        try:
            payload = json.loads(candidate)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            return payload
    return None


def main() -> None:
    args = parse_args()
    job = read_job_status(args.job_id)
    command = job["command"]
    cwd = Path(job.get("command_cwd") or ROOT_DIR)
    log_path = ROOT_DIR / job["log_path"]

    running_job = write_job_status(args.job_id, status="running", started_at=utc_now(), error=None)

    proc = subprocess.run(
        command,
        cwd=cwd,
        text=True,
        capture_output=True,
        env=None,
    )
    finished_at = utc_now()
    result_payload = _extract_json_from_text(proc.stdout)
    log_text = (
        f"$ {' '.join(command)}\n"
        f"job_id={args.job_id}\n"
        f"started_at={running_job.get('started_at')}\n"
        f"finished_at={finished_at}\n"
        f"returncode={proc.returncode}\n\n"
        f"[stdout]\n{proc.stdout}\n\n[stderr]\n{proc.stderr}\n"
    )
    log_path.write_text(log_text)

    updates: dict[str, Any] = {
        "finished_at": finished_at,
        "returncode": proc.returncode,
        "result": result_payload,
        "status": "succeeded" if proc.returncode == 0 else "failed",
        "error": None if proc.returncode == 0 else {"message": proc.stderr.strip() or f"command_failed:{proc.returncode}"},
    }
    write_job_status(args.job_id, **updates)

    if proc.returncode != 0:
        raise SystemExit(proc.returncode)


if __name__ == "__main__":
    main()
