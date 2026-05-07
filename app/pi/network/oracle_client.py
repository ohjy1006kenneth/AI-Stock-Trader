from __future__ import annotations

import json
import subprocess
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from core.contracts.schemas import PipelineManifestRecord, RunStatus
from services.r2.paths import pipeline_manifest_path
from services.r2.writer import R2Writer

_REPO_ROOT = Path(__file__).resolve().parents[3]
_MODAL_CONFIG_PATH = _REPO_ROOT / "config" / "modal.json"
_DAILY_LAYER1_SCRIPT = Path("app/lab/data_pipelines/run_daily_layer1.py")


class CommandRunner(Protocol):
    """Command execution surface used for Modal dispatch."""

    def run(self, command: Sequence[str], *, cwd: Path) -> CommandResult:
        """Run a command and return its exit status plus captured output."""


class ObjectStoreReader(Protocol):
    """Minimal object-store reads required for manifest polling."""

    def exists(self, key: str) -> bool:
        """Return True when a key exists."""

    def get_object(self, key: str) -> bytes:
        """Read one object payload."""


@dataclass(frozen=True)
class CommandResult:
    """Captured command execution result."""

    returncode: int
    stdout: str = ""
    stderr: str = ""


@dataclass(frozen=True)
class Layer1TriggerConfig:
    """Configuration for dispatching the daily Layer 1 Modal job."""

    run_id: str
    as_of_date: str
    layer0_run_id: str
    benchmark_ticker: str = "SPY"


@dataclass(frozen=True)
class Layer1WaitConfig:
    """Configuration for polling the daily Layer 1 manifest."""

    run_id: str
    as_of_date: str
    layer0_run_id: str
    poll_interval_seconds: int
    poll_timeout_seconds: int


@dataclass(frozen=True)
class Layer1DispatchResult:
    """Summary of one submitted daily Layer 1 Modal dispatch."""

    run_id: str
    manifest_key: str
    command: tuple[str, ...]


@dataclass(frozen=True)
class PiModalRuntimeConfig:
    """Pi-side Modal dispatch and polling settings."""

    layer1_poll_interval_seconds: int
    layer1_poll_timeout_seconds: int


class SubprocessCommandRunner:
    """Default subprocess-backed command runner for Pi Modal dispatch."""

    def run(self, command: Sequence[str], *, cwd: Path) -> CommandResult:
        """Execute a command and capture stdout/stderr for failure reporting."""
        completed = subprocess.run(
            list(command),
            cwd=cwd,
            capture_output=True,
            text=True,
            check=False,
        )
        return CommandResult(
            returncode=completed.returncode,
            stdout=completed.stdout,
            stderr=completed.stderr,
        )


def load_pi_modal_runtime_config(path: Path = _MODAL_CONFIG_PATH) -> PiModalRuntimeConfig:
    """Load Pi-side Modal poll settings from repository config."""
    payload = json.loads(path.read_text(encoding="utf-8"))
    modal_timeout_seconds = int(payload["timeout_seconds"])
    poll_interval_seconds = int(payload["layer1_poll_interval_seconds"])
    poll_timeout_seconds = int(payload["layer1_poll_timeout_seconds"])

    if modal_timeout_seconds <= 0:
        raise ValueError("timeout_seconds must be positive")
    if poll_interval_seconds <= 0:
        raise ValueError("layer1_poll_interval_seconds must be positive")
    if poll_timeout_seconds <= 0:
        raise ValueError("layer1_poll_timeout_seconds must be positive")
    if poll_timeout_seconds < modal_timeout_seconds:
        raise ValueError(
            "layer1_poll_timeout_seconds must be greater than or equal to timeout_seconds"
        )

    return PiModalRuntimeConfig(
        layer1_poll_interval_seconds=poll_interval_seconds,
        layer1_poll_timeout_seconds=poll_timeout_seconds,
    )


def trigger_layer1_feature_generation(
    config: Layer1TriggerConfig,
    *,
    command_runner: CommandRunner | None = None,
) -> Layer1DispatchResult:
    """Dispatch the daily Layer 1 Modal run from the Pi runtime."""
    runner = command_runner or SubprocessCommandRunner()
    command = (
        "python",
        "-m",
        "modal",
        "run",
        _DAILY_LAYER1_SCRIPT.as_posix(),
        "--run-id",
        config.run_id,
        "--as-of-date",
        config.as_of_date,
        "--layer0-run-id",
        config.layer0_run_id,
        "--benchmark-ticker",
        config.benchmark_ticker,
    )
    result = runner.run(command, cwd=_REPO_ROOT)
    if result.returncode != 0:
        raise RuntimeError(
            "Modal Layer 1 dispatch failed: "
            f"returncode={result.returncode} stderr={result.stderr.strip()!r} "
            f"stdout={result.stdout.strip()!r}"
        )
    return Layer1DispatchResult(
        run_id=config.run_id,
        manifest_key=pipeline_manifest_path("layer1", config.run_id),
        command=tuple(command),
    )


def wait_for_layer1_manifest(
    config: Layer1WaitConfig,
    *,
    reader: ObjectStoreReader | None = None,
    monotonic: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> PipelineManifestRecord:
    """Poll R2 for the expected daily Layer 1 manifest and fail closed when invalid."""
    active_reader = reader or R2Writer()
    manifest_key = pipeline_manifest_path("layer1", config.run_id)
    deadline = monotonic() + float(config.poll_timeout_seconds)

    while True:
        if active_reader.exists(manifest_key):
            manifest = PipelineManifestRecord.model_validate_json(
                active_reader.get_object(manifest_key).decode("utf-8")
            )
            _validate_layer1_manifest(manifest, config)
            if manifest.status == RunStatus.COMPLETED:
                return manifest
            if manifest.status in {RunStatus.FAILED, RunStatus.BLOCKED}:
                raise RuntimeError(
                    f"Layer 1 manifest indicates {manifest.status}: {manifest_key}"
                )

        if monotonic() >= deadline:
            raise TimeoutError(f"Timed out waiting for Layer 1 manifest: {manifest_key}")
        sleep(float(config.poll_interval_seconds))


def request_predictions(context: dict[str, object]) -> dict[str, object]:
    """Return deterministic placeholder prediction outputs for dry-run orchestration."""
    _ = context
    return {
        "scores": {
            "AAPL": 0.81,
            "MSFT": 0.74,
            "NVDA": 0.69,
        }
    }


def _validate_layer1_manifest(
    manifest: PipelineManifestRecord,
    config: Layer1WaitConfig,
) -> None:
    """Fail closed on stale or malformed Layer 1 manifests."""
    if manifest.stage != "layer1":
        raise RuntimeError(f"Unexpected manifest stage for Layer 1 wait: {manifest.stage!r}")
    metadata = manifest.metadata
    if metadata.get("as_of_date") != config.as_of_date:
        raise RuntimeError(
            "Layer 1 manifest is stale for requested as_of_date: "
            f"expected {config.as_of_date}, got {metadata.get('as_of_date')!r}"
        )
    if metadata.get("layer0_run_id") != config.layer0_run_id:
        raise RuntimeError(
            "Layer 1 manifest is stale for requested upstream run: "
            f"expected {config.layer0_run_id}, got {metadata.get('layer0_run_id')!r}"
        )
    if manifest.status == RunStatus.COMPLETED and manifest.finished_at is None:
        raise RuntimeError("Completed Layer 1 manifest must include finished_at")
