from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from app.pi.network import oracle_client
from app.pi.network.oracle_client import (
    CommandResult,
    Layer1TriggerConfig,
    Layer1WaitConfig,
    load_pi_modal_runtime_config,
    trigger_layer1_feature_generation,
    wait_for_layer1_manifest,
)
from core.contracts.schemas import PipelineManifestRecord, RunStatus
from services.r2.paths import pipeline_manifest_path


class _Reader:
    def __init__(self, payloads: dict[str, bytes]) -> None:
        self.payloads = payloads

    def exists(self, key: str) -> bool:
        return key in self.payloads

    def get_object(self, key: str) -> bytes:
        return self.payloads[key]


class _Runner:
    def __init__(self, result: CommandResult) -> None:
        self.result = result
        self.commands: list[tuple[tuple[str, ...], Path]] = []

    def run(self, command, *, cwd: Path) -> CommandResult:
        self.commands.append((tuple(command), cwd))
        return self.result


def test_trigger_layer1_feature_generation_builds_modal_command() -> None:
    """The Pi dispatch uses `python -m modal run` against the daily Layer 1 script."""
    runner = _Runner(CommandResult(returncode=0, stdout="ok"))

    result = trigger_layer1_feature_generation(
        Layer1TriggerConfig(
            run_id="layer1-daily-2026-04-06",
            as_of_date="2026-04-06",
            layer0_run_id="layer0-daily-2026-04-06",
        ),
        command_runner=runner,
    )

    assert result.manifest_key == pipeline_manifest_path("layer1", "layer1-daily-2026-04-06")
    assert runner.commands == [
        (
            (
                "python",
                "-m",
                "modal",
                "run",
                "app/lab/data_pipelines/run_daily_layer1.py",
                "--run-id",
                "layer1-daily-2026-04-06",
                "--as-of-date",
                "2026-04-06",
                "--layer0-run-id",
                "layer0-daily-2026-04-06",
                "--benchmark-ticker",
                "SPY",
            ),
            oracle_client._REPO_ROOT,
        )
    ]


def test_trigger_layer1_feature_generation_raises_on_failed_command() -> None:
    """A non-zero Modal CLI exit fails closed before downstream stages continue."""
    runner = _Runner(CommandResult(returncode=1, stderr="auth failed"))

    with pytest.raises(RuntimeError, match="Modal Layer 1 dispatch failed"):
        trigger_layer1_feature_generation(
            Layer1TriggerConfig(
                run_id="layer1-daily-2026-04-06",
                as_of_date="2026-04-06",
                layer0_run_id="layer0-daily-2026-04-06",
            ),
            command_runner=runner,
        )


def test_wait_for_layer1_manifest_returns_completed_manifest() -> None:
    """Polling returns the completed manifest when metadata matches the requested run."""
    key = pipeline_manifest_path("layer1", "layer1-daily-2026-04-06")
    reader = _Reader(
        {
            key: _manifest_payload(
                run_id="layer1-daily-2026-04-06",
                status=RunStatus.COMPLETED,
                metadata={
                    "as_of_date": "2026-04-06",
                    "layer0_run_id": "layer0-daily-2026-04-06",
                },
            )
        }
    )

    manifest = wait_for_layer1_manifest(
        Layer1WaitConfig(
            run_id="layer1-daily-2026-04-06",
            as_of_date="2026-04-06",
            layer0_run_id="layer0-daily-2026-04-06",
            poll_interval_seconds=1,
            poll_timeout_seconds=5,
        ),
        reader=reader,
        monotonic=lambda: 0.0,
        sleep=lambda _: None,
    )

    assert manifest.status == RunStatus.COMPLETED
    assert manifest.output_path == "features/layer1"


def test_wait_for_layer1_manifest_rejects_stale_manifest() -> None:
    """Completed manifests with mismatched upstream metadata fail closed as stale."""
    key = pipeline_manifest_path("layer1", "layer1-daily-2026-04-06")
    reader = _Reader(
        {
            key: _manifest_payload(
                run_id="layer1-daily-2026-04-06",
                status=RunStatus.COMPLETED,
                metadata={
                    "as_of_date": "2026-04-05",
                    "layer0_run_id": "layer0-daily-2026-04-06",
                },
            )
        }
    )

    with pytest.raises(RuntimeError, match="stale"):
        wait_for_layer1_manifest(
            Layer1WaitConfig(
                run_id="layer1-daily-2026-04-06",
                as_of_date="2026-04-06",
                layer0_run_id="layer0-daily-2026-04-06",
                poll_interval_seconds=1,
                poll_timeout_seconds=5,
            ),
            reader=reader,
            monotonic=lambda: 0.0,
            sleep=lambda _: None,
        )


@pytest.mark.parametrize("status", [RunStatus.FAILED, RunStatus.BLOCKED])
def test_wait_for_layer1_manifest_raises_on_failed_or_blocked_manifest(
    status: RunStatus,
) -> None:
    """Failed or blocked Layer 1 manifests stop the Pi runtime before inference."""
    key = pipeline_manifest_path("layer1", "layer1-daily-2026-04-06")
    reader = _Reader(
        {
            key: _manifest_payload(
                run_id="layer1-daily-2026-04-06",
                status=status,
                metadata={
                    "as_of_date": "2026-04-06",
                    "layer0_run_id": "layer0-daily-2026-04-06",
                },
            )
        }
    )

    with pytest.raises(RuntimeError, match="indicates"):
        wait_for_layer1_manifest(
            Layer1WaitConfig(
                run_id="layer1-daily-2026-04-06",
                as_of_date="2026-04-06",
                layer0_run_id="layer0-daily-2026-04-06",
                poll_interval_seconds=1,
                poll_timeout_seconds=5,
            ),
            reader=reader,
            monotonic=lambda: 0.0,
            sleep=lambda _: None,
        )


def test_wait_for_layer1_manifest_times_out_when_missing() -> None:
    """Polling raises TimeoutError when the expected manifest never appears."""
    moments = [0.0, 10.0]

    def _monotonic() -> float:
        return moments.pop(0) if moments else 10.0

    with pytest.raises(TimeoutError, match="Timed out"):
        wait_for_layer1_manifest(
            Layer1WaitConfig(
                run_id="layer1-daily-2026-04-06",
                as_of_date="2026-04-06",
                layer0_run_id="layer0-daily-2026-04-06",
                poll_interval_seconds=1,
                poll_timeout_seconds=5,
            ),
            reader=_Reader({}),
            monotonic=_monotonic,
            sleep=lambda _: None,
        )


def test_load_pi_modal_runtime_config_reads_repo_config() -> None:
    """Pi polling settings live in repository config rather than code constants."""
    config = load_pi_modal_runtime_config()

    assert config.layer1_poll_interval_seconds > 0
    assert config.layer1_poll_timeout_seconds > 0
    config_payload = json.loads((oracle_client._REPO_ROOT / "config" / "modal.json").read_text())
    assert config.layer1_poll_timeout_seconds >= int(config_payload["timeout_seconds"])


def test_load_pi_modal_runtime_config_rejects_poll_timeout_shorter_than_modal_timeout(
    tmp_path: Path,
) -> None:
    """Pi startup fails closed when polling would expire before the Modal job deadline."""
    config_path = tmp_path / "modal.json"
    config_path.write_text(
        json.dumps(
            {
                "layer1_daily_app_name": "ai-stock-trader-layer1-daily",
                "hmm_regime_app_name": "ai-stock-trader-hmm-regime-detection",
                "r2_secret_name": "ai-stock-trader-r2",
                "timeout_seconds": 1800,
                "layer1_poll_interval_seconds": 5,
                "layer1_poll_timeout_seconds": 900,
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(
        ValueError,
        match="layer1_poll_timeout_seconds must be greater than or equal to timeout_seconds",
    ):
        load_pi_modal_runtime_config(config_path)


def _manifest_payload(
    *,
    run_id: str,
    status: RunStatus,
    metadata: dict[str, object],
) -> bytes:
    """Build one serialized Layer 1 manifest payload for polling tests."""
    manifest = PipelineManifestRecord(
        run_id=run_id,
        stage="layer1",
        status=status,
        started_at=datetime(2026, 4, 6, tzinfo=UTC),
        finished_at=datetime(2026, 4, 6, 0, 0, 1, tzinfo=UTC),
        output_path="features/layer1",
        metadata=metadata,
    )
    return manifest.model_dump_json().encode("utf-8")
