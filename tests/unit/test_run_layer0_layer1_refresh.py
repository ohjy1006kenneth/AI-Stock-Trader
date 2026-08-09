from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import pytest

from app.pi.run_layer0_layer1_refresh import (
    PipelineError,
    RefreshConfig,
    _commands,
    acquire_lock,
    build_plan,
    latest_target,
    load_env,
    ready_reports,
    refresh,
    release_lock,
    verify_ready,
)


class FakeR2:
    def __init__(self, objects: dict[str, bytes] | None = None) -> None:
        self.objects = objects or {}
        self.calls: list[str] = []

    def list_keys(self, prefix: str) -> list[str]:
        self.calls.append(f"list:{prefix}")
        return [key for key in self.objects if key.startswith(prefix)]

    def get_object(self, key: str) -> bytes:
        self.calls.append(f"get:{key}")
        return self.objects[key]

    def exists(self, key: str) -> bool:
        self.calls.append(f"exists:{key}")
        return key in self.objects


def test_latest_target_holiday_weekend_and_cutoff() -> None:
    assert latest_target(datetime.fromisoformat("2026-06-19T19:00:00-04:00")) == "2026-06-18"
    assert latest_target(datetime.fromisoformat("2026-06-20T12:00:00-04:00")) == "2026-06-18"
    assert latest_target(datetime.fromisoformat("2026-06-22T17:59:00-04:00")) == "2026-06-18"
    assert latest_target(datetime.fromisoformat("2026-06-22T18:00:00-04:00")) == "2026-06-22"


def test_build_plan_skips_holiday_weekend_filters_ready_and_caps() -> None:
    selected, skipped, remaining = build_plan("2026-06-18", "2026-06-22", set(), 1)
    assert selected == ["2026-06-18"]
    assert skipped == ["2026-06-19", "2026-06-20", "2026-06-21"]
    assert remaining == ["2026-06-22"]
    assert build_plan("2026-06-18", "2026-06-22", {"2026-06-18"}, 0)[0] == ["2026-06-22"]


def test_ready_reports_requires_boolean_true_and_uses_latest() -> None:
    key = "artifacts/reports/integration/layer1_archive_validation_x_2026-06-18_to_2026-06-18.json"
    client = FakeR2({key: json.dumps({"to_date": "2026-06-18", "ready_for_layer2": True}).encode()})
    assert set(ready_reports(client)) == {"2026-06-18"}
    false = FakeR2({key: b'{"ready_for_layer2": false}'})
    assert ready_reports(false) == {}


def test_load_env_is_injected_and_does_not_log_values(tmp_path: Path) -> None:
    env_file = tmp_path / "r2.env"
    env_file.write_text("SECRET_TOKEN='not-logged'\n# comment\n")
    config = RefreshConfig(repo_root=tmp_path, home=tmp_path, env_files=(env_file,))
    assert load_env(config)["SECRET_TOKEN"] == "not-logged"
    assert load_env(config)["HOME"] == str(tmp_path)


def test_lock_live_and_stale_behavior(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    config = RefreshConfig(repo_root=tmp_path, home=tmp_path)
    acquire_lock(config)
    with pytest.raises(PipelineError):
        acquire_lock(config)
    release_lock(config)
    lock_dir = config.effective_lock_dir
    lock_dir.mkdir(parents=True)
    (lock_dir / "info.txt").write_text("pid=999999\n")
    acquire_lock(config)
    release_lock(config)


def test_commands_preserve_exact_run_ids_and_paths(tmp_path: Path) -> None:
    commands = _commands(RefreshConfig(repo_root=tmp_path, home=tmp_path), "2026-06-18")
    assert commands[0][-6:] == ["--from-date", "2026-06-18", "--to-date", "2026-06-18", "--run-id", "layer0-daily-2026-06-18"]
    assert commands[1][0].endswith("/.venv/bin/modal")
    assert commands[1][2] == "app/lab/data_pipelines/run_daily_layer1.py::app.modal_main"
    assert commands[2][-7:] == ["--run-id", "layer1-daily-2026-06-18", "--from-date", "2026-06-18", "--to-date", "2026-06-18", "--use-r2-universe"]


def test_refresh_dry_run_has_zero_subprocess_or_r2_mutation(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    client = FakeR2()
    calls: list[list[str]] = []
    monkeypatch.setattr("app.pi.run_layer0_layer1_refresh.subprocess.run", lambda *a, **k: calls.append(a[0]))
    args = type("Args", (), {"target_date": "2026-06-22", "from_date": "2026-06-18", "max_days": 0, "dry_run": True})()
    assert refresh(args, RefreshConfig(repo_root=tmp_path, home=tmp_path), lambda _env: client) == 0
    assert calls == []
    assert all(not call.startswith("put") for call in client.calls)


def test_verify_ready_missing_or_false_fails() -> None:
    client = FakeR2()
    with pytest.raises(PipelineError, match="missing"):
        verify_ready(client, "2026-06-18")
    key = "artifacts/reports/integration/layer1_archive_validation_layer1-daily-2026-06-18_2026-06-18_to_2026-06-18.json"
    client.objects[key] = b'{"ready_for_layer2": false}'
    with pytest.raises(PipelineError, match="not ready"):
        verify_ready(client, "2026-06-18")
