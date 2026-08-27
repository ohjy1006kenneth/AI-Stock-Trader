from __future__ import annotations

import json
import subprocess
from datetime import datetime
from pathlib import Path

import pytest

from app.pi.run_layer0_layer1_refresh import (
    CommandResult,
    PipelineError,
    RefreshConfig,
    _commands,
    _sanitize_output,
    acquire_lock,
    build_plan,
    latest_target,
    load_env,
    main,
    ready_reports,
    refresh,
    release_lock,
    run_command,
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
    manifest_key = "manifests/layer1/x.json"
    objects = {
        key: json.dumps({"run_id": "x", "from_date": "2026-06-18", "to_date": "2026-06-18", "ready_for_layer2": True, "manifest_key": manifest_key}).encode(),
        manifest_key: json.dumps({"run_id": "x", "stage": "layer1", "status": "completed", "metadata": {"requested_tickers": [], "processed_dates": ["2026-06-18"]}}).encode(),
    }
    client = FakeR2(objects)
    assert set(ready_reports(client)) == {"2026-06-18"}
    false = FakeR2({key: b'{"ready_for_layer2": false}'})
    assert ready_reports(false) == {}


def test_ready_reports_expands_only_exact_full_universe_manifest() -> None:
    report_key = "artifacts/reports/integration/layer1_archive_validation_range_2026-05-07_to_2026-05-22.json"
    manifest_key = "manifests/layer1/range.json"
    dates = ["2026-05-07", "2026-05-08", "2026-05-11", "2026-05-12", "2026-05-13", "2026-05-14", "2026-05-15", "2026-05-18", "2026-05-19", "2026-05-20", "2026-05-21", "2026-05-22"]
    client = FakeR2({
        report_key: json.dumps({"run_id": "range", "from_date": "2026-05-07", "to_date": "2026-05-22", "ready_for_layer2": True, "manifest_key": manifest_key}).encode(),
        manifest_key: json.dumps({"run_id": "range", "stage": "layer1", "status": "completed", "metadata": {"requested_tickers": [], "processed_dates": dates}}).encode(),
    })
    assert set(ready_reports(client)) == set(dates)

    client.objects[manifest_key] = json.dumps({"run_id": "range", "stage": "layer1", "status": "completed", "metadata": {"requested_tickers": ["AAPL"], "processed_dates": dates}}).encode()
    assert ready_reports(client) == {}


@pytest.mark.parametrize("manifest", [
    None,
    {"run_id": "other", "stage": "layer1", "status": "completed", "metadata": {"requested_tickers": [], "processed_dates": ["2026-06-18"]}},
    {"run_id": "x", "stage": "layer1", "status": "failed", "metadata": {"requested_tickers": [], "processed_dates": ["2026-06-18"]}},
    {"run_id": "x", "stage": "layer1", "status": "completed", "metadata": {"requested_tickers": [], "processed_dates": ["2026-06-18", "2026-06-18"]}},
])
def test_ready_reports_rejects_unproven_manifest_without_raising(manifest: dict[str, object] | None) -> None:
    report_key = "artifacts/reports/integration/layer1_archive_validation_x_2026-06-18_to_2026-06-18.json"
    manifest_key = "manifests/layer1/x.json"
    objects = {report_key: json.dumps({"run_id": "x", "from_date": "2026-06-18", "to_date": "2026-06-18", "ready_for_layer2": True, "manifest_key": manifest_key}).encode()}
    if manifest is not None:
        objects[manifest_key] = json.dumps(manifest).encode()
    assert ready_reports(FakeR2(objects)) == {}


def test_load_env_is_injected_and_does_not_log_values(tmp_path: Path) -> None:
    env_file = tmp_path / "r2.env"
    env_file.write_text("SECRET_TOKEN='not-logged'\n# comment\n")
    config = RefreshConfig(repo_root=tmp_path, home=tmp_path, env_files=(env_file,))
    assert load_env(config)["SECRET_TOKEN"] == "not-logged"
    assert load_env(config)["HOME"] == str(tmp_path)


def test_sanitizer_redacts_arbitrary_uri_userinfo_and_token_aware_env_values() -> None:
    env = {
        "R2_TOKEN": "FAKE-r2-secret-12345",
        "AWS_SECRET_ACCESS_KEY": "FAKE-aws-access-12345",
        "API_KEY": "FAKE-api-key-12345",
        "X-AMZ-SIGNATURE": "FAKE-signature-12345",
        "MONKEY": "banana",
    }
    value = (
        "s3://user:password@bucket/path?token=FAKE-r2-secret-12345&monkey=banana "
        "AWS_SECRET_ACCESS_KEY=FAKE-aws-access-12345 API_KEY=FAKE-api-key-12345 "
        "X-AMZ-SIGNATURE=FAKE-signature-12345 monkey=banana"
    )

    sanitized = _sanitize_output(value, env)

    assert sanitized == (
        "s3://<redacted>@bucket/path?token=<redacted>&monkey=banana "
        "AWS_SECRET_ACCESS_KEY=<redacted> API_KEY=<redacted> "
        "X-AMZ-SIGNATURE=<redacted> monkey=banana"
    )


def test_sanitizer_retains_http_uri_structure() -> None:
    value = "https://user:password@host:443/path?x=1#fragment"

    assert _sanitize_output(value, {}) == "https://<redacted>@host:443/path?x=1#fragment"


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


def test_verify_ready_requires_exact_identity() -> None:
    key = "artifacts/reports/integration/layer1_archive_validation_layer1-daily-2026-06-18_2026-06-18_to_2026-06-18.json"
    client = FakeR2({key: json.dumps({"ready_for_layer2": True}).encode()})
    with pytest.raises(PipelineError, match="not ready"):
        verify_ready(client, "2026-06-18")
    client.objects[key] = json.dumps({"run_id": "layer1-daily-2026-06-18", "from_date": "2026-06-18", "to_date": "2026-06-18", "ready_for_layer2": True}).encode()
    assert verify_ready(client, "2026-06-18")["ready_for_layer2"] is True


def test_timeout_overrides_require_positive_integers(monkeypatch: pytest.MonkeyPatch) -> None:
    from app.pi.run_layer0_layer1_refresh import _timeouts
    config = RefreshConfig()
    assert _timeouts(config) == (7200, 21600, 1800)
    monkeypatch.setenv("AI_STOCK_TRADER_LAYER0_COMMAND_TIMEOUT_SECONDS", "11")
    monkeypatch.setenv("AI_STOCK_TRADER_LAYER1_COMMAND_TIMEOUT_SECONDS", "22")
    monkeypatch.setenv("AI_STOCK_TRADER_VALIDATE_COMMAND_TIMEOUT_SECONDS", "33")
    assert _timeouts(config) == (11, 22, 33)
    monkeypatch.setenv("AI_STOCK_TRADER_LAYER0_COMMAND_TIMEOUT_SECONDS", "0")
    with pytest.raises(PipelineError, match="timeout"):
        _timeouts(config)


def test_refresh_stops_after_first_failed_stage(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    client = FakeR2()
    calls: list[list[str]] = []

    def fake_run(command: list[str], *args: object, **kwargs: object) -> CommandResult:
        calls.append(command)
        return CommandResult(command, 9)

    monkeypatch.setattr("app.pi.run_layer0_layer1_refresh.run_command", fake_run)
    args = type("Args", (), {"target_date": "2026-06-18", "from_date": "2026-06-18", "max_days": 1, "dry_run": False})()
    with pytest.raises(PipelineError, match="failed"):
        refresh(args, RefreshConfig(repo_root=tmp_path, home=tmp_path), lambda _env: client)
    assert len(calls) == 1


def _ready_objects(days: list[str]) -> dict[str, bytes]:
    objects: dict[str, bytes] = {}
    for day in days:
        report_key = f"artifacts/reports/integration/layer1_archive_validation_x_{day}_to_{day}.json"
        manifest_key = f"manifests/layer1/x-{day}.json"
        objects[report_key] = json.dumps(
            {
                "run_id": "x",
                "from_date": day,
                "to_date": day,
                "ready_for_layer2": True,
                "manifest_key": manifest_key,
            }
        ).encode()
        objects[manifest_key] = json.dumps(
            {
                "run_id": "x",
                "stage": "layer1",
                "status": "completed",
                "metadata": {"requested_tickers": [], "processed_dates": [day]},
            }
        ).encode()
    return objects


def test_refresh_default_history_crosses_holiday_and_weekend(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    client = FakeR2(_ready_objects(["2026-06-18", "2026-06-22", "2026-06-23"]))
    calls: list[list[str]] = []
    monkeypatch.setattr(
        "app.pi.run_layer0_layer1_refresh.run_command",
        lambda command, *args, **kwargs: (calls.append(command) or CommandResult(command, 0)),
    )
    monkeypatch.setattr("app.pi.run_layer0_layer1_refresh.verify_ready", lambda *_args: {})
    args = type("Args", (), {"target_date": "2026-06-24", "from_date": None, "max_days": 0, "dry_run": False})()
    assert refresh(args, RefreshConfig(repo_root=tmp_path, home=tmp_path), lambda _env: client) == 0
    assert [command[5] for command in calls[::3]] == ["2026-06-24"]


def test_refresh_default_history_gap_fails_before_log_or_subprocess(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    client = FakeR2(_ready_objects(["2026-06-18", "2026-06-23"]))
    calls: list[list[str]] = []
    monkeypatch.setattr(
        "app.pi.run_layer0_layer1_refresh.run_command",
        lambda command, *args, **kwargs: (calls.append(command) or CommandResult(command, 0)),
    )
    args = type("Args", (), {"target_date": "2026-06-24", "from_date": None, "max_days": 0, "dry_run": False})()
    with pytest.raises(PipelineError, match="2026-06-22"):
        refresh(args, RefreshConfig(repo_root=tmp_path, home=tmp_path), lambda _env: client)
    assert calls == []
    assert not (tmp_path / ".hermes").exists()


def test_refresh_explicit_from_date_selects_gap_oldest_first(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    client = FakeR2(_ready_objects(["2026-06-18", "2026-06-23"]))
    calls: list[list[str]] = []
    monkeypatch.setattr(
        "app.pi.run_layer0_layer1_refresh.run_command",
        lambda command, *args, **kwargs: (calls.append(command) or CommandResult(command, 0)),
    )
    monkeypatch.setattr("app.pi.run_layer0_layer1_refresh.verify_ready", lambda *_args: {})
    args = type("Args", (), {"target_date": "2026-06-24", "from_date": "2026-06-18", "max_days": 1, "dry_run": False})()
    assert refresh(args, RefreshConfig(repo_root=tmp_path, home=tmp_path), lambda _env: client) == 0
    assert calls[0][3] == "2026-06-22"


def test_run_command_redacts_normal_output_and_command_display(tmp_path: Path) -> None:
    secret = "provider-secret-value"
    env = {"R2_TOKEN": secret}
    command = ["https://user:password@host/path?token=" + secret + "&monkey=banana"]
    output = "https://user:password@host/path?api_key=" + secret + "&monkey=banana env=" + secret
    original = subprocess.run
    try:
        subprocess.run = lambda *args, **kwargs: type("Result", (), {"stdout": output, "returncode": 0})()  # type: ignore[method-assign]
        result = run_command(command, env, RefreshConfig(repo_root=tmp_path, home=tmp_path), tmp_path / "run.log", 10)
    finally:
        subprocess.run = original
    content = (tmp_path / "run.log").read_text()
    assert result.command == command
    assert secret not in content and secret not in result.output_tail
    assert "monkey=banana" in content and "<redacted>" in content


def test_run_command_redacts_s3_userinfo_and_query_credentials_in_log_and_tail(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    secret = "FAKE-r2-secret-12345"
    uri = f"s3://user:password@bucket/path?token={secret}&monkey=banana"
    command = ["fake-refresh", uri]
    monkeypatch.setattr(
        "app.pi.run_layer0_layer1_refresh.subprocess.run",
        lambda *args, **kwargs: type("Result", (), {"stdout": uri, "returncode": 0})(),
    )

    result = run_command(
        command,
        {"R2_TOKEN": secret},
        RefreshConfig(repo_root=tmp_path, home=tmp_path),
        tmp_path / "run.log",
        10,
    )

    content = (tmp_path / "run.log").read_text()
    expected = "s3://<redacted>@bucket/path?token=<redacted>&monkey=banana"
    assert result.command == command
    assert result.returncode == 0
    assert expected in content and expected in result.output_tail
    assert "user:password" not in content and "user:password" not in result.output_tail
    assert secret not in content and secret not in result.output_tail
    assert "monkey=banana" in content and "monkey=banana" in result.output_tail


@pytest.mark.parametrize("payload", ["timeout-secret", b"timeout-secret"])
def test_run_command_redacts_timeout_output_str_and_bytes(tmp_path: Path, payload: str | bytes, monkeypatch: pytest.MonkeyPatch) -> None:
    secret = "timeout-secret"
    monkeypatch.setattr(
        "app.pi.run_layer0_layer1_refresh.subprocess.run",
        lambda *args, **kwargs: (_ for _ in ()).throw(subprocess.TimeoutExpired(["cmd"], 10, output=payload)),
    )
    result = run_command(["cmd"], {"TOKEN": secret}, RefreshConfig(repo_root=tmp_path, home=tmp_path), tmp_path / "run.log", 10)
    content = (tmp_path / "run.log").read_text()
    assert result.returncode == 124
    assert secret not in content and secret not in result.output_tail
    assert "command timed out" in result.output_tail


def test_main_dry_run_does_not_create_lock_or_construct_client(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr("app.pi.run_layer0_layer1_refresh.RefreshConfig", lambda **kwargs: RefreshConfig(repo_root=tmp_path, home=tmp_path, **kwargs))
    monkeypatch.setattr("app.pi.run_layer0_layer1_refresh.install_signal_handlers", lambda _config: None)
    assert main(["--from-date", "2026-06-18", "--target-date", "2026-06-18", "--dry-run"]) == 0
    assert not (tmp_path / ".hermes").exists()
