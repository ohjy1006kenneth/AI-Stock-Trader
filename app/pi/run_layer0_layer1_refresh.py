"""Holiday-aware, data-only Layer 0 to Layer 1 refresh for the Pi runtime."""
from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import signal
import subprocess
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Protocol
from zoneinfo import ZoneInfo

from loguru import logger

from core.common.trading_calendar import (
    is_us_equity_trading_session,
    previous_trading_day,
    trading_dates,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
REPORT_RE = re.compile(
    r"layer1_archive_validation_(?P<run_id>.+)_(?P<from>\d{4}-\d{2}-\d{2})_to_"
    r"(?P<to>\d{4}-\d{2}-\d{2})\.json$"
)
LOCK_STALE_AFTER = timedelta(hours=6)
ENV_NAMES = ("r2.env", "alpaca.env", "fred.env", "simfin.env")
TIMEOUT_ENV_NAMES = (
    "AI_STOCK_TRADER_LAYER0_COMMAND_TIMEOUT_SECONDS",
    "AI_STOCK_TRADER_LAYER1_COMMAND_TIMEOUT_SECONDS",
    "AI_STOCK_TRADER_VALIDATE_COMMAND_TIMEOUT_SECONDS",
)
_CREDENTIAL_QUERY_RE = re.compile(
    r"(?i)([?&](?:token|api[-_]key|apikey|access[-_]key(?:[-_]id)?|secret(?:[-_]access[-_]key)?|"
    r"password|passwd|credential|signature|x-amz-credential|x-amz-signature|"
    r"x-amz-security-token)=)[^&#\s]+"
)
_URL_USERINFO_RE = re.compile(r"(?i)([a-z][a-z0-9+.-]*://)([^/@\s?#]+)@")
_SENSITIVE_ENV_RE = re.compile(
    r"(?i)(?:^|[_-])(?:TOKEN|KEY|SECRET|PASSWORD|PASSWD|CREDENTIAL|SIGNATURE)(?:$|[_-])"
)


class PipelineError(RuntimeError):
    """Raised when a refresh cannot safely complete."""


class R2Client(Protocol):
    """Small R2 surface required by this orchestrator."""

    def list_keys(self, prefix: str) -> list[str]: ...
    def get_object(self, key: str) -> bytes: ...
    def exists(self, key: str) -> bool: ...


@dataclass(frozen=True)
class RefreshConfig:
    """Filesystem, timeout, and environment configuration for one refresh."""

    repo_root: Path = REPO_ROOT
    home: Path = field(default_factory=Path.home)
    log_dir: Path | None = None
    lock_dir: Path | None = None
    env_files: tuple[Path, ...] | None = None
    max_days: int = 30
    layer0_timeout: int = 7200
    layer1_timeout: int = 21600
    validator_timeout: int = 1800

    def __post_init__(self) -> None:
        if self.log_dir is None:
            object.__setattr__(self, "log_dir", self.home / ".hermes" / "profiles" / "trading" / "logs" / "ai-stock-trader-data-pipeline")
        if self.lock_dir is None:
            object.__setattr__(self, "lock_dir", self.home / ".hermes" / "profiles" / "trading" / "state" / "ai-stock-trader-layer0-layer1-daily.lock")
        if self.env_files is None:
            object.__setattr__(self, "env_files", tuple(self.repo_root / "config" / name for name in ENV_NAMES))

    @property
    def effective_log_dir(self) -> Path:
        """Return the configured log directory."""
        assert self.log_dir is not None
        return self.log_dir

    @property
    def effective_lock_dir(self) -> Path:
        """Return the configured lock directory."""
        assert self.lock_dir is not None
        return self.lock_dir


@dataclass(frozen=True)
class CommandResult:
    """Result of one bounded subprocess invocation."""

    command: list[str]
    returncode: int
    output_tail: str = ""


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """Parse the stable refresh CLI options."""
    parser = argparse.ArgumentParser(description="Refresh Layer 0 and Layer 1 data.")
    parser.add_argument("--target-date")
    parser.add_argument("--from-date")
    parser.add_argument("--max-days", type=int, default=int(os.getenv("AI_STOCK_TRADER_MAX_DAILY_CATCHUP_DAYS", "30")))
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args(argv)


def load_env(config: RefreshConfig) -> dict[str, str]:
    """Load repository env files without logging their values."""
    env = os.environ.copy()
    env["HOME"] = str(config.home)
    env["PYTHONPATH"] = str(config.repo_root) + (os.pathsep + env["PYTHONPATH"] if env.get("PYTHONPATH") else "")
    env["AI_STOCK_TRADER_REPO_ROOT"] = str(config.repo_root)
    for path in config.env_files or ():
        if not path.exists():
            continue
        for raw in path.read_text(encoding="utf-8").splitlines():
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            if key:
                env[key] = value.strip().strip('"').strip("'")
    return env


def _pid_is_active(pid: int) -> bool:
    """Return whether a process id is live and not a zombie."""
    try:
        stat = Path(f"/proc/{pid}/stat")
        if stat.exists() and len(stat.read_text().split()) >= 3 and stat.read_text().split()[2] == "Z":
            return False
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _lock_detail(lock_dir: Path) -> str:
    """Read lock metadata for diagnostics without exposing environment values."""
    info = lock_dir / "info.txt"
    return info.read_text(encoding="utf-8") if info.exists() else "no lock info"


def _lock_pid(detail: str) -> int | None:
    """Extract a recorded PID from lock metadata."""
    for line in detail.splitlines():
        if line.startswith("pid="):
            try:
                return int(line.split("=", 1)[1])
            except ValueError:
                return None
    return None


def _timeouts(config: RefreshConfig) -> tuple[int, int, int]:
    """Resolve positive command timeout overrides without exposing values."""
    defaults = (config.layer0_timeout, config.layer1_timeout, config.validator_timeout)
    resolved: list[int] = []
    for name, default in zip(TIMEOUT_ENV_NAMES, defaults):
        raw = os.getenv(name)
        if raw is None:
            resolved.append(default)
            continue
        try:
            value = int(raw)
        except ValueError as exc:
            raise PipelineError(f"invalid timeout configuration for {name}") from exc
        if value <= 0:
            raise PipelineError(f"invalid timeout configuration for {name}")
        resolved.append(value)
    return (resolved[0], resolved[1], resolved[2])


def acquire_lock(config: RefreshConfig) -> None:
    """Acquire the stale-aware single-run lock."""
    lock_dir = config.effective_lock_dir
    lock_dir.parent.mkdir(parents=True, exist_ok=True)
    while True:
        try:
            lock_dir.mkdir()
            (lock_dir / "info.txt").write_text(
                f"owner=os-cron\nstarted_at={datetime.now().isoformat()}\npid={os.getpid()}\n",
                encoding="utf-8",
            )
            return
        except FileExistsError as exc:
            detail = _lock_detail(lock_dir)
            pid = _lock_pid(detail)
            if pid is None:
                try:
                    age = datetime.now() - datetime.fromtimestamp(lock_dir.stat().st_mtime)
                except OSError as stat_exc:
                    raise PipelineError("refresh lock metadata is malformed or unsafe") from stat_exc
                if age <= LOCK_STALE_AFTER:
                    raise PipelineError("another refresh is already running (lock metadata is incomplete)") from exc
                raise PipelineError("refresh lock metadata is malformed or unsafe") from exc
            stale = not _pid_is_active(pid)
            reason = f"recorded pid {pid} is not active" if stale else f"recorded pid {pid} is still active"
            if not stale:
                raise PipelineError(f"another refresh is already running ({reason})") from exc
            try:
                shutil.rmtree(lock_dir)
            except OSError as cleanup_exc:
                logger.error("stale refresh lock cleanup failed: {}", type(cleanup_exc).__name__)
                raise PipelineError("stale refresh lock cleanup failed") from cleanup_exc
            logger.warning("removed stale refresh lock: {}", reason)


def release_lock(config: RefreshConfig) -> None:
    """Release the run lock, failing when cleanup cannot be confirmed."""
    lock_dir = config.effective_lock_dir
    try:
        shutil.rmtree(lock_dir)
    except FileNotFoundError:
        return
    except OSError as exc:
        logger.error("refresh lock cleanup failed: {}", type(exc).__name__)
        raise PipelineError("refresh lock cleanup failed") from exc


def install_signal_handlers(config: RefreshConfig) -> None:
    """Ensure termination signals release the lock before exiting."""
    def handle(signum: int, _frame: object) -> None:
        release_lock(config)
        raise SystemExit(128 + signum)
    signal.signal(signal.SIGTERM, handle)
    signal.signal(signal.SIGINT, handle)


def latest_target(now: datetime | None = None) -> str:
    """Return the latest eligible regular session in New York time."""
    current = (now or datetime.now(ZoneInfo("America/New_York"))).astimezone(ZoneInfo("America/New_York"))
    today = current.date()
    if current.hour < 18 or not is_us_equity_trading_session(today):
        return previous_trading_day(today.isoformat())
    return today.isoformat()


def ready_reports(client: R2Client) -> dict[str, dict[str, Any]]:
    """Return only durable validation reports explicitly ready for Layer 2."""
    reports: dict[str, dict[str, Any]] = {}
    for key in client.list_keys("artifacts/reports/integration/"):
        match = REPORT_RE.search(Path(key).name)
        if match is None:
            continue
        try:
            payload = json.loads(client.get_object(key).decode("utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError, TypeError):
            continue
        if (
            not isinstance(payload, dict)
            or payload.get("ready_for_layer2") is not True
            or payload.get("run_id") != match.group("run_id")
            or payload.get("from_date") != match.group("from")
            or payload.get("to_date") != match.group("to")
        ):
            continue
        target = match.group("to")
        if target not in reports or str(payload.get("manifest_finished_at", "")) > str(reports[target].get("manifest_finished_at", "")):
            payload["report_key"] = key
            reports[target] = payload
    return reports


def build_plan(start: str, target: str, ready: set[str], max_days: int) -> tuple[list[str], list[str], list[str]]:
    """Build valid sessions, skipped requested dates, and max-day remainder."""
    if date.fromisoformat(start) > date.fromisoformat(target):
        return [], [], []
    all_dates = trading_dates(start, target)
    cursor = date.fromisoformat(start)
    end = date.fromisoformat(target)
    skipped: list[str] = []
    while cursor <= end:
        if not is_us_equity_trading_session(cursor):
            skipped.append(cursor.isoformat())
        cursor += timedelta(days=1)
    missing = [day for day in all_dates if day not in ready]
    selected = missing if max_days <= 0 else missing[:max_days]
    return selected, skipped, missing[len(selected):]


def _sanitize_output(value: str, env: dict[str, str]) -> str:
    """Redact URL credentials and inherited credential values from child output."""
    sanitized = _URL_USERINFO_RE.sub(r"\1<redacted>@", value)
    sanitized = _CREDENTIAL_QUERY_RE.sub(r"\1<redacted>", sanitized)
    secrets = {raw for name, raw in env.items() if _SENSITIVE_ENV_RE.search(name) and len(raw) >= 4}
    for secret in sorted(secrets, key=len, reverse=True):
        sanitized = sanitized.replace(secret, "<redacted>")
    return sanitized


def run_command(command: list[str], env: dict[str, str], config: RefreshConfig, log_path: Path, timeout: int) -> CommandResult:
    """Run one command with a watchdog and append output to the run log."""
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("a", encoding="utf-8") as log:
        log.write("\n$ " + _sanitize_output(" ".join(command), env) + f"\n[timeout_seconds={timeout}]\n")
        try:
            proc = subprocess.run(command, cwd=config.repo_root, env=env, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=timeout)
            output = proc.stdout or ""
            code = proc.returncode
        except subprocess.TimeoutExpired as exc:
            raw_output = exc.stdout or ""
            output = raw_output.decode(errors="replace") if isinstance(raw_output, bytes) else raw_output
            output += f"\ncommand timed out after {timeout} seconds\n"
            code = 124
        output = _sanitize_output(output, env)
        log.write(output)
        log.write(f"\n[exit_code={code}]\n")
    return CommandResult(command, code, "\n".join(output.splitlines()[-40:]))


def _commands(config: RefreshConfig, day: str) -> tuple[list[str], list[str], list[str]]:
    """Construct the exact three commands for one session."""
    layer0 = f"layer0-daily-{day}"
    layer1 = f"layer1-daily-{day}"
    python = str(config.repo_root / ".venv/bin/python")
    modal = str(config.repo_root / ".venv/bin/modal")
    return (
        [python, "app/lab/data_pipelines/backfill_layer0.py", "--from-date", day, "--to-date", day, "--run-id", layer0],
        [modal, "run", "app/lab/data_pipelines/run_daily_layer1.py::app.modal_main", "--run-id", layer1, "--as-of-date", day, "--layer0-run-id", layer0],
        [python, "app/lab/data_pipelines/validate_layer1_archive.py", "--run-id", layer1, "--from-date", day, "--to-date", day, "--use-r2-universe"],
    )


def verify_ready(client: R2Client, day: str) -> dict[str, Any]:
    """Require the exact durable report for a completed session."""
    run_id = f"layer1-daily-{day}"
    key = f"artifacts/reports/integration/layer1_archive_validation_{run_id}_{day}_to_{day}.json"
    if not client.exists(key):
        raise PipelineError("missing final Layer 1 validation report")
    try:
        payload = json.loads(client.get_object(key).decode("utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError, TypeError) as exc:
        raise PipelineError("final Layer 1 validation report is unreadable") from exc
    if (
        not isinstance(payload, dict)
        or payload.get("ready_for_layer2") is not True
        or payload.get("run_id") != run_id
        or payload.get("from_date") != day
        or payload.get("to_date") != day
    ):
        raise PipelineError("final Layer 1 validation report is not ready")
    return payload


def refresh(args: argparse.Namespace, config: RefreshConfig, client_factory: Callable[[dict[str, str]], R2Client], now: datetime | None = None) -> int:
    """Execute or dry-run a bounded, fail-closed refresh plan."""
    timeouts = _timeouts(config)
    env = load_env(config)
    if args.dry_run:
        if args.from_date is None:
            raise PipelineError("dry-run requires explicit --from-date to avoid durable R2 discovery")
        target = args.target_date or latest_target(now)
        selected, skipped, remaining = build_plan(args.from_date, target, set(), args.max_days)
        logger.info("Layer 0->1 dry-run target={} skipped={} sessions={} remaining={}", target, skipped, selected, remaining)
        return 0
    client = client_factory(env)
    ready = ready_reports(client)
    target = args.target_date or latest_target(now)
    if args.from_date:
        start = args.from_date
    elif not ready:
        start = target
    else:
        ready_dates = sorted(ready)
        missing_frontier = sorted(set(trading_dates(ready_dates[0], ready_dates[-1])) - set(ready_dates))
        if missing_frontier:
            raise PipelineError(
                "default ready history is non-contiguous; missing regular sessions: "
                + ", ".join(missing_frontier)
            )
        start = ready_dates[-1]
        start = (date.fromisoformat(start) + timedelta(days=1)).isoformat()
    selected, skipped, remaining = build_plan(start, target, set(ready), args.max_days)
    logger.info("Layer 0->1 refresh target={} skipped={} sessions={} remaining={}", target, skipped, selected, remaining)
    if not selected:
        return 0
    log_path = config.effective_log_dir / f"refresh-{datetime.now().strftime('%Y%m%d-%H%M%S')}.log"
    for day in selected:
        for command, timeout in zip(_commands(config, day), timeouts):
            result = run_command(command, env, config, log_path, timeout)
            if result.returncode != 0:
                raise PipelineError(f"refresh command failed for {day} with exit code {result.returncode}")
        verify_ready(client_factory(env), day)
        logger.info("completed {} with ready_for_layer2=true", day)
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    """Run the CLI with lock and fail-closed error handling."""
    args = parse_args(argv)
    config = RefreshConfig(max_days=args.max_days)
    install_signal_handlers(config)
    if args.dry_run:
        return refresh(args, config, lambda _env: (_ for _ in ()).throw(PipelineError("dry-run R2 access")))
    acquire_lock(config)
    primary: BaseException | None = None
    try:
        from services.r2.client import CloudflareR2Client
        return refresh(args, config, lambda _env: CloudflareR2Client.from_env())
    except BaseException as exc:
        primary = exc
        raise
    finally:
        try:
            release_lock(config)
        except PipelineError:
            if primary is None:
                raise
            logger.error("refresh lock cleanup failed after primary failure")


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except PipelineError as exc:
        logger.error("Layer 0->1 refresh failed: {}", exc)
        raise SystemExit(1) from exc
