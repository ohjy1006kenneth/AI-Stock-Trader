from __future__ import annotations

from app.pi.run_daily import run_daily_dry_run


def run_main(as_of_date: str) -> list[dict[str, object]]:
    """Run the edge orchestration entrypoint in deterministic dry-run mode."""
    return run_daily_dry_run(as_of_date)


def openclaw_entrypoint(as_of_date: str) -> list[dict[str, object]]:
    """Container runtime entrypoint representing the OpenClaw execution process."""
    return run_daily_dry_run(as_of_date)
