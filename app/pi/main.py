from __future__ import annotations

from app.pi.run_daily import run_daily_dry_run


def run_main(as_of_date: str) -> list[dict[str, object]]:
    """Run the OpenClaw edge entrypoint in deterministic Docker dry-run mode."""
    return run_daily_dry_run(as_of_date)
