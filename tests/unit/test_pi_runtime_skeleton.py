from __future__ import annotations

from pathlib import Path
import sys


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.pi.main import run_main


EXPECTED_STAGES = [
    "pull_market_and_news",
    "run_cloud_inference",
    "construct_portfolio_targets",
    "apply_hard_risk_controls",
    "persist_approved_proposal",
    "reconcile_account_state",
    "translate_targets_to_orders",
    "execute_and_monitor",
    "persist_execution_logs",
    "send_run_summary",
]


def test_pi_runtime_dry_run_stage_order() -> None:
    """Dry-run should produce all stages in canonical order."""
    manifests = run_main("2026-04-06")
    assert [row["stage"] for row in manifests] == EXPECTED_STAGES
    assert all(row["status"] == "completed" for row in manifests)


def test_pi_runtime_includes_container_runtime_metadata() -> None:
    """Every stage manifest should expose Docker/OpenClaw/cron runtime metadata."""
    manifests = run_main("2026-04-06")
    assert all(row["runtime_context"]["runtime_engine"] == "openclaw" for row in manifests)
    assert all(row["runtime_context"]["execution_environment"] == "docker" for row in manifests)
    assert all(row["runtime_context"]["scheduler"] == "cron" for row in manifests)


def test_pi_runtime_dry_run_is_deterministic() -> None:
    """Running dry-run with the same date should produce identical manifests."""
    first = run_main("2026-04-06")
    second = run_main("2026-04-06")
    assert first == second
