from __future__ import annotations

from datetime import datetime, timedelta, timezone

from app.pi.execution.order_executor import execute_orders
from app.pi.fetchers.market_context import collect_market_context
from app.pi.network.oracle_client import request_predictions
from app.pi.reporting.run_summary import build_run_summary


RUNTIME_CONTEXT = {
    "runtime_engine": "openclaw",
    "execution_environment": "docker",
    "scheduler": "cron",
}


def _manifest(
    run_id: str,
    stage: str,
    index: int,
    metadata: dict[str, object] | None = None,
) -> dict[str, object]:
    """Create a deterministic stage manifest record for dry-run orchestration."""
    start = datetime(2026, 1, 1, tzinfo=timezone.utc) + timedelta(seconds=index * 10)
    end = start + timedelta(seconds=1)
    return {
        "run_id": run_id,
        "stage": stage,
        "status": "completed",
        "started_at": start.isoformat(),
        "finished_at": end.isoformat(),
        "runtime_context": dict(RUNTIME_CONTEXT),
        "metadata": metadata or {},
    }


def run_daily_dry_run(as_of_date: str) -> list[dict[str, object]]:
    """Execute a deterministic dry-run of the daily edge runtime flow."""
    run_id = f"dryrun-{as_of_date}"
    manifests: list[dict[str, object]] = []

    context = collect_market_context(as_of_date)
    manifests.append(_manifest(run_id, "pull_market_and_news", 0, {"tickers": 3}))

    predictions = request_predictions(context)
    manifests.append(_manifest(run_id, "run_cloud_inference", 1, {"scores": 3}))

    score_map = predictions["scores"]
    targets = [
        {"ticker": ticker, "target_dollars": round(score * 1000.0, 2)}
        for ticker, score in score_map.items()
    ]
    manifests.append(_manifest(run_id, "construct_portfolio_targets", 2, {"targets": len(targets)}))

    approved = [target for target in targets if target["target_dollars"] > 700.0]
    manifests.append(_manifest(run_id, "apply_hard_risk_controls", 3, {"approved": len(approved)}))

    manifests.append(_manifest(run_id, "persist_approved_proposal", 4, {"approved": len(approved)}))
    manifests.append(_manifest(run_id, "reconcile_account_state", 5))

    orders = [
        {"ticker": row["ticker"], "action": "BUY", "target_dollars": row["target_dollars"]}
        for row in approved
    ]
    manifests.append(_manifest(run_id, "translate_targets_to_orders", 6, {"orders": len(orders)}))

    execution_summary = execute_orders(orders)
    manifests.append(_manifest(run_id, "execute_and_monitor", 7, execution_summary))

    manifests.append(_manifest(run_id, "persist_execution_logs", 8, {"written": True}))

    summary_snapshot = manifests + [_manifest(run_id, "send_run_summary", 9)]
    summary = build_run_summary(summary_snapshot)
    manifests.append(_manifest(run_id, "send_run_summary", 9, summary))

    return manifests
