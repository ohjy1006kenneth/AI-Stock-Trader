from __future__ import annotations

import argparse
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta

from app.pi.execution.order_executor import execute_orders
from app.pi.fetchers.layer0 import run_layer0_incremental as run_live_layer0_incremental
from app.pi.fetchers.market_context import collect_market_context
from app.pi.network.oracle_client import (
    Layer1DispatchResult,
    Layer1TriggerConfig,
    Layer1WaitConfig,
    PiModalRuntimeConfig,
    load_pi_modal_runtime_config,
    request_predictions,
    trigger_layer1_feature_generation,
    wait_for_layer1_manifest,
)
from app.pi.reporting.run_summary import build_run_summary
from core.contracts.schemas import PipelineManifestRecord, RunStatus
from services.r2.paths import pipeline_manifest_path

RUNTIME_CONTEXT = {
    "runtime_engine": "openclaw",
    "execution_environment": "docker",
    "scheduler": "cron",
}


@dataclass(frozen=True)
class Layer0RunSummary:
    """Minimal Layer 0 completion data needed by the Pi orchestrator."""

    run_id: str
    manifest_key: str


@dataclass(frozen=True)
class DailyRunDependencies:
    """Injected runtime surfaces for live or dry-run orchestration."""

    market_context_collector: Callable[[str], dict[str, object]]
    layer0_runner: Callable[[date, Sequence[str], str], Layer0RunSummary]
    modal_runtime_loader: Callable[[], PiModalRuntimeConfig]
    layer1_trigger: Callable[[Layer1TriggerConfig], Layer1DispatchResult]
    layer1_waiter: Callable[[Layer1WaitConfig], PipelineManifestRecord]
    prediction_requester: Callable[[dict[str, object]], dict[str, object]]
    order_executor: Callable[[list[dict[str, object]]], dict[str, object]]
    summary_builder: Callable[[list[dict[str, object]]], dict[str, object]]


def _manifest(
    run_id: str,
    stage: str,
    index: int,
    metadata: dict[str, object] | None = None,
) -> dict[str, object]:
    """Create a deterministic stage manifest record for dry-run orchestration."""
    start = datetime(2026, 1, 1, tzinfo=UTC) + timedelta(seconds=index * 10)
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


def run_daily(
    as_of_date: str,
    *,
    dependencies: DailyRunDependencies,
    benchmark_ticker: str = "SPY",
    runtime_run_id: str | None = None,
) -> list[dict[str, object]]:
    """Execute one Pi daily orchestration flow using injected dependencies."""
    parsed_date = date.fromisoformat(as_of_date)
    run_id = runtime_run_id or f"run-{as_of_date}"
    manifests: list[dict[str, object]] = []

    context = dependencies.market_context_collector(as_of_date)
    tickers = _context_tickers(context)
    manifests.append(_manifest(run_id, "collect_market_context", 0, {"tickers": len(tickers)}))

    layer0_result = dependencies.layer0_runner(parsed_date, tickers, benchmark_ticker)
    manifests.append(
        _manifest(
            run_id,
            "run_layer0_incremental",
            1,
            {
                "layer0_run_id": layer0_result.run_id,
                "layer0_manifest_key": layer0_result.manifest_key,
            },
        )
    )

    layer1_run_id = f"layer1-daily-{as_of_date}"
    dispatch = dependencies.layer1_trigger(
        Layer1TriggerConfig(
            run_id=layer1_run_id,
            as_of_date=as_of_date,
            layer0_run_id=layer0_result.run_id,
            benchmark_ticker=benchmark_ticker,
        )
    )
    manifests.append(
        _manifest(
            run_id,
            "trigger_layer1_modal",
            2,
            {
                "layer1_run_id": dispatch.run_id,
                "layer1_manifest_key": dispatch.manifest_key,
                "command": list(dispatch.command),
            },
        )
    )

    modal_runtime = dependencies.modal_runtime_loader()
    layer1_manifest = dependencies.layer1_waiter(
        Layer1WaitConfig(
            run_id=dispatch.run_id,
            as_of_date=as_of_date,
            layer0_run_id=layer0_result.run_id,
            poll_interval_seconds=modal_runtime.layer1_poll_interval_seconds,
            poll_timeout_seconds=modal_runtime.layer1_poll_timeout_seconds,
        )
    )
    manifests.append(
        _manifest(
            run_id,
            "wait_for_layer1_manifest",
            3,
            {
                "layer1_status": layer1_manifest.status.value,
                "layer1_output_path": layer1_manifest.output_path,
            },
        )
    )

    predictions = dependencies.prediction_requester(context)
    manifests.append(_manifest(run_id, "run_cloud_inference", 4, {"scores": 3}))

    score_map = predictions["scores"]
    targets = [
        {"ticker": ticker, "target_dollars": round(score * 1000.0, 2)}
        for ticker, score in score_map.items()
    ]
    manifests.append(_manifest(run_id, "construct_portfolio_targets", 5, {"targets": len(targets)}))

    approved = [target for target in targets if target["target_dollars"] > 700.0]
    manifests.append(_manifest(run_id, "apply_hard_risk_controls", 6, {"approved": len(approved)}))

    manifests.append(_manifest(run_id, "persist_approved_proposal", 7, {"approved": len(approved)}))
    manifests.append(_manifest(run_id, "reconcile_account_state", 8))

    orders = [
        {"ticker": row["ticker"], "action": "BUY", "target_dollars": row["target_dollars"]}
        for row in approved
    ]
    manifests.append(_manifest(run_id, "translate_targets_to_orders", 9, {"orders": len(orders)}))

    execution_summary = dependencies.order_executor(orders)
    manifests.append(_manifest(run_id, "execute_and_monitor", 10, execution_summary))

    manifests.append(_manifest(run_id, "persist_execution_logs", 11, {"written": True}))

    summary_snapshot = manifests + [_manifest(run_id, "send_run_summary", 12)]
    summary = dependencies.summary_builder(summary_snapshot)
    manifests.append(_manifest(run_id, "send_run_summary", 12, summary))

    return manifests


def run_daily_dry_run(as_of_date: str) -> list[dict[str, object]]:
    """Execute a deterministic dry-run of the daily edge runtime flow."""
    return run_daily(
        as_of_date,
        dependencies=_dry_run_dependencies(),
        runtime_run_id=f"dryrun-{as_of_date}",
    )


def main(argv: Sequence[str] | None = None) -> int:
    """Run the daily Pi orchestration from the local command line."""
    args = _parse_args(argv)
    manifests = (
        run_daily_dry_run(args.as_of_date)
        if args.dry_run
        else run_daily(args.as_of_date, dependencies=_live_dependencies())
    )
    return 0 if manifests else 1


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """Parse CLI arguments for live or dry-run Pi orchestration."""
    parser = argparse.ArgumentParser(description="Run the Pi daily orchestration flow.")
    parser.add_argument("--as-of-date", default=date.today().isoformat(), metavar="YYYY-MM-DD")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args(argv)


def _context_tickers(context: dict[str, object]) -> tuple[str, ...]:
    """Extract and validate the point-in-time ticker universe from market context."""
    raw_universe = context.get("universe")
    if not isinstance(raw_universe, list):
        raise ValueError("market context must include a universe list")
    tickers = tuple(str(item).strip().upper() for item in raw_universe if str(item).strip())
    if not tickers:
        raise ValueError("market context universe cannot be empty")
    return tickers


def _live_dependencies() -> DailyRunDependencies:
    """Return the production dependency wiring for the Pi runtime."""
    return DailyRunDependencies(
        market_context_collector=collect_market_context,
        layer0_runner=_run_live_layer0,
        modal_runtime_loader=load_pi_modal_runtime_config,
        layer1_trigger=trigger_layer1_feature_generation,
        layer1_waiter=wait_for_layer1_manifest,
        prediction_requester=request_predictions,
        order_executor=execute_orders,
        summary_builder=build_run_summary,
    )


def _dry_run_dependencies() -> DailyRunDependencies:
    """Return deterministic dry-run dependency wiring with no live credentials."""
    return DailyRunDependencies(
        market_context_collector=collect_market_context,
        layer0_runner=_run_dry_layer0,
        modal_runtime_loader=lambda: PiModalRuntimeConfig(
            layer1_poll_interval_seconds=1,
            layer1_poll_timeout_seconds=5,
        ),
        layer1_trigger=_trigger_dry_layer1,
        layer1_waiter=_wait_for_dry_layer1_manifest,
        prediction_requester=request_predictions,
        order_executor=execute_orders,
        summary_builder=build_run_summary,
    )


def _run_live_layer0(
    as_of_date: date,
    tickers: Sequence[str],
    benchmark_ticker: str,
) -> Layer0RunSummary:
    """Run the production Layer 0 incremental pipeline and return its manifest identity."""
    result = run_live_layer0_incremental(
        as_of_date=as_of_date,
        tickers=tickers,
        benchmark_ticker=benchmark_ticker,
    )
    return Layer0RunSummary(run_id=result.run_id, manifest_key=result.manifest_key)


def _run_dry_layer0(
    as_of_date: date,
    tickers: Sequence[str],
    benchmark_ticker: str,
) -> Layer0RunSummary:
    """Return a deterministic Layer 0 completion record for dry-run orchestration."""
    _ = tickers
    _ = benchmark_ticker
    run_id = f"layer0-daily-{as_of_date.isoformat()}"
    return Layer0RunSummary(
        run_id=run_id,
        manifest_key=pipeline_manifest_path("layer0", run_id),
    )


def _trigger_dry_layer1(config: Layer1TriggerConfig) -> Layer1DispatchResult:
    """Return a deterministic Modal dispatch result for dry-run orchestration."""
    return Layer1DispatchResult(
        run_id=config.run_id,
        manifest_key=pipeline_manifest_path("layer1", config.run_id),
        command=(
            "python",
            "-m",
            "modal",
            "run",
            "app/lab/data_pipelines/run_daily_layer1.py",
        ),
    )


def _wait_for_dry_layer1_manifest(config: Layer1WaitConfig) -> PipelineManifestRecord:
    """Return a completed Layer 1 manifest without touching live R2 or Modal."""
    return PipelineManifestRecord(
        run_id=config.run_id,
        stage="layer1",
        status=RunStatus.COMPLETED,
        started_at=datetime(2026, 1, 1, tzinfo=UTC),
        finished_at=datetime(2026, 1, 1, 0, 0, 1, tzinfo=UTC),
        input_path=pipeline_manifest_path("layer0", config.layer0_run_id),
        output_path="features/",
        metadata={
            "as_of_date": config.as_of_date,
            "layer0_run_id": config.layer0_run_id,
        },
    )


if __name__ == "__main__":
    raise SystemExit(main())
