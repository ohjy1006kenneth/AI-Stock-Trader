from __future__ import annotations

from datetime import UTC, date, datetime

from app.pi.network.oracle_client import (
    Layer1DispatchResult,
    Layer1TriggerConfig,
    Layer1WaitConfig,
    PiModalRuntimeConfig,
    wait_for_layer1_manifest,
)
from app.pi.run_daily import DailyRunDependencies, Layer0RunSummary, run_daily
from core.contracts.schemas import PipelineManifestRecord, RunStatus
from services.r2.paths import pipeline_manifest_path


class _ReaderWriter:
    def __init__(self) -> None:
        self.payloads: dict[str, bytes] = {}

    def exists(self, key: str) -> bool:
        return key in self.payloads

    def get_object(self, key: str) -> bytes:
        return self.payloads[key]

    def put_object(self, key: str, data: bytes | str) -> None:
        self.payloads[key] = data if isinstance(data, bytes) else data.encode("utf-8")


def test_pi_runtime_waits_for_layer1_manifest_with_mocked_modal_trigger() -> None:
    """Pi orchestration can mock Modal submission yet still gate on a real manifest poll."""
    store = _ReaderWriter()

    def _market_context(_as_of_date: str) -> dict[str, object]:
        return {
            "as_of_date": "2026-04-06",
            "universe": ["AAPL", "MSFT", "NVDA"],
            "account": {"equity": 100_000.0, "cash": 25_000.0},
        }

    def _layer0_runner(as_of_date: date, tickers) -> Layer0RunSummary:
        _ = tickers
        run_id = f"layer0-daily-{as_of_date.isoformat()}"
        return Layer0RunSummary(
            run_id=run_id,
            manifest_key=pipeline_manifest_path("layer0", run_id),
        )

    def _layer1_trigger(config: Layer1TriggerConfig) -> Layer1DispatchResult:
        manifest = PipelineManifestRecord(
            run_id=config.run_id,
            stage="layer1",
            status=RunStatus.COMPLETED,
            started_at=datetime(2026, 4, 6, tzinfo=UTC),
            finished_at=datetime(2026, 4, 6, 0, 0, 1, tzinfo=UTC),
            output_path="features/layer1",
            metadata={
                "as_of_date": config.as_of_date,
                "layer0_run_id": config.layer0_run_id,
            },
        )
        manifest_key = pipeline_manifest_path("layer1", config.run_id)
        store.put_object(manifest_key, manifest.model_dump_json())
        return Layer1DispatchResult(
            run_id=config.run_id,
            manifest_key=manifest_key,
            command=("python", "-m", "modal", "run"),
        )

    def _layer1_waiter(config: Layer1WaitConfig):
        return wait_for_layer1_manifest(
            config,
            reader=store,
            monotonic=lambda: 0.0,
            sleep=lambda _: None,
        )

    dependencies = DailyRunDependencies(
        market_context_collector=_market_context,
        layer0_runner=_layer0_runner,
        modal_runtime_loader=lambda: PiModalRuntimeConfig(
            layer1_poll_interval_seconds=1,
            layer1_poll_timeout_seconds=5,
        ),
        layer1_trigger=_layer1_trigger,
        layer1_waiter=_layer1_waiter,
        prediction_requester=lambda _context: {
            "scores": {"AAPL": 0.81, "MSFT": 0.74, "NVDA": 0.69}
        },
        order_executor=lambda orders: {
            "submitted": len(orders),
            "filled": len(orders),
            "orders": orders,
        },
        summary_builder=lambda manifests: {
            "stage_count": len(manifests),
            "stages": [manifest["stage"] for manifest in manifests],
            "all_completed": True,
        },
    )

    manifests = run_daily("2026-04-06", dependencies=dependencies, runtime_run_id="itest-run")

    assert [row["stage"] for row in manifests][:4] == [
        "collect_market_context",
        "run_layer0_incremental",
        "trigger_layer1_modal",
        "wait_for_layer1_manifest",
    ]
    assert manifests[3]["metadata"]["layer1_status"] == "completed"
