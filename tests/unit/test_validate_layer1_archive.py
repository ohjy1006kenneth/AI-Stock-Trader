from __future__ import annotations

import io
import json
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
import pytest

from app.lab.data_pipelines.validate_layer1_archive import (
    Layer1ValidationReport,
    build_layer1_output_prefixes,
    load_universe_mapping,
    render_validation_report,
    validate_layer1_archive,
    write_validation_report,
)
from core.contracts.schemas import FeatureRecord, PipelineManifestRecord, RunStatus
from core.features.io import feature_records_to_parquet_bytes
from services.r2.paths import (
    layer1_ticker_history_path,
    layer1_validation_report_path,
    pipeline_manifest_path,
)


class _Reader:
    """Minimal in-memory stand-in for the R2 archive reader."""

    def __init__(self, objects: dict[str, bytes]) -> None:
        self.objects = dict(objects)

    def exists(self, key: str) -> bool:
        return key in self.objects

    def get_object(self, key: str) -> bytes:
        return self.objects[key]

    def list_keys(self, prefix: str) -> list[str]:
        return sorted(key for key in self.objects if key.startswith(prefix))


class _NoManifestInspectionReader(_Reader):
    """Reader that fails if manifest listing is attempted unexpectedly."""

    def list_keys(self, prefix: str) -> list[str]:
        if prefix == "artifacts/manifests/layer1/":
            raise AssertionError("manifest inspection should be opt-in")
        return super().list_keys(prefix)


class _ManifestRaceReader(_Reader):
    """Reader that simulates a manifest disappearing after list_keys returns it."""

    def get_object(self, key: str) -> bytes:
        if key == pipeline_manifest_path("layer1", "layer1-readiness-2024-01-02-v9"):
            raise FileNotFoundError(key)
        return super().get_object(key)


def _history_bytes(ticker: str, dates: list[str]) -> bytes:
    """Return a valid Layer 1 feature-history payload for one ticker."""
    records = [
        FeatureRecord(
            date=as_of_date,
            ticker=ticker,
            features={"returns_1d": 0.01},
        )
        for as_of_date in dates
    ]
    return feature_records_to_parquet_bytes(records)


def _manifest_bytes(run_id: str, status: RunStatus) -> bytes:
    """Return a serialized Layer 1 manifest payload for one run."""
    return PipelineManifestRecord(
        run_id=run_id,
        stage="layer1",
        status=status,
        started_at=datetime(2024, 1, 5, 12, 0, tzinfo=UTC),
        finished_at=(
            datetime(2024, 1, 5, 12, 30, tzinfo=UTC)
            if status is RunStatus.COMPLETED
            else None
        ),
        output_path="features/layer1/",
    ).model_dump_json().encode("utf-8")


def test_validate_layer1_archive_marks_ready_when_every_history_present() -> None:
    """A complete archive yields ready_for_layer2=True with no missing histories."""
    universe = {
        "2024-01-02": ["AAPL", "MSFT"],
        "2024-01-03": ["AAPL"],
    }
    reader = _Reader(
        {
            layer1_ticker_history_path("AAPL"): _history_bytes(
                "AAPL", ["2024-01-02", "2024-01-03"]
            ),
            layer1_ticker_history_path("MSFT"): _history_bytes("MSFT", ["2024-01-02"]),
            pipeline_manifest_path("layer1", "layer1-2024-01-02_to_2024-01-03"): _manifest_bytes(
                "layer1-2024-01-02_to_2024-01-03",
                RunStatus.COMPLETED,
            ),
        }
    )

    report = validate_layer1_archive(
        run_id="layer1-2024-01-02_to_2024-01-03",
        from_date="2024-01-02",
        to_date="2024-01-03",
        universe=universe,
        reader=reader,
    )

    assert report.ready_for_layer2 is True
    assert report.manifest_key == pipeline_manifest_path(
        "layer1", "layer1-2024-01-02_to_2024-01-03"
    )
    assert report.report_key == layer1_validation_report_path(
        "layer1-2024-01-02_to_2024-01-03",
        "2024-01-02",
        "2024-01-03",
    )
    assert report.expected_ticker_files == 2
    assert report.present_ticker_files == 2
    assert report.expected_rows == 3
    assert report.present_rows == 3
    assert report.missing_ticker_files == []
    assert report.schema_failures == 0
    assert report.related_manifests == []
    assert report.manifest_errors == []


def test_validate_layer1_archive_reports_missing_histories() -> None:
    """Missing ticker histories keep ready_for_layer2=False and list the keys."""
    universe = {
        "2024-01-02": ["AAPL", "MSFT"],
    }
    reader = _Reader(
        {
            layer1_ticker_history_path("AAPL"): _history_bytes("AAPL", ["2024-01-02"]),
        }
    )

    report = validate_layer1_archive(
        run_id="layer1-missing",
        from_date="2024-01-02",
        to_date="2024-01-02",
        universe=universe,
        reader=reader,
    )

    assert report.ready_for_layer2 is False
    assert report.expected_ticker_files == 2
    assert report.present_ticker_files == 1
    assert report.missing_ticker_files == [layer1_ticker_history_path("MSFT")]


def test_validate_layer1_archive_flags_corrupt_histories() -> None:
    """Histories that fail to decode are reported via schema_failures."""
    bad_frame = pd.DataFrame(
        [{"date": "2024-01-02", "ticker": "AAPL", "features": "not-json"}]
    )
    buffer = io.BytesIO()
    bad_frame.to_parquet(buffer, index=False)
    reader = _Reader({layer1_ticker_history_path("AAPL"): buffer.getvalue()})

    report = validate_layer1_archive(
        run_id="layer1-corrupt",
        from_date="2024-01-02",
        to_date="2024-01-02",
        universe={"2024-01-02": ["AAPL"]},
        reader=reader,
    )

    assert report.ready_for_layer2 is False
    assert report.schema_failures == 1
    assert report.schema_failure_keys == [layer1_ticker_history_path("AAPL")]


def test_validate_layer1_archive_flags_row_count_mismatch() -> None:
    """History files must contain exactly the dates implied by the universe."""
    reader = _Reader(
        {layer1_ticker_history_path("AAPL"): _history_bytes("AAPL", ["2024-01-02"])}
    )

    report = validate_layer1_archive(
        run_id="layer1-row-mismatch",
        from_date="2024-01-02",
        to_date="2024-01-03",
        universe={"2024-01-02": ["AAPL"], "2024-01-03": ["AAPL"]},
        reader=reader,
    )

    assert report.ready_for_layer2 is False
    assert report.row_count_failures == 1
    assert report.row_count_failure_keys == [layer1_ticker_history_path("AAPL")]


def test_validate_layer1_archive_rejects_non_iso_dates() -> None:
    """Non-canonical YYYY-MM-DD inputs raise immediately."""
    reader = _Reader({})
    with pytest.raises(ValueError, match="from_date"):
        validate_layer1_archive(
            run_id="layer1",
            from_date="2024-1-2",
            to_date="2024-01-03",
            universe={},
            reader=reader,
        )


def test_validate_layer1_archive_empty_universe_is_not_ready() -> None:
    """An empty universe means we have nothing to check; never marked ready."""
    reader = _Reader({})

    report = validate_layer1_archive(
        run_id="layer1-empty",
        from_date="2024-01-02",
        to_date="2024-01-02",
        universe={},
        reader=reader,
    )

    assert report.ready_for_layer2 is False
    assert report.expected_rows == 0


def test_validate_layer1_archive_samples_daily_shards_deterministically() -> None:
    """Leakage spot checks keep a stable bounded sample for larger universes."""
    universe = {
        "2024-01-02": ["AAPL", "MSFT", "NVDA"],
        "2024-01-03": ["AAPL", "MSFT", "NVDA"],
        "2024-01-04": ["AAPL", "MSFT", "NVDA"],
        "2024-01-05": ["AAPL", "MSFT", "NVDA"],
    }
    reader = _Reader({})

    first_report = validate_layer1_archive(
        run_id="layer1-sampled",
        from_date="2024-01-02",
        to_date="2024-01-05",
        universe=universe,
        reader=reader,
    )
    second_report = validate_layer1_archive(
        run_id="layer1-sampled",
        from_date="2024-01-02",
        to_date="2024-01-05",
        universe=universe,
        reader=reader,
    )

    first_sample = first_report.leakage_spot_checks[0]["sampled_pairs"]
    second_sample = second_report.leakage_spot_checks[0]["sampled_pairs"]

    assert len(first_sample) == 10
    assert first_sample == second_sample


def test_validate_layer1_archive_skips_manifest_inspection_by_default() -> None:
    """Daily orchestration does not inspect sibling manifests unless opted in."""
    reader = _NoManifestInspectionReader(
        {layer1_ticker_history_path("AAPL"): _history_bytes("AAPL", ["2024-01-02"])}
    )

    report = validate_layer1_archive(
        run_id="layer1-2024-01-02",
        from_date="2024-01-02",
        to_date="2024-01-02",
        universe={"2024-01-02": ["AAPL"]},
        reader=reader,
    )

    assert report.ready_for_layer2 is True
    assert report.related_manifests == []
    assert report.manifest_errors == []


def test_validate_layer1_archive_requires_completed_manifest_when_requested() -> None:
    """Standalone readiness validation fails closed when the exact manifest is still running."""
    reader = _Reader(
        {
            layer1_ticker_history_path("AAPL"): _history_bytes("AAPL", ["2024-01-02"]),
            pipeline_manifest_path("layer1", "layer1-readiness-2024-01-02-v1"): _manifest_bytes(
                "layer1-readiness-2024-01-02-v1",
                RunStatus.RUNNING,
            ),
            pipeline_manifest_path("layer1", "layer1-readiness-2024-01-02-v2"): _manifest_bytes(
                "layer1-readiness-2024-01-02-v2",
                RunStatus.COMPLETED,
            ),
        }
    )

    report = validate_layer1_archive(
        run_id="layer1-readiness-2024-01-02-v1",
        from_date="2024-01-02",
        to_date="2024-01-02",
        universe={"2024-01-02": ["AAPL"]},
        reader=reader,
        require_completed_manifest=True,
    )

    assert report.ready_for_layer2 is False
    assert report.manifest_status == "running"
    assert report.manifest_errors == ["exact_manifest_not_completed:running"]
    assert report.related_manifests == [
        {
            "key": pipeline_manifest_path("layer1", "layer1-readiness-2024-01-02-v1"),
            "run_id": "layer1-readiness-2024-01-02-v1",
            "status": "running",
            "finished_at": None,
        },
        {
            "key": pipeline_manifest_path("layer1", "layer1-readiness-2024-01-02-v2"),
            "run_id": "layer1-readiness-2024-01-02-v2",
            "status": "completed",
            "finished_at": "2024-01-05T12:30:00Z",
        },
    ]
    assert report.stale_manifest_keys == []


def test_validate_layer1_archive_reports_missing_exact_manifest_when_absent() -> None:
    """Required manifest validation fails closed when only sibling manifests exist."""
    reader = _Reader(
        {
            layer1_ticker_history_path("AAPL"): _history_bytes("AAPL", ["2024-01-02"]),
            pipeline_manifest_path("layer1", "layer1-readiness-2024-01-02-v2"): _manifest_bytes(
                "layer1-readiness-2024-01-02-v2",
                RunStatus.COMPLETED,
            ),
        }
    )

    report = validate_layer1_archive(
        run_id="layer1-readiness-2024-01-02-v1",
        from_date="2024-01-02",
        to_date="2024-01-02",
        universe={"2024-01-02": ["AAPL"]},
        reader=reader,
        require_completed_manifest=True,
    )

    assert report.ready_for_layer2 is False
    assert report.manifest_status is None
    assert report.manifest_errors == ["missing_exact_manifest"]
    assert report.related_manifests == [
        {
            "key": pipeline_manifest_path("layer1", "layer1-readiness-2024-01-02-v2"),
            "run_id": "layer1-readiness-2024-01-02-v2",
            "status": "completed",
            "finished_at": "2024-01-05T12:30:00Z",
        }
    ]


def test_validate_layer1_archive_records_manifest_read_race_instead_of_crashing() -> None:
    """Manifest read races are reported as validation errors instead of exceptions."""
    key = pipeline_manifest_path("layer1", "layer1-readiness-2024-01-02-v9")
    reader = _ManifestRaceReader(
        {
            layer1_ticker_history_path("AAPL"): _history_bytes("AAPL", ["2024-01-02"]),
            key: _manifest_bytes("layer1-readiness-2024-01-02-v9", RunStatus.COMPLETED),
        }
    )

    report = validate_layer1_archive(
        run_id="layer1-readiness-2024-01-02-v9",
        from_date="2024-01-02",
        to_date="2024-01-02",
        universe={"2024-01-02": ["AAPL"]},
        reader=reader,
        require_completed_manifest=True,
    )

    assert report.ready_for_layer2 is False
    assert report.manifest_errors == ["exact_manifest_missing_during_read"]
    assert report.related_manifests == [
        {
            "key": key,
            "run_id": "layer1-readiness-2024-01-02-v9",
            "status": "missing",
            "finished_at": None,
            "error": key,
        }
    ]


def test_validate_layer1_archive_documents_sibling_running_manifests() -> None:
    """Successful readiness reports still call out stale sibling manifests."""
    reader = _Reader(
        {
            layer1_ticker_history_path("AAPL"): _history_bytes("AAPL", ["2024-01-02"]),
            pipeline_manifest_path("layer1", "layer1-readiness-2024-01-02-v4"): _manifest_bytes(
                "layer1-readiness-2024-01-02-v4",
                RunStatus.RUNNING,
            ),
            pipeline_manifest_path("layer1", "layer1-readiness-2024-01-02-v9"): _manifest_bytes(
                "layer1-readiness-2024-01-02-v9",
                RunStatus.COMPLETED,
            ),
        }
    )

    report = validate_layer1_archive(
        run_id="layer1-readiness-2024-01-02-v9",
        from_date="2024-01-02",
        to_date="2024-01-02",
        universe={"2024-01-02": ["AAPL"]},
        reader=reader,
        require_completed_manifest=True,
    )

    assert report.ready_for_layer2 is True
    assert report.manifest_errors == []
    assert report.stale_manifest_keys == [
        pipeline_manifest_path("layer1", "layer1-readiness-2024-01-02-v4")
    ]


def test_write_validation_report_writes_json(tmp_path: Path) -> None:
    """Validation reports are persisted as deterministic JSON under the report dir."""
    report = Layer1ValidationReport(
        run_id="layer1",
        from_date="2024-01-02",
        to_date="2024-01-02",
        validation_status="failed",
        expected_ticker_files=1,
        present_ticker_files=0,
        expected_rows=1,
        present_rows=0,
        schema_failures=0,
        row_count_failures=0,
        missing_ticker_files=[layer1_ticker_history_path("AAPL")],
        ready_for_layer2=False,
    )

    path = write_validation_report(report, tmp_path)

    assert path.name == "layer1_archive_validation_layer1_2024-01-02_to_2024-01-02.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload["validation_status"] == "failed"
    assert payload["ready_for_layer2"] is False
    assert payload["missing_ticker_files"] == [layer1_ticker_history_path("AAPL")]


def test_render_validation_report_preserves_manifest_and_report_keys() -> None:
    """Rendered validation JSON includes manifest/report linkage for durable storage."""
    report = Layer1ValidationReport(
        run_id="layer1",
        from_date="2024-01-02",
        to_date="2024-01-02",
        validation_status="completed",
        expected_ticker_files=1,
        present_ticker_files=1,
        expected_rows=1,
        present_rows=1,
        schema_failures=0,
        row_count_failures=0,
        manifest_key="artifacts/manifests/layer1/layer1.json",
        report_key=layer1_validation_report_path("layer1", "2024-01-02", "2024-01-02"),
        manifest_status="completed",
        stale_manifest_keys=["artifacts/manifests/layer1/layer1-v0.json"],
        ready_for_layer2=True,
    )

    payload = json.loads(render_validation_report(report))

    assert payload["manifest_key"] == "artifacts/manifests/layer1/layer1.json"
    assert payload["report_key"] == layer1_validation_report_path(
        "layer1", "2024-01-02", "2024-01-02"
    )
    assert payload["manifest_status"] == "completed"
    assert payload["stale_manifest_keys"] == ["artifacts/manifests/layer1/layer1-v0.json"]
    assert payload["validation_status"] == "completed"


def test_build_layer1_output_prefixes_includes_validation_and_history_layouts() -> None:
    """Standalone validator reports include the canonical R2 prefixes they checked."""
    prefixes = build_layer1_output_prefixes(["2024-01-02", "2024-01-03"])

    assert prefixes["layer1_history"] == "features/layer1/"
    assert prefixes["layer1_daily_shards"] == "features/layer1/2024-01-03/"
    assert prefixes["regime_outputs"] == "features/layer1_5/regime/"
    assert prefixes["layer1_manifests"] == "artifacts/manifests/layer1/"
    assert prefixes["validation_reports"] == "artifacts/reports/integration/"


def test_load_universe_mapping_normalizes_tickers(tmp_path: Path) -> None:
    """The universe loader uppercases tickers and validates the JSON shape."""
    target = tmp_path / "universe.json"
    target.write_text(
        json.dumps({"2024-01-02": ["aapl", "msft"], "2024-01-03": ["googl"]}),
        encoding="utf-8",
    )

    mapping = load_universe_mapping(target)

    assert mapping == {"2024-01-02": ["AAPL", "MSFT"], "2024-01-03": ["GOOGL"]}


def test_load_universe_mapping_rejects_non_object_payloads(tmp_path: Path) -> None:
    """A list-shaped JSON file is rejected with a clear error."""
    target = tmp_path / "universe.json"
    target.write_text(json.dumps(["aapl"]), encoding="utf-8")
    with pytest.raises(ValueError, match="object"):
        load_universe_mapping(target)
