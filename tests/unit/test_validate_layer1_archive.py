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
    layer1_regime_path,
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


class _HiddenHistoryListingReader(_Reader):
    """Reader that can resolve expected histories but hides them from prefix listing."""

    def list_keys(self, prefix: str) -> list[str]:
        if prefix == "features/layer1/":
            return []
        return super().list_keys(prefix)


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


def _history_bytes_with_features(
    ticker: str,
    rows: list[tuple[str, dict[str, object]]],
) -> bytes:
    """Return a Layer 1 history payload with explicit per-date feature maps."""
    records = [
        FeatureRecord(date=as_of_date, ticker=ticker, features=features)
        for as_of_date, features in rows
    ]
    return feature_records_to_parquet_bytes(records)


def _regime_artifact_bytes(rows: list[dict[str, object]]) -> bytes:
    """Return a parquet payload for one Layer 1.5 regime artifact."""
    buffer = io.BytesIO()
    pd.DataFrame(rows).to_parquet(buffer, index=False)
    return buffer.getvalue()


def _regime_manifest_bytes(
    *,
    run_id: str,
    output_path: str,
    status: RunStatus = RunStatus.COMPLETED,
    inference_dates: list[str] | None = None,
) -> bytes:
    """Return a serialized Layer 1.5 manifest payload for one run."""
    return PipelineManifestRecord(
        run_id=run_id,
        stage="layer1_5_regime",
        status=status,
        started_at=datetime(2024, 1, 5, 11, 0, tzinfo=UTC),
        finished_at=(
            datetime(2024, 1, 5, 11, 30, tzinfo=UTC)
            if status is RunStatus.COMPLETED
            else None
        ),
        output_path=output_path,
        metadata={"inference_dates": inference_dates or []},
    ).model_dump_json().encode("utf-8")


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


def _ready_regime_features(
    *,
    label: str = "bull",
    confidence: float = 0.8,
    bear: float = 0.1,
    sideways: float = 0.1,
    bull: float = 0.8,
) -> dict[str, object]:
    """Return a coherent ready-state regime feature map for one ticker-day row."""
    return {
        "regime_label": label,
        "regime_confidence": confidence,
        "regime_prob_bear": bear,
        "regime_prob_sideways": sideways,
        "regime_prob_bull": bull,
    }


def _ready_regime_objects(parent_run_id: str, date_text: str) -> dict[str, bytes]:
    """Return a completed per-date Layer 1.5 artifact plus manifest."""
    stage_run_id = f"{parent_run_id}-{date_text}"
    output_key = layer1_regime_path(stage_run_id)
    return {
        output_key: _regime_artifact_bytes(
            [
                {
                    "date": date_text,
                    **_ready_regime_features(),
                    "regime_required_for_layer2": True,
                    "regime_readiness_status": "ready",
                    "regime_readiness_reason": "ready",
                    "regime_missing_features": "",
                    "regime_probability_sum": 1.0,
                    "training_rows": 40,
                    "complete_training_rows": 40,
                    "min_training_rows": 30,
                }
            ]
        ),
        pipeline_manifest_path("layer1_5_regime", stage_run_id): _regime_manifest_bytes(
            run_id=stage_run_id,
            output_path=output_key,
            inference_dates=[date_text],
        ),
    }


def test_validate_layer1_archive_marks_ready_when_every_history_present() -> None:
    """A complete archive yields ready_for_layer2=True with no missing histories."""
    universe = {
        "2024-01-02": ["AAPL", "MSFT"],
        "2024-01-03": ["AAPL"],
    }
    reader = _Reader(
        {
            layer1_ticker_history_path("AAPL"): _history_bytes_with_features(
                "AAPL",
                [
                    ("2024-01-02", {"returns_1d": 0.01, **_ready_regime_features()}),
                    ("2024-01-03", {"returns_1d": 0.01, **_ready_regime_features()}),
                ],
            ),
            layer1_ticker_history_path("MSFT"): _history_bytes_with_features(
                "MSFT",
                [("2024-01-02", {"returns_1d": 0.01, **_ready_regime_features()})],
            ),
            pipeline_manifest_path("layer1", "layer1-2024-01-02_to_2024-01-03"): _manifest_bytes(
                "layer1-2024-01-02_to_2024-01-03",
                RunStatus.COMPLETED,
            ),
            **_ready_regime_objects("layer1-2024-01-02_to_2024-01-03", "2024-01-02"),
            **_ready_regime_objects("layer1-2024-01-02_to_2024-01-03", "2024-01-03"),
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
    assert report.present_ticker_counts_by_date == {"2024-01-02": 2, "2024-01-03": 1}
    assert report.missing_tickers_by_date == {}
    assert report.schema_failures == 0
    assert report.archive_layout_failures == []
    assert report.canonical_history_key_count == 2
    assert report.listed_expected_history_ticker_count == 2
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
    assert report.present_ticker_counts_by_date == {"2024-01-02": 1}
    assert report.missing_tickers_by_date == {"2024-01-02": ["MSFT"]}
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


def test_validate_layer1_archive_marks_short_history_regime_as_warning() -> None:
    """Explicit null regime placeholders become warnings when HMM history is insufficient."""
    run_id = "layer1-warmup-2024-01-03"
    output_key = layer1_regime_path(run_id)
    reader = _Reader(
        {
            layer1_ticker_history_path("AAPL"): _history_bytes_with_features(
                "AAPL",
                [
                    (
                        "2024-01-03",
                        {
                            "returns_1d": 0.01,
                            "regime_label": None,
                            "regime_confidence": None,
                            "regime_prob_bear": None,
                            "regime_prob_sideways": None,
                            "regime_prob_bull": None,
                        },
                    )
                ],
            ),
            output_key: _regime_artifact_bytes(
                [
                    {
                        "date": "2024-01-03",
                        "regime_label": None,
                        "regime_confidence": float("nan"),
                        "regime_prob_bear": float("nan"),
                        "regime_prob_sideways": float("nan"),
                        "regime_prob_bull": float("nan"),
                        "regime_required_for_layer2": False,
                        "regime_readiness_status": "warning",
                        "regime_readiness_reason": "insufficient_training_history",
                        "regime_missing_features": "",
                        "regime_probability_sum": float("nan"),
                        "training_rows": 15,
                        "complete_training_rows": 15,
                        "min_training_rows": 30,
                    }
                ]
            ),
            pipeline_manifest_path("layer1_5_regime", run_id): _regime_manifest_bytes(
                run_id=run_id,
                output_path=output_key,
                inference_dates=["2024-01-03"],
            ),
        }
    )

    report = validate_layer1_archive(
        run_id="layer1-warmup",
        from_date="2024-01-03",
        to_date="2024-01-03",
        universe={"2024-01-03": ["AAPL"]},
        reader=reader,
    )

    assert report.ready_for_layer2 is False
    assert report.validation_status == "warning"
    assert report.regime_validation_status == "warning"
    assert report.layer2_regime_optional_dates == ["2024-01-03"]
    assert report.regime_failures == []
    assert report.regime_warnings[0]["reason"] == "insufficient_training_history"
    assert report.regime_window_summary["output_dates_present"] == ["2024-01-03"]
    assert report.regime_window_summary["explicit_null_rate"] == pytest.approx(1.0)
    assert report.regime_feature_coverage_by_date["2024-01-03"]["explicit_null_rows"] == 1


def test_validate_layer1_archive_fails_when_required_regime_fields_are_missing() -> None:
    """Null or absent regime fields fail readiness once Layer 1.5 says they are required."""
    run_id = "layer1-required-regime-2024-01-03"
    output_key = layer1_regime_path(run_id)
    reader = _Reader(
        {
            layer1_ticker_history_path("AAPL"): _history_bytes_with_features(
                "AAPL",
                [("2024-01-03", {"returns_1d": 0.01})],
            ),
            output_key: _regime_artifact_bytes(
                [
                    {
                        "date": "2024-01-03",
                        "regime_label": "bull",
                        "regime_confidence": 0.8,
                        "regime_prob_bear": 0.1,
                        "regime_prob_sideways": 0.1,
                        "regime_prob_bull": 0.8,
                        "regime_required_for_layer2": True,
                        "regime_readiness_status": "ready",
                        "regime_readiness_reason": "ready",
                        "regime_missing_features": "",
                        "regime_probability_sum": 1.0,
                        "training_rows": 40,
                        "complete_training_rows": 40,
                        "min_training_rows": 30,
                    }
                ]
            ),
            pipeline_manifest_path("layer1_5_regime", run_id): _regime_manifest_bytes(
                run_id=run_id,
                output_path=output_key,
                inference_dates=["2024-01-03"],
            ),
        }
    )

    report = validate_layer1_archive(
        run_id="layer1-required-regime",
        from_date="2024-01-03",
        to_date="2024-01-03",
        universe={"2024-01-03": ["AAPL"]},
        reader=reader,
    )

    assert report.ready_for_layer2 is False
    assert report.validation_status == "failed"
    assert report.layer2_regime_required_dates == ["2024-01-03"]
    assert report.regime_failures[0]["reason"] == "required_regime_fields_missing"
    assert report.regime_window_summary["rows_missing_feature_keys"] == 1


def test_validate_layer1_archive_fails_when_required_regime_output_uses_nan() -> None:
    """Required Layer 1.5 rows with NaN regime values fail readiness as missing output."""
    run_id = "layer1-required-regime-nan-2024-01-03"
    output_key = layer1_regime_path(run_id)
    reader = _Reader(
        {
            layer1_ticker_history_path("AAPL"): _history_bytes_with_features(
                "AAPL",
                [
                    (
                        "2024-01-03",
                        {
                            "returns_1d": 0.01,
                            "regime_label": "bull",
                            "regime_confidence": 0.8,
                            "regime_prob_bear": 0.1,
                            "regime_prob_sideways": 0.1,
                            "regime_prob_bull": 0.8,
                        },
                    )
                ],
            ),
            output_key: _regime_artifact_bytes(
                [
                    {
                        "date": "2024-01-03",
                        "regime_label": "bull",
                        "regime_confidence": float("nan"),
                        "regime_prob_bear": float("nan"),
                        "regime_prob_sideways": float("nan"),
                        "regime_prob_bull": float("nan"),
                        "regime_required_for_layer2": True,
                        "regime_readiness_status": "ready",
                        "regime_readiness_reason": "ready",
                        "regime_missing_features": "",
                        "regime_probability_sum": float("nan"),
                        "training_rows": 40,
                        "complete_training_rows": 40,
                        "min_training_rows": 30,
                    }
                ]
            ),
            pipeline_manifest_path("layer1_5_regime", run_id): _regime_manifest_bytes(
                run_id=run_id,
                output_path=output_key,
                inference_dates=["2024-01-03"],
            ),
        }
    )

    report = validate_layer1_archive(
        run_id="layer1-required-regime-nan",
        from_date="2024-01-03",
        to_date="2024-01-03",
        universe={"2024-01-03": ["AAPL"]},
        reader=reader,
    )

    assert report.ready_for_layer2 is False
    assert report.validation_status == "failed"
    assert report.layer2_regime_required_dates == ["2024-01-03"]
    assert report.regime_failures == [
        {
            "date": "2024-01-03",
            "status": "failure",
            "reason": "required_regime_output_is_null",
            "required_for_layer2": True,
            "output_key": output_key,
            "output_present": True,
            "manifest_key": pipeline_manifest_path("layer1_5_regime", run_id),
            "manifest_present": True,
            "manifest_status": "completed",
            "manifest_inference_dates": ["2024-01-03"],
            "missing_features": [],
            "complete_training_rows": 40,
            "min_training_rows": 30,
            "probability_sum": None,
        }
    ]


def test_validate_layer1_archive_fails_when_regime_output_is_missing_for_requested_date() -> None:
    """Layer 1 readiness fails closed when the requested window has no Layer 1.5 output."""
    report = validate_layer1_archive(
        run_id="layer1-missing-regime",
        from_date="2024-01-03",
        to_date="2024-01-03",
        universe={"2024-01-03": ["AAPL"]},
        reader=_Reader(
            {
                layer1_ticker_history_path("AAPL"): _history_bytes("AAPL", ["2024-01-03"]),
            }
        ),
    )

    assert report.ready_for_layer2 is False
    assert report.regime_validation_status == "failed"
    assert report.regime_failures == [
        {
            "date": "2024-01-03",
            "status": "failure",
            "reason": "missing_regime_manifest",
            "required_for_layer2": False,
            "output_key": layer1_regime_path("layer1-missing-regime-2024-01-03"),
            "output_present": False,
            "manifest_key": pipeline_manifest_path(
                "layer1_5_regime",
                "layer1-missing-regime-2024-01-03",
            ),
            "manifest_present": False,
            "manifest_status": None,
        }
    ]
    assert report.regime_window_summary["manifest_dates_missing"] == ["2024-01-03"]
    assert report.regime_window_summary["output_dates_missing"] == ["2024-01-03"]


def test_validate_layer1_archive_fails_when_regime_manifest_is_not_completed() -> None:
    """Incomplete Layer 1.5 manifests block readiness even when the parquet exists."""
    run_id = "layer1-regime-running-2024-01-03"
    output_key = layer1_regime_path(run_id)
    reader = _Reader(
        {
            layer1_ticker_history_path("AAPL"): _history_bytes("AAPL", ["2024-01-03"]),
            output_key: _regime_artifact_bytes(
                [
                    {
                        "date": "2024-01-03",
                        "regime_label": "bull",
                        "regime_confidence": 0.8,
                        "regime_prob_bear": 0.1,
                        "regime_prob_sideways": 0.1,
                        "regime_prob_bull": 0.8,
                    }
                ]
            ),
            pipeline_manifest_path("layer1_5_regime", run_id): _regime_manifest_bytes(
                run_id=run_id,
                output_path=output_key,
                status=RunStatus.RUNNING,
                inference_dates=["2024-01-03"],
            ),
        }
    )

    report = validate_layer1_archive(
        run_id="layer1-regime-running",
        from_date="2024-01-03",
        to_date="2024-01-03",
        universe={"2024-01-03": ["AAPL"]},
        reader=reader,
    )

    assert report.ready_for_layer2 is False
    assert report.regime_failures[0]["reason"] == "regime_manifest_not_completed"
    assert report.regime_window_summary["manifest_dates_present"] == ["2024-01-03"]
    assert report.regime_window_summary["output_dates_present"] == ["2024-01-03"]


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


def test_validate_layer1_archive_warns_when_dated_shards_are_partial_but_histories_are_complete() -> None:
    """Partial dated shards stay non-authoritative and do not block ready histories."""
    universe = {"2024-01-02": ["AAPL", "MSFT"]}
    reader = _Reader(
        {
            layer1_ticker_history_path("AAPL"): _history_bytes_with_features(
                "AAPL",
                [("2024-01-02", {"returns_1d": 0.01, **_ready_regime_features()})],
            ),
            layer1_ticker_history_path("MSFT"): _history_bytes_with_features(
                "MSFT",
                [("2024-01-02", {"returns_1d": 0.01, **_ready_regime_features()})],
            ),
            "features/layer1/2024-01-02/AAPL.parquet": feature_records_to_parquet_bytes(
                [FeatureRecord(date="2024-01-02", ticker="AAPL", features={"returns_1d": 0.01})]
            ),
            **_ready_regime_objects("layer1-partial-dated-shards", "2024-01-02"),
        }
    )

    report = validate_layer1_archive(
        run_id="layer1-partial-dated-shards",
        from_date="2024-01-02",
        to_date="2024-01-02",
        universe=universe,
        reader=reader,
    )

    assert report.ready_for_layer2 is True
    assert report.archive_layout_failures == []
    assert report.archive_layout_warnings == ["dated_shards_partial_non_authoritative"]
    assert report.dated_shard_counts_by_date == {"2024-01-02": 1}
    assert report.dated_shard_counts_by_ticker == {"AAPL": 1, "MSFT": 0}
    assert report.dated_shard_missing_tickers_by_date == {"2024-01-02": ["MSFT"]}
    assert report.dated_shard_non_aapl_examples == []


def test_validate_layer1_archive_uses_non_aapl_examples_for_expected_anchor_ticker() -> None:
    """Dated-shard diagnostics still show contrast examples when AAPL is not requested."""
    universe = {"2024-01-02": ["MSFT", "NVDA"]}
    reader = _Reader(
        {
            layer1_ticker_history_path("MSFT"): _history_bytes_with_features(
                "MSFT",
                [("2024-01-02", {"returns_1d": 0.01, **_ready_regime_features()})],
            ),
            layer1_ticker_history_path("NVDA"): _history_bytes_with_features(
                "NVDA",
                [("2024-01-02", {"returns_1d": 0.01, **_ready_regime_features()})],
            ),
            "features/layer1/2024-01-02/MSFT.parquet": feature_records_to_parquet_bytes(
                [FeatureRecord(date="2024-01-02", ticker="MSFT", features={"returns_1d": 0.01})]
            ),
            "features/layer1/2024-01-02/NVDA.parquet": feature_records_to_parquet_bytes(
                [FeatureRecord(date="2024-01-02", ticker="NVDA", features={"returns_1d": 0.02})]
            ),
            **_ready_regime_objects("layer1-non-aapl-dated-shards", "2024-01-02"),
        }
    )

    report = validate_layer1_archive(
        run_id="layer1-non-aapl-dated-shards",
        from_date="2024-01-02",
        to_date="2024-01-02",
        universe=universe,
        reader=reader,
    )

    assert report.ready_for_layer2 is True
    assert report.dated_shard_non_aapl_examples == ["features/layer1/2024-01-02/NVDA.parquet"]


def test_validate_layer1_archive_fails_when_canonical_histories_are_not_listable() -> None:
    """Readiness fails when canonical histories exist but are not discoverable by listing."""
    reader = _HiddenHistoryListingReader(
        {
            layer1_ticker_history_path("AAPL"): _history_bytes("AAPL", ["2024-01-02"]),
            layer1_ticker_history_path("MSFT"): _history_bytes("MSFT", ["2024-01-02"]),
        }
    )

    report = validate_layer1_archive(
        run_id="layer1-hidden-history-listing",
        from_date="2024-01-02",
        to_date="2024-01-02",
        universe={"2024-01-02": ["AAPL", "MSFT"]},
        reader=reader,
    )

    assert report.ready_for_layer2 is False
    assert report.archive_layout_failures == ["canonical_history_listing_incomplete"]
    assert report.canonical_history_key_count == 0
    assert report.missing_listed_expected_tickers == ["AAPL", "MSFT"]


def test_validate_layer1_archive_skips_manifest_inspection_by_default() -> None:
    """Daily orchestration does not inspect sibling manifests unless opted in."""
    reader = _NoManifestInspectionReader(
        {
            layer1_ticker_history_path("AAPL"): _history_bytes_with_features(
                "AAPL",
                [("2024-01-02", {"returns_1d": 0.01, **_ready_regime_features()})],
            ),
            **_ready_regime_objects("layer1-2024-01-02", "2024-01-02"),
        }
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
            layer1_ticker_history_path("AAPL"): _history_bytes_with_features(
                "AAPL",
                [("2024-01-02", {"returns_1d": 0.01, **_ready_regime_features()})],
            ),
            pipeline_manifest_path("layer1", "layer1-readiness-2024-01-02-v4"): _manifest_bytes(
                "layer1-readiness-2024-01-02-v4",
                RunStatus.RUNNING,
            ),
            pipeline_manifest_path("layer1", "layer1-readiness-2024-01-02-v9"): _manifest_bytes(
                "layer1-readiness-2024-01-02-v9",
                RunStatus.COMPLETED,
            ),
            **_ready_regime_objects("layer1-readiness-2024-01-02-v9", "2024-01-02"),
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
    assert prefixes["layer1_canonical_history"] == prefixes["layer1_history"]
    assert prefixes["layer1_daily_shards"] == "features/layer1/2024-01-03/"
    assert prefixes["layer1_dated_shards"] == prefixes["layer1_daily_shards"]
    assert prefixes["regime_outputs"] == "features/layer1_5/regime/"
    assert prefixes["regime_manifests"] == "artifacts/manifests/layer1_5_regime/"
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
