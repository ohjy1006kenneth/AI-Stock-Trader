from __future__ import annotations

import io
import json
from pathlib import Path

import pandas as pd
import pytest

from app.lab.data_pipelines.validate_layer1_archive import (
    Layer1ValidationReport,
    load_universe_mapping,
    validate_layer1_archive,
    write_validation_report,
)
from core.contracts.schemas import FeatureRecord
from core.features.io import feature_records_to_parquet_bytes
from services.r2.paths import layer1_ticker_history_path


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
    assert report.expected_ticker_files == 2
    assert report.present_ticker_files == 2
    assert report.expected_rows == 3
    assert report.present_rows == 3
    assert report.missing_ticker_files == []
    assert report.schema_failures == 0


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


def test_write_validation_report_writes_json(tmp_path: Path) -> None:
    """Validation reports are persisted as deterministic JSON under the report dir."""
    report = Layer1ValidationReport(
        run_id="layer1",
        from_date="2024-01-02",
        to_date="2024-01-02",
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

    assert path.name == "layer1_archive_validation_2024-01-02_to_2024-01-02.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload["ready_for_layer2"] is False
    assert payload["missing_ticker_files"] == [layer1_ticker_history_path("AAPL")]


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
