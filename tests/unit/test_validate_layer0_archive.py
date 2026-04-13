from __future__ import annotations

import json
from datetime import date

from app.lab.data_pipelines.validate_layer0_archive import (
    validate_layer0_archive,
    write_validation_report,
)
from services.r2.paths import (
    pipeline_manifest_path,
    raw_fundamentals_path,
    raw_macro_path,
    raw_news_path,
    raw_price_path,
    raw_universe_path,
)


class _Reader:
    def __init__(self, keys: set[str]) -> None:
        self.keys = keys

    def exists(self, key: str) -> bool:
        return key in self.keys

    def list_keys(self, prefix: str) -> list[str]:
        return sorted(key for key in self.keys if key.startswith(prefix))


def test_validate_layer0_archive_reports_ready_when_required_keys_exist() -> None:
    """Validation is ready when prices, daily archives, range archives, and manifest exist."""
    from_date = date(2024, 1, 1)
    to_date = date(2024, 1, 3)
    run_id = "layer0-historical-2024-01-01_to_2024-01-03"
    keys = {
        raw_price_path("AAPL"),
        raw_news_path("2024-01-01"),
        raw_news_path("2024-01-02"),
        raw_news_path("2024-01-03"),
        raw_universe_path("2024-01-01"),
        raw_universe_path("2024-01-02"),
        raw_universe_path("2024-01-03"),
        raw_fundamentals_path(from_date, to_date),
        raw_macro_path(from_date, to_date),
        pipeline_manifest_path("layer0", run_id),
    }

    report = validate_layer0_archive(
        from_date=from_date,
        to_date=to_date,
        run_id=run_id,
        reader=_Reader(keys),  # type: ignore[arg-type]
    )

    assert report.ready_for_layer1 is True
    assert report.price_archive_count == 1
    assert report.news_days_present == 3
    assert report.universe_days_present == 3


def test_validate_layer0_archive_reports_missing_dates() -> None:
    """Validation reports exact missing news and business-day universe archives."""
    report = validate_layer0_archive(
        from_date=date(2024, 1, 5),
        to_date=date(2024, 1, 8),
        run_id="missing",
        reader=_Reader({raw_price_path("AAPL")}),  # type: ignore[arg-type]
    )

    assert report.ready_for_layer1 is False
    assert report.missing_news_dates == [
        "2024-01-05",
        "2024-01-06",
        "2024-01-07",
        "2024-01-08",
    ]
    assert report.missing_universe_dates == ["2024-01-05", "2024-01-08"]


def test_write_validation_report_writes_json(tmp_path) -> None:
    """Validation reports are persisted as sorted JSON in the integration report directory."""
    report = validate_layer0_archive(
        from_date=date(2024, 1, 1),
        to_date=date(2024, 1, 1),
        run_id="missing",
        reader=_Reader(set()),  # type: ignore[arg-type]
    )

    path = write_validation_report(report, tmp_path)

    assert path.name == "layer0_archive_validation_2024-01-01_to_2024-01-01.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload["ready_for_layer1"] is False
