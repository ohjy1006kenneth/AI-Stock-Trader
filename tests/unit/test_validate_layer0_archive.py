from __future__ import annotations

import json
from datetime import date
from pathlib import Path

from app.lab.data_pipelines.validate_layer0_archive import (
    validate_layer0_archive,
    write_validation_report,
)
from services.r2.paths import (
    layer0_ohlcv_provenance_report_path,
    pipeline_manifest_path,
    raw_fundamentals_path,
    raw_macro_path,
    raw_news_path,
    raw_price_path,
    raw_universe_path,
)
from tests.fixtures.layer0_ohlcv_support import (
    build_provenance_report,
    daily_raw_provenance,
    historical_adjusted_provenance,
)


class _Reader:
    def __init__(self, keys: set[str], objects: dict[str, bytes] | None = None) -> None:
        self.keys = keys
        self.objects = objects or {}

    def exists(self, key: str) -> bool:
        return key in self.keys

    def list_keys(self, prefix: str) -> list[str]:
        return sorted(key for key in self.keys if key.startswith(prefix))

    def get_object(self, key: str) -> bytes:
        return self.objects[key]


def _historical_manifest_objects(run_id: str) -> dict[str, bytes]:
    report_key = layer0_ohlcv_provenance_report_path(run_id)
    return {
        pipeline_manifest_path("layer0", run_id): json.dumps(
            {
                "run_id": run_id,
                "stage": "layer0",
                "status": "completed",
                "metadata": {
                    "mode": "historical_backfill",
                    "prices": {
                        "adjustment_provenance": {
                            "policy_id": "alpaca_historical_1day_adjustment_all",
                            "provider": "alpaca",
                            "request_adjustment": "all",
                            "stored_ohlc_basis": "provider_adjusted",
                            "normalized_adj_close_policy": "copy_close_to_adj_close",
                            "feed": "sip",
                        },
                        "provenance_report_key": report_key,
                    },
                },
            }
        ).encode("utf-8"),
        report_key: json.dumps(
            build_provenance_report(
                run_id=run_id,
                mode="historical_backfill",
                provenance=historical_adjusted_provenance(),
                observed_rows=3,
            )
        ).encode("utf-8"),
    }


def test_validate_layer0_archive_reports_ready_when_required_keys_exist() -> None:
    """Validation is ready when all required archive families and manifest exist."""
    from_date = date(2024, 1, 1)
    to_date = date(2024, 1, 3)
    run_id = "layer0-historical-2024-01-01_to_2024-01-03"
    objects = _historical_manifest_objects(run_id)
    keys = {
        raw_price_path("AAPL"),
        raw_news_path("2024-01-01"),
        raw_news_path("2024-01-02"),
        raw_news_path("2024-01-03"),
        raw_universe_path("2024-01-01"),
        raw_universe_path("2024-01-02"),
        raw_universe_path("2024-01-03"),
        raw_fundamentals_path("AAPL"),
        raw_macro_path("2024-01-02"),
        pipeline_manifest_path("layer0", run_id),
        layer0_ohlcv_provenance_report_path(run_id),
    }

    report = validate_layer0_archive(
        from_date=from_date,
        to_date=to_date,
        run_id=run_id,
        reader=_Reader(keys, objects),
    )

    assert report.ready_for_layer1 is True
    assert report.price_archive_count == 1
    assert report.canonical_price_archive_count == 1
    assert report.news_days_present == 3
    assert report.universe_days_present == 3
    assert report.fundamentals_ticker_count == 1
    assert report.macro_day_count == 1
    assert report.noncanonical_price_keys == []
    assert report.ohlcv_provenance_report_present is True
    assert report.ohlcv_provenance_policy_id == "alpaca_historical_1day_adjustment_all"
    assert report.ohlcv_provenance_validation_errors == []


def test_validate_layer0_archive_blocks_layer1_when_active_fundamentals_are_missing_or_empty() -> None:
    """Layer 0 validation surfaces active constituent SimFin coverage gaps."""
    from_date = date(2024, 1, 1)
    to_date = date(2024, 1, 3)
    run_id = "layer0-historical-2024-01-01_to_2024-01-03"
    objects = _historical_manifest_objects(run_id)
    keys = {
        raw_price_path("AAPL"),
        raw_news_path("2024-01-01"),
        raw_news_path("2024-01-02"),
        raw_news_path("2024-01-03"),
        raw_universe_path("2024-01-01"),
        raw_universe_path("2024-01-02"),
        raw_universe_path("2024-01-03"),
        raw_fundamentals_path("AAPL"),
        raw_fundamentals_path("MSFT"),
        raw_macro_path("2024-01-02"),
        pipeline_manifest_path("layer0", run_id),
        layer0_ohlcv_provenance_report_path(run_id),
    }

    report = validate_layer0_archive(
        from_date=from_date,
        to_date=to_date,
        run_id=run_id,
        reader=_Reader(keys, objects),
        active_fundamentals_tickers=["AAPL", "MSFT", "GOOGL"],
        fundamentals_min_rows=1,
        fundamentals_row_counter=lambda _reader, key: {
            raw_fundamentals_path("AAPL"): 5,
            raw_fundamentals_path("MSFT"): 0,
        }.get(key, 0),
    )

    assert report.ready_for_layer1 is False
    assert report.fundamentals_tickers_expected == 3
    assert report.fundamentals_tickers_present == 2
    assert report.fundamentals_tickers_below_min_rows == ["GOOGL", "MSFT"]


def test_validate_layer0_archive_reports_missing_dates() -> None:
    """Validation reports exact missing news and business-day universe archives."""
    report = validate_layer0_archive(
        from_date=date(2024, 1, 5),
        to_date=date(2024, 1, 8),
        run_id="missing",
        reader=_Reader({raw_price_path("AAPL")}),
    )

    assert report.ready_for_layer1 is False
    assert report.missing_news_dates == [
        "2024-01-05",
        "2024-01-06",
        "2024-01-07",
        "2024-01-08",
    ]
    assert report.missing_universe_dates == ["2024-01-05", "2024-01-08"]
    assert "missing_manifest_payload" in report.ohlcv_provenance_validation_errors


def test_validate_layer0_archive_blocks_noncanonical_price_keys() -> None:
    """Validation fails closed when the raw price archive contains legacy filenames."""
    from_date = date(2024, 1, 1)
    to_date = date(2024, 1, 1)
    run_id = "layer0-historical-2024-01-01"
    objects = _historical_manifest_objects(run_id)
    keys = {
        raw_price_path("AAPL"),
        "raw/prices/AAPL_2017-01-03_2024-01-01.parquet",
        raw_news_path("2024-01-01"),
        raw_universe_path("2024-01-01"),
        raw_fundamentals_path("AAPL"),
        raw_macro_path("2024-01-01"),
        pipeline_manifest_path("layer0", run_id),
        layer0_ohlcv_provenance_report_path(run_id),
    }

    report = validate_layer0_archive(
        from_date=from_date,
        to_date=to_date,
        run_id=run_id,
        reader=_Reader(keys, objects),
    )

    assert report.ready_for_layer1 is False
    assert report.price_archive_count == 2
    assert report.canonical_price_archive_count == 1
    assert report.noncanonical_price_keys == [
        "raw/prices/AAPL_2017-01-03_2024-01-01.parquet"
    ]


def test_validate_layer0_archive_blocks_missing_or_mismatched_ohlcv_provenance() -> None:
    """Validation fails closed when Layer 0 omits OHLCV adjustment provenance."""
    run_id = "layer0-historical-2024-01-02"
    report_key = layer0_ohlcv_provenance_report_path(run_id)
    manifest_key = pipeline_manifest_path("layer0", run_id)
    objects = {
        manifest_key: json.dumps(
            {
                "run_id": run_id,
                "stage": "layer0",
                "status": "completed",
                "metadata": {
                    "mode": "historical_backfill",
                    "prices": {
                        "adjustment_provenance": {
                            "policy_id": "alpaca_live_1day_adjustment_raw",
                            "provider": "alpaca",
                            "request_adjustment": "raw",
                            "stored_ohlc_basis": "raw",
                            "normalized_adj_close_policy": "copy_close_to_adj_close",
                            "feed": "iex",
                        },
                        "provenance_report_key": report_key,
                    },
                },
            }
        ).encode("utf-8"),
        report_key: json.dumps(
            build_provenance_report(
                run_id=run_id,
                mode="historical_backfill",
                provenance=daily_raw_provenance(),
                observed_rows=2,
            )
        ).encode("utf-8"),
    }
    keys = {
        raw_price_path("AAPL"),
        raw_news_path("2024-01-02"),
        raw_universe_path("2024-01-02"),
        raw_fundamentals_path("AAPL"),
        raw_macro_path("2024-01-02"),
        manifest_key,
        report_key,
    }

    report = validate_layer0_archive(
        from_date=date(2024, 1, 2),
        to_date=date(2024, 1, 2),
        run_id=run_id,
        reader=_Reader(keys, objects),
    )

    assert report.ready_for_layer1 is False
    assert report.ohlcv_provenance_report_present is True
    assert report.ohlcv_provenance_policy_id == "alpaca_live_1day_adjustment_raw"
    assert "manifest_policy_id_expected_alpaca_historical_1day_adjustment_all" in (
        report.ohlcv_provenance_validation_errors
    )
    assert "report_request_adjustment_expected_all" in report.ohlcv_provenance_validation_errors


def test_validate_layer0_archive_surfaces_split_like_discontinuity_count() -> None:
    """Validation preserves split-like audit counts without treating them as policy failures."""
    run_id = "layer0-historical-2024-01-03"
    report_key = layer0_ohlcv_provenance_report_path(run_id)
    objects = _historical_manifest_objects(run_id)
    objects[report_key] = json.dumps(
        build_provenance_report(
            run_id=run_id,
            mode="historical_backfill",
            provenance=historical_adjusted_provenance(),
            observed_rows=2,
            split_like_discontinuity_count=1,
        )
    ).encode("utf-8")
    keys = {
        raw_price_path("AAPL"),
        raw_news_path("2024-01-03"),
        raw_universe_path("2024-01-03"),
        raw_fundamentals_path("AAPL"),
        raw_macro_path("2024-01-03"),
        pipeline_manifest_path("layer0", run_id),
        report_key,
    }

    report = validate_layer0_archive(
        from_date=date(2024, 1, 3),
        to_date=date(2024, 1, 3),
        run_id=run_id,
        reader=_Reader(keys, objects),
    )

    assert report.ready_for_layer1 is True
    assert report.ohlcv_split_like_discontinuity_count == 1


def test_write_validation_report_writes_json(tmp_path: Path) -> None:
    """Validation reports are persisted as sorted JSON under integration reports."""
    report = validate_layer0_archive(
        from_date=date(2024, 1, 1),
        to_date=date(2024, 1, 1),
        run_id="missing",
        reader=_Reader(set()),
    )

    path = write_validation_report(report, tmp_path)

    assert path.name == "layer0_archive_validation_2024-01-01_to_2024-01-01.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload["ready_for_layer1"] is False
