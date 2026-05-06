from __future__ import annotations

from io import BytesIO

import pandas as pd
import pytest

from scripts.cleanup_legacy_price_duplicates import (
    audit_legacy_price_duplicates,
    delete_legacy_price_duplicates,
)
from services.r2.paths import raw_price_path


class _Writer:
    def __init__(self, objects: dict[str, bytes]) -> None:
        self.objects = dict(objects)
        self.deleted_keys: list[str] = []

    def list_keys(self, prefix: str) -> list[str]:
        return sorted(key for key in self.objects if key.startswith(prefix))

    def get_object(self, key: str) -> bytes:
        return self.objects[key]

    def delete_object(self, key: str) -> None:
        self.deleted_keys.append(key)
        self.objects.pop(key, None)

    def exists(self, key: str) -> bool:
        return key in self.objects


def _parquet_bytes(rows: list[dict[str, object]]) -> bytes:
    buffer = BytesIO()
    pd.DataFrame(rows).to_parquet(buffer, index=False)
    return buffer.getvalue()


def test_audit_legacy_price_duplicates_marks_safe_candidates() -> None:
    """Audit marks a legacy key safe when the canonical archive fully covers it."""
    writer = _Writer(
        {
            raw_price_path("AAPL"): _parquet_bytes(
                [
                    {"date": "2017-01-03", "close": 1.0},
                    {"date": "2017-01-04", "close": 2.0},
                    {"date": "2017-01-05", "close": 3.0},
                ]
            ),
            "raw/prices/AAPL_2017-01-03_2017-01-04.parquet": _parquet_bytes(
                [
                    {"date": "2017-01-03", "close": 1.0},
                    {"date": "2017-01-04", "close": 2.0},
                ]
            ),
        }
    )

    report = audit_legacy_price_duplicates(writer)

    assert report.candidate_count == 1
    assert report.verified_safe_count == 1
    assert report.unsafe_count == 0
    assert report.records[0].verification_status == "verified_safe"
    assert report.records[0].canonical_key == raw_price_path("AAPL")


def test_audit_legacy_price_duplicates_handles_empty_inventory() -> None:
    """Audit returns an empty report when no legacy price keys exist."""
    writer = _Writer({raw_price_path("AAPL"): _parquet_bytes([{"date": "2017-01-03"}])})

    report = audit_legacy_price_duplicates(writer)

    assert report.candidate_count == 0
    assert report.records == []


@pytest.mark.parametrize(
    ("rows", "error_pattern"),
    [
        ([{"close": 1.0}], "missing required 'date' column"),
        ([{"date": "not-a-date", "close": 1.0}], "contains non-date values"),
    ],
)
def test_audit_legacy_price_duplicates_rejects_invalid_legacy_payloads(
    rows: list[dict[str, object]],
    error_pattern: str,
) -> None:
    """Audit fails closed when a legacy payload lacks usable date information."""
    writer = _Writer(
        {
            raw_price_path("AAPL"): _parquet_bytes([{"date": "2017-01-03", "close": 1.0}]),
            "raw/prices/AAPL_2017-01-03_2017-01-03.parquet": _parquet_bytes(rows),
        }
    )

    with pytest.raises(ValueError, match=error_pattern):
        audit_legacy_price_duplicates(writer)


def test_delete_legacy_price_duplicates_respects_verification_mode() -> None:
    """Deletion removes safe records by default and all records with explicit approval."""
    writer = _Writer(
        {
            raw_price_path("AAPL"): _parquet_bytes(
                [
                    {"date": "2017-01-03", "close": 1.0},
                    {"date": "2017-01-04", "close": 2.0},
                ]
            ),
            raw_price_path("MSFT"): _parquet_bytes([{"date": "2017-01-04", "close": 5.0}]),
            "raw/prices/AAPL_2017-01-03_2017-01-04.parquet": _parquet_bytes(
                [
                    {"date": "2017-01-03", "close": 1.0},
                    {"date": "2017-01-04", "close": 2.0},
                ]
            ),
            "raw/prices/MSFT_2017-01-03_2017-01-04.parquet": _parquet_bytes(
                [
                    {"date": "2017-01-03", "close": 4.0},
                    {"date": "2017-01-04", "close": 5.0},
                ]
            ),
        }
    )

    report = audit_legacy_price_duplicates(writer)
    safe_only = delete_legacy_price_duplicates(report, writer=writer)

    assert safe_only.deleted_count == 1
    assert writer.deleted_keys == ["raw/prices/AAPL_2017-01-03_2017-01-04.parquet"]
    assert "raw/prices/MSFT_2017-01-03_2017-01-04.parquet" in writer.objects

    second_report = audit_legacy_price_duplicates(writer)
    delete_legacy_price_duplicates(
        second_report,
        writer=writer,
        allow_unverified_delete=True,
    )

    assert writer.deleted_keys == [
        "raw/prices/AAPL_2017-01-03_2017-01-04.parquet",
        "raw/prices/MSFT_2017-01-03_2017-01-04.parquet",
    ]
    assert "raw/prices/MSFT_2017-01-03_2017-01-04.parquet" not in writer.objects
