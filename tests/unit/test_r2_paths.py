from __future__ import annotations

from datetime import date, datetime

import pytest

from services.r2.paths import (
    build_r2_key,
    pipeline_manifest_path,
    raw_fundamentals_path,
    raw_macro_path,
    raw_news_path,
    raw_price_path,
    raw_reference_path,
    raw_security_master_path,
    raw_universe_path,
)


def test_build_r2_key_joins_posix_parts() -> None:
    """R2 keys use deterministic POSIX separators."""
    assert build_r2_key("raw", "prices", "123.parquet") == "raw/prices/123.parquet"


def test_layer0_raw_path_builders_return_canonical_keys() -> None:
    """Layer 0 path builders return the documented raw artifact locations."""
    assert raw_price_path("AAPL") == "raw/prices/AAPL.parquet"
    assert raw_news_path("2025-01-02") == "raw/news/2025-01-02.jsonl"
    assert raw_news_path(datetime(2025, 1, 2, 15, 30)) == "raw/news/2025-01-02.jsonl"
    assert raw_universe_path(date(2025, 1, 2)) == "raw/universe/2025-01-02.csv"
    assert raw_fundamentals_path("AAPL") == "raw/fundamentals/AAPL.parquet"
    assert raw_macro_path(date(2025, 1, 2)) == "raw/macro/2025-01-02.parquet"
    assert raw_macro_path("2025-01-02") == "raw/macro/2025-01-02.parquet"
    assert raw_reference_path("tiingo_security_master") == "raw/reference/tiingo_security_master.json"
    assert (
        raw_security_master_path("2025-01-02")
        == "raw/reference/security_master/2025-01-02.json"
    )


def test_pipeline_manifest_path_returns_canonical_key() -> None:
    """Pipeline manifests are stored under artifact manifests by stage and run."""
    assert (
        pipeline_manifest_path("layer0", "run-001")
        == "artifacts/manifests/layer0/run-001.json"
    )


@pytest.mark.parametrize("bad_part", ["", "   ", ".", "..", "/absolute", "raw/news", r"raw\\news"])
def test_build_r2_key_rejects_unsafe_parts(bad_part: str) -> None:
    """Unsafe key parts must fail before reaching object storage."""
    with pytest.raises((TypeError, ValueError)):
        build_r2_key("raw", bad_part, "file.json")


def test_build_r2_key_requires_parts() -> None:
    """At least one path part is required."""
    with pytest.raises(ValueError, match="at least one"):
        build_r2_key()


@pytest.mark.parametrize("bad_date", ["20250102", "2025-99-02", "not-a-date"])
def test_date_paths_reject_invalid_dates(bad_date: str) -> None:
    """Date-based paths require ISO calendar dates."""
    with pytest.raises(ValueError, match="YYYY-MM-DD"):
        raw_news_path(bad_date)


@pytest.mark.parametrize("bad_extension", [".json", "json.gz", "", ".."])
def test_raw_reference_path_rejects_unsafe_extensions(bad_extension: str) -> None:
    """Reference extensions cannot smuggle path segments or extra dots."""
    with pytest.raises(ValueError):
        raw_reference_path("security_master", bad_extension)
