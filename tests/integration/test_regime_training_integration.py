from __future__ import annotations

import pandas as pd

from core.features.regime_training import (
    HMM_TRAINING_FEATURE_COLUMNS,
    build_hmm_training_frame,
    complete_hmm_training_matrix,
)


def test_hmm_training_pipeline_builds_complete_modal_ready_matrix() -> None:
    """Benchmark and macro archives produce a complete numeric matrix for HMM fitting."""
    bars = _benchmark_bars()
    macro = _macro_archive()

    training = build_hmm_training_frame(bars, macro)
    matrix = complete_hmm_training_matrix(training)

    assert len(training) == len(bars)
    assert len(matrix) > 0
    assert list(matrix.columns) == list(HMM_TRAINING_FEATURE_COLUMNS)
    assert matrix.index.is_monotonic_increasing
    assert matrix.notna().all().all()


def _benchmark_bars() -> pd.DataFrame:
    """Build an integration-sized benchmark OHLCV archive."""
    rows: list[dict[str, object]] = []
    for index, date in enumerate(pd.bdate_range("2023-10-02", periods=100)):
        close = 400.0 + index + (index % 5) * 0.25
        rows.append(
            {
                "date": date.date().isoformat(),
                "ticker": "SPY",
                "open": close - 0.5,
                "high": close + 1.0,
                "low": close - 1.0,
                "close": close,
                "volume": 2_000_000 + index,
                "adj_close": close,
                "dollar_volume": close * (2_000_000 + index),
            }
        )
    return pd.DataFrame(rows)


def _macro_archive() -> pd.DataFrame:
    """Build point-in-time macro archive rows used by regime training."""
    return pd.DataFrame(
        [
            _macro_row("VIXCLS", "2023-10-02", "2023-10-02", 16.0),
            _macro_row("VIXCLS", "2023-11-01", "2023-11-01", 18.5),
            _macro_row("VIXCLS", "2023-12-01", "2023-12-01", 15.5),
            _macro_row("DGS10", "2023-10-02", "2023-10-02", 4.7),
            _macro_row("DGS2", "2023-10-02", "2023-10-02", 5.0),
            _macro_row("DGS3MO", "2023-10-02", "2023-10-02", 5.3),
            _macro_row("BAMLH0A0HYM2", "2023-10-02", "2023-10-02", 4.1),
        ]
    )


def _macro_row(
    series_id: str,
    observation_date: str,
    realtime_start: str,
    value: float,
) -> dict[str, object]:
    """Build one normalized FRED macro archive row."""
    return {
        "source": "fred",
        "series_id": series_id,
        "observation_date": observation_date,
        "realtime_start": realtime_start,
        "realtime_end": realtime_start,
        "retrieved_at": "2024-01-01T00:00:00+00:00",
        "value": value,
        "is_missing": False,
        "raw": {},
    }
