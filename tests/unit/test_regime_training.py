from __future__ import annotations

import pandas as pd
import pytest

from core.features.regime_training import (
    HMM_TRAINING_COLUMNS,
    HMM_TRAINING_FEATURE_COLUMNS,
    build_hmm_training_frame,
    complete_hmm_training_matrix,
)


def _benchmark_bars(count: int = 90) -> pd.DataFrame:
    """Build deterministic benchmark OHLCV rows."""
    rows: list[dict[str, object]] = []
    dates = pd.bdate_range("2024-01-02", periods=count)
    for index, date in enumerate(dates):
        close = 400.0 + index
        rows.append(
            {
                "date": date.date().isoformat(),
                "ticker": "SPY",
                "open": close - 0.5,
                "high": close + 1.0,
                "low": close - 1.0,
                "close": close,
                "volume": 1_000_000 + index,
                "adj_close": close,
                "dollar_volume": close * (1_000_000 + index),
            }
        )
    return pd.DataFrame(rows)


def _macro_row(
    *,
    series_id: str,
    observation_date: str,
    realtime_start: str,
    value: float | None,
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
        "is_missing": value is None,
        "raw": {},
    }


def _macro_frame() -> pd.DataFrame:
    """Build macro rows that cover all HMM training macro features."""
    return pd.DataFrame(
        [
            _macro_row(
                series_id="VIXCLS",
                observation_date="2024-01-02",
                realtime_start="2024-01-02",
                value=15.0,
            ),
            _macro_row(
                series_id="VIXCLS",
                observation_date="2024-04-01",
                realtime_start="2024-04-01",
                value=18.0,
            ),
            _macro_row(
                series_id="DGS10",
                observation_date="2024-01-02",
                realtime_start="2024-01-02",
                value=4.0,
            ),
            _macro_row(
                series_id="DGS2",
                observation_date="2024-01-02",
                realtime_start="2024-01-02",
                value=3.5,
            ),
            _macro_row(
                series_id="DGS3MO",
                observation_date="2024-01-02",
                realtime_start="2024-01-02",
                value=3.0,
            ),
            _macro_row(
                series_id="BAMLH0A0HYM2",
                observation_date="2024-01-02",
                realtime_start="2024-01-02",
                value=3.75,
            ),
        ]
    )


def test_build_hmm_training_frame_returns_canonical_columns() -> None:
    """HMM training frame has the declared canonical columns."""
    training = build_hmm_training_frame(_benchmark_bars(), _macro_frame())

    assert list(training.columns) == list(HMM_TRAINING_COLUMNS)
    assert len(training) == 90
    assert set(HMM_TRAINING_FEATURE_COLUMNS).issubset(training.columns)


def test_build_hmm_training_frame_empty_benchmark_returns_empty_frame() -> None:
    """Empty benchmark input returns an empty canonical frame."""
    bars = pd.DataFrame(
        columns=["date", "open", "high", "low", "close", "adj_close", "volume"]
    )

    training = build_hmm_training_frame(bars, _macro_frame())

    assert len(training) == 0
    assert list(training.columns) == list(HMM_TRAINING_COLUMNS)


def test_build_hmm_training_frame_rejects_missing_benchmark_columns() -> None:
    """Benchmark OHLCV input must provide adjusted closes."""
    with pytest.raises(ValueError, match="adj_close"):
        build_hmm_training_frame(pd.DataFrame([{"date": "2024-01-02"}]), _macro_frame())


def test_build_hmm_training_frame_validates_macro_columns() -> None:
    """Malformed macro archives fail before HMM fitting input is emitted."""
    macro = pd.DataFrame([{"series_id": "VIXCLS", "value": 15.0}])

    with pytest.raises(ValueError, match="observation_date"):
        build_hmm_training_frame(_benchmark_bars(), macro)


def test_build_hmm_training_frame_marks_nan_inputs_incomplete() -> None:
    """NaN benchmark-derived features are retained but excluded from the fitting matrix."""
    bars = _benchmark_bars()
    bars.loc[len(bars) - 2, "adj_close"] = float("nan")

    training = build_hmm_training_frame(bars, _macro_frame())
    matrix = complete_hmm_training_matrix(training)

    assert not training.loc[len(training) - 1, "is_complete"]
    assert training.loc[len(training) - 1, "date"] not in matrix.index


def test_benchmark_features_are_shifted_to_prevent_same_day_leakage() -> None:
    """Date T benchmark features use returns through T-1."""
    bars = _benchmark_bars(70)
    training = build_hmm_training_frame(bars, _macro_frame())

    close = bars["adj_close"].astype(float)
    expected = close.pct_change(5).shift(1).iloc[-1]

    assert training.loc[len(training) - 1, "spy_return_5d"] == pytest.approx(expected)


def test_macro_features_use_strictly_prior_realtime_start() -> None:
    """Same-day macro releases are unavailable until the next target date."""
    bars = _benchmark_bars(3)
    macro = pd.DataFrame(
        [
            _macro_row(
                series_id="VIXCLS",
                observation_date="2024-01-02",
                realtime_start="2024-01-02",
                value=15.0,
            ),
            _macro_row(
                series_id="DGS10",
                observation_date="2024-01-02",
                realtime_start="2024-01-02",
                value=4.0,
            ),
            _macro_row(
                series_id="DGS2",
                observation_date="2024-01-02",
                realtime_start="2024-01-02",
                value=3.5,
            ),
            _macro_row(
                series_id="DGS3MO",
                observation_date="2024-01-02",
                realtime_start="2024-01-02",
                value=3.0,
            ),
            _macro_row(
                series_id="BAMLH0A0HYM2",
                observation_date="2024-01-02",
                realtime_start="2024-01-02",
                value=3.75,
            ),
        ]
    )

    training = build_hmm_training_frame(bars, macro)

    assert pd.isna(training.loc[0, "vix_level"])
    assert training.loc[1, "vix_level"] == pytest.approx(15.0)


def test_complete_hmm_training_matrix_returns_only_complete_numeric_rows() -> None:
    """HMM fitting matrix drops warm-up rows and contains only numeric features."""
    training = build_hmm_training_frame(_benchmark_bars(), _macro_frame())

    matrix = complete_hmm_training_matrix(training)

    assert len(matrix) < len(training)
    assert list(matrix.columns) == list(HMM_TRAINING_FEATURE_COLUMNS)
    assert matrix.index.name == "date"
    assert matrix.notna().all().all()


def test_complete_hmm_training_matrix_rejects_missing_columns() -> None:
    """Matrix extraction validates the training-frame contract."""
    with pytest.raises(ValueError, match="is_complete"):
        complete_hmm_training_matrix(pd.DataFrame([{"date": "2024-01-02"}]))
