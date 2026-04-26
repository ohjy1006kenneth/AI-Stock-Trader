from __future__ import annotations

import pandas as pd

from core.features.regime_detection import HMMRegimeConfig, fit_and_emit_hmm_regime_features
from core.features.regime_training import build_hmm_training_frame


def test_hmm_regime_detection_runs_on_layer15_training_frame() -> None:
    """Layer 1.5 training data can be fitted and emitted without provider calls."""
    bars = _benchmark_bars()
    macro = _macro_archive()
    training = build_hmm_training_frame(bars, macro)
    complete_dates = training[training["is_complete"].astype(bool)]["date"].tolist()
    train_end_date = complete_dates[45]
    inference_dates = complete_dates[50:60]

    features = fit_and_emit_hmm_regime_features(
        training,
        train_end_date=train_end_date,
        inference_dates=inference_dates,
        config=HMMRegimeConfig(min_training_rows=40, max_iterations=30),
    )

    assert features["date"].tolist() == inference_dates
    assert features["regime_confidence"].notna().all()
    assert features[["regime_prob_bear", "regime_prob_sideways", "regime_prob_bull"]].notna().all().all()


def _benchmark_bars() -> pd.DataFrame:
    """Build a synthetic benchmark archive with three visible market regimes."""
    rows: list[dict[str, object]] = []
    close = 400.0
    for index, date in enumerate(pd.bdate_range("2023-01-02", periods=150)):
        if index < 50:
            close *= 0.997
        elif index < 100:
            close *= 1.0002
        else:
            close *= 1.003
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
    """Build point-in-time macro rows needed for complete regime training rows."""
    rows: list[dict[str, object]] = []
    for date in pd.bdate_range("2022-12-01", periods=170):
        date_text = date.date().isoformat()
        rows.extend(
            [
                _macro_row("VIXCLS", date_text, date_text, 18.0),
                _macro_row("DGS10", date_text, date_text, 4.0),
                _macro_row("DGS2", date_text, date_text, 3.8),
                _macro_row("DGS3MO", date_text, date_text, 3.5),
                _macro_row("BAMLH0A0HYM2", date_text, date_text, 3.2),
            ]
        )
    return pd.DataFrame(rows)


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
