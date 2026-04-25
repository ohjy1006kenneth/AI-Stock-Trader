from __future__ import annotations

import pandas as pd
import pytest

from core.features.regime_detection import (
    HMM_REGIME_COLUMNS,
    HMMRegimeConfig,
    emit_hmm_regime_features,
    fit_and_emit_hmm_regime_features,
    fit_hmm_regime_model,
)
from core.features.regime_training import HMM_TRAINING_FEATURE_COLUMNS


def _synthetic_training_frame() -> pd.DataFrame:
    """Build a deterministic three-regime HMM training frame."""
    rows: list[dict[str, object]] = []
    dates = pd.bdate_range("2024-01-02", periods=135)
    regimes = (
        [("bear", -0.025, 0.55, 28.0)] * 45
        + [("sideways", 0.0005, 0.18, 17.0)] * 45
        + [("bull", 0.018, 0.12, 13.0)] * 45
    )
    for index, (label, daily_return, volatility, vix_level) in enumerate(regimes):
        row: dict[str, object] = {
            "date": dates[index].date().isoformat(),
            "spy_log_return_1d": daily_return + (index % 3) * 0.0002,
            "spy_return_5d": daily_return * 5.0,
            "spy_realized_vol_21d": volatility,
            "spy_realized_vol_63d": volatility * 0.9,
            "spy_vol_ratio_21_63": volatility / (volatility * 0.9),
            "spy_drawdown_63d": -0.18 if label == "bear" else (-0.03 if label == "sideways" else 0.0),
            "vix_level": vix_level,
            "vix_change_5d": 1.0 if label == "bear" else (-0.2 if label == "bull" else 0.0),
            "yield_curve_slope_10y_2y": -0.6 if label == "bear" else 0.2,
            "yield_curve_slope_10y_3m": -0.8 if label == "bear" else 0.3,
            "high_yield_spread": 5.5 if label == "bear" else (3.5 if label == "sideways" else 2.5),
            "is_complete": True,
        }
        rows.append(row)
    return pd.DataFrame(rows)


def test_fit_and_emit_hmm_regime_features_recovers_synthetic_bull_regime() -> None:
    """A trained HMM emits the expected semantic label on clear synthetic regimes."""
    training = _synthetic_training_frame()
    train_end_date = str(training.loc[100, "date"])
    inference_dates = training.loc[106:, "date"].astype(str).tolist()

    features = fit_and_emit_hmm_regime_features(
        training,
        train_end_date=train_end_date,
        inference_dates=inference_dates,
        config=HMMRegimeConfig(min_training_rows=90, max_iterations=60),
    )

    assert list(features.columns) == list(HMM_REGIME_COLUMNS)
    assert len(features) == len(inference_dates)
    assert (features["regime_label"] == "bull").mean() >= 0.90
    assert features["regime_confidence"].iloc[-1] >= 0.70
    assert features[["regime_prob_bear", "regime_prob_sideways", "regime_prob_bull"]].sum(
        axis=1
    ).iloc[-1] == pytest.approx(1.0)


def test_emit_hmm_regime_features_rejects_inference_inside_training_window() -> None:
    """Inference dates must be strictly after the explicit training window."""
    training = _synthetic_training_frame()
    train_end_date = str(training.loc[100, "date"])
    model = fit_hmm_regime_model(
        training,
        train_end_date=train_end_date,
        config=HMMRegimeConfig(min_training_rows=90, max_iterations=20),
    )

    with pytest.raises(ValueError, match="strictly after train_end_date"):
        emit_hmm_regime_features(
            training,
            model=model,
            inference_dates=[str(training.loc[100, "date"])],
        )


def test_fit_hmm_regime_model_uses_rows_before_train_end_date() -> None:
    """The fitter records a bounded train window and excludes train_end_date itself."""
    training = _synthetic_training_frame()
    train_end_date = str(training.loc[90, "date"])

    model = fit_hmm_regime_model(
        training,
        train_end_date=train_end_date,
        config=HMMRegimeConfig(min_training_rows=80, max_iterations=20),
    )

    assert model.train_start_date == str(training.loc[0, "date"])
    assert model.train_end_date == train_end_date
    assert model.feature_columns == HMM_TRAINING_FEATURE_COLUMNS


def test_fit_hmm_regime_model_rejects_too_few_complete_training_rows() -> None:
    """The HMM refuses undersized training windows."""
    training = _synthetic_training_frame().iloc[:20].copy()

    with pytest.raises(ValueError, match="fewer complete rows"):
        fit_hmm_regime_model(
            training,
            train_end_date="2024-12-31",
            config=HMMRegimeConfig(min_training_rows=30),
        )


def test_fit_hmm_regime_model_rejects_missing_training_columns() -> None:
    """Training-frame validation names missing HMM feature columns."""
    training = _synthetic_training_frame().drop(columns=["spy_log_return_1d"])

    with pytest.raises(ValueError, match="spy_log_return_1d"):
        fit_hmm_regime_model(training, train_end_date="2024-12-31")
