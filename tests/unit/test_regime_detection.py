from __future__ import annotations

import pandas as pd
import pytest

from core.features.regime_detection import (
    HMM_REGIME_COLUMNS,
    HMMRegimeConfig,
    emit_hmm_regime_features,
    fit_and_emit_hmm_regime_features,
    fit_hmm_regime_model,
    inspect_hmm_regime_readiness,
    validate_hmm_regime_probabilities,
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


def test_fit_hmm_regime_model_drops_all_nan_features_in_train_window() -> None:
    """The fitter ignores features that are entirely unavailable in the bounded window."""
    training = _synthetic_training_frame()
    training["high_yield_spread"] = float("nan")
    train_end_date = str(training.loc[100, "date"])

    model = fit_hmm_regime_model(
        training,
        train_end_date=train_end_date,
        config=HMMRegimeConfig(min_training_rows=90, max_iterations=20),
    )

    assert "high_yield_spread" not in model.feature_columns
    assert "spy_log_return_1d" in model.feature_columns


def test_inspect_hmm_regime_readiness_marks_short_history_not_trainable() -> None:
    """Short training windows surface explicit readiness diagnostics."""
    training = _synthetic_training_frame().iloc[:20].copy()
    train_end_date = str(training.iloc[15]["date"])
    inference_date = str(training.iloc[19]["date"])

    readiness = inspect_hmm_regime_readiness(
        training,
        train_end_date=train_end_date,
        inference_dates=[inference_date],
        config=HMMRegimeConfig(min_training_rows=30),
    )

    assert readiness.can_fit_model is False
    assert readiness.complete_training_rows == 15
    assert readiness.complete_inference_dates == (inference_date,)


def test_emit_hmm_regime_features_uses_active_feature_subset_for_inference_rows() -> None:
    """Inference scoring should use the fitted feature subset, not stale is_complete flags."""
    training = _synthetic_training_frame()
    training["high_yield_spread"] = float("nan")
    training["is_complete"] = False
    train_end_date = str(training.loc[100, "date"])
    inference_date = str(training.loc[110, "date"])

    readiness = inspect_hmm_regime_readiness(
        training,
        train_end_date=train_end_date,
        inference_dates=[inference_date],
        config=HMMRegimeConfig(min_training_rows=90, max_iterations=20),
    )
    model = fit_hmm_regime_model(
        training,
        train_end_date=train_end_date,
        config=HMMRegimeConfig(min_training_rows=90, max_iterations=20),
    )
    features = emit_hmm_regime_features(
        training,
        model=model,
        inference_dates=[inference_date],
    )

    assert readiness.can_fit_model is True
    assert readiness.complete_inference_dates == (inference_date,)
    assert "high_yield_spread" not in model.feature_columns
    assert features.loc[0, "date"] == inference_date
    assert features.loc[0, "regime_label"] in {"bear", "sideways", "bull"}
    assert features.loc[0, "regime_confidence"] == pytest.approx(
        max(
            features.loc[0, "regime_prob_bear"],
            features.loc[0, "regime_prob_sideways"],
            features.loc[0, "regime_prob_bull"],
        )
    )


def test_validate_hmm_regime_probabilities_rejects_bad_probability_sums() -> None:
    """Populated regime rows must stay normalized and internally consistent."""
    features = pd.DataFrame(
        [
            {
                "date": "2024-06-03",
                "regime_label": "bull",
                "regime_confidence": 0.75,
                "regime_prob_bear": 0.1,
                "regime_prob_sideways": 0.1,
                "regime_prob_bull": 0.75,
            }
        ],
        columns=list(HMM_REGIME_COLUMNS),
    )

    errors = validate_hmm_regime_probabilities(features)

    assert errors == ["2024-06-03: regime probabilities sum to 0.950000"]
