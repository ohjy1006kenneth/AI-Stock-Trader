"""Unit tests for the XGBoostRanker (Layer 2 model).

Tests that require xgboost are skipped automatically when the package is
not installed (e.g. in lightweight CI environments using only base deps).
"""
from __future__ import annotations

import pytest

from core.contracts.schemas import FeatureRecord, ScoreRecord
from core.models.xgboost_ranker import (
    _LABEL_COLUMNS as LABEL_FEATURE_COLUMNS,  # noqa: F401
)
from core.models.xgboost_ranker import (
    XGBoostRanker,
    XGBoostRankerConfig,
    _build_training_arrays,
    _compute_version,
    _extract_regime,
    _rank_array,
    _safe_float,
)

xgb = pytest.importorskip("xgboost", reason="xgboost not installed")
pytest.importorskip("sklearn", reason="scikit-learn not installed")


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _make_feature_record(
    date: str,
    ticker: str,
    *,
    momentum: float = 0.01,
    sentiment: float = 0.5,
    regime: str | None = None,
) -> FeatureRecord:
    features: dict[str, object] = {
        "momentum_1m": momentum,
        "sentiment_score": sentiment,
        "volume_ratio": 1.2,
    }
    if regime is not None:
        features["regime_label"] = regime
        features["regime_prob_bull"] = 0.7
    return FeatureRecord(date=date, ticker=ticker, features=features)


def _make_label_record(date: str, ticker: str, forward_return: float | None) -> FeatureRecord:
    features: dict[str, object] = {
        "forward_return_1d": forward_return,
        "forward_return_5d": forward_return,
        "forward_return_20d": forward_return,
        "forward_log_return_1d": None,
        "forward_log_return_5d": None,
        "forward_log_return_20d": None,
        "survives_to_t1": 1,
        "survives_to_t5": 1,
        "survives_to_t20": 1,
    }
    return FeatureRecord(date=date, ticker=ticker, features=features)


def _make_training_pair(
    n_dates: int = 30,
    n_tickers: int = 10,
) -> tuple[list[FeatureRecord], list[FeatureRecord]]:
    tickers = [f"TK{i:02d}" for i in range(n_tickers)]
    dates = [f"2024-01-{d + 1:02d}" for d in range(min(n_dates, 28))]
    import random

    rng = random.Random(42)

    features = []
    labels = []
    for date in dates:
        for ticker in tickers:
            momentum = rng.uniform(-0.05, 0.05)
            sentiment = rng.uniform(0.0, 1.0)
            fwd_return = momentum + rng.uniform(-0.02, 0.02)
            features.append(_make_feature_record(date, ticker, momentum=momentum, sentiment=sentiment))
            labels.append(_make_label_record(date, ticker, fwd_return))
    return features, labels


# ---------------------------------------------------------------------------
# Helper tests (no xgboost required — but xgboost is imported at module level)
# ---------------------------------------------------------------------------


def test_safe_float_handles_none_and_nan() -> None:
    assert _safe_float(None) == 0.0
    assert _safe_float(float("nan")) == 0.0
    assert _safe_float(float("inf")) == 0.0
    assert _safe_float(1.5) == 1.5
    assert _safe_float(0) == 0.0


def test_rank_array_ascending_order() -> None:

    arr = [3.0, 1.0, 2.0]
    ranks = _rank_array(arr)
    # 1.0 → rank 0, 2.0 → rank 1, 3.0 → rank 2
    assert list(ranks) == [2.0, 0.0, 1.0]


def test_extract_regime_returns_label_or_none() -> None:
    rec_with = _make_feature_record("2024-01-01", "AAPL", regime="bull")
    rec_without = _make_feature_record("2024-01-01", "AAPL")
    assert _extract_regime(rec_with) == "bull"
    assert _extract_regime(rec_without) is None


def test_build_training_arrays_excludes_label_columns() -> None:
    feat_records, label_records = _make_training_pair(n_dates=5, n_tickers=3)
    X, y, cols = _build_training_arrays(feat_records, label_records)
    for label_col in LABEL_FEATURE_COLUMNS:
        assert label_col not in cols, f"Label column {label_col!r} leaked into features"


def test_build_training_arrays_returns_consistent_shapes() -> None:
    feat_records, label_records = _make_training_pair(n_dates=5, n_tickers=4)
    X, y, cols = _build_training_arrays(feat_records, label_records)
    assert X.shape[0] == len(y)
    assert X.shape[1] == len(cols)
    assert len(cols) > 0


def test_build_training_arrays_drops_missing_labels() -> None:
    features = [_make_feature_record("2024-01-01", "AAPL")]
    labels_with_none = [_make_label_record("2024-01-01", "AAPL", None)]
    labels_valid = [_make_label_record("2024-01-01", "AAPL", 0.01)]

    X_none, y_none, _ = _build_training_arrays(features, labels_with_none)
    X_valid, y_valid, _ = _build_training_arrays(features, labels_valid)

    assert len(y_none) == 0
    assert len(y_valid) == 1


def test_compute_version_is_deterministic() -> None:
    config = XGBoostRankerConfig()
    cols = ("a", "b", "c")
    v1 = _compute_version(config=config, feature_columns=cols, train_start="2024-01-01", train_end="2024-06-30", n_samples=100)
    v2 = _compute_version(config=config, feature_columns=cols, train_start="2024-01-01", train_end="2024-06-30", n_samples=100)
    assert v1 == v2
    assert v1.startswith("v1-")


def test_compute_version_changes_with_config() -> None:
    cols = ("a",)
    v1 = _compute_version(config=XGBoostRankerConfig(n_estimators=100), feature_columns=cols, train_start="2024-01-01", train_end="2024-06-30", n_samples=50)
    v2 = _compute_version(config=XGBoostRankerConfig(n_estimators=200), feature_columns=cols, train_start="2024-01-01", train_end="2024-06-30", n_samples=50)
    assert v1 != v2


# ---------------------------------------------------------------------------
# XGBoostRanker tests
# ---------------------------------------------------------------------------


def test_ranker_not_fitted_by_default() -> None:
    ranker = XGBoostRanker()
    assert not ranker.is_fitted
    assert ranker.model_version == ""
    assert ranker.feature_columns == ()


def test_ranker_fit_returns_self() -> None:
    features, labels = _make_training_pair()
    ranker = XGBoostRanker(XGBoostRankerConfig(n_estimators=10))
    result = ranker.fit(features, labels)
    assert result is ranker


def test_ranker_fit_sets_is_fitted() -> None:
    features, labels = _make_training_pair()
    ranker = XGBoostRanker(XGBoostRankerConfig(n_estimators=10))
    ranker.fit(features, labels)
    assert ranker.is_fitted
    assert ranker.model_version.startswith("v1-")
    assert len(ranker.feature_columns) > 0


def test_ranker_score_returns_score_records_for_date() -> None:
    features, labels = _make_training_pair(n_dates=5, n_tickers=5)
    ranker = XGBoostRanker(XGBoostRankerConfig(n_estimators=10))
    ranker.fit(features, labels)

    test_date = "2024-01-01"
    test_features = [r for r in features if r.date == test_date]
    scores = ranker.score(test_features, test_date)

    assert len(scores) == len(test_features)
    for score in scores:
        assert isinstance(score, ScoreRecord)
        assert score.date == test_date
        assert 0.0 <= score.pos_prob <= 1.0
        assert 0.0 <= score.rank_score <= 1.0
        assert 0.0 <= score.confidence <= 1.0
        assert score.model_version == ranker.model_version


def test_ranker_score_returns_empty_for_unknown_date() -> None:
    features, labels = _make_training_pair(n_dates=5, n_tickers=3)
    ranker = XGBoostRanker(XGBoostRankerConfig(n_estimators=10))
    ranker.fit(features, labels)
    scores = ranker.score(features, "2099-12-31")
    assert scores == []


def test_ranker_score_rank_scores_span_zero_to_one() -> None:
    features, labels = _make_training_pair(n_dates=3, n_tickers=6)
    ranker = XGBoostRanker(XGBoostRankerConfig(n_estimators=10))
    ranker.fit(features, labels)

    test_date = "2024-01-01"
    test_features = [r for r in features if r.date == test_date]
    scores = ranker.score(test_features, test_date)
    rank_vals = [s.rank_score for s in scores]

    assert min(rank_vals) == pytest.approx(0.0, abs=1e-9)
    assert max(rank_vals) == pytest.approx(1.0, abs=1e-9)


def test_ranker_score_raises_when_not_fitted() -> None:
    ranker = XGBoostRanker()
    with pytest.raises(RuntimeError, match="fit"):
        ranker.score([], "2024-01-01")


def test_ranker_fit_raises_on_empty_overlap() -> None:
    features = [_make_feature_record("2024-01-01", "AAPL")]
    labels = [_make_label_record("2024-02-01", "AAPL", 0.01)]  # no date overlap
    ranker = XGBoostRanker(XGBoostRankerConfig(n_estimators=10))
    with pytest.raises(ValueError, match="No training samples"):
        ranker.fit(features, labels)


def test_ranker_serialization_round_trip() -> None:
    features, labels = _make_training_pair(n_dates=5, n_tickers=4)
    original = XGBoostRanker(XGBoostRankerConfig(n_estimators=10))
    original.fit(features, labels)

    payload = original.to_bytes()
    restored = XGBoostRanker.from_bytes(payload)

    assert restored.model_version == original.model_version
    assert restored.feature_columns == original.feature_columns
    assert restored.is_fitted

    test_date = "2024-01-01"
    test_features = [r for r in features if r.date == test_date]
    scores_orig = original.score(test_features, test_date)
    scores_rest = restored.score(test_features, test_date)

    assert len(scores_orig) == len(scores_rest)
    for s1, s2 in zip(scores_orig, scores_rest):
        assert s1.ticker == s2.ticker
        assert s1.return_score == pytest.approx(s2.return_score, abs=1e-6)


def test_ranker_to_bytes_raises_when_not_fitted() -> None:
    ranker = XGBoostRanker()
    with pytest.raises(RuntimeError, match="unfitted"):
        ranker.to_bytes()


def test_ranker_from_bytes_raises_on_bad_schema() -> None:
    import pickle

    bad_bundle = {"schema": "unknown_v99", "xgb_model_bytes": b"", "feature_columns": []}
    with pytest.raises(ValueError, match="schema"):
        XGBoostRanker.from_bytes(pickle.dumps(bad_bundle))


def test_ranker_excludes_string_features_from_matrix() -> None:
    """Regime label (string) must not appear in feature columns."""
    features = [_make_feature_record("2024-01-01", f"TK{i}", regime="bull") for i in range(5)]
    labels = [_make_label_record("2024-01-01", f"TK{i}", 0.01 * i) for i in range(5)]
    features += [_make_feature_record("2024-01-02", f"TK{i}", regime="bear") for i in range(5)]
    labels += [_make_label_record("2024-01-02", f"TK{i}", -0.01 * i) for i in range(5)]

    ranker = XGBoostRanker(XGBoostRankerConfig(n_estimators=10))
    ranker.fit(features, labels)
    assert "regime_label" not in ranker.feature_columns
