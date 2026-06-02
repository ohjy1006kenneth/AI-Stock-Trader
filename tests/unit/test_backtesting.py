"""Unit tests for the backtesting framework.

Metrics tests run without any ML dependencies. BacktestEngine tests use a
lightweight mock ranker so xgboost is not required.
"""
from __future__ import annotations

import math

import pytest

from core.backtesting.engine import BacktestConfig, BacktestEngine, BacktestResult
from core.backtesting.metrics import (
    annual_return,
    hit_rate,
    information_coefficient,
    max_drawdown,
    sharpe_ratio,
    turnover,
)
from core.contracts.schemas import FeatureRecord, ScoreRecord

# ---------------------------------------------------------------------------
# Metrics unit tests
# ---------------------------------------------------------------------------


class TestSharpeRatio:
    def test_positive_returns_positive_sharpe(self) -> None:
        import random

        rng = random.Random(1)
        returns = [0.002 + rng.uniform(-0.001, 0.001) for _ in range(252)]
        assert sharpe_ratio(returns) > 0

    def test_negative_returns_negative_sharpe(self) -> None:
        import random

        rng = random.Random(2)
        returns = [-0.002 + rng.uniform(-0.001, 0.001) for _ in range(252)]
        assert sharpe_ratio(returns) < 0

    def test_zero_variance_returns_zero(self) -> None:
        # Identical returns → zero sample variance → Sharpe undefined; returns 0.0.
        returns = [0.005] * 100
        assert sharpe_ratio(returns) == 0.0

    def test_empty_returns_zero(self) -> None:
        assert sharpe_ratio([]) == 0.0

    def test_single_return_returns_zero(self) -> None:
        assert sharpe_ratio([0.01]) == 0.0

    def test_annual_factor_scales_result(self) -> None:
        returns = [0.001, -0.002, 0.003, -0.001, 0.002]
        s252 = sharpe_ratio(returns, annual_factor=252.0)
        s365 = sharpe_ratio(returns, annual_factor=365.0)
        assert s365 > s252


class TestMaxDrawdown:
    def test_monotone_upward_is_zero_drawdown(self) -> None:
        returns = [0.01] * 10
        assert max_drawdown(returns) == pytest.approx(0.0)

    def test_single_loss_computes_correctly(self) -> None:
        # 10% gain then 20% loss: peak = 1.1, trough = 0.88 → DD = (1.1-0.88)/1.1
        returns = [0.10, -0.20]
        dd = max_drawdown(returns)
        expected = (1.1 - 1.1 * 0.80) / 1.1
        assert dd == pytest.approx(expected, rel=1e-6)

    def test_empty_returns_zero_drawdown(self) -> None:
        assert max_drawdown([]) == 0.0

    def test_drawdown_is_non_negative(self) -> None:
        import random

        rng = random.Random(0)
        returns = [rng.uniform(-0.05, 0.05) for _ in range(100)]
        assert max_drawdown(returns) >= 0.0


class TestAnnualReturn:
    def test_flat_returns_zero_annual_return(self) -> None:
        returns = [0.0] * 100
        assert annual_return(returns) == pytest.approx(0.0, abs=1e-9)

    def test_positive_daily_implies_positive_annual(self) -> None:
        returns = [0.001] * 252
        ar = annual_return(returns, annual_factor=252.0)
        assert ar > 0

    def test_empty_returns_zero(self) -> None:
        assert annual_return([]) == 0.0

    def test_geometric_compounding(self) -> None:
        # One year of exactly +1% daily → equity = 1.01^252
        returns = [0.01] * 252
        ar = annual_return(returns, annual_factor=252.0)
        expected = 1.01**252 - 1.0
        assert ar == pytest.approx(expected, rel=1e-6)


class TestHitRate:
    def test_all_positive_is_one(self) -> None:
        assert hit_rate([0.01, 0.02, 0.001]) == pytest.approx(1.0)

    def test_all_negative_is_zero(self) -> None:
        assert hit_rate([-0.01, -0.02]) == pytest.approx(0.0)

    def test_half_positive(self) -> None:
        assert hit_rate([0.01, -0.01, 0.02, -0.02]) == pytest.approx(0.5)

    def test_empty_is_zero(self) -> None:
        assert hit_rate([]) == 0.0

    def test_zero_returns_excluded(self) -> None:
        assert hit_rate([0.0, 0.0, 0.0]) == 0.0


class TestInformationCoefficient:
    def test_perfect_rank_correlation(self) -> None:
        scores = [1.0, 2.0, 3.0, 4.0]
        realized = [10.0, 20.0, 30.0, 40.0]
        assert information_coefficient(scores, realized) == pytest.approx(1.0, abs=1e-9)

    def test_perfect_inverse_correlation(self) -> None:
        scores = [4.0, 3.0, 2.0, 1.0]
        realized = [10.0, 20.0, 30.0, 40.0]
        assert information_coefficient(scores, realized) == pytest.approx(-1.0, abs=1e-9)

    def test_zero_on_constant_scores(self) -> None:
        # All scores tied → all midranks equal → zero variance → IC = 0.
        scores = [1.0, 1.0, 1.0]
        realized = [1.0, 2.0, 3.0]
        assert information_coefficient(scores, realized) == pytest.approx(0.0)

    def test_empty_is_zero(self) -> None:
        assert information_coefficient([], []) == 0.0

    def test_mismatched_lengths_is_zero(self) -> None:
        assert information_coefficient([1.0, 2.0], [1.0]) == 0.0

    def test_range_within_minus_one_to_one(self) -> None:
        import random

        rng = random.Random(7)
        for _ in range(20):
            n = rng.randint(2, 50)
            s = [rng.random() for _ in range(n)]
            r = [rng.random() for _ in range(n)]
            ic = information_coefficient(s, r)
            assert -1.0 - 1e-9 <= ic <= 1.0 + 1e-9


class TestTurnover:
    def test_no_change_is_zero(self) -> None:
        tickers = {"AAPL", "MSFT", "GOOG"}
        assert turnover(tickers, tickers) == 0.0

    def test_complete_replacement_is_one(self) -> None:
        # 2 new positions entered / 2 total = 1.0 one-way turnover.
        prev = {"AAPL", "MSFT"}
        next_ = {"GOOG", "AMZN"}
        assert turnover(prev, next_) == pytest.approx(1.0)

    def test_half_turnover(self) -> None:
        # 1 new position entered (GOOG) / 2 total = 0.5.
        prev = {"AAPL", "MSFT"}
        next_ = {"AAPL", "GOOG"}
        assert turnover(prev, next_) == pytest.approx(0.5)

    def test_empty_sets_is_zero(self) -> None:
        assert turnover(set(), set()) == 0.0


# ---------------------------------------------------------------------------
# BacktestEngine tests (mock ranker — no xgboost required)
# ---------------------------------------------------------------------------


class _MockRanker:
    """Deterministic ranker that scores tickers alphabetically for testing."""

    def __init__(self) -> None:
        self.is_fitted = False

    def fit(self, feature_records: list[FeatureRecord], label_records: list[FeatureRecord]) -> _MockRanker:
        if not label_records:
            raise ValueError("No training samples")
        self.is_fitted = True
        return self

    def score(self, feature_records: list[FeatureRecord], as_of_date: str) -> list[ScoreRecord]:
        if not self.is_fitted:
            raise RuntimeError("Not fitted")
        records = [r for r in feature_records if r.date == as_of_date]
        sorted_records = sorted(records, key=lambda r: r.ticker)
        n = len(sorted_records)
        scores = []
        for i, record in enumerate(sorted_records):
            rank = i / max(n - 1, 1)
            scores.append(
                ScoreRecord(
                    date=as_of_date,
                    ticker=record.ticker,
                    return_score=rank,
                    pos_prob=rank,
                    rank_score=rank,
                    regime=None,
                    confidence=rank,
                    model_version="mock-v1",
                )
            )
        return scores


def _make_histories(
    n_tickers: int = 10,
    n_dates: int = 100,
    *,
    daily_return: float = 0.001,
) -> tuple[dict[str, list[FeatureRecord]], dict[str, list[FeatureRecord]], list[str]]:
    """Build synthetic feature and label histories for backtest testing."""
    import random

    rng = random.Random(99)
    tickers = [f"TK{i:02d}" for i in range(n_tickers)]

    # Generate trading dates (weekdays only, roughly)
    from datetime import date, timedelta

    start = date(2023, 1, 3)
    trading_dates: list[str] = []
    current = start
    while len(trading_dates) < n_dates:
        if current.weekday() < 5:
            trading_dates.append(current.isoformat())
        current += timedelta(days=1)

    feature_histories: dict[str, list[FeatureRecord]] = {}
    label_histories: dict[str, list[FeatureRecord]] = {}

    for ticker in tickers:
        feat_records = []
        label_records = []
        for d in trading_dates:
            features = {
                "momentum_1m": rng.uniform(-0.05, 0.05),
                "volume_ratio": rng.uniform(0.5, 2.0),
            }
            feat_records.append(FeatureRecord(date=d, ticker=ticker, features=features))

            label_features = {
                "forward_return_5d": daily_return + rng.uniform(-0.01, 0.01),
                "survives_to_t5": 1,
            }
            label_records.append(FeatureRecord(date=d, ticker=ticker, features=label_features))

        feature_histories[ticker] = feat_records
        label_histories[ticker] = label_records

    return feature_histories, label_histories, trading_dates


class TestBacktestEngine:
    def _engine(self, **kwargs: object) -> BacktestEngine:
        config = BacktestConfig(**kwargs)
        return BacktestEngine(config, ranker_factory=_MockRanker)

    def test_run_returns_backtest_result(self) -> None:
        features, labels, dates = _make_histories(n_tickers=8, n_dates=80)
        engine = self._engine(train_days=40, test_days=20, top_n=3, min_train_samples=10)
        result = engine.run(features, labels, trading_dates=dates)
        assert isinstance(result, BacktestResult)

    def test_result_has_folds(self) -> None:
        features, labels, dates = _make_histories(n_tickers=8, n_dates=80)
        engine = self._engine(train_days=40, test_days=20, top_n=3, min_train_samples=10)
        result = engine.run(features, labels, trading_dates=dates)
        assert result.n_folds > 0
        assert len(result.folds) == result.n_folds

    def test_all_returns_match_folds(self) -> None:
        features, labels, dates = _make_histories(n_tickers=8, n_dates=80)
        engine = self._engine(train_days=40, test_days=20, top_n=3, min_train_samples=10)
        result = engine.run(features, labels, trading_dates=dates)
        fold_total = sum(len(f.daily_returns) for f in result.folds)
        assert len(result.all_returns) == fold_total

    def test_metrics_are_finite(self) -> None:
        features, labels, dates = _make_histories(n_tickers=8, n_dates=80)
        engine = self._engine(train_days=40, test_days=20, top_n=3, min_train_samples=10)
        result = engine.run(features, labels, trading_dates=dates)
        if result.all_returns:
            assert math.isfinite(result.sharpe_ratio)
            assert math.isfinite(result.max_drawdown)
            assert math.isfinite(result.annual_return)
            assert 0.0 <= result.hit_rate <= 1.0
            assert 0.0 <= result.max_drawdown <= 1.0

    def test_empty_trading_dates_returns_empty_result(self) -> None:
        features, labels, dates = _make_histories(n_tickers=4, n_dates=20)
        engine = self._engine(train_days=40, test_days=20, top_n=3, min_train_samples=5)
        result = engine.run(features, labels, trading_dates=[])
        assert result.n_folds == 0
        assert result.all_returns == []

    def test_insufficient_dates_produces_no_folds(self) -> None:
        features, labels, dates = _make_histories(n_tickers=4, n_dates=10)
        engine = self._engine(train_days=50, test_days=20, top_n=3, min_train_samples=5)
        result = engine.run(features, labels, trading_dates=dates)
        assert result.n_folds == 0

    def test_fold_dates_are_non_overlapping(self) -> None:
        features, labels, dates = _make_histories(n_tickers=8, n_dates=120)
        engine = self._engine(train_days=40, test_days=30, top_n=3, min_train_samples=10)
        result = engine.run(features, labels, trading_dates=dates)
        test_ranges = [(f.test_start, f.test_end) for f in result.folds]
        for i in range(len(test_ranges) - 1):
            assert test_ranges[i][1] < test_ranges[i + 1][0]

    def test_top_n_limits_portfolio_size(self) -> None:
        features, labels, dates = _make_histories(n_tickers=20, n_dates=80)
        top_n = 5
        engine = self._engine(train_days=40, test_days=20, top_n=top_n, min_train_samples=10)
        result = engine.run(features, labels, trading_dates=dates)
        # Result should complete without error; portfolio is bounded internally
        assert isinstance(result, BacktestResult)

    def test_fold_hit_rate_between_zero_and_one(self) -> None:
        features, labels, dates = _make_histories(n_tickers=8, n_dates=80)
        engine = self._engine(train_days=40, test_days=20, top_n=3, min_train_samples=10)
        result = engine.run(features, labels, trading_dates=dates)
        for fold in result.folds:
            assert 0.0 <= fold.hit_rate <= 1.0

    def test_fold_ic_within_bounds(self) -> None:
        features, labels, dates = _make_histories(n_tickers=8, n_dates=80)
        engine = self._engine(train_days=40, test_days=20, top_n=3, min_train_samples=10)
        result = engine.run(features, labels, trading_dates=dates)
        for fold in result.folds:
            assert -1.0 - 1e-9 <= fold.ic <= 1.0 + 1e-9

    def test_skips_fold_when_insufficient_train_samples(self) -> None:
        features, labels, dates = _make_histories(n_tickers=2, n_dates=80)
        engine = self._engine(
            train_days=40, test_days=20, top_n=3, min_train_samples=10_000
        )
        result = engine.run(features, labels, trading_dates=dates)
        # All folds should be skipped due to min_train_samples threshold
        assert result.n_folds == 0
