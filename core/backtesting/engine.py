"""Walk-forward backtesting engine for Layer 2 model validation.

The engine slices the feature/label archive into non-overlapping train and
test folds, fits a fresh XGBoostRanker on each train window, and simulates
an equal-weight top-N portfolio on each test date. All scoring is done with
data available strictly before the test date, so no look-ahead leakage
occurs.

Typical usage::

    from core.backtesting import BacktestEngine, BacktestConfig

    engine = BacktestEngine(BacktestConfig(train_days=252, test_days=63, top_n=20))
    result = engine.run(feature_histories, label_histories, trading_dates=dates)
    print(f"Sharpe: {result.sharpe_ratio:.2f}  MaxDD: {result.max_drawdown:.1%}")
"""
from __future__ import annotations

import math
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from core.backtesting.metrics import (
    annual_return,
    hit_rate,
    information_coefficient,
    max_drawdown,
    sharpe_ratio,
    turnover,
)
from core.contracts.schemas import FeatureRecord, ScoreRecord

TARGET_LABEL = "forward_return_5d"


@dataclass(frozen=True)
class BacktestConfig:
    """Configuration for one walk-forward backtest run."""

    train_days: int = 252
    """Number of trading days in each rolling training window."""

    test_days: int = 63
    """Number of trading days in each out-of-sample test fold."""

    top_n: int = 20
    """Number of stocks held in the equal-weight portfolio on each test date."""

    label_horizon: str = TARGET_LABEL
    """Label key used both as the training target and for daily PnL realisation."""

    min_train_samples: int = 50
    """Minimum rows required to fit the model; folds with fewer rows are skipped."""


@dataclass
class FoldResult:
    """Per-fold out-of-sample metrics."""

    fold_index: int
    train_start: str
    train_end: str
    test_start: str
    test_end: str
    daily_returns: list[float]
    hit_rate: float
    ic: float
    avg_turnover: float
    n_test_dates: int


@dataclass
class BacktestResult:
    """Aggregated walk-forward backtest results."""

    config: BacktestConfig
    folds: list[FoldResult]
    all_dates: list[str]
    all_returns: list[float]
    sharpe_ratio: float
    max_drawdown: float
    annual_return: float
    hit_rate: float
    ic: float
    avg_turnover: float
    n_folds: int


class BacktestEngine:
    """Walk-forward backtest engine that refits the model on each train window.

    Args:
        config: Backtest configuration (window sizes, portfolio size).
        ranker_factory: Optional callable that returns a fresh unfitted ranker.
            Defaults to ``XGBoostRanker`` with default config. Useful in tests
            to inject a lightweight mock without requiring xgboost.
    """

    def __init__(
        self,
        config: BacktestConfig | None = None,
        *,
        ranker_factory: Callable[[], Any] | None = None,
    ) -> None:
        self.config = config or BacktestConfig()
        self._ranker_factory = ranker_factory or _default_xgb_factory

    def run(
        self,
        feature_histories: dict[str, list[FeatureRecord]],
        label_histories: dict[str, list[FeatureRecord]],
        *,
        trading_dates: list[str],
    ) -> BacktestResult:
        """Execute the walk-forward simulation.

        Args:
            feature_histories: ``{ticker: [FeatureRecord, ...]}`` covering the
                full evaluation period.
            label_histories: ``{ticker: [FeatureRecord, ...]}`` from the label
                archive; each record's ``features`` dict must contain the
                ``label_horizon`` key (e.g. ``forward_return_5d``).
            trading_dates: All trading calendar dates in the evaluation period,
                sorted ascending (YYYY-MM-DD strings).

        Returns:
            A ``BacktestResult`` with per-fold and aggregate metrics.
        """
        dates = sorted(set(trading_dates))
        n_dates = len(dates)

        # Flatten histories to fast lookup: (date, ticker) -> features/labels
        feat_lookup: dict[str, dict[str, FeatureRecord]] = _index_by_date(feature_histories)
        label_lookup: dict[str, dict[str, FeatureRecord]] = _index_by_date(label_histories)

        folds: list[FoldResult] = []
        all_dates: list[str] = []
        all_returns: list[float] = []

        step = self.config.test_days
        train_end_idx = self.config.train_days - 1

        fold_index = 0
        while train_end_idx < n_dates:
            test_start_idx = train_end_idx + 1
            test_end_idx = min(test_start_idx + step - 1, n_dates - 1)

            if test_start_idx >= n_dates:
                break

            train_dates = dates[: train_end_idx + 1]
            test_dates = dates[test_start_idx : test_end_idx + 1]

            fold = self._run_fold(
                fold_index=fold_index,
                train_dates=train_dates,
                test_dates=test_dates,
                feat_lookup=feat_lookup,
                label_lookup=label_lookup,
            )
            if fold is not None:
                folds.append(fold)
                all_dates.extend(test_dates[: len(fold.daily_returns)])
                all_returns.extend(fold.daily_returns)

            train_end_idx += step
            fold_index += 1

        if not all_returns:
            return BacktestResult(
                config=self.config,
                folds=folds,
                all_dates=[],
                all_returns=[],
                sharpe_ratio=0.0,
                max_drawdown=0.0,
                annual_return=0.0,
                hit_rate=0.0,
                ic=0.0,
                avg_turnover=0.0,
                n_folds=len(folds),
            )

        avg_ic = (
            sum(f.ic for f in folds) / len(folds) if folds else 0.0
        )
        avg_to = (
            sum(f.avg_turnover for f in folds) / len(folds) if folds else 0.0
        )

        return BacktestResult(
            config=self.config,
            folds=folds,
            all_dates=all_dates,
            all_returns=all_returns,
            sharpe_ratio=sharpe_ratio(all_returns),
            max_drawdown=max_drawdown(all_returns),
            annual_return=annual_return(all_returns),
            hit_rate=hit_rate(all_returns),
            ic=avg_ic,
            avg_turnover=avg_to,
            n_folds=len(folds),
        )

    def _run_fold(
        self,
        *,
        fold_index: int,
        train_dates: list[str],
        test_dates: list[str],
        feat_lookup: dict[str, dict[str, FeatureRecord]],
        label_lookup: dict[str, dict[str, FeatureRecord]],
    ) -> FoldResult | None:
        """Fit the model on the training window and simulate the test window."""
        # Collect train feature and label records
        train_features: list[FeatureRecord] = []
        train_labels: list[FeatureRecord] = []
        for d in train_dates:
            for ticker, record in (feat_lookup.get(d) or {}).items():
                train_features.append(record)
                label_rec = (label_lookup.get(d) or {}).get(ticker)
                if label_rec is not None:
                    train_labels.append(label_rec)

        if len(train_features) < self.config.min_train_samples:
            return None

        ranker = self._ranker_factory()
        try:
            ranker.fit(train_features, train_labels)
        except (ValueError, RuntimeError):
            return None

        # Simulate portfolio on test dates
        daily_returns: list[float] = []
        all_scores_flat: list[float] = []
        all_realized_flat: list[float] = []
        daily_turnovers: list[float] = []
        prev_tickers: set[str] = set()

        for test_date in test_dates:
            date_features = list((feat_lookup.get(test_date) or {}).values())
            if not date_features:
                continue

            scores: list[ScoreRecord] = ranker.score(date_features, test_date)
            if not scores:
                continue

            # Top-N by rank_score
            scores.sort(key=lambda s: s.rank_score, reverse=True)
            top_scores = scores[: self.config.top_n]
            top_tickers = {s.ticker for s in top_scores}

            # Compute portfolio return as equal-weight mean of realized returns
            date_labels = label_lookup.get(test_date) or {}
            position_returns: list[float] = []
            for score in top_scores:
                label_rec = date_labels.get(score.ticker)
                if label_rec is None:
                    continue
                raw = label_rec.features.get(self.config.label_horizon)
                if raw is None:
                    continue
                try:
                    realized = float(raw)
                except (TypeError, ValueError):
                    continue
                if not math.isfinite(realized):
                    continue
                position_returns.append(realized)
                all_scores_flat.append(score.rank_score)
                all_realized_flat.append(realized)

            if not position_returns:
                continue

            port_return = sum(position_returns) / len(position_returns)
            daily_returns.append(port_return)
            daily_turnovers.append(turnover(prev_tickers, top_tickers))
            prev_tickers = top_tickers

        if not daily_returns:
            return None

        fold_ic = information_coefficient(all_scores_flat, all_realized_flat)
        fold_to = sum(daily_turnovers) / len(daily_turnovers) if daily_turnovers else 0.0

        return FoldResult(
            fold_index=fold_index,
            train_start=train_dates[0],
            train_end=train_dates[-1],
            test_start=test_dates[0],
            test_end=test_dates[-1],
            daily_returns=daily_returns,
            hit_rate=hit_rate(daily_returns),
            ic=fold_ic,
            avg_turnover=fold_to,
            n_test_dates=len(daily_returns),
        )


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _index_by_date(
    histories: dict[str, list[FeatureRecord]],
) -> dict[str, dict[str, FeatureRecord]]:
    """Build a {date: {ticker: record}} lookup from ticker-keyed histories."""
    by_date: dict[str, dict[str, FeatureRecord]] = {}
    for ticker, records in histories.items():
        for record in records:
            by_date.setdefault(record.date, {})[ticker] = record
    return by_date


def _default_xgb_factory() -> Any:
    """Lazy factory so importing engine.py never triggers the xgboost import chain."""
    from core.models.xgboost_ranker import XGBoostRanker

    return XGBoostRanker()
