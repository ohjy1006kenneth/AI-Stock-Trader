import unittest

from cloud_training.backtesting.validation_metrics import (
    PredictionRow,
    _brier_score,
    _calibration_buckets,
    _max_drawdown,
    _turnover,
    build_walk_forward_summary,
    evaluate_promotion,
)
from cloud_training.data_pipelines.build_predictive_dataset import select_tickers


class ValidationMetricsTests(unittest.TestCase):
    def test_max_drawdown_is_positive_fraction(self):
        returns = [0.10, -0.20, 0.05, -0.10]
        drawdown = _max_drawdown(returns)
        self.assertGreater(drawdown, 0.0)
        self.assertLess(drawdown, 1.0)

    def test_turnover_uses_half_gross_weight_change(self):
        history = [
            {"AAPL": 0.5, "MSFT": 0.5},
            {"AAPL": 0.25, "MSFT": 0.75},
        ]
        self.assertEqual(_turnover(history), 0.25)

    def test_walk_forward_reports_insufficient_history_when_scaffold_cannot_form_window(self):
        dates = [f"2025-01-{day:02d}" for day in range(1, 29)]
        summary = build_walk_forward_summary(dates, train_years=2, test_months=6)
        self.assertEqual(summary["window_count"], 0)
        self.assertFalse(summary["sufficient_history"])

    def test_promotion_requires_sharpe_drawdown_benchmark_and_window(self):
        portfolio_metrics = {
            "trading_days": 252,
            "sharpe": 1.3,
            "max_drawdown": 0.10,
            "benchmark_available": False,
            "excess_return_vs_spy": None,
        }
        walk_forward = {"window_count": 0}
        promotion = evaluate_promotion(portfolio_metrics, walk_forward)
        self.assertFalse(promotion["promote"])
        self.assertFalse(promotion["checks"]["has_spy_benchmark"])
        self.assertFalse(promotion["checks"]["has_walk_forward_window"])

    def test_calibration_outputs_bucket_summary_and_brier_score(self):
        predictions = [
            PredictionRow("AAPL", "2025-01-01", "2025-01-02", 0.01, 1, 0.9, 0.8, 0.01, 0.5),
            PredictionRow("MSFT", "2025-01-01", "2025-01-02", -0.02, 0, 0.2, 0.9, 0.02, 0.2),
        ]
        buckets = _calibration_buckets(predictions, buckets=5)
        self.assertEqual(len(buckets), 5)
        self.assertGreater(_brier_score(predictions), 0.0)

    def test_select_tickers_always_keeps_spy(self):
        tickers = select_tickers(["AAPL", "MSFT", "SPY", "GOOGL"], max_tickers=2)
        self.assertIn("SPY", tickers)
        self.assertEqual(len(tickers), 3)


if __name__ == "__main__":
    unittest.main()
