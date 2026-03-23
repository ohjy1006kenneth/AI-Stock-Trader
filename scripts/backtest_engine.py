from __future__ import annotations

from common import BACKTESTS_DIR, now_iso, write_json, write_text


def main() -> None:
    metrics = {
        "generated_at": now_iso(),
        "status": "placeholder",
        "benchmark": "SPY",
        "metrics": {
            "cagr": None,
            "annualized_volatility": None,
            "sharpe_ratio": None,
            "max_drawdown": None,
            "win_rate": None,
            "number_of_trades": None,
            "average_holding_period_days": None,
            "turnover": None,
            "benchmark_relative_return": None
        }
    }
    write_json(BACKTESTS_DIR / "metrics.json", metrics)
    write_text(BACKTESTS_DIR / "backtest_report.md", "# Backtest Report\n\nPlaceholder run only.\n")
    print("Backtest placeholder written")


if __name__ == "__main__":
    main()
