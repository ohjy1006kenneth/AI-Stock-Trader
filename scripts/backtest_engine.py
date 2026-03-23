from __future__ import annotations

from common import BACKTESTS_DIR, OUTPUTS_DIR, now_iso, read_json, write_json, write_text

ASSUMPTIONS = {
    "benchmark": "SPY",
    "transaction_cost_bps": 10,
    "slippage_bps": 5,
    "rebalancing": "monthly_placeholder",
    "bias_controls": [
        "no look-ahead fields in factor formulas",
        "only uses trailing history available at decision time",
        "survivorship bias not fully solved in V1 static universe"
    ]
}


def main() -> None:
    rankings = read_json(OUTPUTS_DIR / "alpha_rankings.json", {"items": []})
    qualified = read_json(OUTPUTS_DIR / "qualified_universe.json", {"items": []})
    metrics = {
        "strategy_version": "v1",
        "benchmark": ASSUMPTIONS["benchmark"],
        "generated_at": now_iso(),
        "metrics": {
            "cagr": None,
            "annualized_volatility": None,
            "sharpe_ratio": None,
            "max_drawdown": None,
            "win_rate": None,
            "number_of_trades": None,
            "average_holding_period": None,
            "turnover": None,
            "benchmark_relative_comparison": None,
        },
        "yearly_breakdown": [],
        "assumptions": ASSUMPTIONS,
        "status": "placeholder_with_explicit_assumptions"
    }
    write_json(BACKTESTS_DIR / "metrics.json", metrics)
    write_text(BACKTESTS_DIR / "backtest_report.md", "# Backtest Report\n\nThis V1 file is intentionally conservative.\n\nWhat is implemented now:\n- explicit benchmark and transaction-cost placeholders\n- explicit bias-control notes\n- machine-readable metrics skeleton\n\nWhat is not yet implemented:\n- full walk-forward engine\n- survivorship-bias mitigation\n- realistic historical constituent reconstruction\n- trustworthy benchmark series ingestion\n\nCurrent alpha item count: %d\nCurrent qualified universe count: %d\n" % (len(rankings.get("items", [])), len(qualified.get("items", []))))
    print("Backtest report placeholder-with-assumptions written")


if __name__ == "__main__":
    main()
