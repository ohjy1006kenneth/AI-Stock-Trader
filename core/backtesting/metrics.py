"""Pure-Python performance metrics for backtesting.

All functions operate on plain Python lists of floats so they can run in any
environment without pandas or numpy. Inputs are daily portfolio returns
(proportional, e.g. 0.01 = 1 % gain).
"""
from __future__ import annotations

import math


def sharpe_ratio(daily_returns: list[float], annual_factor: float = 252.0) -> float:
    """Annualized Sharpe ratio (zero risk-free rate).

    Returns 0.0 when there are fewer than 2 observations or zero variance.
    """
    n = len(daily_returns)
    if n < 2:
        return 0.0
    mean = sum(daily_returns) / n
    variance = sum((r - mean) ** 2 for r in daily_returns) / (n - 1)
    stdev = math.sqrt(max(variance, 0.0))
    if stdev == 0.0:
        return 0.0
    # Suppress near-zero variance caused by floating-point accumulation on
    # constant return streams (coefficient of variation below machine-noise floor).
    if abs(mean) > 0.0 and stdev / abs(mean) < 1e-9:
        return 0.0
    return (mean / stdev) * math.sqrt(annual_factor)


def max_drawdown(daily_returns: list[float]) -> float:
    """Maximum peak-to-trough drawdown from a sequence of daily returns.

    Returns a positive fraction (e.g. 0.15 = 15 % drawdown).
    """
    peak = 1.0
    equity = 1.0
    max_dd = 0.0
    for r in daily_returns:
        equity *= 1.0 + r
        if equity > peak:
            peak = equity
        dd = (peak - equity) / peak if peak > 0 else 0.0
        if dd > max_dd:
            max_dd = dd
    return max_dd


def annual_return(daily_returns: list[float], annual_factor: float = 252.0) -> float:
    """Annualized geometric return from daily returns.

    Returns 0.0 for an empty sequence.
    """
    n = len(daily_returns)
    if n == 0:
        return 0.0
    equity = 1.0
    for r in daily_returns:
        equity *= 1.0 + r
    if equity <= 0:
        return -1.0
    return equity ** (annual_factor / n) - 1.0


def hit_rate(daily_returns: list[float]) -> float:
    """Fraction of trading days with a strictly positive portfolio return."""
    if not daily_returns:
        return 0.0
    return sum(1 for r in daily_returns if r > 0.0) / len(daily_returns)


def information_coefficient(
    scores: list[float],
    realized_returns: list[float],
) -> float:
    """Spearman rank correlation between model scores and realized returns.

    The IC measures how well the model's cross-sectional ranking predicts
    actual return ordering. Returns 0.0 when inputs are empty or mismatched.
    """
    n = len(scores)
    if n < 2 or len(realized_returns) != n:
        return 0.0

    rank_s = _rank_values(scores)
    rank_r = _rank_values(realized_returns)
    mean_rank = (n - 1) / 2.0

    cov = sum((rank_s[i] - mean_rank) * (rank_r[i] - mean_rank) for i in range(n)) / n
    var = sum((rank_s[i] - mean_rank) ** 2 for i in range(n)) / n

    return cov / var if var > 0.0 else 0.0


def turnover(prev_tickers: set[str], next_tickers: set[str]) -> float:
    """One-way portfolio turnover as a fraction of the portfolio.

    Computed as the number of positions entered (or equivalently exited, since
    a balanced rebalance has equal buys and sells) divided by the larger
    portfolio size. A full replacement from N to N different stocks = 1.0.
    """
    total = max(len(prev_tickers), len(next_tickers))
    if total == 0:
        return 0.0
    entered = len(next_tickers - prev_tickers)
    return entered / total


def _rank_values(values: list[float]) -> list[float]:
    """Return average (midrank) ranks for a list of floats.

    Tied values receive the mean of the ranks they would otherwise occupy,
    so a list of identical values produces a constant rank equal to (n-1)/2.
    This is the correct treatment for Spearman rank correlation.
    """
    n = len(values)
    sorted_indices = sorted(range(n), key=lambda i: values[i])

    ranks = [0.0] * n
    i = 0
    while i < n:
        j = i
        while j < n and values[sorted_indices[j]] == values[sorted_indices[i]]:
            j += 1
        avg_rank = (i + j - 1) / 2.0
        for k in range(i, j):
            ranks[sorted_indices[k]] = avg_rank
        i = j
    return ranks
