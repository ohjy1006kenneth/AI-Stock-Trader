# Backtest Report

Generated at: 2026-03-24T19:04:21.883909+00:00

## Simulation Rules
- Rebalance frequency: monthly
- CORE entries: quality pass + alpha >= 0.65 + trend filter
- SWING entries: alpha >= 0.80 + trend filter
- SWING exits: 10% trailing stop, 15% take-profit, 20-day max hold, signal decay < 0.35
- CORE exits: quality decay placeholder based on current fundamental screen
- Daily marking with same-day close approximation

## Assumptions
- Transaction cost: 10 bps per side
- Slippage: 5 bps per side
- Starting cash: 100000.0
- Benchmark: SPY

## Results Snapshot
- CAGR: -0.11746012483886958
- Annualized volatility: 0.10340323002174924
- Sharpe ratio: -1.1666552599410398
- Max drawdown: -0.09249114034427741
- Number of trades: 5
- Turnover: 0.5920047162882125
- Benchmark-relative comparison: -0.049177999999999944

## Limitations
- Survivorship bias is still not solved.
- yfinance is still fallback data.
- Fundamentals are not point-in-time clean.
- Same-day close execution is unrealistic for live implementation.
- This is a research backtest, not evidence of tradable profitability.

## Bottom Line
- V1 verdict: weak_or_inconclusive
- Treat any positive result as provisional until bias controls and data quality improve.
