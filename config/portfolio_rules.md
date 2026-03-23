# Portfolio Rules

## CORE Sleeve

### Entry Requirements
A stock may enter CORE only if:
1. it passes the quality filter,
2. it has approved alpha support,
3. it satisfies liquidity and position limit rules,
4. it is not blocked by a data-quality warning.

### Intended Holding Period
- months or longer

### Sell Logic
Sell only on rule-based deterioration, for example:
- net margin below configured threshold,
- leverage / debt quality deterioration beyond threshold,
- free-cash-flow quality deterioration,
- quality score dropping below minimum production threshold,
- hard risk limit breach.

### Non-Rules
- no forced end-of-day liquidation
- do not sell on ordinary daily noise alone

## SWING Sleeve

### Entry Requirements
A stock may enter SWING if:
1. approved alpha score is strong,
2. liquidity sanity checks pass,
3. event risk checks pass where possible,
4. it is not blocked by a data-quality warning.

### Intended Holding Period
- roughly 3 to 20 trading days

### Default Exit Rules
- trailing stop-loss: 10%
- optional take-profit: 12% to 15%
- max holding period: configurable, default 20 trading days
- optional momentum fade / signal decay exit
- avoid new entries right before earnings when possible

### Non-Rules
- no forced close at market close

## Portfolio Limits
Suggested initial defaults:
- max total positions: 15
- max CORE weight per position: 12%
- max SWING weight per position: 6%
- max sleeve allocation CORE: 70%
- max sleeve allocation SWING: 30%
- maintain cash buffer: 5%

## V1 Deterministic Ranking Scaffold
- Alpha score = 0.45 * momentum_rank + 0.20 * low_vol_rank + 0.20 * trend_bonus + 0.15 * quality_proxy
- `momentum_rank` is based on 12-1 momentum
- `low_vol_rank` rewards lower 30-day realized volatility
- `trend_bonus` = 1 if close > 200-day simple moving average, else 0
- `quality_proxy` = 1 if profitability proxy is available and positive, else 0

This ranking is a conservative scaffold for research operations, not a profitability claim.

## Mutation Boundary
- The Strategist may only write structured decisions.
- The Strategist must never mutate the portfolio ledger directly.
- Only the deterministic Mock Portfolio Executor may update `ledger/mock_portfolio.json`.
- All execution outcomes must be written to `outputs/execution_log.json`.
