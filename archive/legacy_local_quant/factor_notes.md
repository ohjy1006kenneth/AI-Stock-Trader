# Factor Notes

These notes describe the factor logic that is actually implemented today in deterministic Python. This is meant to remove the "mystery meat" feeling around the current model.

## Current production scoring logic

The current V1 alpha ranking is a weighted blend of:
- 12-1 momentum
- lower 30-day realized volatility
- a 200-day trend check
- a quality-pass bonus

In code, the current composition is:
- `0.50 * momentum_rank`
- `0.20 * low_vol_rank`
- `0.15 * trend_bonus`
- `0.15 * quality_bonus`

This produces a relative ranking within the current static 20-name universe. It is a ranking scaffold, not a profitability claim.

## 1) Momentum 12-1

Definition:
- uses adjusted close history
- measures return from roughly 12 months ago to 1 month ago
- skips the most recent 21 trading days

What it is trying to capture:
- medium-term persistence in winners
- avoid putting too much weight on the noisiest recent month

Why it is used:
- momentum is one of the simplest and most explainable medium-term signals
- it works reasonably well as a first-pass ranking component in a deterministic system

Weaknesses:
- momentum can reverse violently
- can favor crowded names late in a cycle
- static universe construction limits how much trust to place in the ranks

## 2) Realized volatility 30d

Definition:
- annualized realized volatility from trailing adjusted-close returns over 30 trading days
- lower volatility ranks better in the current V1 alpha score

What it is trying to capture:
- smoother names can sometimes provide better tactical entries than highly chaotic ones
- adds a stabilizing counterweight to raw momentum

Why it is used:
- prevents the score from becoming a pure chase-the-hottest-move model
- easy to compute and explain

Weaknesses:
- low volatility can simply mean complacency or crowding
- can unfairly penalize legitimate fast-moving leaders
- recent volatility can change quickly across regimes

## 3) Trend bonus via SMA(200)

Definition:
- binary bonus if the current close is above the 200-day simple moving average

What it is trying to capture:
- whether the name is still in a healthy long-term trend
- a basic filter against buying momentum names already below their major trend line

Why it is used:
- very explainable
- useful sanity check in a simple V1 model

Weaknesses:
- binary cutoff can be blunt
- whipsaws are common near the moving average
- it reacts slowly after sharp drawdowns and recoveries

## 4) Quality bonus

Definition:
- binary bonus if the stock passes the deterministic quality screen used for CORE eligibility

Current V1 quality screen:
- U.S. common equity only
- net margin > 12%
- debt/equity < 40.0 in current source units
- positive free cash flow
- revenue growth > 5%
- average volume >= 1,000,000
- market cap >= $5B

What it is trying to capture:
- basic business quality and investability
- make sure CORE candidates get some ranking support instead of relying on price action alone

Why it is used:
- creates a bridge between the CORE and SWING logic
- helps distinguish tactical setups in higher-quality names from tactical setups in weaker names

Weaknesses:
- currently depends on a convenience fundamental data source with unit-definition caveats
- binary thresholding creates cliff effects
- point-in-time cleanliness is not solved yet

## 5) Final alpha score

What it is trying to do:
- produce a deterministic, explainable ranking rather than a black-box prediction
- reward medium-term strength
- prefer lower recent volatility
- require some trend sanity
- give a modest edge to names passing the quality screen

What it is not:
- not a forecast of intrinsic value
- not a claim of expected outperformance in all regimes
- not yet a mature research-backed composite

## Why alpha and quality can disagree

This is intentional.

- The quality screen is a thresholded business-quality gate.
- The alpha score is a tactical relative ranking.

That means a stock can:
- look attractive tactically but fail CORE quality rules
- look fundamentally strong but not have enough tactical support right now

Example:
- a stock like GOOGL can be a valid SWING candidate if it ranks highly on momentum/volatility/trend
- but it should not be a CORE candidate unless it also passes the current deterministic quality screen

## Current limitations

The main caveats are still real:
- the universe is static and small
- survivorship bias is not solved
- fundamentals are not point-in-time clean
- some field units, especially debt/equity, should be normalized against a better source
- V1 weights are heuristic and only lightly validated so far

## Future upgrades (not current production logic)

These are reasonable next ideas, but they are not implemented yet:
- normalize debt/equity units from a cleaner source
- add point-in-time fundamental snapshots
- validate weights with broader out-of-sample testing
- add pair-level trade analytics and cleaner win-rate accounting
- expand universe construction beyond the current static seed list
