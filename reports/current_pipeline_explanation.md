# Current Pipeline Explanation

## 1) Universe stage

The universe stage builds the list of names the system is allowed to evaluate.

Current behavior:
- `scripts/build_universe.py` writes `outputs/universe.json`
- the current universe is a static 20-name U.S.-stock research seed list

Purpose:
- keep the first version bounded and deterministic
- make every downstream stage operate on the same explicit ticker set

Limitation:
- because the universe is static and small, rankings are relative only within that set
- survivorship bias is still unresolved in this V1 setup

## 2) Data collection stage

The data collection stage fetches market and fundamental fields for every universe name.

Current behavior:
- `scripts/fetch_price_data.py` writes `outputs/price_snapshot.json`
- `scripts/fetch_fundamental_data.py` writes `outputs/fundamental_snapshot.json`

Price data is used for things like:
- adjusted-close history
- close history
- momentum calculation
- realized volatility calculation
- trend filter calculation

Fundamental data is used for things like:
- net margin
- debt/equity
- free cash flow
- revenue growth
- average volume
- market cap

## 3) Quality filter

The quality filter is a hard eligibility screen.

Current behavior:
- `scripts/quality_filter.py` reads `outputs/fundamental_snapshot.json`
- it writes `outputs/qualified_universe.json`

What it does:
- keeps only names that pass the deterministic CORE-quality rules
- rejects names that fail one or more rules

Current V1 rules after revision:
- must be U.S. common equity
- net margin > 12%
- debt/equity < 40.0 in current source units
- positive free cash flow
- revenue growth > 5%
- average volume >= 1,000,000
- market cap >= $5B

Meaning:
- this stage is about business quality and investability
- it is not trying to identify the best tactical setup

## 4) Alpha ranking

The alpha ranking is a tactical scoring layer.

Current behavior:
- `scripts/calculate_alpha_score.py` reads price data and `qualified_universe.json`
- it writes `outputs/alpha_rankings.json`

Current alpha components in code:
- 12-1 momentum rank
- lower 30-day realized volatility rank
- trend bonus if close > SMA(200)
- quality bonus if the stock passed the quality filter

Current score composition:
- `0.50 * momentum_rank`
- `0.20 * low_vol_rank`
- `0.15 * trend_bonus`
- `0.15 * quality_bonus`

Meaning:
- alpha is a relative ranking over the current universe
- it is tactical, not a direct statement of intrinsic quality

## 5) Why alpha and quality can disagree

Because they answer different questions.

- The quality filter asks: "Is this a fundamentally acceptable CORE name under our deterministic rules?"
- The alpha rank asks: "Does this currently look attractive as a tactical setup relative to the other names?"

So a stock can:
- pass quality but rank poorly on alpha right now
- rank highly on alpha but fail the quality screen

That disagreement is normal and expected.

## 6) Why GOOGL can be a valid SWING buy but not a CORE buy

A SWING and a CORE position have different entry logic.

A stock like GOOGL can be a valid SWING candidate when:
- its alpha score is high enough
- the trend filter passes
- it is not already blocked by portfolio constraints

But it should only be a CORE candidate when it also passes the deterministic quality screen.

So the system can legitimately say:
- "GOOGL looks strong tactically right now"
- while also saying
- "GOOGL is not CORE-eligible unless it clears the quality gate"

That is not a contradiction. It is the intended separation between sleeves.

## 7) Exact conditions for each outcome

### CORE buy candidate
A stock becomes a CORE buy candidate when all of the following are true:
- it passes the deterministic quality screen
- its alpha score is at least `0.65`
- its trend filter passes
- it is not already held
- it passes portfolio and allocation checks in execution

In strategist code, this maps to:
- `quality pass + alpha >= 0.65 + trend_filter_pass`

### SWING buy candidate
A stock becomes a SWING buy candidate when all of the following are true:
- its alpha score is at least `0.80`
- its trend filter passes
- it is not already held
- it passes portfolio and allocation checks in execution

Importantly:
- SWING does not require a full CORE quality pass
- quality still helps indirectly through the quality bonus in alpha scoring

### REVIEW outcome
A stock becomes `REVIEW` when:
- it is not an exit candidate
- it is not already held in a way that changes action
- it does not meet the CORE-buy rules
- and it does not meet the SWING-buy rules

In plain English:
- the system is saying "keep watching this, but do not buy it now"

### HOLD outcome
The executor supports `HOLD`, but the current strategist mostly emits `REVIEW`, `BUY`, or `SELL`.

Practical meaning of HOLD if used:
- existing position remains valid
- no deterministic sell event has fired
- no new buy or sell action is required

## 8) Sentry and exits

`script/sentry_monitor.py` produces deterministic exit events such as:
- trailing stop hit
- take-profit hit
- signal decay
- scheduled review due

The strategist can convert those into SELL decisions for held positions.

## 9) Execution boundary

The strategist never mutates the ledger.

Only `scripts/mock_portfolio_executor.py` may:
- update `ledger/mock_portfolio.json`
- write `outputs/execution_log.json`

This boundary is intentional and should not be broken.

## 10) Plain-English summary

The pipeline works like this:
1. choose the names to evaluate
2. fetch deterministic price and fundamental data
3. run a hard quality screen for CORE eligibility
4. rank names tactically with the alpha model
5. let the strategist convert those results into BUY/SELL/REVIEW decisions
6. let the executor apply only valid decisions to the mock ledger
7. report the final state

The whole point of the current design is:
- CORE = quality plus alpha support
- SWING = tactical alpha-based trades
- execution = deterministic and auditable
- ledger mutation = strictly isolated to the executor
