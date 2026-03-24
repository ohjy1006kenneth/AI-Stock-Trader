# Backtest Validation Memo

## Verdict
**Revise, not reject.** The revised CORE filter looks materially more realistic than the prior version, but the current research stack is still too immature to treat any favorable result as trustworthy evidence.

## What changed
The current code revises the CORE quality thresholds to:

- net margin minimum: **12%** (from 15%)
- debt-to-equity maximum: **40.0** (from 0.5)
- revenue growth minimum: **5%** (from 8%)

The most important change is the debt-to-equity threshold. In the current fundamental snapshot, many obviously high-quality large-cap names show `debt_to_equity` values such as:

- AAPL: 102.63
- MSFT: 31.539
- GOOGL: 16.133
- NVDA: 7.255
- ISRG: 0.953

Under the old `0.5` cutoff, nearly everything failed. That made the prior CORE screen unrealistically strict and likely inconsistent with the field scaling coming from the current `yfinance` data source. The old rule was effectively excluding the entire intended investable universe.

## Does the revised CORE filter look more realistic?
**Yes, directionally.** The revised filter is more believable as a practical large-cap quality screen.

Why:

- The old screen produced a degenerate result: **zero qualified names** in the current universe.
- The debt-to-equity field from `yfinance` appears to be delivered on a scale where `0.5` was not a reasonable cutoff for this dataset.
- A higher debt-to-equity cap, plus modestly relaxed margin and growth thresholds, is more consistent with screening actual profitable mega-cap and large-cap businesses rather than demanding an unrealistically pristine balance sheet.

That said, “more realistic” does **not** mean “validated.” It only means the filter now better matches the apparent units and distribution of the current data.

## Is this a sensible correction or likely overfitting?
**It looks more like a sensible correction than classic overfitting.**

Reasons this looks like correction:

1. The old filter was obviously broken in practice because it excluded essentially the whole seed universe.
2. The revised threshold is addressing a likely data-interpretation / scale problem, not adding many bespoke knobs.
3. The changes move the screen from “pathologically restrictive” to “plausibly usable.”

Reasons to still be cautious:

1. The new values are still hand-chosen, not justified by a documented research note or sector-aware calibration.
2. The formula registry is still catching up to the code, so the rationale is documented but not deeply validated.
3. Relaxing thresholds can improve backtest coverage and may mechanically improve results, even if the change is conceptually valid.

So the honest framing is:

- **Probably a necessary realism fix**
- **Not yet proven robust**
- **Not enough evidence yet to claim the change improves strategy quality rather than simply increasing pass rate**

## Current backtest state
The deterministic backtest was rerun after the threshold revision.

Current snapshot:
- CAGR: `-0.11746012483886958`
- Annualized volatility: `0.10340323002174924`
- Sharpe ratio: `-1.1666552599410398`
- Max drawdown: `-0.09249114034427741`
- Number of trades: `5`
- Benchmark-relative comparison: `-0.049177999999999944`

Current verdict from the backtest artifact itself remains:
- **weak_or_inconclusive**

So the revised filter improved realism and practical pass-through, but did **not** suddenly create convincing performance evidence.

## Remaining realism and bias problems
Even if a rerun had looked better, the result would still be provisional because several major limitations remain:

### 1. Survivorship bias is still present
The universe is a fixed static seed list of 20 current large-cap names. That is a classic survivorship-biased setup.

Implications:
- failed delisted names are absent
- historical membership drift is ignored
- today’s winners are overrepresented by construction

### 2. Fundamentals are not point-in-time clean
The backtest uses convenience fundamentals, not historically lagged point-in-time fundamentals.

Implications:
- past dates may implicitly see information that was not available then
- CORE eligibility can leak future accounting knowledge backward into the simulation
- CORE decay logic is especially vulnerable

### 3. Same-day close approximation is optimistic
The backtest applies daily exit logic and executes at the same day’s close for deterministic simplicity.

Implications:
- stop / take-profit / decay events are handled with unrealistically convenient timing
- live implementation would generally require next-bar or next-session execution assumptions
- realized results may look cleaner than achievable

### 4. Research registry maturity is still catching up
The registry now documents the implemented factors, but the research process is still early rather than deeply validated.

Implications:
- factor governance is still maturing
- threshold changes are not yet backed by robust sector-aware calibration
- documented logic is now clearer, but still provisional

## What remains provisional
The following claims are still provisional and should not be promoted as validated:

- that the revised CORE sleeve produces a durable edge
- that quality + alpha interaction is robust out of sample
- that current turnover / cost assumptions are realistic enough for decision use
- that the screen is portable beyond this 20-name seed universe
- that the relaxed thresholds are sector-neutral or economically justified

## What should be improved next
Priority order:

1. **Fix point-in-time fundamentals**
   - this is the biggest realism gap for the CORE sleeve
   - until this is solved, treat all CORE backtest results as research-only

2. **Document and normalize debt-to-equity units**
   - confirm source interpretation formally
   - avoid future silent scale mismatches

3. **Replace the static survivor universe**
   - expand to a broader historical universe with membership control

4. **Improve execution realism**
   - test next-day open or next-bar approximations
   - stress transaction cost and slippage assumptions

5. **Deepen validation of the alpha weights and thresholds**
   - especially across sectors and out-of-sample windows

## Bottom line
The revised CORE threshold direction looks **more realistic and more defensible** than the prior version. In particular, changing debt-to-equity from `0.5` to `40.0` looks like a practical correction to a likely scaling / interpretation problem, not an obviously overfit trick.

But this is still only a **plausibility upgrade**, not validation.

Because survivorship bias, non-point-in-time fundamentals, same-day close execution, and an early-stage research layer all remain unresolved, the correct stance is:

**Encouraging correction, still provisional, not yet trustworthy evidence.**
