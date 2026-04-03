# Architecture Design Bible

This is the canonical design-spec for the trading system.
When implementation choices are ambiguous, this document wins unless you explicitly override it later.

## 1. System split never changes

Deployment architecture stays:

**Cloud Lab -> Cloud Oracle -> Edge Pi**

Rules:
- AI-heavy training / feature generation stays cloud-side
- Pi stays lightweight
- Pi fetches data, calls the oracle, executes paper trades, and reports
- paper trading only
- no live trading

## 2. Canonical algorithm stack

The algorithm stack is:

0. data and universe selection
1. feature generation
2. predictive model
3. portfolio decision
4. risk engine
5. execution engine

This layered design is preferred over a monolithic end-to-end model.

---

## 3. Canonical now (mainline implementation path)

This is the stack to prioritize on the critical path now.

### Layer 0 — Data and universe selection
No AI here.

Requirements:
- survivorship-bias-aware / survivorship-bias-minimized universe handling
- liquidity filters: minimum ADV, minimum price
- corporate-action-aware handling: splits, dividends, adjusted data sanity
- stale-price / missing-bar detection
- halt / bad-data detection

Rule:
- silent Layer 0 bugs are high-severity because they poison all downstream layers

### Layer 1 — Feature generation

#### Text branch
Inputs:
- Alpaca / Benzinga news
- earnings transcripts later if practical

Canonical now:
- **FinBERT**
- recency-weighted aggregation
- rolling sentiment / coverage features
- article relevance features when feasible
- source credibility weighting when feasible

Outputs:
- sentiment probabilities
- recency-weighted ticker sentiment
- daily news volume / coverage features
- relevance / quality features where available

#### Market branch
Inputs:
- price
- volume
- returns
- volatility
- gaps
- daily / intraday bars where available
- SPY / VIX / sector ETF context where available

Canonical now:
- engineered market features first
- momentum / volatility / trend / relative-strength style features
- no LSTM required on the mainline yet

#### Context branch
Inputs:
- fundamentals
- rates
- macro
- sector / factor features
- earnings proximity where available
- implied-vol / short-interest style context later where available

Canonical now:
- engineered context features only

### Regime detection
Canonical now:
- **HMM first**
- GMM is an acceptable alternative / comparison, not the preferred default

Expected output:
- regime state such as bull / bear / sideways (or equivalent taxonomy)

Usage:
- feed the predictive layer
- later support separate predictive models per regime

### Layer 2 — Predictive model
Canonical now:
- **XGBoost primary**
- **LightGBM** as direct alternative / comparison
- Random Forest only as sanity-check baseline

Target direction:
- **sector-neutralized forward return** is the canonical target design

Implementation baseline today:
- some repo code still uses next-day log return / positive-return labels
- treat that as current implementation baseline, not final design truth

Preferred outputs:
- expected return score
- calibrated probability
- cross-sectional rank score

Calibration:
- Platt scaling first
- isotonic regression as optional comparison

Monitoring:
- **SHAP is canonical** for debugging, monitoring, and drift inspection

### Layer 3 — Portfolio decision
Canonical now:
1. **cvxpy-style constrained mean-variance optimizer**
2. contextual bandit later
3. RL later still

Requirements:
- optimizer consumes predictive outputs plus covariance / risk inputs
- turnover penalty should be included early
- output should be target weights / target exposure / rebalance decisions

### Layer 4 — Risk engine
No AI here.

Hard rules include:
- max position size
- sector cap
- beta cap
- daily loss limit
- max turnover
- max leverage
- correlation cap / anti-clustering rule
- drawdown-based exposure scaling
- order size vs ADV sanity check
- signal staleness checks
- bad / stale data stop-trading rules
- fat-finger checks

Optional support tools:
- GARCH-style volatility forecasting may support the risk layer
- but the risk engine itself remains rule-based

### Layer 5 — Execution engine
No AI here.

Responsibilities:
- convert weights to orders
- choose order types
- place / monitor / cancel / retry
- reconcile fills with Alpaca account state
- estimate slippage before placement where possible
- track fill quality
- feed fill-quality observations back into upstream monitoring / diagnostics

Execution engine must not contain model logic.

---

## 4. Canonical later (deferred, but still part of the end-state)

These are still part of the long-term intended architecture, but they are **not** on the mainline critical path until the simpler stack is validated.

### Deferred text upgrades
- Sentence Transformers
- BERTopic
- LDA
- richer transcript/document understanding

### Deferred market-model upgrades
- LSTM
- GRU
- learned sequence embeddings
- heavier forecasters like TFT / N-BEATS / N-HiTS, but only if they beat the simpler tabular stack in walk-forward validation

### Deferred decision upgrades
- contextual bandit after optimizer baseline is proven
- RL policy layers after Layers 0–4 are stable
- PPO / SAC only after simpler approaches are validated

Rule:
- do not let deferred models destabilize the current milestone sequence

---

## 5. Validation is canonical

Validation protocol:
- **walk-forward only** as the canonical validation method
- train on 2 years
- test on the next 6 months
- then walk forward

Do not use ordinary k-fold as the primary financial-model validation method.

Promotion thresholds:
- Sharpe > 1.2
- Max Drawdown < 12%
- at least 1 year of meaningful backtest coverage for promotion judgment
- must beat SPY

Costs / realism:
- include realistic slippage assumptions
- include SEC / FINRA fees where relevant
- if the system cannot beat SPY after costs and constraints, it should not be promoted

---

## 6. Contract / output philosophy

The predictive layer should expose outputs usable by downstream decision and risk layers, such as:

```json
{
  "signal": 0.82,
  "confidence": 0.74,
  "embeddings": [0.12, -0.45]
}
```

Notes:
- `signal` = expected-return score and/or calibrated probability
- `confidence` = uncertainty-aware compression for downstream use
- `embeddings` = optional latent representation for future downstream use

---

## 7. Single-sentence default

If forced to summarize the canonical current direction in one line:

**Start with FinBERT + engineered market/context features + HMM regime detection + XGBoost predictive model + cvxpy optimizer + hard risk engine, and defer LSTM/topic-modeling/RL until that stack is validated.**
