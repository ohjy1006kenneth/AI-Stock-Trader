# Architecture

## Overview

This repository implements a production-oriented quantitative trading system for U.S. equities.

The system is designed around four deployment surfaces:

1. **Laptop (development)**  
   Used for research, coding, local tests, and pull request preparation.

2. **Cloud (heavy compute)**  
   Used for:
   - FinBERT inference on news text
   - XGBoost inference and retraining
   - larger offline backtests and packaging jobs

3. **Raspberry Pi 5 (edge runtime / orchestration)**  
   Used for:
   - scheduling and orchestration
   - market/account synchronization
   - lightweight portfolio construction
   - hard risk checks
   - order execution through Alpaca
   - monitoring and reporting

4. **Object storage (R2/S3-compatible)**  
   Used as the persistent source of truth for:
   - raw market/news snapshots
   - feature tables
   - model scores
   - portfolio proposals
   - execution reports
   - manifests
   - model bundles and diagnostics

The design principle is:

- heavy compute in the cloud
- lightweight orchestration on the Pi
- deterministic execution
- state stored in object storage
- explicit contracts between every layer

---

## Layered system design

The system follows a strict layered architecture.

### Layer 0 — Data & Universe Selection

Layer 0 guarantees that all downstream layers operate on clean, honest, point-in-time data.

Responsibilities:
- construct a point-in-time eligible universe
- avoid survivorship bias
- apply liquidity and tradeability filters
- use adjusted OHLCV correctly
- detect stale, missing, or corrupted market data
- generate daily eligibility masks and quality flags

Examples of outputs:
- point-in-time universe membership
- adjusted OHLCV history
- liquidity flags
- halted / tradeable / illiquid masks
- daily eligibility mask

This layer must be correct before any model or backtest can be trusted.

### Layer 1 — Feature Generation

Layer 1 converts raw data into aligned numerical features indexed by `(date, ticker)`.

Feature branches include:

#### Text / NLP branch
- raw news text
- FinBERT sentiment probabilities
- article counts
- sentiment aggregation windows
- recency-weighted sentiment
- coverage/source richness features

#### Market branch
- returns and momentum
- volatility and ATR
- trend indicators
- volume and liquidity features
- cross-asset context
- gap features

#### Context branch
- fundamentals
- macro context
- rates and spreads
- sector/factor context
- earnings proximity features

Output:
- an aligned feature table with one row per `(date, ticker)`

### Layer 1.5 — Regime Detection

This optional layer classifies the current market environment.

Examples:
- bull
- bear
- sideways

Typical methods:
- HMM
- GMM
- other regime classifiers

Output:
- regime label
- regime probability/confidence

This regime context can be appended to Layer 1 outputs before prediction.

### Layer 2 — Prediction

Layer 2 produces predictive scores from aligned features.

The first serious predictive model in this repository is **XGBoost**.

Why:
- strong baseline for structured financial data
- handles nonlinear feature interactions well
- typically more robust than overly complex deep models on small/noisy datasets

Inputs:
- Layer 1 feature table
- optional Layer 1.5 regime features

Outputs:
- expected return score
- calibrated probability of outperformance
- rank score
- model confidence
- model version metadata

This layer predicts signal quality. It does **not** decide portfolio weights or broker orders.

### Layer 3 — Portfolio Decision

Layer 3 translates predictive signals into target portfolio intent.

Responsibilities:
- score filtering / candidate selection
- portfolio construction
- weight generation
- turnover-aware rebalancing
- optional optimizer or policy logic

Possible implementations:
- simple ranked weighting
- constrained optimizer
- contextual bandit selection
- more advanced policy logic later

Output:
- target weights / target dollars per ticker

### Layer 4 — Risk Engine

Layer 4 is a hard-rule override layer.

It must remain model-free and deterministic.

Responsibilities:
- enforce maximum position sizes
- enforce sector and beta limits
- enforce liquidity / ADV caps
- suppress stale or bad-data signals
- apply daily loss / drawdown-based scaling
- reject impossible or dangerous orders

This layer can:
- modify orders
- suppress tickers
- reject trades entirely

Output:
- final approved order set

### Layer 5 — Execution Engine

Layer 5 executes approved trades and records reality.

Responsibilities:
- reconcile broker state first
- convert target dollars/weights to orders
- submit and monitor orders
- cancel/retry stale orders
- record fills and slippage
- update execution reports and runtime summaries

This layer should remain deterministic, auditable, and simple.

---

## Runtime deployment model

## Laptop
The laptop is used for:
- development
- issue implementation
- local unit/integration tests
- notebooks and experiments
- pull requests

## Cloud
The cloud is used for:
- FinBERT inference
- XGBoost inference
- retraining jobs
- artifact packaging
- larger validation runs

## Raspberry Pi
The Pi is used for:
- cron scheduling
- step orchestration
- local state checks
- lightweight portfolio logic
- hard risk rule enforcement
- Alpaca order execution
- Telegram summaries and anomaly alerts

## Object storage
Object storage is the source of truth for:
- raw data
- processed datasets
- manifests
- scores
- bundles
- reports
- diagnostics

The Pi should not be the canonical holder of historical runtime data.

---

## Canonical runtime flow

### After market close
1. pull EOD bars and news
2. persist raw snapshots
3. build or refresh the aligned feature table
4. run predictive inference
5. construct target portfolio
6. apply hard risk rules
7. store approved order proposal

### Before / at next market open
1. reconcile actual broker state
2. translate approved target to executable orders
3. place orders
4. monitor fills and retries
5. log execution quality

### Throughout the day
1. monitor runtime health
2. detect mismatches or stale stages
3. alert via Telegram on anomalies

---

## Validation philosophy

No candidate is promotable merely because:
- the code runs
- the artifact exports
- the bundle exists

A candidate becomes promotable only if it survives:
- real-data training
- honest out-of-sample evaluation
- walk-forward validation
- cost-aware review
- risk-aware review

Walk-forward validation must preserve time order.
A true holdout period should remain untouched until the design stabilizes.

---

## Architectural rules

1. Prediction is separate from portfolio construction.
2. Portfolio construction is separate from risk enforcement.
3. Risk enforcement is separate from execution.
4. State belongs in object storage and canonical manifests.
5. Schemas are explicit and versioned.
6. No layer may silently change another layer's contract.
7. The Pi orchestrates; it does not perform heavy compute.
8. Cloud compute does not own persistent state.

---

## Repository mapping

- `app/lab/` — cloud training, validation, packaging
- `app/cloud/` — cloud inference surface
- `app/pi/` — edge runtime surface
- `core/contracts/` — shared inter-layer schemas
- `core/data/` — point-in-time universe/data logic
- `core/features/` — reusable feature logic
- `core/models/` — predictive model abstractions
- `core/portfolio/` — portfolio construction logic
- `core/risk/` — hard risk rules
- `core/execution/` — deterministic execution helpers
- `services/` — external system adapters

Ownership boundary summary:
- `app/` coordinates runtime surfaces.
- `core/` owns business logic and contracts.
- `services/` owns third-party integration adapters.
- `docs/` owns architecture and contract intent.