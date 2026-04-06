# AI Stock Trader

A cloud-native, paper-trading stock research and execution system.

## What this project is

This repo builds a layered quantitative trading system with a strict deployment split:

- **Cloud Lab** — heavy AI workloads: feature generation, dataset building, training, validation, and model packaging
- **Cloud Oracle** — hosted inference layer
- **Edge Pi** — lightweight runtime that fetches market/account context, calls the oracle, executes paper trades, and reports results

The project is intentionally **paper trading only**.

## Canonical algorithm architecture

This is the current baseline architecture for the repo:

0. **Data and universe selection**
   - point-in-time universe handling
   - survivorship bias avoidance
   - liquidity, stale-data, halt, and corporate-action checks
1. **Feature generation**
   - FinBERT-style news sentiment
   - engineered market features
   - macro, factor, sector, and fundamental context
2. **Regime detection and prediction**
   - HMM or GMM regime labels first
   - XGBoost-first predictive model
   - cross-sectional ranking and calibrated confidence
3. **Portfolio construction**
   - optimizer-first target generation
   - turnover-aware rebalance logic
   - optional contextual bandit layer later
4. **Risk engine**
   - hard-rule position, exposure, and loss controls
   - signal staleness checks and proposal rejection
5. **Execution engine**
   - deterministic order translation
   - broker reconciliation
   - fill logging and execution quality tracking

Future work that stays compatible with this baseline:
- LSTM / GRU sequence models
- sentence embedding and topic features
- stronger contextual bandits
- RL only after the simpler stack is proven

## Repo structure

- `app/` — runnable deployment surfaces
   - `app/lab/` — Cloud Lab workloads (feature generation, training, evaluation, packaging)
   - `app/cloud/` — Cloud Oracle inference service and contract handling
   - `app/pi/` — Edge Pi runtime for fetch, execution, reconciliation, and reporting
- `core/` — shared business and domain logic
- `services/` — external service integrations (Alpaca, R2, Modal, Telegram, observability)
- `config/` — non-secret configuration, schemas, examples, and requirement split notes
- `docs/` — architecture notes, setup guides, and process docs
- `tests/` — unit, integration, and pipeline tests
- `data/` — local-only data directories and runtime local state
- `artifacts/` — generated bundles, logs, and report outputs
- `.github/` — CI and repository automation metadata

## Start here

If you want to understand the architecture first, read:

1. `docs/architecture.md`
2. `docs/runtime_flow.md`
3. `docs/data_contracts.md`
4. `docs/deployment.md`

## Secrets and local state

Real secrets are **not** committed.

Use:
- `config/alpaca.env.example`

and create your own local:
- `config/alpaca.env`

Generated datasets, reports, runtime state, deployment builds, and local portfolio state are intentionally ignored by git.

## Status

This repo has been restructured into a documentation-first scaffold before implementation starts.
