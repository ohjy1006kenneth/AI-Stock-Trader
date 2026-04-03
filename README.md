# AI Stock Trader

A cloud-native, paper-trading stock research and execution system.

## What this project is

This repo builds a layered quantitative trading system with a strict deployment split:

- **Cloud Lab** — heavy AI workloads: feature generation, dataset building, training, validation, and model packaging
- **Cloud Oracle** — hosted inference layer
- **Edge Pi** — lightweight runtime that fetches market/account context, calls the oracle, executes paper trades, and reports results

The project is intentionally **paper trading only**.

## Current canonical algorithm direction

The current mainline stack is:

0. **Data & universe selection**
   - survivorship-aware universe handling
   - liquidity filters
   - corporate-action / stale-data / halt checks
1. **Feature generation**
   - FinBERT text features
   - engineered market features
   - engineered macro / factor / fundamental context
2. **Regime detection + predictive model**
   - HMM-first regime detection
   - XGBoost-first predictive model
3. **Portfolio decision**
   - optimizer first (cvxpy-style)
   - contextual bandit later
4. **Risk engine**
   - hard-rule risk controls
5. **Execution engine**
   - non-AI order translation / execution / reconciliation

Deferred but still canonical future work:
- LSTM / GRU sequence models
- Sentence Transformers / topic modeling
- contextual-bandit upgrades
- RL after the simpler stack is validated

## Repo layout

- `cloud_training/` — cloud-side training, feature generation, backtesting, and HF Space packaging
- `cloud_inference/` — inference-side artifact loading, feature adaptation, and request/response contracts
- `pi_edge/` — edge runtime for fetch / call / execute / report
- `runtime/` — shared runtime support modules
- `config/` — non-secret config, schemas, and examples
- `docs/` — architecture, setup, and process docs
- `tests/` — unit and integration tests
- `data/` — local-only data directories (kept mostly empty in git)
- `artifacts/` — artifact schemas / placeholders; generated payloads are not committed
- `ledger/` — local portfolio state placeholder (real runtime state not committed)

## Start here

If you want to understand the architecture first, read:

1. `docs/architecture_design_bible.md`
2. `docs/current_project_state.md`
3. `docs/architecture.md`

## Secrets and local state

Real secrets are **not** committed.

Use:
- `config/alpaca.env.example`

and create your own local:
- `config/alpaca.env`

Generated datasets, reports, runtime state, deployment builds, and local portfolio state are intentionally ignored by git.

## Status

This repo is under active development toward the first promotable predictive model and first clean public-facing architecture baseline.
