# AI Stock Trader

A cloud-native, paper-trading stock research and execution system.

## Autonomous engineering workflow

The **coding** workflow is separate from the market-data/trading runtime:
GitHub Issues -> scheduled `trading-lead` planning -> Hermes Kanban ->
deterministic dispatcher -> one `trading-dev` task ->
Hermes `openai-codex` credential pool A then B (capacity only) ->
GitHub PR/CI -> independent review and merge gates -> separately authorized
deployment. A deterministic supervisor manages worker liveness, cooldowns
and restart reconciliation without another permanent LLM agent.

Trading's project-specific planning cadence is **not assumed** to be the
CupMemo 10 a.m./10 p.m. schedule. Verify the installed profile/cron
configuration. See [development-agent workflow](docs/agent_workflow.md)
and [agent rules](AGENTS.md). The former HQ code-monkey/reviewer profile
names are obsolete operating instructions, not proof of installed state.

## What this project is

This repo builds a layered quantitative trading system with a strict deployment split:

- **Cloud Lab** — heavy AI workloads: feature generation, dataset building, training, validation, and model packaging
- **Cloud Oracle** — hosted inference layer
- **Edge Pi** — Docker-containerized OpenClaw runtime triggered by cron for fetch, execution, and reporting

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
- article embedding and topic features
- stronger contextual bandits
- RL only after the simpler stack is proven

## Repo structure

- `app/` — runnable deployment surfaces
   - `app/lab/` — Cloud Lab workloads (feature generation, training, evaluation, packaging)
     and the separate guarded read-only `app/lab/stock_evidence_site/` consumer
   - `app/cloud/` — Cloud Oracle inference service and contract handling
   - `app/pi/` — Edge Pi runtime for fetch, execution, reconciliation, and reporting
- `core/` — shared business and domain logic
- `services/` — external service integrations (Alpaca, optional order-book gating, R2, Modal, observability)
- `config/` — non-secret configuration, schemas, examples, and requirement split notes
- `docs/` — architecture notes, setup guides, and process docs
- `tests/` — unit, integration, and pipeline tests
- `data/` — local-only data directories and runtime local state
- `artifacts/` — generated bundles, logs, and report outputs
- `.github/` — CI and repository automation metadata

## Ownership boundaries

- `app/` owns deployment entrypoints and stage orchestration by runtime surface.
- `core/` owns domain logic, contracts, and deterministic transformations.
- `services/` owns external API and infrastructure adapters only.
- `docs/` owns architecture intent and inter-layer contract references.

Boundary rule:
- Keep `core/` independent from environment-specific SDK details.
- Keep `services/` free of domain/business trading rules.
- Keep `app/` thin; call into `core/` and `services/` rather than duplicating logic.

## Start here

If you want to understand the architecture first, read:

1. `docs/architecture.md`
2. `docs/runtime_flow.md`
3. `docs/data_contracts.md`
4. `docs/deployment.md`

The [standalone stock evidence reviewer](docs/stock_evidence_site.md) is outside
daily orchestration and is not deployed or semantically accepted. It uses an
explicit local historical preview or configured private backend without fallback.
The default preview fixture is untracked/excluded and absent from a clean checkout;
fixture provisioning, runtime dependency declarations and separate rollout/security/
browser/human gates remain prerequisites, not claims of current readiness.

## CI and local tests

- CI runs on every push and pull request.
- CI command: `pytest tests/unit/ -v --tb=short`
- Local command: `pytest tests/unit/ -v --tb=short`

## Secrets and local state

Real secrets are **not** committed.

Store environment values in local-only `.env` files or your secret manager.
Do not commit secret values to this repository.

Generated datasets, reports, runtime state, deployment builds, and local portfolio state are intentionally ignored by git.

## Status

Milestone 0 foundation work is in progress.
