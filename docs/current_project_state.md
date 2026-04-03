# Current Project State

This file is the canonical working summary of the trading project's current architecture, repo shape, agent ownership, runtime flow, and milestone direction.

## Canonical architecture references

- `docs/current_project_state.md` = current repo/project status snapshot
- `docs/architecture_design_bible.md` = canonical design-spec / architecture choice reference; read this first when reconstructing project direction next session
- `docs/architecture.md` = high-level system/deployment split overview

## 0. Fast handoff summary

If resuming this project with minimal tokens, the current working truth is:
- deployment architecture stays **Cloud Lab -> Cloud Oracle -> Edge Pi**
- canonical **mainline now** = Layer 0 integrity + FinBERT + engineered market/context features + HMM-first regime detection + XGBoost-first predictive model + cvxpy-style optimizer + hard-rule risk engine + non-AI execution
- canonical **later, explicitly deferred** = LSTM/GRU, Sentence Transformers, BERTopic/LDA, contextual-bandit upgrades, RL
- Milestone 3 now explicitly prioritizes Layer 0/1 integrity, HMM-first regime detection, XGBoost-first predictive modeling, optimizer-first decisions, and hard risk controls
- the repo has been cleaned for public visibility: `agents/`, `memory/`, legacy `archive/`, legacy `trash/`, and tracked `config/sp500_constituents.json` were removed from GitHub; secrets remain local in ignored `config/alpaca.env`
- current backlog shape to remember: #21 active data/model issue, with #22 (Layer 0 hardening) and #26 (HMM-first regime detection) as the most aligned next architectural follow-ons

## 1. System shape

The system is a cloud-native AI trading stack with three main deployment layers:

- **Cloud Lab** — training, model building, backtesting, validation; now canonically packaged for a Hugging Face Space runtime for heavy AI workloads
- **Cloud Oracle** — hosted inference API
- **Edge Pi** — data fetch, API call, paper execution, reporting

Within that deployment split, the updated algorithmic stack is:
- Layer 0: data and universe selection
- Layer 1: feature generation
- Layer 2: predictive model (XGBoost-first, regime-aware later)
- Layer 3: portfolio decision (optimizer first, contextual bandit before RL)
- Layer 4: risk engine
- Layer 5: execution engine

Deferred-but-canonical future additions remain in scope:
- LSTM / GRU market-sequence models
- Sentence Transformers and topic modeling in the text branch
- contextual-bandit upgrades after optimizer baseline
- RL only after the simpler stack is validated

Intended flow:

**Cloud Training -> Cloud Inference API -> Raspberry Pi Execution**

## 2. Main directories

### `cloud_training/`
This is the offline lab.

It is meant to hold:
- data pipelines
- model architecture
- training entrypoints
- backtesting / validation
- Hugging Face Space packaging for cloud-side training/runtime

Current subareas include:
- `data_pipelines/`
- `model_architecture/`
- `training/`
- `build_hf_space.py`
- `sync_hf_space.py`
- `publish_hf_bundle.py`
- `backtesting/`

### `cloud_inference/`
This is the Cloud Oracle.

It is meant to:
- host the trained model behind a Hugging Face endpoint
- return inference outputs to the Pi

Main files:
- `cloud_inference/handler.py`
- `cloud_inference/requirements.txt`

### `pi_edge/`
This is the Raspberry Pi runtime.

It is meant to:
- refresh the S&P 500 snapshot
- fetch market/fundamental data
- call the Hugging Face API
- execute paper orders through Alpaca
- sync paper portfolio state
- generate reports

Key subareas/files:
- `pi_edge/fetchers/`
- `pi_edge/network/`
- `pi_edge/execution/`
- `pi_edge/reporting/`
- `pi_edge/preflight_check.py`
- `pi_edge/run_daily_cron.sh`

## 3. Universe and data

### Universe design
The active universe is now S&P 500-based, not the old 20-name seed list.

Logic:
- Wikipedia = membership refresh source
- Alpaca = tradability filter
- `config/sp500_constituents.json` = runtime source of truth

So runtime does not scrape Wikipedia every day; it uses the local snapshot.

Updated canonical direction for Layer 0:
- universe quality is now treated as a first-class algorithm layer, not just plumbing
- add survivorship-bias-aware handling where feasible
- add explicit liquidity filters (minimum ADV / minimum price)
- add corporate-action-aware checks
- add halt / bad-data detection
- treat Layer 0 silent data corruption as a high-severity failure class

Current size:
- old universe: 20
- new universe: 503 tickers
- usable price data: 503
- usable fundamentals: 503

### Current fetchers
On the Pi side, the main fetchers are:
- `build_universe.py`
- `refresh_sp500_constituents.py`
- `fetch_price_data.py`
- `fetch_fundamental_data.py`

## 4. Execution model

The system is now paper trading only through Alpaca.

Key execution files:
- `pi_edge/execution/alpaca_paper.py`
- `pi_edge/execution/paper_portfolio_executor.py`
- `ledger/paper_portfolio.json`

Current status:
- Alpaca paper integration works
- paper portfolio sync works
- portfolio status works
- reporting works
- final Oracle response -> rebalance translation is not fully complete yet

## 5. Agent architecture

There are currently 5 active agents.

### `trading`
The orchestrator.

Owns:
- coordination
- cross-cutting integration
- canonical project state
- issue management
- milestone sequencing

It is the PM / integrator, not the specialist math engine.

### `trading-quant-researcher`
The Signal Architect.

Owns:
- predictive model math
- feature/model research
- code under `cloud_training/model_architecture/`
- code under `cloud_training/data_pipelines/`

Intended AI ownership:
- FinBERT
- LSTM
- XGBoost
- hybrid predictive-state generation

Produces predictive state such as:
- score
- confidence
- vector/state representation

Does not do:
- portfolio sizing
- RL policy
- broker execution
- portfolio mutation

### `trading-backtest-validator`
The Referee & Simulator.

Owns:
- `cloud_training/backtesting/`
- simulation environments
- historical evaluation
- promotion/rejection gates
- risk metrics such as:
  - Sharpe
  - drawdown
  - turnover
  - OOS performance

Its job is to decide whether a model/policy is good enough to promote.

It does not own:
- predictive model math
- RL policy code itself
- edge execution

### `trading-portfolio-strategist`
The Decision Architect.

Owns:
- portfolio decision semantics
- constrained optimizer logic
- contextual-bandit-style selection logic
- later policy / RL experimentation only after the simpler decision layer is stable
- code under `cloud_training/model_architecture/policy/`

Current preferred ownership order:
- cvxpy-style constrained optimizer first
- contextual bandit next
- PPO / SAC / RL later, not first

Inputs:
- predictive state from the researcher
- current portfolio/account state
- constraints/risk state

Outputs:
- action
- sizing
- decision policy

Does not:
- own FinBERT/LSTM/XGBoost
- submit broker orders
- mutate portfolio state

### `trading-executor-reporter`
The Broker.

Owns:
- `pi_edge/execution/`
- `pi_edge/reporting/`
- `pi_edge/network/`

Its job is to:
- call the cloud inference API
- translate response into paper orders
- submit via Alpaca
- sync paper portfolio state
- generate reports, summaries, alerts

It is the only execution-side role.

## 6. Daily operational flow

Intended daily runtime flow on the Pi:
1. refresh S&P 500 snapshot
2. build active runtime universe
3. fetch latest market/fundamental data
4. call Hugging Face inference endpoint
5. receive model/policy response
6. submit paper order through Alpaca
7. sync paper portfolio state
8. generate reports

Current reality:
- steps 1–3 work
- Alpaca paper execution works
- full HF inference integration is not finished yet
- edge currently blocks if `HF_INFERENCE_URL` is missing

## 7. Current repo structure

Important parts:
- `agents/` = OpenClaw behavior docs and prompts
- `config/` = runtime config, contracts, S&P 500 snapshot, execution settings
- `cloud_training/` = model/data/backtest code
- `cloud_inference/` = oracle API handler
- `pi_edge/` = Pi runtime
- `data/` = runtime data artifacts
- `ledger/` = paper portfolio state
- `reports/` = daily/pipeline/backtest/diagnostic reports
- `artifacts/` = future model bundles and schemas
- `docs/` = architecture/process docs
- `archive/` = legacy local-quant layout and old scaffolding

## 8. Current project-management model

Project management has moved to GitHub Issues.

That means:
- `trading` acts like the PM/orchestrator
- Issues are the real task queue
- specialist agents work from bounded issues
- blockers should only go to the human when a real decision is needed

Current working style:
- low chat noise
- one issue at a time on the critical path
- optional parallel issues when dependency-safe
- issue-completion summaries instead of constant narration
- optimize for maximum disciplined throughput, meaning maximum parallel work whenever dependency-safe
- `ready` means dependency-clear and available to start
- the PM/orchestrator may freely add, split, or remove issues when it improves flow
- send the user an update every time an issue status changes
- Project board status sync is now maintained manually by the PM/orchestrator instead of GitHub Actions automation
- the PM/orchestrator should operate autonomously after direction is given, including proactively starting review as soon as specialist coding appears complete
- specialists should ask aggressive clarifying questions whenever architecture, contracts, semantics, or design choices are ambiguous instead of silently assuming
- prefer continuous autonomous orchestration over passive availability: launch specialist work intentionally, react immediately to completion, perform the next PM step, launch the next dependency-safe work, and repeat until blocked or milestone-complete

## 9. Current milestone direction

Current main milestone:

**Milestone 3 — First promotable predictive model**

Meaning:
- harden Layer 0 data / universe quality
- build the first real aligned feature stack
- train the first serious XGBoost-first predictive model
- validate it under proper promotion gates
- pair it with a simpler portfolio-decision layer before any serious RL push

Current algorithmic direction after the user's updated spec:
- XGBoost / LightGBM-style predictive modeling first
- HMM-first regime detection is part of the canonical target design
- optimizer-first decision layer
- contextual bandit before RL
- LSTM / topic modeling / RL remain part of the long-term design, but are deferred until Layers 0–4 are stable

Current active issue:
- #21 Strengthen real aligned dataset for first promotable candidate
- owner: `trading-quant-researcher`

## 10. What is working vs not working

### Working now
- GitHub-backed repo structure
- S&P 500 snapshot refresh workflow
- Alpaca paper integration
- paper portfolio tracking
- cron wiring for the new edge script
- agent responsibility matrix
- GitHub Issues workflow

### Not finished yet
- actual HF inference endpoint integration
- strict request/response contract fully finalized
- real predictive model code completed enough to deploy
- real RL policy code completed enough to deploy
- full cloud-native end-to-end daily pipeline past the inference boundary

## 11. In one sentence

The system is now a cloud-native AI paper-trading architecture where:
- the cloud is supposed to train and serve the intelligence,
- the Pi is supposed to fetch data, call the oracle, and execute paper trades,
- and OpenClaw is supposed to orchestrate development through specialist agents and GitHub Issues.
