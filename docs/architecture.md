# Cloud-Native Deep Learning Architecture

## System shape
Cloud Lab -> Cloud API Oracle -> Edge Pi

## System-level rules
- Cloud Lab trains models and policies.
- Cloud Oracle serves inference via API.
- Edge Pi fetches data, calls the API, executes paper trades, and reports.
- The Pi should stay lightweight.
- Training does not happen on the Pi.
- Paper trading only.
- No live trading.
- No agent should assume another agent's model behavior.
- No agent should silently invent schemas, targets, outputs, reward functions, or action semantics.
- When uncertain, agents must ask questions explicitly and aggressively.
- Specialists should write code only inside their own domain.
- `trading` integrates cross-cutting work and owns final coordination.
- Predictive target is next-day log return.
- Signal is a normalized probability in [0,1] of a positive next-day return.
- Confidence is derived from predictive variance.
- Cloud keeps embeddings internal.
- Cloud Oracle operates on one portfolio-level batch request covering the full eligible universe.
- Pi rebalances once per day based on returned target weights.
- Pi applies no shadow confidence filter.
- Long-only, with per-ticker target weights constrained to [0.0, 0.20].
- If total target weights exceed 1.0, the Oracle payload is invalid and edge execution must reject it.

## Agent responsibility matrix

### `trading` — The Orchestrator
Role:
- canonical project owner
- top-level coordinator
- cross-cutting integrator
- keeper of source of truth

Behavior:
- coordinates specialist agents
- integrates cross-domain changes
- resolves ambiguity across research, policy, inference, and edge runtime
- should not unnecessarily do specialist work itself
- should aggressively ask questions when system-wide assumptions are unclear

Domain:
- whole-repo orchestration
- `docs/architecture.md`
- `agents/` behavior docs
- integration across `cloud_training/`, `cloud_inference/`, and `pi_edge/`

### `trading-quant-researcher` — The Signal Architect
Domain:
- `cloud_training/model_architecture/`
- `cloud_training/data_pipelines/`

Strict responsibility:
- owns the Predictive AI only
- writes and maintains code for:
  - FinBERT / NLP arm
  - LSTM / time-series arm
  - XGBoost / probability scorer
  - model-fusion logic needed to produce predictive state
- produces mathematical predictive state only

Examples of output:
- confidence score
- probability distribution
- momentum/state vector
- regime score

Must ask aggressively if unclear on:
- target variable
- prediction horizon
- label definition
- sequence length
- sentiment/news sources
- fusion logic
- feature schema
- training targets
- inference output schema

### `trading-backtest-validator` — The Referee & Simulator
Domain:
- `cloud_training/backtesting/`

Strict responsibility:
- owns simulation and validation
- builds and maintains the historical evaluation environment
- builds OpenAI Gymnasium-style market environment if needed
- runs the RL agent or predictive models against historical out-of-sample data
- calculates validation and risk metrics
- acts as the gatekeeper that promotes or rejects models for deployment to cloud inference

Owns metrics like:
- Sharpe
- drawdown
- turnover
- hit rate
- calibration
- out-of-sample performance

Must ask aggressively if unclear on:
- simulation assumptions
- reward-function evaluation
- slippage model
- cost model
- out-of-sample split
- walk-forward design
- promotion thresholds
- benchmark definitions
- what counts as model success/failure

### `trading-portfolio-strategist` — The Decision Architect
Domain:
- `cloud_training/model_architecture/policy/`

Strict responsibility:
- owns the Decision AI only
- writes and maintains the RL policy code such as PPO / SAC or equivalent
- consumes:
  - predictive state from the quant researcher
  - portfolio/account state
  - constraints/risk state
- outputs:
  - final action
  - final position sizing policy
  - execution intent policy

Must ask aggressively if unclear on:
- action space
- observation/state schema
- reward function
- sizing rules
- cash/position constraints
- whether policy outputs discrete action vs continuous sizing
- whether CORE/SWING still exists as a concept
- how confidence should influence policy

### `trading-executor-reporter` — The Broker
Domain:
- `pi_edge/execution/`
- `pi_edge/reporting/`
- `pi_edge/network/`

Strict responsibility:
- owns edge execution and reporting only
- takes the exact inference/API response from the strategist/policy layer
- submits physical paper orders via Alpaca integration
- syncs and updates paper portfolio state
- generates daily summaries and runtime reports
- owns:
  - API client usage at the edge
  - order submission
  - order/result logging
  - daily summaries
  - pipeline summaries
  - trade alerts

Must ask aggressively if unclear on:
- inference request schema
- inference response schema
- order translation rules
- report fields
- portfolio sync semantics
- alert fields
- edge failure-handling expectations

## Explicit model ownership
- FinBERT -> `trading-quant-researcher`
- LSTM -> `trading-quant-researcher`
- XGBoost probability scorer -> `trading-quant-researcher`
- RL policy -> `trading-portfolio-strategist`
- backtesting / simulation -> `trading-backtest-validator`
- edge execution / reporting -> `trading-executor-reporter`
