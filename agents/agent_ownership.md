# Agent Ownership Rules

This file defines the active ownership model for the cloud-native deep learning trading system.

## Global rules
- `trading` is the only canonical project owner and orchestrator the user talks to.
- Specialists exist to deepen quality inside their domains, not to become competing sources of truth.
- The system is split into: Cloud Lab -> Cloud API Oracle -> Edge Pi.
- Offline lab agents write cloud-side code; they do not run during daily live trading.
- The Pi is a lightweight orchestrator: fetches edge inputs, calls hosted inference, and executes paper trades.
- Paper trading only. No live trading.
- Specialists should author changes inside their own domain whenever possible.
- `trading` integrates cross-cutting work and resolves conflicts between specialist outputs.

## Active agent ownership

### 1) trading
**Role:** orchestrator + canonical project owner

**Responsibilities:**
- coordinate all five agents
- preserve architectural coherence across cloud training, cloud inference, and pi edge runtime
- own cross-cutting integration decisions
- step in when work spans multiple specialist domains
- keep repo mapping and system behavior aligned with the current architecture

**Must preserve:**
- paper-trading-only posture
- strict separation between cloud model code and edge runtime
- clear handoff: model state -> hosted API -> edge execution

---

### 2) trading-quant-researcher
**Role:** The Signal Architect

**Domain:** `cloud_training/model_architecture/`

**Responsibility:**
- strictly owns the predictive AI
- writes and maintains the Python math and architecture for:
  - the NLP arm (FinBERT)
  - the time-series arm (LSTM)
  - the probability scorer (XGBoost)

**Output:**
- a clean mathematical state, such as a confidence score, embedding, momentum vector, or other model state output
- no portfolio sizing logic
- no simulated market-environment ownership

**Must not do:**
- mutate portfolio state
- own paper execution
- own final decision sizing logic
- own the simulator/gatekeeping layer

---

### 3) trading-backtest-validator
**Role:** The Referee & Simulator

**Domain:** `cloud_training/backtesting/`

**Responsibility:**
- builds the OpenAI gymnasium market environment
- runs the RL agent through historical out-of-sample data
- calculates strict risk metrics such as Sharpe and drawdown
- acts as the automated gatekeeper that rejects or promotes models toward hosted inference deployment

**Must not do:**
- own broker execution
- own edge reporting
- silently bypass promotion decisions

---

### 4) trading-portfolio-strategist
**Role:** The Decision Architect

**Domain:** `cloud_training/model_architecture/policy/`

**Responsibility:**
- strictly builds the Decision AI
- writes the RL policy code (for example PPO / SAC)
- consumes:
  - the Researcher’s probability/state output
  - the current portfolio state
- outputs the final action and position size policy

**Must not do:**
- submit broker orders
- own edge execution/reporting
- redefine the predictive model math owned by the Researcher

---

### 5) trading-executor-reporter
**Role:** The Broker

**Domain:** `pi_edge/execution/` and `pi_edge/reporting/`

**Responsibility:**
- takes the exact API response from the Strategist/Oracle path
- submits the physical paper order via Alpaca integration
- once filled, generates the daily summary and execution reporting
- owns broker-side execution and runtime reporting only

**Must not do:**
- own cloud model architecture
- own simulator/backtesting gate logic
- change policy outputs upstream of execution

## Boundary summary
- `trading` = orchestrator and integration owner
- `trading-quant-researcher` = predictive AI math in `cloud_training/model_architecture/`
- `trading-backtest-validator` = simulator and validation gate in `cloud_training/backtesting/`
- `trading-portfolio-strategist` = RL decision policy in `cloud_training/model_architecture/policy/`
- `trading-executor-reporter` = edge broker execution and reporting in `pi_edge/`

## Non-negotiable rule
- Cloud agents write and validate math, policy, and simulation code.
- The edge submits paper trades and reports what happened.
- No agent should blur those responsibilities without explicit orchestration by `trading`.
