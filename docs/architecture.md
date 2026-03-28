# Cloud-Native Deep Learning Architecture

## System shape
Cloud Lab -> Cloud API Oracle -> Edge Pi

## Agent responsibility matrix

### `trading`
- orchestrator and canonical project owner
- coordinates all specialist agents
- owns cross-cutting integration and final architectural coherence

### `trading-quant-researcher` — The Signal Architect
**Domain:** `cloud_training/model_architecture/`

Owns the predictive AI:
- FinBERT NLP arm
- LSTM time-series arm
- probability scorer (XGBoost)

Final deliverable:
- a clean mathematical state such as confidence scores, vectors, embeddings, or similar signal-state outputs

Does not own:
- portfolio sizing
- simulator environments
- broker execution

### `trading-backtest-validator` — The Referee & Simulator
**Domain:** `cloud_training/backtesting/`

Owns:
- OpenAI gymnasium market environment
- historical out-of-sample simulator runs
- strict risk metrics such as Sharpe and drawdown
- automated reject/promote gatekeeping toward hosted inference deployment

Does not own:
- edge execution
- cloud predictive architecture

### `trading-portfolio-strategist` — The Decision Architect
**Domain:** `cloud_training/model_architecture/policy/`

Owns the Decision AI:
- RL policy code such as PPO / SAC
- consumes predictive state + current portfolio state
- outputs final action and position size policy

Does not own:
- broker execution
- edge reporting

### `trading-executor-reporter` — The Broker
**Domain:** `pi_edge/execution/` and `pi_edge/reporting/`

Owns:
- taking the exact API response from the Strategist/Oracle path
- submitting physical paper orders via Alpaca integration
- generating daily execution summaries once fills occur

Does not own:
- cloud model architecture
- simulator/backtesting gate logic

## Separation of concerns
- `cloud_training/` = offline research, predictive model code, RL policy code, simulator/backtesting
- `cloud_inference/` = hosted inference API / endpoint logic
- `pi_edge/` = edge fetching, hosted inference calls, broker execution, reporting

## Boundary
- Predictive math lives in the cloud lab.
- Decision policy lives in cloud policy code.
- Hosted inference returns the model/policy response.
- The edge executes and reports.
- `trading` preserves coherence across the whole chain.
