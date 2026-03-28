# Initial Issue Set — First Real Brain Loop

Milestone: **First Real Brain Loop**

## Issue 1 — Predictive dataset builder
- Owner agent: `trading-quant-researcher`
- Type: `implementation`
- Domains: `cloud-training`, `data-pipeline`
- Priority: `p0`
- Objective: build deterministic training dataset from raw OHLCV + Alpaca news aligned to T+1 log-return target
- Affected modules:
  - `cloud_training/data_pipelines/alpaca_news.py`
  - `cloud_training/data_pipelines/build_predictive_dataset.py`
- Dependencies: none
- Done criteria:
  - dataset builder runs
  - outputs deterministic dataset rows
  - includes 21-day history windows and T+1 target

## Issue 2 — First predictive model training scaffold
- Owner agent: `trading-quant-researcher`
- Type: `implementation`
- Domains: `cloud-training`
- Priority: `p0`
- Objective: train first real predictive model that outputs signal + confidence
- Affected modules:
  - `cloud_training/model_architecture/hybrid_model.py`
  - `cloud_training/training/`
- Dependencies: Issue 1
- Done criteria:
  - training script runs on produced dataset
  - emits signal probability and confidence output

## Issue 3 — First artifact bundle definition
- Owner agent: `trading-quant-researcher`
- Type: `implementation`
- Domains: `cloud-training`, `cloud-inference`
- Priority: `p1`
- Objective: define/export first servable predictive artifact
- Affected modules:
  - `cloud_training/training/`
  - `cloud_inference/`
- Dependencies: Issue 2
- Done criteria:
  - artifact format exists
  - artifact can be loaded by inference handler

## Issue 4 — Hugging Face inference contract implementation
- Owner agent: `trading-executor-reporter`
- Type: `integration`
- Domains: `cloud-inference`, `pi-edge`
- Priority: `p0`
- Objective: wire Pi edge HF client and handler against locked request/response contracts
- Affected modules:
  - `pi_edge/network/hf_api_client.py`
  - `cloud_inference/handler.py`
  - `config/cloud_oracle_request.schema.json`
  - `config/cloud_oracle_response.schema.json`
- Dependencies: Issue 3
- Done criteria:
  - edge can call Oracle
  - response validates cleanly

## Issue 5 — Pi Hugging Face connectivity test
- Owner agent: `trading-executor-reporter`
- Type: `integration`
- Domains: `pi-edge`, `cloud-inference`
- Priority: `p1`
- Objective: prove Pi can make a successful authenticated batch call to the Oracle
- Affected modules:
  - `pi_edge/network/hf_api_client.py`
  - `pi_edge/run_daily_cron.sh`
- Dependencies: Issue 4
- Done criteria:
  - successful authenticated request
  - valid response captured on Pi

## Issue 6 — Policy contract scaffold
- Owner agent: `trading-portfolio-strategist`
- Type: `implementation`
- Domains: `cloud-training`, `policy`
- Priority: `p1`
- Objective: define the first concrete RL policy input/output interface without overbuilding full RL yet
- Affected modules:
  - `cloud_training/model_architecture/policy/`
- Dependencies: Issue 2, Issue 4
- Done criteria:
  - policy input contract documented in code
  - policy output target-weight contract aligned with Oracle schema

## Issue 7 — Validation metrics scaffold
- Owner agent: `trading-backtest-validator`
- Type: `validation`
- Domains: `cloud-training`, `validation`
- Priority: `p1`
- Objective: implement the first validation scaffold for walk-forward metrics and promotion checks
- Affected modules:
  - `cloud_training/backtesting/`
- Dependencies: Issue 2
- Done criteria:
  - computes Sharpe, drawdown, turnover, hit rate, OOS comparison vs SPY
  - can express pass/fail against current promotion thresholds

## Issue 8 — Rebalance translation from target weights
- Owner agent: `trading-executor-reporter`
- Type: `implementation`
- Domains: `pi-edge`, `execution`
- Priority: `p0`
- Objective: translate Oracle target weights into paper rebalance orders safely
- Affected modules:
  - `pi_edge/execution/paper_portfolio_executor.py`
  - `pi_edge/reporting/`
- Dependencies: Issue 5
- Done criteria:
  - target weight -> share delta calculation implemented
  - invalid payloads rejected safely
  - omitted tickers treated according to locked semantics
