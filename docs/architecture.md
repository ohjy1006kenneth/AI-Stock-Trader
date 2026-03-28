# Simplified Trading Architecture

## Canonical owner
- `trading`
- This is the only orchestrator the user talks to.
- This is the canonical project and runtime owner.
- It supervises the specialist roles directly.

## Hard constraints
- Keep the system paper-trading only.
- Keep repeated calculations in deterministic Python.
- Do not put LLM reasoning into the numeric hot path.
- Preserve the deterministic ledger boundary.
- The executor remains the only ledger mutator.
- Specialist roles must not become competing sources of truth.

## Active specialist model
1. **Quant Researcher**
2. **Backtest Validator**
3. **Portfolio Strategist**
4. **Executor / Reporter**

## Workflow
1. **Quant Researcher** defines formulas, candidate features, target ideas, and experiment proposals.
2. **Backtest Validator** checks realism, bias control, leakage risk, and promotion readiness.
3. **Portfolio Strategist** converts approved deterministic or approved inference outputs into BUY / SELL / HOLD / REVIEW decisions.
4. **Executor / Reporter** applies paper decisions to the ledger and produces reporting.
5. `trading` integrates and supervises everything directly.

## Operating split

### Cloud side
Purpose:
- historical data prep
- feature engineering
- model training
- heavy backtesting
- validation dataset prep
- artifact export

### Raspberry Pi side
Purpose:
- fetch latest data
- compute runtime features
- load approved artifacts
- run inference
- convert outputs into decisions
- paper execution
- reporting

## Active project layers

### 1) Research
Primary examples:
- formula registry
- factor notes
- trusted-source notes
- experiment proposals

### 2) Validation
Primary examples:
- backtest engine
- validation outputs
- promotion verdicts

### 3) Runtime strategy
Primary examples:
- runtime data collection
- quality screen
- alpha ranking
- sentry logic
- decision-policy conversion

### 4) Execution / reporting
Primary examples:
- executor
- portfolio status
- alerts
- daily summary
- pipeline summary
- runtime wrappers

## Deterministic file flow contract
1. Runtime data and strategy steps write canonical artifacts.
2. The strategist writes decisions only.
3. The executor reads decisions and the current ledger.
4. The executor is the only component allowed to mutate the portfolio state.
5. Reporting reads final deterministic artifacts and produces summaries.

## Promotion workflow
1. Research proposes or revises a feature, formula, or model idea.
2. The idea is documented with assumptions and caveats.
3. Training / backtest work is run outside the runtime node.
4. Validation decides whether the result is weak, provisional, or promotion-worthy.
5. Only approved logic influences runtime decision behavior.
