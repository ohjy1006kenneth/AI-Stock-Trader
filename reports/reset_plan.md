# Trading Project Controlled Reset

## Goal
Simplify the trading project from an over-scaffolded multi-agent layout into a smaller quant workflow:
- `trading` = orchestrator + canonical runtime owner
- Quant Researcher
- Backtest Validator
- Portfolio Strategist
- Executor / Reporter

The reset preserves working infrastructure while removing or archiving redundant scaffolding.

## Historical note
This document describes the reset intent that led to the current simplified architecture. Some references from the original plan have been generalized so this note does not point at dead pre-refactor entrypoints.

## Final simplified agent architecture
- `trading` = orchestrator + canonical runtime owner
- `trading-quant-researcher` = Quant Researcher
- `trading-backtest-validator` = Backtest Validator
- `trading-portfolio-strategist` = Portfolio Strategist
- `trading-executor-reporter` = Executor / Reporter

## Final cleaned workflow
1. The Quant Researcher documents factors, target ideas, and experiment proposals.
2. Deterministic/runtime code calculates current signals and screens.
3. The Backtest Validator decides whether ideas look useful or overfit before promotion.
4. The Portfolio Strategist converts approved outputs into CORE/SWING actions.
5. The Executor / Reporter updates the mock ledger and reports what happened.
6. `trading` supervises everything directly.

## Preserved parts of the old system
- deterministic executor boundary
- mock ledger
- portfolio status tool
- runtime wrappers and preflight flow
- improved quality filter
- improved formula registry
- backtest engine as base infrastructure
- deterministic file flow
- CORE / SWING structure
