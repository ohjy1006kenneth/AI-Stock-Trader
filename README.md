# US Stock Mock Research & Portfolio System

A conservative, reproducible OpenClaw workspace for US-equity research, backtesting, and mock portfolio management.

## Scope
- US stocks only
- Mock portfolio only
- No broker connectivity
- No live trading
- Deterministic production path
- LLMs used only for research, review, anomaly investigation, reporting, and controlled maintenance

## Portfolio Model
- CORE sleeve: long-term holdings, held for months or longer
- SWING sleeve: tactical holdings, usually 3 to 20 trading days
- No forced end-of-day liquidation

## Agent Roles
1. Data Guardian
2. Scholar Researcher
3. Backtest Validator
4. Code Maintainer
5. Strategist
6. Mock Portfolio Executor
7. Daily Reporter

## Deterministic Runtime
Python handles:
- ingestion
- factor calculations
- quality filters
- rankings
- backtests
- position sizing
- monitoring
- deterministic decision application
- mark-to-market ledger persistence

LLMs handle:
- research synthesis
- formula extraction
- backtest interpretation
- code review
- anomaly triage
- daily summary writing
- optional executor failure explanation only

## Approval Flow
Research proposal -> backtest validation -> code update -> approval gate -> production promotion

See also:
- `config/architecture.md`
- `config/promotion_policy.md`
- `config/portfolio_rules.md`
