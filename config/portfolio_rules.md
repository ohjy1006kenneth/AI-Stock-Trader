# Portfolio Rules

## CORE Sleeve
Entry requirements:
- pass deterministic quality screen
- acceptable or strong approved alpha support
- pass portfolio/risk limits
- no explicit exclusion event

Characteristics:
- months-or-longer default holding period
- lower turnover
- no forced end-of-day liquidation

Exit requirements:
- rule-based deterioration only
- examples:
  - net margin falls below threshold
  - debt profile worsens materially
  - free cash flow quality breaks
  - other predeclared quality rule breaks

## SWING Sleeve
Entry requirements:
- strong approved alpha signal
- liquidity sanity checks pass
- no blocking exclusion rule
- not required to satisfy full CORE quality standard

Characteristics:
- default 3 to 20 trading day horizon
- tactical, higher turnover than CORE
- no forced end-of-day liquidation

Exit requirements:
- default 10% trailing stop-loss
- optional 12% to 15% take-profit
- maximum holding period
- optional momentum fade / signal decay rule
- avoid new entries immediately before earnings where practical

## Deterministic Mutation Boundary
- Strategist writes decisions only.
- Mock Portfolio Executor is the only ledger mutator.
