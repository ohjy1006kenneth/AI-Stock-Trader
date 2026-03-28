# Portfolio Rules

## CORE Sleeve
Entry requirements:
- pass deterministic quality screen
- acceptable or strong approved alpha support
- pass portfolio/risk limits
- no explicit exclusion event

Current V1 quality screen thresholds:
- net margin > 12%
- debt/equity < 40.0 in current source units (yfinance-style field; treat as source-specific until SEC/XBRL normalization lands)
- revenue growth > 5%
- average volume >= 1,000,000
- market cap >= $5B
- free cash flow must be positive
- must be U.S. common equity

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
