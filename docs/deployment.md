# Deployment

This document describes deployment surfaces and responsibilities.

## Surfaces

- app/lab: cloud training and packaging jobs
- app/cloud: hosted inference service
- app/pi: edge runtime and execution process (containerized on Pi)

## External dependency roles

- Wikipedia revision history: point-in-time S&P 500 membership source
- Tiingo: canonical historical OHLCV and raw news archives
- SimFin: Layer 0 as-reported fundamentals and earnings-date archive used by Layer 1 context features
- FRED: Layer 0 macro and rates archive used by Layer 1 context and regime features
- Alpaca Market Data + Trading API: live daily prices, broker reconciliation, and execution

Layer 0 owns every external data pull. Layer 1 and later milestones read existing R2
archives only; they do not call Wikipedia, Tiingo, SimFin, FRED, or Alpaca for feature
inputs.

## Pi runtime container model

- Runtime host: Raspberry Pi 5
- Scheduler: host cron
- Runtime process: Docker container
- Runtime engine in container: OpenClaw

Expected execution chain:
1. Cron triggers scheduled command on Pi host
2. Host starts or invokes the edge runtime container
3. OpenClaw executes the daily runtime entrypoint inside container
4. Runtime emits deterministic manifests and reports

## Baseline rollout order

1. Build and validate the complete Layer 0 historical backfill in R2:
   Wikipedia universe, Tiingo OHLCV/news, SimFin fundamentals/earnings, and FRED macro/rates
2. Validate the Layer 0 daily incremental path:
   Alpaca live bars, Tiingo news, SimFin refreshes, FRED refreshes, universe masks, and manifests
3. Build Layer 1 features strictly from existing R2 Layer 0 archives
4. Deploy cloud oracle with fixed contracts
5. Validate edge-to-cloud handshake plus Alpaca live-market-data normalization
6. Dry-run risk and execution path
7. Enable paper execution

Baseline paper execution is long-only equities. Hedge and long-short capabilities must stay
disabled by policy until the relevant risk and execution gates are implemented:
- defensive index hedges: explicit approved instrument list, hedge notional caps, and
  broker/account permission checks
- sector hedges: margin or inverse-instrument approval, sector ETF mapping, and net/gross
  exposure controls
- true long-short: borrow/locate checks, margin checks, short-specific order semantics, and
  updated execution contracts

## Operational notes

- Keep secrets outside git
- Log every stage deterministically
- Fail closed on missing risk checks
- Keep risk thresholds in policy/config so long-only, hedged, and long-short modes can use
  the same Layer 4 rule framework
- Keep runtime assumptions synchronized across AGENTS, docs, and issue acceptance criteria
