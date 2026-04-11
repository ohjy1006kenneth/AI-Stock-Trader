# Deployment

This document describes deployment surfaces and responsibilities.

## Surfaces

- `app/lab`: cloud training, validation, historical backfill, and packaging jobs
- `app/cloud`: hosted inference service surface
- `app/pi`: edge runtime and execution process, containerized on the Pi

## External dependency roles

- Wikipedia revision history: point-in-time S&P 500 membership source
- Tiingo: canonical historical OHLCV and raw news archives
- SimFin: as-reported fundamentals and earnings dates for the context branch
- FRED: macro and rates context for features and regime detection
- Alpaca Market Data + Trading API: live daily prices, broker reconciliation, and execution
- Cloudflare R2: shared object store for cross-surface data handoff and artifacts

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

1. Build and validate the historical Tiingo/Wikipedia Layer 0 backfill in R2
2. Validate SimFin and FRED context ingestion against point-in-time rules
3. Deploy cloud oracle with fixed contracts
4. Validate edge-to-cloud handshake plus Alpaca live-market-data normalization
5. Dry-run risk and execution path
6. Enable paper execution

## Operational notes

- Keep secrets outside git
- Log every stage deterministically
- Fail closed on missing risk checks
- Keep runtime assumptions synchronized across AGENTS, docs, and issue acceptance criteria
