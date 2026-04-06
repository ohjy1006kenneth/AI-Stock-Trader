# Deployment

This document describes deployment surfaces and responsibilities.

## Surfaces

- app/lab: cloud training and packaging jobs
- app/cloud: hosted inference service
- app/pi: edge runtime and execution process (containerized on Pi)

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

1. Deploy cloud oracle with fixed contracts
2. Validate edge-to-cloud handshake
3. Dry-run risk and execution path
4. Enable paper execution

## Operational notes

- Keep secrets outside git
- Log every stage deterministically
- Fail closed on missing risk checks
- Keep runtime assumptions synchronized across AGENTS, docs, and issue acceptance criteria
