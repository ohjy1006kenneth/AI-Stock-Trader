# Deployment

This document describes deployment surfaces and responsibilities.

## Surfaces

- app/lab: cloud training and packaging jobs
- app/cloud: hosted inference service
- app/pi: edge runtime and execution process

## Pi runtime topology

- Host: Raspberry Pi 5
- Container runtime: Docker
- In-container process: OpenClaw
- Scheduler trigger: host cron invoking container entrypoint

Recommended shape:
- one container for edge orchestration runtime
- explicit env var injection for non-secret config and mounted secret file paths
- mounted persistent path for runtime manifests/log artifacts

Example host trigger concept:
- cron -> `docker run`/`docker exec` -> OpenClaw runtime entrypoint

## Baseline rollout order

1. Deploy cloud oracle with fixed contracts
2. Validate edge-to-cloud handshake
3. Dry-run risk and execution path
4. Enable paper execution

## Operational notes

- Keep secrets outside git
- Log every stage deterministically
- Fail closed on missing risk checks
- Keep container image/runtime version pinned for reproducibility
