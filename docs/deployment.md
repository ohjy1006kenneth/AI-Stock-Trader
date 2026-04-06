# Deployment

This document describes deployment surfaces and responsibilities.

## Surfaces

- app/lab: cloud training and packaging jobs
- app/cloud: hosted inference service
- app/pi: edge runtime and execution process

## Baseline rollout order

1. Deploy cloud oracle with fixed contracts
2. Validate edge-to-cloud handshake
3. Dry-run risk and execution path
4. Enable paper execution

## Operational notes

- Keep secrets outside git
- Log every stage deterministically
- Fail closed on missing risk checks
