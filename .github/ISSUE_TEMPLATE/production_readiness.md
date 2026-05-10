---
name: Production readiness
about: Validate a layer, pipeline, Modal/R2 run, or data archive for downstream readiness
title: "[Readiness] "
labels: backlog, owner:codex
assignees: ''
---

## Readiness target
- Layer:
- Date/window:
- Candidate run_id:
- Upstream dependency/run_id:
- Downstream consumer/layer:

## Current evidence
<!-- Facts from R2, manifests, local reports, logs, previous issues/PRs. Do not paste secrets. -->
- Existing manifests:
- Existing reports:
- R2 prefixes inspected:
- Known gaps:

## Files to read first
- `AGENTS.md`
- `TODO.md`
- `docs/architecture.md`
- `docs/data_contracts.md`
- `docs/deployment.md`
- `core/contracts/schemas.py`

## Required commands
<!-- Use HOME=/home/juyoungoh for authenticated gh/modal operations where needed. -->
```bash
# readiness command(s)
```

## R2 artifacts that must exist
<!-- List exact manifest/report/data prefixes expected for success. -->
- `artifacts/manifests/...`
- `artifacts/reports/...`
- `features/...`

## Validation criteria
- [ ] Manifest has a terminal successful status, not `running`
- [ ] Validator/readiness report exists in a durable documented location
- [ ] Report includes run_id, date/window, manifest key, output prefixes, counts, validation status, and next-layer readiness boolean
- [ ] Output counts match expected universe/date window or exceptions are documented
- [ ] Stale/interrupted manifests are superseded or documented so operators do not mistake them for active success
- [ ] `ready_for_next_layer` / `ready_for_layer2` is true, or the issue is blocked with a precise human-action reason

## Verification artifacts to include in PR/issue comment
- Final run_id:
- Authoritative manifest key:
- Readiness report path/key:
- R2 output prefix(es):
- Validation summary/counts:
- Commands/tests run:

## Production / external dependency impact
- [ ] Touches R2 data
- [ ] Touches Modal apps/secrets
- [ ] Touches provider APIs
- [ ] Touches Pi runtime / cron / Docker
- [ ] Requires human action or credentials
- [ ] No external side effects

## Idempotency / stale artifact handling
- [ ] Safe to rerun with a new run_id
- [ ] Existing production artifacts must not be deleted unless explicitly justified
- [ ] Stale/interrupted manifests must be superseded or documented
- [ ] Destructive cleanup requires explicit justification in the PR

## Blocked protocol
If blocked by secrets, Modal workspace state, auth, provider access, or a human decision, apply the `blocked` label, post the AGENTS.md BLOCKED comment format, and stop work on this issue.
