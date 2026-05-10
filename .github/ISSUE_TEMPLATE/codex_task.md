---
name: Codex task
about: Implementation, repair, or validation task delegated to Codex
title: "[Codex] "
labels: backlog, owner:codex
assignees: ''
---

## Task
<!-- One clear sentence: what should exist after this is done that does not exist now? -->

## Layer(s) affected
- [ ] Layer 0 — Data & universe
- [ ] Layer 1 — Features
- [ ] Layer 2 — Model
- [ ] Layer 3 — Portfolio
- [ ] Layer 4 — Risk
- [ ] Layer 5 — Execution
- [ ] Infrastructure / services
- [ ] Tests only
- [ ] Docs only

## Current evidence / context
<!-- Facts, commands, logs, R2 keys, manifests, reports, linked issues/PRs. Do not paste secrets. -->
- Related:
- See: `docs/architecture.md`

## Files to read first
<!-- Codex must read these before writing any code. Be explicit. -->
- `AGENTS.md`
- `TODO.md`
- `docs/architecture.md`
- `docs/data_contracts.md`
- `core/contracts/schemas.py`

## Expected files / allowed scope
<!-- List expected files. If Codex discovers another file must change, explain why in the PR. -->
-

## Never touch for this task
<!-- Reinforce forbidden files or paths specific to this task. Include secret-bearing files if not required. -->
-

## Production / external dependency impact
- [ ] Touches R2 data
- [ ] Touches Modal apps/secrets
- [ ] Touches Alpaca/FRED/SimFin/provider APIs
- [ ] Touches Pi runtime / cron / Docker
- [ ] Requires human action or credentials
- [ ] No external side effects

## Required commands / reproduction
<!-- Exact commands to run, if known. Use HOME=/home/juyoungoh for gh/modal where needed. -->
```bash
# command(s)
```

## Acceptance criteria
<!-- Every checkbox must be satisfied before the PR is opened, or the issue must be blocked with a precise reason. -->
- [ ] Output matches schema in `core/contracts/schemas.py` when schemas are involved
- [ ] `./.venv/bin/pytest tests/unit/ -v --tb=short` passes with no failures
- [ ] Relevant integration/readiness tests pass or are explicitly documented as skipped with reason
- [ ] All new public functions have type hints and docstrings
- [ ] No `print()` statements — use logger from `services/observability/logging.py`
- [ ] No hardcoded secrets, credentials, or local-only paths
- [ ] Docs are updated if behavior, commands, or artifact locations change

## Verification artifacts to include in PR/issue comment
- Commands run:
- Test result summary:
- Manifest key(s):
- Report path(s):
- R2 output prefix(es):
- Known skipped/missing data:

## Test fixtures / sample data
<!-- Point to existing fixtures or say “create minimal fixtures under tests/fixtures/...” -->
-

## Idempotency / cleanup guidance
- [ ] Safe to rerun with a new run_id
- [ ] Existing production artifacts must not be deleted unless explicitly justified
- [ ] Stale/interrupted manifests must be superseded or documented
- [ ] Destructive cleanup requires explicit justification in the PR

---
<!-- Codex: update labels and project board as work progresses (see AGENTS.md). -->
<!-- Codex: backlog → in-progress when starting, in-progress → review when PR opens. -->
<!-- Codex: if you need a human decision, post BLOCKED comment format and stop. -->
