---
name: Schema migration
about: Changing a data contract between layers — requires careful coordination
title: "[Schema] "
labels: backlog, schema-change
assignees: ''
---

## Which schema is changing
<!-- File and class name in core/contracts/. -->

## Layers affected
- [ ] Layer 0 — Data & universe
- [ ] Layer 1 — Features
- [ ] Layer 2 — Model
- [ ] Layer 3 — Portfolio
- [ ] Layer 4 — Risk
- [ ] Layer 5 — Execution
- [ ] Infrastructure / services

## Current schema
```python
# paste current Pydantic model here
```

## Proposed schema
```python
# paste proposed Pydantic model here
```

## Reason for change

## Migration plan
<!-- How will existing Parquet files/manifests/reports in R2 be handled? -->
- [ ] Backward compatible — no migration needed
- [ ] Migration script needed → `scripts/migrate_<schema>_v<N>.py`
- [ ] Breaking change — coordinate with all consumers before merging

## Files to read first
- `AGENTS.md`
- `TODO.md`
- `docs/data_contracts.md`
- `core/contracts/schemas.py`

## Acceptance checklist
- [ ] Human approved the proposed contract change before implementation
- [ ] All consumers updated in same PR or follow-up issues created
- [ ] Migration script written and tested if needed
- [ ] Fixtures updated under `tests/fixtures/` or another documented test fixture location
- [ ] Docs updated: `docs/data_contracts.md` and any affected runtime/deployment docs
- [ ] R2/backfill impact documented

---
<!-- Schema changes must be reviewed by human before Codex implements consumers. -->
<!-- Use blocked label until human approves the proposed schema when needed. -->
