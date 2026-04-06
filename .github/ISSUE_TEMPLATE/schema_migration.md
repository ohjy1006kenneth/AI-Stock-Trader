---
name: Schema migration
about: Changing a data contract between layers — requires careful coordination
title: "[Schema] "
labels: backlog, schema-change
assignees: ''
---

## Which schema is changing
<!-- File and class name in core/contracts/ -->

## Current schema
```python
# paste current Pydantic model here
```

## Proposed schema
```python
# paste proposed Pydantic model here
```

## Reason for change

## Layers affected
<!-- List every layer that reads or writes this schema -->
- 

## Migration plan
<!-- How will existing Parquet files in R2 be handled? -->
- [ ] Backward compatible — no migration needed
- [ ] Migration script needed → `scripts/migrate_<schema>_v<N>.py`
- [ ] Breaking change — coordinate with all consumers before merging

## Checklist
- [ ] All consumers updated in same PR or follow-up issues created
- [ ] Migration script written and tested if needed
- [ ] R2 sample fixtures updated in `data/sample/`
- [ ] `config/schemas/` JSON schemas updated

---
<!-- Schema changes must be reviewed by human before Codex implements consumers -->
<!-- Label: blocked until human approves the proposed schema -->