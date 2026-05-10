---
name: Bug report
about: Something is broken in production, CI, tests, Modal, R2, or runtime behavior
title: "[Bug] "
labels: backlog, bug
assignees: ''
---

## What is broken
<!-- One sentence description. -->

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

## Where it breaks
<!-- File/function, command, Modal app, R2 prefix, CI job, or runtime surface if known. -->

## How to reproduce
<!-- Minimal steps or command that triggers the bug. Do not paste secrets. -->
```bash
# command(s)
```

## Expected behavior

## Actual behavior

## Error output / evidence
<!-- Paste stack trace/log excerpt, manifest status, report path, or R2 key evidence. Redact secrets. -->

## Environment
- Mode: [ ] paper [ ] live [ ] backtest [ ] local [ ] Modal [ ] Pi/container [ ] CI
- Date/time of failure:
- Relevant logs/reports:

## Files likely involved
-

## Suggested fix
<!-- Optional — if you have a hypothesis. -->

## Acceptance criteria
- [ ] Bug is reproduced or evidence is sufficient to prove it
- [ ] Fix includes regression test or documented reason test is not feasible
- [ ] Relevant unit/integration tests pass
- [ ] If production artifacts were affected, validation/report paths are included in the PR/issue comment

---
<!-- If assigning to Codex: add/change label to owner:codex. -->
<!-- If handling yourself: add/change label to owner:me. -->
