---
name: Codex task
about: Task delegated to Codex — will be worked autonomously
title: "[Codex] "
labels: backlog, owner:codex
assignees: ''
---

## Task
<!-- One clear sentence. What should exist after this is done that doesn't exist now. -->

## Files to read first
<!-- Codex must read these before writing any code. Be explicit. -->
- `core/contracts/schemas.py`
- 

## Files to create or modify
<!-- Explicit list. Codex must not touch anything outside this list. -->
- 

## Never touch for this task
<!-- Reinforce forbidden files specific to this task -->
- 

## Acceptance criteria
<!-- Every checkbox must be satisfied before the PR is opened -->
- [ ] Output matches schema in `core/contracts/schemas.py`
- [ ] `pytest tests/unit/ -v` passes with no failures
- [ ] All new public functions have type hints and docstrings
- [ ] No `print()` statements — use logger from `services/observability/logging.py`
- [ ] No hardcoded values — config goes in `config/`
- [ ] 
- [ ] 

## Context and references
<!-- Links to related issues, design decisions, architecture docs -->
- Related: #
- See: `docs/architecture.md`

## Sample data
<!-- Point to fixture files to use in tests -->
- `data/sample/`

---
<!-- Codex: update labels and project board as work progresses (see AGENTS.md → GitHub issue/board ownership) -->
<!-- Codex: backlog → in-progress when starting, in-progress → review when PR opens -->
<!-- Codex: if you need a human decision, post BLOCKED comment format and stop -->