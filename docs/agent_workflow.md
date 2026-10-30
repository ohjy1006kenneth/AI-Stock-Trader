# Trading — Hermes Kanban engineering workflow

**Target design as of 2026-10-29.** This describes the new coding control
plane; a Markdown commit does not create profiles, install a scheduler,
authorize trading transactions or prove the Pi is running these services.

## One durable pipeline

```text
You / ChatGPT / GitHub Issues
            |
            v
trading-lead (scheduled planning; actual cadence to verify)
            |-- GitHub: requirements / acceptance / decisions
            +-- Hermes Kanban: priority / dependencies / state
                                      |
                                      v
                           Deterministic dispatcher
                                      |
                                      v
                             trading-dev
                    Single leased implementation task
                      Isolated Git branch/workspace
                                      |
                                      v
                       Hermes openai-codex provider
                           Credential A -> B
                                      |
                                      v
                            GitHub PR + CI
                                      |
                                      v
                    Independent review / merge gates
                                      |
                                      v
                 Authorized application deployment
```

This mirrors the adopted CupMemo pattern, **not** its specific schedule.
Only CupMemo has an explicit 10:00/22:00 planning requirement; the
Trading lead's schedule must be verified from the owner's actual Hermes
configuration or explicitly specified before changes to cron.

The deterministic supervisor, not another AI manager, owns process
liveness, leases, shared Pi resource/capacity constraints, persisted
attempt counts, quota cooldowns, provider selection and crash recovery.
One developer task is active **within this project**. A separate
Pi-wide heavy-worker cap, if installed, takes precedence over local
eligibility; do not infer a new limit from historical HQ issue comments.

## Canonical ownership

- **GitHub Issues:** durable outcome scope, acceptance criteria, technical
  and risk decisions, final completion; no second product backlog.
- **Hermes Kanban:** one durable root issue card, dependencies, priority,
  assignments, attempt history, review handoffs, failure diagnostics.
- **`trading-lead`:** reads authorized requirements, decomposes and admits
  bounded tasks, orders dependencies, reconciles PR/CI/review, handles merge
  within documented authority. No perpetual LLM delegation loop.
- **`trading-dev`:** makes the smallest scoped change on a stable isolated
  branch, runs meaningful tests, publishes or updates one linked PR.
  No self-merge, PR self-approval, or live brokerage operations.
- **Credential pool A -> B:** one ordered `openai-codex` provider pool.
  Switch only for verified ordinary allowance/capacity failure with separately
  authorized credentials and compliant provider terms. A provider safety
  refusal, inaccessible credentials or unrelated tool errors are not a
  reason to cycle accounts.
- **Independent reviews:** exact PR head review and required CI gates.
  No permanently running `trading-code-reviewer` or `code-monkey-trading`
  profile is required by this architecture.

Keep the original domain contract files authoritative: `docs/architecture.md`,
`docs/data_contracts.md`, `docs/runtime_flow.md` and
`core/contracts/schemas.py`. This document changes no stock model,
risk rule, deployment surface or data pipeline behavior.

## Stable issue/card/PR reconciliation

Use the GitHub issue's actual `ohjy1006kenneth/AI-Stock-Trader#N`
identity as the durable key. One root Kanban card per outcome issue;
store its real card ID, branch, PR URL, current head SHA, attempts and
accepted changes. Do not create duplicate cards or PRs after a restart.
The existing repo's task/PR templates and any already-active branch
naming contract take precedence; do not rename an in-progress branch
just to imitate CupMemo's `hermes/GH-N`.

Before dispatch, verify prerequisites, priority, current GitHub outcome,
existing Kanban lease and workspace, and any open or merged PR. On a
crash or worker exit, reconcile this evidence before resuming the next
**missing** step. A timed-out command or silent chat is not proof a
worker/process is gone. Wait for verified cooldown rather than spin.

After implementation the card enters review; all required CI for the
exact head and an independent reviewer verdict must pass before any
authorized merge. Preserve human review for Stock-tab semantic evidence
and all consequential backfills; keep schema/risk approvals mandatory.

## Operational and financial guardrails

The trading product is **paper-trading only** under the existing project
design. Never transform ordinary code-merge permission into a broker
order, credential change, production release, live data backfill or
risk-policy override. These require separate permissions, relevant
tests and any explicit human gate documented in `AGENTS.md`,
`docs/deployment.md` or the GitHub outcome issue.

Implementation failure, provider capacity wait, safety refusal and
human-decision blocker are **different states**. Preserve exact
diagnostics and attempt counts. Do not classify no capacity as feature
failure or bypass refused actions by account switching.

## Commissioning checklist

- Verify actual Hermes lead/developer profiles and the owner-approved
  Trading planning schedule; docs do not create either.
- Verify GitHub/Kanban keys and a single dispatcher lease, with no old
  code-monkey/Steward job claiming the same work.
- Verify the `openai-codex` account pool and quota cooldown behavior
  using simulated failures; do not expend real subscription allowance.
- Test restart reconciliation, duplicate PR prevention, blocked human
  gate, reviewer/CI gating and pause/resume.
- Keep all runtime, deployment, model, trading and brokerage services
  unchanged during the documentation migration.
