# GitHub Issues Workflow

GitHub Issues are the primary task-management system for this project.

## Core operating model
- `trading` is the orchestrator and issue manager.
- Specialist agents work from bounded GitHub Issues.
- Active work tracking should live in GitHub Issues, not only in chat.
- While a specialist is actively working an issue, `trading` should stay quiet in chat unless blocked or a human decision is required.
- When an issue is completed, `trading` should send a concise issue-completion summary.
- When a milestone is completed, `trading` should send a milestone summary.
- If a blocker or human decision is required, `trading` should ask immediately and clearly.

## Issue lifecycle
Use labels to represent lifecycle state:
- `status:backlog` -> not yet selected
- `status:ready` -> clarified and ready to start
- `status:in-progress` -> actively being worked by the specialist owner
- `status:blocked` -> cannot proceed without dependency or decision
- `status:review` -> specialist implementation is done and the issue is awaiting orchestrator review by `trading`
- `status:done` -> orchestrator review passed; completed and closed

## How lifecycle is represented
### Labels
State labels:
- `status:backlog`
- `status:ready`
- `status:in-progress`
- `status:blocked`
- `status:review`
- `status:done`

Agent labels:
- `agent:trading`
- `agent:trading-quant-researcher`
- `agent:trading-backtest-validator`
- `agent:trading-portfolio-strategist`
- `agent:trading-executor-reporter`

Workstream labels:
- `type:implementation`
- `type:blocker`
- `type:validation`
- `type:integration`
- `type:milestone`

Domain labels:
- `domain:cloud-training`
- `domain:cloud-inference`
- `domain:pi-edge`
- `domain:data-pipeline`
- `domain:policy`
- `domain:execution`
- `domain:reporting`
- `domain:validation`

Decision labels:
- `needs-decision`
- `question`

Priority labels:
- `priority:p0`
- `priority:p1`
- `priority:p2`

### Milestones
Milestones represent project phases, not individual tasks.

### Assignees
- `trading` decides issue ownership.
- Each issue should have one primary specialist owner.
- Cross-domain issues stay owned by `trading` unless intentionally handed off.

### Comments
Use comments for:
- progress updates
- dependency notes
- blocker explanation
- handoff to next specialist
- review outcome

## Review rule
The expected lifecycle is:
1. specialist completes bounded implementation work
2. issue moves to `status:review`
3. `trading` performs orchestrator review against done criteria and dependency readiness
4. `trading` then moves the issue to:
   - `status:done` if review passes
   - `status:in-progress` if fixes are required
   - `status:blocked` if a true blocker or decision gap is discovered
5. if non-blocking extra work remains, create a follow-up issue instead of stretching the original issue indefinitely

## How trading should orchestrate
`trading` should:
1. create or refine bounded issues
2. assign the correct specialist label and owner
3. mark dependencies explicitly in the issue body
4. move status labels as work progresses
5. open blocker/decision issues only when a real human decision is required
6. close issues only when done criteria are met

## Blocker workflow
When a specialist is blocked by a real decision or ambiguity:
1. `trading` opens or updates a blocker issue
2. apply labels:
   - `type:blocker`
   - `status:blocked`
   - `needs-decision`
   - `question`
3. include clearly:
   - what is unclear
   - why it matters
   - which issue is blocked
   - what exact answer is needed

## Chat discipline
Use chat mainly for:
- answering blocker issues
- confirming milestone-level direction
- issue-completion summaries
- milestone summaries
- handling integration decisions that cannot be resolved from issues alone

Do not send constant progress chatter while work is actively underway.

## Required issue-completion summary format
For each completed issue, use exactly this structure:
- Issue: [number and title]
- Owner specialist: [agent name]
- Status: completed / moved to review / blocked
- What changed: short but useful summary
- Files changed: list
- Checks/tests run: list
- Any follow-up issues created: list
- Next issue selected: [number and title]

Issue summaries must make specialist work visible through concrete file changes, implementation details, and checks. Do not hide work behind vague phrasing like "the agent worked on it".

## Parallelization policy
- Maintain one primary critical-path issue at all times.
- Allow up to 2 additional parallel issues only when they are dependency-safe.
- Never assign more than one active issue to the same specialist at once.
- Do not start an issue in parallel if it depends on outputs or contracts that are not ready yet.
- If a parallel issue is only safe as scaffolding, keep it explicitly limited to scaffolding and contract preparation.
- Do not activate the whole issue set at once; keep concurrency disciplined and dependency-aware.

## Active issue selection rule
When selecting active work, `trading` should explicitly classify the issue set into:
1. one primary critical-path issue
2. up to two additional parallel-safe issues
3. blocked issues waiting on dependencies or human decisions

For each active or blocked issue, record:
- owning specialist
- dependency reason
- why it is active now or why it must wait
