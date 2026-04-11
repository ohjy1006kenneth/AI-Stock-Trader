# AGENTS.md — Quant Trading System

## Overview
This is a production quant trading system. It runs daily on a Raspberry Pi 5
(orchestration) backed by cloud compute (Modal), Cloudflare R2 storage,
and Alpaca for brokerage. The system trades US equities once per day at market open
using signals generated after the previous day's close.

Pi runtime assumption:
- The Pi runtime executes inside a Docker container.
- OpenClaw is the runtime engine inside that container.
- Cron on the Pi host triggers the containerized daily run.

Most development is driven by Codex. A human reviewer approves all PRs before merge.
Do not wait for human input mid-task unless you are genuinely blocked. If blocked,
follow the BLOCKED protocol below.

---

## Start of every session — read these first

Before touching any code or issue, read these files in order:

1. `AGENTS.md` (this file) — rules, structure, contracts
2. `TODO.md` — known gaps, pending decisions, technical debt
3. `docs/architecture.md` — system design baseline
4. `docs/data_contracts.md` — canonical inter-layer contracts
5. `core/contracts/schemas.py` — Pydantic schemas (source of truth)

If you are about to create or comment on a GitHub issue, also read:
- `.github/ISSUE_TEMPLATE/codex_task.md`
- `.github/ISSUE_TEMPLATE/schema_migration.md`
- `.github/ISSUE_TEMPLATE/bug_report.md`

---

## Absolute rules — never violate these

### Never modify these files under any circumstances
- 

If a task requires changes to any of these files, stop immediately and follow
the BLOCKED protocol. Do not attempt the change. Do not work around it.

### Never do these things
- Hardcode API keys, credentials, secrets, or file paths
- Change output schemas in `core/contracts/` without a schema migration issue
- Merge your own PRs
- Install packages not already in the appropriate `requirements/*.txt` without noting it in the PR
- Silence exceptions with bare `except:` or `except Exception: pass`
- Use `print()` — use the logger from `services/observability/logging.py`

---

## GitHub project board sync — critical

Project board movement is manual unless explicit automation is set up.
Do not assume labels will move board items automatically.

You must keep both issue labels and project board status accurate at all times.
This is how the human knows what is happening without being pulled into every task.

### Label rules

| Situation | Labels to apply |
|---|---|
| You pick up an issue and start working | Remove `backlog` → add `in-progress` |
| PR is open and ready for human review | Remove `in-progress` → add `review` |
| You cannot proceed without a human decision | Remove `in-progress` → add `blocked` |
| Issue is fully merged and closed | Add `done` (CI handles this automatically) |

### Board rules

| Situation | Board action |
|---|---|
| You pick up an issue and start working | Move item to In Progress |
| PR is open and ready for human review | Move item to Review |
| You cannot proceed without a human decision | Move item to Blocked |
| Issue is merged and closed | Move item to Done |

### How to update labels AND board via GitHub CLI — run BOTH every time

Label and board are independent. Changing a label does NOT move the board.
You must run both commands on every state transition. No exceptions.

```bash
# === When starting a task ===
# 1. Update label
gh issue edit <number> --remove-label "backlog" --add-label "in-progress"
# 2. Move board (requires issue's project item ID and Status field ID — fetch via GraphQL if unknown)
gh api graphql -f query='mutation { updateProjectV2ItemFieldValue(input: {
  projectId: "PVT_kwHOBDy_OM4BTCRF"
  itemId: "<PVTI_...>"
  fieldId: "PVTSSF_lAHOBDy_OM4BTCRFzhAZUEg"
  value: { singleSelectOptionId: "47fc9ee4" }  # In Progress
}) { projectV2Item { id } } }'

# === When opening PR (review) ===
gh issue edit <number> --remove-label "in-progress" --add-label "review"
# Board: singleSelectOptionId "94f39d90" = Review

# === When blocked ===
gh issue edit <number> --remove-label "in-progress" --add-label "blocked"
# Board: singleSelectOptionId "e1e88749" = Blocked
```

Project board option IDs (do not change these):
- Backlog:     f75ad846
- In Progress: 47fc9ee4
- Blocked:     e1e88749
- Review:      94f39d90
- Done:        98236657

To get a project item ID for an issue:
```bash
gh api graphql -f query='{
  repository(owner: "ohjy1006kenneth", name: "AI-Stock-Trader") {
    issue(number: <N>) {
      projectItems(first: 5) { nodes { id project { id } } }
    }
  }
}'
```

Always update label AND board as the very first action when starting a task
and as the very last action before finishing.

---

## BLOCKED protocol

Use this when you need a human decision before proceeding. Do not guess.
Do not make assumptions and continue. Stop and block.

Reasons to block:
- The task requires modifying a forbidden file
- Two valid implementations exist with meaningfully different tradeoffs
- The task description contradicts something in the existing codebase
- A dependency issue exists that requires architectural input
- You are missing credentials or config values that aren't in examples
- There is ambiguity about what you need to do
- Implementation needs a human decision on the architecture level
- A human action is required before proceeding (for example auth, access approval, or external setup)
- Runtime assumptions in code/docs/issues conflict (for example Docker/OpenClaw/cron mismatch)

When blocking:
1. Apply the `blocked` label to the issue
2. Post a comment on the issue using this format:
Blocked — human decision needed
Reason: [explanation of the reason]
Decision: (If there is any decisiosn to make)
A) [option A and its tradeoff]
B) [option B and its tradeoff]
My recommendation: [A or B and why]

Actions: [explanation of what action to take]

Files involved: [list]
Waiting for: human to comment with decision
3. Stop all work on this issue
4. Move to another issue if one is available in the backlog

---

## Task execution workflow

For every issue you work on, follow this sequence exactly:
1. Read AGENTS.md, TODO.md, docs/architecture.md,
2. docs/data_contracts.md, core/contracts/schemas.py
3. Read the issue fully
4. Read every file listed under "Files to read first" in the issue
5. Update label: backlog → in-progress
6. gh issue edit <number> --remove-label "backlog" --add-label "in-progress"
7. Write the code
8. Write or update tests in tests/unit/ or tests/integration/
9. Run: pytest tests/unit/ -v --tb=short
10. Fix all failures — never open a PR with failing tests
11. Open PR using .github/pull_request_template.md
12. Write "Closes #<number>" in the PR body
13. Update label: in-progress → review
14. gh issue edit <number> --remove-label "in-progress" --add-label "review"
15. Update TODO.md if this task resolves or reveals anything worth noting
16. Do not merge — wait for human approval

## Required docs to read first

Before implementation, read these sources when they exist:
- docs/architecture.md
- docs/runtime_flow.md
- docs/data_contracts.md
- docs/deployment.md
- .github/ISSUE_TEMPLATE/ (all templates relevant to the issue type)
- TODO.md (if present)

If multiple docs conflict, block and ask for a human decision.

---

## Project structure
This structure is the current truth, but it can evolve.

If Codex changes the structure, update this section and the root README
in the same task so documentation stays accurate.

ai-stock-trader/
├── app/                          # Deployable runtime surfaces
│   ├── lab/                      # Cloud Lab: training, validation, packaging
│   │   ├── data_pipelines/       # Feature/dataset pipeline area
│   │   ├── model_architecture/   # Model and policy architecture area
│   │   │   └── policy/           # Portfolio policy logic area
│   │   ├── backtesting/          # Evaluation and promotion metrics
│   │   └── training/             # Training orchestration area
│   ├── cloud/                    # Cloud Oracle inference service surface
│   └── pi/                       # Edge Pi runtime surface
│       ├── fetchers/             # Input collection (market/account/news)
│       ├── network/              # Cloud/broker communication
│       ├── execution/            # Order translation and execution
│       └── reporting/            # Daily summaries and alerts
├── core/                         # Shared deployment-agnostic domain logic
│   ├── common/                   # Cross-cutting shared utilities
│   ├── contracts/                # Internal contracts and schemas
│   ├── data/                     # Universe and point-in-time data logic
│   ├── features/                 # Reusable feature interfaces/transforms
│   ├── models/                   # Model abstraction layer
│   ├── portfolio/                # Portfolio construction logic
│   ├── risk/                     # Hard risk-rule logic
│   └── execution/                # Deterministic execution helpers
├── services/                     # External service adapters
│   ├── alpaca/                   # Broker and market data integration
│   ├── r2/                       # Object storage integration
│   ├── modal/                    # Cloud job/deployment integration
│   └── observability/            # Logging/metrics/alerts integration
├── config/                       # Non-secret configuration and policy
│   └── requirements/             # Environment-specific requirement notes
├── docs/                         # Architecture documentation only
│   ├── architecture.md           # System architecture baseline
│   ├── runtime_flow.md           # Operational runtime sequence
│   ├── data_contracts.md         # Canonical data contracts
│   └── deployment.md             # Deployment surfaces and rollout order
├── data/                         # Local data/runtime state placeholders
│   ├── raw/                      # Raw snapshots
│   ├── processed/                # Transformed datasets
│   ├── cache/                    # Local cache space
│   └── runtime/                  # Runtime local state
│       └── ledger/               # Local ledger state placeholder
├── artifacts/                    # Generated outputs and release artifacts
│   ├── bundles/                  # Packaged model/deployment bundles
│   ├── deployments/              # Deployment outputs
│   ├── logs/                     # Deterministic run logs
│   └── reports/                  # Generated reports
│       ├── backtests/
│       ├── daily/
│       ├── diagnostics/
│       ├── integration/
│       ├── pipeline/
│       └── templates/
├── tests/                        # Placeholder for automated test suites
└── .github/                      # Repository automation and templates

---

## Data contracts

All inter-layer data must conform to schemas defined in `core/contracts/schemas.py`.
Do not change a schema unless there is a dedicated schema migration issue
and explicit human approval.

Schemas evolve as the system grows. The rule is not "never change a schema"
— it is **never change a schema silently.**

Every schema change, however small, requires:
1. A dedicated issue using the `schema_migration` template
2. Human review and approval before implementation begins
3. All consumers of the changed schema updated in the same PR or in
   follow-up issues created before the migration PR is merged

Current inter-layer contracts (canonical source: `docs/data_contracts.md`
and `core/contracts/schemas.py`):


Layer 0 output  → UniverseRecord, OHLCVRecord
Layer 0 raw archives → Tiingo news, SimFin fundamentals/earnings, FRED macro/rates
  (R2 artifacts used by Layer 1; not separate Pydantic inter-layer contracts)
Layer 1 output  → FeatureRecord
Layer 2 output  → ScoreRecord  {date, ticker, return_score, pos_prob,
rank_score, regime, confidence}
Layer 3 output  → PortfolioRecord {ticker, weight, target_dollars,
current_dollars, change}
Layer 4 output  → ApprovedOrderRecord {ticker, action, target_dollars,
approved, rules_triggered}

If you discover that `core/contracts/schemas.py` and `docs/data_contracts.md`
disagree, block immediately — do not pick one and proceed.

---

## Architecture changes

Any change to how layers communicate, where data lives, what runs where,
or how the system is structured is an architecture change.

Examples: adding a new layer, changing storage layout, moving logic between
Pi and Modal, introducing a new entrypoint, changing R2 path conventions.

When you make or discover an architecture change, you must update **all**
affected canonical docs in the **same PR or task** — not as a follow-up:

| Doc | Update when |
|---|---|
| `docs/architecture.md` | Layer design, storage layout, data sources, runtime surface changes |
| `docs/runtime_flow.md` | Execution sequence, phase structure, step ordering changes |
| `docs/data_contracts.md` | Inter-layer contract changes (also triggers schema migration protocol) |
| `AGENTS.md` — Project structure section | Directory layout changes |

Rules:
1. Never merge an architecture change without updating the affected docs above
2. If the architecture change also changes a schema, follow the Schema changes
   protocol in parallel
3. If two valid designs exist with meaningfully different tradeoffs, block and
   ask for a human decision — do not pick one and proceed

---

## Schema changes

When you discover a schema needs to change:

1. Do not change it in the current task
2. Create a new issue using `.github/ISSUE_TEMPLATE/schema_migration.md`
3. Add the new issue number to `TODO.md` under `## Schema migrations pending`
4. Block your current issue if the schema change is a prerequisite,
   or continue if it is not

---

## Testing standards

Every PR that adds or modifies logic must include tests. No exceptions.

- Mirror source paths: `core/features/market_features.py`
  → `tests/unit/test_market_features.py`
- Use `data/sample/` fixture files — never fetch live data in unit tests
- Cover: happy path, empty input, missing columns, NaN input
- Minimum: one test per public function
- Must pass before opening PR: `pytest tests/unit/ -v --tb=short`

---

## Style

- Python 3.11
- Type hints on all function signatures
- Docstrings on all public functions (one-line minimum)
- Max line length: 100 characters
- Use `from __future__ import annotations` at top of every file
- Imports: stdlib → third-party → internal (separated by blank lines)
- Never use mutable default arguments
- Prefer explicit over implicit — no magic, no metaclass tricks

---

## What good looks like

A good Codex PR:
- Does exactly what the issue says, nothing more
- Has tests that would catch regressions
- Has no failing tests
- Has no forbidden file touches
- Has type hints and docstrings
- References the issue with "Closes #N"
- Has a clear PR description explaining what was done and why

A bad Codex PR:
- Modifies files not listed in the issue
- Has no tests
- Silences exceptions
- Has print() statements
- Hardcodes any value that belongs in config
- Changes schemas without a migration issue
