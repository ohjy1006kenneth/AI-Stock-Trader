# Persistent OpenClaw Agents

Created for this project:
- `data-guardian` → `~/.openclaw/workspace-data-guardian`
- `scholar-researcher` → `~/.openclaw/workspace-scholar-researcher`
- `backtest-validator` → `~/.openclaw/workspace-backtest-validator`
- `code-maintainer` → `~/.openclaw/workspace-code-maintainer`
- `strategist` → `~/.openclaw/workspace-strategist`
- `mock-portfolio-executor` → `~/.openclaw/workspace-mock-portfolio-executor`
- `daily-reporter` → `~/.openclaw/workspace-daily-reporter`
- `trading-orchestrator` → `~/.openclaw/workspace-trading-orchestrator`

## Notes
- Agent creation used `openclaw agents add`.
- Identity was set with `openclaw agents set-identity`.
- Role boundaries, readable/writable files, wake conditions, and escalation rules were written into each agent workspace via `AGENTS.md` and `ROLE.md`.
- Prompt files were copied into each agent workspace under `prompts/`.
- No inbound routing bindings were added yet.
