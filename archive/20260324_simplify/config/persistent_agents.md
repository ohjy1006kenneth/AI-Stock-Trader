# Persistent OpenClaw Agents

Created for this project:
- `trading-data-guardian` → `~/.openclaw/workspace-trading-data-guardian`
- `trading-scholar` → `~/.openclaw/workspace-trading-scholar`
- `trading-backtest-validator` → `~/.openclaw/workspace-trading-backtest-validator`
- `trading-code-maintainer` → `~/.openclaw/workspace-trading-code-maintainer`
- `trading-strategist` → `~/.openclaw/workspace-trading-strategist`
- `trading-executor` → `~/.openclaw/workspace-trading-executor`
- `trading-daily-reporter` → `~/.openclaw/workspace-trading-daily-reporter`
- `trading-orchestrator` → `~/.openclaw/workspace-trading-orchestrator`

## Notes
- Agent creation used `openclaw agents add`.
- Identity was set with `openclaw agents set-identity`.
- Role boundaries, readable/writable files, wake conditions, and escalation rules were written into each agent workspace via `AGENTS.md` and `ROLE.md`.
- Prompt files were copied into each agent workspace under `prompts/`.
- No inbound routing bindings were added yet.
