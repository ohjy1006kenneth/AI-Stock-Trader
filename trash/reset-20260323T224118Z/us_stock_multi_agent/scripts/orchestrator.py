from __future__ import annotations

from common import DATA_DIR, load_json


def main() -> None:
    routing = load_json(DATA_DIR / "model_routing.json", {})
    triggers = load_json(DATA_DIR / "triggers.json", {"items": []}).get("items", [])

    print("OpenClaw Multi-Agent Orchestrator")
    print("Paper trading only")
    print()

    if not triggers:
        print("No triggers fired. Do not wake LLM agents.")
        return

    print("Triggers detected. Wake the 4 specialized agents in this order:")
    print()
    print(f"1. Macro Scout -> model={routing.get('macro_scout', {}).get('model', 'default')}")
    print(f"2. Technical Analyst -> model={routing.get('technical_analyst', {}).get('model', 'default')}")
    print(f"3. Portfolio Risk Manager -> model={routing.get('portfolio_risk_manager', {}).get('model', 'default')}")
    print(f"4. Mock Execution Agent -> model={routing.get('mock_execution_agent', {}).get('model', 'default')}")
    print()
    print("Suggested orchestration pattern inside OpenClaw:")
    print("- Spawn/prompt Macro Scout only for triggered tickers.")
    print("- Pass Markdown context files between agents.")
    print("- Use the stronger model only for final decision/execution analysis.")
    print()
    print("Triggered tickers:")
    for item in triggers:
        print(f"- {item['ticker']}: {', '.join(item.get('event_types', []))}")


if __name__ == "__main__":
    main()
