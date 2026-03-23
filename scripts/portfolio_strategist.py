from __future__ import annotations

from common import LEDGER_DIR, OUTPUTS_DIR, now_iso, read_json, write_json


def main() -> None:
    alpha = read_json(OUTPUTS_DIR / "alpha_rankings.json", {"items": []})
    qualified = read_json(OUTPUTS_DIR / "qualified_universe.json", {"items": []})
    portfolio = read_json(LEDGER_DIR / "mock_portfolio.json", {})
    payload = {
        "generated_at": now_iso(),
        "portfolio_id": portfolio.get("portfolio_id"),
        "decisions": [],
        "notes": {
            "core_logic": "Only use approved quality + alpha outputs",
            "swing_logic": "Only use approved alpha + risk rules",
            "status": "placeholder"
        },
        "inputs_summary": {
            "alpha_items": len(alpha.get("items", [])),
            "qualified_items": len(qualified.get("items", []))
        }
    }
    write_json(OUTPUTS_DIR / "strategist_decisions.json", payload)
    print("Strategist decision placeholder written")


if __name__ == "__main__":
    main()
