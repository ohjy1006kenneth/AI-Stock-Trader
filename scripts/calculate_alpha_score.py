from __future__ import annotations

from common import DATA_DIR, OUTPUTS_DIR, RESEARCH_DIR, now_iso, read_json, write_json


def main() -> None:
    prices = read_json(DATA_DIR / "price_data.json", {"rows": []})
    registry = read_json(RESEARCH_DIR / "formula_registry.json", {"factors": []})
    payload = {
        "generated_at": now_iso(),
        "model_version": "v1-draft",
        "approved_factors_used": [f["factor_name"] for f in registry.get("factors", []) if f.get("approved_for_production")],
        "items": [],
        "notes": "Placeholder alpha scoring; deterministic calculations only."
    }
    write_json(OUTPUTS_DIR / "alpha_rankings.json", payload)
    print(f"Alpha score placeholder processed {len(prices.get('rows', []))} rows")


if __name__ == "__main__":
    main()
