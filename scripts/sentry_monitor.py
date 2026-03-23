from __future__ import annotations

from common import OUTPUTS_DIR, now_iso, write_json


def main() -> None:
    payload = {
        "generated_at": now_iso(),
        "events": [],
        "rules": {
            "escalate_on": [
                "trailing_stop_hit",
                "take_profit_hit",
                "meaningful_price_move",
                "scheduled_review_due",
                "data_integrity_issue",
                "new_factor_model_version_available"
            ]
        },
        "notes": "Monitoring loop must remain deterministic and non-LLM."
    }
    write_json(OUTPUTS_DIR / "sentry_events.json", payload)
    print("Sentry placeholder written")


if __name__ == "__main__":
    main()
