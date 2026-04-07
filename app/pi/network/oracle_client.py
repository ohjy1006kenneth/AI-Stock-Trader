from __future__ import annotations


def request_predictions(context: dict[str, object]) -> dict[str, object]:
    """Return deterministic placeholder prediction outputs for dry-run orchestration."""
    _ = context
    return {
        "scores": {
            "AAPL": 0.81,
            "MSFT": 0.74,
            "NVDA": 0.69,
        }
    }
