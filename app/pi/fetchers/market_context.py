from __future__ import annotations


def collect_market_context(as_of_date: str) -> dict[str, object]:
    """Build a deterministic market/account context payload for the run date."""
    return {
        "as_of_date": as_of_date,
        "universe": ["AAPL", "MSFT", "NVDA"],
        "account": {"equity": 100_000.0, "cash": 25_000.0},
    }
