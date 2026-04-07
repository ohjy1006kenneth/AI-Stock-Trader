from __future__ import annotations


def execute_orders(orders: list[dict[str, object]]) -> dict[str, object]:
    """Return a deterministic execution summary for dry-run order flow."""
    return {
        "submitted": len(orders),
        "filled": len(orders),
        "orders": orders,
    }
