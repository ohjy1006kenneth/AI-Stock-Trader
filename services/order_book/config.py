from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path

DEFAULT_ORDER_BOOK_FEATURE_CONFIG_PATH = (
    Path(__file__).resolve().parents[2] / "config" / "order_book_features.json"
)
_PROVIDER_RE = re.compile(r"^[A-Za-z0-9_-]+$")


@dataclass(frozen=True)
class OrderBookFeatureConfig:
    """Repository-owned runtime gate for optional Layer 1 order-book features."""

    enabled: bool = False
    provider: str | None = None

    @property
    def is_active(self) -> bool:
        """Return True only when the branch is explicitly enabled and a provider exists."""
        return self.enabled and self.provider is not None


def load_order_book_feature_config(
    path: Path = DEFAULT_ORDER_BOOK_FEATURE_CONFIG_PATH,
) -> OrderBookFeatureConfig:
    """Load the optional Layer 1 order-book feature config from repository state."""
    payload = json.loads(path.read_text(encoding="utf-8"))

    enabled = bool(payload.get("enabled", False))
    raw_provider = payload.get("provider")
    if raw_provider is None:
        provider = None
    elif not isinstance(raw_provider, str):
        raise ValueError("order_book_features.provider must be a string or null")
    else:
        provider = raw_provider.strip().lower() or None
        if provider is not None and _PROVIDER_RE.fullmatch(provider) is None:
            raise ValueError(
                "order_book_features.provider must contain only letters, digits, '-' or '_'"
            )

    return OrderBookFeatureConfig(enabled=enabled, provider=provider)
