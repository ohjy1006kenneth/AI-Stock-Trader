from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path
from typing import Any

from core.common.env_files import resolve_env_values

SIMFIN_MODAL_ENV_FILE = Path(__file__).resolve().parents[2] / "config" / "simfin.env"
SIMFIN_MODAL_ENV_KEYS = (
    "SIMFIN_API_KEY",
    "SIMFIN_BASE_URL",
    "SEC_USER_AGENT",
)


def build_modal_secrets(
    modal_module: Any,
    *,
    named_secret_names: Sequence[str],
    env_file: Path | None = None,
    env_keys: Sequence[str] = (),
) -> list[object]:
    """Build Modal secret bindings from named secrets plus selected local env values."""
    secrets = [modal_module.Secret.from_name(name) for name in named_secret_names]
    env_values = resolve_env_values(keys=env_keys, env_file=env_file)
    if env_values:
        secrets.append(modal_module.Secret.from_dict(env_values))
    return secrets
