from __future__ import annotations

import os
from collections.abc import Sequence
from pathlib import Path

from dotenv import dotenv_values


def resolve_env_values(*, keys: Sequence[str], env_file: Path | None = None) -> dict[str, str]:
    """Resolve non-empty environment values from `os.environ` or an optional env file."""
    file_values = _read_env_file(env_file)
    resolved: dict[str, str] = {}
    for key in keys:
        env_value = _normalize_env_value(os.getenv(key))
        if env_value is not None:
            resolved[key] = env_value
            continue
        file_value = _normalize_env_value(file_values.get(key))
        if file_value is not None:
            resolved[key] = file_value
    return resolved


def populate_env_from_file(
    *,
    keys: Sequence[str],
    env_file: Path,
    override: bool = False,
    override_blank: bool = True,
) -> dict[str, str]:
    """Populate selected env vars from a dotenv file, preserving non-empty existing values."""
    file_values = _read_env_file(env_file)
    applied: dict[str, str] = {}
    for key in keys:
        current = os.getenv(key)
        if not override:
            current_value = _normalize_env_value(current)
            if current_value is not None:
                continue
            if current is not None and not override_blank:
                continue
        file_value = _normalize_env_value(file_values.get(key))
        if file_value is None:
            continue
        os.environ[key] = file_value
        applied[key] = file_value
    return applied


def _read_env_file(env_file: Path | None) -> dict[str, str | None]:
    """Return dotenv values for an existing env file."""
    if env_file is None or not env_file.exists():
        return {}
    return dict(dotenv_values(env_file))


def _normalize_env_value(value: str | None) -> str | None:
    """Normalize one environment value, treating blank strings as missing."""
    if value is None:
        return None
    normalized = value.strip()
    return normalized or None
