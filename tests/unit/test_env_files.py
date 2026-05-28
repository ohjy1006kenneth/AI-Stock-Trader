from __future__ import annotations

import os
from pathlib import Path

import pytest

from core.common.env_files import populate_env_from_file, resolve_env_values


def test_resolve_env_values_prefers_non_blank_environment_values(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Process env should win over the file when a non-blank value already exists."""
    env_file = tmp_path / "test.env"
    env_file.write_text("SEC_USER_AGENT=file-agent\n", encoding="utf-8")
    monkeypatch.setenv("SEC_USER_AGENT", "env-agent")

    resolved = resolve_env_values(keys=("SEC_USER_AGENT",), env_file=env_file)

    assert resolved == {"SEC_USER_AGENT": "env-agent"}


def test_resolve_env_values_uses_env_file_for_blank_environment_values(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Blank env vars should be treated as missing and backfilled from the env file."""
    env_file = tmp_path / "test.env"
    env_file.write_text("SEC_USER_AGENT=file-agent\n", encoding="utf-8")
    monkeypatch.setenv("SEC_USER_AGENT", "   ")

    resolved = resolve_env_values(keys=("SEC_USER_AGENT",), env_file=env_file)

    assert resolved == {"SEC_USER_AGENT": "file-agent"}


def test_populate_env_from_file_preserves_non_blank_existing_values(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Existing non-blank env vars should remain unchanged by default."""
    env_file = tmp_path / "test.env"
    env_file.write_text("SEC_USER_AGENT=file-agent\n", encoding="utf-8")
    monkeypatch.setenv("SEC_USER_AGENT", "env-agent")

    applied = populate_env_from_file(keys=("SEC_USER_AGENT",), env_file=env_file)

    assert applied == {}
    assert os.environ["SEC_USER_AGENT"] == "env-agent"


def test_populate_env_from_file_replaces_blank_existing_values(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Blank env vars should be replaced from the env file when allowed."""
    env_file = tmp_path / "test.env"
    env_file.write_text("SEC_USER_AGENT=file-agent\n", encoding="utf-8")
    monkeypatch.setenv("SEC_USER_AGENT", "")

    applied = populate_env_from_file(keys=("SEC_USER_AGENT",), env_file=env_file)

    assert applied == {"SEC_USER_AGENT": "file-agent"}
    assert os.environ["SEC_USER_AGENT"] == "file-agent"
