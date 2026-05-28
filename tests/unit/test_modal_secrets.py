from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import pytest

from services.modal.secrets import build_modal_secrets


@dataclass(frozen=True)
class _FakeSecretRef:
    """Minimal Modal secret reference for helper tests."""

    name: str | None = None
    payload: dict[str, str] = field(default_factory=dict)


class _FakeSecretFactory:
    """Fake `modal.Secret` API surface."""

    @staticmethod
    def from_name(name: str) -> _FakeSecretRef:
        """Return a named secret reference."""
        return _FakeSecretRef(name=name)

    @staticmethod
    def from_dict(payload: dict[str, str]) -> _FakeSecretRef:
        """Return an inline secret reference."""
        return _FakeSecretRef(payload=dict(payload))


class _FakeModal:
    """Fake Modal module for secret-helper tests."""

    Secret = _FakeSecretFactory


def test_build_modal_secrets_adds_named_and_env_file_secrets(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """The helper should attach named secrets plus resolved env-file values."""
    env_file = tmp_path / "simfin.env"
    env_file.write_text(
        "\n".join(
            [
                "SIMFIN_API_KEY=file-key",
                "SIMFIN_BASE_URL=https://example.simfin.test/api/v3",
                "SEC_USER_AGENT=file-agent",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.delenv("SIMFIN_API_KEY", raising=False)
    monkeypatch.setenv("SIMFIN_BASE_URL", "https://env.simfin.test/api/v3")
    monkeypatch.setenv("SEC_USER_AGENT", "   ")

    secrets = build_modal_secrets(
        _FakeModal,
        named_secret_names=("ai-stock-trader-r2",),
        env_file=env_file,
        env_keys=("SIMFIN_API_KEY", "SIMFIN_BASE_URL", "SEC_USER_AGENT"),
    )

    assert secrets[0] == _FakeSecretRef(name="ai-stock-trader-r2")
    assert secrets[1] == _FakeSecretRef(
        payload={
            "SIMFIN_API_KEY": "file-key",
            "SIMFIN_BASE_URL": "https://env.simfin.test/api/v3",
            "SEC_USER_AGENT": "file-agent",
        }
    )


def test_build_modal_secrets_omits_inline_secret_when_no_env_values(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """The helper should skip `from_dict` when no selected env values resolve."""
    env_file = tmp_path / "empty.env"
    env_file.write_text("", encoding="utf-8")
    monkeypatch.delenv("SEC_USER_AGENT", raising=False)

    secrets = build_modal_secrets(
        _FakeModal,
        named_secret_names=("ai-stock-trader-r2",),
        env_file=env_file,
        env_keys=("SEC_USER_AGENT",),
    )

    assert secrets == [_FakeSecretRef(name="ai-stock-trader-r2")]
