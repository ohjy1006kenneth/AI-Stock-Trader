from __future__ import annotations

from pathlib import Path

from services.r2.client import CloudflareR2Client, has_required_r2_env_vars


DEFAULT_LOCAL_R2_ROOT = Path("data/runtime/r2_mock")


class LocalR2Client:
    """Filesystem-backed mock object store used when real R2 credentials are absent."""

    def __init__(self, root: Path) -> None:
        """Initialize the mock storage root."""
        self.root = root.resolve()
        self.root.mkdir(parents=True, exist_ok=True)

    def put_object(self, key: str, data: bytes | str) -> None:
        """Persist an object beneath the local mock root."""
        target = self._resolve_key(key)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(_coerce_bytes(data))

    def get_object(self, key: str) -> bytes:
        """Read an object from the local mock root."""
        return self._resolve_key(key).read_bytes()

    def list_keys(self, prefix: str) -> list[str]:
        """List all keys beneath the local mock root that match the prefix."""
        keys = [
            path.relative_to(self.root).as_posix()
            for path in self.root.rglob("*")
            if path.is_file()
        ]
        return sorted(key for key in keys if key.startswith(prefix))

    def exists(self, key: str) -> bool:
        """Return True when a key exists in the local mock root."""
        return self._resolve_key(key).exists()

    def _resolve_key(self, key: str) -> Path:
        """Resolve a key to a safe path beneath the configured root."""
        candidate = (self.root / key).resolve()
        try:
            candidate.relative_to(self.root)
        except ValueError as exc:
            raise ValueError(f"Key escapes the configured mock root: {key}") from exc
        return candidate


class R2Writer:
    """Unified object-store wrapper for real R2 and the local mock."""

    def __init__(self, local_root: Path | None = None) -> None:
        """Select the real client when credentials exist, otherwise use the local mock."""
        if has_required_r2_env_vars():
            self._client = CloudflareR2Client.from_env()
            self.mode = "r2"
        else:
            root = (local_root or DEFAULT_LOCAL_R2_ROOT).resolve()
            self._client = LocalR2Client(root=root)
            self.mode = "local"

    def put_object(self, key: str, data: bytes | str) -> None:
        """Write an object via the active backend."""
        self._client.put_object(key, data)

    def get_object(self, key: str) -> bytes:
        """Read an object via the active backend."""
        return self._client.get_object(key)

    def list_keys(self, prefix: str) -> list[str]:
        """List keys via the active backend."""
        return self._client.list_keys(prefix)

    def exists(self, key: str) -> bool:
        """Return True when the key exists in the active backend."""
        if hasattr(self._client, "exists"):
            return self._client.exists(key)
        return key in self.list_keys(key)


def _coerce_bytes(data: bytes | str) -> bytes:
    """Normalize text payloads to UTF-8 bytes before persistence."""
    if isinstance(data, bytes):
        return data
    return data.encode("utf-8")
