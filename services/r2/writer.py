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

    def delete_object(self, key: str) -> None:
        """Delete an object from the local mock root when it exists."""
        target = self._resolve_key(key)
        if target.exists():
            target.unlink()

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
        """Select the local mock when `local_root` is supplied, otherwise the real client.

        Passing an explicit `local_root` is a hard request for the local mock — even when
        Cloudflare R2 credentials are present in the environment. This prevents tests that
        intend to write to a temporary directory from silently leaking objects into the
        production R2 bucket. Production callers continue to construct `R2Writer()` with
        no arguments, which selects the real client whenever env vars are configured.
        """
        if local_root is not None:
            self._client = LocalR2Client(root=local_root.resolve())
            self.mode = "local"
        elif has_required_r2_env_vars():
            self._client = CloudflareR2Client.from_env()
            self.mode = "r2"
        else:
            self._client = LocalR2Client(root=DEFAULT_LOCAL_R2_ROOT.resolve())
            self.mode = "local"

    def put_object(self, key: str, data: bytes | str) -> None:
        """Write an object via the active backend."""
        self._client.put_object(key, data)

    def get_object(self, key: str) -> bytes:
        """Read an object via the active backend."""
        return self._client.get_object(key)

    def delete_object(self, key: str) -> None:
        """Delete an object via the active backend."""
        self._client.delete_object(key)

    def list_keys(self, prefix: str) -> list[str]:
        """List keys via the active backend."""
        return self._client.list_keys(prefix)

    def exists(self, key: str) -> bool:
        """Return True when the key exists in the active backend."""
        return self._client.exists(key)


def _coerce_bytes(data: bytes | str) -> bytes:
    """Normalize text payloads to UTF-8 bytes before persistence."""
    if isinstance(data, bytes):
        return data
    return data.encode("utf-8")
