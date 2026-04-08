from __future__ import annotations

from io import BytesIO
from pathlib import Path
import os
from typing import Protocol

import boto3
from dotenv import load_dotenv


R2_ENDPOINT_ENV = "R2_ENDPOINT_URL"
R2_ACCESS_KEY_ENV = "R2_ACCESS_KEY_ID"
R2_SECRET_KEY_ENV = "R2_SECRET_ACCESS_KEY"
R2_BUCKET_ENV = "R2_BUCKET_NAME"
REQUIRED_R2_ENV_VARS = (
    R2_ENDPOINT_ENV,
    R2_ACCESS_KEY_ENV,
    R2_SECRET_KEY_ENV,
    R2_BUCKET_ENV,
)
R2_ENV_FILE = Path(__file__).resolve().parents[2] / "config" / "r2.env"


class ReadableBody(Protocol):
    """Protocol for boto-style streaming response bodies."""

    def read(self) -> bytes:
        """Read the full object body."""


class CloudflareR2Client:
    """Thin Cloudflare R2 client wrapper over boto3."""

    def __init__(self, bucket_name: str, s3_client: object) -> None:
        """Store the configured bucket and boto-compatible client."""
        self.bucket_name = bucket_name
        self._client = s3_client

    @classmethod
    def from_env(cls) -> CloudflareR2Client:
        """Build a client from the expected Cloudflare R2 environment variables."""
        _load_local_r2_env_file()
        missing_vars = [name for name in REQUIRED_R2_ENV_VARS if not os.getenv(name)]
        if missing_vars:
            missing = ", ".join(sorted(missing_vars))
            raise ValueError(f"Missing required R2 environment variables: {missing}")

        bucket_name = os.environ[R2_BUCKET_ENV]
        client = boto3.client(
            "s3",
            endpoint_url=os.environ[R2_ENDPOINT_ENV],
            aws_access_key_id=os.environ[R2_ACCESS_KEY_ENV],
            aws_secret_access_key=os.environ[R2_SECRET_KEY_ENV],
            region_name="auto",
        )
        return cls(bucket_name=bucket_name, s3_client=client)

    def put_object(self, key: str, data: bytes | str) -> None:
        """Write an object to the configured bucket."""
        self._client.put_object(Bucket=self.bucket_name, Key=key, Body=_coerce_bytes(data))

    def get_object(self, key: str) -> bytes:
        """Read an object from the configured bucket."""
        response = self._client.get_object(Bucket=self.bucket_name, Key=key)
        body = response["Body"]
        if isinstance(body, BytesIO):
            return body.getvalue()
        return _read_body(body)

    def list_keys(self, prefix: str) -> list[str]:
        """List object keys beneath the given prefix."""
        paginator = self._client.get_paginator("list_objects_v2")
        keys: list[str] = []
        for page in paginator.paginate(Bucket=self.bucket_name, Prefix=prefix):
            for item in page.get("Contents", []):
                key = item.get("Key")
                if key is not None:
                    keys.append(str(key))
        return sorted(keys)


def has_required_r2_env_vars() -> bool:
    """Return True when all environment variables for real R2 access are present."""
    _load_local_r2_env_file()
    return all(os.getenv(name) for name in REQUIRED_R2_ENV_VARS)


def _coerce_bytes(data: bytes | str) -> bytes:
    """Normalize text payloads to UTF-8 bytes before transport."""
    if isinstance(data, bytes):
        return data
    return data.encode("utf-8")


def _read_body(body: ReadableBody) -> bytes:
    """Read a boto-style streaming body object."""
    return body.read()


def _load_local_r2_env_file() -> None:
    """Load local R2 settings from config/r2.env when the file exists."""
    load_dotenv(dotenv_path=R2_ENV_FILE, override=False)
