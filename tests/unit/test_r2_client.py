from __future__ import annotations

from io import BytesIO
from pathlib import Path
import sys


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from services.r2.client import (
    R2_ACCESS_KEY_ENV,
    R2_BUCKET_ENV,
    R2_ENDPOINT_ENV,
    R2_SECRET_KEY_ENV,
    CloudflareR2Client,
    has_required_r2_env_vars,
)
from services.r2.writer import LocalR2Client, R2Writer


class FakePaginator:
    """Paginator stub for list_objects_v2 responses."""

    def __init__(self, pages: list[dict[str, object]]) -> None:
        """Store the pages to replay."""
        self.pages = pages
        self.calls: list[dict[str, str]] = []

    def paginate(self, **kwargs: str) -> list[dict[str, object]]:
        """Record pagination calls and return stub pages."""
        self.calls.append(kwargs)
        return self.pages


class FakeS3Client:
    """Small boto3 client stub for unit tests."""

    def __init__(self) -> None:
        """Initialize empty call tracking."""
        self.put_calls: list[dict[str, object]] = []
        self.get_calls: list[dict[str, str]] = []
        self.paginator = FakePaginator(
            pages=[
                {"Contents": [{"Key": "raw/prices/AAPL.parquet"}]},
                {"Contents": [{"Key": "raw/news/2026-04-08.jsonl"}]},
            ]
        )

    def put_object(self, **kwargs: object) -> None:
        """Record put_object calls."""
        self.put_calls.append(kwargs)

    def get_object(self, **kwargs: str) -> dict[str, BytesIO]:
        """Return a stub get_object response."""
        self.get_calls.append(kwargs)
        return {"Body": BytesIO(b"payload")}

    def get_paginator(self, operation_name: str) -> FakePaginator:
        """Return the list_objects_v2 paginator stub."""
        assert operation_name == "list_objects_v2"
        return self.paginator


def test_cloudflare_r2_client_from_env_configures_boto3(monkeypatch) -> None:
    """CloudflareR2Client.from_env should build a boto3 client from env vars."""
    captured: dict[str, object] = {}

    def fake_boto_client(service_name: str, **kwargs: object) -> object:
        captured["service_name"] = service_name
        captured["kwargs"] = kwargs
        return object()

    monkeypatch.setenv(R2_ENDPOINT_ENV, "https://example.r2.cloudflarestorage.com")
    monkeypatch.setenv(R2_ACCESS_KEY_ENV, "access-key")
    monkeypatch.setenv(R2_SECRET_KEY_ENV, "secret-key")
    monkeypatch.setenv(R2_BUCKET_ENV, "bucket-name")
    monkeypatch.setattr("services.r2.client.boto3.client", fake_boto_client)

    client = CloudflareR2Client.from_env()

    assert client.bucket_name == "bucket-name"
    assert captured["service_name"] == "s3"
    assert captured["kwargs"] == {
        "endpoint_url": "https://example.r2.cloudflarestorage.com",
        "aws_access_key_id": "access-key",
        "aws_secret_access_key": "secret-key",
        "region_name": "auto",
    }


def test_cloudflare_r2_client_from_env_loads_local_config_file(
    tmp_path: Path, monkeypatch
) -> None:
    """CloudflareR2Client.from_env should load credentials from config/r2.env."""
    env_file = tmp_path / "r2.env"
    env_file.write_text(
        "\n".join(
            [
                "R2_ENDPOINT_URL=https://from-file.r2.cloudflarestorage.com",
                "R2_ACCESS_KEY_ID=file-access-key",
                "R2_SECRET_ACCESS_KEY=file-secret-key",
                "R2_BUCKET_NAME=file-bucket",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    captured: dict[str, object] = {}

    def fake_boto_client(service_name: str, **kwargs: object) -> object:
        captured["service_name"] = service_name
        captured["kwargs"] = kwargs
        return object()

    monkeypatch.delenv(R2_ENDPOINT_ENV, raising=False)
    monkeypatch.delenv(R2_ACCESS_KEY_ENV, raising=False)
    monkeypatch.delenv(R2_SECRET_KEY_ENV, raising=False)
    monkeypatch.delenv(R2_BUCKET_ENV, raising=False)
    monkeypatch.setattr("services.r2.client.R2_ENV_FILE", env_file)
    monkeypatch.setattr("services.r2.client.boto3.client", fake_boto_client)

    client = CloudflareR2Client.from_env()

    assert client.bucket_name == "file-bucket"
    assert captured["service_name"] == "s3"
    assert captured["kwargs"] == {
        "endpoint_url": "https://from-file.r2.cloudflarestorage.com",
        "aws_access_key_id": "file-access-key",
        "aws_secret_access_key": "file-secret-key",
        "region_name": "auto",
    }


def test_cloudflare_r2_client_requires_all_env_vars(monkeypatch) -> None:
    """CloudflareR2Client.from_env should fail when required env vars are missing."""
    monkeypatch.delenv(R2_ENDPOINT_ENV, raising=False)
    monkeypatch.delenv(R2_ACCESS_KEY_ENV, raising=False)
    monkeypatch.delenv(R2_SECRET_KEY_ENV, raising=False)
    monkeypatch.delenv(R2_BUCKET_ENV, raising=False)
    monkeypatch.setattr("services.r2.client.R2_ENV_FILE", Path("/tmp/does-not-exist-r2.env"))

    try:
        CloudflareR2Client.from_env()
    except ValueError as exc:
        assert "Missing required R2 environment variables" in str(exc)
        return
    assert False, "Expected ValueError when R2 environment variables are missing"


def test_cloudflare_r2_client_delegates_object_operations() -> None:
    """CloudflareR2Client should proxy puts, gets, and key listing to boto3."""
    fake_client = FakeS3Client()
    client = CloudflareR2Client(bucket_name="bucket-name", s3_client=fake_client)

    client.put_object("raw/prices/AAPL.parquet", "hello")
    payload = client.get_object("raw/prices/AAPL.parquet")
    keys = client.list_keys("raw/")

    assert fake_client.put_calls == [
        {
            "Bucket": "bucket-name",
            "Key": "raw/prices/AAPL.parquet",
            "Body": b"hello",
        }
    ]
    assert fake_client.get_calls == [
        {"Bucket": "bucket-name", "Key": "raw/prices/AAPL.parquet"}
    ]
    assert payload == b"payload"
    assert keys == ["raw/news/2026-04-08.jsonl", "raw/prices/AAPL.parquet"]
    assert fake_client.paginator.calls == [{"Bucket": "bucket-name", "Prefix": "raw/"}]


def test_has_required_r2_env_vars_checks_full_set(monkeypatch) -> None:
    """has_required_r2_env_vars should only return True when every env var exists."""
    monkeypatch.setattr("services.r2.client.R2_ENV_FILE", Path("/tmp/does-not-exist-r2.env"))
    for env_var in (R2_ENDPOINT_ENV, R2_ACCESS_KEY_ENV, R2_SECRET_KEY_ENV, R2_BUCKET_ENV):
        monkeypatch.setenv(env_var, env_var.lower())

    assert has_required_r2_env_vars() is True

    monkeypatch.delenv(R2_BUCKET_ENV, raising=False)
    assert has_required_r2_env_vars() is False


def test_has_required_r2_env_vars_loads_local_config_file(
    tmp_path: Path, monkeypatch
) -> None:
    """has_required_r2_env_vars should load config/r2.env when present."""
    env_file = tmp_path / "r2.env"
    env_file.write_text(
        "\n".join(
            [
                "R2_ENDPOINT_URL=https://example.r2.cloudflarestorage.com",
                "R2_ACCESS_KEY_ID=access-key",
                "R2_SECRET_ACCESS_KEY=secret-key",
                "R2_BUCKET_NAME=bucket-name",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    monkeypatch.delenv(R2_ENDPOINT_ENV, raising=False)
    monkeypatch.delenv(R2_ACCESS_KEY_ENV, raising=False)
    monkeypatch.delenv(R2_SECRET_KEY_ENV, raising=False)
    monkeypatch.delenv(R2_BUCKET_ENV, raising=False)
    monkeypatch.setattr("services.r2.client.R2_ENV_FILE", env_file)

    assert has_required_r2_env_vars() is True


def test_local_r2_client_round_trips_objects(tmp_path: Path) -> None:
    """LocalR2Client should store bytes on disk and list them by prefix."""
    client = LocalR2Client(root=tmp_path)

    client.put_object("raw/prices/AAPL.parquet", b"abc")
    client.put_object("raw/news/2026-04-08.jsonl", "headline")

    assert client.get_object("raw/prices/AAPL.parquet") == b"abc"
    assert client.exists("raw/news/2026-04-08.jsonl") is True
    assert client.list_keys("raw/") == [
        "raw/news/2026-04-08.jsonl",
        "raw/prices/AAPL.parquet",
    ]


def test_local_r2_client_rejects_path_escape(tmp_path: Path) -> None:
    """LocalR2Client should reject keys that escape the mock root."""
    client = LocalR2Client(root=tmp_path)

    try:
        client.put_object("../outside.txt", b"bad")
    except ValueError as exc:
        assert "escapes the configured mock root" in str(exc)
        return
    assert False, "Expected ValueError for a key that escapes the root"


def test_r2_writer_falls_back_to_local_mock(tmp_path: Path, monkeypatch) -> None:
    """R2Writer should use the local filesystem mock when R2 env vars are absent."""
    monkeypatch.delenv(R2_ENDPOINT_ENV, raising=False)
    monkeypatch.delenv(R2_ACCESS_KEY_ENV, raising=False)
    monkeypatch.delenv(R2_SECRET_KEY_ENV, raising=False)
    monkeypatch.delenv(R2_BUCKET_ENV, raising=False)
    monkeypatch.setattr("services.r2.client.R2_ENV_FILE", Path("/tmp/does-not-exist-r2.env"))

    writer = R2Writer(local_root=tmp_path)
    writer.put_object("raw/universe/2026-04-08.csv", "ticker,date")

    assert writer.mode == "local"
    assert writer.get_object("raw/universe/2026-04-08.csv") == b"ticker,date"
    assert writer.exists("raw/universe/2026-04-08.csv") is True


def test_r2_writer_uses_remote_client_when_env_vars_exist(monkeypatch) -> None:
    """R2Writer should select the CloudflareR2Client when all env vars are present."""
    fake_client = FakeS3Client()

    monkeypatch.setenv(R2_ENDPOINT_ENV, "https://example.r2.cloudflarestorage.com")
    monkeypatch.setenv(R2_ACCESS_KEY_ENV, "access-key")
    monkeypatch.setenv(R2_SECRET_KEY_ENV, "secret-key")
    monkeypatch.setenv(R2_BUCKET_ENV, "bucket-name")
    monkeypatch.setattr(
        "services.r2.writer.CloudflareR2Client.from_env",
        classmethod(lambda cls: CloudflareR2Client(bucket_name="bucket-name", s3_client=fake_client)),
    )

    writer = R2Writer()
    writer.put_object("processed/features/2026-04-08.parquet", b"binary-data")

    assert writer.mode == "r2"
    assert fake_client.put_calls == [
        {
            "Bucket": "bucket-name",
            "Key": "processed/features/2026-04-08.parquet",
            "Body": b"binary-data",
        }
    ]


def test_r2_writer_uses_remote_client_when_local_config_file_exists(
    tmp_path: Path, monkeypatch
) -> None:
    """R2Writer should switch to R2 mode when config/r2.env provides credentials."""
    env_file = tmp_path / "r2.env"
    env_file.write_text(
        "\n".join(
            [
                "R2_ENDPOINT_URL=https://from-file.r2.cloudflarestorage.com",
                "R2_ACCESS_KEY_ID=file-access-key",
                "R2_SECRET_ACCESS_KEY=file-secret-key",
                "R2_BUCKET_NAME=file-bucket",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    fake_client = FakeS3Client()

    monkeypatch.delenv(R2_ENDPOINT_ENV, raising=False)
    monkeypatch.delenv(R2_ACCESS_KEY_ENV, raising=False)
    monkeypatch.delenv(R2_SECRET_KEY_ENV, raising=False)
    monkeypatch.delenv(R2_BUCKET_ENV, raising=False)
    monkeypatch.setattr("services.r2.client.R2_ENV_FILE", env_file)
    monkeypatch.setattr(
        "services.r2.writer.CloudflareR2Client.from_env",
        classmethod(lambda cls: CloudflareR2Client(bucket_name="file-bucket", s3_client=fake_client)),
    )

    writer = R2Writer()
    writer.put_object("processed/features/2026-04-08.parquet", b"binary-data")

    assert writer.mode == "r2"
    assert fake_client.put_calls == [
        {
            "Bucket": "file-bucket",
            "Key": "processed/features/2026-04-08.parquet",
            "Body": b"binary-data",
        }
    ]
