from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from services.r2.client import CloudflareR2Client  # noqa: E402
from services.r2.writer import R2Writer  # noqa: E402


@pytest.mark.skipif(
    os.getenv("RUN_R2_INTEGRATION") != "1",
    reason="Set RUN_R2_INTEGRATION=1 to run live R2 integration checks.",
)
def test_r2_live_round_trip() -> None:
    """Verify the configured Cloudflare R2 bucket accepts a simple round trip."""
    writer = R2Writer()
    assert writer.mode == "r2"

    client = CloudflareR2Client.from_env()
    test_key = "integration/r2-smoke/latest.txt"
    expected_payload = b"r2-smoke-ok"

    writer.put_object(test_key, expected_payload)

    assert writer.get_object(test_key) == expected_payload
    assert test_key in client.list_keys("integration/r2-smoke/")
