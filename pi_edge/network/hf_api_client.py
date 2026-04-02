from __future__ import annotations

from pathlib import Path
import sys

ROOT_DIR = Path(__file__).resolve()
for _ in range(5):
    if (ROOT_DIR / ".gitignore").exists():
        break
    ROOT_DIR = ROOT_DIR.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import json
import time
import uuid
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

from cloud_inference.contracts import validate_request_payload, validate_response_payload
from runtime.common.common import CONFIG_DIR, env_str, load_local_env_file

READY_MANIFEST_ENV = "HF_MODEL_REPO_READY_MANIFEST_URL"
MODEL_REPO_ID_ENV = "HF_MODEL_REPO_ID"
ENFORCE_READY_MANIFEST_ENV = "HF_ENFORCE_READY_MANIFEST"
DEFAULT_READY_MANIFEST_PATH = "endpoints/oracle/ready.json"
DEFAULT_RETRYABLE_HTTP_CODES = {429, 500, 502, 503, 504}


def _env_flag(name: str, default: bool = False) -> bool:
    value = env_str(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _retry_delay_seconds(*, attempt: int, retry_after_header: str | None) -> float:
    if retry_after_header:
        try:
            parsed = float(retry_after_header.strip())
            if parsed >= 0:
                return min(parsed, 30.0)
        except Exception:
            pass
    return min(2 ** max(attempt - 1, 0), 30)


def _http_json_request(url: str, *, token: str | None = None, method: str = "GET", payload: dict[str, Any] | None = None, timeout: int = 60, extra_headers: dict[str, str] | None = None, max_attempts: int = 1, retryable_http_codes: set[int] | None = None) -> dict[str, Any]:
    retryable_http_codes = retryable_http_codes or DEFAULT_RETRYABLE_HTTP_CODES
    for attempt in range(1, max_attempts + 1):
        headers = {
            "Accept": "application/json",
            **(extra_headers or {}),
        }
        if token:
            headers["Authorization"] = f"Bearer {token}"
        data = None
        if payload is not None:
            headers["Content-Type"] = "application/json"
            data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(url, data=data, method=method, headers=headers)
        try:
            with urllib.request.urlopen(req, timeout=timeout) as response:
                body = response.read().decode("utf-8")
            break
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8") if exc.fp else ""
            if exc.code in retryable_http_codes and attempt < max_attempts:
                time.sleep(_retry_delay_seconds(attempt=attempt, retry_after_header=exc.headers.get("Retry-After") if exc.headers else None))
                continue
            raise RuntimeError(f"hf_http_{exc.code}:{body}") from exc
        except urllib.error.URLError as exc:
            if attempt < max_attempts:
                time.sleep(_retry_delay_seconds(attempt=attempt, retry_after_header=None))
                continue
            raise RuntimeError(f"hf_network_error:{exc}") from exc

    try:
        parsed = json.loads(body) if body else {}
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"hf_invalid_json_response:{exc}") from exc
    if not isinstance(parsed, dict):
        raise RuntimeError("hf_response_must_be_json_object")
    return parsed


def _build_model_repo_ready_manifest_url(repo_id: str) -> str:
    quoted_repo = "/".join(urllib.parse.quote(part, safe="") for part in repo_id.split("/"))
    return f"https://huggingface.co/{quoted_repo}/resolve/main/{DEFAULT_READY_MANIFEST_PATH}"


def require_hf_config() -> tuple[str, str]:
    load_local_env_file(CONFIG_DIR / "alpaca.env")
    endpoint = env_str("HF_INFERENCE_URL")
    token = env_str("HF_API_TOKEN")
    if not endpoint:
        raise RuntimeError("missing_hf_inference_url")
    if not token:
        raise RuntimeError("missing_hf_api_token")
    return endpoint, token


def resolve_ready_manifest_url() -> str | None:
    load_local_env_file(CONFIG_DIR / "alpaca.env")
    explicit = env_str(READY_MANIFEST_ENV)
    if explicit:
        return explicit
    repo_id = env_str(MODEL_REPO_ID_ENV)
    if repo_id:
        return _build_model_repo_ready_manifest_url(repo_id)
    return None


def fetch_endpoint_ready_manifest(*, token: str | None = None, timeout: int = 30) -> dict[str, Any] | None:
    url = resolve_ready_manifest_url()
    if not url:
        return None
    manifest = _http_json_request(url, token=token, timeout=timeout)
    approved_bundle = manifest.get("approved_bundle", {})
    if manifest.get("manifest_version") != "oracle_endpoint_ready_v1":
        raise RuntimeError(f"hf_ready_manifest_invalid_version:{manifest.get('manifest_version')}")
    if manifest.get("endpoint") != "cloud_oracle":
        raise RuntimeError(f"hf_ready_manifest_invalid_endpoint:{manifest.get('endpoint')}")
    artifact_name = approved_bundle.get("artifact_name")
    repo_path = approved_bundle.get("repo_path")
    if not isinstance(artifact_name, str) or not artifact_name:
        raise RuntimeError("hf_ready_manifest_missing_artifact_name")
    if not isinstance(repo_path, str) or not repo_path:
        raise RuntimeError("hf_ready_manifest_missing_repo_path")
    return manifest


def validate_response_against_ready_manifest(response_payload: dict[str, Any], ready_manifest: dict[str, Any]) -> dict[str, Any]:
    approved_bundle = ready_manifest.get("approved_bundle", {})
    expected_artifact = approved_bundle.get("artifact_name")
    actual_model_version = response_payload.get("model_version")
    if actual_model_version != expected_artifact:
        raise RuntimeError(
            f"hf_model_version_mismatch:expected={expected_artifact}:actual={actual_model_version}"
        )
    return {
        "manifest_version": ready_manifest.get("manifest_version"),
        "endpoint": ready_manifest.get("endpoint"),
        "expected_artifact_name": expected_artifact,
        "expected_repo_path": approved_bundle.get("repo_path"),
        "approved_manifest_path": approved_bundle.get("approved_manifest_path"),
        "bundle_manifest_path": approved_bundle.get("bundle_manifest_path"),
        "repo_id": ready_manifest.get("repo_id"),
    }


def call_oracle(payload: dict[str, Any]) -> dict[str, Any]:
    endpoint, token = require_hf_config()
    validate_request_payload(payload)
    request_id = str(uuid.uuid4())
    transport_payload = {"inputs": payload, "request_id": request_id}
    parsed = _http_json_request(
        endpoint,
        token=token,
        method="POST",
        payload=transport_payload,
        timeout=60,
        extra_headers={"X-Request-Id": request_id},
        max_attempts=3,
    )

    universe_tickers = [item["ticker"] for item in payload.get("universe", [])]
    current_positions = [item["ticker"] for item in payload.get("portfolio", {}).get("positions", [])]
    try:
        validate_response_payload(parsed, universe_tickers, current_positions)
    except ValueError as exc:
        raise RuntimeError(f"hf_invalid_response:{exc}") from exc

    ready_manifest = fetch_endpoint_ready_manifest(token=token)
    if ready_manifest is not None:
        enforcement_enabled = _env_flag(ENFORCE_READY_MANIFEST_ENV, default=False)
        try:
            validate_response_against_ready_manifest(parsed, ready_manifest)
        except RuntimeError:
            if enforcement_enabled:
                raise

    return parsed
