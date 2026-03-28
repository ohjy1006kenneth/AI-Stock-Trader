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

from runtime.common.common import CONFIG_DIR, env_str, load_local_env_file


def require_hf_endpoint_config() -> str:
    load_local_env_file(CONFIG_DIR / "alpaca.env")
    endpoint = env_str("HF_INFERENCE_URL")
    if not endpoint:
        raise RuntimeError("missing_hf_inference_url")
    return endpoint
