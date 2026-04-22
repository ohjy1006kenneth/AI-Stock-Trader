from __future__ import annotations

import json
from pathlib import Path
from typing import Any

_SAMPLE_DIR = Path(__file__).resolve().parents[2] / "data" / "sample"


def load_sample_json(filename: str) -> dict[str, Any]:
    """Load a JSON fixture from the repository sample-data directory."""
    path = _SAMPLE_DIR / filename
    return json.loads(path.read_text(encoding="utf-8"))
