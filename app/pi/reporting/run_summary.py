from __future__ import annotations


def build_run_summary(manifests: list[dict[str, object]]) -> dict[str, object]:
    """Build a compact deterministic summary from stage manifests."""
    return {
        "stage_count": len(manifests),
        "stages": [manifest["stage"] for manifest in manifests],
        "all_completed": all(manifest["status"] == "completed" for manifest in manifests),
    }
