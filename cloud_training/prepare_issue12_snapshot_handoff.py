from __future__ import annotations

import argparse
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT_DIR = Path(__file__).resolve().parents[1]
PRICE_SNAPSHOT_PATH = ROOT_DIR / "data" / "runtime" / "market" / "price_snapshot.json"
FUNDAMENTAL_SNAPSHOT_PATH = ROOT_DIR / "data" / "runtime" / "market" / "fundamental_snapshot.json"
DEFAULT_OUTPUT_DIR = ROOT_DIR / "artifacts" / "deployments" / "issue12_snapshot_handoff"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate and stage the Issue #12 market snapshot inputs for cloud runtime provisioning."
    )
    parser.add_argument(
        "--output-dir",
        default=str(DEFAULT_OUTPUT_DIR),
        help="directory to receive the staged snapshot package",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="replace an existing output directory if present",
    )
    return parser.parse_args()


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text())


def _snapshot_summary(name: str, path: Path) -> dict[str, Any]:
    if not path.exists():
        raise SystemExit(f"missing_required_snapshot:{path.relative_to(ROOT_DIR)}")
    payload = _load_json(path)
    items = payload.get("items", []) if isinstance(payload, dict) else []
    if not isinstance(items, list) or not items:
        raise SystemExit(f"snapshot_has_no_items:{path.relative_to(ROOT_DIR)}")
    first_item = items[0] if isinstance(items[0], dict) else {}
    history_len = len(first_item.get("history", [])) if isinstance(first_item.get("history"), list) else None
    return {
        "name": name,
        "relative_path": str(path.relative_to(ROOT_DIR)),
        "generated_at": payload.get("generated_at"),
        "source": payload.get("source"),
        "fallback_source_used": payload.get("fallback_source_used"),
        "item_count": len(items),
        "file_size_bytes": path.stat().st_size,
        "first_item_ticker": first_item.get("ticker"),
        "first_item_history_rows": history_len,
    }


def stage_package(output_dir: Path, overwrite: bool) -> dict[str, Any]:
    if output_dir.exists():
        if not overwrite:
            raise SystemExit(f"output_dir_exists_use_overwrite:{output_dir}")
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    staged_market_dir = output_dir / "data" / "runtime" / "market"
    staged_market_dir.mkdir(parents=True, exist_ok=True)

    price_summary = _snapshot_summary("price_snapshot", PRICE_SNAPSHOT_PATH)
    fundamental_summary = _snapshot_summary("fundamental_snapshot", FUNDAMENTAL_SNAPSHOT_PATH)

    staged_price = staged_market_dir / PRICE_SNAPSHOT_PATH.name
    staged_fundamental = staged_market_dir / FUNDAMENTAL_SNAPSHOT_PATH.name
    shutil.copy2(PRICE_SNAPSHOT_PATH, staged_price)
    shutil.copy2(FUNDAMENTAL_SNAPSHOT_PATH, staged_fundamental)

    manifest = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "issue": 12,
        "purpose": "cloud_runtime_snapshot_provisioning",
        "package_root": str(output_dir.relative_to(ROOT_DIR)) if output_dir.is_relative_to(ROOT_DIR) else str(output_dir),
        "staged_files": {
            "price_snapshot": str(staged_price.relative_to(output_dir)),
            "fundamental_snapshot": str(staged_fundamental.relative_to(output_dir)),
        },
        "source_workspace": str(ROOT_DIR),
        "operator_instructions": [
            "Copy the staged data/runtime/market/*.json files into the cloud/HF runtime before running Issue #12.",
            "Ensure ALPACA_API_KEY and ALPACA_API_SECRET are available in the cloud runtime.",
            "Run `.venv/bin/python -m cloud_training.training.run_issue12_cloud_pipeline --build-dataset --epochs 250 --export-bundle` in the cloud runtime.",
        ],
        "snapshots": {
            "price_snapshot": price_summary,
            "fundamental_snapshot": fundamental_summary,
        },
    }
    manifest_path = output_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2))
    return {
        "output_dir": str(output_dir),
        "manifest": manifest,
        "manifest_path": str(manifest_path),
    }


def main() -> None:
    args = parse_args()
    output_dir = Path(args.output_dir)
    if not output_dir.is_absolute():
        output_dir = ROOT_DIR / output_dir
    result = stage_package(output_dir, overwrite=args.overwrite)
    print(json.dumps({
        "status": "ok",
        "output_dir": str(Path(result["output_dir"]).relative_to(ROOT_DIR)),
        "manifest_path": str(Path(result["manifest_path"]).relative_to(ROOT_DIR)),
        "price_snapshot": result["manifest"]["snapshots"]["price_snapshot"],
        "fundamental_snapshot": result["manifest"]["snapshots"]["fundamental_snapshot"],
    }, indent=2))


if __name__ == "__main__":
    main()
