from __future__ import annotations

import argparse
import json
import math
import random
from datetime import date, timedelta
from pathlib import Path

from cloud_training.model_architecture.hybrid_model import FEATURE_NAMES

ROOT_DIR = Path(__file__).resolve().parents[2]
OUTPUT_DIR = ROOT_DIR / "data" / "processed" / "training"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate a synthetic Issue #12 dataset for local smoke validation")
    parser.add_argument("--rows", type=int, default=360)
    parser.add_argument("--output-prefix", default="issue12_synthetic_smoke")
    parser.add_argument("--seed", type=int, default=12)
    return parser.parse_args()


def _history(base_price: float, row_idx: int) -> list[dict[str, float | int | str]]:
    start = date(2024, 1, 1) + timedelta(days=row_idx)
    history = []
    for offset in range(21):
        px = base_price * (1.0 + 0.002 * offset)
        history.append({
            "date": (start + timedelta(days=offset)).isoformat(),
            "open": round(px * 0.997, 4),
            "high": round(px * 1.003, 4),
            "low": round(px * 0.994, 4),
            "close": round(px, 4),
            "volume": 1_000_000 + (offset * 1000),
        })
    return history


def main() -> None:
    args = parse_args()
    rng = random.Random(args.seed)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    rows = []
    start = date(2024, 1, 1)
    tickers = ["AAA", "BBB", "CCC"]
    for idx in range(args.rows):
        ticker = tickers[idx % len(tickers)]
        as_of_date = start + timedelta(days=idx)
        target_date = as_of_date + timedelta(days=1)
        latent = math.sin(idx / 17.0) + (0.35 if ticker == "AAA" else -0.1 if ticker == "BBB" else 0.05)
        sample = {
            "ticker": ticker,
            "as_of_date": as_of_date.isoformat(),
            "target_date": target_date.isoformat(),
            "sequence_length": 21,
            "history": _history(100.0 + idx * 0.1, idx),
            "news": [],
        }
        for feature_idx, feature_name in enumerate(FEATURE_NAMES):
            noise = rng.uniform(-0.15, 0.15)
            weight = 1.0 if feature_idx < 8 else 0.5 if feature_idx < 20 else 0.2
            sample[feature_name] = round((latent * weight) + noise, 6)
        target_score = (
            1.8 * sample["return_5d"]
            + 1.6 * sample["return_21d"]
            - 1.2 * sample["realized_vol_21d"]
            + 0.9 * sample["finbert_sentiment_score_recency_weighted"]
            + 0.6 * sample["sector_relative_return_21d"]
            + rng.uniform(-0.2, 0.2)
        )
        target_positive = 1 if target_score > 0 else 0
        sample["target_positive_return"] = target_positive
        sample["target_log_return_t_plus_1"] = round(0.012 if target_positive else -0.011, 6)
        rows.append(sample)

    prefix = f"{args.output_prefix}_{args.rows}rows"
    jsonl_path = OUTPUT_DIR / f"{prefix}.jsonl"
    manifest_path = OUTPUT_DIR / f"{prefix}.manifest.json"
    with jsonl_path.open("w") as handle:
        for row in rows:
            handle.write(json.dumps(row) + "\n")
    manifest_path.write_text(json.dumps({
        "dataset_name": prefix,
        "rows": len(rows),
        "target": "synthetic_next_day_log_return_positive_probability",
        "jsonl_path": str(jsonl_path.relative_to(ROOT_DIR)),
        "purpose": "issue12_local_smoke_only",
        "notes": "Synthetic numerically correlated dataset used to validate train/export plumbing without heavy compute or external data dependencies.",
    }, indent=2))
    print(json.dumps({
        "status": "ok",
        "jsonl": str(jsonl_path.relative_to(ROOT_DIR)),
        "manifest": str(manifest_path.relative_to(ROOT_DIR)),
        "rows": len(rows),
    }, indent=2))


if __name__ == "__main__":
    main()
