"""CLI for generating AAPL Layer 1 pilot evidence bundles."""
from __future__ import annotations

import argparse
import os
import sys
from collections.abc import Sequence
from pathlib import Path

from loguru import logger


def _resolve_repo_root() -> Path:
    """Return the repository root for local runs and Modal-mounted runs."""
    env_root = os.getenv("AI_STOCK_TRADER_REPO_ROOT")
    if env_root:
        return Path(env_root).resolve()
    resolved = Path(__file__).resolve()
    return resolved.parents[3] if len(resolved.parents) > 3 else resolved.parent


_REPO_ROOT = _resolve_repo_root()
sys.path.insert(0, str(_REPO_ROOT))

from core.features.aapl_evidence import (  # noqa: E402
    HUMAN_REVIEW_STATUSES,
    build_aapl_pilot_evidence_bundle,
    default_aapl_pilot_evidence_paths,
    write_aapl_pilot_evidence_outputs,
)
from services.r2.writer import R2Writer  # noqa: E402


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """Parse CLI arguments for AAPL pilot evidence generation."""
    parser = argparse.ArgumentParser(
        description="Generate machine and human-review evidence for the AAPL Layer 1 pilot."
    )
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--ticker", default="AAPL")
    parser.add_argument("--from-date", required=True, metavar="YYYY-MM-DD")
    parser.add_argument("--to-date", required=True, metavar="YYYY-MM-DD")
    parser.add_argument("--layer0-run-id", required=True)
    parser.add_argument("--layer1-run-id", default=None)
    parser.add_argument(
        "--human-semantic-review-status",
        choices=sorted(HUMAN_REVIEW_STATUSES),
        default="pending",
        help="Human decision on FinBERT/topic/HMM semantic correctness.",
    )
    default_paths = default_aapl_pilot_evidence_paths("<run_id>")
    parser.add_argument(
        "--write-json",
        type=Path,
        default=None,
        help=f"Machine-integrity JSON path, default {default_paths['json']}",
    )
    parser.add_argument(
        "--write-markdown",
        type=Path,
        default=None,
        help=f"Human-review Markdown path, default {default_paths['markdown']}",
    )
    parser.add_argument(
        "--write-csv",
        type=Path,
        default=None,
        help=f"Human-review CSV path, default {default_paths['csv']}",
    )
    args = parser.parse_args(argv)
    if str(args.ticker).strip().upper() != "AAPL":
        parser.error("This workflow is intentionally limited to --ticker AAPL")
    return args


def main(argv: Sequence[str] | None = None) -> int:
    """Generate AAPL pilot evidence outputs and return a fail-closed exit code."""
    args = parse_args(argv)
    writer = R2Writer()
    paths = default_aapl_pilot_evidence_paths(str(args.run_id).strip())
    json_path = args.write_json or paths["json"]
    markdown_path = args.write_markdown or paths["markdown"]
    csv_path = args.write_csv or paths["csv"]

    bundle = build_aapl_pilot_evidence_bundle(
        run_id=str(args.run_id).strip(),
        ticker="AAPL",
        from_date=str(args.from_date).strip(),
        to_date=str(args.to_date).strip(),
        layer0_run_id=str(args.layer0_run_id).strip(),
        layer1_run_id=(
            str(args.layer1_run_id).strip() if args.layer1_run_id is not None else None
        ),
        human_semantic_review_status=str(args.human_semantic_review_status).strip(),
        writer=writer,
    )
    output_paths = write_aapl_pilot_evidence_outputs(
        bundle,
        json_path=json_path,
        markdown_path=markdown_path,
        csv_path=csv_path,
    )
    logger.info("AAPL evidence JSON written: {}", output_paths["json"])
    logger.info("AAPL human-review Markdown written: {}", output_paths["markdown"])
    logger.info("AAPL human-review CSV written: {}", output_paths["csv"])
    logger.info("Machine integrity: {}", bundle.machine_integrity_status)
    logger.info("Human semantic review: {}", bundle.human_semantic_review_status)
    logger.info("Recommendation for #202: {}", bundle.recommendation_for_issue_202)
    if bundle.recommendation_for_issue_202 != "proceed":
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
