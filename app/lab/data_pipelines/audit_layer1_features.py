"""Operator-facing Layer 1 feature correctness audit."""
from __future__ import annotations

import argparse
import os
import sys
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

from core.features.audit import (  # noqa: E402
    DEFAULT_AUDIT_OUTPUT_DIR,
    audit_layer1_features,
    render_audit_summary,
    write_audit_report,
)
from services.r2.writer import R2Writer  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse CLI arguments for the Layer 1 feature audit."""
    parser = argparse.ArgumentParser(description="Audit Layer 1 feature correctness.")
    parser.add_argument(
        "--run-id",
        default=None,
        help="Optional audit run identifier used in output filenames.",
    )
    parser.add_argument(
        "--layer1-run-id",
        default=None,
        help=(
            "Optional authoritative Layer 1 run identifier. When provided, the regime audit "
            "loads the exact per-date Layer 1.5 manifest for that parent run."
        ),
    )
    parser.add_argument(
        "--as-of-date",
        required=True,
        help="Audit date in YYYY-MM-DD format.",
    )
    parser.add_argument(
        "--tickers",
        required=True,
        help="Comma-delimited ticker sample to audit, for example AAPL,MSFT.",
    )
    parser.add_argument(
        "--benchmark-ticker",
        default="SPY",
        help="Benchmark ticker used for market cross-asset features.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_AUDIT_OUTPUT_DIR,
        help="Directory where the JSON report and text summary are written.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Run the Layer 1 audit from the command line."""
    args = parse_args(argv)
    tickers = tuple(item.strip() for item in str(args.tickers).split(",") if item.strip())
    run_id = args.run_id or f"layer1-audit-{args.as_of_date}"
    report = audit_layer1_features(
        run_id=run_id,
        layer1_run_id=(
            str(args.layer1_run_id).strip() if args.layer1_run_id is not None else None
        ),
        as_of_date=str(args.as_of_date),
        tickers=tickers,
        benchmark_ticker=str(args.benchmark_ticker),
        writer=R2Writer(),
    )
    output_paths = write_audit_report(report, output_dir=args.output_dir)
    logger.info("Layer 1 feature audit report written: {}", output_paths.json_path)
    logger.info("Layer 1 feature audit summary written: {}", output_paths.summary_path)
    logger.info("\n{}", render_audit_summary(report))
    return 1 if report.summary.get("fail", 0) > 0 else 0


if __name__ == "__main__":
    raise SystemExit(main())
