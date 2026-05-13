"""Operator-facing read-only backend builder for the Layer 1 audit dashboard."""
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

from core.features.dashboard_backend import (  # noqa: E402
    DEFAULT_DASHBOARD_OUTPUT_DIR,
    build_layer1_audit_dashboard_report,
    render_layer1_audit_dashboard_summary,
    write_layer1_audit_dashboard_report,
)
from services.r2.writer import R2Writer  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse CLI arguments for the dashboard backend report."""
    parser = argparse.ArgumentParser(
        description="Build the read-only Layer 1 audit dashboard backend report."
    )
    parser.add_argument(
        "--run-id",
        default=None,
        help="Optional report identifier used in output filenames.",
    )
    parser.add_argument(
        "--from-date",
        required=True,
        help="Inclusive start date in YYYY-MM-DD format.",
    )
    parser.add_argument(
        "--to-date",
        required=True,
        help="Inclusive end date in YYYY-MM-DD format.",
    )
    parser.add_argument(
        "--tickers",
        required=True,
        help="Comma-delimited ticker subset, for example AAPL,MSFT.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_DASHBOARD_OUTPUT_DIR,
        help="Directory where the JSON report and text summary are written.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Build the dashboard backend report from stored Layer 1 histories."""
    args = parse_args(argv)
    tickers = tuple(item.strip() for item in str(args.tickers).split(",") if item.strip())
    run_id = args.run_id or f"layer1-audit-dashboard-{args.from_date}-to-{args.to_date}"
    report = build_layer1_audit_dashboard_report(
        run_id=run_id,
        from_date=str(args.from_date),
        to_date=str(args.to_date),
        tickers=tickers,
        writer=R2Writer(),
    )
    output_paths = write_layer1_audit_dashboard_report(report, output_dir=args.output_dir)
    logger.info("Layer 1 audit dashboard JSON written: {}", output_paths.json_path)
    logger.info("Layer 1 audit dashboard summary written: {}", output_paths.summary_path)
    logger.info("\n{}", render_layer1_audit_dashboard_summary(report))
    if report.summary.get("family_fail_count", 0) > 0:
        return 1
    return 1 if report.summary.get("spot_check_fail_count", 0) > 0 else 0


if __name__ == "__main__":
    raise SystemExit(main())
