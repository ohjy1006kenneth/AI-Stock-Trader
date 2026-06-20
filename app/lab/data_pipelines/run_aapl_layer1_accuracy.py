"""AAPL-only Layer 1 feature generation and accuracy workflow."""
from __future__ import annotations

import argparse
import os
import shlex
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

from app.lab.data_pipelines.run_daily_layer1 import (  # noqa: E402
    Layer1ValidationError,
    modal_main,
    modal_range_main,
)
from core.features.aapl_accuracy import (  # noqa: E402
    DEFAULT_AAPL_ACCURACY_CONFIG_PATH,
    DEFAULT_AAPL_ACCURACY_OUTPUT_DIR,
    AAPLFeatureAccuracyConfig,
    build_aapl_feature_accuracy_report,
    build_terminal_aapl_feature_accuracy_report,
    load_aapl_feature_accuracy_config,
    write_aapl_feature_accuracy_report,
)
from services.r2.writer import R2Writer  # noqa: E402


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """Parse CLI arguments for the AAPL-only Layer 1 accuracy workflow."""
    parser = argparse.ArgumentParser(
        description="Run or audit the AAPL-only Layer 1 feature accuracy pilot."
    )
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--ticker", default="AAPL")
    parser.add_argument("--from-date", required=True, metavar="YYYY-MM-DD")
    parser.add_argument("--to-date", required=True, metavar="YYYY-MM-DD")
    parser.add_argument("--layer0-run-id", default=None)
    parser.add_argument("--layer1-run-id", default=None)
    parser.add_argument("--benchmark-ticker", default=None)
    parser.add_argument("--config-path", type=Path, default=DEFAULT_AAPL_ACCURACY_CONFIG_PATH)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_AAPL_ACCURACY_OUTPUT_DIR)
    parser.add_argument(
        "--run-layer1",
        action="store_true",
        help="First run Layer 1 generation narrowed to AAPL before writing diagnostics.",
    )
    parser.add_argument(
        "--allow-layer0-manifest-date-range",
        action="store_true",
        help="Allow a completed Layer 0 manifest whose date window contains this pilot window.",
    )
    parser.add_argument("--min-sentence-chars", type=int, default=2)
    parser.add_argument("--hmm-train-start-date", default=None, metavar="YYYY-MM-DD")
    parser.add_argument("--hmm-max-iterations", type=int, default=100)
    parser.add_argument("--hmm-min-training-rows", type=int, default=30)
    args = parser.parse_args(argv)
    if str(args.ticker).strip().upper() != "AAPL":
        parser.error("This workflow is intentionally limited to --ticker AAPL")
    return args


def main(argv: Sequence[str] | None = None) -> int:
    """Run the AAPL-only Layer 1 pilot and write the accuracy report."""
    args = parse_args(argv)
    writer = R2Writer()
    config = load_aapl_feature_accuracy_config(args.config_path)
    benchmark_ticker = (
        str(args.benchmark_ticker).strip().upper()
        if args.benchmark_ticker is not None
        else config.benchmark_ticker
    )
    config = AAPLFeatureAccuracyConfig(
        ticker=config.ticker,
        benchmark_ticker=benchmark_ticker,
        target_horizon_days=config.target_horizon_days,
        quality_thresholds=config.quality_thresholds,
        market_parameter_candidates=config.market_parameter_candidates,
    )
    if args.run_layer1:
        try:
            result = _run_scoped_layer1_on_modal(
                run_id=str(args.run_id).strip(),
                from_date=str(args.from_date).strip(),
                to_date=str(args.to_date).strip(),
                layer0_run_id=(
                    str(args.layer0_run_id).strip()
                    if args.layer0_run_id is not None
                    else str(args.run_id).strip()
                ),
                benchmark_ticker=benchmark_ticker,
                allow_layer0_manifest_date_range=bool(args.allow_layer0_manifest_date_range),
                min_sentence_chars=int(args.min_sentence_chars),
                hmm_train_start_date=(
                    str(args.hmm_train_start_date).strip()
                    if args.hmm_train_start_date is not None
                    else None
                ),
                hmm_max_iterations=int(args.hmm_max_iterations),
                hmm_min_training_rows=int(args.hmm_min_training_rows),
            )
        except Layer1ValidationError as exc:
            logger.error("AAPL Layer 1 pilot validation failed: {}", exc)
            _write_terminal_failure_report(
                args=args,
                config=config,
                writer=writer,
                failure=exc,
            )
            return 1
        except Exception as exc:  # noqa: BLE001
            logger.error("AAPL Layer 1 pilot failed: {}", exc)
            _write_terminal_failure_report(
                args=args,
                config=config,
                writer=writer,
                failure=exc,
            )
            return 1
        logger.info(
            "AAPL Layer 1 pilot generated manifest={} report={}",
            result.get("manifest_key"),
            result.get("validation_report_key"),
        )

    report = build_aapl_feature_accuracy_report(
        run_id=str(args.run_id).strip(),
        from_date=str(args.from_date).strip(),
        to_date=str(args.to_date).strip(),
        layer1_run_id=(
            str(args.layer1_run_id).strip()
            if args.layer1_run_id is not None
            else str(args.run_id).strip()
        ),
        layer0_run_id=(
            str(args.layer0_run_id).strip() if args.layer0_run_id is not None else None
        ),
        config=config,
        writer=writer,
    )
    local_report_path = write_aapl_feature_accuracy_report(
        report,
        output_dir=args.output_dir,
    )
    logger.info("AAPL accuracy report written locally: {}", local_report_path)
    logger.info("AAPL accuracy report written to object store: {}", report.report_key)
    logger.info(
        "Recommendation for #202: {}",
        report.recommendation_for_issue_202,
    )
    return 0 if report.acceptance.get("accepted") is True else 1


def _write_terminal_failure_report(
    *,
    args: argparse.Namespace,
    config: AAPLFeatureAccuracyConfig,
    writer: R2Writer,
    failure: BaseException,
) -> None:
    """Persist a fail-closed AAPL pilot report for escaped Layer 1 failures."""
    report = build_terminal_aapl_feature_accuracy_report(
        run_id=str(args.run_id).strip(),
        from_date=str(args.from_date).strip(),
        to_date=str(args.to_date).strip(),
        layer1_run_id=(
            str(args.layer1_run_id).strip()
            if args.layer1_run_id is not None
            else str(args.run_id).strip()
        ),
        layer0_run_id=(
            str(args.layer0_run_id).strip() if args.layer0_run_id is not None else None
        ),
        config=config,
        writer=writer,
        failure_type=type(failure).__name__,
        failure_message=str(failure),
        rerun_command=_build_rerun_command(args),
    )
    local_report_path = write_aapl_feature_accuracy_report(
        report,
        output_dir=args.output_dir,
    )
    logger.info("AAPL terminal diagnostic report written locally: {}", local_report_path)
    logger.info("AAPL terminal diagnostic report written to object store: {}", report.report_key)
    logger.info("Recommendation for #202: {}", report.recommendation_for_issue_202)


def _build_rerun_command(args: argparse.Namespace) -> str:
    """Return a shell command that reruns the same AAPL-only pilot."""
    command = [
        "./.venv/bin/modal",
        "run",
        "app/lab/data_pipelines/run_aapl_layer1_accuracy.py",
        "--run-id",
        str(args.run_id).strip(),
        "--ticker",
        "AAPL",
        "--from-date",
        str(args.from_date).strip(),
        "--to-date",
        str(args.to_date).strip(),
    ]
    if args.layer0_run_id is not None:
        command.extend(["--layer0-run-id", str(args.layer0_run_id).strip()])
    if args.layer1_run_id is not None:
        command.extend(["--layer1-run-id", str(args.layer1_run_id).strip()])
    if args.benchmark_ticker is not None:
        command.extend(["--benchmark-ticker", str(args.benchmark_ticker).strip()])
    if args.config_path != DEFAULT_AAPL_ACCURACY_CONFIG_PATH:
        command.extend(["--config-path", str(args.config_path)])
    if args.output_dir != DEFAULT_AAPL_ACCURACY_OUTPUT_DIR:
        command.extend(["--output-dir", str(args.output_dir)])
    if args.run_layer1:
        command.append("--run-layer1")
    if args.allow_layer0_manifest_date_range:
        command.append("--allow-layer0-manifest-date-range")
    command.extend(["--min-sentence-chars", str(int(args.min_sentence_chars))])
    if args.hmm_train_start_date is not None:
        command.extend(["--hmm-train-start-date", str(args.hmm_train_start_date).strip()])
    command.extend(["--hmm-max-iterations", str(int(args.hmm_max_iterations))])
    command.extend(["--hmm-min-training-rows", str(int(args.hmm_min_training_rows))])
    return " ".join(shlex.quote(part) for part in command)


def _run_scoped_layer1_on_modal(
    *,
    run_id: str,
    from_date: str,
    to_date: str,
    layer0_run_id: str,
    benchmark_ticker: str,
    allow_layer0_manifest_date_range: bool,
    min_sentence_chars: int,
    hmm_train_start_date: str | None,
    hmm_max_iterations: int,
    hmm_min_training_rows: int,
) -> dict[str, object]:
    """Submit the AAPL-only Layer 1 pilot through Modal instead of local heavy NLP."""
    if from_date == to_date:
        return modal_main(
            run_id=run_id,
            as_of_date=from_date,
            layer0_run_id=layer0_run_id,
            tickers=("AAPL",),
            benchmark_ticker=benchmark_ticker,
            allow_layer0_manifest_date_range=allow_layer0_manifest_date_range,
            min_sentence_chars=min_sentence_chars,
            hmm_train_start_date=hmm_train_start_date,
            hmm_max_iterations=hmm_max_iterations,
            hmm_min_training_rows=hmm_min_training_rows,
        )
    return modal_range_main(
        run_id=run_id,
        from_date=from_date,
        to_date=to_date,
        layer0_run_id=layer0_run_id,
        tickers=("AAPL",),
        benchmark_ticker=benchmark_ticker,
        allow_layer0_manifest_date_range=allow_layer0_manifest_date_range,
        min_sentence_chars=min_sentence_chars,
        hmm_train_start_date=hmm_train_start_date,
        hmm_max_iterations=hmm_max_iterations,
        hmm_min_training_rows=hmm_min_training_rows,
    )


if __name__ == "__main__":
    raise SystemExit(main())
