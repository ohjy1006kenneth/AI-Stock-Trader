"""Layer 1 feature correctness audit helpers."""
from __future__ import annotations

import csv
import importlib
import io
import json
import math
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal, Protocol

from core.contracts.schemas import FeatureRecord, PipelineManifestRecord, RunStatus
from core.features.catalog import (
    FeatureRule,
    feature_catalog,
    to_float_or_none,
    validate_feature_value,
)
from core.features.context_features import (
    CONTEXT_FEATURE_COLUMNS,
    compute_context_features,
    context_features_to_records,
)
from core.features.io import parquet_bytes_to_feature_records
from core.features.loaders import load_fundamentals_frame, load_macro_frame, load_ohlcv_frame
from core.features.macro_features import compute_macro_features
from core.features.market_features import (
    MARKET_FEATURE_COLUMNS,
    compute_market_features,
    market_features_to_records,
)
from core.features.news_preprocessing import (
    news_sentiment_frame_to_records,
    preprocess_news_articles,
)
from core.features.regime_detection import (
    HMM_REGIME_COLUMNS,
    HMM_REGIME_FEATURE_COLUMNS,
    regime_features_to_records,
    validate_hmm_regime_probabilities,
)
from core.features.sector_features import (
    SECTOR_FEATURE_COLUMNS,
    compute_sector_features,
    load_sector_etf_config,
    sector_features_to_records,
)
from core.features.sentiment_features import (
    SENTIMENT_FEATURE_COLUMNS,
    load_source_credibility_config,
    sentiment_feature_records_from_scored_news,
)
from core.features.text_topics import TOPIC_FEATURE_COLUMNS, topic_labels_to_feature_records
from services.r2.paths import (
    layer1_ticker_history_path,
    pipeline_manifest_path,
    raw_news_path,
    raw_universe_path,
)
from services.r2.writer import R2Writer

AuditStatus = Literal["pass", "warn", "fail"]
DEFAULT_AUDIT_OUTPUT_DIR = Path("artifacts/reports/diagnostics")
FLOAT_REL_TOL = 1e-7
FLOAT_ABS_TOL = 1e-9
_SENTIMENT_BUCKET_TIMEZONE = "America/New_York"


class AuditReader(Protocol):
    """Object-store operations required by the Layer 1 audit harness."""

    def exists(self, key: str) -> bool:
        """Return True when `key` exists."""

    def get_object(self, key: str) -> bytes:
        """Return object bytes stored at `key`."""

    def list_keys(self, prefix: str) -> list[str]:
        """List keys beneath `prefix`."""


@dataclass(frozen=True)
class AuditFinding:
    """One operator-facing audit result."""

    status: AuditStatus
    category: str
    subject: str
    message: str
    details: dict[str, object] = field(default_factory=dict)

    def to_dict(self) -> dict[str, object]:
        """Return a JSON-serializable representation."""
        return asdict(self)


@dataclass(frozen=True)
class BranchAuditResult:
    """Comparison result for one branch/ticker/date audit."""

    branch: str
    ticker: str
    as_of_date: str
    status: AuditStatus
    compared_features: int
    mismatches: list[str] = field(default_factory=list)
    missing_expected_features: list[str] = field(default_factory=list)
    unexpected_actual_features: list[str] = field(default_factory=list)
    artifact_key: str | None = None

    def to_dict(self) -> dict[str, object]:
        """Return a JSON-serializable representation."""
        return asdict(self)


@dataclass(frozen=True)
class Layer1FeatureAuditReport:
    """Durable Layer 1 audit report for one date/ticker sample."""

    run_id: str
    layer1_run_id: str | None
    as_of_date: str
    benchmark_ticker: str
    tickers: tuple[str, ...]
    generated_at: str
    summary: dict[str, int]
    catalog_summary: dict[str, object]
    branch_results: list[dict[str, object]]
    leakage_checks: list[dict[str, object]]
    history_rows: dict[str, dict[str, object]]
    findings: list[dict[str, object]]

    def to_dict(self) -> dict[str, object]:
        """Return a JSON-serializable representation."""
        return asdict(self)


@dataclass(frozen=True)
class AuditOutputPaths:
    """Filesystem targets written for one completed audit."""

    json_path: Path
    summary_path: Path


def audit_layer1_features(
    *,
    run_id: str,
    layer1_run_id: str | None = None,
    as_of_date: str,
    tickers: Sequence[str],
    benchmark_ticker: str = "SPY",
    writer: AuditReader | None = None,
) -> Layer1FeatureAuditReport:
    """Audit stored Layer 1 histories against deterministic recomputation."""
    _validate_iso_date(as_of_date, label="as_of_date")
    normalized_tickers = _normalize_tickers(tickers)
    if not normalized_tickers:
        raise ValueError("tickers must contain at least one non-empty ticker")

    active_writer = writer or R2Writer()
    findings: list[AuditFinding] = []
    branch_results: list[BranchAuditResult] = []
    leakage_checks: list[AuditFinding] = []
    catalog = feature_catalog()
    history_rows = _load_history_rows(
        writer=active_writer,
        as_of_date=as_of_date,
        tickers=normalized_tickers,
        findings=findings,
    )

    universe_scope = _load_universe_scope(active_writer, as_of_date, findings)
    news_artifact = _load_daily_branch_manifest(
        writer=active_writer,
        stage="layer1_news_preprocessing",
        as_of_date=as_of_date,
    )
    topic_artifact = _load_daily_branch_manifest(
        writer=active_writer,
        stage="layer1_text_topics",
        as_of_date=as_of_date,
    )
    sentiment_artifact = _load_daily_branch_manifest(
        writer=active_writer,
        stage="layer1_finbert_sentiment",
        as_of_date=as_of_date,
    )
    regime_artifact = _load_regime_manifest(writer=active_writer, as_of_date=as_of_date)
    if layer1_run_id is not None:
        regime_artifact = _load_regime_manifest(
            writer=active_writer,
            as_of_date=as_of_date,
            layer1_run_id=layer1_run_id,
        )
    else:
        regime_artifact = _load_regime_manifest(writer=active_writer, as_of_date=as_of_date)

    catalog_summary = _validate_catalog(
        history_rows=history_rows,
        catalog=catalog,
        findings=findings,
    )

    benchmark_bars = load_ohlcv_frame(  # type: ignore[arg-type]
        benchmark_ticker,
        writer=active_writer,
    )
    macro_frame = load_macro_frame(writer=active_writer)  # type: ignore[arg-type]
    sector_scope = tuple(sorted(set(universe_scope) if universe_scope else set(normalized_tickers)))
    sector_ohlcv_by_ticker: dict[str, object] = {}
    sector_fundamentals_by_ticker: dict[str, object] = {}
    for ticker in sector_scope:
        try:
            sector_ohlcv_by_ticker[ticker] = load_ohlcv_frame(ticker, writer=active_writer)  # type: ignore[arg-type]
        except FileNotFoundError:
            findings.append(
                AuditFinding(
                    status="warn",
                    category="layer0",
                    subject=f"{ticker} price archive",
                    message="Sector audit skipped this ticker because the OHLCV archive is missing.",
                )
            )
            continue
        try:
            sector_fundamentals_by_ticker[ticker] = load_fundamentals_frame(  # type: ignore[arg-type]
                ticker,
                writer=active_writer,
            )
        except FileNotFoundError:
            sector_fundamentals_by_ticker[ticker] = _empty_fundamentals_frame()
    sector_config = load_sector_etf_config()
    sector_expected = {
        ticker: (
            record.features
            if (
                record := _record_for_date(
                    sector_features_to_records(frame),
                    as_of_date,
                )
            )
            is not None
            else {}
        )
        for ticker, frame in compute_sector_features(
            ohlcv_by_ticker=sector_ohlcv_by_ticker,
            fundamentals_by_ticker=sector_fundamentals_by_ticker,
            target_dates_by_ticker={ticker: (as_of_date,) for ticker in sector_ohlcv_by_ticker},
            sector_price_frames=_load_sector_price_frames(
                writer=active_writer,
                sector_config=sector_config,
                findings=findings,
            ),
            sector_config=sector_config,
        ).items()
    }

    _audit_news_preprocessing(
        writer=active_writer,
        as_of_date=as_of_date,
        tickers=normalized_tickers,
        allowed_universe=universe_scope,
        artifact=news_artifact,
        findings=findings,
        leakage_checks=leakage_checks,
    )
    topic_expected = _audit_topic_branch(
        writer=active_writer,
        artifact=topic_artifact,
        as_of_date=as_of_date,
        tickers=normalized_tickers,
        findings=findings,
    )
    sentiment_expected = _audit_sentiment_branch(
        writer=active_writer,
        artifact=sentiment_artifact,
        as_of_date=as_of_date,
        tickers=normalized_tickers,
        findings=findings,
        leakage_checks=leakage_checks,
    )
    regime_expected = _audit_regime_branch(
        writer=active_writer,
        artifact=regime_artifact,
        as_of_date=as_of_date,
        tickers=normalized_tickers,
        findings=findings,
        leakage_checks=leakage_checks,
    )

    for ticker in normalized_tickers:
        history = history_rows.get(ticker)
        if history is None:
            continue
        ohlcv = load_ohlcv_frame(ticker, writer=active_writer)  # type: ignore[arg-type]
        try:
            fundamentals = load_fundamentals_frame(  # type: ignore[arg-type]
                ticker,
                writer=active_writer,
            )
        except FileNotFoundError:
            fundamentals = _empty_fundamentals_frame()
            findings.append(
                AuditFinding(
                    status="warn",
                    category="layer0",
                    subject=f"{ticker} fundamentals archive",
                    message="Fundamentals archive missing; audit used an empty archive.",
                )
            )

        market_record = _record_for_date(
            market_features_to_records(
                compute_market_features(ohlcv, ticker, benchmark_bars=benchmark_bars)
            ),
            as_of_date,
        )
        context_record = _record_for_date(
            context_features_to_records(
                compute_context_features(
                    fundamentals=fundamentals,
                    ohlcv=ohlcv,
                    macro=macro_frame,
                    ticker=ticker,
                    macro_features=compute_macro_features(macro_frame, ohlcv["date"].tolist()),
                    target_dates=(as_of_date,),
                )
            ),
            as_of_date,
        )

        branch_results.append(
            _compare_branch(
                branch="market",
                ticker=ticker,
                as_of_date=as_of_date,
                actual=history.features,
                expected=market_record.features if market_record is not None else {},
                feature_names=MARKET_FEATURE_COLUMNS,
                artifact_key=None,
                findings=findings,
            )
        )
        branch_results.append(
            _compare_branch(
                branch="context",
                ticker=ticker,
                as_of_date=as_of_date,
                actual=history.features,
                expected=context_record.features if context_record is not None else {},
                feature_names=CONTEXT_FEATURE_COLUMNS,
                artifact_key=None,
                findings=findings,
            )
        )
        branch_results.append(
            _compare_branch(
                branch="sector",
                ticker=ticker,
                as_of_date=as_of_date,
                actual=history.features,
                expected=sector_expected.get(ticker, {}),
                feature_names=SECTOR_FEATURE_COLUMNS,
                artifact_key=None,
                findings=findings,
            )
        )
        branch_results.append(
            _compare_branch(
                branch="topics",
                ticker=ticker,
                as_of_date=as_of_date,
                actual=history.features,
                expected=topic_expected.get(ticker, {}),
                feature_names=TOPIC_FEATURE_COLUMNS,
                artifact_key=topic_artifact.output_path if topic_artifact is not None else None,
                findings=findings,
            )
        )
        branch_results.append(
            _compare_branch(
                branch="sentiment",
                ticker=ticker,
                as_of_date=as_of_date,
                actual=history.features,
                expected=sentiment_expected.get(ticker, {}),
                feature_names=SENTIMENT_FEATURE_COLUMNS,
                artifact_key=(
                    _manifest_metadata_str(sentiment_artifact, "sentiment_feature_key")
                    if sentiment_artifact is not None
                    else None
                ),
                findings=findings,
            )
        )
        branch_results.append(
            _compare_branch(
                branch="regime",
                ticker=ticker,
                as_of_date=as_of_date,
                actual=history.features,
                expected=regime_expected.get(ticker, {}),
                feature_names=HMM_REGIME_FEATURE_COLUMNS,
                artifact_key=regime_artifact.output_path if regime_artifact is not None else None,
                findings=findings,
            )
        )
        leakage_checks.extend(
            _fundamentals_leakage_checks(
                ticker=ticker,
                as_of_date=as_of_date,
                fundamentals=fundamentals,
            )
        )
        leakage_checks.extend(
            _macro_leakage_checks(
                as_of_date=as_of_date,
                macro_frame=macro_frame,
            )
        )

    all_findings = findings + leakage_checks
    summary = _summarize_findings(all_findings)
    return Layer1FeatureAuditReport(
        run_id=run_id,
        layer1_run_id=layer1_run_id,
        as_of_date=as_of_date,
        benchmark_ticker=benchmark_ticker,
        tickers=normalized_tickers,
        generated_at=datetime.now(UTC).replace(microsecond=0).isoformat(),
        summary=summary,
        catalog_summary=catalog_summary,
        branch_results=[result.to_dict() for result in branch_results],
        leakage_checks=[item.to_dict() for item in leakage_checks],
        history_rows={
            ticker: {
                "history_key": layer1_ticker_history_path(ticker),
                "feature_count": len(record.features),
            }
            for ticker, record in history_rows.items()
        },
        findings=[item.to_dict() for item in all_findings],
    )
def write_audit_report(
    report: Layer1FeatureAuditReport,
    *,
    output_dir: Path | None = None,
) -> AuditOutputPaths:
    """Write the durable JSON report and a human-readable summary file."""
    target_dir = output_dir or DEFAULT_AUDIT_OUTPUT_DIR
    target_dir.mkdir(parents=True, exist_ok=True)
    json_path = target_dir / audit_report_json_filename(report)
    summary_path = target_dir / audit_report_summary_filename(report)
    json_path.write_text(json.dumps(report.to_dict(), indent=2, sort_keys=True), encoding="utf-8")
    summary_path.write_text(render_audit_summary(report), encoding="utf-8")
    return AuditOutputPaths(json_path=json_path, summary_path=summary_path)


def audit_report_json_filename(report: Layer1FeatureAuditReport) -> str:
    """Return the deterministic JSON filename for one Layer 1 audit."""
    return f"layer1_feature_audit_{report.run_id}_{report.as_of_date}.json"


def audit_report_summary_filename(report: Layer1FeatureAuditReport) -> str:
    """Return the deterministic text-summary filename for one Layer 1 audit."""
    return f"layer1_feature_audit_{report.run_id}_{report.as_of_date}.txt"


def render_audit_summary(report: Layer1FeatureAuditReport) -> str:
    """Render a concise operator summary for one completed audit."""
    lines = [
        "Layer 1 Feature Audit",
        f"Run ID: {report.run_id}",
        (
            f"Layer 1 Run ID: {report.layer1_run_id}"
            if report.layer1_run_id is not None
            else "Layer 1 Run ID: auto-select latest completed branch manifests"
        ),
        f"As-of date: {report.as_of_date}",
        f"Tickers: {', '.join(report.tickers)}",
        f"Benchmark: {report.benchmark_ticker}",
        (
            "Summary: "
            f"PASS={report.summary.get('pass', 0)} "
            f"WARN={report.summary.get('warn', 0)} "
            f"FAIL={report.summary.get('fail', 0)}"
        ),
        "",
        "Catalog:",
        (
            f"  features_checked={report.catalog_summary.get('features_checked', 0)} "
            f"unknown={report.catalog_summary.get('unknown_features', 0)} "
            f"missing_required={report.catalog_summary.get('missing_required', 0)} "
            f"type_or_range_failures={report.catalog_summary.get('value_failures', 0)}"
        ),
        "",
        "Branch Results:",
    ]
    for branch_result in report.branch_results:
        lines.append(
            "  "
            f"{branch_result['branch']}:{branch_result['ticker']} "
            f"{str(branch_result['status']).upper()} "
            f"compared={branch_result['compared_features']} "
            f"mismatches={len(branch_result['mismatches'])}"
        )
    lines.extend(["", "Key Findings:"])
    for finding in report.findings:
        if finding["status"] == "pass":
            continue
        lines.append(
            f"  {str(finding['status']).upper()} "
            f"[{finding['category']}] {finding['subject']}: {finding['message']}"
        )
    if all(item["status"] == "pass" for item in report.findings):
        lines.append("  PASS [audit] No warnings or failures.")
    return "\n".join(lines) + "\n"


def _load_history_rows(
    *,
    writer: AuditReader,
    as_of_date: str,
    tickers: Sequence[str],
    findings: list[AuditFinding],
) -> dict[str, FeatureRecord]:
    rows: dict[str, FeatureRecord] = {}
    for ticker in tickers:
        key = layer1_ticker_history_path(ticker)
        if not writer.exists(key):
            findings.append(
                AuditFinding(
                    status="fail",
                    category="history",
                    subject=ticker,
                    message="Missing per-ticker Layer 1 history file.",
                    details={"history_key": key},
                )
            )
            continue
        records = parquet_bytes_to_feature_records(writer.get_object(key))
        matches = [record for record in records if record.date == as_of_date]
        if len(matches) != 1:
            findings.append(
                AuditFinding(
                    status="fail",
                    category="history",
                    subject=ticker,
                    message="Expected exactly one history row for the audited date.",
                    details={
                        "history_key": key,
                        "matching_rows": len(matches),
                        "as_of_date": as_of_date,
                    },
                )
            )
            continue
        rows[ticker] = matches[0]
        findings.append(
            AuditFinding(
                status="pass",
                category="history",
                subject=ticker,
                message="Loaded Layer 1 history row for the audited date.",
                details={"history_key": key, "feature_count": len(matches[0].features)},
            )
        )
    return rows


def _load_universe_scope(
    writer: AuditReader,
    as_of_date: str,
    findings: list[AuditFinding],
) -> set[str] | None:
    key = raw_universe_path(as_of_date)
    if not writer.exists(key):
        findings.append(
            AuditFinding(
                status="warn",
                category="layer0",
                subject=as_of_date,
                message="Universe mask missing; news preprocessing audit used no ticker filter.",
                details={"universe_key": key},
            )
        )
        return None
    payload = writer.get_object(key).decode("utf-8")
    eligible: set[str] = set()
    for row in csv.DictReader(io.StringIO(payload)):
        ticker = _normalize_ticker(row.get("ticker"))
        if ticker is None:
            continue
        if (
            _truthy(row.get("in_universe"))
            and _truthy(row.get("tradable"), default=True)
            and _truthy(row.get("liquid"), default=True)
            and _truthy(row.get("data_quality_ok"), default=True)
            and not _truthy(row.get("halted"))
        ):
            eligible.add(ticker)
    findings.append(
        AuditFinding(
            status="pass",
            category="layer0",
            subject=as_of_date,
            message="Loaded Layer 0 eligible universe mask for the audited date.",
            details={"universe_key": key, "eligible_ticker_count": len(eligible)},
        )
    )
    return eligible


def _validate_catalog(
    *,
    history_rows: Mapping[str, FeatureRecord],
    catalog: Mapping[str, FeatureRule],
    findings: list[AuditFinding],
) -> dict[str, object]:
    features_checked = 0
    unknown_features = 0
    missing_required = 0
    value_failures = 0

    for ticker, record in sorted(history_rows.items()):
        row_unknown = sorted(set(record.features) - set(catalog))
        row_missing = sorted(
            name for name, rule in catalog.items() if rule.required and name not in record.features
        )
        row_failures: list[str] = []
        for feature_name, feature_value in sorted(record.features.items()):
            rule = catalog.get(feature_name)
            if rule is None:
                continue
            features_checked += 1
            message = _validate_feature_value(feature_name, feature_value, rule)
            if message is not None:
                row_failures.append(message)
        unknown_features += len(row_unknown)
        missing_required += len(row_missing)
        value_failures += len(row_failures)

        if row_unknown:
            findings.append(
                AuditFinding(
                    status="warn",
                    category="catalog",
                    subject=ticker,
                    message="History row contains uncataloged feature names.",
                    details={"unknown_features": row_unknown},
                )
            )
        if row_missing:
            findings.append(
                AuditFinding(
                    status="fail",
                    category="catalog",
                    subject=ticker,
                    message="History row is missing required catalog features.",
                    details={"missing_required_features": row_missing},
                )
            )
        if row_failures:
            findings.append(
                AuditFinding(
                    status="fail",
                    category="catalog",
                    subject=ticker,
                    message="History row violates feature type/range expectations.",
                    details={"violations": row_failures},
                )
            )
        if not row_unknown and not row_missing and not row_failures:
            findings.append(
                AuditFinding(
                    status="pass",
                    category="catalog",
                    subject=ticker,
                    message="Feature catalog validation passed.",
                    details={"feature_count": len(record.features)},
                )
            )

    return {
        "features_checked": features_checked,
        "unknown_features": unknown_features,
        "missing_required": missing_required,
        "value_failures": value_failures,
    }


def _validate_feature_value(
    feature_name: str,
    value: object,
    rule: FeatureRule,
) -> str | None:
    return validate_feature_value(feature_name, value, rule)


def _audit_news_preprocessing(
    *,
    writer: AuditReader,
    as_of_date: str,
    tickers: Sequence[str],
    allowed_universe: set[str] | None,
    artifact: PipelineManifestRecord | None,
    findings: list[AuditFinding],
    leakage_checks: list[AuditFinding],
) -> None:
    key = raw_news_path(as_of_date)
    if not writer.exists(key):
        findings.append(
            AuditFinding(
                status="warn",
                category="news",
                subject=as_of_date,
                message="Raw Layer 0 news archive missing; skipped preprocessing audit.",
                details={"raw_news_key": key},
            )
        )
        return
    raw_articles = _load_json_lines(
        writer.get_object(key),
        findings=findings,
        subject=as_of_date,
    )
    preprocessed = preprocess_news_articles(
        raw_articles,
        as_of_date=as_of_date,
        point_in_time_tickers=allowed_universe,
    )
    leakage_checks.append(
        AuditFinding(
            status="pass",
            category="leakage",
            subject="news timestamps",
            message="Recomputed news preprocessing from Layer 0 raw articles.",
            details={
                "raw_news_key": key,
                "recomputed_sentence_rows": len(preprocessed),
                "tickers_requested": list(tickers),
            },
        )
    )

    if artifact is None:
        findings.append(
            AuditFinding(
                status="warn",
                category="news",
                subject=as_of_date,
                message="No completed news preprocessing manifest found for the audited date.",
            )
        )
        return

    stored_records = news_sentiment_frame_to_records(
        _read_parquet_frame(writer, artifact.output_path or "")
    )
    expected_index = {
        _news_identity(record): record
        for record in preprocessed
        if record.ticker in tickers
    }
    stored_index = {
        _news_identity(record): record
        for record in stored_records
        if record.ticker in tickers
    }
    missing = sorted(set(expected_index) - set(stored_index))
    unexpected = sorted(set(stored_index) - set(expected_index))
    timestamp_mismatches = []
    for identity in sorted(set(expected_index) & set(stored_index)):
        if expected_index[identity].published_at != stored_index[identity].published_at:
            timestamp_mismatches.append(identity)
    if missing or unexpected or timestamp_mismatches:
        findings.append(
            AuditFinding(
                status="fail",
                category="news",
                subject=as_of_date,
                message="Stored news preprocessing artifact does not match raw Layer 0 inputs.",
                details={
                    "artifact_key": artifact.output_path,
                    "missing_rows": missing,
                    "unexpected_rows": unexpected,
                    "timestamp_mismatches": timestamp_mismatches,
                },
            )
        )
    else:
        findings.append(
            AuditFinding(
                status="pass",
                category="news",
                subject=as_of_date,
                message="Stored news preprocessing artifact matches raw Layer 0 inputs.",
                details={"artifact_key": artifact.output_path, "rows_compared": len(stored_index)},
            )
        )
    return None


def _audit_topic_branch(
    *,
    writer: AuditReader,
    artifact: PipelineManifestRecord | None,
    as_of_date: str,
    tickers: Sequence[str],
    findings: list[AuditFinding],
) -> dict[str, dict[str, object]]:
    if artifact is None:
        findings.append(
            AuditFinding(
                status="warn",
                category="topics",
                subject=as_of_date,
                message="No completed text-topic manifest found for the audited date.",
            )
        )
        return {}
    topic_label_key = _manifest_metadata_str(artifact, "topic_label_key")
    if topic_label_key is None:
        findings.append(
            AuditFinding(
                status="warn",
                category="topics",
                subject=artifact.run_id,
                message="Text-topic manifest is missing topic_label_key metadata.",
                details={"artifact_key": artifact.output_path},
            )
        )
        return {}
    records = topic_labels_to_feature_records(_read_parquet_frame(writer, topic_label_key))
    expected = {
        record.ticker: dict(record.features)
        for record in records
        if record.date == as_of_date and record.ticker in tickers
    }
    findings.append(
        AuditFinding(
            status="pass",
            category="topics",
            subject=as_of_date,
            message="Recomputed topic feature rows from stored topic labels.",
            details={"topic_label_key": topic_label_key, "records": len(expected)},
        )
    )
    return expected


def _audit_sentiment_branch(
    *,
    writer: AuditReader,
    artifact: PipelineManifestRecord | None,
    as_of_date: str,
    tickers: Sequence[str],
    findings: list[AuditFinding],
    leakage_checks: list[AuditFinding],
) -> dict[str, dict[str, object]]:
    if artifact is None:
        findings.append(
            AuditFinding(
                status="warn",
                category="sentiment",
                subject=as_of_date,
                message="No completed FinBERT sentiment manifest found for the audited date.",
            )
        )
        return {}
    scored_news_key = _manifest_metadata_str(artifact, "scored_news_key")
    if scored_news_key is None:
        findings.append(
            AuditFinding(
                status="warn",
                category="sentiment",
                subject=artifact.run_id,
                message="FinBERT manifest is missing scored_news_key metadata.",
                details={"artifact_key": artifact.output_path},
            )
        )
        return {}
    scored_news = _read_parquet_frame(writer, scored_news_key)
    records = sentiment_feature_records_from_scored_news(
        scored_news,
        credibility_config=load_source_credibility_config(),
        bucket_timezone=_SENTIMENT_BUCKET_TIMEZONE,
    )
    expected = {
        record.ticker: dict(record.features)
        for record in records
        if record.date == as_of_date and record.ticker in tickers
    }
    findings.append(
        AuditFinding(
            status="pass",
            category="sentiment",
            subject=as_of_date,
            message="Recomputed sentiment feature rows from stored scored-news artifacts.",
            details={"scored_news_key": scored_news_key, "records": len(expected)},
        )
    )
    leakage_checks.append(
        AuditFinding(
            status="pass",
            category="leakage",
            subject="news sentiment bucketing",
            message="Sentiment features were recomputed using published_at bucket dates.",
            details={"scored_news_key": scored_news_key},
        )
    )
    return expected


def _audit_regime_branch(
    *,
    writer: AuditReader,
    artifact: PipelineManifestRecord | None,
    as_of_date: str,
    tickers: Sequence[str],
    findings: list[AuditFinding],
    leakage_checks: list[AuditFinding],
) -> dict[str, dict[str, object]]:
    """Audit one Layer 1.5 regime artifact against the stored ticker histories."""
    if artifact is None:
        findings.append(
            AuditFinding(
                status="warn",
                category="regime",
                subject=as_of_date,
                message="No completed regime manifest found for the audited date.",
            )
        )
        return {}
    frame = _read_parquet_frame(writer, artifact.output_path or "")
    _require_columns(frame, HMM_REGIME_COLUMNS, label="regime output")
    probability_errors = validate_hmm_regime_probabilities(frame)
    if probability_errors:
        findings.append(
            AuditFinding(
                status="fail",
                category="regime",
                subject=artifact.run_id,
                message="Regime artifact probabilities are not internally coherent.",
                details={
                    "artifact_key": artifact.output_path,
                    "errors": probability_errors[:10],
                },
            )
        )
    records = regime_features_to_records(frame)
    expected_record = _record_for_date(records, as_of_date)
    if expected_record is None:
        findings.append(
            AuditFinding(
                status="warn",
                category="regime",
                subject=artifact.run_id,
                message="Regime output does not contain the audited inference date.",
                details={"artifact_key": artifact.output_path, "as_of_date": as_of_date},
            )
        )
        return {}
    regime_row = _frame_row_for_date(frame, as_of_date)
    readiness_errors = _regime_audit_readiness_errors(
        row=regime_row,
        as_of_date=as_of_date,
    )
    if readiness_errors:
        findings.append(
            AuditFinding(
                status="fail",
                category="regime",
                subject=artifact.run_id,
                message="Regime artifact readiness/null-state metadata is inconsistent.",
                details={
                    "artifact_key": artifact.output_path,
                    "errors": readiness_errors,
                },
            )
        )
    train_end_date = _manifest_metadata_str(artifact, "train_end_date")
    if train_end_date is not None and train_end_date >= as_of_date:
        leakage_checks.append(
            AuditFinding(
                status="fail",
                category="leakage",
                subject="regime training window",
                message="Regime manifest train_end_date is not strictly before the audited date.",
                details={"train_end_date": train_end_date, "as_of_date": as_of_date},
            )
        )
    else:
        leakage_checks.append(
            AuditFinding(
                status="pass",
                category="leakage",
                subject="regime training window",
                message="Regime manifest train_end_date is strictly before the audited date.",
                details={"train_end_date": train_end_date, "as_of_date": as_of_date},
            )
        )
    manifest_readiness_errors = _regime_manifest_readiness_errors(
        manifest=artifact,
        row=regime_row,
        as_of_date=as_of_date,
    )
    if manifest_readiness_errors:
        findings.append(
            AuditFinding(
                status="fail",
                category="regime",
                subject=artifact.run_id,
                message="Regime manifest readiness metadata does not match the parquet row.",
                details={
                    "artifact_key": artifact.output_path,
                    "errors": manifest_readiness_errors,
                },
            )
        )
    regime_values = {
        name: expected_record.features.get(name) for name in HMM_REGIME_FEATURE_COLUMNS
    }
    if all(value is None for value in regime_values.values()):
        findings.append(
            AuditFinding(
                status="warn",
                category="regime",
                subject=as_of_date,
                message="Regime artifact uses explicit null placeholder values for the audited date.",
                details={"artifact_key": artifact.output_path, "features": regime_values},
            )
        )
    elif not probability_errors:
        findings.append(
            AuditFinding(
                status="pass",
                category="regime",
                subject=as_of_date,
                message="Loaded coherent broadcast regime features for the audited date.",
                details={"artifact_key": artifact.output_path, "features": regime_values},
            )
        )
    return {ticker: dict(expected_record.features) for ticker in tickers}


def _compare_branch(
    *,
    branch: str,
    ticker: str,
    as_of_date: str,
    actual: Mapping[str, object],
    expected: Mapping[str, object],
    feature_names: Sequence[str],
    artifact_key: str | None,
    findings: list[AuditFinding],
) -> BranchAuditResult:
    mismatches: list[str] = []
    missing_expected: list[str] = []
    unexpected_actual: list[str] = []
    compared_features = 0

    for feature_name in feature_names:
        in_actual = feature_name in actual
        in_expected = feature_name in expected
        if in_actual and not in_expected:
            unexpected_actual.append(feature_name)
            continue
        if in_expected and not in_actual:
            missing_expected.append(feature_name)
            continue
        if not in_expected and not in_actual:
            continue
        compared_features += 1
        if not _values_match(actual.get(feature_name), expected.get(feature_name)):
            mismatches.append(
                f"{feature_name}: actual={actual.get(feature_name)!r} "
                f"expected={expected.get(feature_name)!r}"
            )

    status: AuditStatus
    if mismatches or missing_expected or unexpected_actual:
        status = "fail"
        findings.append(
            AuditFinding(
                status="fail",
                category=branch,
                subject=f"{ticker}/{as_of_date}",
                message="Branch recomputation does not match the stored Layer 1 history row.",
                details={
                    "artifact_key": artifact_key,
                    "mismatches": mismatches,
                    "missing_expected_features": missing_expected,
                    "unexpected_actual_features": unexpected_actual,
                },
            )
        )
    elif compared_features == 0:
        status = "warn"
        findings.append(
            AuditFinding(
                status="warn",
                category=branch,
                subject=f"{ticker}/{as_of_date}",
                message="No branch features were available to compare for this ticker/date.",
                details={"artifact_key": artifact_key},
            )
        )
    else:
        status = "pass"
        findings.append(
            AuditFinding(
                status="pass",
                category=branch,
                subject=f"{ticker}/{as_of_date}",
                message="Stored Layer 1 history row matches deterministic branch recomputation.",
                details={"artifact_key": artifact_key, "compared_features": compared_features},
            )
        )

    return BranchAuditResult(
        branch=branch,
        ticker=ticker,
        as_of_date=as_of_date,
        status=status,
        compared_features=compared_features,
        mismatches=mismatches,
        missing_expected_features=missing_expected,
        unexpected_actual_features=unexpected_actual,
        artifact_key=artifact_key,
    )


def _fundamentals_leakage_checks(
    *,
    ticker: str,
    as_of_date: str,
    fundamentals: Any,
) -> list[AuditFinding]:
    if len(fundamentals) == 0 or "availability_date" not in fundamentals.columns:
        return [
            AuditFinding(
                status="warn",
                category="leakage",
                subject=f"{ticker} fundamentals",
                message="No fundamentals availability rows were present for leakage inspection.",
            )
        ]
    prior_rows = fundamentals[fundamentals["availability_date"] < as_of_date]
    future_rows = fundamentals[fundamentals["availability_date"] >= as_of_date]
    latest_prior = (
        None if len(prior_rows) == 0 else str(prior_rows.iloc[-1]["availability_date"])
    )
    earliest_future = (
        None
        if len(future_rows) == 0
        else str(future_rows.sort_values("availability_date").iloc[0]["availability_date"])
    )
    if len(future_rows) > 0:
        return [
            AuditFinding(
                status="warn",
                category="leakage",
                subject=f"{ticker} fundamentals",
                message=(
                    "Fundamentals archive contains rows on or after the audited date; "
                    "verify the feature path used only prior availability_date values."
                ),
                details={
                    "latest_prior_availability_date": latest_prior,
                    "earliest_non_prior_availability_date": earliest_future,
                    "rows_on_or_after_audit_date": int(len(future_rows)),
                    "as_of_date": as_of_date,
                },
            )
        ]
    return [
        AuditFinding(
            status="pass",
            category="leakage",
            subject=f"{ticker} fundamentals",
            message="Fundamentals leakage guard inspected availability_date boundaries.",
            details={
                "latest_prior_availability_date": latest_prior,
                "earliest_non_prior_availability_date": earliest_future,
                "rows_on_or_after_audit_date": 0,
                "as_of_date": as_of_date,
            },
        )
    ]


def _macro_leakage_checks(
    *,
    as_of_date: str,
    macro_frame: Any,
) -> list[AuditFinding]:
    if len(macro_frame) == 0:
        return [
            AuditFinding(
                status="warn",
                category="leakage",
                subject="macro vintages",
                message="Macro archive is empty; skipped realtime_start leakage inspection.",
            )
        ]
    violating = macro_frame[macro_frame["realtime_start"].astype(str) >= as_of_date]
    if len(violating) > 0:
        return [
            AuditFinding(
                status="warn",
                category="leakage",
                subject="macro vintages",
                message=(
                    "Macro archive contains rows on or after the audited date; "
                    "verify the feature path used only strictly prior realtime_start values."
                ),
                details={
                    "rows_total": int(len(macro_frame)),
                    "rows_on_or_after_audit_date": int(len(violating)),
                    "as_of_date": as_of_date,
                },
            )
        ]
    return [
        AuditFinding(
            status="pass",
            category="leakage",
            subject="macro vintages",
            message="Macro realtime_start values were inspected for strictly-prior availability.",
            details={
                "rows_total": int(len(macro_frame)),
                "rows_on_or_after_audit_date": 0,
                "as_of_date": as_of_date,
            },
        )
    ]


def _load_daily_branch_manifest(
    *,
    writer: AuditReader,
    stage: str,
    as_of_date: str,
) -> PipelineManifestRecord | None:
    selected: PipelineManifestRecord | None = None
    for manifest in _completed_manifests(writer=writer, stage=stage):
        manifest_date = _manifest_metadata_str(manifest, "as_of_date")
        if manifest_date != as_of_date:
            continue
        if selected is None or _manifest_rank(manifest) >= _manifest_rank(selected):
            selected = manifest
    return selected


def _load_regime_manifest(
    *,
    writer: AuditReader,
    as_of_date: str,
    layer1_run_id: str | None = None,
) -> PipelineManifestRecord | None:
    if layer1_run_id is not None:
        manifest_key = pipeline_manifest_path(
            "layer1_5_regime",
            f"{layer1_run_id}-{as_of_date}",
        )
        if not writer.exists(manifest_key):
            return None
        manifest = PipelineManifestRecord.model_validate_json(writer.get_object(manifest_key))
        return manifest if manifest.status is RunStatus.COMPLETED else None
    selected: PipelineManifestRecord | None = None
    for manifest in _completed_manifests(writer=writer, stage="layer1_5_regime"):
        inference_dates = manifest.metadata.get("inference_dates")
        if not isinstance(inference_dates, list) or as_of_date not in inference_dates:
            continue
        if selected is None or _manifest_rank(manifest) >= _manifest_rank(selected):
            selected = manifest
    return selected


def _completed_manifests(
    *,
    writer: AuditReader,
    stage: str,
) -> list[PipelineManifestRecord]:
    prefix = f"artifacts/manifests/{stage}/"
    manifests: list[PipelineManifestRecord] = []
    for key in writer.list_keys(prefix):
        if not key.endswith(".json"):
            continue
        manifest = PipelineManifestRecord.model_validate_json(writer.get_object(key))
        if manifest.status is RunStatus.COMPLETED:
            manifests.append(manifest)
    return manifests


def _manifest_rank(manifest: PipelineManifestRecord) -> datetime:
    return manifest.finished_at or manifest.started_at


def _manifest_metadata_str(
    manifest: PipelineManifestRecord | None,
    field_name: str,
) -> str | None:
    if manifest is None:
        return None
    raw_value = manifest.metadata.get(field_name)
    if raw_value is None:
        return None
    if not isinstance(raw_value, str):
        raise ValueError(f"manifest {manifest.run_id} metadata[{field_name!r}] must be a string")
    return raw_value


def _frame_row_for_date(frame: Any, as_of_date: str) -> Mapping[str, object]:
    """Return the single parquet row for `as_of_date` as a mapping."""
    rows = frame[frame["date"].astype(str) == as_of_date].reset_index(drop=True)
    if len(rows) != 1:
        raise ValueError(f"Expected exactly one regime row for {as_of_date}, found {len(rows)}")
    return rows.iloc[0].to_dict()


def _regime_audit_readiness_errors(
    *,
    row: Mapping[str, object],
    as_of_date: str,
) -> list[str]:
    """Return regime-readiness consistency errors for one parquet row."""
    errors: list[str] = []
    required_for_layer2 = _coerce_bool(row.get("regime_required_for_layer2"))
    readiness_status = _optional_normalized_string(row.get("regime_readiness_status"))
    readiness_reason = _optional_normalized_string(row.get("regime_readiness_reason"))
    probability_sum = to_float_or_none(row.get("regime_probability_sum"))
    feature_values = [
        to_float_or_none(row.get("regime_confidence")),
        to_float_or_none(row.get("regime_prob_bear")),
        to_float_or_none(row.get("regime_prob_sideways")),
        to_float_or_none(row.get("regime_prob_bull")),
    ]
    label = _optional_normalized_string(row.get("regime_label"))
    fully_null = label is None and all(value is None for value in feature_values)
    fully_populated = label is not None and all(value is not None for value in feature_values)
    if not fully_null and not fully_populated:
        errors.append(f"{as_of_date}: regime row must be fully populated or fully null")
    if readiness_status not in {None, "ready", "warning"}:
        errors.append(f"{as_of_date}: invalid regime_readiness_status={readiness_status!r}")
    if readiness_reason is None:
        errors.append(f"{as_of_date}: regime_readiness_reason must be non-empty")
    if required_for_layer2:
        if readiness_status not in {None, "ready"}:
            errors.append(f"{as_of_date}: required regime row must use readiness_status='ready'")
        if not fully_populated:
            errors.append(f"{as_of_date}: required regime row cannot use null placeholders")
        if probability_sum is None:
            errors.append(f"{as_of_date}: required regime row must record regime_probability_sum")
    else:
        if readiness_status not in {None, "warning"}:
            errors.append(
                f"{as_of_date}: warning regime row must use readiness_status='warning'"
            )
        if not fully_null:
            errors.append(f"{as_of_date}: warning regime row must use explicit null placeholders")
        if probability_sum is not None:
            errors.append(f"{as_of_date}: warning regime row must not record probability_sum")
    return errors


def _regime_manifest_readiness_errors(
    *,
    manifest: PipelineManifestRecord,
    row: Mapping[str, object],
    as_of_date: str,
) -> list[str]:
    """Return errors when manifest readiness metadata conflicts with the parquet row."""
    payload = manifest.metadata.get("regime_readiness_by_date")
    if not isinstance(payload, Mapping):
        return []
    date_payload = payload.get(as_of_date)
    if not isinstance(date_payload, Mapping):
        return [f"{as_of_date}: manifest metadata missing regime_readiness_by_date entry"]

    errors: list[str] = []
    expected_status = _optional_normalized_string(date_payload.get("status"))
    expected_reason = _optional_normalized_string(date_payload.get("reason"))
    expected_required = _coerce_bool(date_payload.get("required_for_layer2"))
    expected_missing = sorted(_normalize_str_list(date_payload.get("missing_features")))
    expected_probability_sum = to_float_or_none(date_payload.get("probability_sum"))

    actual_status = _optional_normalized_string(row.get("regime_readiness_status"))
    actual_reason = _optional_normalized_string(row.get("regime_readiness_reason"))
    actual_required = _coerce_bool(row.get("regime_required_for_layer2"))
    actual_missing = sorted(
        item
        for item in str(row.get("regime_missing_features") or "").split(",")
        if item
    )
    actual_probability_sum = to_float_or_none(row.get("regime_probability_sum"))

    if expected_status != actual_status:
        errors.append(
            f"{as_of_date}: manifest status {expected_status!r} != row status {actual_status!r}"
        )
    if expected_reason != actual_reason:
        errors.append(
            f"{as_of_date}: manifest reason {expected_reason!r} != row reason {actual_reason!r}"
        )
    if expected_required != actual_required:
        errors.append(
            f"{as_of_date}: manifest required_for_layer2={expected_required} "
            f"!= row {actual_required}"
        )
    if expected_missing != actual_missing:
        errors.append(
            f"{as_of_date}: manifest missing_features {expected_missing!r} "
            f"!= row {actual_missing!r}"
        )
    if not _float_match(expected_probability_sum, actual_probability_sum):
        errors.append(
            f"{as_of_date}: manifest probability_sum={expected_probability_sum!r} "
            f"!= row {actual_probability_sum!r}"
        )
    return errors


def _optional_normalized_string(value: object) -> str | None:
    """Return a stripped string or None for null-like values."""
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _coerce_bool(value: object) -> bool:
    """Return a boolean for common manifest/row encodings."""
    if isinstance(value, bool):
        return value
    text = _optional_normalized_string(value)
    return text == "True" or text == "true" or text == "1"


def _normalize_str_list(value: object) -> list[str]:
    """Normalize common list-like manifest payloads into strings."""
    if value is None:
        return []
    if isinstance(value, str):
        return [item for item in value.split(",") if item]
    if isinstance(value, Sequence):
        return [str(item) for item in value if str(item)]
    return [str(value)]


def _float_match(left: float | None, right: float | None, *, tolerance: float = 1e-9) -> bool:
    """Return True when two optional floats match within tolerance."""
    if left is None or right is None:
        return left is None and right is None
    return math.isclose(left, right, rel_tol=tolerance, abs_tol=tolerance)


def _record_for_date(
    records: Sequence[FeatureRecord],
    as_of_date: str,
) -> FeatureRecord | None:
    matches = [record for record in records if record.date == as_of_date]
    if not matches:
        return None
    if len(matches) > 1:
        raise ValueError(f"Multiple FeatureRecords found for {as_of_date}")
    return matches[0]


def _load_json_lines(
    payload: bytes,
    *,
    findings: list[AuditFinding] | None = None,
    subject: str = "jsonl payload",
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    malformed_line_numbers: list[int] = []
    non_object_line_numbers: list[int] = []
    for line_number, line in enumerate(payload.decode("utf-8").splitlines(), start=1):
        text = line.strip()
        if not text:
            continue
        try:
            row = json.loads(text)
        except json.JSONDecodeError:
            malformed_line_numbers.append(line_number)
            continue
        if not isinstance(row, dict):
            non_object_line_numbers.append(line_number)
            continue
        rows.append(row)
    if findings is not None and (malformed_line_numbers or non_object_line_numbers):
        findings.append(
            AuditFinding(
                status="warn",
                category="news",
                subject=subject,
                message="Skipped malformed JSON Lines rows while reading the raw news archive.",
                details={
                    "rows_loaded": len(rows),
                    "malformed_line_numbers": malformed_line_numbers,
                    "non_object_line_numbers": non_object_line_numbers,
                },
            )
        )
    return rows


def _news_identity(record: Any) -> str:
    published = record.published_at.isoformat() if getattr(record, "published_at", None) else ""
    return "|".join(
        [
            str(record.date),
            str(record.ticker),
            str(record.article_id or ""),
            str(record.sentence_index if record.sentence_index is not None else ""),
            str(record.text or ""),
            published,
        ]
    )


def _read_parquet_frame(writer: AuditReader, key: str) -> Any:
    if not key:
        raise ValueError("artifact key cannot be empty")
    pd = _require_pandas()
    return pd.read_parquet(io.BytesIO(writer.get_object(key)))


def _require_columns(frame: Any, columns: Sequence[str], *, label: str) -> None:
    missing = sorted(set(columns) - set(frame.columns))
    if missing:
        raise ValueError(f"{label} missing required columns: {missing}")


def _require_pandas() -> Any:
    try:
        import pandas as pd

        importlib.import_module("pyarrow")
    except ModuleNotFoundError as exc:
        raise ModuleNotFoundError(
            "pandas and pyarrow are required to run the Layer 1 feature audit."
        ) from exc
    return pd


def _empty_fundamentals_frame() -> Any:
    pd = _require_pandas()
    return pd.DataFrame(
        columns=[
            "source",
            "ticker",
            "report_date",
            "availability_date",
            "retrieved_at",
            "fiscal_year",
            "fiscal_period",
            "statement",
            "earnings_date",
            "raw_json",
        ]
    )


def _load_sector_price_frames(
    *,
    writer: AuditReader,
    sector_config,
    findings: list[AuditFinding],
) -> dict[str, object]:
    """Return available sector ETF histories for audit recomputation."""
    frames: dict[str, object] = {}
    for etf_ticker in sorted(set(sector_config.sector_to_etf.values())):
        try:
            frames[etf_ticker] = load_ohlcv_frame(etf_ticker, writer=writer)  # type: ignore[arg-type]
        except FileNotFoundError:
            findings.append(
                AuditFinding(
                    status="warn",
                    category="layer0",
                    subject=f"{etf_ticker} sector ETF archive",
                    message="Sector ETF OHLCV archive missing; affected sector features are null.",
                )
            )
    return frames


def _normalize_tickers(tickers: Sequence[str]) -> tuple[str, ...]:
    unique = []
    seen: set[str] = set()
    for ticker in tickers:
        normalized = _normalize_ticker(ticker)
        if normalized is None or normalized in seen:
            continue
        seen.add(normalized)
        unique.append(normalized)
    return tuple(sorted(unique))


def _normalize_ticker(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip().upper()
    return text or None


def _truthy(value: object, *, default: bool = False) -> bool:
    if value is None:
        return default
    return str(value).strip().lower() in {"1", "true", "t", "yes", "y"}


def _to_float(value: object) -> float | None:
    return to_float_or_none(value)


def _values_match(left: object, right: object) -> bool:
    if left is None or right is None:
        return left is None and right is None
    if isinstance(left, str) or isinstance(right, str):
        return left == right
    left_float = _to_float(left)
    right_float = _to_float(right)
    if left_float is None or right_float is None:
        return left == right
    return math.isclose(left_float, right_float, rel_tol=FLOAT_REL_TOL, abs_tol=FLOAT_ABS_TOL)


def _validate_iso_date(value: str, *, label: str) -> None:
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"{label} must be YYYY-MM-DD") from exc
    if parsed.date().isoformat() != value:
        raise ValueError(f"{label} must be YYYY-MM-DD")


def _summarize_findings(findings: Sequence[AuditFinding]) -> dict[str, int]:
    summary = {"pass": 0, "warn": 0, "fail": 0}
    for finding in findings:
        summary[finding.status] += 1
    return summary
