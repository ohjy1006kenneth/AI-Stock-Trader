"""AAPL pilot evidence bundle generation for objective and human review gates."""
from __future__ import annotations

import csv
import importlib
import io
import json
import math
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from datetime import date as Date
from pathlib import Path
from typing import Any, Protocol

from core.common.trading_calendar import calendar_dates, skipped_non_trading_dates, trading_dates
from core.contracts.schemas import FeatureRecord, NewsSentimentRecord, PipelineManifestRecord
from core.features.catalog import feature_catalog, validate_feature_value
from core.features.io import parquet_bytes_to_feature_record, parquet_bytes_to_feature_records
from services.r2.paths import (
    layer1_aapl_accuracy_report_path,
    layer1_feature_path,
    layer1_news_preprocessing_path,
    layer1_regime_path,
    layer1_sentiment_feature_path,
    layer1_sentiment_score_path,
    layer1_topic_feature_path,
    pipeline_manifest_path,
    raw_news_path,
    raw_price_path,
    raw_universe_path,
)
from services.r2.writer import R2Writer

DEFAULT_AAPL_EVIDENCE_OUTPUT_DIR = Path("artifacts/reports/diagnostics")
HUMAN_REVIEW_STATUSES = frozenset({"pending", "accepted", "rejected"})


class AAPLEvidenceReader(Protocol):
    """Object-store operations required by the AAPL pilot evidence verifier."""

    def exists(self, key: str) -> bool:
        """Return True when an object key exists."""

    def get_object(self, key: str) -> bytes:
        """Read an object payload by key."""

    def list_keys(self, prefix: str) -> list[str]:
        """List keys under a prefix."""


@dataclass(frozen=True)
class IntegrityGate:
    """One objective machine-integrity gate result."""

    name: str
    passed: bool
    details: dict[str, object] = field(default_factory=dict)

    def to_dict(self) -> dict[str, object]:
        """Return a JSON-serializable gate payload."""
        return asdict(self)


@dataclass(frozen=True)
class HumanReviewRow:
    """One compact row for human semantic inspection of AAPL pilot outputs."""

    date: str
    ticker: str
    review_status: str
    raw_article_id: str | None
    raw_headline: str | None
    raw_snippet: str | None
    raw_source: str | None
    raw_published_at: str | None
    raw_news_key: str
    preprocessed_news_key: str
    finbert_scored_news_key: str
    finbert_positive: float | None
    finbert_negative: float | None
    finbert_neutral: float | None
    finbert_score: float | None
    finbert_relevance: float | None
    topic_feature_key: str
    topic_count: float | None
    topic_sentence_count: float | None
    sentiment_feature_key: str
    sentiment_score: float | None
    sentiment_article_count: float | None
    regime_key: str
    regime_label: str | None
    regime_confidence: float | None
    regime_prob_bear: float | None
    regime_prob_sideways: float | None
    regime_prob_bull: float | None
    feature_key: str
    notes: str

    def to_dict(self) -> dict[str, object]:
        """Return a JSON-serializable review row."""
        return asdict(self)


@dataclass(frozen=True)
class AAPLPilotEvidenceBundle:
    """Complete evidence bundle for the AAPL Layer 1 pilot gate."""

    run_id: str
    layer1_run_id: str
    layer0_run_id: str
    ticker: str
    from_date: str
    to_date: str
    generated_at: str
    machine_integrity_status: str
    human_semantic_review_status: str
    recommendation_for_issue_202: str
    gates: tuple[IntegrityGate, ...]
    artifact_keys: dict[str, object]
    row_counts: dict[str, object]
    null_rates: dict[str, object]
    stale_artifacts: dict[str, list[str]]
    source_provenance: dict[str, object]
    human_review_rows: tuple[HumanReviewRow, ...]

    def to_dict(self) -> dict[str, object]:
        """Return the evidence bundle as a deterministic JSON-compatible mapping."""
        return {
            "run_id": self.run_id,
            "layer1_run_id": self.layer1_run_id,
            "layer0_run_id": self.layer0_run_id,
            "ticker": self.ticker,
            "from_date": self.from_date,
            "to_date": self.to_date,
            "generated_at": self.generated_at,
            "machine_integrity_status": self.machine_integrity_status,
            "human_semantic_review_status": self.human_semantic_review_status,
            "recommendation_for_issue_202": self.recommendation_for_issue_202,
            "gates": [gate.to_dict() for gate in self.gates],
            "artifact_keys": self.artifact_keys,
            "row_counts": self.row_counts,
            "null_rates": self.null_rates,
            "stale_artifacts": self.stale_artifacts,
            "source_provenance": self.source_provenance,
            "human_review_rows": [row.to_dict() for row in self.human_review_rows],
        }


def build_aapl_pilot_evidence_bundle(
    *,
    run_id: str,
    from_date: str,
    to_date: str,
    layer0_run_id: str,
    layer1_run_id: str | None = None,
    ticker: str = "AAPL",
    human_semantic_review_status: str = "pending",
    writer: AAPLEvidenceReader | None = None,
    now: datetime | None = None,
) -> AAPLPilotEvidenceBundle:
    """Build a fail-closed AAPL pilot evidence bundle from stored Layer 0/1 artifacts."""
    _validate_inputs(
        run_id=run_id,
        from_date=from_date,
        to_date=to_date,
        layer0_run_id=layer0_run_id,
        ticker=ticker,
        human_semantic_review_status=human_semantic_review_status,
    )
    active_writer = writer or R2Writer()
    active_layer1_run_id = (layer1_run_id or run_id).strip()
    requested_dates = calendar_dates(from_date, to_date)
    dates = trading_dates(from_date, to_date)
    skipped_dates = skipped_non_trading_dates(from_date, to_date)
    stage_run_ids = {date_text: f"{active_layer1_run_id}-{date_text}" for date_text in dates}

    artifact_keys = _artifact_keys(
        run_id=run_id,
        layer0_run_id=layer0_run_id,
        layer1_run_id=active_layer1_run_id,
        ticker=ticker,
        from_date=from_date,
        to_date=to_date,
        requested_dates=requested_dates,
        dates=dates,
        skipped_dates=skipped_dates,
        stage_run_ids=stage_run_ids,
    )
    loaded = _load_expected_artifacts(
        writer=active_writer,
        ticker=ticker,
        dates=dates,
        artifact_keys=artifact_keys,
    )
    gates = _build_integrity_gates(
        dates=dates,
        ticker=ticker,
        loaded=loaded,
        artifact_keys=artifact_keys,
    )
    machine_status = "pass" if all(gate.passed for gate in gates) else "fail"
    recommendation = _recommend_issue_202(
        machine_integrity_status=machine_status,
        human_semantic_review_status=human_semantic_review_status,
    )
    review_rows = _build_human_review_rows(
        dates=dates,
        ticker=ticker,
        human_semantic_review_status=human_semantic_review_status,
        loaded=loaded,
        artifact_keys=artifact_keys,
    )
    return AAPLPilotEvidenceBundle(
        run_id=run_id.strip(),
        layer1_run_id=active_layer1_run_id,
        layer0_run_id=layer0_run_id.strip(),
        ticker=ticker.strip().upper(),
        from_date=from_date,
        to_date=to_date,
        generated_at=(now or datetime.now(UTC)).replace(microsecond=0).isoformat(),
        machine_integrity_status=machine_status,
        human_semantic_review_status=human_semantic_review_status,
        recommendation_for_issue_202=recommendation,
        gates=tuple(gates),
        artifact_keys=artifact_keys,
        row_counts=_row_counts(loaded, artifact_keys),
        null_rates=_null_rates(loaded),
        stale_artifacts=_stale_artifacts(active_writer, artifact_keys),
        source_provenance=_source_provenance(loaded, artifact_keys),
        human_review_rows=tuple(review_rows),
    )


def render_aapl_pilot_evidence_json(bundle: AAPLPilotEvidenceBundle) -> str:
    """Render the machine-integrity evidence bundle as deterministic JSON."""
    return json.dumps(bundle.to_dict(), indent=2, sort_keys=True)


def render_aapl_pilot_human_review_markdown(bundle: AAPLPilotEvidenceBundle) -> str:
    """Render a compact Markdown review packet for human semantic approval."""
    lines = [
        f"# AAPL Pilot Human Review - {bundle.run_id}",
        "",
        f"- Window: `{bundle.from_date}` to `{bundle.to_date}`",
        f"- Machine integrity: `{bundle.machine_integrity_status}`",
        f"- Human semantic review: `{bundle.human_semantic_review_status}`",
        f"- Recommendation for #202: `{bundle.recommendation_for_issue_202}`",
        "",
        "FinBERT, topic-model, and HMM semantic correctness is a human decision. "
        "Mark the semantic review accepted only after inspecting these rows against "
        "the underlying articles and market context.",
        "",
        "| Date | Headline | Source | FinBERT | Topic | Regime | Keys |",
        "|---|---|---|---|---|---|---|",
    ]
    for row in bundle.human_review_rows:
        sentiment = _format_sentiment(row)
        topic = _format_optional_number(row.topic_count)
        regime = _format_regime(row)
        keys = (
            f"`{row.raw_news_key}`<br>`{row.finbert_scored_news_key}`"
            f"<br>`{row.regime_key}`"
        )
        lines.append(
            "| "
            f"{row.date} | {_markdown_cell(row.raw_headline)} | "
            f"{_markdown_cell(row.raw_source)} | {sentiment} | {topic} | {regime} | {keys} |"
        )
    return "\n".join(lines) + "\n"


def render_aapl_pilot_human_review_csv(bundle: AAPLPilotEvidenceBundle) -> str:
    """Render the human-review rows as CSV text."""
    buffer = io.StringIO()
    fieldnames = list(HumanReviewRow.__dataclass_fields__)
    writer = csv.DictWriter(buffer, fieldnames=fieldnames)
    writer.writeheader()
    for row in bundle.human_review_rows:
        writer.writerow(row.to_dict())
    return buffer.getvalue()


def write_aapl_pilot_evidence_outputs(
    bundle: AAPLPilotEvidenceBundle,
    *,
    json_path: Path,
    markdown_path: Path,
    csv_path: Path,
) -> dict[str, Path]:
    """Write JSON, Markdown, and CSV evidence outputs to local artifact paths."""
    json_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_path.parent.mkdir(parents=True, exist_ok=True)
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(render_aapl_pilot_evidence_json(bundle), encoding="utf-8")
    markdown_path.write_text(
        render_aapl_pilot_human_review_markdown(bundle),
        encoding="utf-8",
    )
    csv_path.write_text(render_aapl_pilot_human_review_csv(bundle), encoding="utf-8")
    return {"json": json_path, "markdown": markdown_path, "csv": csv_path}


def default_aapl_pilot_evidence_paths(
    run_id: str,
    *,
    output_dir: Path = DEFAULT_AAPL_EVIDENCE_OUTPUT_DIR,
) -> dict[str, Path]:
    """Return deterministic local output paths for one AAPL evidence bundle."""
    safe_run_id = run_id.strip()
    return {
        "json": output_dir / f"aapl_pilot_evidence_{safe_run_id}.json",
        "markdown": output_dir / f"aapl_pilot_human_review_{safe_run_id}.md",
        "csv": output_dir / f"aapl_pilot_human_review_rows_{safe_run_id}.csv",
    }


def _validate_inputs(
    *,
    run_id: str,
    from_date: str,
    to_date: str,
    layer0_run_id: str,
    ticker: str,
    human_semantic_review_status: str,
) -> None:
    if not run_id.strip():
        raise ValueError("run_id cannot be empty")
    if not layer0_run_id.strip():
        raise ValueError("layer0_run_id is required for source provenance")
    if ticker.strip().upper() != "AAPL":
        raise ValueError("AAPL evidence workflow is intentionally limited to ticker=AAPL")
    _validate_iso_date(from_date, "from_date")
    _validate_iso_date(to_date, "to_date")
    if from_date > to_date:
        raise ValueError("from_date must be <= to_date")
    if human_semantic_review_status not in HUMAN_REVIEW_STATUSES:
        allowed = ", ".join(sorted(HUMAN_REVIEW_STATUSES))
        raise ValueError(f"human_semantic_review_status must be one of: {allowed}")


def _artifact_keys(
    *,
    run_id: str,
    layer0_run_id: str,
    layer1_run_id: str,
    ticker: str,
    from_date: str,
    to_date: str,
    requested_dates: Sequence[str],
    dates: Sequence[str],
    skipped_dates: Sequence[str],
    stage_run_ids: Mapping[str, str],
) -> dict[str, object]:
    return {
        "accuracy_report": layer1_aapl_accuracy_report_path(run_id, from_date, to_date),
        "requested_calendar_dates": list(requested_dates),
        "expected_trading_dates": list(dates),
        "skipped_non_trading_dates": list(skipped_dates),
        "layer0_manifest": pipeline_manifest_path("layer0", layer0_run_id),
        "layer1_manifest": pipeline_manifest_path("layer1", layer1_run_id),
        "raw_price": raw_price_path(ticker),
        "raw_news": {date_text: raw_news_path(date_text) for date_text in dates},
        "raw_universe": {date_text: raw_universe_path(date_text) for date_text in dates},
        "feature_shards": {
            date_text: layer1_feature_path(date_text, ticker) for date_text in dates
        },
        "preprocessed_news": {
            date_text: layer1_news_preprocessing_path(date_text, stage_run_ids[date_text])
            for date_text in dates
        },
        "topic_features": {
            date_text: layer1_topic_feature_path(date_text, stage_run_ids[date_text])
            for date_text in dates
        },
        "finbert_scored_news": {
            date_text: layer1_sentiment_score_path(date_text, stage_run_ids[date_text])
            for date_text in dates
        },
        "sentiment_features": {
            date_text: layer1_sentiment_feature_path(date_text, stage_run_ids[date_text])
            for date_text in dates
        },
        "regime": {
            date_text: layer1_regime_path(date_text, stage_run_ids[date_text])
            for date_text in dates
        },
        "stage_manifests": {
            date_text: {
                "news_preprocessing": pipeline_manifest_path(
                    "layer1_news_preprocessing", stage_run_ids[date_text]
                ),
                "text_topics": pipeline_manifest_path(
                    "layer1_text_topics", stage_run_ids[date_text]
                ),
                "finbert_sentiment": pipeline_manifest_path(
                    "layer1_finbert_sentiment", stage_run_ids[date_text]
                ),
                "regime": pipeline_manifest_path(
                    "layer1_5_regime", stage_run_ids[date_text]
                ),
            }
            for date_text in dates
        },
    }


def _load_expected_artifacts(
    *,
    writer: AAPLEvidenceReader,
    ticker: str,
    dates: Sequence[str],
    artifact_keys: Mapping[str, object],
) -> dict[str, object]:
    loaded: dict[str, object] = {
        "missing_keys": [],
        "read_errors": [],
        "feature_records": {},
        "raw_news_rows": {},
        "preprocessed_rows": {},
        "topic_records": {},
        "scored_news_rows": {},
        "sentiment_records": {},
        "regime_rows": {},
        "manifests": {},
    }
    for label in ("accuracy_report", "raw_price"):
        _load_bytes_artifact(writer, loaded, label, str(artifact_keys[label]))
    for label in ("layer0_manifest", "layer1_manifest"):
        _load_manifest(writer, loaded, label, str(artifact_keys[label]))

    for date_text in dates:
        _load_raw_news(writer, loaded, date_text, _date_key(artifact_keys, "raw_news", date_text))
        _load_bytes_artifact(
            writer,
            loaded,
            f"raw_universe:{date_text}",
            _date_key(artifact_keys, "raw_universe", date_text),
        )
        _load_feature_record(
            writer,
            loaded,
            date_text,
            ticker,
            _date_key(artifact_keys, "feature_shards", date_text),
        )
        _load_parquet_rows(
            writer,
            loaded,
            "preprocessed_rows",
            date_text,
            _date_key(artifact_keys, "preprocessed_news", date_text),
        )
        _load_feature_records(
            writer,
            loaded,
            "topic_records",
            date_text,
            _date_key(artifact_keys, "topic_features", date_text),
        )
        _load_parquet_rows(
            writer,
            loaded,
            "scored_news_rows",
            date_text,
            _date_key(artifact_keys, "finbert_scored_news", date_text),
            validate_news_sentiment=True,
        )
        _load_feature_records(
            writer,
            loaded,
            "sentiment_records",
            date_text,
            _date_key(artifact_keys, "sentiment_features", date_text),
        )
        _load_parquet_rows(
            writer,
            loaded,
            "regime_rows",
            date_text,
            _date_key(artifact_keys, "regime", date_text),
        )
        for stage, key in _stage_manifest_keys(artifact_keys, date_text).items():
            _load_manifest(writer, loaded, f"{stage}:{date_text}", key)
    return loaded


def _load_bytes_artifact(
    writer: AAPLEvidenceReader,
    loaded: dict[str, object],
    label: str,
    key: str,
) -> None:
    if not writer.exists(key):
        _append_loaded(loaded, "missing_keys", key)
        return
    try:
        loaded[label] = writer.get_object(key)
    except Exception as exc:  # noqa: BLE001
        _append_loaded(loaded, "read_errors", {"key": key, "error": str(exc)})


def _load_manifest(
    writer: AAPLEvidenceReader,
    loaded: dict[str, object],
    label: str,
    key: str,
) -> None:
    if not writer.exists(key):
        _append_loaded(loaded, "missing_keys", key)
        return
    try:
        manifest = PipelineManifestRecord.model_validate_json(writer.get_object(key))
        manifests = loaded["manifests"]
        assert isinstance(manifests, dict)
        manifests[label] = manifest
    except Exception as exc:  # noqa: BLE001
        _append_loaded(loaded, "read_errors", {"key": key, "error": str(exc)})


def _load_feature_record(
    writer: AAPLEvidenceReader,
    loaded: dict[str, object],
    date_text: str,
    ticker: str,
    key: str,
) -> None:
    if not writer.exists(key):
        _append_loaded(loaded, "missing_keys", key)
        return
    try:
        record = parquet_bytes_to_feature_record(writer.get_object(key))
        if record.date != date_text or record.ticker != ticker:
            raise ValueError(f"identity mismatch: expected {date_text}/{ticker}")
        records = loaded["feature_records"]
        assert isinstance(records, dict)
        records[date_text] = record
    except Exception as exc:  # noqa: BLE001
        _append_loaded(loaded, "read_errors", {"key": key, "error": str(exc)})


def _load_feature_records(
    writer: AAPLEvidenceReader,
    loaded: dict[str, object],
    bucket: str,
    date_text: str,
    key: str,
) -> None:
    if not writer.exists(key):
        _append_loaded(loaded, "missing_keys", key)
        return
    try:
        records = parquet_bytes_to_feature_records(writer.get_object(key))
        by_date = loaded[bucket]
        assert isinstance(by_date, dict)
        by_date[date_text] = records
    except Exception as exc:  # noqa: BLE001
        _append_loaded(loaded, "read_errors", {"key": key, "error": str(exc)})


def _load_parquet_rows(
    writer: AAPLEvidenceReader,
    loaded: dict[str, object],
    bucket: str,
    date_text: str,
    key: str,
    *,
    validate_news_sentiment: bool = False,
) -> None:
    if not writer.exists(key):
        _append_loaded(loaded, "missing_keys", key)
        return
    try:
        frame = _read_parquet_frame(writer.get_object(key))
        rows = [_clean_mapping(row) for row in frame.to_dict("records")]
        if validate_news_sentiment:
            rows = [_validate_news_sentiment_row(row) for row in rows]
        by_date = loaded[bucket]
        assert isinstance(by_date, dict)
        by_date[date_text] = rows
    except Exception as exc:  # noqa: BLE001
        _append_loaded(loaded, "read_errors", {"key": key, "error": str(exc)})


def _load_raw_news(
    writer: AAPLEvidenceReader,
    loaded: dict[str, object],
    date_text: str,
    key: str,
) -> None:
    if not writer.exists(key):
        _append_loaded(loaded, "missing_keys", key)
        return
    try:
        rows = []
        for line in writer.get_object(key).decode("utf-8").splitlines():
            if line.strip():
                rows.append(json.loads(line))
        raw_rows = loaded["raw_news_rows"]
        assert isinstance(raw_rows, dict)
        raw_rows[date_text] = rows
    except Exception as exc:  # noqa: BLE001
        _append_loaded(loaded, "read_errors", {"key": key, "error": str(exc)})


def _build_integrity_gates(
    *,
    dates: Sequence[str],
    ticker: str,
    loaded: Mapping[str, object],
    artifact_keys: Mapping[str, object],
) -> list[IntegrityGate]:
    gates = [
        _gate_no_missing_artifacts(loaded),
        _gate_no_read_errors(loaded),
        _gate_manifest_completed(loaded, "layer0_manifest"),
        _gate_manifest_completed(loaded, "layer1_manifest"),
        _gate_accuracy_report_available(loaded),
        _gate_feature_coverage(dates, loaded, artifact_keys),
        _gate_stage_row_coverage(dates, ticker, loaded),
        _gate_catalog_schema(dates, loaded),
        _gate_finite_numeric_values(loaded),
        _gate_probability_sums(dates, loaded),
        _gate_point_in_time_news(dates, loaded),
        _gate_stage_manifest_outputs(dates, loaded, artifact_keys),
    ]
    return gates


def _gate_no_missing_artifacts(loaded: Mapping[str, object]) -> IntegrityGate:
    missing = list(loaded.get("missing_keys", []))
    return IntegrityGate(
        name="expected_artifacts_exist",
        passed=len(missing) == 0,
        details={"missing_keys": missing},
    )


def _gate_no_read_errors(loaded: Mapping[str, object]) -> IntegrityGate:
    errors = list(loaded.get("read_errors", []))
    return IntegrityGate(
        name="artifacts_read_and_validate",
        passed=len(errors) == 0,
        details={"read_errors": errors},
    )


def _gate_manifest_completed(
    loaded: Mapping[str, object],
    manifest_label: str,
) -> IntegrityGate:
    manifest = _manifest(loaded, manifest_label)
    return IntegrityGate(
        name=f"{manifest_label}_completed",
        passed=manifest is not None and str(manifest.status.value) == "completed",
        details={
            "status": None if manifest is None else str(manifest.status.value),
            "stage": None if manifest is None else manifest.stage,
            "output_path": None if manifest is None else manifest.output_path,
        },
    )


def _gate_accuracy_report_available(loaded: Mapping[str, object]) -> IntegrityGate:
    payload = loaded.get("accuracy_report")
    parsed: dict[str, object] | None = None
    if isinstance(payload, bytes):
        try:
            parsed = json.loads(payload.decode("utf-8"))
        except json.JSONDecodeError:
            parsed = None
    terminal = (
        isinstance(parsed, dict)
        and isinstance(parsed.get("input_evidence"), dict)
        and isinstance(parsed["input_evidence"].get("terminal_diagnostic"), dict)
    )
    return IntegrityGate(
        name="aapl_accuracy_report_available",
        passed=isinstance(parsed, dict) and not terminal,
        details={
            "available": isinstance(parsed, dict),
            "terminal_diagnostic": terminal,
            "recommendation": None if parsed is None else parsed.get("recommendation_for_issue_202"),
        },
    )


def _gate_feature_coverage(
    dates: Sequence[str],
    loaded: Mapping[str, object],
    artifact_keys: Mapping[str, object],
) -> IntegrityGate:
    records = _records_by_date(loaded, "feature_records")
    present_dates = sorted(records)
    return IntegrityGate(
        name="date_first_feature_coverage",
        passed=present_dates == list(dates),
        details={
            "expected_trading_dates": list(dates),
            "present_dates": present_dates,
            "skipped_non_trading_dates": list(
                _sequence_artifact_value(artifact_keys, "skipped_non_trading_dates")
            ),
        },
    )


def _gate_stage_row_coverage(
    dates: Sequence[str],
    ticker: str,
    loaded: Mapping[str, object],
) -> IntegrityGate:
    missing: list[dict[str, object]] = []
    for bucket in (
        "raw_news_rows",
        "preprocessed_rows",
        "topic_records",
        "scored_news_rows",
        "sentiment_records",
        "regime_rows",
    ):
        rows_by_date = _records_by_date(loaded, bucket)
        for date_text in dates:
            rows = rows_by_date.get(date_text, [])
            if bucket == "regime_rows":
                has_rows = bool(rows)
            elif bucket == "raw_news_rows":
                has_rows = any(
                    isinstance(row, Mapping) and _row_contains_ticker(row, ticker)
                    for row in rows
                )
            elif bucket in {"topic_records", "sentiment_records"}:
                has_rows = any(isinstance(row, FeatureRecord) and row.ticker == ticker for row in rows)
            else:
                has_rows = any(str(_row_value(row, "ticker")).upper() == ticker for row in rows)
            if not has_rows:
                missing.append({"bucket": bucket, "date": date_text})
    return IntegrityGate(
        name="stage_row_coverage",
        passed=len(missing) == 0,
        details={"missing_or_empty": missing},
    )


def _gate_catalog_schema(
    dates: Sequence[str],
    loaded: Mapping[str, object],
) -> IntegrityGate:
    catalog = feature_catalog()
    failures: list[dict[str, object]] = []
    for date_text in dates:
        record = _records_by_date(loaded, "feature_records").get(date_text)
        if not isinstance(record, FeatureRecord):
            continue
        for feature_name, rule in catalog.items():
            if not rule.required:
                continue
            message = validate_feature_value(
                feature_name,
                record.features.get(feature_name),
                rule,
            )
            if message is not None:
                failures.append(
                    {"date": date_text, "feature": feature_name, "message": message}
                )
    return IntegrityGate(
        name="feature_schema_and_catalog",
        passed=len(failures) == 0,
        details={"failures": failures[:100], "failure_count": len(failures)},
    )


def _gate_finite_numeric_values(loaded: Mapping[str, object]) -> IntegrityGate:
    failures: list[dict[str, object]] = []
    for date_text, record in _records_by_date(loaded, "feature_records").items():
        if not isinstance(record, FeatureRecord):
            continue
        for name, value in record.features.items():
            if isinstance(value, (int, float)) and not _is_finite(value):
                failures.append({"date": date_text, "field": name, "value": str(value)})
    for bucket in ("scored_news_rows", "regime_rows"):
        for date_text, rows in _records_by_date(loaded, bucket).items():
            for row in rows if isinstance(rows, list) else []:
                if not isinstance(row, Mapping):
                    continue
                for name, value in row.items():
                    if isinstance(value, (int, float)) and not _is_finite(value):
                        failures.append(
                            {"date": date_text, "bucket": bucket, "field": name}
                        )
    return IntegrityGate(
        name="finite_numeric_values",
        passed=len(failures) == 0,
        details={"failures": failures[:100], "failure_count": len(failures)},
    )


def _gate_probability_sums(
    dates: Sequence[str],
    loaded: Mapping[str, object],
) -> IntegrityGate:
    failures: list[dict[str, object]] = []
    for date_text in dates:
        for row in _records_by_date(loaded, "scored_news_rows").get(date_text, []):
            total = _sum_fields(
                row,
                ("sentiment_positive", "sentiment_negative", "sentiment_neutral"),
            )
            if total is not None and abs(total - 1.0) > 1e-4:
                failures.append({"date": date_text, "type": "finbert", "sum": total})
        for row in _records_by_date(loaded, "regime_rows").get(date_text, []):
            total = _sum_fields(
                row,
                ("regime_prob_bear", "regime_prob_sideways", "regime_prob_bull"),
            )
            if total is not None and abs(total - 1.0) > 1e-4:
                failures.append({"date": date_text, "type": "regime", "sum": total})
            label = str(_row_value(row, "regime_label") or "")
            confidence = _to_finite_float(_row_value(row, "regime_confidence"))
            label_probability = _regime_label_probability(row, label)
            if confidence is not None and label_probability is not None:
                if abs(confidence - label_probability) > 1e-4:
                    failures.append(
                        {
                            "date": date_text,
                            "type": "regime_confidence",
                            "confidence": confidence,
                            "label_probability": label_probability,
                        }
                    )
    return IntegrityGate(
        name="probability_sums",
        passed=len(failures) == 0,
        details={"failures": failures},
    )


def _gate_point_in_time_news(
    dates: Sequence[str],
    loaded: Mapping[str, object],
) -> IntegrityGate:
    failures: list[dict[str, object]] = []
    for date_text in dates:
        cutoff = f"{date_text}T23:59:59"
        for row in _records_by_date(loaded, "raw_news_rows").get(date_text, []):
            timestamp = _news_timestamp(row)
            if timestamp is not None and timestamp[:19] > cutoff:
                failures.append(
                    {"date": date_text, "timestamp": timestamp, "cutoff": cutoff}
                )
    return IntegrityGate(
        name="point_in_time_news_timestamps",
        passed=len(failures) == 0,
        details={"failures": failures},
    )


def _gate_stage_manifest_outputs(
    dates: Sequence[str],
    loaded: Mapping[str, object],
    artifact_keys: Mapping[str, object],
) -> IntegrityGate:
    failures: list[dict[str, object]] = []
    stage_to_artifact = {
        "news_preprocessing": "preprocessed_news",
        "text_topics": "topic_features",
        "finbert_sentiment": "sentiment_features",
        "regime": "regime",
    }
    for date_text in dates:
        for stage, artifact_name in stage_to_artifact.items():
            manifest = _manifest(loaded, f"{stage}:{date_text}")
            expected_key = _date_key(artifact_keys, artifact_name, date_text)
            if manifest is None:
                continue
            if str(manifest.status.value) != "completed":
                failures.append(
                    {"date": date_text, "stage": stage, "status": manifest.status.value}
                )
            if manifest.output_path != expected_key:
                failures.append(
                    {
                        "date": date_text,
                        "stage": stage,
                        "expected_output": expected_key,
                        "manifest_output": manifest.output_path,
                    }
                )
    return IntegrityGate(
        name="stage_manifest_outputs",
        passed=len(failures) == 0,
        details={"failures": failures},
    )


def _build_human_review_rows(
    *,
    dates: Sequence[str],
    ticker: str,
    human_semantic_review_status: str,
    loaded: Mapping[str, object],
    artifact_keys: Mapping[str, object],
) -> list[HumanReviewRow]:
    review_rows: list[HumanReviewRow] = []
    for date_text in dates:
        raw_rows = [
            row
            for row in _records_by_date(loaded, "raw_news_rows").get(date_text, [])
            if _row_contains_ticker(row, ticker)
        ]
        scored_rows = [
            row
            for row in _records_by_date(loaded, "scored_news_rows").get(date_text, [])
            if str(_row_value(row, "ticker")).upper() == ticker
        ]
        topic_record = _first_feature_record(loaded, "topic_records", date_text, ticker)
        sentiment_record = _first_feature_record(
            loaded, "sentiment_records", date_text, ticker
        )
        feature_record = _records_by_date(loaded, "feature_records").get(date_text)
        regime_row = _first_mapping_row(loaded, "regime_rows", date_text)
        rows_for_date = scored_rows or [None]
        for scored_row in rows_for_date:
            raw_row = _match_raw_row(raw_rows, scored_row)
            review_rows.append(
                _human_review_row(
                    date_text=date_text,
                    ticker=ticker,
                    human_semantic_review_status=human_semantic_review_status,
                    raw_row=raw_row,
                    scored_row=scored_row,
                    topic_record=topic_record,
                    sentiment_record=sentiment_record,
                    feature_record=feature_record,
                    regime_row=regime_row,
                    artifact_keys=artifact_keys,
                )
            )
    return review_rows


def _human_review_row(
    *,
    date_text: str,
    ticker: str,
    human_semantic_review_status: str,
    raw_row: Mapping[str, object] | None,
    scored_row: Mapping[str, object] | None,
    topic_record: FeatureRecord | None,
    sentiment_record: FeatureRecord | None,
    feature_record: object,
    regime_row: Mapping[str, object] | None,
    artifact_keys: Mapping[str, object],
) -> HumanReviewRow:
    topic_features = {} if topic_record is None else topic_record.features
    sentiment_features = {} if sentiment_record is None else sentiment_record.features
    return HumanReviewRow(
        date=date_text,
        ticker=ticker,
        review_status=human_semantic_review_status,
        raw_article_id=_string_or_none(_row_value(raw_row, "id")),
        raw_headline=_string_or_none(_row_value(raw_row, "headline")),
        raw_snippet=_snippet(_row_value(raw_row, "summary") or _row_value(raw_row, "text")),
        raw_source=_string_or_none(_row_value(raw_row, "source")),
        raw_published_at=_string_or_none(_news_timestamp(raw_row)),
        raw_news_key=_date_key(artifact_keys, "raw_news", date_text),
        preprocessed_news_key=_date_key(artifact_keys, "preprocessed_news", date_text),
        finbert_scored_news_key=_date_key(artifact_keys, "finbert_scored_news", date_text),
        finbert_positive=_to_finite_float(_row_value(scored_row, "sentiment_positive")),
        finbert_negative=_to_finite_float(_row_value(scored_row, "sentiment_negative")),
        finbert_neutral=_to_finite_float(_row_value(scored_row, "sentiment_neutral")),
        finbert_score=_to_finite_float(_row_value(scored_row, "sentiment_score")),
        finbert_relevance=_to_finite_float(_row_value(scored_row, "relevance_score")),
        topic_feature_key=_date_key(artifact_keys, "topic_features", date_text),
        topic_count=_to_finite_float(topic_features.get("nlp_topic_count")),
        topic_sentence_count=_to_finite_float(topic_features.get("nlp_sentence_count")),
        sentiment_feature_key=_date_key(artifact_keys, "sentiment_features", date_text),
        sentiment_score=_to_finite_float(sentiment_features.get("nlp_sentiment_score")),
        sentiment_article_count=_to_finite_float(sentiment_features.get("nlp_article_count")),
        regime_key=_date_key(artifact_keys, "regime", date_text),
        regime_label=_string_or_none(_row_value(regime_row, "regime_label")),
        regime_confidence=_to_finite_float(_row_value(regime_row, "regime_confidence")),
        regime_prob_bear=_to_finite_float(_row_value(regime_row, "regime_prob_bear")),
        regime_prob_sideways=_to_finite_float(_row_value(regime_row, "regime_prob_sideways")),
        regime_prob_bull=_to_finite_float(_row_value(regime_row, "regime_prob_bull")),
        feature_key=_date_key(artifact_keys, "feature_shards", date_text),
        notes=_review_notes(raw_row, scored_row, feature_record),
    )


def _row_counts(
    loaded: Mapping[str, object],
    artifact_keys: Mapping[str, object],
) -> dict[str, object]:
    counts: dict[str, object] = {}
    for bucket in (
        "raw_news_rows",
        "preprocessed_rows",
        "topic_records",
        "scored_news_rows",
        "sentiment_records",
        "regime_rows",
    ):
        counts[bucket] = {
            date_text: len(rows) if isinstance(rows, list) else 1
            for date_text, rows in _records_by_date(loaded, bucket).items()
        }
    counts["feature_records"] = len(_records_by_date(loaded, "feature_records"))
    counts["expected_trading_dates"] = len(
        _sequence_artifact_value(artifact_keys, "expected_trading_dates")
    )
    counts["skipped_non_trading_dates"] = len(
        _sequence_artifact_value(artifact_keys, "skipped_non_trading_dates")
    )
    return counts


def _null_rates(loaded: Mapping[str, object]) -> dict[str, object]:
    feature_nulls = {}
    for date_text, record in _records_by_date(loaded, "feature_records").items():
        if isinstance(record, FeatureRecord):
            values = list(record.features.values())
            feature_nulls[date_text] = _null_rate(values)
    scored_nulls = {
        date_text: _mapping_null_rate(rows)
        for date_text, rows in _records_by_date(loaded, "scored_news_rows").items()
        if isinstance(rows, list)
    }
    return {"feature_records": feature_nulls, "scored_news_rows": scored_nulls}


def _stale_artifacts(
    writer: AAPLEvidenceReader,
    artifact_keys: Mapping[str, object],
) -> dict[str, list[str]]:
    stale: dict[str, list[str]] = {}
    for family in (
        "preprocessed_news",
        "topic_features",
        "finbert_scored_news",
        "sentiment_features",
        "regime",
    ):
        keys_by_date = artifact_keys[family]
        assert isinstance(keys_by_date, Mapping)
        for date_text, expected_key in keys_by_date.items():
            prefix = str(expected_key).rsplit("/", 1)[0] + "/"
            siblings = [key for key in writer.list_keys(prefix) if key != expected_key]
            if siblings:
                stale[f"{family}:{date_text}"] = siblings
    return stale


def _source_provenance(
    loaded: Mapping[str, object],
    artifact_keys: Mapping[str, object],
) -> dict[str, object]:
    manifests = loaded.get("manifests")
    manifest_payload = {}
    if isinstance(manifests, Mapping):
        for label, manifest in manifests.items():
            if isinstance(manifest, PipelineManifestRecord):
                manifest_payload[label] = {
                    "stage": manifest.stage,
                    "status": manifest.status.value,
                    "output_path": manifest.output_path,
                    "metadata": manifest.metadata,
                }
    return {
        "manifests": manifest_payload,
        "requested_calendar_dates": artifact_keys["requested_calendar_dates"],
        "expected_trading_dates": artifact_keys["expected_trading_dates"],
        "skipped_non_trading_dates": artifact_keys["skipped_non_trading_dates"],
        "raw_price_key": artifact_keys["raw_price"],
        "raw_news_keys": artifact_keys["raw_news"],
        "raw_universe_keys": artifact_keys["raw_universe"],
    }


def _recommend_issue_202(
    *,
    machine_integrity_status: str,
    human_semantic_review_status: str,
) -> str:
    if machine_integrity_status != "pass":
        return "do_not_proceed"
    if human_semantic_review_status == "accepted":
        return "proceed"
    if human_semantic_review_status == "rejected":
        return "do_not_proceed"
    return "needs_human_review"


def _validate_news_sentiment_row(row: Mapping[str, object]) -> dict[str, object]:
    cleaned = _clean_mapping(row)
    return NewsSentimentRecord(**cleaned).model_dump(mode="json")


def _manifest(
    loaded: Mapping[str, object],
    label: str,
) -> PipelineManifestRecord | None:
    manifests = loaded.get("manifests")
    if not isinstance(manifests, Mapping):
        return None
    manifest = manifests.get(label)
    return manifest if isinstance(manifest, PipelineManifestRecord) else None


def _records_by_date(loaded: Mapping[str, object], bucket: str) -> dict[str, Any]:
    value = loaded.get(bucket)
    return dict(value) if isinstance(value, Mapping) else {}


def _date_key(artifact_keys: Mapping[str, object], family: str, date_text: str) -> str:
    keys = artifact_keys[family]
    if not isinstance(keys, Mapping):
        raise TypeError(f"{family} is not keyed by date")
    return str(keys[date_text])


def _sequence_artifact_value(
    artifact_keys: Mapping[str, object],
    family: str,
) -> tuple[str, ...]:
    value = artifact_keys.get(family, ())
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        raise TypeError(f"{family} is not a sequence")
    return tuple(str(item) for item in value)


def _stage_manifest_keys(
    artifact_keys: Mapping[str, object],
    date_text: str,
) -> dict[str, str]:
    keys_by_date = artifact_keys["stage_manifests"]
    if not isinstance(keys_by_date, Mapping):
        raise TypeError("stage_manifests must be keyed by date")
    stage_keys = keys_by_date[date_text]
    if not isinstance(stage_keys, Mapping):
        raise TypeError("stage manifest keys must be a mapping")
    return {str(stage): str(key) for stage, key in stage_keys.items()}


def _append_loaded(loaded: dict[str, object], bucket: str, value: object) -> None:
    items = loaded[bucket]
    assert isinstance(items, list)
    items.append(value)


def _read_parquet_frame(payload: bytes) -> Any:
    pd = _require_pandas()
    return pd.read_parquet(io.BytesIO(payload))


def _require_pandas() -> Any:
    try:
        import pandas as pd

        importlib.import_module("pyarrow")
    except ModuleNotFoundError as exc:
        raise ModuleNotFoundError(
            "pandas and pyarrow are required for the AAPL evidence workflow."
        ) from exc
    return pd


def _clean_mapping(row: Mapping[str, object]) -> dict[str, object]:
    return {str(key): _clean_value(value) for key, value in row.items()}


def _clean_value(value: object) -> object:
    if value is None:
        return None
    try:
        if _require_pandas().isna(value):
            return None
    except (TypeError, ValueError):
        return value
    return value


def _row_value(row: Mapping[str, object] | None, key: str) -> object:
    if row is None:
        return None
    return row.get(key)


def _row_contains_ticker(row: Mapping[str, object], ticker: str) -> bool:
    symbols = row.get("symbols")
    if isinstance(symbols, str):
        return ticker in {part.strip().upper() for part in symbols.split(",")}
    if isinstance(symbols, Sequence):
        return ticker in {str(part).strip().upper() for part in symbols}
    return str(row.get("ticker", "")).upper() == ticker


def _match_raw_row(
    raw_rows: Sequence[Mapping[str, object]],
    scored_row: Mapping[str, object] | None,
) -> Mapping[str, object] | None:
    if not raw_rows:
        return None
    article_id = _row_value(scored_row, "article_id")
    if article_id is not None:
        for raw_row in raw_rows:
            if str(raw_row.get("id")) == str(article_id):
                return raw_row
    return raw_rows[0]


def _first_feature_record(
    loaded: Mapping[str, object],
    bucket: str,
    date_text: str,
    ticker: str,
) -> FeatureRecord | None:
    rows = _records_by_date(loaded, bucket).get(date_text, [])
    for row in rows if isinstance(rows, list) else []:
        if isinstance(row, FeatureRecord) and row.ticker == ticker:
            return row
    return None


def _first_mapping_row(
    loaded: Mapping[str, object],
    bucket: str,
    date_text: str,
) -> Mapping[str, object] | None:
    rows = _records_by_date(loaded, bucket).get(date_text, [])
    for row in rows if isinstance(rows, list) else []:
        if isinstance(row, Mapping):
            return row
    return None


def _news_timestamp(row: Mapping[str, object] | None) -> str | None:
    if row is None:
        return None
    for key in ("published_at", "created_at", "updated_at"):
        value = row.get(key)
        if value is not None:
            return str(value)
    return None


def _snippet(value: object, *, limit: int = 180) -> str | None:
    if value is None:
        return None
    text = " ".join(str(value).split())
    if len(text) <= limit:
        return text
    return text[: limit - 3] + "..."


def _string_or_none(value: object) -> str | None:
    return None if value is None else str(value)


def _review_notes(
    raw_row: Mapping[str, object] | None,
    scored_row: Mapping[str, object] | None,
    feature_record: object,
) -> str:
    notes: list[str] = []
    if raw_row is None:
        notes.append("missing_raw_news")
    if scored_row is None:
        notes.append("missing_finbert_scored_row")
    if not isinstance(feature_record, FeatureRecord):
        notes.append("missing_feature_record")
    return ";".join(notes)


def _sum_fields(row: object, fields: Sequence[str]) -> float | None:
    values = [_to_finite_float(_row_value(row, field)) for field in fields]
    if any(value is None for value in values):
        return None
    return sum(value for value in values if value is not None)


def _regime_label_probability(row: object, label: str) -> float | None:
    label_to_field = {
        "bear": "regime_prob_bear",
        "sideways": "regime_prob_sideways",
        "bull": "regime_prob_bull",
    }
    field = label_to_field.get(label)
    if field is None:
        return None
    return _to_finite_float(_row_value(row, field))


def _to_finite_float(value: object) -> float | None:
    if value is None:
        return None
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    return numeric if _is_finite(numeric) else None


def _is_finite(value: object) -> bool:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return False
    return not (math.isnan(numeric) or math.isinf(numeric))


def _null_rate(values: Sequence[object]) -> float:
    if not values:
        return 0.0
    return sum(value is None for value in values) / len(values)


def _mapping_null_rate(rows: Sequence[object]) -> float:
    values: list[object] = []
    for row in rows:
        if isinstance(row, Mapping):
            values.extend(row.values())
    return _null_rate(values)


def _format_sentiment(row: HumanReviewRow) -> str:
    score = _format_optional_number(row.finbert_score)
    probs = ", ".join(
        [
            f"p={_format_optional_number(row.finbert_positive)}",
            f"n={_format_optional_number(row.finbert_negative)}",
            f"u={_format_optional_number(row.finbert_neutral)}",
        ]
    )
    return f"{score}<br>{probs}"


def _format_regime(row: HumanReviewRow) -> str:
    confidence = _format_optional_number(row.regime_confidence)
    return f"{_markdown_cell(row.regime_label)} ({confidence})"


def _format_optional_number(value: float | None) -> str:
    return "" if value is None else f"{value:.4g}"


def _markdown_cell(value: object) -> str:
    if value is None:
        return ""
    return str(value).replace("|", "\\|").replace("\n", " ")


def _validate_iso_date(value: str, field_name: str) -> None:
    try:
        parsed = Date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"{field_name} must be YYYY-MM-DD") from exc
    if parsed.isoformat() != value:
        raise ValueError(f"{field_name} must be YYYY-MM-DD")


def _business_dates(from_date: str, to_date: str) -> tuple[str, ...]:
    """Return regular US equity trading sessions for backward-compatible callers."""
    return trading_dates(from_date, to_date)
