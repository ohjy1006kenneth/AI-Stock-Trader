from __future__ import annotations

import csv
import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from core.features.aapl_evidence import (
    build_aapl_pilot_evidence_bundle,
    default_aapl_pilot_evidence_paths,
)
from services.r2.paths import layer1_aapl_accuracy_report_path
from services.r2.writer import R2Writer

DEFAULT_SEMANTIC_REVIEW_ARTIFACT_DIR = Path("artifacts/reports/diagnostics")
_DEFAULT_RELEVANCE_MIN = 0.0
_TEXT_FIELDS = (
    "date",
    "ticker",
    "raw_headline",
    "raw_snippet",
    "raw_source",
    "raw_article_id",
    "notes",
)
_SOURCE_KEY_FIELDS = (
    "raw_news_key",
    "preprocessed_news_key",
    "finbert_scored_news_key",
    "topic_feature_key",
    "sentiment_feature_key",
    "regime_key",
    "feature_key",
)


class SemanticReviewObjectReader(Protocol):
    """Object-store operations required by the semantic-review dashboard."""

    def exists(self, key: str) -> bool:
        """Return True when an object key exists."""

    def get_object(self, key: str) -> bytes:
        """Read one object payload."""


@dataclass(frozen=True)
class SemanticReviewDashboardConfig:
    """Configuration for loading one semantic-review artifact set."""

    run_id: str
    from_date: str | None = None
    to_date: str | None = None
    ticker: str | None = None
    layer0_run_id: str | None = None
    layer1_run_id: str | None = None
    artifact_dir: Path = DEFAULT_SEMANTIC_REVIEW_ARTIFACT_DIR
    evidence_json_path: Path | None = None
    review_csv_path: Path | None = None
    accuracy_report_path: Path | None = None
    use_r2: bool = True
    local_r2_root: Path | None = None


@dataclass(frozen=True)
class SemanticReviewFilters:
    """Filters applied to semantic-review rows."""

    date: str | None = None
    from_date: str | None = None
    to_date: str | None = None
    ticker: str | None = None
    search: str | None = None
    min_relevance: float | None = None
    review_status: str | None = None


def build_semantic_review_payload(
    config: SemanticReviewDashboardConfig,
    *,
    filters: SemanticReviewFilters | None = None,
    reader: SemanticReviewObjectReader | None = None,
) -> dict[str, object]:
    """Build a browser-ready semantic-review payload from local or R2 artifacts."""
    _validate_config(config)
    load_result = _load_semantic_review_artifacts(config, reader=reader)
    evidence = load_result["evidence"] if isinstance(load_result["evidence"], Mapping) else {}
    accuracy_report = (
        load_result["accuracy_report"]
        if isinstance(load_result["accuracy_report"], Mapping)
        else {}
    )
    rows = _review_rows_from_artifacts(evidence, load_result["review_rows"])
    normalized_rows = _normalize_rows(rows)
    filtered_rows = apply_semantic_review_filters(
        normalized_rows,
        filters or SemanticReviewFilters(),
    )
    grouped_rows = group_semantic_review_rows(filtered_rows)
    return {
        "load_status": "ok" if evidence or normalized_rows else "missing_artifacts",
        "run": _run_summary(config, evidence, normalized_rows),
        "gates": list(evidence.get("gates", [])) if isinstance(evidence, Mapping) else [],
        "filters": _filters_to_dict(filters or SemanticReviewFilters()),
        "rows": filtered_rows,
        "groups": grouped_rows,
        "available_filters": _available_filters(normalized_rows),
        "artifact_keys": evidence.get("artifact_keys", {}) if isinstance(evidence, Mapping) else {},
        "source_provenance": (
            evidence.get("source_provenance", {}) if isinstance(evidence, Mapping) else {}
        ),
        "row_counts": evidence.get("row_counts", {}) if isinstance(evidence, Mapping) else {},
        "null_rates": evidence.get("null_rates", {}) if isinstance(evidence, Mapping) else {},
        "stale_artifacts": (
            evidence.get("stale_artifacts", {}) if isinstance(evidence, Mapping) else {}
        ),
        "accuracy_report": _accuracy_summary(accuracy_report),
        "source_files": load_result["source_files"],
        "missing_artifacts": load_result["missing_artifacts"],
        "read_errors": load_result["read_errors"],
        "readonly_notice": (
            "Read-only semantic review. Record human acceptance or rejection on the "
            "relevant GitHub issue; this dashboard does not mutate R2 or feature artifacts."
        ),
    }


def apply_semantic_review_filters(
    rows: Sequence[Mapping[str, object]],
    filters: SemanticReviewFilters,
) -> list[dict[str, object]]:
    """Return rows matching the requested date, ticker, relevance, and text filters."""
    normalized_search = _normalize_search(filters.search)
    ticker = filters.ticker.strip().upper() if filters.ticker else None
    min_relevance = (
        _DEFAULT_RELEVANCE_MIN if filters.min_relevance is None else filters.min_relevance
    )
    filtered: list[dict[str, object]] = []
    for row in rows:
        date_text = str(row.get("date") or "")
        if filters.date and date_text != filters.date:
            continue
        if filters.from_date and date_text < filters.from_date:
            continue
        if filters.to_date and date_text > filters.to_date:
            continue
        if ticker and str(row.get("ticker") or "").upper() != ticker:
            continue
        if filters.review_status and str(row.get("review_status") or "") != filters.review_status:
            continue
        relevance = _optional_float(row.get("finbert_relevance"))
        if relevance is not None and relevance < min_relevance:
            continue
        if normalized_search and normalized_search not in _search_blob(row):
            continue
        filtered.append(dict(row))
    return filtered


def group_semantic_review_rows(rows: Sequence[Mapping[str, object]]) -> list[dict[str, object]]:
    """Group rows by normalized headline, ticker, and source for duplicate review."""
    groups: dict[str, dict[str, object]] = {}
    for row in rows:
        key = str(row.get("duplicate_group_id") or _duplicate_group_id(row))
        group = groups.setdefault(
            key,
            {
                "group_id": key,
                "headline": row.get("raw_headline"),
                "ticker": row.get("ticker"),
                "source": row.get("raw_source"),
                "count": 0,
                "dates": [],
                "rows": [],
            },
        )
        group["count"] = int(group["count"]) + 1
        dates = group["dates"]
        assert isinstance(dates, list)
        date_text = row.get("date")
        if date_text is not None and date_text not in dates:
            dates.append(date_text)
        group_rows = group["rows"]
        assert isinstance(group_rows, list)
        group_rows.append(row)
    return sorted(
        groups.values(),
        key=lambda item: (-int(item["count"]), str(item.get("headline") or "")),
    )


def _load_semantic_review_artifacts(
    config: SemanticReviewDashboardConfig,
    *,
    reader: SemanticReviewObjectReader | None,
) -> dict[str, object]:
    source_files: dict[str, str] = {}
    missing_artifacts: list[str] = []
    read_errors: list[dict[str, str]] = []
    evidence = _load_local_json(
        _local_evidence_path(config),
        source_files=source_files,
        read_errors=read_errors,
        label="evidence_json",
    )
    review_rows = _load_local_csv(
        _local_review_csv_path(config),
        source_files=source_files,
        read_errors=read_errors,
        label="review_csv",
    )
    accuracy_report = _load_local_json(
        _local_accuracy_report_path(config),
        source_files=source_files,
        read_errors=read_errors,
        label="accuracy_report",
    )

    active_reader = reader
    needs_reader = config.use_r2 and active_reader is None and (
        evidence is None
        or review_rows is None
        or (
            accuracy_report is None
            and config.from_date is not None
            and config.to_date is not None
        )
    )
    if needs_reader:
        active_reader = R2Writer(local_root=config.local_r2_root)
    if config.use_r2 and active_reader is not None:
        if evidence is None:
            evidence = _load_r2_json(
                active_reader,
                _evidence_key(config.run_id),
                source_files=source_files,
                missing_artifacts=missing_artifacts,
                read_errors=read_errors,
                label="evidence_json",
            )
        if review_rows is None:
            review_rows = _load_r2_csv(
                active_reader,
                _review_csv_key(config.run_id),
                source_files=source_files,
                missing_artifacts=missing_artifacts,
                read_errors=read_errors,
                label="review_csv",
            )
        if accuracy_report is None and config.from_date and config.to_date:
            accuracy_report = _load_r2_json(
                active_reader,
                layer1_aapl_accuracy_report_path(
                    config.run_id,
                    config.from_date,
                    config.to_date,
                ),
                source_files=source_files,
                missing_artifacts=missing_artifacts,
                read_errors=read_errors,
                label="accuracy_report",
            )
        if evidence is None and _can_build_aapl_bundle(config):
            evidence = _build_aapl_evidence_in_memory(
                config,
                active_reader,
                source_files=source_files,
                read_errors=read_errors,
            )

    if evidence is None:
        missing_artifacts.append(str(_local_evidence_path(config)))
    if review_rows is None and not (
        isinstance(evidence, Mapping) and isinstance(evidence.get("human_review_rows"), list)
    ):
        missing_artifacts.append(str(_local_review_csv_path(config)))
    return {
        "evidence": evidence or {},
        "review_rows": review_rows or [],
        "accuracy_report": accuracy_report or {},
        "source_files": source_files,
        "missing_artifacts": sorted(set(missing_artifacts)),
        "read_errors": read_errors,
    }


def _build_aapl_evidence_in_memory(
    config: SemanticReviewDashboardConfig,
    reader: SemanticReviewObjectReader,
    *,
    source_files: dict[str, str],
    read_errors: list[dict[str, str]],
) -> dict[str, object] | None:
    try:
        bundle = build_aapl_pilot_evidence_bundle(
            run_id=config.run_id,
            from_date=str(config.from_date),
            to_date=str(config.to_date),
            layer0_run_id=str(config.layer0_run_id),
            layer1_run_id=config.layer1_run_id,
            ticker=(config.ticker or "AAPL").upper(),
            writer=reader,
        )
    except Exception as exc:  # noqa: BLE001
        read_errors.append({"source": "aapl_bundle_builder", "error": str(exc)})
        return None
    source_files["evidence_json"] = "in-memory:aapl_pilot_evidence_bundle"
    return bundle.to_dict()


def _review_rows_from_artifacts(
    evidence: Mapping[str, object],
    review_rows: object,
) -> list[Mapping[str, object]]:
    evidence_rows = evidence.get("human_review_rows")
    if isinstance(evidence_rows, list):
        return [row for row in evidence_rows if isinstance(row, Mapping)]
    if isinstance(review_rows, list):
        return [row for row in review_rows if isinstance(row, Mapping)]
    return []


def _normalize_rows(rows: Sequence[Mapping[str, object]]) -> list[dict[str, object]]:
    normalized = [_normalize_row(index, row) for index, row in enumerate(rows)]
    counts: dict[str, int] = {}
    for row in normalized:
        key = str(row["duplicate_group_id"])
        counts[key] = counts.get(key, 0) + 1
    for row in normalized:
        row["duplicate_count"] = counts[str(row["duplicate_group_id"])]
    return normalized


def _normalize_row(index: int, row: Mapping[str, object]) -> dict[str, object]:
    normalized: dict[str, object] = {str(key): _clean_scalar(value) for key, value in row.items()}
    normalized["row_id"] = index
    normalized["date"] = str(normalized.get("date") or "")
    normalized["ticker"] = str(normalized.get("ticker") or "").upper()
    for field in (
        "finbert_positive",
        "finbert_negative",
        "finbert_neutral",
        "finbert_score",
        "finbert_relevance",
        "topic_count",
        "topic_sentence_count",
        "sentiment_score",
        "sentiment_article_count",
        "regime_confidence",
        "regime_prob_bear",
        "regime_prob_sideways",
        "regime_prob_bull",
    ):
        normalized[field] = _optional_float(normalized.get(field))
    normalized["finbert_polarity"] = _finbert_polarity(normalized)
    normalized["duplicate_group_id"] = _duplicate_group_id(normalized)
    normalized["source_artifact_keys"] = {
        field: normalized.get(field)
        for field in _SOURCE_KEY_FIELDS
        if normalized.get(field) not in {None, ""}
    }
    return normalized


def _run_summary(
    config: SemanticReviewDashboardConfig,
    evidence: Mapping[str, object],
    rows: Sequence[Mapping[str, object]],
) -> dict[str, object]:
    ticker = config.ticker or evidence.get("ticker") or _first_row_value(rows, "ticker")
    from_date = config.from_date or evidence.get("from_date") or _min_row_value(rows, "date")
    to_date = config.to_date or evidence.get("to_date") or _max_row_value(rows, "date")
    return {
        "run_id": evidence.get("run_id", config.run_id),
        "layer1_run_id": evidence.get("layer1_run_id", config.layer1_run_id),
        "layer0_run_id": evidence.get("layer0_run_id", config.layer0_run_id),
        "ticker": ticker,
        "from_date": from_date,
        "to_date": to_date,
        "generated_at": evidence.get("generated_at"),
        "machine_integrity_status": evidence.get("machine_integrity_status", "unknown"),
        "human_semantic_review_status": evidence.get(
            "human_semantic_review_status",
            "unknown",
        ),
        "recommendation_for_issue_202": evidence.get(
            "recommendation_for_issue_202",
            "unknown",
        ),
        "review_row_count": len(rows),
    }


def _accuracy_summary(report: Mapping[str, object]) -> dict[str, object]:
    if not report:
        return {}
    acceptance = report.get("acceptance")
    input_evidence = report.get("input_evidence")
    return {
        "report_key": report.get("report_key"),
        "recommendation_for_issue_202": report.get("recommendation_for_issue_202"),
        "accepted": (
            acceptance.get("accepted")
            if isinstance(acceptance, Mapping)
            else report.get("accepted")
        ),
        "input_evidence": input_evidence if isinstance(input_evidence, Mapping) else {},
    }


def _available_filters(rows: Sequence[Mapping[str, object]]) -> dict[str, object]:
    relevance_values = [
        value
        for value in (_optional_float(row.get("finbert_relevance")) for row in rows)
        if value is not None
    ]
    return {
        "dates": sorted({str(row.get("date")) for row in rows if row.get("date")}),
        "tickers": sorted({str(row.get("ticker")) for row in rows if row.get("ticker")}),
        "review_statuses": sorted(
            {str(row.get("review_status")) for row in rows if row.get("review_status")}
        ),
        "relevance_min": min(relevance_values, default=None),
        "relevance_max": max(relevance_values, default=None),
    }


def _filters_to_dict(filters: SemanticReviewFilters) -> dict[str, object]:
    return {
        "date": filters.date,
        "from_date": filters.from_date,
        "to_date": filters.to_date,
        "ticker": filters.ticker,
        "search": filters.search,
        "min_relevance": filters.min_relevance,
        "review_status": filters.review_status,
    }


def _load_local_json(
    path: Path | None,
    *,
    source_files: dict[str, str],
    read_errors: list[dict[str, str]],
    label: str,
) -> dict[str, object] | None:
    if path is None or not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        read_errors.append({"source": str(path), "error": str(exc)})
        return None
    if not isinstance(payload, dict):
        read_errors.append({"source": str(path), "error": "JSON payload is not an object"})
        return None
    source_files[label] = str(path)
    return payload


def _load_local_csv(
    path: Path | None,
    *,
    source_files: dict[str, str],
    read_errors: list[dict[str, str]],
    label: str,
) -> list[dict[str, object]] | None:
    if path is None or not path.exists():
        return None
    try:
        with path.open("r", encoding="utf-8", newline="") as handle:
            rows = [dict(row) for row in csv.DictReader(handle)]
    except (OSError, csv.Error) as exc:
        read_errors.append({"source": str(path), "error": str(exc)})
        return None
    source_files[label] = str(path)
    return rows


def _load_r2_json(
    reader: SemanticReviewObjectReader,
    key: str,
    *,
    source_files: dict[str, str],
    missing_artifacts: list[str],
    read_errors: list[dict[str, str]],
    label: str,
) -> dict[str, object] | None:
    if not reader.exists(key):
        missing_artifacts.append(key)
        return None
    try:
        payload = json.loads(reader.get_object(key).decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError, OSError) as exc:
        read_errors.append({"source": key, "error": str(exc)})
        return None
    if not isinstance(payload, dict):
        read_errors.append({"source": key, "error": "JSON payload is not an object"})
        return None
    source_files[label] = f"r2:{key}"
    return payload


def _load_r2_csv(
    reader: SemanticReviewObjectReader,
    key: str,
    *,
    source_files: dict[str, str],
    missing_artifacts: list[str],
    read_errors: list[dict[str, str]],
    label: str,
) -> list[dict[str, object]] | None:
    if not reader.exists(key):
        missing_artifacts.append(key)
        return None
    try:
        text = reader.get_object(key).decode("utf-8")
        rows = [dict(row) for row in csv.DictReader(text.splitlines())]
    except (UnicodeDecodeError, csv.Error, OSError) as exc:
        read_errors.append({"source": key, "error": str(exc)})
        return None
    source_files[label] = f"r2:{key}"
    return rows


def _local_evidence_path(config: SemanticReviewDashboardConfig) -> Path | None:
    if config.evidence_json_path is not None:
        return config.evidence_json_path
    default_paths = default_aapl_pilot_evidence_paths(config.run_id, output_dir=config.artifact_dir)
    candidates = [
        default_paths["json"],
        config.artifact_dir / "evidence.json",
        config.artifact_dir / f"{config.run_id}.json",
    ]
    return _first_existing_or_default(candidates)


def _local_review_csv_path(config: SemanticReviewDashboardConfig) -> Path | None:
    if config.review_csv_path is not None:
        return config.review_csv_path
    default_paths = default_aapl_pilot_evidence_paths(config.run_id, output_dir=config.artifact_dir)
    candidates = [
        default_paths["csv"],
        config.artifact_dir / "review.csv",
        config.artifact_dir / f"{config.run_id}.csv",
    ]
    return _first_existing_or_default(candidates)


def _local_accuracy_report_path(config: SemanticReviewDashboardConfig) -> Path | None:
    if config.accuracy_report_path is not None:
        return config.accuracy_report_path
    candidates: list[Path] = []
    if config.from_date and config.to_date:
        accuracy_key = layer1_aapl_accuracy_report_path(
            config.run_id,
            config.from_date,
            config.to_date,
        )
        candidates.append(
            config.artifact_dir / Path(accuracy_key).name
        )
    candidates.extend(
        sorted(
            config.artifact_dir.glob(
                f"layer1_aapl_feature_accuracy_{config.run_id}_*.json"
            )
        )
    )
    return _first_existing_or_default(candidates)


def _first_existing_or_default(candidates: Sequence[Path]) -> Path | None:
    for path in candidates:
        if path.exists():
            return path
    return candidates[0] if candidates else None


def _evidence_key(run_id: str) -> str:
    return f"artifacts/reports/diagnostics/aapl_pilot_evidence_{run_id}.json"


def _review_csv_key(run_id: str) -> str:
    return f"artifacts/reports/diagnostics/aapl_pilot_human_review_rows_{run_id}.csv"


def _can_build_aapl_bundle(config: SemanticReviewDashboardConfig) -> bool:
    return (
        bool(config.from_date)
        and bool(config.to_date)
        and bool(config.layer0_run_id)
        and (config.ticker is None or config.ticker.upper() == "AAPL")
    )


def _duplicate_group_id(row: Mapping[str, object]) -> str:
    headline = _normalize_headline(str(row.get("raw_headline") or "missing-headline"))
    ticker = str(row.get("ticker") or "").upper()
    source = str(row.get("raw_source") or "").lower()
    return f"headline:{ticker}:{source}:{headline}"


def _normalize_headline(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", value.lower()).strip()


def _normalize_search(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = " ".join(value.lower().split())
    return normalized or None


def _search_blob(row: Mapping[str, object]) -> str:
    return " ".join(str(row.get(field) or "").lower() for field in _TEXT_FIELDS)


def _finbert_polarity(row: Mapping[str, object]) -> str:
    probabilities = {
        "positive": _optional_float(row.get("finbert_positive")),
        "negative": _optional_float(row.get("finbert_negative")),
        "neutral": _optional_float(row.get("finbert_neutral")),
    }
    present = {key: value for key, value in probabilities.items() if value is not None}
    if not present:
        return "unknown"
    return max(present.items(), key=lambda item: item[1])[0]


def _clean_scalar(value: object) -> object:
    if value == "":
        return None
    return value


def _optional_float(value: object) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _first_row_value(rows: Sequence[Mapping[str, object]], field: str) -> object | None:
    for row in rows:
        value = row.get(field)
        if value not in {None, ""}:
            return value
    return None


def _min_row_value(rows: Sequence[Mapping[str, object]], field: str) -> object | None:
    values = [str(row.get(field)) for row in rows if row.get(field)]
    return min(values) if values else None


def _max_row_value(rows: Sequence[Mapping[str, object]], field: str) -> object | None:
    values = [str(row.get(field)) for row in rows if row.get(field)]
    return max(values) if values else None


def _validate_config(config: SemanticReviewDashboardConfig) -> None:
    if not config.run_id.strip():
        raise ValueError("run_id cannot be empty")
    for label, value in (("from_date", config.from_date), ("to_date", config.to_date)):
        if value is not None and not re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
            raise ValueError(f"{label} must be YYYY-MM-DD")
    if config.from_date and config.to_date and config.from_date > config.to_date:
        raise ValueError("from_date must be <= to_date")
