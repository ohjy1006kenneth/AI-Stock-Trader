"""Bounded, deterministic semantic QA aggregation for the four-ticker pilot."""

from __future__ import annotations

import math
import re
from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import asdict, is_dataclass
from datetime import UTC, datetime
from typing import Any

PILOT_TICKERS = ("AAPL", "AMD", "NVDA", "MSFT")
SEMANTIC_QA_SCHEMA_ID = "layer1-semantic-qa/v1"
FRESHNESS_THRESHOLD_SECONDS = 86_400
_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")

_STAGE_FIELDS = {
    "total_preprocessed_chunks": ("preprocessing_row_count", "preprocessed_chunk_count"),
    "local_target_evidence": ("local_target_evidence_count",),
    "materially_relevant": ("materially_relevant_count",),
    "accepted_or_borderline": ("accepted_or_borderline_count",),
    "sentiment_scored": ("sentence_count", "sentiment_scored_count"),
    "included_in_signal": ("included_in_signal_count", "signal_rows"),
    "nonzero_effective_contribution": ("nonzero_effective_contribution_count",),
    "article_context_only_rejected": ("article_context_only_rejected_count",),
}


def normalize_semantic_qa_query(
    *,
    run_id: str | None,
    from_date: str | None,
    to_date: str | None,
    tickers: str | None = None,
    sample_limit: str | int | None = None,
) -> dict[str, Any]:
    """Validate and normalize the opaque run/date/ticker/limit request tuple."""
    if run_id is None or not run_id:
        raise ValueError("run_id is required")
    if (
        from_date is None
        or to_date is None
        or not _DATE_RE.fullmatch(from_date)
        or not _DATE_RE.fullmatch(to_date)
    ):
        raise ValueError("from_date and to_date must use YYYY-MM-DD")
    try:
        start = datetime.strptime(from_date, "%Y-%m-%d").date()
        end = datetime.strptime(to_date, "%Y-%m-%d").date()
    except ValueError as exc:
        raise ValueError("from_date and to_date must be valid dates") from exc
    if start > end:
        raise ValueError("from_date must be on or before to_date")
    requested = (
        PILOT_TICKERS
        if not tickers
        else tuple(dict.fromkeys(x.strip().upper() for x in tickers.split(",") if x.strip()))
    )
    unsupported = sorted(set(requested) - set(PILOT_TICKERS))
    if unsupported or not requested:
        raise ValueError(f"unsupported tickers: {', '.join(unsupported or [''])}".strip())
    normalized_tickers = tuple(ticker for ticker in PILOT_TICKERS if ticker in requested)
    try:
        limit = 25 if sample_limit in (None, "") else int(sample_limit)
    except (TypeError, ValueError) as exc:
        raise ValueError("sample_limit must be an integer from 1 to 50") from exc
    if not 1 <= limit <= 50:
        raise ValueError("sample_limit must be an integer from 1 to 50")
    return {
        "run_id": run_id,
        "from_date": from_date,
        "to_date": to_date,
        "tickers": normalized_tickers,
        "sample_limit": limit,
    }


def build_semantic_qa_payload(
    *,
    reports: Mapping[str, Mapping[str, Any] | Any],
    run_id: str,
    from_date: str,
    to_date: str,
    tickers: Sequence[str] = PILOT_TICKERS,
    sample_limit: int = 25,
    generated_at: str | None = None,
) -> dict[str, Any]:
    """Build a bounded four-ticker QA payload from canonical per-ticker reports."""
    normalized_tickers = tuple(
        ticker for ticker in PILOT_TICKERS if ticker in {x.upper() for x in tickers}
    )
    warnings: list[dict[str, Any]] = []
    ticker_payloads: dict[str, Any] = {}
    artifact_ids: dict[str, list[str]] = {}
    source_times: list[datetime] = []
    for ticker in normalized_tickers:
        report = reports.get(ticker)
        if report is None:
            ticker_payloads[ticker] = _empty_ticker()
            artifact_ids[ticker] = []
            warnings.append(
                _warning(
                    "missing_review_artifacts",
                    ticker,
                    None,
                    "No review report was available for this ticker.",
                )
            )
            continue
        data = _as_mapping(report)
        ticker_payloads[ticker] = _build_ticker(data, ticker, sample_limit, warnings)
        artifact_ids[ticker] = _artifact_ids(data)
        timestamp = _parse_timestamp(data.get("generated_at"))
        if timestamp is not None:
            source_times.append(timestamp)
    now = _parse_timestamp(generated_at) or datetime.now(UTC)
    freshness = _freshness(source_times, now, warnings)
    status = (
        "empty"
        if not any(_ticker_has_rows(item) for item in ticker_payloads.values())
        else ("warning" if warnings or freshness["state"] == "stale" else "ready")
    )
    matrix = _build_leakage_matrix(reports, normalized_tickers)
    integrity = _integrity(ticker_payloads, reports, run_id, from_date, to_date, normalized_tickers)
    if integrity["status"] == "fail":
        status = "warning"
    return {
        "ok": True,
        "status": status,
        "schema_id": SEMANTIC_QA_SCHEMA_ID,
        "generated_at": now.isoformat(),
        "run": {
            "run_id": run_id,
            "requested_start": from_date,
            "requested_end": to_date,
            "tickers": list(normalized_tickers),
            "artifact_ids": artifact_ids,
        },
        "freshness": freshness,
        "tickers": ticker_payloads,
        "leakage_matrix": matrix,
        "integrity": integrity,
        "human_gate": {
            "ticker_dispositions": {ticker: None for ticker in normalized_tickers},
            "overall_disposition": None,
            "all_tickers_disposed": False,
            "can_accept_four_ticker_pilot": False,
        },
        "warnings": warnings,
    }


def _build_ticker(
    report: Mapping[str, Any], ticker: str, limit: int, warnings: list[dict[str, Any]]
) -> dict[str, Any]:
    rows = _evidence_rows(report, ticker)
    queues: dict[str, Any] = {
        name: []
        for name in (
            "potential_false_positives",
            "potential_false_negatives",
            "top_contributors",
            "cross_ticker_anomalies",
        )
    }
    for row in rows:
        local = _local_evidence(row, ticker)
        included = _bool(
            row.get("included_in_signal", row.get("signal", {}).get("included_in_signal"))
        )
        rejected = _bool(row.get("rejected")) or str(row.get("relevance_decision", "")).lower() in {
            "rejected",
            "exclude",
            "excluded",
        }
        material = _material_evidence(row)
        owner = _owner(row)
        relationship = _bool(row.get("explicit_local_material_relationship"))
        if included and (not local or (owner not in {None, ticker} and not relationship)):
            queues["potential_false_positives"].append(_queue_row(row, "potential_false_positive"))
        elif rejected and local and material:
            queues["potential_false_negatives"].append(_queue_row(row, "potential_false_negative"))
        if included and owner not in {None, ticker} and not relationship:
            queues["cross_ticker_anomalies"].append(_queue_row(row, "cross_ticker_anomaly"))
        contribution = _row_contribution(row)
        if included and contribution is not None and contribution != 0:
            queues["top_contributors"].append(_queue_row(row, "top_contributor"))
    queues["top_contributors"].sort(
        key=lambda row: (
            -abs(_row_contribution(row) or 0),
            str(row.get("row_id", "")),
        )
    )
    for name in queues:
        # These rows are the complete canonical producer slice; only the returned rows are bounded.
        queues[name] = _queue(queues[name], len(queues[name]), limit)
    summary = {
        "signal_rows": _canonical(
            report,
            ("signal_rows", "included_in_signal_count"),
            sum(1 for row in rows if _bool(row.get("included_in_signal"))),
        ),
        "rejected_rows": _canonical(
            report, ("rejected_rows",), sum(1 for row in rows if _bool(row.get("rejected")))
        ),
        "leakage_candidates": queues["potential_false_positives"]["canonical_count"],
        "direct_accepted": _canonical(
            report,
            ("direct_accepted",),
            sum(1 for row in rows if _bool(row.get("direct_accepted"))),
        ),
        "borderline": _canonical(
            report, ("borderline",), sum(1 for row in rows if _bool(row.get("borderline")))
        ),
        "total_contribution": _total_contribution(report, rows),
        "human_status": None,
    }
    return {
        "summary": summary,
        "funnel": _funnel(report, rows, queues, ticker, warnings),
        "queues": queues,
    }


def _evidence_rows(report: Mapping[str, Any], ticker: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    relevance = {_identity(row): row for row in _list(report.get("relevance_gate_rows"))}
    preprocessing = {_identity(row): row for row in _list(report.get("preprocessing_rows"))}
    for article in _list(report.get("article_groups")):
        for sentence in _list(article.get("sentence_rows")):
            merged = dict(article)
            merged.update(sentence)
            key = _identity(merged)
            merged["relevance"] = relevance.get(key, relevance.get(_identity(article), {}))
            merged["article_context"] = {
                key: article.get(key)
                for key in ("headline", "source", "url", "article_id", "date")
                if key in article
            }
            merged["chunk_local_evidence"] = preprocessing.get(key, {})
            merged["ticker"] = merged.get("ticker", ticker)
            rows.append(merged)
    if not rows:
        rows = [dict(row) for row in _list(report.get("relevance_gate_rows"))]
    return sorted(rows, key=lambda row: (_identity(row), str(row.get("text", ""))))


def _funnel(
    report: Mapping[str, Any],
    rows: list[dict[str, Any]],
    queues: Mapping[str, Any],
    ticker: str,
    warnings: list[dict[str, Any]],
) -> dict[str, Any]:
    stages: dict[str, Any] = {}
    for stage, names in _STAGE_FIELDS.items():
        count = _canonical(report, names, None)
        sample = min(count, len(rows)) if count is not None else len(rows)
        stages[stage] = _count_record(count, sample)
        if count is None:
            warnings.append(
                _warning(
                    "canonical_count_unavailable",
                    ticker,
                    stage,
                    "Producer did not expose a trustworthy canonical count.",
                )
            )
    if not rows and (
        report.get("row_count") == 0
        or all(_canonical(report, names, None) == 0 for names in _STAGE_FIELDS.values())
    ):
        warnings.append(_warning("no_review_rows", ticker, None, "The producer established an empty review result."))
    return stages


def _stage_fallback(stage: str, rows: list[dict[str, Any]], ticker: str) -> int:
    if stage == "total_preprocessed_chunks":
        return len(rows)
    if stage == "local_target_evidence":
        return sum(1 for r in rows if _local_evidence(r, ticker))
    if stage == "materially_relevant":
        return sum(1 for r in rows if _material_evidence(r))
    if stage == "accepted_or_borderline":
        return sum(1 for r in rows if not _bool(r.get("rejected")))
    if stage == "sentiment_scored":
        return sum(
            1
            for r in rows
            if any(r.get(k) is not None for k in ("sentiment_score", "positive_probability"))
        )
    if stage == "included_in_signal":
        return sum(1 for r in rows if _bool(r.get("included_in_signal")))
    if stage == "nonzero_effective_contribution":
        return sum(1 for r in rows if _number(r.get("effective_contribution")) not in (None, 0))
    return sum(1 for r in rows if _bool(r.get("rejected")) and not _local_evidence(r, ticker))


def _queue(rows: list[dict[str, Any]], canonical: int | None, limit: int) -> dict[str, Any]:
    sample = rows[:limit]
    count = canonical
    omitted = count - len(sample) if count is not None and count >= len(sample) else None
    return {
        "canonical_count": count,
        "sample_count": len(sample),
        "omitted_count": omitted,
        "rows": sample,
    }


def _queue_row(row: Mapping[str, Any], reason: str) -> dict[str, Any]:
    identity_keys = (
        "row_id",
        "ticker",
        "run_id",
        "date",
        "article_id",
        "sentence_index",
        "chunk_index",
        "artifact_key",
        "source",
        "provider",
        "headline",
        "url",
        "text",
        "effective_contribution",
        "sentiment_score",
        "final_contribution",
        "final_signal_contribution",
        "evidence_owner",
        "evidence_subject",
        "explicit_local_material_relationship",
    )
    result = {key: row[key] for key in identity_keys if key in row and _json_safe(row[key])}
    result["row_id"] = str(row.get("row_id") or _identity(row))
    result["reason_code"] = reason
    result["article_context"] = dict(row.get("article_context", {}))
    result["chunk_local_evidence"] = dict(row.get("chunk_local_evidence", {}))
    result["relevance"] = dict(row.get("relevance", {}))
    result["signal"] = {
        key: row[key]
        for key in ("included_in_signal", "effective_contribution", "contribution")
        if key in row
    }
    return result


def _build_leakage_matrix(reports: Mapping[str, Any], columns: Sequence[str]) -> dict[str, Any]:
    subjects: dict[str, dict[str, Any]] = defaultdict(dict)
    for ticker in columns:
        report = reports.get(ticker)
        if report is None:
            continue
        for row in _evidence_rows(_as_mapping(report), ticker):
            owner = _owner(row)
            relationship = _bool(row.get("explicit_local_material_relationship"))
            included = _bool(row.get("included_in_signal"))
            if not included or owner in {None, ticker} or relationship:
                continue
            subject = owner or "unknown_generic"
            cell = subjects[subject].setdefault(
                ticker, {"canonical_count": 0, "row_ids": [], "suspicious": True}
            )
            cell["canonical_count"] += 1
            if len(cell["row_ids"]) < 50:
                cell["row_ids"].append(str(row.get("row_id") or _identity(row)))
    return {
        subject: {
            column: subjects.get(subject, {}).get(
                column, {"canonical_count": 0, "row_ids": [], "suspicious": False}
            )
            for column in columns
        }
        for subject in sorted(subjects)
    }


def _integrity(
    payloads: Mapping[str, Any],
    reports: Mapping[str, Any],
    run_id: str,
    from_date: str,
    to_date: str,
    tickers: Sequence[str],
) -> dict[str, Any]:
    issues: list[str] = []
    stages: dict[str, Any] = {}
    for ticker in tickers:
        report = reports.get(ticker)
        if report is None:
            continue
        data = _as_mapping(report)
        for key, expected in (("run_id", run_id), ("ticker", ticker), ("from_date", from_date), ("to_date", to_date), ("schema_id", SEMANTIC_QA_SCHEMA_ID)):
            if data.get(key) is not None and data.get(key) != expected:
                issues.append(f"{ticker}:{key}_mismatch")
        for stage, names in _STAGE_FIELDS.items():
            count = _canonical(data, names, None)
            rows = _evidence_rows(data, ticker)
            sample = min(count, len(rows)) if count is not None else len(rows)
            omitted = count - sample if count is not None and sample is not None and count >= sample else None
            stages[f"{ticker}:{stage}"] = _count_record(count, sample)
            if count is not None and sample is not None and omitted is not None and sample + omitted != count:
                issues.append(f"{ticker}:{stage}_not_reconciled")
    return {
        "status": "fail"
        if issues
        else ("warn" if any(not reports.get(t) for t in tickers) else "pass"),
        "issue_codes": sorted(issues),
        "stages": stages,
    }


def _empty_ticker() -> dict[str, Any]:
    empty = {"canonical_count": None, "sample_count": 0, "omitted_count": None, "status": "unknown"}
    return {
        "summary": {
            "signal_rows": None,
            "rejected_rows": None,
            "leakage_candidates": None,
            "direct_accepted": None,
            "borderline": None,
            "total_contribution": None,
            "human_status": None,
        },
        "funnel": {stage: dict(empty) for stage in _STAGE_FIELDS},
        "queues": {
            name: {"canonical_count": None, "sample_count": 0, "omitted_count": None, "rows": []}
            for name in (
                "potential_false_positives",
                "potential_false_negatives",
                "top_contributors",
                "cross_ticker_anomalies",
            )
        },
    }


def _count_record(canonical: int | None, sample: int | None) -> dict[str, Any]:
    omitted = canonical - sample if canonical is not None and sample is not None and canonical >= sample else None
    return {
        "canonical_count": canonical,
        "sample_count": sample,
        "omitted_count": omitted,
        "status": "measured" if canonical is not None else "unknown",
    }


def _freshness(
    times: Sequence[datetime], now: datetime, warnings: list[dict[str, Any]]
) -> dict[str, Any]:
    if not times:
        return {
            "state": "unknown",
            "source_generated_at": None,
            "age_seconds": None,
            "threshold_seconds": FRESHNESS_THRESHOLD_SECONDS,
        }
    source = max(times)
    age = max(0, int((now - source).total_seconds()))
    if age > FRESHNESS_THRESHOLD_SECONDS:
        warnings.append(
            _warning(
                "stale_review_artifacts",
                None,
                None,
                "Review artifacts are older than the advisory freshness threshold.",
            )
        )
    return {
        "state": "stale" if age > FRESHNESS_THRESHOLD_SECONDS else "fresh",
        "source_generated_at": source.isoformat(),
        "age_seconds": age,
        "threshold_seconds": FRESHNESS_THRESHOLD_SECONDS,
    }


def _artifact_ids(report: Mapping[str, Any]) -> list[str]:
    return sorted(
        str(item)
        for values in (_as_mapping(report.get("artifact_keys"))).values()
        for item in (values if isinstance(values, list) else [])
    )


def _canonical(report: Mapping[str, Any], names: Sequence[str], fallback: int | None) -> int | None:
    for name in names:
        value = report.get(name)
        if value is None and isinstance(report.get("summary"), Mapping):
            value = report["summary"].get(name)
        if isinstance(value, int) and value >= 0:
            return value
    return fallback


def _canonical_queue_count(report: Mapping[str, Any], name: str) -> int | None:
    return _canonical(report, (f"{name}_count",), None)


def _total_contribution(
    report: Mapping[str, Any], rows: Sequence[Mapping[str, Any]]
) -> float | None:
    value = _number(report.get("total_contribution"))
    if value is not None:
        return value
    values = [_row_contribution(row) for row in rows]
    values = [x for x in values if x is not None]
    return float(sum(values)) if values else None


def _identity(row: Mapping[str, Any]) -> str:
    return ":".join(
        str(row.get(key, ""))
        for key in ("date", "ticker", "article_id", "sentence_index", "chunk_index")
    )


def _owner(row: Mapping[str, Any]) -> str | None:
    value = row.get("evidence_owner", row.get("owner", row.get("evidence_subject")))
    return str(value).upper() if value not in (None, "") else None


def _local_evidence(row: Mapping[str, Any], ticker: str) -> bool:
    nested = row.get("chunk_local_evidence")
    if isinstance(nested, Mapping):
        for key in (
            "has_requested_ticker_evidence",
            "local_target_evidence",
            "direct_target_evidence",
        ):
            if key in nested:
                return _bool(nested[key])
        mentions = nested.get("ticker_mentions")
        if isinstance(mentions, (list, tuple)):
            return ticker in {str(x).upper() for x in mentions}
    for key in ("has_requested_ticker_evidence", "local_target_evidence", "direct_target_evidence"):
        if key in row:
            return _bool(row[key])
    return (
        ticker in {str(x).upper() for x in row.get("ticker_mentions", [])}
        if isinstance(row.get("ticker_mentions"), (list, tuple))
        else False
    )


def _material_evidence(row: Mapping[str, Any]) -> bool:
    reason = " ".join(
        str(x)
        for x in (row.get("reason_codes", []), row.get("event_type", ""), row.get("text", ""))
    ).lower()
    return any(
        token in reason
        for token in (
            "product",
            "legal",
            "regulat",
            "supply",
            "chip",
            "gpu",
            "datacenter",
            "azure",
            "copilot",
            "enterprise",
            "business",
            "material",
        )
    )


def _bool(value: Any) -> bool:
    return (
        value is True
        or (
            isinstance(value, str)
            and value.strip().lower() in {"true", "yes", "1", "included", "accepted"}
        )
        or (isinstance(value, (int, float)) and value == 1)
    )


def _number(value: Any) -> float | None:
    try:
        number = float(value)
        return number if math.isfinite(number) else None
    except (TypeError, ValueError):
        return None


def _parse_timestamp(value: Any) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)
    except ValueError:
        return None


def _as_mapping(value: Any) -> Mapping[str, Any]:
    return (
        value.to_dict()
        if hasattr(value, "to_dict")
        else asdict(value)  # type: ignore[arg-type]
        if is_dataclass(value)
        else value
        if isinstance(value, Mapping)
        else {}
    )


def _list(value: Any) -> list[Mapping[str, Any]]:
    return [x for x in value if isinstance(x, Mapping)] if isinstance(value, list) else []


def _json_safe(value: Any) -> bool:
    return value is None or isinstance(value, (str, int, float, bool, list, dict))


def _ticker_has_rows(value: Mapping[str, Any]) -> bool:
    summary = value.get("summary", {})
    if any(isinstance(summary.get(key), int) and summary[key] > 0 for key in summary):
        return True
    return any(
        bool(stage.get("canonical_count") or stage.get("sample_count"))
        for stage in value.get("funnel", {}).values()
        if isinstance(stage, Mapping)
    )


def _row_contribution(row: Mapping[str, Any]) -> float | None:
    """Read the producer's canonical final contribution with compatibility fallbacks."""
    for key in ("final_signal_contribution", "final_contribution", "effective_contribution", "contribution"):
        value = _number(row.get(key))
        if value is not None:
            return value
    return None


def _warning(code: str, ticker: str | None, stage: str | None, message: str) -> dict[str, Any]:
    return {
        "code": code,
        "severity": "warning",
        "ticker": ticker,
        "stage": stage,
        "message": message,
    }


__all__ = [
    "PILOT_TICKERS",
    "SEMANTIC_QA_SCHEMA_ID",
    "build_semantic_qa_payload",
    "normalize_semantic_qa_query",
]
