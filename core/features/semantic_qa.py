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


_REJECTED_DECISIONS = frozenset({"rejected", "reject", "exclude", "excluded"})
_ACCEPTED_DECISIONS = frozenset({"accepted", "accept", "include", "included"})
_BORDERLINE_DECISIONS = frozenset({"borderline"})
_MATRIX_UNKNOWN_SUBJECT = "unknown_generic"


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
    matrix = _build_leakage_matrix(reports, normalized_tickers, sample_limit)
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
        included = _included_in_signal(row)
        decision = _relevance_decision(row)
        rejected = decision in _REJECTED_DECISIONS or _bool(row.get("rejected"))
        material = _material_evidence(row)
        owner = _evidence_owner(row, ticker)
        relationship = _bool(row.get("explicit_local_material_relationship"))
        if included and (not local or (owner not in {None, ticker} and not relationship)):
            queues["potential_false_positives"].append(
                _queue_row(row, "potential_false_positive", owner=owner)
            )
        elif rejected and local and material:
            queues["potential_false_negatives"].append(
                _queue_row(row, "potential_false_negative", owner=owner)
            )
        if included and owner not in {None, ticker} and not relationship:
            queues["cross_ticker_anomalies"].append(
                _queue_row(row, "cross_ticker_anomaly", owner=owner)
            )
        contribution = _row_contribution(row)
        if included and contribution is not None and contribution != 0:
            queues["top_contributors"].append(
                _queue_row(row, "top_contributor", owner=owner)
            )
    queues["top_contributors"].sort(
        key=lambda row: (
            -abs(_row_contribution(row) or 0),
            str(row.get("row_id", "")),
        )
    )
    for name in queues:
        # These rows are the complete canonical producer slice; only the returned rows are bounded.
        queues[name] = _queue(queues[name], len(queues[name]), limit)
    accepted, borderline, rejected_count, has_decision = _decision_counts(rows)
    established_empty = _establishes_empty(report, rows)
    summary: dict[str, Any] = {
        "signal_rows": _summary_count(
            report,
            ("signal_rows", "included_in_signal_count"),
            sum(1 for row in rows if _included_in_signal(row)),
            observable=any(_has_included_flag(row) for row in rows),
            established_empty=established_empty,
            ticker=ticker,
            warnings=warnings,
        ),
        "rejected_rows": _summary_count(
            report,
            ("rejected_rows",),
            rejected_count,
            observable=has_decision,
            established_empty=established_empty,
            ticker=ticker,
            warnings=warnings,
        ),
        "leakage_candidates": queues["potential_false_positives"]["canonical_count"],
        "direct_accepted": _summary_count(
            report,
            ("direct_accepted",),
            accepted,
            observable=has_decision,
            established_empty=established_empty,
            ticker=ticker,
            warnings=warnings,
        ),
        "borderline": _summary_count(
            report,
            ("borderline",),
            borderline,
            observable=has_decision,
            established_empty=established_empty,
            ticker=ticker,
            warnings=warnings,
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
        # ``sample_count`` is the retained row slice for this stage, not the canonical count.
        stages[stage] = _count_record(count, _stage_stats(stage, rows, ticker))
        if count is None:
            warnings.append(
                _warning(
                    "canonical_count_unavailable",
                    ticker,
                    stage,
                    "Producer did not expose a trustworthy canonical count.",
                )
            )
    if _establishes_empty(report, rows):
        warnings.append(_warning("no_review_rows", ticker, None, "The producer established an empty review result."))
    return stages


def _stage_stats(stage: str, rows: list[dict[str, Any]], ticker: str) -> int:
    """Return the retained row count observed for a canonical funnel/integrity stage."""
    if stage == "total_preprocessed_chunks":
        return len(rows)
    if stage == "local_target_evidence":
        return sum(1 for r in rows if _local_evidence(r, ticker))
    if stage == "materially_relevant":
        return sum(1 for r in rows if _material_evidence(r))
    if stage == "accepted_or_borderline":
        return sum(1 for r in rows if _relevance_decision(r) in (_ACCEPTED_DECISIONS | _BORDERLINE_DECISIONS))
    if stage == "sentiment_scored":
        return sum(
            1
            for r in rows
            if any(r.get(k) is not None for k in ("sentiment_score", "positive_probability"))
        )
    if stage == "included_in_signal":
        return sum(1 for r in rows if _included_in_signal(r))
    if stage == "nonzero_effective_contribution":
        return sum(1 for r in rows if _row_contribution(r) not in (None, 0))
    rejected = (r for r in rows if _relevance_decision(r) in _REJECTED_DECISIONS or _bool(r.get("rejected")))
    return sum(1 for r in rejected if not _local_evidence(r, ticker))


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


def _queue_row(
    row: Mapping[str, Any], reason: str, *, owner: str | None = None
) -> dict[str, Any]:
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
    if owner is not None:
        result["evidence_owner"] = owner
    result["article_context"] = dict(row.get("article_context", {}))
    result["chunk_local_evidence"] = dict(row.get("chunk_local_evidence", {}))
    result["relevance"] = dict(row.get("relevance", {}))
    result["signal"] = {
        key: row[key]
        for key in (
            "included_in_signal",
            "effective_contribution",
            "contribution",
            "final_contribution",
            "final_signal_contribution",
        )
        if key in row
    }
    return result


def _build_leakage_matrix(
    reports: Mapping[str, Any], columns: Sequence[str], limit: int
) -> dict[str, Any]:
    """Derive the full canonical owner matrix before any bounded row sampling.

    The matrix is built from every qualifying canonical contributing row, so cell
    ``canonical_count`` is the true canonical count. Only the retained ``row_ids``
    are bounded by the normalized ``sample_limit``.
    """
    subjects: dict[str, dict[str, Any]] = defaultdict(dict)
    for ticker in columns:
        report = reports.get(ticker)
        if report is None:
            continue
        for row in _evidence_rows(_as_mapping(report), ticker):
            if not _included_in_signal(row):
                continue
            owner = _evidence_owner(row, ticker)
            subject = owner or _MATRIX_UNKNOWN_SUBJECT
            relationship = _bool(row.get("explicit_local_material_relationship"))
            cell = subjects[subject].setdefault(
                ticker, {"canonical_count": 0, "row_ids": [], "suspicious": False}
            )
            cell["canonical_count"] += 1
            if len(cell["row_ids"]) < limit:
                cell["row_ids"].append(str(row.get("row_id") or _identity(row)))
            if subject != ticker and not relationship:
                cell["suspicious"] = True
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
    identity_checks = (
        ("run_id", run_id, ("run_id",)),
        ("ticker", None, ("ticker",)),
        ("from_date", from_date, ("from_date", "requested_start")),
        ("to_date", to_date, ("to_date", "requested_end")),
        ("schema_id", SEMANTIC_QA_SCHEMA_ID, ("schema_id",)),
    )
    for ticker in tickers:
        report = reports.get(ticker)
        if report is None:
            continue
        data = _as_mapping(report)
        for key, expected, names in identity_checks:
            if key == "ticker":
                expected = ticker
            value = _first_present(data, names)
            if value is not None and expected is not None and value != expected:
                issues.append(f"{ticker}:{key}_mismatch")
        rows = _evidence_rows(data, ticker)
        for stage, names in _STAGE_FIELDS.items():
            count = _canonical(data, names, None)
            record = _reconcile_record(count, _stage_stats(stage, rows, ticker))
            stages[f"{ticker}:{stage}"] = record
            if record["reconciles"] is False:
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
    """Return a funnel stage record ``{canonical_count, sample_count, omitted_count, status}``."""
    omitted = canonical - sample if canonical is not None and sample is not None else None
    if omitted is not None and omitted < 0:
        omitted = None
    return {
        "canonical_count": canonical,
        "sample_count": sample,
        "omitted_count": omitted,
        "status": "measured" if canonical is not None else "unknown",
    }


def _reconcile_record(canonical: int | None, sample: int | None) -> dict[str, Any]:
    """Return an integrity stage record ``{..., reconciles}`` with the reconciliation verdict."""
    if canonical is None or sample is None:
        return {
            "canonical_count": canonical,
            "sample_count": sample,
            "omitted_count": None,
            "reconciles": None,
        }
    omitted = canonical - sample
    reconciles = omitted >= 0 and sample + omitted == canonical
    return {
        "canonical_count": canonical,
        "sample_count": sample,
        "omitted_count": omitted if omitted >= 0 else None,
        "reconciles": reconciles,
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


def _owner(row: Mapping[str, Any], ticker: str | None = None) -> str | None:
    """Return the normalized evidence owner, deriving from canonical producer fields."""
    explicit = row.get("evidence_owner", row.get("owner", row.get("evidence_subject")))
    if explicit not in (None, ""):
        return str(explicit).strip().upper() or None
    if ticker is None:
        return None
    mentions = _pilot_mentions(row)
    if not mentions:
        return None
    if ticker in mentions:
        return ticker
    if len(mentions) == 1:
        return next(iter(mentions))
    return None


def _evidence_owner(row: Mapping[str, Any], ticker: str) -> str | None:
    """Return the chunk-local evidence owner relative to the signal ticker.

    Producer rows do not emit an explicit ``evidence_owner``; the canonical owner
    is derived from the chunk-local pilot ticker mentions. A row whose chunk-local
    mentions include the signal ticker is owned by that ticker; a row that mentions
    exactly one other pilot ticker is owned by that ticker; otherwise the owner is
    unknown and the matrix records the ``unknown_generic`` subject.
    """
    return _owner(row, ticker)


def _pilot_mentions(row: Mapping[str, Any]) -> set[str]:
    """Return the set of pilot tickers mentioned in the chunk-local evidence."""
    mentioned: set[str] = set()
    nested = row.get("chunk_local_evidence")
    sources = [row.get("ticker_mentions")]
    if isinstance(nested, Mapping):
        sources.append(nested.get("ticker_mentions"))
    for source in sources:
        if isinstance(source, (list, tuple)):
            mentioned.update(str(item).strip().upper() for item in source if str(item).strip())
    return mentioned & set(PILOT_TICKERS)


def _included_in_signal(row: Mapping[str, Any]) -> bool:
    """Return the producer's inclusion decision, tolerating a nested signal object."""
    value = row.get("included_in_signal")
    if value is None and isinstance(row.get("signal"), Mapping):
        value = row["signal"].get("included_in_signal")
    return _bool(value)


def _has_included_flag(row: Mapping[str, Any]) -> bool:
    """Return True when a row exposes an inclusion decision (even if false)."""
    if row.get("included_in_signal") is not None:
        return True
    signal = row.get("signal")
    return isinstance(signal, Mapping) and signal.get("included_in_signal") is not None


def _relevance_decision(row: Mapping[str, Any]) -> str:
    """Return the normalized producer relevance decision for a row."""
    value = row.get("relevance_decision")
    if value is None and isinstance(row.get("relevance"), Mapping):
        value = row["relevance"].get("relevance_decision")
    return str(value).strip().lower() if value not in (None, "") else ""


def _decision_counts(rows: Sequence[Mapping[str, Any]]) -> tuple[int, int, int, bool]:
    """Count accepted/borderline/rejected decisions and whether decisions are observable."""
    accepted = borderline = rejected = 0
    observable = False
    for row in rows:
        decision = _relevance_decision(row)
        if not decision:
            continue
        observable = True
        if decision in _REJECTED_DECISIONS:
            rejected += 1
        elif decision in _BORDERLINE_DECISIONS:
            borderline += 1
        elif decision in _ACCEPTED_DECISIONS:
            accepted += 1
    return accepted, borderline, rejected, observable


def _establishes_empty(report: Mapping[str, Any], rows: Sequence[Mapping[str, Any]]) -> bool:
    """Return True only when the producer canonically established an empty result."""
    if rows:
        return False
    if report.get("row_count") == 0:
        return True
    known = [_canonical(report, names, None) for names in _STAGE_FIELDS.values()]
    return bool(known) and all(value == 0 for value in known)


def _first_present(data: Mapping[str, Any], names: Sequence[str]) -> Any:
    """Return the first present, non-null value among candidate identity keys."""
    for name in names:
        if data.get(name) is not None:
            return data[name]
    return None


def _summary_count(
    report: Mapping[str, Any],
    names: Sequence[str],
    measured: int,
    *,
    observable: bool,
    established_empty: bool,
    ticker: str,
    warnings: list[dict[str, Any]],
) -> int | None:
    """Return a trustworthy summary count or ``null`` plus a warning when unavailable."""
    canonical = _canonical(report, names, None)
    if canonical is not None:
        return canonical
    if observable:
        return measured
    if established_empty:
        return 0
    warnings.append(
        _warning(
            "canonical_count_unavailable",
            ticker,
            names[0],
            "Producer did not expose a trustworthy canonical count.",
        )
    )
    return None


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
