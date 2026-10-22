"""Bounded, local-only preparation helper for the #281 non-live pilot.

This module builds and validates a *proposal* for a fresh post-fix
AAPL/AMD/NVDA/MSFT Layer 1 NLP/HMM diagnostic pilot. It performs no
execution: no network, no subprocess, no Modal/R2/provider calls, no
canonical feature writes, and no launches. An accepted plan or a
machine-complete synthetic export grants no authority of any kind;
semantic acceptance, pilot readiness, live freshness, and execution
remain explicitly false and reserved for the human/domain-owner gates
recorded on issue #281.
"""

from __future__ import annotations

import math
import re
from datetime import datetime, timedelta, timezone
from typing import Any

SOURCE_SHA = "c76d7671a26c07145e863ef6969b21a32ec28781"

# Retired historical acceptance candidate; must never be reused as a
# substitute for a fresh post-fix pilot run.
REJECTED_PACKET_ID = "issue281-post-pr306-current-prod-20260904-v1"

# Stage-only artifact namespace. Nothing here may overwrite canonical
# features/{date}/{ticker}.parquet or full-universe production state.
STAGE_NAMESPACE = "stage/issue281-nonlive-pilot"

REQUIRED_TICKERS = ("AAPL", "AMD", "MSFT", "NVDA")

# Archived five-session candidate window (labels/proposals only; these
# inputs are not revalidated by this helper and do not prove live
# freshness).
CANDIDATE_WINDOW_DATES = (
    "2026-10-05",
    "2026-10-06",
    "2026-10-07",
    "2026-10-08",
    "2026-10-09",
)
WINDOW_START = CANDIDATE_WINDOW_DATES[0]
WINDOW_END = CANDIDATE_WINDOW_DATES[-1]
INPUT_CUTOFF_DATE = WINDOW_END

CANONICAL_FEATURE_PARQUET_PATTERN = "features/{date}/{ticker}.parquet"

# Local preparation-evidence validation literals only. These are the
# relevance_decision values this helper accepts when checking a pilot
# export for machine completeness; they are NOT a new canonical pipeline
# schema and imply no inferred semantics about the pipeline itself.
SUPPORTED_RELEVANCE_DECISIONS = frozenset({"accepted", "borderline", "rejected"})

_SHA_RE = re.compile(r"[0-9a-f]{40}")

# Run ids must be a single safe path component: no separators, no dots,
# no whitespace, no NUL, bounded length. This is validated identically
# by the plan builder and the stage path validator.
_RUN_ID_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]{0,127}")

# Historical input observations from the read-only R2 preflight. They
# are recorded as unverified observations requiring revalidation; this
# helper does not fetch, extend, or backfill anything to conceal them.
_HISTORICAL_INPUT_OBSERVATIONS = (
    {"observation": "missing_raw_news", "date": "2026-10-12"},
    {"observation": "missing_universe", "date": "2026-10-12"},
    {"observation": "missing_macro", "date": "2026-10-09"},
)


class PilotPreparationError(ValueError):
    """Raised when a pilot preparation proposal violates bounded scope."""


def _is_nonblank_str(value: Any) -> bool:
    """Return True only for an actual string with non-whitespace content."""
    return isinstance(value, str) and bool(value.strip())


def _validate_source_sha(source_sha: Any, expected_source_sha: Any) -> None:
    """Require exact lowercase 40-hex SHAs (full-string) that match."""
    for label, value in (
        ("source_sha", source_sha),
        ("expected_source_sha", expected_source_sha),
    ):
        if not isinstance(value, str) or _SHA_RE.fullmatch(value) is None:
            raise PilotPreparationError(
                f"{label} must be a lowercase 40-hex commit SHA, got {value!r}"
            )
    if source_sha != expected_source_sha:
        raise PilotPreparationError(
            "source_sha does not match expected_source_sha; "
            "refusing to plan against an unverified source head"
        )


def _validate_run_id(run_id: Any) -> str:
    """Require a nonblank, safe, single path component run id.

    Rejects traversal, slashes/backslashes, NUL, dots, empty or
    whitespace-bearing values, and non-strings. Returns the run id
    unchanged when valid.
    """
    if not isinstance(run_id, str) or _RUN_ID_RE.fullmatch(run_id) is None:
        raise PilotPreparationError(
            "run id must be a nonblank single safe path component matching "
            f"{_RUN_ID_RE.pattern!r} (no separators, dots, or whitespace), "
            f"got {run_id!r}"
        )
    return run_id


def _validate_generation_time(proposed_generation_time: Any) -> None:
    """Require an aware ISO-8601 time strictly after the cutoff date in UTC."""
    if not isinstance(proposed_generation_time, str):
        raise PilotPreparationError(
            "proposed_generation_time must be an ISO-8601 string, got "
            f"{proposed_generation_time!r}"
        )
    try:
        generated = datetime.fromisoformat(proposed_generation_time)
    except ValueError as exc:
        raise PilotPreparationError(
            f"proposed_generation_time is not ISO-8601: {proposed_generation_time!r}"
        ) from exc
    if generated.tzinfo is None:
        raise PilotPreparationError(
            "proposed_generation_time must be timezone-aware"
        )
    # Timezone-consistent comparison: convert to UTC and require the
    # generation instant to be at or after the first instant after the
    # input cutoff date, rather than comparing local calendar dates.
    cutoff_end = datetime.fromisoformat(
        f"{INPUT_CUTOFF_DATE}T00:00:00+00:00"
    ) + timedelta(days=1)
    if generated.astimezone(timezone.utc) < cutoff_end:
        raise PilotPreparationError(
            "proposed_generation_time must be strictly after input cutoff "
            f"{INPUT_CUTOFF_DATE} (UTC); a generation at or before the "
            "cutoff cannot be a fresh post-fix pilot"
        )


def build_pilot_preparation_plan(
    *,
    source_sha: str,
    expected_source_sha: str,
    proposed_run_id: str,
    proposed_generation_time: str,
    tickers: tuple[str, ...] = REQUIRED_TICKERS,
    window_start: str = WINDOW_START,
    window_end: str = WINDOW_END,
) -> dict[str, Any]:
    """Build a bounded, proposal-only preparation plan for the #281 pilot.

    The returned plan is JSON-serializable and grants no authority: it
    is a labeled proposal for a new replacement run inside the stage
    namespace, never a launch, reservation, or semantic acceptance.
    """
    _validate_source_sha(source_sha, expected_source_sha)
    _validate_run_id(proposed_run_id)
    if proposed_run_id == REJECTED_PACKET_ID:
        raise PilotPreparationError(
            "proposed_run_id reuses the retired rejected packet id; "
            "the replacement pilot must be a new run, not resumed bytes"
        )
    if tuple(tickers) != REQUIRED_TICKERS:
        raise PilotPreparationError(
            f"tickers must be exactly {REQUIRED_TICKERS}, got {tuple(tickers)}"
        )
    if (window_start, window_end) != (WINDOW_START, WINDOW_END):
        raise PilotPreparationError(
            f"window must be exactly {WINDOW_START}..{WINDOW_END}, "
            f"got {window_start}..{window_end}"
        )
    _validate_generation_time(proposed_generation_time)

    planned_artifacts: list[dict[str, Any]] = []
    for date in CANDIDATE_WINDOW_DATES:
        for kind in ("nlp_diagnostics", "hmm_diagnostics"):
            planned_artifacts.append(
                {
                    "path": (
                        f"{STAGE_NAMESPACE}/{proposed_run_id}/{kind}/{date}.json"
                    ),
                    "kind": kind,
                    "date": date,
                }
            )
    planned_artifacts.append(
        {
            "path": f"{STAGE_NAMESPACE}/{proposed_run_id}/export_plan.json",
            "kind": "export_plan",
        }
    )

    # Defense in depth: every generated artifact path is validated
    # against the stage namespace before the plan is returned.
    for artifact in planned_artifacts:
        validate_stage_artifact_path(artifact["path"], run_id=proposed_run_id)

    return {
        "artifact_lineage": "new_replacement",
        "authority": {
            "execution_authorized": False,
            "launch_authorized": False,
            "semantic_acceptance": False,
            "pilot_readiness": False,
            "live_freshness": False,
        },
        "historical_input_observations": [
            {
                "observation": obs["observation"],
                "date": obs["date"],
                "status": "unverified_observation",
                "revalidation_required": True,
            }
            for obs in _HISTORICAL_INPUT_OBSERVATIONS
        ],
        "planned_artifacts": planned_artifacts,
        "prohibited": {
            "canonical_feature_parquet_pattern": CANONICAL_FEATURE_PARQUET_PATTERN,
            "full_universe_output": True,
            "namespace_escape": True,
            "path_traversal": True,
        },
        "proposed_run_id": proposed_run_id,
        "resource_proposal": {
            "proposal_only": True,
            "reservation": False,
            "launch": False,
        },
        "source_sha": source_sha,
        "tickers": list(REQUIRED_TICKERS),
        "timing": {
            "input_cutoff_date": INPUT_CUTOFF_DATE,
            "proposed_generation_time": proposed_generation_time,
            "live_freshness_proven": False,
        },
        "window": {"start": window_start, "end": window_end},
    }


def validate_stage_artifact_path(path: str, *, run_id: str) -> str:
    """Validate that an artifact path stays inside the stage namespace.

    Both the path and the run id must be safe: the run id must be a
    single safe path component, and the path must be relative, free of
    traversal, outside canonical feature output, and strictly inside
    ``{STAGE_NAMESPACE}/{run_id}/``. Returns the path unchanged when
    valid; raises PilotPreparationError otherwise.
    """
    _validate_run_id(run_id)
    if not isinstance(path, str) or not path:
        raise PilotPreparationError(f"artifact path must be a non-empty string: {path!r}")
    if path.startswith("/"):
        raise PilotPreparationError(f"absolute path not allowed: {path!r}")
    if any(component in ("", ".", "..") for component in path.split("/")):
        raise PilotPreparationError(
            f"empty, dot, or traversal path components not allowed: {path!r}"
        )
    if path.startswith("features/"):
        raise PilotPreparationError(
            f"canonical feature output path is prohibited: {path!r}"
        )
    prefix = f"{STAGE_NAMESPACE}/{run_id}/"
    if not path.startswith(prefix) or len(path) <= len(prefix):
        raise PilotPreparationError(
            f"artifact path escapes stage namespace {prefix!r}: {path!r}"
        )
    return path


def _no_grants() -> dict[str, bool]:
    """Return a fresh all-false grant mapping for an assessment result."""
    return {
        "semantic_acceptance": False,
        "live_freshness": False,
        "execution_authority": False,
        "pilot_readiness": False,
    }


def _assess_export_identity(
    identity: Any, *, plan: dict[str, Any], rejections: list[str]
) -> None:
    """Validate the export identity block against the plan."""
    if not isinstance(identity, dict) or not all(
        key in identity for key in ("run_id", "source_sha", "window", "tickers")
    ):
        rejections.append("incomplete_export_identity")
        return
    run_id = identity["run_id"]
    if run_id == REJECTED_PACKET_ID:
        rejections.append("historical_packet_substitution")
    if run_id != plan["proposed_run_id"]:
        rejections.append("mismatched_run_id")
    if identity["source_sha"] != plan["source_sha"]:
        rejections.append("mismatched_source_sha")
    expected_window = {"start": plan["window"]["start"], "end": plan["window"]["end"]}
    if identity["window"] != expected_window:
        rejections.append("mismatched_window")
    tickers_value = identity["tickers"]
    if not isinstance(tickers_value, list) or any(
        not _is_nonblank_str(ticker) for ticker in tickers_value
    ):
        rejections.append("malformed_identity_tickers")
    elif len(set(tickers_value)) != len(tickers_value):
        rejections.append("duplicate_ticker_entries")
    elif set(tickers_value) != set(plan["tickers"]):
        rejections.append("incomplete_ticker_coverage")


def _assess_chunk(chunk: Any, *, ticker: str, ticker_rejections: set[str]) -> None:
    """Validate one chunk object for typed machine completeness."""
    if not isinstance(chunk, dict):
        ticker_rejections.add(f"malformed_chunk:{ticker}")
        return
    if not _is_nonblank_str(chunk.get("text")):
        ticker_rejections.add(f"missing_chunk_text:{ticker}")
    if "relevance_decision" not in chunk:
        ticker_rejections.add(f"missing_relevance_decision:{ticker}")
    elif chunk["relevance_decision"] not in SUPPORTED_RELEVANCE_DECISIONS:
        ticker_rejections.add(f"invalid_relevance_decision:{ticker}")
    if "reason_codes" not in chunk:
        ticker_rejections.add(f"missing_reason_codes:{ticker}")
    else:
        reason_codes = chunk["reason_codes"]
        if (
            not isinstance(reason_codes, list)
            or not reason_codes
            or any(not _is_nonblank_str(code) for code in reason_codes)
        ):
            ticker_rejections.add(f"invalid_reason_codes:{ticker}")
    if "included_in_signal" not in chunk:
        ticker_rejections.add(f"missing_inclusion_flag:{ticker}")
    elif not isinstance(chunk["included_in_signal"], bool):
        ticker_rejections.add(f"invalid_inclusion_flag:{ticker}")
    if "effective_contribution" not in chunk:
        ticker_rejections.add(f"missing_effective_contribution:{ticker}")
    else:
        contribution = chunk["effective_contribution"]
        if (
            isinstance(contribution, bool)
            or not isinstance(contribution, (int, float))
            or not math.isfinite(contribution)
        ):
            ticker_rejections.add(f"invalid_effective_contribution:{ticker}")


def _assess_article_context(
    article_context: Any, *, ticker: str, rejections: list[str]
) -> None:
    """Validate the per-ticker article context mapping."""
    if article_context is None:
        rejections.append(f"missing_article_context:{ticker}")
        return
    if not isinstance(article_context, dict):
        rejections.append(f"malformed_article_context:{ticker}")
        return
    if not article_context:
        rejections.append(f"missing_article_context:{ticker}")
        return
    for article_id, article in article_context.items():
        if (
            not _is_nonblank_str(article_id)
            or not isinstance(article, dict)
            or not _is_nonblank_str(article.get("headline"))
            or not _is_nonblank_str(article.get("source"))
        ):
            rejections.append(f"invalid_article_context_entry:{ticker}")
            return


def assess_export_evidence(
    export: dict[str, Any], *, plan: dict[str, Any]
) -> dict[str, Any]:
    """Assess an export payload for machine completeness against a plan.

    Machine completeness only proves required fields are present, typed,
    and consistent with the plan; it grants no semantic acceptance, live
    freshness, execution authority, or pilot readiness. Malformed
    containers produce bounded rejection diagnostics, never exceptions.
    """
    if not isinstance(export, dict):
        return {
            "grants": _no_grants(),
            "machine_complete": False,
            "rejections": ["malformed_export_container"],
        }

    rejections: list[str] = []

    _assess_export_identity(
        export.get("export_identity"), plan=plan, rejections=rejections
    )

    fallback = export.get("cached_fallback_used")
    if fallback is True:
        rejections.append("cached_fallback_used")
    elif fallback is not False:
        rejections.append("missing_cached_fallback_proof")

    ticker_exports = export.get("tickers")
    if not isinstance(ticker_exports, dict):
        rejections.append("malformed_tickers_block")
        ticker_exports = {}
    for ticker in plan["tickers"]:
        block = ticker_exports.get(ticker)
        if not isinstance(block, dict):
            rejections.append(f"missing_ticker_export:{ticker}")
            continue
        if "chunks" not in block:
            rejections.append(f"missing_chunks:{ticker}")
            chunks: list[Any] = []
        else:
            chunks_value = block["chunks"]
            if not isinstance(chunks_value, list):
                rejections.append(f"malformed_chunks:{ticker}")
                chunks = []
            else:
                chunks = chunks_value
                if not chunks:
                    rejections.append(f"missing_chunks:{ticker}")
        ticker_rejections: set[str] = set()
        for chunk in chunks:
            _assess_chunk(chunk, ticker=ticker, ticker_rejections=ticker_rejections)
        rejections.extend(sorted(ticker_rejections))
        _assess_article_context(
            block.get("article_context"), ticker=ticker, rejections=rejections
        )

    return {
        "grants": _no_grants(),
        "machine_complete": not rejections,
        "rejections": rejections,
    }
