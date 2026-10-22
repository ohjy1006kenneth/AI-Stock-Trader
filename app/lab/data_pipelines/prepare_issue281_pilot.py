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

import re
from datetime import datetime
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

_SHA_RE = re.compile(r"^[0-9a-f]{40}$")

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


def _validate_source_sha(source_sha: str, expected_source_sha: str) -> None:
    """Require an exact lowercase 40-hex SHA matching the expected value."""
    if not _SHA_RE.match(source_sha):
        raise PilotPreparationError(
            f"source_sha must be a lowercase 40-hex commit SHA, got {source_sha!r}"
        )
    if source_sha != expected_source_sha:
        raise PilotPreparationError(
            "source_sha does not match expected_source_sha; "
            "refusing to plan against an unverified source head"
        )


def _validate_generation_time(proposed_generation_time: str) -> None:
    """Require generation time to be strictly after the input cutoff date."""
    try:
        generated = datetime.fromisoformat(proposed_generation_time)
    except ValueError as exc:
        raise PilotPreparationError(
            f"proposed_generation_time is not ISO-8601: {proposed_generation_time!r}"
        ) from exc
    cutoff = datetime.fromisoformat(f"{INPUT_CUTOFF_DATE}T00:00:00+00:00")
    if generated.tzinfo is None:
        raise PilotPreparationError(
            "proposed_generation_time must be timezone-aware"
        )
    if generated.date() <= cutoff.date():
        raise PilotPreparationError(
            "proposed_generation_time must be strictly after input cutoff "
            f"{INPUT_CUTOFF_DATE}; a generation at or before the cutoff "
            "cannot be a fresh post-fix pilot"
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
    if proposed_run_id == REJECTED_PACKET_ID:
        raise PilotPreparationError(
            "proposed_run_id reuses the retired rejected packet id; "
            "the replacement pilot must be a new run, not resumed bytes"
        )
    if not proposed_run_id:
        raise PilotPreparationError("proposed_run_id must be non-empty")
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

    Returns the path unchanged when valid; raises PilotPreparationError
    for absolute paths, traversal, canonical feature paths, or any
    namespace escape.
    """
    if path.startswith("/"):
        raise PilotPreparationError(f"absolute path not allowed: {path!r}")
    if ".." in path.split("/"):
        raise PilotPreparationError(f"path traversal not allowed: {path!r}")
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


def assess_export_evidence(
    export: dict[str, Any], *, plan: dict[str, Any]
) -> dict[str, Any]:
    """Assess an export payload for machine completeness against a plan.

    Machine completeness only proves required fields are present and
    consistent with the plan; it grants no semantic acceptance, live
    freshness, execution authority, or pilot readiness.
    """
    rejections: list[str] = []

    identity = export.get("export_identity")
    if not isinstance(identity, dict) or not all(
        key in identity for key in ("run_id", "source_sha", "window", "tickers")
    ):
        rejections.append("incomplete_export_identity")
    else:
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
        if set(identity["tickers"]) != set(plan["tickers"]):
            rejections.append("incomplete_ticker_coverage")

    if export.get("cached_fallback_used"):
        rejections.append("cached_fallback_used")

    ticker_exports = export.get("tickers")
    if not isinstance(ticker_exports, dict):
        ticker_exports = {}
    for ticker in plan["tickers"]:
        block = ticker_exports.get(ticker)
        if not isinstance(block, dict):
            rejections.append(f"missing_ticker_export:{ticker}")
            continue
        chunks = block.get("chunks")
        if not chunks:
            rejections.append(f"missing_chunks:{ticker}")
            chunks = []
        ticker_rejections: set[str] = set()
        for chunk in chunks:
            if not isinstance(chunk, dict):
                ticker_rejections.add(f"malformed_chunk:{ticker}")
                continue
            if not chunk.get("text"):
                ticker_rejections.add(f"missing_chunk_text:{ticker}")
            if "relevance_decision" not in chunk:
                ticker_rejections.add(f"missing_relevance_decision:{ticker}")
            if not chunk.get("reason_codes"):
                ticker_rejections.add(f"missing_reason_codes:{ticker}")
            if "included_in_signal" not in chunk:
                ticker_rejections.add(f"missing_inclusion_flag:{ticker}")
            if "effective_contribution" not in chunk:
                ticker_rejections.add(f"missing_effective_contribution:{ticker}")
        rejections.extend(sorted(ticker_rejections))
        if not block.get("article_context"):
            rejections.append(f"missing_article_context:{ticker}")

    return {
        "grants": {
            "semantic_acceptance": False,
            "live_freshness": False,
            "execution_authority": False,
            "pilot_readiness": False,
        },
        "machine_complete": not rejections,
        "rejections": rejections,
    }
