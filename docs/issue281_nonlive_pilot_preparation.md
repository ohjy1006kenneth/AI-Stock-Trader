# Issue #281 — Non-live pilot preparation helper (local-only)

Status: bounded local preparation only. No pilot execution, launch, network,
Modal/R2/provider calls, canonical feature writes, backfill, brokerage,
deployment, publication, merge, or semantic acceptance is performed or
authorized by this work. All original issue-level holds remain intact.

## Context

Issue #281 was REJECTED for semantic acceptance on 2026-10-13 (issue comment
6156849004). The historical packet `issue281-post-pr306-current-prod-20260904-v1`
is retired as an acceptance candidate. The required replacement is a newly
executed AAPL/AMD/MSFT/NVDA post-fix pilot with actual chunk text, relevance
decisions/reason codes, signal inclusion flags, effective contribution, and
separate article context.

This change adds only a stdlib-only, local-only helper module
(`app/lab/data_pipelines/prepare_issue281_pilot.py`), its focused unit tests
(`tests/unit/test_prepare_issue281_pilot.py`), and this document. It is a new
replacement draft, not resumed previously reviewed bytes.

## What the helper does

- `build_pilot_preparation_plan(...)` builds a JSON-serializable,
  proposal-only plan for a bounded stage-only pilot:
  - requires an exact lowercase 40-hex source SHA matching the expected SHA;
  - refuses to reuse the retired rejected packet id as a run id;
  - pins tickers to exactly AAPL/AMD/MSFT/NVDA;
  - pins the candidate window to the archived five sessions
    2026-10-05..2026-10-09;
  - requires the proposed generation time to be strictly after the input
    cutoff date 2026-10-09;
  - labels the input cutoff vs. generation time and never claims live
    freshness;
  - records the historical input observations (missing raw news and universe
    for 2026-10-12, missing macro for 2026-10-09) as unverified observations
    requiring revalidation — they are labels/proposals, not revalidated data;
  - plans artifacts only under `stage/issue281-nonlive-pilot/<run_id>/`
    (`nlp_diagnostics`, `hmm_diagnostics` per date, plus an `export_plan`),
    and explicitly prohibits canonical `features/{date}/{ticker}.parquet`
    output, full-universe output, namespace escape, and path traversal;
  - marks resources as proposal-only (no reservation, no launch) and grants
    no authority of any kind.

- `validate_stage_artifact_path(path, run_id=...)` rejects absolute paths,
  `..` traversal, canonical feature paths, and any escape from the stage
  namespace; it returns the path unchanged when valid.

- `assess_export_evidence(export, plan=...)` checks an export payload for
  machine completeness against a plan: export identity (run id, source SHA,
  window, ticker coverage), cached-fallback use, per-ticker chunk text,
  relevance decision, reason codes, inclusion flag, effective contribution,
  and article context. The local export contract requires unique ticker arrays;
  nonblank string chunk text; a relevance decision exactly `accepted`,
  `borderline`, or `rejected`; nonempty lists of nonblank reason-code strings;
  a real boolean inclusion flag; finite numeric (not boolean) contribution;
  explicit `cached_fallback_used: false`; and a nonempty article-context map
  whose entries have nonblank string `headline` and `source`. Invalid/malformed
  evidence yields rejection diagnostics rather than completeness. Machine
  completeness grants nothing: semantic acceptance, live freshness, execution
  authority, and pilot readiness all remain false.

  Assessment validates the plan identity itself before comparing export values:
  lowercase 40-hex source-SHA format, safe non-retired run id, exact unique
  AAPL/AMD/MSFT/NVDA ticker list, and the fixed 2026-10-05..2026-10-09 window.
  The SHA comparison is a caller-provided consistency check only; it does not
  independently verify Git provenance or authorize execution. Malformed plans
  return `malformed_assessment_plan` with every grant false.

## Local API/export shape example

This illustrates only the helper's local validation contract; the values are
synthetic, not pipeline output or evidence:

```python
plan = build_pilot_preparation_plan(
    source_sha="c76d7671a26c07145e863ef6969b21a32ec28781",
    expected_source_sha="c76d7671a26c07145e863ef6969b21a32ec28781",
    proposed_run_id="example-replacement-v1",
    proposed_generation_time="2026-10-20T12:00:00+00:00",
)
export = {
    "export_identity": {
        "run_id": plan["proposed_run_id"],
        "source_sha": plan["source_sha"],
        "window": plan["window"],
        "tickers": ["AAPL", "AMD", "MSFT", "NVDA"],  # unique array
    },
    "cached_fallback_used": False,  # explicit proof field; not missing/null
    "tickers": {
        ticker: {
            "chunks": [{
                "text": "Synthetic nonblank chunk text",
                "relevance_decision": "borderline",  # accepted|borderline|rejected
                "reason_codes": ["direct_target_event"],
                "included_in_signal": True,  # actual bool
                "effective_contribution": 0.25,  # finite number, never bool
            }],
            "article_context": {
                "article-1": {"headline": "Synthetic headline", "source": "synthetic"}
            },
        }
        for ticker in plan["tickers"]
    },
}
assessment = assess_export_evidence(export, plan=plan)
assert assessment["machine_complete"]  # no acceptance/readiness is implied
assert all(value is False for value in assessment["grants"].values())
```

The plan's resource fields are proposal labels only, not an allocation or
reservation.

## What the helper does not do

- No execution, launch, subprocess, network, Modal/R2/provider calls, data or
  model downloads, canonical feature writes, daily orchestration, backfill,
  brokerage, viewer changes, deployments, publication, merge, or semantic
  acceptance.
- No clearing of legacy holds. Human acceptance remains false; pilot
  readiness remains false until actual output completeness, freshness,
  fallback status, and semantic checks pass on a genuinely fresh post-fix
  pilot, followed by human review of the new evidence.

## Verification

Focused tests (stdlib unittest, direct file execution):

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=. python3 -B tests/unit/test_prepare_issue281_pilot.py -v
```

Result at this revision: 68 tests, all OK.

Full-suite pytest, `ruff check .`, and CI gates were unavailable in the
system interpreter for this task and remain explicitly pending rather than
fabricated; `git diff --check` was run and passed.
