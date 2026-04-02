# Current Project State

Last updated: 2026-04-02 UTC

## Current Milestone
- **Milestone 2 — First Operational AI Paper-Trading Loop**

## Active Issues
- **#12 — First serious XGBoost predictive model**
  - Status: blocked on current Hugging Face Space long-run observability / completion reporting path
  - Owner: `trading-quant-researcher`
  - Current focus: resume the cloud dataset -> train -> export path using the new job/control-plane contract from Issue #19 instead of the old fragile direct Gradio run flow
- **#13 — Predictive calibration and validation gate**
  - Status: in-progress
  - Owner: `trading-backtest-validator`
  - Current stance: current candidate rejected for promotion; waiting on a stronger real Issue #12 candidate
- **#14 — Online Oracle refresh for XGBoost model**
  - Status: in-progress
  - Owner: `trading-executor-reporter`
  - Current stance: packaging/runtime-selection hardening done locally; final acceptance waits on an accepted #12 artifact and live endpoint validation

## Next Issue
- **Primary next issue:** #12
- With #19 completed, the next active work is to apply the new HF Jobs + durable run-status path to unblock the real Issue #12 cloud run.
- After #12 produces an accepted candidate, re-run #13 review and continue #14 live cloud/inference integration.

## Current Blockers
- #12 is currently blocked on replacing the fragile direct Space-trigger path with the new durable HF Jobs / control-plane execution path from #19.
- HF Space build/runtime has been fixed to Python 3.11 and reaches RUNNING, but old long-running Gradio session observability was insufficient.
- No Pi-hosted AI-heavy training/build work is allowed.

## Completed Issues
- #19 HF Jobs training control plane and durable run-status contract
- #1 Predictive dataset builder
- #2 First predictive model training scaffold
- #3 First artifact bundle definition
- #4 Hugging Face inference contract implementation
- #5 Pi Hugging Face connectivity test
- #6 Policy contract scaffold
- #7 Validation metrics scaffold
- #8 Rebalance translation from target weights
- #9 Deploy custom Cloud Oracle on Hugging Face
- #10 FinBERT feature pipeline for ticker-day sentiment
- #11 Expanded market and context feature pipeline
- #18 Wire Hugging Face Space training environment

## Latest Model / Artifact / Runtime State
- Latest real non-smoke local candidate produced before cloud-only correction:
  - report: `reports/pipeline/issue12_cloud_pipeline_20260402T043343Z.json`
  - artifact: `data/processed/models/issue12_xgb_baseline_20260402T043343Z.artifact.json`
  - metrics: `data/processed/models/issue12_xgb_baseline_20260402T043343Z.metrics.json`
  - diagnostics: `data/processed/models/issue12_xgb_baseline_20260402T043343Z.diagnostics.json`
  - bundle: `artifacts/bundles/predictive_signal_bundle_v1_20260402T043343Z.bundle.json`
- Quality of that local real candidate was weak (near-random AUC), so it is not promotion-ready.
- Cloud handoff status:
  - HF Space repo: `FunkMonk87/AI-Stock-Trader-Lab`
  - fresh snapshot handoff uploaded to the Space repo
  - Space package synced
  - Space runtime moved to Python 3.11 and reached RUNNING
  - durable HF job contract and background job runner now exist for Space control-plane behavior
  - canonical model repo contract and endpoint-readiness manifests now exist for approved artifact flow

## Important Decisions
- AI-heavy training, dataset building, and model execution must run in the cloud, not on the Raspberry Pi.
- The Hugging Face architecture is now split correctly: Space = control plane/UI only, HF Jobs = long-running training runner, HF model repo = canonical artifact registry, HF Inference Endpoint = Oracle for the Pi.
- The existing Hugging Face Space remains the control-plane entrypoint for Issue #12 work.
- PM status changes are never terminal by themselves; after every PM action, immediately continue with the next operational step in the same cycle.
- Valid user-facing update boundaries are only:
  1. code-change boundary with concrete files + checks
  2. state-transition boundary with next operational step already started
  3. blocker boundary needing a human answer now
  4. completion boundary
  5. review boundary with a real decision
- Issue completion is only valid when all are done:
  - code committed and pushed to GitHub
  - persistent repo memory/docs updated
  - concise GitHub issue completion summary written
  - next active issue recorded

## Re-anchor Checklist
On restart / context loss, read in this order:
1. `agents/SOUL.md`
2. `agents/AGENTS.md`
3. `agents/agent_ownership.md`
4. `memory/current_project_state.md`
5. recent daily memory files
