# Current Project State

Last updated: 2026-04-02 UTC

## Current Milestone
- **Milestone 2 — First Operational AI Paper-Trading Loop**

## Active Issues
- **#14 — Online Oracle refresh for XGBoost model**
  - Status: in-progress
  - Owner: `trading-executor-reporter`
  - Current stance: packaging/runtime-selection hardening done locally; final acceptance waits on an accepted #12 artifact and live endpoint validation

## Next Issue
- **Primary next issue:** #15
- With #14 split and the integration-hardening portion completed, the next dependency-ready implementation issue is #15 while the new live-refresh follow-up remains blocked on a future promoted candidate.

## Current Blockers
- The current model candidate is not promotion-worthy.
- A new follow-up issue for live Oracle refresh will remain blocked until a future candidate passes validation/promotion.
- No Pi-hosted AI-heavy training/build work is allowed.

## Completed Issues
- #14 Online Oracle refresh for XGBoost model (integration-hardening portion completed; live-refresh follow-up split out)
- #13 Predictive calibration and validation gate (completed with reject / do not promote decision for current candidate)
- #12 First serious XGBoost predictive model
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
- Canonical cloud artifact path is now established at HF model repo `FunkMonk87/ai-stock-trader-oracle`.
- Published canonical bundle/manifests for current candidate:
  - `bundles/issue12_xgb_date_split_fix_bundle_20260402T200937Z.bundle.json`
  - `manifests/bundles/issue12_xgb_date_split_fix_bundle_20260402T200937Z.manifest.json`
  - `manifest.json`
  - `channels/approved/manifest.json`
  - `endpoints/oracle/ready.json`
- Cloud-source snapshot commit used for validation:
  - `6f39dba2f5bf8a703805af47da77c8e9341537e1`
- Validation outcome for the current candidate remains reject / do not promote:
  - fails minimum backtest days
  - fails sharpe threshold
  - lacks walk-forward window
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
- A successful real cloud Issue #12 run is sufficient to move #12 out of execution work and into validation/review; model quality/promotion is decided by Issue #13, not by holding #12 open forever.
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
