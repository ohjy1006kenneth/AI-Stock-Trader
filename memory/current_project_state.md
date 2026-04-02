# Current Project State

Last updated: 2026-04-02 UTC

## Current Milestone
- **Milestone 2 — First Operational AI Paper-Trading Loop** ✅ completed

## Active Issues
- **#14 — Online Oracle refresh for XGBoost model**
  - Status: in-progress
  - Owner: `trading-executor-reporter`
  - Current stance: packaging/runtime-selection hardening done locally; final acceptance waits on an accepted #12 artifact and live endpoint validation

## Next Issue
- **Primary next issue:** #20
- Milestone 2 loop acceptance is complete. The remaining open follow-up is #20, which stays blocked until a future model candidate passes validation/promotion and is ready for live Oracle refresh.

## Current Blockers
- #20 remains blocked until a future candidate passes validation/promotion and is approved for live Oracle refresh.
- No Pi-hosted AI-heavy training/build work is allowed.

## Completed Issues
- #17 Daily autonomous paper loop acceptance
- #16 Risk engine hard-rule layer
- #15 First operational decision layer pre-RL
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
- Prediction and decision are now explicitly separated in the cloud path: predictive model produces signal/confidence, then the non-RL constrained long-only policy layer converts those into target weights.
- A hard-rule risk layer now sits between oracle response and execution, enforcing configured caps on per-name weight, total positions, and cash buffer before paper orders are translated/submitted.
- The daily autonomous paper loop is now acceptance-ready end-to-end: preflight -> S&P snapshot refresh -> universe build -> market/fundamental fetch -> cloud oracle call -> hard-risk layer -> paper execution -> reporting.
- HF oracle calls now retry transient upstream failures (`429/500/502/503/504` and network errors) with backoff / Retry-After support.
- Reporting semantics distinguish `PENDING` accepted orders from actually `EXECUTED` filled orders.
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
