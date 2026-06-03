# Layer 1 Feature Audit

## Purpose

`app/lab/data_pipelines/audit_layer1_features.py` is an operator-facing audit for
Layer 1 feature correctness. It does not rebuild production histories or write to
R2. It reads existing Layer 0 and Layer 1 artifacts, recomputes deterministic
feature branches, and writes local reports under `artifacts/reports/diagnostics/`
by default.

The audit focuses on Layer 0/1 correctness only:
- market features from raw OHLCV and benchmark OHLCV
- context features from raw fundamentals and macro archives
- NLP topic aggregates from stored topic-label artifacts
- NLP sentiment aggregates from stored scored-news artifacts
- regime broadcast features from stored Layer 1.5 regime artifacts
- schema/catalog checks for stored `FeatureRecord` histories
- point-in-time checks for news timestamps, fundamentals `availability_date`, and
  macro `realtime_start`

## Command

```bash
./.venv/bin/python app/lab/data_pipelines/audit_layer1_features.py \
    --as-of-date 2026-04-10 \
    --tickers AAPL,MSFT \
    --output-dir artifacts/reports/diagnostics
```

Optional flags:
- `--run-id`: override the report identifier used in filenames
- `--benchmark-ticker`: override the market benchmark, default `SPY`

## Outputs

For a run id of `layer1-audit-2026-04-10`, the audit writes:

- `artifacts/reports/diagnostics/layer1_feature_audit_layer1-audit-2026-04-10_2026-04-10.json`
- `artifacts/reports/diagnostics/layer1_feature_audit_layer1-audit-2026-04-10_2026-04-10.txt`

The JSON report is the durable machine-readable artifact. The text file is the
operator summary with PASS/WARN/FAIL counts and the key findings.

## How To Read The Report

- `history`: confirms the target `features/layer1/{TICKER}.parquet` row exists.
- `catalog`: validates required feature names, branch ownership, types, nullability,
  and configured value ranges.
- `market` / `context`: compare stored history values to direct recomputation from
  Layer 0 raw archives.
- `topics` / `sentiment` / `regime`: compare stored history values to deterministic
  recomputation from the latest completed branch artifacts for the audited date.
- `leakage`: records the date-boundary checks used for raw news timestamps,
  fundamentals `availability_date`, macro `realtime_start`, and regime
  `train_end_date`.

Interpretation:
- `PASS`: the stored history matched the recomputed branch or passed the structural check.
- `WARN`: the audit could not verify a branch because an optional artifact was missing.
- `FAIL`: the stored history, branch artifact, or point-in-time boundary was inconsistent.

Exit code:
- `0` when the audit produced no failures
- `1` when at least one failure was detected

## AAPL Accuracy Pilot Before Broad Backfill

Before starting the broad point-in-time historical Layer 1 backfill, run the
AAPL-only accuracy workflow:

```bash
HOME=/home/juyoungoh ./.venv/bin/python app/lab/data_pipelines/run_aapl_layer1_accuracy.py \
    --run-id layer1-aapl-accuracy-<window>-v1 \
    --from-date <from> \
    --to-date <to> \
    --layer0-run-id <layer0-run-id> \
    --allow-layer0-manifest-date-range \
    --run-layer1
```

The workflow is intentionally limited to `AAPL`. It validates the date-first
pilot shards such as `features/{YYYY-MM-DD}/AAPL.parquet`, writes a local JSON
report under `artifacts/reports/diagnostics/`, and uploads the durable report to:

`artifacts/reports/diagnostics/layer1_aapl_feature_accuracy_{run_id}_{from}_to_{to}.json`

The configurable parameters live in `config/layer1_aapl_accuracy.json`:
- target forward-return horizon
- quality thresholds for feature rows, required-feature null rate, label pairs, and
  candidate correlation
- market parameter candidates for return, volatility, and volume-ratio windows

Use `recommendation_for_issue_202` in the report as the operator handoff:
- `proceed`: AAPL pilot evidence is acceptable for starting #202.
- `needs_human_review`: review the report before deciding whether to tune parameters.
- `do_not_proceed`: keep #202 blocked until missing shards or catalog failures are fixed.
