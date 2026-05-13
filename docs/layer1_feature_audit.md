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
