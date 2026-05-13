# Layer 1 Audit Dashboard Backend

## Purpose

`app/lab/data_pipelines/build_layer1_feature_audit_dashboard.py` prepares the
read-only backend payload for the Layer 0/1 feature audit dashboard. It does
not write to R2 or modify `features/layer1/*.parquet`. It only reads stored
Layer 1 per-ticker histories, computes visualization inputs locally, and
writes local report artifacts under `artifacts/reports/diagnostics/` by default.

This backend is intentionally scoped to Layer 0/1 audit visibility only:
- feature completeness heatmap cells
- feature-family status cards
- raw-vs-computed market feature spot checks from Layer 0 OHLCV
- formula audit cards for selected deterministic market features
- null-rate summaries by feature and family
- numeric outlier and range-violation records

It does not load or depend on Layer 2 scores, training data, or inference
artifacts.

## Command

```bash
./.venv/bin/python app/lab/data_pipelines/build_layer1_feature_audit_dashboard.py \
    --from-date 2024-05-06 \
    --to-date 2024-05-08 \
    --tickers AAPL,MSFT \
    --output-dir artifacts/reports/diagnostics
```

Optional flags:
- `--run-id`: override the report identifier used in filenames

## Outputs

For a run id of `layer1-audit-dashboard-2024-05-06-to-2024-05-08`, the command
writes:

- `artifacts/reports/diagnostics/layer1_feature_audit_dashboard_layer1-audit-dashboard-2024-05-06-to-2024-05-08_2024-05-06_to_2024-05-08.json`
- `artifacts/reports/diagnostics/layer1_feature_audit_dashboard_layer1-audit-dashboard-2024-05-06-to-2024-05-08_2024-05-06_to_2024-05-08.txt`

The JSON file is the durable backend payload for a future web/UI surface. The
text file is a compact operator summary.

## How To Read The Report

- `selection_rows`: the `(date, ticker)` history rows loaded from the selected
  date window.
- `heatmap_cells`: per-feature completeness/validity cells for each selected
  row. `pass` means present and valid, `warn` means nullable/optional missingness
  or uncataloged features, and `fail` means invalid values or missing required
  features.
- `feature_null_summaries`: missing/null/invalid rates for each feature across
  the selected rows.
- `family_status_summaries`: aggregated card data for Market, Macro/Context,
  Fundamentals/Earnings, NLP/Topic, and Regime. These summaries also carry the
  family-level missing/null/invalid rates used by null-rate charts.
- `outlier_records`: numeric exceptions for two rule types:
  - `range_violation`: the value breached the canonical catalog min/max rule
  - `distribution_outlier`: the value fell outside the dashboard's
    `Q1 - 3*IQR` / `Q3 + 3*IQR` fence, computed only when at least four valid
    observations exist for that feature in the selected window
- `spot_check_records`: point-in-time-safe raw-vs-stored comparisons for
  deterministic market features currently including `returns_1d`,
  `returns_5d`, `realized_vol_21d`, `volume_ratio_20`, and `rsi_14`. Each
  record includes the raw OHLCV inputs used, recomputed value, stored Layer 1
  value, absolute/relative difference, tolerance, status, and explicit missing
  reason when the feature could not be recomputed.
- `formula_audit_cards`: human-readable calculation payloads for the same
  deterministic market features. These cards show the exact formula text, the
  concrete numbers substituted for the selected `(date, ticker)`, and the
  point-in-time note describing the latest source bar used.

## Exit Code

- `0` when no family status resolved to `fail` and no market spot check
  resolved to `fail`
- `1` when at least one family status or market spot check resolved to `fail`
