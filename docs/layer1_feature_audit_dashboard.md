# Layer 1 Audit Dashboard Backend And UI

## Purpose

`app/lab/data_pipelines/build_layer1_feature_audit_dashboard.py` prepares the
read-only backend payload for the Layer 0/1 feature audit dashboard, and
`python -m app.lab.feature_audit_dashboard` serves the live local web UI for
the same payload. Both entrypoints are read-only: they do not write to R2 or
modify `features/layer1/*.parquet`. They only read stored Layer 1 per-ticker
histories, compute visualization inputs locally, and optionally write local
report artifacts under `artifacts/reports/diagnostics/`.

This backend is intentionally scoped to Layer 0/1 audit visibility only:
- feature completeness heatmap cells
- feature-family status cards
- raw-vs-computed market feature spot checks from Layer 0 OHLCV
- formula audit cards for selected deterministic market features
- null-rate summaries by feature and family
- numeric outlier and range-violation records

It does not load or depend on Layer 2 scores, training data, or inference
artifacts.

## Live UI

Launch the local dashboard from the repo root:

```bash
HOME=/home/juyoungoh ./.venv/bin/python -m app.lab.feature_audit_dashboard \
    --from-date 2024-05-06 \
    --to-date 2024-05-08 \
    --tickers AAPL,MSFT \
    --host 127.0.0.1 \
    --port 8765
```

Then open `http://127.0.0.1:8765/`.

The UI is intentionally scoped to Layer 0/1 QA only. It does not expose Layer 2,
training, inference, portfolio, risk, or execution panels.

### Required env/config

The UI reads the same archives as the backend builder and uses `R2Writer()`:

- Real R2 mode: set `R2_ENDPOINT_URL`, `R2_ACCESS_KEY_ID`,
  `R2_SECRET_ACCESS_KEY`, and `R2_BUCKET_NAME` (or provide them through
  `config/r2.env` as used elsewhere in the repo)
- Local mock mode: omit those env vars and the dashboard will read from the
  default local mock root `data/runtime/r2_mock/`
- Explicit local mock root: pass `--local-root <path>` to force a specific
  local fixture or mock-R2 directory, even when real R2 credentials are present

The dashboard never mutates those stores. It only reads:
- `features/layer1/{TICKER}.parquet`
- `raw/prices/{ticker}.parquet` for deterministic market-feature spot checks

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

## PASS / WARN / FAIL Interpretation

The live UI uses the same status semantics everywhere:

- `PASS`: the selected value or family is present, valid, and matches the
  audit rule or recomputation tolerance
- `WARN`: review is required, but the issue is not a hard mismatch. Typical
  causes are optional feature absence, nullable values, skipped recomputation,
  missing raw OHLCV for a spot check, or another non-fatal data-quality gap
- `FAIL`: the dashboard found a hard correctness problem such as a missing
  required feature, an invalid stored value, or a stored-vs-computed mismatch

How to read the main panels:

- Feature-family status panels:
  `FAIL` means at least one family member has required missingness, invalid
  values, or flagged outliers; `WARN` means only optional missingness/nulls or
  softer quality issues are present; `PASS` means the family is clean for the
  selected window
- Feature completeness heatmap:
  each cell is one stored `(date, ticker, feature)` value. The cell status is
  about completeness and catalog validity only; raw-vs-computed mismatches are
  shown separately in the spot-check chart and formula cards
- Formula audit cards:
  `PASS` means the stored Layer 1 market feature matches the recomputed value
  from Layer 0 OHLCV within tolerance; `WARN` means the recomputation was
  skipped because the raw source window was unavailable or malformed; `FAIL`
  means the stored value diverged from the recomputed value or was invalid
- Outlier scatter/table:
  `range_violation` flags invalid-range issues against canonical min/max rules,
  while `distribution_outlier` flags extreme values outside the dashboard's
  `Q1 - 3*IQR` / `Q3 + 3*IQR` fence

## Exit Code

- `0` when no family status resolved to `fail` and no market spot check
  resolved to `fail`
- `1` when at least one family status or market spot check resolved to `fail`
