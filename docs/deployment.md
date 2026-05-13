# Deployment

This document describes deployment surfaces and responsibilities.

## Surfaces

- app/lab: cloud training and packaging jobs
- app/cloud: hosted inference service
- app/pi: edge runtime and execution process (containerized on Pi)

## External dependency roles

- Wikipedia revision history: point-in-time S&P 500 membership source
- Alpaca delayed SIP market data:
  canonical historical adjusted OHLCV archives from 2017-01-01 onward
- Alpaca News: Layer 0 raw Benzinga-sourced news archive used by Layer 1 text features
- SimFin:
  Layer 0 as-reported fundamentals and earnings-date archive used by Layer 1
  context features
- FRED: Layer 0 macro and rates archive used by Layer 1 context and regime features
- Alpaca Trading API: broker reconciliation and execution

Layer 0 owns every external data pull. Layer 1 and later milestones read existing R2
archives only; they do not call Wikipedia, Alpaca, SimFin, or FRED for feature inputs.

## Pi runtime container model

- Runtime host: Raspberry Pi 5
- Scheduler: host cron
- Runtime process: Docker container
- Runtime engine in container: OpenClaw
- Pi image includes only lightweight orchestration dependencies; heavy Layer 1 ML packages
  stay in Modal images even though the Pi image carries the `run_daily_layer1.py` entrypoint
  needed for `modal run`

Expected execution chain:
1. Cron triggers scheduled command on Pi host
2. Host starts or invokes the edge runtime container
3. OpenClaw/Hermes executes the daily runtime entrypoint inside container
4. Pi runtime runs Layer 0 incremental locally and writes the Layer 0 manifest
5. Pi runtime triggers the Modal Layer 1 daily feature job
6. Pi runtime waits on the Layer 1 manifest in R2 before continuing to inference
7. Runtime emits deterministic manifests and reports

## Baseline rollout order

1. Build and validate the complete Layer 0 historical backfill in R2:
   Wikipedia universe, Alpaca delayed SIP OHLCV, Alpaca news,
   SimFin fundamentals/earnings, and FRED macro/rates
2. Validate the Layer 0 daily incremental path:
   Alpaca live bars/news, SimFin refreshes, FRED refreshes, universe masks, and manifests
3. Validate the Pi -> Modal Layer 1 handoff:
   Pi submits the daily Modal job, then blocks on the R2 Layer 1 manifest
4. Build Layer 1 features strictly from existing R2 Layer 0 archives
   - Use `app/lab/data_pipelines/run_daily_layer1.py` as the single orchestration entrypoint
   - Pi-triggered single-date runs invoke the same module through `python -m modal run`
     with `--as-of-date` and `--layer0-run-id`
   - The single-date CLI path submits the heavy NLP/HMM stages through their dedicated
     Modal apps first, then invokes the final daily Layer 1 app only for history assembly
     and validation
   - Lab/backfill runs can supply a shared `run_id` plus `--from-date` / `--to-date`
   - The command derives ticker scope from Layer 0 universe masks and fails closed on
     missing upstream manifests or archives
5. Deploy cloud oracle with fixed contracts
6. Validate edge-to-cloud handshake plus Alpaca live-market-data normalization
7. Dry-run risk and execution path
8. Enable paper execution

## Modal deployment for Layer 1 and Layer 1.5

Install the cloud dependency set on the machine you use to deploy Modal apps:

```bash
python -m pip install -r requirements/modal.txt
```

Deploy each long-lived runner from the repo root:

```bash
modal deploy app/lab/data_pipelines/run_daily_layer1.py
modal deploy app/lab/data_pipelines/run_news_preprocessing.py
modal deploy app/lab/data_pipelines/run_text_topics.py
modal deploy app/lab/data_pipelines/run_finbert_sentiment.py
modal deploy app/lab/data_pipelines/run_hmm_regime_detection.py
modal deploy app/lab/data_pipelines/backfill_layer1.py
```

Runtime expectations:
- `run_daily_layer1.py`: orchestration entrypoint for Pi-triggered single-date jobs plus
  local/lab date-range orchestration from existing Layer 0 archives. The single-date
  `--as-of-date` path delegates heavy NLP/HMM work to the stage-specific Modal apps before
  running the final daily Layer 1 assembly/validation app.
- `run_news_preprocessing.py`: CPU only; contract normalization and sentence splitting.
- `run_text_topics.py`: cloud-only embeddings/topic modeling; CPU smoke path, GPU optional
  when backfilling larger historical corpora.
- `run_finbert_sentiment.py`: cloud-only heavy NLP; CPU smoke path, GPU recommended when
  throughput matters.
- `run_hmm_regime_detection.py`: CPU only; keep regime fitting off the Pi.
- `backfill_layer1.py`: CPU only; historical Layer 1 feature assembly runs on Modal/lab,
  not on the Pi runtime container.

Config ownership:
- `config/news_preprocessing.json` owns the news preprocessing app name, R2 secret, and
  timeout.
- `config/text_models.json` owns the text-topics app name, R2 secret, timeout, and Modal
  image settings.
- `config/finbert_sentiment.json` owns the FinBERT app name, R2 secret, timeout, and
  Modal image settings.
- `config/modal.json` owns the Pi-triggered daily Layer 1 app name and poll settings, the
  Layer 1 backfill and HMM regime app names, their timeouts, and shared Modal image
  settings.

Production readiness command and inspection:

```bash
./.venv/bin/modal run app/lab/data_pipelines/run_daily_layer1.py \
    --run-id layer1-readiness-2026-04-10-v7 \
    --as-of-date 2026-04-10 \
    --layer0-run-id layer0-historical-2017-01-01_to_2026-04-10 \
    --allow-layer0-manifest-date-range
```

Generate the final operator-facing readiness report for the same run/window:

```bash
./.venv/bin/python app/lab/data_pipelines/validate_layer1_archive.py \
    --run-id layer1-readiness-2026-04-10-v7 \
    --from-date 2026-04-10 \
    --to-date 2026-04-10 \
    --use-r2-universe
```

Inspect R2 outputs via:
- `artifacts/manifests/layer1_news_preprocessing/{run_id}-{date}.json`
- `artifacts/manifests/layer1_text_topics/{run_id}-{date}.json`
- `artifacts/manifests/layer1_finbert_sentiment/{run_id}-{date}.json`
- `artifacts/manifests/layer1_5_regime/{run_id}-{date}.json`
- `artifacts/manifests/layer1/{run_id}.json`
- `artifacts/reports/integration/layer1_archive_validation_{run_id}_{from}_to_{to}.json`
- `features/layer1/`, `features/layer1/news_sentiment/`, `features/layer1/topic_features/`,
  `features/layer1/sentiment_features/`, and `features/layer1_5/regime/`

For a targeted correctness audit on a sample of stored Layer 1 histories, run:

```bash
./.venv/bin/python app/lab/data_pipelines/audit_layer1_features.py \
    --as-of-date 2026-04-10 \
    --tickers AAPL,MSFT \
    --output-dir artifacts/reports/diagnostics
```

This audit is read-only with respect to production feature artifacts. It recomputes
deterministic Layer 0/1 branches from existing archives, validates the feature
catalog, and writes a local JSON report plus text summary. See
`docs/layer1_feature_audit.md` for interpretation details.

For the read-only dashboard backend payload used by the Layer 0/1 audit UI, run:

```bash
./.venv/bin/python app/lab/data_pipelines/build_layer1_feature_audit_dashboard.py \
    --from-date 2026-04-08 \
    --to-date 2026-04-10 \
    --tickers AAPL,MSFT \
    --output-dir artifacts/reports/diagnostics
```

This command reads stored `features/layer1/{TICKER}.parquet` histories and writes
local JSON/text report artifacts only. It does not modify R2 objects. See
`docs/layer1_feature_audit_dashboard.md` for the heatmap, family-status,
raw-vs-computed spot-check, formula-card, null-rate, and outlier payload
details.

For the live local Layer 0/1 QA dashboard UI itself, run:

```bash
HOME=/home/juyoungoh ./.venv/bin/python -m app.lab.feature_audit_dashboard \
    --from-date 2026-04-08 \
    --to-date 2026-04-10 \
    --tickers AAPL,MSFT \
    --host 127.0.0.1 \
    --port 8765
```

Then open `http://127.0.0.1:8765/`. The UI is read-only and limited to Layer 0/1
QA only; it does not show Layer 2, training, inference, portfolio, risk, or
execution panels. Real R2 reads require the standard `R2_*` environment
variables or `config/r2.env`; otherwise the app reads from the default local
mock store `data/runtime/r2_mock/`. See
`docs/layer1_feature_audit_dashboard.md` for PASS/WARN/FAIL interpretation and
panel-specific guidance.

Operational notes for the readiness report:
- The local validator writes
  `artifacts/reports/integration/layer1_archive_validation_{run_id}_{from}_to_{to}.json`.
- The report records the authoritative manifest key/status plus sibling stale `running`
  manifests so operators can distinguish the successful run from interrupted attempts such as
  `...-v4` or `...-v6`.

Baseline paper execution is long-only equities. Hedge and long-short capabilities must stay
disabled by policy until the relevant risk and execution gates are implemented:
- defensive index hedges: explicit approved instrument list, hedge notional caps, and
  broker/account permission checks
- sector hedges: margin or inverse-instrument approval, sector ETF mapping, and net/gross
  exposure controls
- true long-short: borrow/locate checks, margin checks, short-specific order semantics, and
  updated execution contracts

## Operational notes

- Keep secrets outside git
- Log every stage deterministically
- Fail closed on missing risk checks
- Keep risk thresholds in policy/config so long-only, hedged, and long-short modes can use
  the same Layer 4 rule framework
- Keep runtime assumptions synchronized across AGENTS, docs, and issue acceptance criteria
