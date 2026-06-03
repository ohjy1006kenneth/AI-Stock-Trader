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
  Layer 0 primary fundamentals and earnings-date archive source used by Layer 1
  context features
- SEC company-facts API:
  public Layer 0 fallback for active-ticker fundamentals gaps when SimFin returns
  no usable rows
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
   - historical OHLCV provenance must record `feed=sip` and `adjustment=all`
     in the Layer 0 manifest metadata plus
     `artifacts/reports/integration/layer0_ohlcv_provenance_{run_id}.json`
2. Validate the Layer 0 daily incremental path:
   Alpaca live bars/news, SimFin refreshes, FRED refreshes, universe masks, and manifests
   - daily OHLCV provenance must record `adjustment=raw`; the run-date bar is stored as a
     raw snapshot and is not retroactively rewritten in-place during the same run
   - daily FRED refreshes must persist `raw/macro/{run_date}.parquet` as the point-in-time
     snapshot for that run date rather than keying shards only by observation date
3. Validate the Pi -> Modal Layer 1 handoff:
   Pi submits the daily Modal job, then blocks on the R2 Layer 1 manifest
4. Build Layer 1 features strictly from existing R2 Layer 0 archives
   - Use `app/lab/data_pipelines/run_daily_layer1.py` as the single orchestration entrypoint
   - Pi-triggered single-date runs invoke the same module through `python -m modal run`
     with `--as-of-date` and `--layer0-run-id`
   - The single-date CLI path submits the heavy NLP/HMM stages through their dedicated
     Modal apps first, then invokes the final daily Layer 1 app only for history assembly
     and validation
   - Multi-date lab/readiness runs invoke the same module with `--from-date` /
     `--to-date`; when the Modal app is available, that path submits one batched remote
     Layer 1 job so topic modeling and FinBERT stay on the declared Modal dependency stack
     instead of falling back to local heavy-ML execution
   - The command derives ticker scope from Layer 0 universe masks and fails closed on
     missing upstream manifests or archives
   - Optional Level 2 features stay disabled unless `config/order_book_features.json`
     explicitly enables a provider; when enabled, Layer 1 reads only the staged R2 archive
     `raw/order_book/{provider}/{run_date}.parquet` and treats missing per-date coverage as
     a null-feature condition instead of breaking the rest of Layer 1
   - The baseline stack does not define an options-chain archive or non-secret config for
     `iv_rank`, `put_call_ratio`, or `iv_skew`; do not expect an options-derived Layer 1
     branch unless a future task adds an existing-stack provider and documents the archive
     contract
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
  running the final daily Layer 1 assembly/validation app. The multi-date
  `--from-date` / `--to-date` path submits one batched remote Modal job that runs the
  readiness window inside the same declared Modal image, reusing the loaded text and
  FinBERT runtimes across dates before final assembly/validation.
- `run_news_preprocessing.py`: CPU only; contract normalization and sentence splitting.
- `run_text_topics.py`: cloud-only embeddings/topic modeling; CPU smoke path, GPU optional
  when backfilling larger historical corpora.
- `run_finbert_sentiment.py`: cloud-only heavy NLP; CPU smoke path, GPU recommended when
  throughput matters.
- `run_hmm_regime_detection.py`: CPU only; keep regime fitting off the Pi.
- `backfill_layer1.py`: CPU only; historical Layer 1 feature assembly runs on Modal/lab,
  not on the Pi runtime container.
- `app/lab/training/run_finbert_finetuning.py`: offline only; evaluates the pinned baseline
  FinBERT model against archived return-derived labels and can optionally fine-tune a lab
  artifact from the same archives. If GPU is used for this job, cap it at `gpu="T4"`.
  The job writes local `artifacts/` bundle/report/manifest outputs and must not update
  `config/finbert_sentiment.json` or silently replace production inference.

Config ownership:
- `config/news_preprocessing.json` owns the news preprocessing app name, R2 secret, and
  timeout.
- `config/text_models.json` owns the text-topics app name, R2 secret, timeout, and Modal
  image settings.
- `config/finbert_sentiment.json` owns the FinBERT app name, R2 secret, timeout, and
  Modal image settings.
- `config/finbert_finetuning.json` owns the offline FinBERT evaluation/fine-tuning app
  name, R2 secret, training hyperparameters, and the T4-only GPU cap for optional
  fine-tuning runs.
- `config/modal.json` owns the Pi-triggered daily Layer 1 app name and poll settings, the
  single-date Layer 1 timeout, dedicated batched Layer 1 timeout, the batched Layer 1
  `T4` GPU cap, Layer 1 backfill and HMM regime app names, their timeouts, the default
  bounded HMM train lookback used when orchestration does not pass an explicit
  `hmm_train_start_date`, and shared Modal image settings.

Offline FinBERT evaluation/fine-tuning command:

```bash
HOME=/home/juyoungoh ./.venv/bin/python app/lab/training/run_finbert_finetuning.py \
    --run-id finbert-offline-2026-05-14 \
    --from-date 2026-03-01 \
    --to-date 2026-04-10 \
    --news-run-id layer1-readiness-2026-04-10-v7 \
    --fine-tune
```

Promotion rule:
- the offline artifact manifest at
  `artifacts/manifests/lab_finbert_finetuning_artifact/{run_id}.json` is emitted with
  `approved: false`
- production FinBERT continues to use `config/finbert_sentiment.json`
- switching production to a fine-tuned artifact requires explicit human approval plus a
  separate code/config change that updates the production model pin

Production readiness command and inspection:

```bash
./.venv/bin/modal run app/lab/data_pipelines/run_daily_layer1.py \
    --run-id layer1-readiness-2026-04-10-v7 \
    --as-of-date 2026-04-10 \
    --layer0-run-id layer0-historical-2017-01-01_to_2026-04-10 \
    --allow-layer0-manifest-date-range
```

For a multi-date readiness window from a local shell, use the Python entrypoint so the
lightweight local process can submit the batched remote Modal run:

```bash
HOME=/home/juyoungoh ./.venv/bin/python app/lab/data_pipelines/run_daily_layer1.py \
    --run-id layer1-readiness-2026-04-14-to-2026-05-12-batch-v5 \
    --from-date 2026-04-14 \
    --to-date 2026-05-12 \
    --layer0-run-id layer0-readiness-2026-04-14-to-2026-05-12-batch-v1 \
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
- `features/{YYYY-MM-DD}/`, `features/{YYYY-MM-DD}/news_sentiment/`,
  `features/{YYYY-MM-DD}/topic_features/`,
  `features/{YYYY-MM-DD}/sentiment_features/`, and
  `features/{YYYY-MM-DD}/regime/`

Before launching the broad point-in-time historical Layer 1 backfill, run the
AAPL-only accuracy and parameter-calibration pilot:

```bash
HOME=/home/juyoungoh ./.venv/bin/python app/lab/data_pipelines/run_aapl_layer1_accuracy.py \
    --run-id layer1-aapl-accuracy-<window>-v1 \
    --from-date <from> \
    --to-date <to> \
    --layer0-run-id <layer0-run-id> \
    --allow-layer0-manifest-date-range \
    --run-layer1
```

This command is intentionally limited to `AAPL`; it must not be used as the
full-universe backfill tracked by #202. The durable diagnostic report is written
to `artifacts/reports/diagnostics/layer1_aapl_feature_accuracy_{run_id}_{from}_to_{to}.json`
and includes date window, input Layer 0 evidence, date-first feature shard
examples, parameter candidates, quality diagnostics, and a recommendation for
whether #202 should proceed.

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

This command reads stored Layer 1 feature histories and writes local JSON/text report
artifacts only. Legacy `features/layer1/{TICKER}.parquet` histories may be used by this
audit surface until it is fully migrated to date-first reads. It does not modify R2. See
`docs/layer1_feature_audit_dashboard.md` for the heatmap, family-status,
raw-vs-computed spot-check, formula-card, null-rate, and outlier payload
details.

For the live local Layer 0/1 QA dashboard UI itself, run:

```bash
./.venv/bin/python -m app.lab.feature_audit_dashboard \
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
- The same report now distinguishes regime warm-up warnings from hard data-contract failures:
  explicit null regime placeholders are acceptable only when Layer 1.5 diagnostics say the
  bounded HMM window is still too short, and those warnings still keep
  `ready_for_layer2=false` until a later rerun fills the regime fields.

Operational notes for Layer 0 OHLCV provenance:
- `python app/lab/data_pipelines/validate_layer0_archive.py ...` now fails closed when the
  Layer 0 manifest omits the OHLCV adjustment provenance summary or when the companion
  `artifacts/reports/integration/layer0_ohlcv_provenance_{run_id}.json` report is missing
  or inconsistent
- the provenance report is the authoritative audit record for whether a Layer 0 run wrote
  Alpaca `adjustment=all` historical bars or `adjustment=raw` daily bars
- split-like discontinuity samples in that report are heuristic audit context only; they are
  not a substitute for a dedicated provider corporate-actions archive

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
