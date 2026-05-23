# Runtime Flow

This document defines the operational sequence for both the one-time setup phase
and the recurring daily trading loop.

Execution context:
- Pi host cron schedules the daily run.
- The daily run executes inside a Docker container on the Pi.
- OpenClaw is the runtime engine inside that container.
- Hermes/Codex-owned Pi orchestration triggers Modal jobs but does not run heavy ML locally.
- Heavy compute (FinBERT, XGBoost) runs on Modal and reads/writes R2.

---

## Phase 0 - One-time setup (run before first live trading)

This phase builds the historical database in R2 that all downstream layers depend on.
It runs on a laptop or Modal, not the Pi.

```
python app/lab/data_pipelines/backfill_layer0.py \
    --from-date 2017-01-01 \
    --to-date <today>
```

What it produces in R2:
- `raw/prices/{ticker}.parquet` - full Alpaca delayed SIP adjusted OHLCV history per ticker
- `raw/news/YYYY-MM-DD.jsonl` - Alpaca news archive per date
- `raw/universe/YYYY-MM-DD.csv` - eligibility masks for all historical dates
- `raw/fundamentals/` - SimFin-first fundamentals and earnings-date archive with SEC
  company-facts fallback for unresolved ticker gaps
- `raw/macro/` - FRED macro/rate observations archive

Historical backfill uses:
- Wikipedia revision history for point-in-time index membership
- Alpaca delayed SIP (`feed=sip`, `timeframe=1Day`, `adjustment=all`) for canonical historical OHLCV from 2017-01-01 onward
- Alpaca News for historical/live raw news archives
- SimFin for as-reported fundamentals and earnings dates, with public SEC company-facts
  fallback when SimFin has no rows for a ticker that must remain strict-ready
- FRED for macro context series, persisted before feature generation

Without this, Layer 1 feature generation and model training cannot run.
The backfill is idempotent and safe to re-run; skips dates/archives already stored unless
`--overwrite` is provided.

After backfill: run model training and walk-forward validation (Milestones 2-4)
before enabling the live daily loop.

Layer 1 historical feature generation is also a cloud/lab concern, not a Pi concern:

```bash
modal run app/lab/data_pipelines/backfill_layer1.py \
    --run-id layer1-history-20240131 \
    --tickers SPY,AAPL \
    --benchmark-ticker SPY
```

This keeps feature-history assembly, sentence embeddings, FinBERT scoring, topic modeling,
and HMM regime work off the Pi path. The Pi orchestrator should only trigger Modal jobs and
consume their R2 outputs.

---

## Phase 1 - Daily loop (automated, Pi cron)

Execution chain:
1. Cron starts the Pi runtime container on the host.
2. OpenClaw/Hermes runs the Pi daily entrypoint inside that container.
3. The Pi runtime completes Layer 0 locally, then submits the daily Layer 1 job to Modal.
4. The Pi runtime polls the Layer 1 `PipelineManifestRecord` in R2 and does not continue
   to inference until the manifest is present, current, and `status=completed`.

### After market close

1. **Layer 0 incremental** (`app/pi/fetchers/layer0.py`)
   - Fetch today's live bar snapshot from Alpaca Market Data for all eligible tickers
   - Normalize and append it to the canonical raw price store in R2
   - Fetch today's raw Alpaca news -> write `raw/news/YYYY-MM-DD.jsonl` to R2
   - Refresh newly available SimFin filings and earnings-calendar data -> write `raw/fundamentals/`
   - Refresh FRED macro/rate observations available for the run date -> write
     `raw/macro/{YYYY-MM-DD}.parquet`
   - Recompute today's eligibility mask (quality + liquidity filters)
   - Write `raw/universe/YYYY-MM-DD.csv` to R2
   - Write `PipelineManifestRecord` (stage=layer0)

2. **Layer 1 feature generation** (Modal, triggered by Pi/Hermes after Layer 0)
   - Canonical orchestration entrypoint:
     `python app/lab/data_pipelines/run_daily_layer1.py --run-id <run_id> --from-date <YYYY-MM-DD> [--to-date <YYYY-MM-DD>]`
   - Pi runtime shells out through the lightweight Modal client/CLI only
   - Pi passes `run_id`, `as_of_date`, and the completed Layer 0 `run_id` when invoking
     `modal run app/lab/data_pipelines/run_daily_layer1.py` for the daily single-date flow
   - Single-date `--as-of-date` invocations fan out through the stage-specific Modal apps
     first:
     `run_news_preprocessing.py`, `run_text_topics.py`, `run_finbert_sentiment.py`, and
     `run_hmm_regime_detection.py`; the final Layer 1 app then only assembles histories,
     writes the `layer1` manifest, and runs archive validation
   - Multi-date readiness or catch-up invocations from laptop/lab use
     `python app/lab/data_pipelines/run_daily_layer1.py --from-date ... --to-date ...`;
     when the Modal app is available, that command submits one batched remote Layer 1 job
     so topic modeling and FinBERT stay in the Modal image and do not fan out through
     separate per-date local heavy-ML runs
   - Pi records the expected Layer 1 manifest key and waits on R2 before moving on
   - Read today's OHLCV Parquet, news JSON Lines, and universe CSV from R2
   - Read point-in-time SimFin fundamentals and earnings dates from R2
   - Read FRED macro context series from the `raw/macro/{run_date}.parquet` point-in-time
     snapshot (with backward-compatible legacy-vintage recovery when older observation-date
     shards still exist)
   - When `config/order_book_features.json` explicitly enables a provider, read the
     provider-normalized pre-open Level 2 snapshot from
     `raw/order_book/{provider}/{run_date}.parquet`; if the archive is missing for a date,
     keep the branch non-fatal and emit null order-book features for that date/ticker scope
   - No options-derived Layer 1 branch is part of the baseline daily flow; the repo does
     not define a point-in-time historical options-chain archive or config for `iv_rank`,
     `put_call_ratio`, or `iv_skew`
   - Fail closed if the required Layer 0 raw archives or manifests are missing
   - Derive the ticker scope from Layer 0 universe masks; optional ticker filters may only
     narrow that scope, never replace it with a hand-maintained production list
   - Preprocess news into sentence-level `NewsSentimentRecord` rows at
     `features/layer1/news_sentiment/{YYYY-MM-DD}/{run_id}.parquet`
   - Compute pinned-model sentence embeddings and BERTopic labels into
     `features/layer1/text_embeddings/`, `features/layer1/topic_labels/`, and
     `features/layer1/topic_features/`
   - Score preprocessed news with FinBERT into
     `features/layer1/news_sentiment_scored/{YYYY-MM-DD}/{run_id}.parquet` and aggregate
     ticker-day sentiment FeatureRecords into
     `features/layer1/sentiment_features/{YYYY-MM-DD}/{run_id}.parquet`
   - Compute market, NLP, context, and optional order-book spread / imbalance features for today
   - Refresh aligned per-ticker feature histories at `features/layer1/TICKER.parquet` in R2
     while preserving daily single-record shard support for incremental runs
   - Run final archive validation, persist the JSON report under
     `artifacts/reports/integration/layer1_archive_validation_{run_id}_{from}_to_{to}.json`,
     and return nonzero unless `ready_for_layer2=true`
   - Validation distinguishes hard failures from regime warm-up warnings: explicit null
     regime placeholders are allowed only when Layer 1.5 diagnostics show insufficient
     bounded history, but that still leaves `ready_for_layer2=false` until a later rerun
     produces non-null regime fields
   - Write `PipelineManifestRecord` (stage=layer1, statuses: running/completed/failed)
   - Modal runner entrypoints:
     `run_news_preprocessing.py`, `run_text_topics.py`, `run_finbert_sentiment.py`,
     `run_daily_layer1.py`, and `backfill_layer1.py`
   - CPU / GPU expectations:
     preprocessing is CPU only; text topics and FinBERT stay on Modal and must not be
     redirected to Pi-local model execution
   - Example readiness rerun command used during Issue `#126`:

```bash
./.venv/bin/modal run app/lab/data_pipelines/run_daily_layer1.py \
    --run-id layer1-readiness-2026-04-10-v7 \
    --as-of-date 2026-04-10 \
    --layer0-run-id layer0-historical-2017-01-01_to_2026-04-10 \
    --allow-layer0-manifest-date-range
```

   - Inspect the produced artifacts through R2 manifests and validation outputs:
     `artifacts/manifests/layer1_news_preprocessing/{run_id}-{date}.json`,
     `artifacts/manifests/layer1_text_topics/{run_id}-{date}.json`,
     `artifacts/manifests/layer1_finbert_sentiment/{run_id}-{date}.json`,
     `artifacts/manifests/layer1_5_regime/{run_id}-{date}.json`, and
     `artifacts/manifests/layer1/{run_id}.json`
   - Re-run the readiness validator against the canonical R2 universe when needed:

```bash
python app/lab/data_pipelines/validate_layer1_archive.py \
    --run-id layer1-readiness-2026-04-10-v7 \
    --from-date 2026-04-10 \
    --to-date 2026-04-10 \
    --use-r2-universe
```

3. **Layer 1.5 regime detection** (Modal)
   - Read recent SPY returns, VIX, and FRED macro regime inputs from R2
   - When orchestration does not pass an explicit `hmm_train_start_date`, use the configured
     bounded HMM train lookback from `config/modal.json` so catch-up runs do not refetch the
     full macro archive for every single-date regime inference
   - Run HMM to classify current regime (bull / bear / sideways)
   - Write market-wide regime probabilities to `features/layer1_5/regime/{run_id}.parquet`
   - Write `PipelineManifestRecord` (stage=layer1_5_regime)
   - If the bounded HMM train window is too short or the inference row is still incomplete,
     write explicit warning diagnostics plus null regime placeholders rather than failing
     closed inside Layer 1.5 itself; the downstream Layer 1 validator then blocks Layer 2
     handoff until a rerun has enough history
   - Layer 1 feature assembly broadcasts the regime label/probabilities onto ticker rows

4. **Layer 2 inference** (Modal)
   - Read today's feature row from R2
   - Select active XGBoost model for current regime
   - Produce `ScoreRecord` per ticker using sector-neutral or cross-sectional alpha scores
   - Write scores to `processed/scores/YYYY-MM-DD.parquet` in R2
   - Write `PipelineManifestRecord` (stage=layer2)

5. **Layer 3 portfolio construction** (Pi)
   - Pi reads scores from R2
   - Contextual bandit filters the universe to 30-50 candidates
   - Mean-variance optimizer produces signed target weights with turnover penalty
   - Baseline long-only policy keeps ordinary single-stock targets non-negative
   - Future hedge modes may add approved index or sector hedge overlay targets
   - Write `PortfolioRecord` list to R2

6. **Layer 4 risk engine** (Pi)
   - Apply hard rules: position cap, ADV cap, sector cap, beta cap, correlation cap,
     drawdown scaling, fat-finger checks
   - Load all thresholds from policy/config; do not rely on hardcoded risk constants
   - Reject negative single-stock targets unless an explicit hedge/short policy is enabled
   - Write `ApprovedOrderRecord` list to R2 and local SSD
   - Write `PipelineManifestRecord` (stage=layer4)

### Before / at next market open

7. **Layer 5 reconciliation** (Pi)
   - Fetch actual Alpaca account state
   - Reconcile vs. internal state; Alpaca wins on any mismatch
   - Compute delta orders needed to reach approved targets

8. **Layer 5 execution** (Pi)
   - Convert target dollars to whole share counts (round down)
   - Place long-only equity limit orders via Alpaca in the baseline deployment
   - Monitor fills every N minutes; cancel and reprice stale orders after 30 min
   - Log fills to local SQLite ledger and `ExecutionFillRecord` in R2

9. **Reporting** (Pi)
   - Compute daily P&L, slippage, fill quality metrics
   - Send Telegram summary
   - Write daily report to R2

### Throughout the day

10. **Health monitoring** (Pi)
    - Monitor for pipeline stage mismatches or stale manifests
    - Check daily loss limit; if triggered, reduce gross exposure
    - Alert via Telegram on anomalies

---

## Manifest-driven orchestration

Every stage writes a `PipelineManifestRecord` to R2 on completion or failure.
The next stage reads the manifest to verify the upstream stage completed before proceeding.
If a manifest is missing or `status=failed`, the stage halts and alerts.
For the Pi-to-Modal handoff specifically, the Pi runtime must also reject stale Layer 1
manifests whose `as_of_date` or upstream `layer0_run_id` metadata do not match the current run.

Source-of-truth rules for the daily loop:
- Alpaca delayed SIP is the canonical historical archive source for raw adjusted prices
- SimFin is the primary provider for point-in-time fundamentals and earnings dates, with SEC
  company-facts fallback only inside Layer 0 when SimFin returns no usable rows; Layer 1 reads
  the Layer 0 R2 archive rather than calling either provider directly
- FRED is the canonical provider for macro context inputs, but Layer 1 reads the Layer 0
  R2 archive rather than calling FRED directly
- Alpaca is the canonical archive source for raw news and the live source for current-day market data, broker reconciliation, and execution

This ensures no stage silently runs on stale or missing inputs.
