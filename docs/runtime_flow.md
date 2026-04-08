# Runtime Flow

This document defines the operational sequence for both the one-time setup phase
and the recurring daily trading loop.

Execution context:
- Pi host cron schedules the daily run.
- The daily run executes inside a Docker container on the Pi.
- OpenClaw is the runtime engine inside that container.
- Heavy compute (FinBERT, XGBoost) runs on Modal and reads/writes R2.

---

## Phase 0 — One-time setup (run before first live trading)

This phase builds the historical database in R2 that all downstream layers depend on.
It runs on a laptop or Modal — not the Pi.

```
python app/lab/data_pipelines/backfill_layer0.py \
    --from-date 2014-01-01 \
    --to-date <today>
```

What it produces in R2:
- `raw/prices/{ticker}.parquet` — full OHLCV history per ticker (adjusted)
- `raw/news/YYYY-MM-DD.jsonl` — news archive per date
- `raw/universe/YYYY-MM-DD.csv` — eligibility masks for all historical dates

Without this, Layer 1 feature generation and model training cannot run.
The backfill is idempotent — safe to re-run; skips dates already stored.

After backfill: run model training and walk-forward validation (Milestones 2–4)
before enabling the live daily loop.

---

## Phase 1 — Daily loop (automated, Pi cron)

### After market close

1. **Layer 0 incremental** (`app/pi/fetchers/layer0.py`)
   - Fetch today's adjusted OHLCV bars from Polygon for all eligible tickers
   - Append to existing `raw/prices/{ticker}.parquet` in R2
   - Fetch today's news from Polygon → write `raw/news/YYYY-MM-DD.jsonl` to R2
   - Recompute today's eligibility mask (quality + liquidity filters)
   - Write `raw/universe/YYYY-MM-DD.csv` to R2
   - Write `PipelineManifestRecord` (stage=layer0)

2. **Layer 1 feature generation** (Modal)
   - Read today's OHLCV Parquet and news JSON Lines from R2
   - Compute market, NLP, and context features for today
   - Write aligned feature row to `processed/features/YYYY-MM-DD.parquet` in R2
   - Write `PipelineManifestRecord` (stage=layer1)

3. **Layer 1.5 regime detection** (Modal)
   - Read recent SPY returns, VIX, yield curve from R2
   - Run HMM to classify current regime (bull / bear / sideways)
   - Append regime label and confidence to today's feature row
   - Write updated feature row to R2

4. **Layer 2 inference** (Modal)
   - Read today's feature row from R2
   - Select active XGBoost model for current regime
   - Produce `ScoreRecord` per ticker (return_score, pos_prob, rank_score)
   - Write scores to `processed/scores/YYYY-MM-DD.parquet` in R2
   - Write `PipelineManifestRecord` (stage=layer2)

5. **Layer 3 portfolio construction** (Pi)
   - Pi reads scores from R2
   - Contextual bandit filters ~800 universe stocks → 30–50 candidates
   - Mean-variance optimizer produces target weights with turnover penalty
   - Write `PortfolioRecord` list to R2

6. **Layer 4 risk engine** (Pi)
   - Apply hard rules: position cap, ADV cap, sector cap, beta cap,
     correlation cap, drawdown scaling, fat-finger checks
   - Write `ApprovedOrderRecord` list to R2 and local SSD
   - Write `PipelineManifestRecord` (stage=layer4)

### Before / at next market open

7. **Layer 5 reconciliation** (Pi)
   - Fetch actual Alpaca account state
   - Reconcile vs. internal state — Alpaca wins on any mismatch
   - Compute delta orders needed to reach approved targets

8. **Layer 5 execution** (Pi)
   - Convert target dollars to whole share counts (round down)
   - Place limit orders via Alpaca
   - Monitor fills every N minutes; cancel and reprice stale orders after 30 min
   - Log fills to local SQLite ledger and `ExecutionFillRecord` in R2

9. **Reporting** (Pi)
   - Compute daily P&L, slippage, fill quality metrics
   - Send Telegram summary
   - Write daily report to R2

### Throughout the day

10. **Health monitoring** (Pi)
    - Monitor for pipeline stage mismatches or stale manifests
    - Check daily loss limit — if triggered, reduce gross exposure
    - Alert via Telegram on anomalies

---

## Manifest-driven orchestration

Every stage writes a `PipelineManifestRecord` to R2 on completion or failure.
The next stage reads the manifest to verify the upstream stage completed before proceeding.
If a manifest is missing or `status=failed`, the stage halts and alerts.

This ensures no stage silently runs on stale or missing inputs.
