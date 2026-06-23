# data_pipelines

Dataset and feature-building pipelines live here.

This is where news, market, and context inputs are aligned into training-ready tables.

- `run_news_preprocessing.py` writes sentence-level `NewsSentimentRecord` rows from Layer 0
  raw news and universe archives.
- `run_text_topics.py` writes sentence embeddings, BERTopic labels, and ticker-day topic
  FeatureRecords.
- `run_finbert_sentiment.py` writes FinBERT-scored news rows and ticker-day sentiment
  FeatureRecords.
- `run_hmm_regime_detection.py` writes market-wide Layer 1.5 regime features from Layer 0
  price and macro archives.
- `run_daily_layer1.py` is the single-command Layer 1 orchestration entrypoint: it validates
  Layer 0 readiness, derives ticker scope from universe masks, runs the text/regime branches,
  assembles aligned per-ticker histories, and runs final Layer 1 archive validation. Multi-date
  readiness runs use the same batched Modal entrypoint and may request a config-capped `T4`
  GPU worker when the text/FinBERT branches need acceleration.
  The Pi daily runtime invokes the same module through `modal run` for single-date jobs after
  Layer 0 completes, while local/lab runs can use `python ... --from-date/--to-date`.
- `run_aapl_layer1_accuracy.py` is the pre-backfill AAPL-only pilot workflow. With
  `--run-layer1`, it narrows Layer 1 generation to `AAPL` and then writes a feature
  accuracy/parameter-calibration report. Without `--run-layer1`, it audits existing AAPL
  date-first shards. It is intentionally guarded against non-AAPL ticker scope.
- `verify_aapl_pilot_evidence.py` builds the non-dashboard AAPL pilot evidence bundle after
  the accuracy pilot: machine-integrity JSON plus human-review Markdown/CSV. It keeps
  FinBERT/topic/HMM semantic correctness as an explicit human review decision.
- `app.lab.semantic_review_dashboard` serves the same semantic-review evidence as a
  persistent read-only browser dashboard for repeated local or R2-backed inspection. The
  checked-in current AAPL pilot bundle lives under `artifacts/reports/diagnostics/` and can
  also be redirected with `--artifact-dir` when you point at another diagnostics directory.

## Modal entrypoints

Install the cloud dependency set once before deploy or smoke runs:

```bash
python -m pip install -r requirements/modal.txt
```

| Runner | Deploy command | Smoke run | Config owner | CPU / GPU expectation |
|---|---|---|---|---|
| Daily Layer 1 orchestrator | `modal deploy app/lab/data_pipelines/run_daily_layer1.py` | Pi/Hermes invokes this through `python -m modal run app/lab/data_pipelines/run_daily_layer1.py --run-id <run_id> --as-of-date <YYYY-MM-DD> --layer0-run-id <run_id>` after Layer 0 completes | `config/modal.json` | Cloud-only orchestration entrypoint for single-date Pi-triggered runs |
| AAPL Layer 1 accuracy pilot | n/a | `python app/lab/data_pipelines/run_aapl_layer1_accuracy.py --run-id layer1-aapl-accuracy-<window>-v1 --from-date <from> --to-date <to> --layer0-run-id <run_id> --run-layer1` | `config/layer1_aapl_accuracy.json` | AAPL-only local/lab pilot; do not use for full-universe backfill |
| AAPL pilot evidence bundle | n/a | `python -m app.lab.data_pipelines.verify_aapl_pilot_evidence --run-id layer1-aapl-accuracy-<window>-v1 --from-date <from> --to-date <to> --layer0-run-id <run_id>` | n/a | Read-only evidence generation from stored Layer 0/1 artifacts |
| News preprocessing | `modal deploy app/lab/data_pipelines/run_news_preprocessing.py` | `modal run app/lab/data_pipelines/run_news_preprocessing.py --run-id smoke-news --as-of-date 2024-01-02` | `config/news_preprocessing.json` | CPU only |
| Text topics | `modal deploy app/lab/data_pipelines/run_text_topics.py` | `modal run app/lab/data_pipelines/run_text_topics.py --run-id smoke-topics --as-of-date 2024-01-02 --preprocessed-news-key features/2024-01-02/news_sentiment/smoke-news.parquet` | `config/text_models.json` | Cloud-only embeddings/topic modeling; CPU smoke path, GPU optional for larger historical batches |
| FinBERT sentiment | `modal deploy app/lab/data_pipelines/run_finbert_sentiment.py` | `modal run app/lab/data_pipelines/run_finbert_sentiment.py --run-id smoke-finbert --as-of-date 2024-01-02 --preprocessed-news-key features/2024-01-02/news_sentiment/smoke-news.parquet` | `config/finbert_sentiment.json` | Cloud-only heavy NLP; CPU smoke path, GPU recommended when throughput matters |
| HMM regime detection | `modal deploy app/lab/data_pipelines/run_hmm_regime_detection.py` | `modal run app/lab/data_pipelines/run_hmm_regime_detection.py --run-id smoke-hmm --train-start-date 2024-01-02 --train-end-date 2024-01-31 --inference-date 2024-02-01` | `config/modal.json` | CPU only |
| Layer 1 backfill | `modal deploy app/lab/data_pipelines/backfill_layer1.py` | `modal run app/lab/data_pipelines/backfill_layer1.py --run-id smoke-layer1 --tickers SPY,AAPL --benchmark-ticker SPY` | `config/modal.json` | CPU only; compute runs on Modal rather than the Pi |
