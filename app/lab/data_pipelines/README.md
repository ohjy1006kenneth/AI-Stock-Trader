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

## Modal entrypoints

Install the cloud dependency set once before deploy or smoke runs:

```bash
python -m pip install -r requirements/modal.txt
```

| Runner | Deploy command | Smoke run | Config owner | CPU / GPU expectation |
|---|---|---|---|---|
| Daily Layer 1 orchestrator | `modal deploy app/lab/data_pipelines/run_daily_layer1.py` | Pi/Hermes invokes this through `python -m modal run app/lab/data_pipelines/run_daily_layer1.py --run-id <run_id> --as-of-date <YYYY-MM-DD> --layer0-run-id <run_id>` after Layer 0 completes | `config/modal.json` | Cloud-only orchestration entrypoint for single-date Pi-triggered runs |
| News preprocessing | `modal deploy app/lab/data_pipelines/run_news_preprocessing.py` | `modal run app/lab/data_pipelines/run_news_preprocessing.py --run-id smoke-news --as-of-date 2024-01-02` | `config/news_preprocessing.json` | CPU only |
| Text topics | `modal deploy app/lab/data_pipelines/run_text_topics.py` | `modal run app/lab/data_pipelines/run_text_topics.py --run-id smoke-topics --as-of-date 2024-01-02 --preprocessed-news-key features/2024-01-02/news_sentiment/smoke-news.parquet` | `config/text_models.json` | Cloud-only embeddings/topic modeling; CPU smoke path, GPU optional for larger historical batches |
| FinBERT sentiment | `modal deploy app/lab/data_pipelines/run_finbert_sentiment.py` | `modal run app/lab/data_pipelines/run_finbert_sentiment.py --run-id smoke-finbert --as-of-date 2024-01-02 --preprocessed-news-key features/2024-01-02/news_sentiment/smoke-news.parquet` | `config/finbert_sentiment.json` | Cloud-only heavy NLP; CPU smoke path, GPU recommended when throughput matters |
| HMM regime detection | `modal deploy app/lab/data_pipelines/run_hmm_regime_detection.py` | `modal run app/lab/data_pipelines/run_hmm_regime_detection.py --run-id smoke-hmm --train-start-date 2024-01-02 --train-end-date 2024-01-31 --inference-date 2024-02-01` | `config/modal.json` | CPU only |
| Layer 1 backfill | `modal deploy app/lab/data_pipelines/backfill_layer1.py` | `modal run app/lab/data_pipelines/backfill_layer1.py --run-id smoke-layer1 --tickers SPY,AAPL --benchmark-ticker SPY` | `config/modal.json` | CPU only; compute runs on Modal rather than the Pi |
