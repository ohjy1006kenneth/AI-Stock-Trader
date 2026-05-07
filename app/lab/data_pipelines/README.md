# data_pipelines

Dataset and feature-building pipelines live here.

This is where news, market, and context inputs are aligned into training-ready tables.

- `run_news_preprocessing.py` writes sentence-level `NewsSentimentRecord` rows from Layer 0
  raw news and universe archives.
- `run_text_topics.py` writes sentence embeddings, BERTopic labels, and ticker-day topic
  FeatureRecords.
- `run_finbert_sentiment.py` writes FinBERT-scored news rows and ticker-day sentiment
  FeatureRecords.
- `run_hmm_regime_detection.py` writes market-wide regime probabilities under
  `features/layer1_5/regime/` from SPY and macro archives.
- `backfill_layer1.py` assembles final per-ticker `features/layer1/{ticker}.parquet`
  histories from market/context features plus the latest completed sentiment, topic, and
  regime branch artifacts. Use `--require-sentiment-features`,
  `--require-topic-features`, and `--require-regime-features` to fail closed when those
  optional branches are missing.

## Modal entrypoints

Install the cloud dependency set once before deploy or smoke runs:

```bash
python -m pip install -r requirements/modal.txt
```

| Runner | Deploy command | Smoke run | Config owner | CPU / GPU expectation |
|---|---|---|---|---|
| News preprocessing | `modal deploy app/lab/data_pipelines/run_news_preprocessing.py` | `modal run app/lab/data_pipelines/run_news_preprocessing.py --run-id smoke-news --as-of-date 2024-01-02` | `config/news_preprocessing.json` | CPU only |
| Text topics | `modal deploy app/lab/data_pipelines/run_text_topics.py` | `modal run app/lab/data_pipelines/run_text_topics.py --run-id smoke-topics --as-of-date 2024-01-02 --preprocessed-news-key features/layer1/news_sentiment/2024-01-02/smoke-news.parquet` | `config/text_models.json` | Cloud-only embeddings/topic modeling; CPU smoke path, GPU optional for larger historical batches |
| FinBERT sentiment | `modal deploy app/lab/data_pipelines/run_finbert_sentiment.py` | `modal run app/lab/data_pipelines/run_finbert_sentiment.py --run-id smoke-finbert --as-of-date 2024-01-02 --preprocessed-news-key features/layer1/news_sentiment/2024-01-02/smoke-news.parquet` | `config/finbert_sentiment.json` | Cloud-only heavy NLP; CPU smoke path, GPU recommended when throughput matters |
| HMM regime detection | `modal deploy app/lab/data_pipelines/run_hmm_regime_detection.py` | `modal run app/lab/data_pipelines/run_hmm_regime_detection.py --run-id smoke-hmm --train-start-date 2024-01-02 --train-end-date 2024-01-31 --inference-date 2024-02-01` | `config/modal.json` | CPU only |
| Layer 1 backfill | `modal deploy app/lab/data_pipelines/backfill_layer1.py` | `modal run app/lab/data_pipelines/backfill_layer1.py --run-id smoke-layer1 --tickers SPY,AAPL --benchmark-ticker SPY` | `config/modal.json` | CPU only; compute runs on Modal rather than the Pi |
