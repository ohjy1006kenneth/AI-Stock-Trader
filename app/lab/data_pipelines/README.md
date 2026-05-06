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
  `features/layer1_5/regime/`.
- `backfill_layer1.py` assembles final per-ticker `features/layer1/{ticker}.parquet`
  histories from market/context features plus the latest completed sentiment, topic, and
  regime branch artifacts. Use `--require-sentiment-features`,
  `--require-topic-features`, and `--require-regime-features` to fail closed when those
  optional branches are missing.
