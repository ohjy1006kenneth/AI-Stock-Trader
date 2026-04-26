# data_pipelines

Dataset and feature-building pipelines live here.

This is where news, market, and context inputs are aligned into training-ready tables.

- `run_news_preprocessing.py` writes sentence-level `NewsSentimentRecord` rows from Layer 0
  raw news and universe archives.
- `run_text_topics.py` writes sentence embeddings, BERTopic labels, and ticker-day topic
  FeatureRecords.
- `run_finbert_sentiment.py` writes FinBERT-scored news rows and ticker-day sentiment
  FeatureRecords.
