# Layer 1 semantic-review dashboard

This dashboard is a read-only reviewer aid for the Layer 1 AAPL pilot.
It is designed to make the full NLP pipeline visible before a human accepts
the pilot for the broad point-in-time backfill tracked by #202.

## What the dashboard shows

- Ticker/entity preprocessing rows from `features/{date}/news_sentiment/{run_id}.parquet`,
  including `ticker_mentions`, `entity_mentions`, `source_text_field`,
  `source_text_order`, and `source_text_provenance`.
- Article embedding cache rows from `features/{date}/text_embeddings/{run_id}.parquet`,
  including model identity, revision, cache key, and embedding dimension.
- BERTopic article labels from `features/{date}/topic_labels/{run_id}.parquet`,
  including topic ids, probabilities, model metadata, labels, and keywords when
  those optional columns exist.
- Relevance-gate audit rows from
  `features/{date}/news_relevance_gate/{run_id}.parquet`, including
  accepted/borderline/rejected decisions, reason codes, ticker/entity evidence,
  score components, topic evidence, and embedding cache keys.
- Sentence/chunk FinBERT rows from
  `features/{date}/news_sentiment_scored/{run_id}.parquet`, grouped by
  `date -> article_id -> sentence_index`.
- Source-weighted ticker-date semantic aggregate rows from
  `features/{date}/sentiment_features/{run_id}.parquet`, including parsed
  source-weight summaries, topic sentiment summaries, contributing article ids,
  relevance reason codes, and semantic warning codes.
- HMM regime is shown once per trading date in a dedicated date header.
  Confidence and probabilities are therefore clearly date-level, not per row.
- Articles are split into accepted and flagged groups.
  Flagged articles stay visible with the evidence that caused the flag.

## Contamination / relevance handling

The review payload surfaces the following conditions:

- `ticker_mismatch`
- `no_requested_ticker_evidence`
- `low_relevance_score`
- `missing_relevance_score`
- `duplicate_normalized_headline`
- `duplicate_sentence_rows`

For the AAPL pilot, any article that lacks direct Apple/AAPL source-text
support is flagged and kept out of the default acceptance path.
This prevents unrelated or weakly relevant rows from silently dominating the
review queue.

The dashboard keeps pre-FinBERT relevance decisions visible even when a row was
rejected and therefore never received FinBERT scores. Missing ticker/entity or
provenance evidence is surfaced through `missing_evidence_flags`.

Human semantic review remains `needs_human_review` in the dashboard/API until
the completed NLP pipeline evidence is inspected and explicitly accepted by the
user through the separate AAPL pilot evidence flow.

## Local run

```bash
./.venv/bin/python -m app.lab.semantic_review_dashboard \
  --run-id layer1-aapl-accuracy-2026-05-06-to-2026-05-28-v4-after-pr221 \
  --from-date 2026-05-06 \
  --to-date 2026-05-28 \
  --ticker AAPL \
  --host 127.0.0.1 \
  --port 8766
```

API example:

```bash
curl -fsS 'http://127.0.0.1:8766/api/review?ticker=AAPL'
```

## Payload shape

Top-level response fields:

- `report`: the canonical report payload
- `summary`: aggregate counts
- `date_groups`: one entry per trading date
- `article_groups`: flat article cards for cross-date inspection
- `accepted_articles`: accepted article cards
- `flagged_articles`: flagged article cards
- `pipeline_sections`: stage-separated raw preprocessing, embedding, topic,
  relevance, FinBERT, semantic aggregate, and regime rows
- `artifact_keys`: resolved R2 keys used for each evidence section
- `human_semantic_review_status`: currently `needs_human_review`
- `recommendation_for_issue_202`: currently `needs_human_review`
- `warnings`: non-fatal load issues

Each `date_groups[]` entry contains:

- `date`
- `regime` with `scope: "date-level"`
- `semantic_aggregates[]` with ticker-date semantic aggregate rows
- article counts
- nested `articles[]`

Each `articles[]` entry contains:

- `article_id`
- `headline`
- `normalized_headline`
- `article_status`
- `contamination_flags`
- `requested_ticker_term_hits`
- `evidence_snippets`
- `preprocessing_rows[]`
- `topic_evidence[]`
- `relevance_gate_rows[]`
- `sentence_rows[]`

Each sentence_rows[] entry contains:

- `sentence_index`
- `chunk_index`
- `source_text_field`
- `source_text_order`
- `ticker_mentions`
- `entity_mentions`
- `text`
- FinBERT probabilities and score
- `row_granularity: "sentence-level"`

Each `pipeline_sections.semantic_aggregate_rows[]` entry contains parsed:

- `features`
- `source_weight_summary`
- `topic_sentiment_summary`
- `relevance_reason_codes`
- `semantic_warning_codes`
- `contributing_article_ids`

## Review guidance

1. Check the date header first for the HMM regime context.
2. Inspect the pipeline evidence cards to confirm each NLP stage is present.
3. Expand an article card to inspect preprocessing, topic, relevance, and
   sentence-level FinBERT rows together.
4. Use the evidence snippets and ticker-hit fields to decide whether the article
   is actually about AAPL.
5. Do not accept the pilot if the flagged section contains unrelated articles
   that are not explained by source-text evidence.
