# Layer 1 semantic-review dashboard

This dashboard is a read-only reviewer aid for the Layer 1 AAPL pilot.
It is designed to make sentence-level FinBERT evidence and date-level HMM
regimes visible at the same time without confusing the two granularities.

## What the dashboard shows

- Raw scored-news rows are grouped by `date -> article_id -> sentence_index`.
- Each article card exposes the scored-news `sentence_index` and scored `text`.
- The same raw article can contain multiple sentence rows; those rows are shown
  together so reviewers do not mistake sentence-level FinBERT evidence for
  contradictory duplicated article-level rows.
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

Upstream preprocessing rows also carry `normalized_headline`, `ticker_mentions`,
`entity_mentions`, and `source_text_provenance` so the dashboard can explain why
an article was considered AAPL-relevant before FinBERT scoring.

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
- `warnings`: non-fatal load issues

Each `date_groups[]` entry contains:

- `date`
- `regime` with `scope: "date-level"`
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

## Review guidance

1. Check the date header first for the HMM regime context.
2. Expand an article card to inspect its sentence rows.
3. Use the evidence snippets and ticker-hit fields to decide whether the article
   is actually about AAPL.
4. Do not accept the pilot if the flagged section contains unrelated articles
   that are not explained by source-text evidence.
