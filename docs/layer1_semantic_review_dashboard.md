# Layer 1 semantic-review dashboard

This dashboard is a read-only reviewer aid for the Layer 1 AAPL pilot.
It is intentionally beginner-friendly: the default view stays clean, the
advanced technical rows are collapsed by default, and the page explains the
review in plain language before showing raw evidence.

## What the dashboard shows

- A Summary / Gate Status tab that says whether human semantic review can start
  or is `not ready for final human acceptance` because required NLP, HMM, or
  price evidence is missing.
- Separate Article Review and FinBERT Sentence Review tabs so accepted AAPL
  article groups do not get mixed with contamination/no-ticker-evidence rows,
  and sentence-level FinBERT review can show the scored text directly.
- Stable gate cards for preprocessing, embeddings, topic labels, relevance
  gate rows, FinBERT rows, semantic aggregates, HMM rows, selected-ticker price
  rows, benchmark price rows, and benchmark/HMM chart rows.
- A simple status card that says whether the page is ready to review, needs a
  data fix, needs a model/pipeline fix, or does not yet have enough evidence.
- Plain-language overview cards that answer:
  - What am I looking at?
  - Why does it matter?
  - What would make this good or bad?
- A market benchmark chart that uses SPY by default, because the HMM regime is
  market-wide and date-level rather than company-specific.
- Article review cards that stay collapsed by default and show accepted AAPL
  groups first, with flagged/no-ticker-evidence contamination kept separate.
- FinBERT sentence review cards that show the full scored text, source text
  field/order, sentiment probabilities, sentiment score, relevance score/state,
  and row granularity for each scored row.
- Explicit missing-text warnings and source-artifact gap cards when the scored
  sentence text is unavailable.
- Advanced technical sections for preprocessing, embeddings, topic labels,
  relevance-gate rows, FinBERT rows, semantic aggregates, benchmark rows, and
  HMM artifacts.

## Benchmark chart behavior

The chart combines:

- benchmark price history
- date-axis regime bands / markers
- bear, sideways, and bull probabilities
- a short beginner explanation of why the benchmark matters

If SPY price rows or the HMM manifest / training metadata are missing, the page
shows a blocker card instead of an empty chart.

## Review guidance

1. Check the benchmark chart first.
2. Read the review state.
3. Open the Article Review tab when you want article-level evidence.
4. Open the FinBERT Sentence Review tab when you need the scored sentence text
   and row-level probabilities.

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

Rendered smoke gate:

```bash
./.venv/bin/python -m app.lab.semantic_review_dashboard \
  --run-id layer1-aapl-accuracy-2026-05-06-to-2026-05-28-v4-after-pr221 \
  --from-date 2026-05-06 \
  --to-date 2026-05-28 \
  --ticker AAPL \
  --host 127.0.0.1 \
  --port 8766 \
  --smoke \
  --browser-binary chromium \
  --smoke-screenshot artifacts/reports/diagnostics/semantic_review_dashboard_smoke.png
```

The smoke gate checks both the API payload and the rendered browser page. It fails when any
required raw stage section has zero rows or missing date artifacts, when the page falls back
to a cached evidence bundle, or when the rendered HMM benchmark chart would be empty or
misleading. The browser check requires a real SVG chart, numeric SPY benchmark close values,
numeric bear/sideways/bull probabilities, and HMM manifest/training-window metadata.

## Payload shape

Top-level response fields include:

- `report`: the canonical report payload
- `summary`: aggregate counts
- `run_readiness`: stable run-level readiness fields including run ID, ticker,
  date range, recommendation, human-review status, row/article/date counts,
  accepted/flagged counts, blocked-gate count, and final-acceptance boolean
- `summary_cards`: display-ready run ID, ticker, date range, recommendation,
  human-review status, row/article/date, accepted, and flagged cards
- `gate_cards`: one stable card per required evidence gate with status, row
  count, artifact keys, missing/tried keys, failure reasons, and message
- `missing_pipeline_sections`: blocked gate summaries used by the browser to
  show why review remains blocked
- `date_groups`: one entry per trading date
- `article_groups`: flat article cards for cross-date inspection
- `accepted_articles`: accepted article cards
- `flagged_articles`: flagged article cards
- `article_review`: tab-ready accepted and contamination date groups plus counts
- `finbert_sentence_review`: article-level sentence rows, full-text availability,
  missing-text warnings, and source-artifact gaps
- `price_series`: selected-ticker price context rows
- `benchmark_ticker`: the market benchmark used for the HMM chart
- `benchmark_price_series`: benchmark price rows
- `benchmark_market_regime_series`: benchmark price + HMM rows used by the chart
- `market_regime_series`: selected-ticker price + HMM rows for date-level review
- `hmm_evaluation_context`: HMM scope, input feature columns, training window,
  source keys, inference-date coverage, and warning metadata
- `pipeline_sections`: stage-separated raw preprocessing, embedding, topic,
  relevance, FinBERT, semantic aggregate, benchmark, and regime rows
- `artifact_keys`: resolved R2 keys used for each evidence section
- `human_semantic_review_status`: current dashboard-level semantic review state
- `recommendation_for_issue_202`: same semantic review recommendation used in the
  AAPL pilot flow
- `warnings`: non-fatal load issues
- `smoke`: machine-readable smoke result with required stage row counts, visual/browser QA
  assertions, and exact missing/tried artifact keys when the dashboard cannot be accepted

## What the article review cards contain

Each `date_groups[]` entry contains:

- `date`
- `regime` with `scope: "date-level"`
- `price` with `scope: "ticker-date"`
- `market_regime_context` with aligned price/HMM warning flags
- `semantic_aggregates[]` with ticker-date semantic aggregate rows
- article counts
- nested `articles[]`

## What the article cards contain

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

The raw rows are intentionally hidden behind expanders so the default page stays
clean and easy to scan.

## Summary / Gate Status tab

The Summary / Gate Status tab is the first explicit readiness surface. It does
not infer readiness from browser-only logic; it renders the API fields listed
above.

Human semantic review can start only when required NLP, HMM, selected-ticker
price, and benchmark price evidence gates are ready. If any required section is
empty, missing, loaded from the cached AAPL bundle, or blocked by HMM manifest /
training-window warnings, the recommendation is `not ready for final human
acceptance` and `human_review_status` is
`blocked_by_missing_pipeline_evidence`.
