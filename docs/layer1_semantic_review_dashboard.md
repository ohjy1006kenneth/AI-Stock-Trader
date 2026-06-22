# Layer 1 Semantic Review Dashboard

## Purpose

`app.lab.semantic_review_dashboard` serves a persistent, read-only browser dashboard for
human semantic review of Layer 1 evidence. It is a live local server, not a generated static
HTML artifact.

Use it to inspect:
- run status, machine-integrity gates, and #202 recommendation
- FinBERT probabilities, polarity, score, and relevance
- raw headlines, snippets, source, article timestamp, and duplicate headline groups
- topic and ticker-day sentiment features
- HMM regime label, confidence, and bear/sideways/bull probabilities
- exact source artifact keys for raw news, scored news, topic, sentiment, regime, and feature
  shards

The dashboard is read-only. Human acceptance or rejection must be recorded on the relevant
GitHub issue; the dashboard does not write R2 objects or mutate feature artifacts.

## Run Against The Current AAPL Pilot

From the repository root:

```bash
./.venv/bin/python -m app.lab.semantic_review_dashboard \
  --run-id layer1-aapl-accuracy-2026-05-06-to-2026-05-28-v4-after-pr221 \
  --from-date 2026-05-06 \
  --to-date 2026-05-28 \
  --ticker AAPL \
  --host 0.0.0.0 \
  --port 8765
```

Then open `http://127.0.0.1:8765/` or check:

```bash
curl -fsS http://127.0.0.1:8765/health
```

A normal checkout now contains the current pilot diagnostics bundle under
`artifacts/reports/diagnostics/`, so the command above loads the evidence JSON,
human-review CSV, and accuracy report without needing a private home-path
override.

## Alternate Sources

If you copied the bundle elsewhere, point the dashboard at that directory:

```bash
./.venv/bin/python -m app.lab.semantic_review_dashboard \
  --run-id <run_id> \
  --artifact-dir /path/to/diagnostic-artifacts
```

Use exact file paths when the artifacts were split apart:

```bash
./.venv/bin/python -m app.lab.semantic_review_dashboard \
  --run-id <run_id> \
  --evidence-json /path/evidence.json \
  --review-csv /path/review.csv \
  --accuracy-report /path/accuracy.json \
  --no-r2
```

Use a local mock R2 tree when you want the dashboard to read the mock object-store
instead of local files:

```bash
./.venv/bin/python -m app.lab.semantic_review_dashboard \
  --run-id <run_id> \
  --from-date <YYYY-MM-DD> \
  --to-date <YYYY-MM-DD> \
  --ticker AAPL \
  --local-r2-root data/runtime/r2_mock
```

## API

- `GET /health` returns server health and the configured run id.
- `GET /api/review` returns the current dashboard payload.

`/api/review` supports these query filters:

- `date=YYYY-MM-DD`
- `from_date=YYYY-MM-DD`
- `to_date=YYYY-MM-DD`
- `ticker=AAPL`
- `search=<text>`
- `min_relevance=0.75`
- `review_status=pending`

The server reloads artifacts on every `/api/review` request, so a long-running dashboard can
be reused after evidence files are refreshed.
