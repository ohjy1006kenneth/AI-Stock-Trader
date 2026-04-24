# TODO

## Blocked on human decision
<!-- Codex adds entries here when it blocks. Human removes them after resolving. -->
- [ ] #87 blocked pending human approval of schema migration #103 for sentence-level
  news preprocessing fields.

## Schema migrations pending
<!-- Codex adds entries here when a schema change issue is created -->
- [ ] #103 Add per-sentence news preprocessing fields to `NewsSentimentRecord`.

## Known gaps — issues not yet created
- [x] context_features.py not yet implemented (macro, rates, earnings calendar)
- [ ] FinBERT source credibility weighting missing from sentiment_features.py
- [ ] HMM regime detection needs training data pipeline before it can be fitted
- [x] data/sample/ fixture files do not exist yet — needed for all unit tests

## Technical debt
<!-- Things that work but should be improved -->
- [x] `_backfill_historical_prices` re-fetches all OHLCV from Alpaca on every run to populate
  `quality_window`, even when R2 files already exist. Should read existing parquet from R2
  instead of re-fetching, so subsequent runs skip Alpaca entirely for tickers already archived.
- [x] `_backfill_historical_prices` reads all price parquets from R2 to build `quality_window`
  even when all universe masks already exist downstream. Should check if universe masks are
  complete first and skip quality_window population entirely when not needed.
- [x] `CloudflareR2Client` has no `exists()` method; `R2Writer.exists()` falls back to
  `list_keys()` (paginated ListObjectsV2) for every existence check. Should add a
  `head_object`-based `exists()` to `CloudflareR2Client`. Affects all data families
  (prices, universe, news, fundamentals, macro) — thousands of slow calls per backfill run.
- [ ] Fundamentals archive is written as a single `raw/fundamentals/{from}_to_{to}.parquet`
  file, unlike prices (per-ticker) and news/universe (per-day). Shard per-ticker with
  per-filing-date history (e.g. `raw/fundamentals/{TICKER}/{report_date}.parquet`, or
  per-ticker historical files analogous to price parquets) so partial SimFin progress is
  persisted, failures recover batch-by-batch, and incremental updates don't require
  refetching the full range. Requires updating `raw_fundamentals_path`,
  `_write_fundamentals_archive`, `validate_layer0_archive.py`, and Layer 1 consumers.
- [ ] Macro archive is written as a single `raw/macro/{from}_to_{to}.parquet` file. Shard
  to per-day (`raw/macro/{YYYY-MM-DD}.parquet`) so FRED backfills match the per-day
  cadence used by news/universe and partial progress is persisted. Requires updating
  `raw_macro_path`, `_write_macro_archive`, `validate_layer0_archive.py`, and Layer 1
  consumers.

## Discovered during development
<!-- Codex adds here when it notices something out of scope for the current task -->
- [ ] Universe validation still reports a few historical symbol-identity mismatches
  (e.g., UAA, AGN, IQV transition events); evaluate date-bounded symbol mapping
  policy to reduce residual event-boundary violations.
