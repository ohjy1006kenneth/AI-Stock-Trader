# TODO

## Blocked on human decision
<!-- Codex adds entries here when it blocks. Human removes them after resolving. -->

## Schema migrations pending
<!-- Codex adds entries here when a schema change issue is created -->

## Known gaps — issues not yet created
- [x] context_features.py not yet implemented (macro, rates, earnings calendar)
- [x] FinBERT source credibility weighting missing from sentiment_features.py
- [x] HMM regime detection needs training data pipeline before it can be fitted
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
- [ ] Macro archive layout still mixes legacy observation-date shards with run-date
  readiness snapshots under `raw/macro/{YYYY-MM-DD}.parquet`. Issue `#148` should
  consolidate the convention and document/migrate any backward-compatibility cleanup
  needed across backfill, validation, and Layer 1 consumers.
- [ ] `_SHARES_OUTSTANDING_KEYS` in `core/data/layer0_pipeline.py` duplicates `_SHARES_KEYS`
  in `core/features/fundamentals_features.py`; consolidate SimFin share-key ownership before
  the next fundamentals or market-cap filter change.

## Discovered during development
<!-- Codex adds here when it notices something out of scope for the current task -->
- [ ] Date-bounded Wikipedia identity resolution now covers the known `UA`/`UAA`
  and `Q`/`IQV` transition boundaries, but residual non-aliased delisting or
  archive-gap mismatches (for example stale `AGN` boundary holes in old
  membership exports) still need data cleanup or an explicit exception policy.
- [ ] SimFin still lacks direct coverage for several current S&P 500 symbols even after
  per-ticker recovery and safe alias rewrites (confirmed on 2026-05-13 for `BF-B`,
  `BRK-B`, `GEV`, `SOLV`, `SW`, `TKO`, `VLTO`). Decide whether to maintain explicit
  provider-gap exceptions, source a secondary fundamentals provider for those names, or
  relax readiness rules only with explicit human approval.
