# Data Contracts

## Purpose

This document defines the canonical data contracts between layers of the trading system.

The canonical source of truth for concrete schemas is:

- `core/contracts/schemas.py`

If this document and the Python schemas ever disagree, treat that as a blocking issue.

No inter-layer contract may change silently.

---

## Contract principles

All contracts should be:

- explicit
- typed
- versionable
- point-in-time safe
- serializable to JSON / Parquet-compatible rows
- stable enough for downstream consumers

General conventions:

- all timestamps are ISO-8601 strings in UTC unless otherwise specified
- all dates are `YYYY-MM-DD`
- all tickers are uppercase strings
- numerical fields use Python numeric types and must not silently contain strings
- any optional field must be explicitly marked optional in the schema

Schema version metadata:

- `core/contracts/schemas.py` publishes `SCHEMA_VERSION`
- `ArtifactManifestRecord.schema_version` carries the concrete schema version for handoff/audit

---

## Layer 0 contracts

Layer 0 owns all external data ingestion. Wikipedia, Tiingo, SimFin, FRED, and Alpaca
provider calls happen in Layer 0; Layer 1 and later layers read existing R2 archives only.

Layer 0 persists several raw archival datasets for point-in-time safety. These archives are
input artifacts for Layer 1, not additional inter-layer Pydantic contracts. Typed inter-layer
Layer 0 outputs remain `UniverseRecord` and `OHLCVRecord`.

### UniverseRecord

Represents whether a ticker is eligible to be processed on a given date.

Fields:
- `date`
- `ticker`
- `in_universe`
- `tradable`
- `liquid`
- `halted`
- `data_quality_ok`
- `reason`

Use cases:
- point-in-time universe construction
- eligibility masks
- liquidity/tradeability filtering

Implementation notes:
- `core/data/universe.py` builds validated universe records from raw mappings
- daily eligibility masks treated as conjunction of `in_universe`, `tradable`, `liquid`, `!halted`, and `data_quality_ok`
- missing identity fields fail fast; optional fields have sensible defaults

### OHLCVRecord

Represents one adjusted market bar for a ticker on a given date.

Fields:
- `date`
- `ticker`
- `open`
- `high`
- `low`
- `close`
- `volume`
- `adj_close`
- `dollar_volume`

Use cases:
- feature generation
- backtests
- price history storage

Implementation notes:
- `core/data/ohlcv.py` builds validated OHLCV records from raw mappings
- missing price fields fail fast; non-finite values are rejected
- OHLC price relationships are validated before record construction: `high >= low`, `open in [low, high]`, `close in [low, high]`
- volume must be non-negative integer; dollar_volume must be non-negative float

### Layer 0 raw news archive (non-contract artifact)

Purpose:
- preserve point-in-time news availability for training/backtests
- provide deterministic upstream input for Layer 1 sentiment processing

Notes:
- raw news is stored as an archival dataset in Layer 0 storage
- this archive is not a replacement for `NewsSentimentRecord`
- `NewsSentimentRecord` remains a Layer 1 contract produced from raw news
- no schema changes in `core/contracts/schemas.py` are required for this archive

### Layer 0 raw fundamentals archive (non-contract artifact)

Purpose:
- preserve SimFin as-reported fundamentals and earnings-date availability
- keep filing timestamps / effective dates available for point-in-time feature generation
- prevent Layer 1 from calling SimFin directly or accidentally using future restatements

Notes:
- raw fundamentals are stored as archival Layer 0 data in R2, for example under
  `raw/fundamentals/`
- Layer 1 converts these raw records into context features such as valuation ratios,
  leverage, profitability, earnings proximity, and earnings surprises
- this archive is not a replacement for `FeatureRecord`
- no schema changes in `core/contracts/schemas.py` are required unless the project decides
  to promote fundamentals into a typed inter-layer contract later

### Layer 0 raw macro archive (non-contract artifact)

Purpose:
- preserve FRED macro/rate observations available to the system on each run date
- provide deterministic upstream inputs for context features and regime detection
- avoid rewriting historical feature values when upstream macro series are revised

Notes:
- raw macro/rate observations are stored as archival Layer 0 data in R2, for example under
  `raw/macro/`
- Layer 1 converts these raw records into macro and regime-context features such as
  yield-curve slope, policy-rate level, CPI context, and credit/risk proxies
- this archive is not a replacement for `FeatureRecord`
- no schema changes in `core/contracts/schemas.py` are required unless the project decides
  to promote macro observations into a typed inter-layer contract later

---

## Layer 1 contracts

### NewsSentimentRecord

Represents one scored article or one aggregated ticker-day sentiment view.

Fields:
- `date`
- `ticker`
- `headline`
- `source`
- `published_at`
- `sentiment_positive`
- `sentiment_negative`
- `sentiment_neutral`
- `sentiment_score`
- `relevance_score`

Use cases:
- text feature generation
- article-level diagnostics
- sentiment aggregation

Input note:
- generated from Layer 0 raw point-in-time news archives

### FeatureRecord

Represents one fully aligned feature row for one `(date, ticker)`.

Required identity fields:
- `date`
- `ticker`

Feature groups may include:
- market features
- text/sentiment features
- context/fundamental features
- regime features

Input note:
- generated only from Layer 0 R2 archives and manifests; Layer 1 must not call external
  data providers for feature inputs

Examples:
- `returns_1d`
- `returns_21d`
- `realized_vol_21d`
- `volume_ratio`
- `same_day_news_count`
- `news_count_7d`
- `sentiment_score_7d`
- `days_since_last_news`
- `sector_momentum`
- `days_to_earnings`
- `regime_label`
- `regime_confidence`

The schema intentionally allows a flexible `features` dictionary so the system can evolve without hardcoding every feature name into every caller.

---

## Layer 2 contracts

### ScoreRecord

Represents the predictive output for one ticker on one date.

Fields:
- `date`
- `ticker`
- `return_score`
- `pos_prob`
- `rank_score`
- `regime`
- `confidence`
- `model_version`

Interpretation:
- `return_score`: expected relative return score or model score
- `pos_prob`: calibrated probability of positive or relative outperformance
- `rank_score`: normalized rank or cross-sectional ordering metric
- `regime`: regime label used by the model
- `confidence`: model or regime confidence
- `model_version`: artifact/version that produced this score

This record must not contain order instructions.

---

## Layer 3 contracts

### PortfolioRecord

Represents the target portfolio decision for one ticker on one date.

Fields:
- `date`
- `ticker`
- `weight`
- `target_dollars`
- `current_dollars`
- `change_dollars`
- `selection_reason`

Interpretation:
- `weight`: proposed portfolio weight
- `target_dollars`: desired notional exposure
- `current_dollars`: current notional exposure
- `change_dollars`: target minus current
- `selection_reason`: optional human/debug description

This record is still pre-risk and pre-execution.

---

## Layer 4 contracts

### ApprovedOrderRecord

Represents the final risk-approved order intent for one ticker.

Fields:
- `date`
- `ticker`
- `action`
- `target_dollars`
- `approved`
- `rules_triggered`
- `reason`

Interpretation:
- `action`: BUY / SELL / HOLD / REJECT
- `approved`: whether execution may proceed
- `rules_triggered`: list of hard-rule names that altered or rejected the proposal
- `reason`: optional explanation for human/debug use

This is the only order-intent contract that execution should consume.

---

## Layer 5 contracts

### ExecutionFillRecord

Represents the realized outcome of an execution attempt.

Fields:
- `date`
- `ticker`
- `action`
- `shares_target`
- `shares_filled`
- `avg_fill_price`
- `estimated_fill_price`
- `slippage_bps`
- `status`
- `retries`

Use cases:
- execution quality measurement
- daily summaries
- fill reconciliation
- slippage feedback loops

### PipelineManifestRecord

Represents machine-readable completion state for a stage or run.

Fields:
- `run_id`
- `stage`
- `status`
- `started_at`
- `finished_at`
- `input_path`
- `output_path`
- `metadata`

Use cases:
- orchestration
- retries
- monitoring
- downstream stage triggering

---

## Artifact contracts

### ArtifactManifestRecord

Represents the canonical published model or bundle metadata.

Fields:
- `artifact_id`
- `model_version`
- `created_at`
- `stage`
- `metrics_path`
- `diagnostics_path`
- `bundle_path`
- `schema_version`
- `approved`

Use cases:
- validation
- promotion
- inference deployment
- auditability

This manifest should be the canonical handoff object for:
- validation
- deployment
- Oracle refresh

---

## Contract evolution rules

A schema change requires:
1. a dedicated schema migration issue
2. human approval
3. downstream consumer review
4. synchronized code and doc updates

Never:
- silently rename fields
- repurpose fields without migration
- widen or narrow semantics without review
- create parallel undocumented schema versions

---

## Current layer mapping summary

- Layer 0 output → `UniverseRecord`, `OHLCVRecord`
- Layer 0 raw archives → Tiingo news, SimFin fundamentals/earnings, FRED macro/rates
  (R2 artifacts, not separate Pydantic inter-layer contracts)
- Layer 1 output → `NewsSentimentRecord`, `FeatureRecord`
- Layer 2 output → `ScoreRecord`
- Layer 3 output → `PortfolioRecord`
- Layer 4 output → `ApprovedOrderRecord`
- Layer 5 output → `ExecutionFillRecord`, `PipelineManifestRecord`
- Artifact / deployment handoff → `ArtifactManifestRecord`
