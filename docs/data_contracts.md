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
- portfolio and order-intent dollar fields are signed unless a field explicitly says
  otherwise; long-only behavior is enforced by policy/risk rules, not by pretending every
  contract value must be positive

Schema version metadata:

- `core/contracts/schemas.py` publishes `SCHEMA_VERSION`
- `ArtifactManifestRecord.schema_version` carries the concrete schema version for handoff/audit

---

## Layer 0 contracts

Layer 0 owns all external data ingestion. Wikipedia, Alpaca, SimFin, and FRED
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
- `return_score`: expected sector-neutral or cross-sectional alpha score, not a raw market
  beta return forecast
- `pos_prob`: calibrated probability of positive or relative outperformance
- `rank_score`: normalized rank or cross-sectional ordering metric
- `regime`: regime label used by the model
- `confidence`: model or regime confidence
- `model_version`: artifact/version that produced this score

This record must not contain order instructions.

Training note:
- The canonical target should be sector-neutralized forward return or a date-level
  cross-sectional rank. Raw forward returns may be tracked as diagnostics, but they should
  not become the primary target because they cause the model to learn market/sector beta
  instead of stock-specific alpha.

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
- `weight`: signed proposed portfolio weight; positive means long exposure and negative is
  reserved for future hedge/short exposure
- `target_dollars`: signed desired notional exposure after portfolio construction
- `current_dollars`: signed current notional exposure from broker/internal reconciliation
- `change_dollars`: signed target minus current
- `selection_reason`: optional human/debug description

This record is still pre-risk and pre-execution. In the baseline long-only system, Layer 4
must reject or clamp negative single-stock targets unless an explicitly approved hedge
instrument policy is enabled. The schema itself intentionally does not enforce positive-only
weights so defensive hedging, sector hedging, and later long-short books do not require a
trivial numeric-sign migration.

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
- `target_dollars`: signed post-risk target notional; baseline long-only rules should keep
  ordinary equity targets non-negative
- `approved`: whether execution may proceed
- `rules_triggered`: list of hard-rule names that altered or rejected the proposal
- `reason`: optional explanation for human/debug use

This is the only order-intent contract that execution should consume.

Future expansion note:
- `BUY` and `SELL` are sufficient for current long-only target rebalancing. Opening shorts,
  covering shorts, options hedges, and margin-specific order behavior require explicit
  execution-contract review and likely a schema migration to represent position effect,
  instrument type, borrow/locate status, and margin requirements.

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
- Layer 0 raw archives → Alpaca news, SimFin fundamentals/earnings, FRED macro/rates
  (R2 artifacts, not separate Pydantic inter-layer contracts)
- Layer 1 output → `NewsSentimentRecord`, `FeatureRecord`
- Layer 2 output → `ScoreRecord`
- Layer 3 output → `PortfolioRecord`
- Layer 4 output → `ApprovedOrderRecord`
- Layer 5 output → `ExecutionFillRecord`, `PipelineManifestRecord`
- Artifact / deployment handoff → `ArtifactManifestRecord`
