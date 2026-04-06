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

---

## Layer 0 contracts

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
- Layer 1 output → `NewsSentimentRecord`, `FeatureRecord`
- Layer 2 output → `ScoreRecord`
- Layer 3 output → `PortfolioRecord`
- Layer 4 output → `ApprovedOrderRecord`
- Layer 5 output → `ExecutionFillRecord`, `PipelineManifestRecord`
- Artifact / deployment handoff → `ArtifactManifestRecord`