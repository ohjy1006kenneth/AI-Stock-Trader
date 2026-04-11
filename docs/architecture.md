# Architecture

## Overview

This repository implements a production-oriented quantitative trading system for U.S. equities.

The target data stack is:
- historical universe membership from Wikipedia revision history
- canonical historical OHLCV and raw news archives from Tiingo
- point-in-time fundamentals and earnings dates from SimFin
- macro and rate context from FRED
- live market data, broker state, and execution from Alpaca

The system is designed around four deployment surfaces:

1. **Laptop (development)**
   Used for research, coding, local tests, and pull request preparation.

2. **Cloud (heavy compute) — Modal**
   Used for:
   - FinBERT inference on news text
   - XGBoost inference and retraining
   - larger offline backtests and packaging jobs

3. **Raspberry Pi 5 (edge runtime / orchestration)**
   Used for:
   - cron-triggered scheduling and orchestration
   - containerized runtime execution (Docker)
   - OpenClaw runtime execution inside container
   - market/account synchronization
   - lightweight portfolio construction
   - hard risk checks
   - order execution through Alpaca
   - monitoring and reporting

4. **Object storage (Cloudflare R2)**
   Used as the persistent shared handoff layer between Pi and cloud:
   - raw Layer 0 data archives
   - feature tables
   - model scores
   - approved order proposals
   - execution reports
   - manifests
   - model bundles and diagnostics

   Raw data snapshots are persisted to R2 so Layer 1 and later milestones never need to
   call external data providers directly.

The design principle is:
- heavy compute in the cloud (Modal)
- lightweight orchestration on the Pi
- deterministic execution
- shared state in R2 (accessible by both Pi and Modal)
- Pi SSD used as a local cache for raw fetched data and intermediate state
- explicit contracts between every layer

---

## Data sources

| Data type | Source | Notes |
|---|---|---|
| Historical OHLCV (adjusted) | Tiingo EOD API | Canonical long-history store; stable `permaTicker` identity covers delisted/acquired names |
| Historical and live raw news | Tiingo News API | Raw article text with ticker tags for NLP backtests and daily inference |
| S&P 500 universe membership | Wikipedia edit history | Point-in-time constituent list; never use today's snapshot for history |
| Fundamentals / earnings dates | SimFin | As-reported filing data to avoid future restatements leaking backward |
| Macro / rates | FRED | Fed funds, Treasury yields, CPI, and other regime/context inputs |
| Live daily prices / broker state / execution | Alpaca Market Data + Trading API | Current-day bar snapshot, reconciliation source of truth, and order routing |

### Universe construction note
Wikipedia's S&P 500 page revision history provides historical constituent changes at no cost.
The revision history — not the current page — must be scraped to obtain point-in-time membership.
This approach is accurate enough for personal-system backtesting but may have gaps around
spinoffs and rapid constituent changes. Tiingo's `permaTicker` then provides the stable
security identity needed to stitch those historical constituents to surviving, delisted,
acquired, and symbol-changed names without survivorship bias.

### Historical vs. live market data note
Historical price and news storage should be treated as a Tiingo-backed canonical archive.
Alpaca market data is used only for the live daily bar snapshot that the Pi needs near the
close or before the next open. Any Alpaca-sourced live bar written into the raw store must be
normalized into the same contract shape as Tiingo and later reconciled into the canonical
historical archive.

---

## Storage model

### R2 — shared persistent state (Pi ↔ Modal)

R2 is the canonical source of truth for artifacts that cross environment boundaries.
Both the Pi and Modal jobs read and write to R2.

```
r2/
  raw/
    prices/           # OHLCV Parquet, one file per Tiingo permaTicker
    news/             # Raw news as JSON Lines (one article per line)
    universe/         # Daily eligibility masks as CSV
    fundamentals/     # SimFin as-reported point-in-time fundamentals and earnings data
    macro/            # FRED macro/rate observations as available on the run date
    reference/        # Symbol/permaTicker/security master snapshots
  processed/
    features/         # Feature tables as Parquet (dates × tickers × features)
    scores/           # Layer 2 score outputs as Parquet
    orders/           # Approved order proposals as CSV
  artifacts/
    bundles/          # Packaged model bundles
    diagnostics/      # SHAP plots, metrics JSON
    manifests/        # ArtifactManifestRecord JSON files
  reports/
    daily/            # Daily execution summaries
    backtests/        # Backtest result JSON and equity curves
```

### Pi SSD — local cache and runtime state

The Pi's internal SSD is used for:
- locally cached raw data to avoid redundant API calls
- runtime ledger (SQLite) for fills and positions
- short-lived intermediate state during the daily pipeline run
- daily order CSV files for human inspection

The Pi SSD is not the canonical record for anything that Modal or a future compute surface needs.
Any artifact that must survive a Pi restart or be accessed by Modal must be written to R2.

### Format conventions

| Data type | Format | Reason |
|---|---|---|
| OHLCV history | Parquet (per ticker) | 10–50x faster than CSV; columnar reads; `pd.read_parquet()` |
| Feature tables | Parquet (partitioned by date) | Frequently read by model pipeline |
| Model scores | Parquet | Small, fast to read |
| News raw archive | JSON Lines | One article per line; easy to stream |
| Fundamentals archive | Parquet | Point-in-time company fundamentals; efficient ticker/date joins |
| Macro/rates archive | Parquet or CSV | Small daily series; preserve observations used by a run |
| Sentiment scores | Parquet | Processed output from FinBERT pipeline |
| Universe eligibility masks | CSV | Small; human-inspectable |
| Daily order files | CSV | Human-readable for debugging |
| Fill ledger | SQLite | Running record; inspectable without code |
| Manifests | JSON | Machine-readable; small |

General rule: Parquet for anything large or frequently read by the model pipeline.
CSV or SQLite for anything a human needs to inspect without code.

---

## Layered system design

The system follows a strict layered architecture.

### Layer 0 — Data & Universe Selection

Layer 0 guarantees that all downstream layers operate on clean, honest, point-in-time data.

Responsibilities:
- construct a point-in-time eligible universe (Wikipedia revision history + Tiingo security history)
- avoid survivorship bias — use historical constituent lists, never today's index
- apply daily liquidity and tradeability filters
- use Tiingo adjusted OHLCV as the canonical historical market data store
- ingest raw Tiingo news with point-in-time timestamps and raw text
- ingest SimFin as-reported fundamentals and earnings dates as point-in-time raw context
- ingest FRED macro and rate series as point-in-time raw context
- detect stale, missing, or corrupted market data
- generate daily eligibility masks and quality flags
- persist every external data source needed by Layer 1 into R2 before feature generation runs

**Layer 0 operates in two distinct modes:**

**Historical backfill (one-time, run before first live trading):**
Builds the complete R2 database that Layer 1 reads for model training and backtesting.
Runs on laptop or Modal — not the Pi. The Pi has no role in the backfill.
- Entrypoint: `app/lab/data_pipelines/backfill_layer0.py`
- Fetches full OHLCV history (e.g. 2014–present) for all historical constituents keyed by Tiingo `permaTicker`
- Fetches historical raw news archive from Tiingo → `r2://raw/news/YYYY-MM-DD.jsonl` per date
- Fetches SimFin as-reported fundamentals and earnings dates → `r2://raw/fundamentals/`
- Fetches FRED macro/rate series → `r2://raw/macro/`
- Computes eligibility masks for all historical dates → `r2://raw/universe/YYYY-MM-DD.csv`
- Idempotent: safe to re-run; skips dates already stored in R2

**Daily incremental (runs on Pi after every market close):**
Appends today's data to the existing R2 database. Assumes the backfill has already been run.
- Entrypoint: `app/pi/fetchers/layer0.py`
- Fetches today's live market bar snapshot from Alpaca Market Data → appends to the canonical raw price store
- Fetches today's raw news from Tiingo → writes `r2://raw/news/YYYY-MM-DD.jsonl`
- Refreshes newly available SimFin filings / earnings-calendar data → writes `r2://raw/fundamentals/`
- Refreshes FRED macro/rate observations available for the run date → writes `r2://raw/macro/`
- Recomputes today's eligibility mask → writes `r2://raw/universe/YYYY-MM-DD.csv`
- Writes `PipelineManifestRecord` to R2 on completion or failure

**Liquidity filters (applied daily using rolling 20-day window):**
- minimum average daily volume (ADV): $1M/day (lower bound for personal system)
- minimum price: $5 (exclude penny stocks)
- minimum market cap: configurable

**Data quality checks (run on every data pull):**
- flag any bar where volume is zero
- flag any single-day price move > 40% (likely data error unless known event)
- flag any ticker with more than N consecutive missing bars
- suppress trading that ticker for the day if quality checks fail

**Outputs:**
- point-in-time universe membership per date (CSV eligibility mask)
- adjusted OHLCV Parquet files per ticker
- quality flags per ticker per day: tradeable / halted / data-error / illiquid
- raw news archive (JSON Lines) for Layer 1 processing
- raw SimFin fundamentals and earnings-date archive for Layer 1 context features
- raw FRED macro/rate archive for Layer 1 context and regime features

### Layer 1 — Feature Generation

Layer 1 converts existing Layer 0 R2 data into aligned numerical features indexed by
`(date, ticker)`. Layer 1 does not fetch from Wikipedia, Tiingo, SimFin, FRED, or Alpaca;
it reads only the raw archives and manifests produced by Layer 0.

The quality of features matters more than model sophistication. A well-engineered feature set
with XGBoost will outperform a poorly engineered one with a deep neural network.

All features must satisfy: **a feature value used on date T can only use information available
before market open on date T.** Any violation is lookahead bias.

Final feature table shape: `(N_dates × N_tickers)` rows × `M_features` columns.

#### Text / NLP branch

**Pipeline order (must be executed in this sequence):**

```
RAW ARTICLES (Tiingo news)
  → Step 1: Preprocessing
      Clean text, remove boilerplate, split into sentences, tag ticker mentions
  → Step 2a: Sentence Transformers (per article)
      Model: all-mpnet-base-v2
      Output: 384-dim embedding per article
  → Step 2b: BERTopic (across all articles today)
      Input: all articles in the universe today
      Output: topic assignments and probabilities per article
      Recommended: 20–50 topics; tune minimum topic size
  → Step 3a: Relevance filter (cosine similarity)
      Keep only financially relevant articles using reference embeddings
      Down-weight articles far from financial-event reference cluster
  → Step 3b: Topic sentiment
      Feed topic-grouped articles into FinBERT to get sentiment per topic
  → Step 4: FinBERT scoring
      Model: ProsusAI/finbert (finance-domain fine-tuned BERT)
      Input: relevance-filtered articles, one sentence/chunk at a time
      Output: (positive, negative, neutral) probabilities per article
  → Step 5: Aggregation
      Combine FinBERT scores + topic assignments + source credibility weights
      into per-ticker per-day features
```

**Output features from NLP branch:**
```python
ticker_sentiment_score    = weighted_avg(positive - negative)
ticker_sentiment_strength = avg(max(positive, negative))
ticker_article_count      = number of articles today
ticker_sentiment_std      = std of scores (disagreement signal)
ticker_exposed_to_hot_topic = is ticker in a trending topic?
topic_sentiment_score     = FinBERT sentiment per topic
topic_momentum            = is topic growing or shrinking in volume?
```

Note: do not use raw Sentence Transformer dimensions (384) as XGBoost features.
Reduce to 10–20 dimensions via PCA or UMAP before feeding downstream.

#### Market branch

```python
# Momentum
returns_1d   = close.pct_change(1)
returns_5d   = close.pct_change(5)
returns_21d  = close.pct_change(21)
returns_63d  = close.pct_change(63)
momentum     = close.pct_change(21).shift(1)   # skip most recent day to avoid short-term reversal

# Volatility
realized_vol_5d  = returns.rolling(5).std() * sqrt(252)
realized_vol_21d = returns.rolling(21).std() * sqrt(252)
vol_ratio        = realized_vol_5d / realized_vol_21d    # vol regime change indicator
atr              = average_true_range(high, low, close, window=14)

# Trend
sma_20        = close.rolling(20).mean()
sma_50        = close.rolling(50).mean()
sma_200       = close.rolling(200).mean()
price_vs_sma20 = close / sma_20 - 1             # how extended from mean
golden_cross  = (sma_50 > sma_200).astype(int)
rsi_14        = compute_rsi(close, 14)
macd_signal   = macd_line - signal_line

# Volume
volume_ratio      = volume / volume.rolling(20).mean()   # unusual volume
price_volume_corr = rolling_correlation(returns, volume, 10)
dollar_volume     = close * volume                       # liquidity proxy

# Gap
overnight_gap = open / close.shift(1) - 1               # gap up/down at open

# Cross-asset
spy_return_1d    = SPY daily return
vix_level        = VIX close
vix_change       = VIX 5-day change
sector_etf_ret   = return of stock's sector ETF (XLK, XLF, XLE, etc.)
stock_vs_sector  = stock return - sector ETF return      # idiosyncratic return
```

#### Context branch

```python
# Fundamentals (quarterly, forward-filled between reports)
# Source: Layer 0 SimFin as-reported point-in-time archive
pe_ratio        = price / earnings_per_share
pb_ratio        = price / book_value_per_share
debt_to_equity  = total_debt / shareholders_equity
roe             = net_income / shareholders_equity
revenue_growth  = (revenue_t - revenue_t4) / revenue_t4
earnings_surprise = (actual_eps - estimated_eps) / abs(estimated_eps)

# Macro (daily)
# Source: Layer 0 FRED archive; persist observed values so later revisions do not rewrite history
fed_funds_rate  = current Fed funds rate
yield_10y       = 10-year Treasury yield
yield_2y        = 2-year Treasury yield
yield_curve     = yield_10y - yield_2y    # inversion = recession signal
credit_spread   = HYG vs LQD spread       # risk appetite proxy
dollar_index    = DXY level and change

# Earnings calendar proximity (critical context feature)
days_to_earnings  = trading days until next earnings announcement
pre_earnings_flag = 1 if days_to_earnings <= 5 else 0
post_earnings_flag = 1 if days_since_earnings <= 2 else 0

# Sector / factor
sector_momentum          = sector ETF 21-day return
sector_relative_strength = stock's 63d return rank within its sector
```

### Layer 1.5 — Regime Detection

Regime detection asks "what kind of market are we in right now?" and routes to a model
trained specifically for that regime. Training one XGBoost on 10 years of mixed-regime data
produces a model that is mediocre in every regime rather than excellent in any.

**Method: Hidden Markov Model (HMM)**

```python
# Observations fed to HMM
observations = [
    daily_return,         # SPY or universe avg return
    realized_volatility,  # rolling 21-day vol
    vix_level,            # implied volatility
    yield_curve_slope,    # 10y - 2y Treasury spread
]
# Shape: (N_trading_days, 4)
```

**Output:** probability distribution over K regimes per day:
```python
{"regime_bull": 0.82, "regime_bear": 0.05, "regime_sideways": 0.13, "regime_label": "bull"}
```

**Implementation notes:**
- Start with K=3 (bull, bear, sideways); optionally K=4 adding crisis/crash
- Use BIC score to compare K=2,3,4 and select best
- Algorithm: Baum-Welch (Expectation-Maximization) — no manual labeling required
- Read macro regime inputs such as Fed funds, CPI, and yield-curve measures from the
  Layer 0 FRED archive and market-state inputs such as SPY/VIX from the market data branch
- **Lookahead bias:** never train HMM on full history and use its labels for backtesting.
  Correct approach: walk-forward (train on data up to T, label T, slide forward).
  Practical approximation: fit once on first few years, re-fit quarterly.
- Alternative to HMM: Gaussian Mixture Model (GMM) — simpler, no sequential dynamics,
  often works just as well at daily frequency

### Layer 2 — Predictive Model (XGBoost)

Layer 2 produces predictive scores. It does not decide weights or orders.

**Target variable (most important design decision):**

Wrong — raw forward return includes market beta, model learns market direction:
```python
target = stock_return_next_5d
```

Correct — sector-neutralized return, model learns stock-specific alpha:
```python
target = stock_return_next_5d - sector_etf_return_next_5d
```

Best — cross-sectional rank, removes good-day vs. bad-day effects entirely:
```python
# Rank stocks by forward return within each date, normalize to [-1, +1]
target = cross_sectional_rank(stock_return_next_5d, date)
```

Use 5-day forward return as the prediction horizon. Test 1, 5, and 10 days and compare IC.

**Regime-specific model architecture:**
Train one XGBoost model per regime (bull, bear, sideways). At inference, the HMM identifies
the current regime and the corresponding model is activated.

**Recommended XGBoost hyperparameters (starting point):**
```python
xgb.XGBRegressor(
    n_estimators=500,
    max_depth=4,           # shallow trees — reduces overfitting
    learning_rate=0.01,    # low LR + more trees = better generalization
    subsample=0.7,
    colsample_bytree=0.7,
    reg_alpha=0.1,         # L1 regularization
    reg_lambda=1.0,        # L2 regularization
    min_child_weight=20,   # require many samples per leaf
)
```

**Calibration:** Raw XGBoost scores are not probabilities. Use Platt scaling (logistic
regression fitted on validation scores vs. actual binary outcomes) to convert to calibrated
probabilities if Layer 3 needs `pos_prob` for position sizing.

**Monitoring:** Run SHAP weekly on live predictions. If top feature importances shift
dramatically from training, the model is likely in an out-of-distribution regime. Reduce
position sizes until it stabilizes.

**Total feature count:** typically 50–150 features depending on how many branches are active.

### Layer 3 — Portfolio Decision

Layer 3 translates predictive signals into target portfolio intent.
Signal quality and portfolio quality are separate problems. A great Layer 2 with a naive
Layer 3 will underperform a decent Layer 2 with a sophisticated Layer 3.

**Step 1 — Contextual Bandit (pre-filter)**

The bandit selects the 30–50 candidate stocks the optimizer will work with, from the full
~800 stock universe. This is a smart pre-filter that learns which stocks in which contexts
tend to deliver after optimization.

- Context input: regime, VIX level, VIX 5-day change, SPY momentum, yield curve, sector in favor
- Candidate input: all tickers with Layer 2 scores (rank_score, regime_confidence)
- Output: 30–50 tickers with selection probabilities
- Balances exploration (uncertain stocks) vs. exploitation (historically reliable stocks)
- Learns faster than full RL because the decision is simpler (which stocks to include)
- Reaches useful behavior in weeks of paper trading

**Step 2 — Mean-Variance Optimizer**

Takes expected returns (XGBoost scores) and finds weights maximizing expected return
subject to portfolio-level risk constraints.

Inputs:
- expected return per ticker (from Layer 2)
- covariance matrix estimated from past 252 days of returns

Key design choices:
- **Turnover penalty:** adds a cost term so the optimizer only rebalances when expected
  return improvement genuinely justifies transaction costs. Without this, the optimizer
  restructures the entire portfolio on minor score shifts.
- **Covariance matrix:** estimating 500×500 requires 124,000 pairwise correlations from
  252 points — statistically unreliable. Use either a factor model (market + sector +
  idiosyncratic components) or a shrinkage estimator (Ledoit-Wolf) to stabilize weights.

Output: target weight per ticker (fraction of total portfolio).

### Layer 4 — Risk Engine

Layer 4 is a completely separate, model-free hard-rule layer. It does not care what XGBoost
predicted, what the optimizer decided, or what the bandit selected. It applies after
optimization and cannot be gamed by the optimizer.

**Position-level rules:**

| Rule | Threshold | Action |
|---|---|---|
| Max position size | 5–10% of portfolio | Cap weight; redistribute excess to cash or other positions |
| ADV cap | 1% of stock's average daily volume | Reduce order to 1% ADV |
| Signal staleness | >N consecutive missing bars | Suppress that ticker for the day |

**Portfolio-level rules:**

| Rule | Threshold | Action |
|---|---|---|
| Sector concentration cap | 25% per sector | Trim positions in sector proportionally |
| Beta cap | 1.0–1.3 portfolio weighted beta | Reduce highest-beta positions |
| Correlation cap | 0.70–0.80 (30-day rolling pair) | Keep higher-scored ticker, reduce the other |
| Daily loss limit | -2% intraday | Reduce gross exposure to 50% (circuit breaker) |
| Max leverage | 1.0x (long-only cash account) | Scale all weights proportionally |

**Drawdown-based exposure scaling (most impactful rule):**

| Drawdown from peak | Exposure multiplier |
|---|---|
| 0% – 5% | 1.00x (full) |
| 5% – 10% | 0.75x |
| 10% – 15% | 0.50x |
| > 15% | 0.25x |

This rule automatically reduces risk when the system is in a bad patch without any
discretionary intervention.

**Fat-finger checks (run last):**
- order > 20% of account in a single ticker → reject
- ticker not in today's eligible universe → reject
- limit price < 20% or > 200% of previous close → reject

### Layer 5 — Execution Engine

Layer 5 executes approved trades. It should be deterministic, auditable, and simple.
It does not make decisions — it follows instructions from Layer 4.

**Reconciliation-first protocol:**
Before placing any new orders, fetch Alpaca's actual account state and reconcile against
internal state. Alpaca's state is always the authority. Only after reconciliation does Layer 5
calculate delta orders needed to reach Layer 4 targets.

**Weight → shares conversion:**
Layer 4 outputs dollar amounts. Layer 5 converts to whole share counts (round down).
Rounding down means slightly underinvested — intentional; avoids accidental overinvestment.

**Order lifecycle:**
```
place limit order
→ monitor every N minutes
→ if not fully filled after 30 minutes (liquid stocks): cancel remainder, reprice aggressively
→ log final fill vs. estimated fill
```

**Fill quality feedback loop:**
- Weekly: compare realized slippage to model assumptions
- If realized slippage consistently > backtest assumption: increase Layer 3 turnover penalty
  for that ticker, tighten Layer 4 ADV cap for that ticker
- This loop separates a system that improves from one that repeats the same execution errors

---

## Backtesting methodology

### Walk-forward validation (mandatory)

Standard k-fold cross-validation is wrong for financial time series — it allows future data
to contaminate training. Walk-forward validation enforces strict temporal ordering.

Structure (expanding window — preferred):
```
initial_training_window: train 2010–2016, test 2017
walk_1:                  train 2010–2017, test 2018
walk_2:                  train 2010–2018, test 2019
walk_3:                  train 2010–2019, test 2020  ← COVID crash
walk_4:                  train 2010–2020, test 2021
walk_5:                  train 2010–2021, test 2022  ← bear market
walk_6:                  train 2010–2022, test 2023

Final reported performance = concatenation of all out-of-sample test periods
```

### True holdout set

Every time walk-forward performance is observed and the system is tuned, the walk-forward
period becomes slightly contaminated. Reserve the most recent year of data at the very start
and never look at it during development. Run it exactly once after all design decisions are
final. If holdout performance diverges significantly from walk-forward, the system is overfit.

### Backtest validity checklist

A backtest is only trustworthy if all of the following pass:

- [ ] Point-in-time universe (Wikipedia revision history, not today's S&P 500)
- [ ] No survivorship bias — delisted and acquired companies included historically via Tiingo `permaTicker`
- [ ] Adjusted prices used for all model training
- [ ] No lookahead bias in any feature (feature on date T uses only data before T's open)
- [ ] HMM fitted walk-forward, not on full history
- [ ] Transaction costs modeled (~13 bps round trip as a baseline)
- [ ] Realistic fill assumptions (order < 1% of ADV)
- [ ] Walk-forward structure with expanding or rolling window
- [ ] True holdout reserved and untouched

### Minimum performance thresholds before paper trading

| Metric | Threshold |
|---|---|
| Net Sharpe ratio | > 1.0 |
| Maximum drawdown | < 25% |
| Average IC | > 0.05 |
| ICIR | > 1.0 |
| Positive in all walk-forward regimes | Required |

---

## Runtime deployment model

### Laptop
The laptop is used for:
- development
- issue implementation
- local unit/integration tests
- notebooks and experiments
- pull requests

### Cloud (Modal)
Modal is used for:
- FinBERT inference (GPU)
- XGBoost training and inference
- retraining jobs
- artifact packaging
- larger validation and backtesting runs

Modal reads from and writes to R2. It does not hold persistent state.

### Raspberry Pi
The Pi is used for:
- cron scheduling on the host
- Docker container runtime for the edge process
- OpenClaw runtime inside the Pi container
- step orchestration
- local state checks (Pi SSD cache)
- lightweight portfolio and bandit logic
- hard risk rule enforcement
- Alpaca order execution
- Telegram summaries and anomaly alerts

The Pi SSD is a local cache. Any artifact that must be durable or cross-environment
must be written to R2.

### Object storage (Cloudflare R2)
R2 is the source of truth for:
- raw Layer 0 data snapshots from Wikipedia, Tiingo, SimFin, FRED, and Alpaca
- processed feature tables
- manifests
- model scores
- bundles
- execution reports
- diagnostics

---

## Canonical runtime flow

### Prerequisites (run once before live trading begins)

The following must be completed before the daily loop can run:

1. Run historical backfill: `python app/lab/data_pipelines/backfill_layer0.py --from-date 2014-01-01 --to-date <today>`
   - Builds complete Tiingo-backed OHLCV Parquet database in R2 — one file per stable security identity
   - Builds Tiingo historical news archive in R2 — one JSON Lines file per date
   - Builds historical eligibility masks in R2 — one CSV per date
   - Builds SimFin as-reported fundamentals and earnings-date archives in R2
   - Builds FRED macro/rate archives in R2
2. Train and validate the XGBoost models (Milestones 2, 2.5, 3, 4)
3. Deploy validated model bundle to R2 / cloud Oracle

Without the historical backfill, Layer 1 feature generation and model training cannot run.

### After market close (daily, Pi)
1. Pi runs Layer 0 incremental: fetches today's live bar snapshot from Alpaca, today's news from Tiingo, newly available SimFin data, and current FRED observations, then appends normalized raw data to R2
2. Pi triggers Modal: build/refresh aligned feature table for today from existing Layer 0 R2 data → R2
3. Modal reads Layer 0 manifests and fails closed if required raw inputs are missing
4. Modal runs FinBERT + XGBoost inference → scores to R2
5. Pi reads scores from R2
6. Pi runs contextual bandit + optimizer → portfolio proposal
7. Pi applies hard risk rules → approved orders
8. Pi stores approved order proposal to R2 and local SSD

### Before / at next market open
1. Pi fetches current Alpaca account state
2. Pi reconciles broker state vs. internal state (Alpaca wins)
3. Pi translates approved targets to executable orders
4. Pi places orders through Alpaca
5. Pi monitors fills, retries stale orders
6. Pi logs execution quality to R2 and local SQLite ledger

### Throughout the day
1. Pi monitors runtime health
2. Detects mismatches or stale pipeline stages
3. Alerts via Telegram on anomalies or daily loss limit triggers

---

## Validation philosophy

No candidate is promotable merely because:
- the code runs
- the artifact exports
- the bundle exists

A candidate becomes promotable only if it survives:
- real-data training
- honest out-of-sample walk-forward evaluation
- cost-aware review (net Sharpe > 1.0 after transaction costs)
- risk-aware review
- true holdout confirmation

---

## Architectural rules

1. Prediction is separate from portfolio construction.
2. Portfolio construction is separate from risk enforcement.
3. Risk enforcement is separate from execution.
4. State belongs in R2 (cross-environment) or Pi SSD (local cache only).
5. Schemas are explicit and versioned.
6. No layer may silently change another layer's contract.
7. The Pi orchestrates; it does not perform heavy compute.
8. Modal does not own persistent state — R2 does.
9. The Alpaca broker state is always the authority during reconciliation.
10. Every feature used on date T must use only data available before T's market open.

---

## Repository mapping

- `app/lab/` — cloud training, validation, packaging, and one-time data backfill jobs
  - `app/lab/data_pipelines/` — historical backfill entrypoints (run on laptop or Modal, not Pi)
- `app/cloud/` — cloud inference surface (Modal)
- `app/pi/` — edge runtime surface (Pi, daily incremental only)
- `core/contracts/` — shared inter-layer schemas
- `core/data/` — point-in-time universe/data logic (Wikipedia, Tiingo-backed historical data, SimFin/FRED raw context ingestion, Alpaca live-bar normalization)
- `core/features/` — market, NLP, context feature logic over existing Layer 0 archives
- `core/models/` — XGBoost, HMM, calibration
- `core/portfolio/` — contextual bandit, optimizer
- `core/risk/` — hard risk rules
- `core/execution/` — deterministic execution helpers
- `services/` — external system adapters (Tiingo, SimFin, FRED, Alpaca, R2, Modal, observability)

Ownership boundary summary:
- `app/` coordinates runtime surfaces.
- `core/` owns business logic and contracts.
- `services/` owns third-party integration adapters.
- `docs/` owns architecture and contract intent.
