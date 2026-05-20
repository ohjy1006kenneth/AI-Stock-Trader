# config

Non-secret project configuration.

Real credentials belong in local-only `config/*.env` files or a secret manager, never in git.
The repository intentionally does not commit `.env.example` files; use the key list below to
create local env files manually.

## Local env files

Create only the files you need under `config/`:

| Local file | Used for | Required keys |
|---|---|---|
| `config/r2.env` | Cloudflare R2 object storage | `R2_ENDPOINT_URL`, `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY`, `R2_BUCKET_NAME` |
| `config/simfin.env` | SimFin as-reported fundamentals | `SIMFIN_API_KEY` |
| `config/fred.env` | FRED macro/rate observations | `FRED_API_KEY` |
| `config/alpaca.env` | Alpaca delayed SIP OHLCV, daily bars, historical/live news, and later broker access | `ALPACA_API_KEY_ID`, `ALPACA_API_SECRET_KEY` |

Optional overrides:

| Key | Default |
|---|---|
| `SIMFIN_BASE_URL` | `https://backend.simfin.com/api/v3` |
| `FRED_BASE_URL` | `https://api.stlouisfed.org/fred` |
| `ALPACA_DATA_BASE_URL` | `https://data.alpaca.markets` |
| `ALPACA_DATA_FEED` | `iex` for live daily snapshots; historical backfill forces `sip` |

Alpaca also accepts the official `APCA_API_KEY_ID` and `APCA_API_SECRET_KEY` names.

## Cloudflare R2 values

For `config/r2.env`:

- `R2_ENDPOINT_URL`: the S3 API endpoint from Cloudflare, usually `https://<account-id>.r2.cloudflarestorage.com`.
- `R2_ACCESS_KEY_ID`: the access key ID from an R2 API token/access key.
- `R2_SECRET_ACCESS_KEY`: the secret access key shown when the R2 access key is created.
- `R2_BUCKET_NAME`: the exact bucket name created in R2 for this project.

Use an R2 token/access key with read/write access to the bucket. Do not use the Cloudflare global API key.

## Static config

- `config/fred_series.json` controls the default FRED macro/rate series and historical backfill date range.
- `config/sector_etf_mapping.json` controls sector-name normalization and the
  sector-to-ETF mapping used by Layer 1 sector/factor features.
- `config/order_book_features.json` gates the optional Layer 1 Level 2/order-book branch;
  it stays disabled by default until an explicit provider name is configured.
- no repository config currently enables Layer 1 options-derived features; the baseline
  stack defines no non-secret config or point-in-time historical options-chain archive
  contract for `iv_rank`, `put_call_ratio`, or `iv_skew`
- `config/source_credibility.json` controls source credibility weights used for
  FinBERT sentiment aggregation.
- `config/finbert_sentiment.json` controls the Modal app, model identity, batch size,
  source-weight config path, and date-bucketing timezone for FinBERT scoring.
- `config/modal.json` controls the Pi-triggered Layer 1 orchestration apps, including the
  dedicated multi-date readiness timeout and the optional `T4` GPU cap for the batched
  Layer 1 runner.
- `config/requirements/` is deprecated; dependency files live under repository-root `requirements/`.
