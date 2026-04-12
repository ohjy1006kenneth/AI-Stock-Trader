# config

Non-secret project configuration.

This folder contains examples and static policy/configuration files that are safe to commit.
Real credentials belong in local-only `config/*.env` files or a secret manager, never in git.

## Local env files

Copy the examples you need from `config/examples/` into `config/`:

```bash
cp config/examples/r2.env.example config/r2.env
cp config/examples/tiingo.env.example config/tiingo.env
cp config/examples/simfin.env.example config/simfin.env
cp config/examples/fred.env.example config/fred.env
cp config/examples/alpaca.env.example config/alpaca.env
```

Then replace the placeholder values in the local files.

| Local file | Used for | Required keys |
|---|---|---|
| `config/r2.env` | Cloudflare R2 object storage | `R2_ENDPOINT_URL`, `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY`, `R2_BUCKET_NAME` |
| `config/tiingo.env` | Tiingo historical OHLCV and raw news | `TIINGO_API_TOKEN` |
| `config/simfin.env` | SimFin as-reported fundamentals | `SIMFIN_API_KEY` |
| `config/fred.env` | FRED macro/rate observations | `FRED_API_KEY` |
| `config/alpaca.env` | Alpaca live daily bars | `ALPACA_API_KEY_ID`, `ALPACA_API_SECRET_KEY` |

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
- `config/requirements/` is deprecated; dependency files live under repository-root `requirements/`.
