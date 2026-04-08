# polygon

Adapters for the Polygon.io REST API.

Owner: Market data integration boundary.

## Responsibilities

- Fetch adjusted OHLCV bars (daily, for a given ticker and date range)
- Fetch news articles with point-in-time timestamps
- Handle pagination and rate limiting for the student plan tier
- Return data as `OHLCVRecord` and raw news dicts (JSON Lines format)

## What this service does NOT own

- Universe membership (Wikipedia revision history — owned by `core/data/universe.py`)
- Adjusted-price correction logic (Polygon returns pre-adjusted data; we use it as-is)
- Feature computation (owned by `core/features/`)

## Authentication

API key loaded from environment variable `POLYGON_API_KEY`.
Never hardcode it. Never commit it.
See `config/examples/` for the expected `.env.example` shape.

## Rate limits (student plan)

- 5 API calls per minute on the free tier; student plan may differ — check your account
- Use a retry-with-backoff wrapper for all requests
- Cache raw responses to Pi SSD (`data/raw/`) to avoid redundant calls
