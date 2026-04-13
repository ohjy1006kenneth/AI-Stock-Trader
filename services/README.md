# services

External service integrations and adapters.

Owner: External integration boundary.

Responsibilities:
- Encapsulate third-party APIs and storage adapters
- Normalize retries, errors, and request/response handling
- Keep vendor-specific response parsing outside `core/`

Out of scope:
- Trading decision logic and portfolio/risk rules
- Cross-layer schema ownership

Subfolders:
- `alpaca/` — delayed SIP OHLCV, historical/live news, live market data, broker state, and execution integrations
- `r2/` — Cloudflare R2 object storage interfaces, canonical paths, and manifests
- `modal/` — cloud job and deployment integrations
- `observability/` — logging, metrics, and alert glue
- `wikipedia/` — point-in-time S&P 500 universe construction
- `tiingo/` — deprecated legacy historical OHLCV adapters; not a Layer 0 production dependency
- `simfin/` — point-in-time fundamentals and earnings-date adapters
- `fred/` — macro and rates context adapters
