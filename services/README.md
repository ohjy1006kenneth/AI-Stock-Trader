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
- `order_book/` — optional Level 2 provider gating and non-secret config ownership
- `r2/` — Cloudflare R2 object storage interfaces, canonical paths, and manifests
- `modal/` — cloud job and deployment integrations
- `observability/` — logging, metrics, and alert glue
- `wikipedia/` — point-in-time S&P 500 universe construction
- `tiingo/` — deprecated legacy historical OHLCV adapters; not a Layer 0 production dependency
- `simfin/` — point-in-time fundamentals and earnings-date adapters
- `fred/` — macro and rates context adapters

The current baseline repo has no active options-chain adapter or archive contract. Layer 1
options-derived features such as `iv_rank`, `put_call_ratio`, and `iv_skew` are therefore
out of scope until a future task adds an existing-stack provider and the corresponding
repository-owned archive/config surfaces.
