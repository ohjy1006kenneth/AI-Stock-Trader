# services

External service integrations and adapters.

Owner: External integration boundary.

Responsibilities:
- Encapsulate third-party APIs and storage adapters
- Normalize retries, errors, and request/response handling

Out of scope:
- Trading decision logic and portfolio/risk rules
- Cross-layer schema ownership

Subfolders:
- `alpaca/` — market data and broker integrations
- `r2/` — object storage interfaces and manifests
- `modal/` — cloud job and deployment integrations
- `observability/` — logging, metrics, and alert glue