# order_book

Optional Layer 1 order-book configuration and provider boundary.

Owner: Market data integration boundary.

Responsibilities:
- Own the non-secret repository config that gates optional Layer 1 Level 2 features
- Keep provider naming and enablement rules outside `core/`
- Preserve the disabled-by-default default until an explicit provider and archive exist

Out of scope:
- Live provider credentials
- Layer 1 feature computation logic
- Trading or model policy
