# core

Shared business and domain logic.

This folder contains reusable project logic that should be independent from deployment targets.

Subfolders:
- `data/` — universe and dataset-level domain logic
- `features/` — feature engineering interfaces and shared transforms
- `models/` — model wrappers and prediction interfaces
- `portfolio/` — allocation and rebalance decision logic
- `risk/` — hard-rule risk checks and enforcement logic
- `execution/` — deterministic order translation helpers
- `contracts/` — strongly defined internal request and response shapes
- `common/` — cross-cutting shared helpers
