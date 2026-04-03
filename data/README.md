# data

Local data directories used during research, training, and runtime execution.

This repo keeps the folder structure, but large/generated data payloads are intentionally not committed.

Typical subareas:
- `raw/` — local raw snapshots and fetched payloads
- `processed/` — derived training datasets and model-ready intermediate outputs
- `runtime/` — edge/cloud runtime state snapshots
- `cache/` — local caches

If you clone this repo, expect these directories to be mostly empty until you run the pipelines locally or in the cloud.
