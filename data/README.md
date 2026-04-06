# data

Local data directories used during research, training, and runtime execution.

Large or generated data payloads are intentionally not committed.

Typical subfolders:
- `raw/` — local raw snapshots and fetched payloads
- `processed/` — derived training datasets and model-ready intermediate outputs
- `runtime/` — edge or cloud runtime state snapshots
- `cache/` — local caches and temporary artifacts

If you clone this repo, expect these directories to be mostly empty until you run the pipelines locally or in the cloud.
