# app/lab

Cloud Lab research, validation, and packaging code.

Owner: Cloud ML and model validation boundary.

Responsibilities:
- Feature/dataset generation for model training
- Model training, evaluation, and artifact packaging

Out of scope:
- Broker execution and reconciliation logic
- Long-term runtime orchestration on the edge device

This folder is the heavy-compute side of the project. It is where feature creation, dataset assembly, model training, validation, and release packaging happen.

Key subfolders:
- `data_pipelines/` — build aligned datasets from news, market, and context inputs
- `model_architecture/` — model definitions, policy contracts, and selection logic
- `backtesting/` — promotion metrics, validation checks, and evaluation helpers
- `training/` — training entry points, exports, and pipeline runners

This is the main place for model-development work.
