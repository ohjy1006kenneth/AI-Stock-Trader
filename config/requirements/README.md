# requirements (deprecated location)

Dependency files have moved to `requirements/` at the repository root.

- `requirements/base.txt` — shared across all surfaces
- `requirements/pi.txt` — Pi edge runtime (lightweight; no ML stack)
- `requirements/modal.txt` — Modal cloud compute (heavy ML: torch, transformers, xgboost)
- `requirements/dev.txt` — local development and testing

Do not add files here.