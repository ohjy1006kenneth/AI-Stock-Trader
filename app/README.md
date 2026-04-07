# app

Runnable deployment surfaces.

Owner: Runtime surface owners by deployment environment.

Responsibilities:
- Hold environment-specific entrypoints and orchestration glue
- Keep each deployment surface independent and testable

Out of scope:
- Core business logic that belongs in `core/`
- External service client implementation that belongs in `services/`

Subfolders:
- `lab/` — Cloud Lab for heavy AI workloads
- `cloud/` — Cloud Oracle hosted inference layer
- `pi/` — Edge Pi runtime and execution layer