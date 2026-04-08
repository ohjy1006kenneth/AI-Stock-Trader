# tests

Unit, integration, and end-to-end tests for the trading system.

Owner: Verification and regression boundary.

Responsibilities:
- Validate domain logic and contract behavior
- Protect against regressions across milestones

Run tests:
- Local: `pytest tests/unit/ -v --tb=short`
- Live R2 smoke: `make test-r2-live`
- CI: `.github/workflows/ci.yml` runs `pytest tests/unit/ -v --tb=short`

This folder should stay in git.
Tests document expected behavior and make model and pipeline changes safer.
