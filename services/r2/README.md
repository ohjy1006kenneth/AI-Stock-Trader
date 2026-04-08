# r2

Adapters for Cloudflare R2 object storage, manifests, and runtime state persistence.

Modules:
- `client.py` — boto3-backed Cloudflare R2 client built from environment variables
- `writer.py` — auto-selects real R2 when credentials exist, otherwise uses a local
  filesystem mock rooted at `data/runtime/r2_mock`

Local credentials:
- `config/r2.env` is loaded automatically when present
- exported shell environment variables still take precedence over file values

Verification:
- `pytest tests/unit/test_r2_client.py -v --tb=short` exercises local/mock and
  mocked-R2 branches
- `make test-r2-live` runs a real bucket round-trip smoke test when credentials exist

Owner: Object storage integration boundary.
