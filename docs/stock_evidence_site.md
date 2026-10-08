# Stock Evidence Site

Standalone, read-only reviewer for versioned AI-Stock-Trader
Layer-1/stock semantic audit packets. Serves exactly the data delivered by
a local fixture or an independently configured private packet backend; it
never fabricates missing fields and never treats transport success as
semantic readiness.

## Scope

- New namespace: `app/lab/stock_evidence_site/`
- No changes to existing backend routes, schemas, or deployment config.
- The backend adapter uses `httpx`, not stdlib `urllib`. The approved runtime
  dependency chain is `requirements/pi.txt` -> `requirements/base.txt`, with
  direct `httpx>=0.27,<1` plus FastAPI/uvicorn in the Pi requirements and inherited
  loguru from base. S3d proved this chain in an isolated runtime environment,
  not through packages installed in the shared project venv.
- Deployment is **guarded**: nothing here is wired into the existing
  deployment, cron, or dashboard pipeline until a separate, explicitly
  approved rollout task exists.

## Modules

| File | Purpose |
|---|---|
| `app/lab/stock_evidence_site/__init__.py` | Package docstring. |
| `app/lab/stock_evidence_site/sources.py` | `AuditRequest` validation, local historical preview and httpx private backend adapter; source-specific fail-closed identity checks. |
| `app/lab/stock_evidence_site/render.py` | Deterministic server-side HTML; all packet data escaped with `html.escape(quote=True)`; explicit "unknown"/"not delivered" states instead of invented values. |
| `app/lab/stock_evidence_site/app.py` | FastAPI app: `GET /healthz`, `GET /`, `GET /api/stocks/review-options`, `GET /api/stocks/audit`. |

## API contract

| Route | Query params | Behavior |
|---|---|---|
| `GET /healthz` | — | `{"status": "ok", "service": "stock-evidence-site"}` |
| `GET /` | `packet_id`, `date`, `tickers` (comma-separated), `source` (`local`\|`backend`, default `local`) | Rendered evidence site; validation/identity errors render an explicit error page (HTTP 200, no substitution). |
| `GET /api/stocks/review-options` | `source` | Local packet candidates, or proxied from the configured backend. |
| `GET /api/stocks/audit` | `run_id`, `date`, `tickers`, `source` | JSON audit payload for a validated request. |

The root route renders caught `PacketError` states as an explicit error page
with HTTP **200**, including the underlying status in its text. Audit JSON
returns `{"error": "<message>"}` with the actual `PacketError.status`:

- **400**: invalid source, blank run/date/tickers, malformed or invalid calendar date.
- **404**: unknown local packet, missing/unreadable/unexpected-shape local fixture,
  or a requested ticker absent from the local packet.
- **409**: missing or conflicting required identity; no cached/latest substitution.
- **422**: invalid backend JSON, payload over 2,000,000 bytes, malformed required
  payload fields, producer failure, metadata conflict, or no requested audit rows.
  A local packet with no ticker audit rows also returns 422.
- **502**: backend unconfigured, connection/timeout failure, upstream HTTP >=400,
  or a non-object backend audit response.

Review-options JSON also returns caught PacketError statuses; invalid source is
400. FastAPI's own validation (e.g. omitted required audit parameters) returns
its framework 422 response, not the PacketError envelope. Delivered-empty
collections and per-ticker no-data are evidence states, not `EmptyDataset`
HTTP status codes.

## Identity/correlation rules

Local validation requires served run, date, and git head. Present run/date
aliases (including `request.run_id`/`request.date`) must agree. The served head
must equal the admitted historical head
`aaa29eca6fe6e55d3529b4f134471eb6fbe70c85`; it is not the checkout HEAD.

The backend receives `run_id`, `from_date=date`, `to_date=date`, and comma-separated
`tickers`. It must return the same top-level run and exact echoed query dates and
ordered ticker list. Each review must have a unique requested ticker, matching
`producer_correlation` run/from/to/ticker and `packet_metadata`
run/review_date/git_head/selected_ticker. Packet metadata must carry the complete
AAPL/AMD/MSFT/NVDA packet ticker set; present identity aliases cannot conflict.
Both sources fail closed rather than selecting another run, date, or source.
The HTTP wrapper has no git-head query parameter; authoritative head correlation
is an adapter constraint, not a user-selectable revision.

The rendered correlation panel separates requested and served run/date, shows
served head, scope, generated time and R2 mode, and labels the source as
`local historical preview; not current readiness` or `backend; no local substitution`.
Missing generated time/scope stays unknown; backend metadata must be unanimously
populated across reviews to expose these values, and populated conflicts fail.
All requested ticker sections render on a successful payload (the default four
are AAPL, AMD, MSFT, NVDA). A ticker error takes precedence over any apparent audit
row. Missing backend reviews produce per-ticker no-data entries when other valid
rows remain; total absence is a 422 packet error. Backend `ok=false` or `status=fail`
is rejected before partial rendering. Missing local tickers fail the whole request
with 404. An explicitly delivered empty list is different from absent/error data;
neither null counts nor missing text are converted into synthetic evidence or zeros.
Producer readiness flags, including `ready_for_final_human_acceptance`, are
producer-reported only, never human acceptance. Refresh is manual GET/reload;
there is no automatic polling.

## Freshness and HMM honesty

- The local adapter labels its preview `is_fresh=false`, with delivered
  `freshness_threshold_seconds` or a default 86400-second threshold. Rendering
  distinguishes true (classification only, not readiness), false (historical,
  displaying the threshold in seconds or unknown), and null/missing (unknown).
  The backend adapter does not project producer freshness into the wrapper's
  top-level freshness field, so backend freshness renders unknown; timestamps
  are not used to invent a classification.
- HMM is audit-only: the badge shows the delivered regime row count
  (`hmm.regime_rows` list length, delivered benchmark row count, or API auditability
  point count), the single-point audit warning **if and only if** the count is 1,
  and explicitly renders "unknown" when no point count is delivered.
  It is never presented as a trading signal.
- Sentence/chunk text is not in the delivered payload; the UI says so
  instead of synthesizing text.

## Local run (guarded — not part of any live deployment)

Prerequisites: a provisioned reviewer environment and selected source. The default
historical fixture is
`artifacts/reports/diagnostics/issue281-post-pr306-current-prod-20260904-v1_audit_2026-09-04.json`.
It is an **untracked artifact excluded from integration**: a clean checkout does
not include it. The approved historical-preview procedure separately provisions a
byte-identical copy of the existing bounded packet at that candidate-relative path;
never edit, regenerate, commit, or use it as a backend fallback. Verify size 114845
bytes and SHA256 `634039d3c8b5ecb775b738d284b57a64b5cd42506f137ae17bc6d11c10c39737`.
A missing fixture produces an explicit error, not delivery of the preview (the root
error page still returns HTTP 200). Install the approved existing
`requirements/pi.txt` -> `base.txt` chain, including direct httpx, in an isolated
Python 3.11 reviewer environment. See [deployment gates](deployment.md).

```bash
<run-dir>/venv/bin/python -m uvicorn app.lab.stock_evidence_site.app:app --host 127.0.0.1 --port 8890
```

Run from the candidate root; replace `<run-dir>` with the absolute run evidence
directory containing the isolated reviewer environment, not the shared project venv.

Backend source (optional; the private host is never hardcoded):

```bash
export STOCK_EVIDENCE_BACKEND=http://<configured-host>:<port>
```

Set configuration before importing/starting the app (sources are constructed at
import), then explicitly request `source=backend`. Local is the default; backend
failure never falls back to local. This command is a guarded loopback preview,
not deployment authorization. Private-backend compatibility, identity, exposure/
authentication controls, browser evidence, rollout/rollback, and human semantic
acceptance remain separate unfulfilled gates. No Tailscale Serve activation is
established by the implementation comment. #281 acceptance remains unchanged.

## Tests

```bash
./.venv/bin/pytest tests/unit/test_stock_evidence_site.py -q
```

Covers request validation, local preview/404 missing fixture, fail-closed identity,
mocked httpx backend transport/payload/correlation, error-versus-empty evidence,
escaped rendering, unknown metadata/freshness/readiness/HMM states and the wrapper
HTTP surface through TestClient. These are fixture/mock tests, not live backend,
browser, deployment, or semantic-acceptance evidence. The accepted unchanged-code
unit and focused Ruff results are reused for this documentation-only pass.
