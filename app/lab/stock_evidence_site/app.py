"""FastAPI app for the standalone evidence review site.

Read-only. Routes:

* ``GET /healthz`` — liveness, no packet data.
* ``GET /`` — the rendered evidence site (defaults to the local
  fixture packet).
* ``GET /api/stocks/review-options`` — local candidates, or proxy to a
  configured backend when ``source=backend``.
* ``GET /api/stocks/audit`` — JSON audit payload for a validated
  request; errors return the same explicit states as the UI.

Run with: ``uvicorn app.lab.stock_evidence_site.app:app --port 8890``
(loopback; Tailscale Serve proxies to it — see docs/stock_evidence_site.md).
"""

from __future__ import annotations

from typing import Any

from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse, JSONResponse

from app.lab.stock_evidence_site.render import render_error, render_payload
from app.lab.stock_evidence_site.sources import (
    DEFAULT_PACKET_ID,
    DEFAULT_TICKERS,
    PacketError,
    get_sources,
    validate_request,
)

app = FastAPI(title="AI-Stock-Trader Evidence Review", docs_url=None, redoc_url=None)

_local, _backend = get_sources()

_ERRORS: tuple[type, ...] = (PacketError,)


def _request_context(
    packet_id: str | None, date: str | None,
    tickers: str | None, source: str,
) -> dict[str, Any]:
    return {
        "packet_id": packet_id,
        "date": date,
        "tickers": tickers,
        "source": source,
    }


def _audit_payload(source: str, req) -> dict[str, Any]:
    if source == "local":
        return _local.fetch_audit(req)
    return _backend.fetch_audit(req)


@app.get("/healthz")
def healthz() -> dict[str, Any]:
    """Liveness probe."""
    return {"status": "ok", "service": "stock-evidence-site"}


@app.get("/", response_class=HTMLResponse)
def index(
    packet_id: str = Query(DEFAULT_PACKET_ID),
    date: str = Query("2026-09-04"),
    tickers: str = Query(",".join(DEFAULT_TICKERS)),
    source: str = Query("local"),
) -> str:
    """Render the evidence site for the requested packet."""
    ctx = _request_context(packet_id, date, tickers, source)
    try:
        req = validate_request(packet_id, date, tickers, source)
        payload = _audit_payload(source, req)
        payload["source"] = source
        return render_payload(ctx, payload)
    except PacketError as exc:
        return render_error(ctx, {"status": f"HTTP {exc.status}", "message": exc.message})


@app.get("/api/stocks/review-options")
def review_options(source: str = Query("local")) -> JSONResponse:
    """List packet candidates (local fixture, or proxied backend)."""
    try:
        if source == "local":
            return JSONResponse(_local.review_options())
        if source != "backend":
            return JSONResponse(
                {"error": f"invalid source: {source}"}, status_code=400
            )
        return JSONResponse(_backend.review_options())
    except PacketError as exc:
        return JSONResponse({"error": exc.message}, status_code=exc.status)


@app.get("/api/stocks/audit")
def audit(
    run_id: str = Query(...),
    date: str = Query(...),
    tickers: str = Query(...),
    source: str = Query("local"),
) -> JSONResponse:
    """Return the audit packet JSON for a validated request."""
    try:
        req = validate_request(run_id, date, tickers, source)
        payload = _audit_payload(source, req)
        payload["source"] = source
        return JSONResponse(payload)
    except PacketError as exc:
        return JSONResponse({"error": exc.message}, status_code=exc.status)


def _register_exception_handler() -> None:
    @app.exception_handler(PacketError)
    async def _handle_packet_error(request, exc: PacketError):  # pragma: no cover
        return JSONResponse({"error": exc.message}, status_code=exc.status)


_register_exception_handler()
