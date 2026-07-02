"""Local read-only web UI for the Layer 1 semantic-review dashboard."""
from __future__ import annotations

import argparse
import json
import subprocess
import tempfile
from collections.abc import Mapping
from dataclasses import dataclass
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import Lock
from typing import Any
from urllib.parse import parse_qs, urlparse

from loguru import logger

from core.features.aapl_evidence import build_layer1_aapl_evidence_report
from core.features.semantic_review_dashboard import (
    build_layer1_semantic_review_dashboard_payload,
    validate_layer1_semantic_review_dashboard_payload,
)
from services.r2.writer import R2Writer


@dataclass(frozen=True)
class _DashboardDefaults:
    """Default query parameters for the local dashboard server."""

    run_id: str
    from_date: str
    to_date: str
    ticker: str
    host: str
    port: int
    local_root: Path | None = None


@dataclass(frozen=True)
class _DashboardQuery:
    """Resolved query parameters for a single API request."""

    run_id: str
    from_date: str
    to_date: str
    ticker: str


class _DashboardHTTPServer(ThreadingHTTPServer):
    """HTTP server carrying the dashboard defaults and local storage config."""

    def __init__(self, server_address: tuple[str, int], defaults: _DashboardDefaults) -> None:
        super().__init__(server_address, _DashboardRequestHandler)
        self.defaults = defaults
        self.payload_cache: dict[tuple[str, str, str, str], dict[str, object]] = {}
        self.payload_cache_lock = Lock()


class _DashboardRequestHandler(BaseHTTPRequestHandler):
    """Serve the semantic-review shell and read-only JSON API."""

    server: _DashboardHTTPServer

    def do_GET(self) -> None:  # noqa: N802
        """Handle GET routes for the dashboard shell and read-only JSON API."""
        parsed = urlparse(self.path)
        if parsed.path == "/":
            self._send_html(_render_dashboard_html(self.server.defaults))
            return
        if parsed.path == "/api/config":
            self._send_json(_config_payload(self.server.defaults))
            return
        if parsed.path == "/api/review":
            self._handle_review_request(parsed.query)
            return
        if parsed.path == "/health":
            self._send_json({"status": "ok"})
            return
        self._send_json({"error": f"Unknown route: {parsed.path}"}, status=HTTPStatus.NOT_FOUND)

    def log_message(self, format: str, *args: object) -> None:
        """Route stdlib HTTP logs through Loguru."""
        logger.info("semantic-review-dashboard {} - {}", self.address_string(), format % args)

    def _handle_review_request(self, query_text: str) -> None:
        params = parse_qs(query_text, keep_blank_values=False)
        try:
            query = _query_from_params(params=params, defaults=self.server.defaults)
            cache_key = (query.run_id, query.from_date, query.to_date, query.ticker)
            with self.server.payload_cache_lock:
                payload = self.server.payload_cache.get(cache_key)
            if payload is None:
                payload = _build_dashboard_payload(
                    run_id=query.run_id,
                    from_date=query.from_date,
                    to_date=query.to_date,
                    ticker=query.ticker,
                    local_root=self.server.defaults.local_root,
                )
                with self.server.payload_cache_lock:
                    self.server.payload_cache[cache_key] = payload
        except ValueError as exc:
            self._send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
            return
        except FileNotFoundError as exc:
            self._send_json({"error": str(exc)}, status=HTTPStatus.NOT_FOUND)
            return
        self._send_json(payload)

    def _send_html(self, body: str) -> None:
        encoded = body.encode("utf-8")
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def _send_json(self, payload: Mapping[str, Any], *, status: HTTPStatus = HTTPStatus.OK) -> None:
        encoded = json.dumps(payload, indent=2, sort_keys=True).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)


def main(argv: list[str] | None = None) -> int:
    """Launch the local read-only Layer 1 semantic-review dashboard server."""
    args = _parse_args(argv)
    defaults = _DashboardDefaults(
        run_id=str(args.run_id),
        from_date=str(args.from_date),
        to_date=str(args.to_date),
        ticker=str(args.ticker).upper(),
        host=str(args.host),
        port=int(args.port),
        local_root=args.local_root,
    )
    logger.info(
        "Layer 1 semantic-review dashboard listening on http://{}:{}",
        defaults.host,
        defaults.port,
    )
    logger.info(
        "Review defaults: run_id={} ticker={} window={}..{}",
        defaults.run_id,
        defaults.ticker,
        defaults.from_date,
        defaults.to_date,
    )
    if defaults.local_root is not None:
        logger.info("Using local mock R2 root: {}", defaults.local_root)
    if bool(args.smoke):
        return _run_dashboard_smoke(defaults=defaults, args=args)
    server = _DashboardHTTPServer((defaults.host, defaults.port), defaults)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        logger.info("Shutting down Layer 1 semantic-review dashboard.")
    finally:
        server.server_close()
    return 0


def _build_dashboard_payload(
    *,
    run_id: str,
    from_date: str,
    to_date: str,
    ticker: str,
    local_root: Path | None,
) -> dict[str, object]:
    writer = R2Writer(local_root=local_root) if local_root is not None else R2Writer()
    report = build_layer1_aapl_evidence_report(
        run_id=run_id,
        from_date=from_date,
        to_date=to_date,
        ticker=ticker,
        writer=writer,
    )
    return build_layer1_semantic_review_dashboard_payload(report)


def _run_dashboard_smoke(*, defaults: _DashboardDefaults, args: argparse.Namespace) -> int:
    """Run API and rendered-browser smoke checks for the semantic-review dashboard."""
    payload = _build_dashboard_payload(
        run_id=defaults.run_id,
        from_date=defaults.from_date,
        to_date=defaults.to_date,
        ticker=defaults.ticker,
        local_root=defaults.local_root,
    )
    smoke = validate_layer1_semantic_review_dashboard_payload(payload)
    if smoke.get("status") != "pass":
        logger.error("Dashboard smoke failed before browser QA: {}", json.dumps(smoke, indent=2))
        return 1

    browser_binary = _resolve_browser_binary(str(args.browser_binary))
    screenshot_path = Path(args.smoke_screenshot) if args.smoke_screenshot else None
    result = _run_browser_render_smoke(
        browser_binary=browser_binary,
        html=_render_dashboard_html(defaults),
        payload=payload,
        screenshot_path=screenshot_path,
        timeout_seconds=float(args.smoke_timeout_seconds),
    )
    if result["status"] != "pass":
        logger.error("Rendered dashboard smoke failed: {}", json.dumps(result, indent=2))
        return 1
    logger.info("Dashboard API and rendered-browser smoke passed: {}", json.dumps(result))
    return 0


def _run_browser_render_smoke(
    *,
    browser_binary: str,
    html: str,
    payload: Mapping[str, object],
    screenshot_path: Path | None,
    timeout_seconds: float,
) -> dict[str, object]:
    """Render the dashboard in Chromium and verify the chart is not visually misleading."""
    with tempfile.TemporaryDirectory() as tmpdir:
        active_screenshot = screenshot_path or Path(tmpdir) / "semantic_review_dashboard.png"
        html_path = Path(tmpdir) / "semantic_review_dashboard.html"
        html_path.write_text(_inject_smoke_payload(html, payload), encoding="utf-8")
        user_data_dir = Path(tmpdir) / "chromium-profile"
        command = [
            browser_binary,
            "--headless=new",
            "--disable-gpu",
            "--no-sandbox",
            f"--user-data-dir={user_data_dir}",
            "--window-size=1400,1000",
            f"--screenshot={active_screenshot}",
            "--dump-dom",
            html_path.as_uri(),
        ]
        try:
            completed = subprocess.run(
                command,
                check=False,
                capture_output=True,
                text=True,
                timeout=timeout_seconds,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            return {
                "status": "fail",
                "reason": "browser_launch_failed",
                "message": str(exc),
                "browser_binary": browser_binary,
            }

        dom = completed.stdout
        failures: list[str] = []
        if completed.returncode != 0:
            failures.append(f"chromium_exit_{completed.returncode}")
        if 'data-smoke-status="pass"' not in dom:
            failures.append("rendered_smoke_status_not_pass")
        if '<svg class="chart"' not in dom:
            failures.append("missing_rendered_svg_chart")
        if not active_screenshot.exists() or active_screenshot.stat().st_size <= 0:
            failures.append("missing_or_empty_screenshot")
        return {
            "status": "pass" if not failures else "fail",
            "url": html_path.as_uri(),
            "browser_binary": browser_binary,
            "screenshot_path": str(active_screenshot),
            "failures": failures,
            "stderr_tail": completed.stderr[-1000:],
        }


def _inject_smoke_payload(html: str, payload: Mapping[str, object]) -> str:
    """Inject an API payload into the dashboard shell for file-based browser smoke."""
    payload_json = json.dumps(payload, sort_keys=True)
    script = f"""  <script>
    window.__semanticReviewSmokePayload = {payload_json};
    const __semanticReviewSmokeFetch = window.fetch.bind(window);
    window.fetch = async (url, options) => {{
      if (String(url).startsWith('/api/review')) {{
        return new Response(JSON.stringify(window.__semanticReviewSmokePayload), {{
          status: 200,
          headers: {{'Content-Type': 'application/json'}}
        }});
      }}
      return __semanticReviewSmokeFetch(url, options);
    }};
  </script>
"""
    marker = "  <script>\n    const defaults"
    if marker not in html:
        return html.replace("</body>", f"{script}</body>")
    return html.replace(marker, f"{script}{marker}", 1)


def _resolve_browser_binary(browser_binary: str) -> str:
    """Return a browser executable suitable for headless smoke rendering."""
    requested = Path(browser_binary)
    debian_chromium = Path("/usr/lib/chromium/chromium")
    if requested.name == "chromium" and debian_chromium.exists():
        return str(debian_chromium)
    return browser_binary


def _query_from_params(
    *,
    params: Mapping[str, list[str]],
    defaults: _DashboardDefaults,
) -> _DashboardQuery:
    run_id = _first_param(params, "run_id") or defaults.run_id
    from_date = _first_param(params, "from_date") or defaults.from_date
    to_date = _first_param(params, "to_date") or defaults.to_date
    ticker = (_first_param(params, "ticker") or defaults.ticker).upper()
    if not ticker:
        raise ValueError("ticker must contain at least one non-empty ticker")
    return _DashboardQuery(run_id=run_id, from_date=from_date, to_date=to_date, ticker=ticker)


def _config_payload(defaults: _DashboardDefaults) -> dict[str, object]:
    return {
        "defaults": {
            "run_id": defaults.run_id,
            "from_date": defaults.from_date,
            "to_date": defaults.to_date,
            "ticker": defaults.ticker,
            "host": defaults.host,
            "port": defaults.port,
        }
    }


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Serve the read-only Layer 1 semantic-review dashboard UI.",
    )
    parser.add_argument("--run-id", required=True, help="Layer 1 run identifier to inspect.")
    parser.add_argument(
        "--from-date",
        required=True,
        help="Inclusive start date in YYYY-MM-DD format for the initial dashboard load.",
    )
    parser.add_argument(
        "--to-date",
        required=True,
        help="Inclusive end date in YYYY-MM-DD format for the initial dashboard load.",
    )
    parser.add_argument(
        "--ticker",
        required=True,
        help="Ticker symbol to review, for example AAPL.",
    )
    parser.add_argument(
        "--host",
        default="127.0.0.1",
        help="Interface to bind the local dashboard server to.",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8766,
        help="Port to bind the local dashboard server to.",
    )
    parser.add_argument(
        "--local-root",
        type=Path,
        default=None,
        help="Optional filesystem root for the mock R2 store.",
    )
    parser.add_argument(
        "--smoke",
        action="store_true",
        help="Run API and rendered Chromium smoke checks, then exit.",
    )
    parser.add_argument(
        "--browser-binary",
        default="chromium",
        help="Browser executable used by --smoke for rendered dashboard QA.",
    )
    parser.add_argument(
        "--smoke-screenshot",
        type=Path,
        default=None,
        help="Optional screenshot path written by --smoke browser QA.",
    )
    parser.add_argument(
        "--smoke-timeout-seconds",
        type=float,
        default=30.0,
        help="Timeout for --smoke browser rendering.",
    )
    return parser.parse_args(argv)


def _first_param(params: Mapping[str, list[str]], name: str) -> str | None:
    """Return the first query-string value for a parameter name."""
    values = params.get(name)
    if not values:
        return None
    text = values[0].strip()
    return text or None


def _render_dashboard_html(defaults: _DashboardDefaults) -> str:
    """Return the browser shell for the semantic-review dashboard."""
    defaults_json = json.dumps(
        {
            "run_id": defaults.run_id,
            "from_date": defaults.from_date,
            "to_date": defaults.to_date,
            "ticker": defaults.ticker,
        }
    )
    template = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Layer 1 semantic-review dashboard</title>
  <style>
    :root {{
      color-scheme: dark;
      --bg: #0f172a;
      --panel: #111827;
      --panel-soft: #0b1220;
      --border: #243245;
      --text: #e5eefb;
      --muted: #aebbd0;
      --accent: #38bdf8;
      --good: #4ade80;
      --warn: #fbbf24;
      --bad: #fb7185;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      background: linear-gradient(180deg, #0b1120 0%, var(--bg) 100%);
      color: var(--text);
      font-family: system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      line-height: 1.5;
    }}
    header {{
      padding: 24px 28px 16px;
      border-bottom: 1px solid var(--border);
      background: rgba(15, 23, 42, 0.92);
      position: sticky;
      top: 0;
      backdrop-filter: blur(10px);
      z-index: 1;
    }}
    main {{ padding: 20px 28px 32px; max-width: 1280px; margin: 0 auto; }}
    h1, h2, h3, p {{ margin-top: 0; }}
    h1 {{ margin-bottom: 6px; font-size: 2rem; }}
    h2 {{ margin-bottom: 8px; font-size: 1.25rem; }}
    h3 {{ margin-bottom: 6px; font-size: 1rem; }}
    .subtitle {{ color: var(--muted); max-width: 80ch; margin-bottom: 0; }}
    .topline {{ display: flex; gap: 12px; flex-wrap: wrap; margin-top: 14px; }}
    .badge {{
      display: inline-flex;
      align-items: center;
      gap: 6px;
      padding: 6px 10px;
      border: 1px solid var(--border);
      border-radius: 999px;
      background: rgba(17, 24, 39, 0.92);
      color: var(--text);
      font-size: 0.92rem;
    }}
    .badge strong {{ color: #fff; }}
    .stack {{ display: grid; gap: 16px; }}
    .panel {{
      background: rgba(17, 24, 39, 0.95);
      border: 1px solid var(--border);
      border-radius: 18px;
      padding: 18px;
      box-shadow: 0 10px 30px rgba(2, 6, 23, 0.28);
    }}
    .hero-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 12px; }}
    .metric {{
      padding: 14px;
      border: 1px solid var(--border);
      border-radius: 14px;
      background: rgba(11, 18, 32, 0.9);
      min-height: 92px;
    }}
    .metric .label {{ color: var(--muted); font-size: 0.88rem; }}
    .metric .value {{ font-size: 1.6rem; font-weight: 700; margin-top: 6px; }}
    .metric .raw {{ color: var(--muted); font-size: 0.8rem; margin-top: 4px; }}
    .state-card {{ display: grid; gap: 8px; }}
    .tab-bar {{ display: flex; gap: 8px; flex-wrap: wrap; }}
    .tab-button {{
      appearance: none;
      border: 1px solid rgba(56, 189, 248, 0.42);
      border-radius: 999px;
      background: rgba(56, 189, 248, 0.14);
      color: #dff7ff;
      padding: 8px 12px;
      font-weight: 700;
    }}
    .tab-button.active {{
      background: rgba(56, 189, 248, 0.24);
      border-color: rgba(125, 211, 252, 0.72);
      color: #ffffff;
      box-shadow: 0 0 0 1px rgba(125, 211, 252, 0.18) inset;
    }}
    .tab-panel {{ display: grid; gap: 14px; }}
    .readiness-line {{ color: var(--muted); margin-bottom: 0; }}
    .gate-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(230px, 1fr)); gap: 10px; }}
    .gate {{
      padding: 13px;
      border-radius: 12px;
      border: 1px solid var(--border);
      background: rgba(11, 18, 32, 0.84);
      min-height: 132px;
    }}
    .gate .name {{ font-weight: 750; margin-bottom: 6px; }}
    .gate .message {{ color: var(--muted); font-size: 0.9rem; margin: 8px 0 0; }}
    .gate.good {{ border-color: rgba(74, 222, 128, 0.36); }}
    .gate.bad {{ border-color: rgba(251, 113, 133, 0.42); }}
    .state-pill {{
      width: fit-content;
      padding: 6px 12px;
      border-radius: 999px;
      font-weight: 700;
      background: rgba(56, 189, 248, 0.12);
      color: #b9ecff;
      border: 1px solid rgba(56, 189, 248, 0.28);
    }}
    .state-pill.good {{ background: rgba(74, 222, 128, 0.12); color: #c6f5d2; border-color: rgba(74, 222, 128, 0.28); }}
    .state-pill.warn {{ background: rgba(251, 191, 36, 0.12); color: #ffe7a6; border-color: rgba(251, 191, 36, 0.28); }}
    .state-pill.bad {{ background: rgba(251, 113, 133, 0.12); color: #ffc3cf; border-color: rgba(251, 113, 133, 0.28); }}
    .explain-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 12px; }}
    .explain {{ background: rgba(11, 18, 32, 0.75); border: 1px solid var(--border); border-radius: 14px; padding: 14px; }}
    .explain p {{ color: var(--muted); margin-bottom: 0; }}
    .chart-shell {{ display: grid; gap: 12px; }}
    .chart-note {{ color: var(--muted); max-width: 90ch; }}
    .chart-meta {{ display: flex; gap: 10px; flex-wrap: wrap; }}
    .chart-blocker {{
      padding: 16px;
      border-radius: 14px;
      border: 1px solid rgba(251, 191, 36, 0.35);
      background: rgba(120, 53, 15, 0.18);
      color: #fde68a;
    }}
    .chart {{ width: 100%; height: 360px; border: 1px solid var(--border); border-radius: 14px; background: #0a1220; }}
    .chart text {{ fill: #cbd5e1; font-size: 11px; }}
    .axis {{ stroke: #33506d; stroke-width: 1; }}
    .price-line {{ fill: none; stroke: var(--accent); stroke-width: 2.75; }}
    .price-dot {{ stroke: #0a1220; stroke-width: 2; }}
    .legend {{ display: flex; flex-wrap: wrap; gap: 12px; align-items: center; color: var(--muted); font-size: 0.92rem; }}
    .legend-item {{ display: inline-flex; align-items: center; gap: 6px; }}
    .swatch {{ width: 12px; height: 12px; border-radius: 999px; display: inline-block; }}
    .swatch.price {{ background: var(--accent); }}
    .swatch.bear {{ background: #fb7185; }}
    .swatch.sideways {{ background: #fbbf24; }}
    .swatch.bull {{ background: #4ade80; }}
    details {{
      border: 1px solid var(--border);
      border-radius: 14px;
      background: rgba(11, 18, 32, 0.92);
      overflow: hidden;
    }}
    summary {{
      cursor: pointer;
      list-style: none;
      padding: 14px 16px;
      font-weight: 650;
    }}
    summary::-webkit-details-marker {{ display: none; }}
    details .body {{ padding: 0 16px 16px; border-top: 1px solid rgba(36, 50, 69, 0.75); }}
    .date-grid {{ display: grid; gap: 14px; }}
    .date-summary {{ display: flex; gap: 10px; flex-wrap: wrap; align-items: center; color: var(--muted); margin-top: 6px; font-weight: 500; }}
    .compact-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 10px; margin-top: 12px; }}
    .compact {{ padding: 12px; border-radius: 12px; background: rgba(15, 23, 42, 0.65); border: 1px solid rgba(36, 50, 69, 0.8); }}
    .compact .k {{ color: var(--muted); font-size: 0.85rem; }}
    .compact .v {{ font-weight: 650; margin-top: 4px; }}
    .article {{ margin-top: 12px; }}
    .article-title {{ display: flex; gap: 10px; flex-wrap: wrap; align-items: center; }}
    .article-copy {{ color: var(--muted); margin: 10px 0; }}
    .article-copy strong {{ color: var(--text); }}
    .row-list {{ display: grid; gap: 8px; }}
    .row-item {{ border: 1px solid rgba(36, 50, 69, 0.8); border-radius: 12px; padding: 12px; background: rgba(15, 23, 42, 0.68); }}
    .row-item pre {{ margin: 8px 0 0; white-space: pre-wrap; word-break: break-word; color: #d6e4ff; }}
    .muted {{ color: var(--muted); }}
    .good {{ color: #baf7c9; }}
    .warn {{ color: #fde68a; }}
    .bad {{ color: #fecdd3; }}
    .sentiment-positive {{ color: #baf7c9; }}
    .sentiment-negative {{ color: #fecdd3; }}
    .sentiment-neutral {{ color: #dbeafe; }}
    .sentiment-unknown {{ color: var(--muted); }}
    .hidden {{ display: none !important; }}
    .section-note {{ color: var(--muted); max-width: 90ch; }}
    .footer-note {{ color: var(--muted); font-size: 0.9rem; }}
    .loading {{ color: var(--muted); padding: 8px 0; }}
  </style>
</head>
<body data-smoke-status="loading">
  <header>
    <h1>Layer 1 semantic-review dashboard</h1>
    <p class="subtitle">A calm, beginner-friendly review page for checking whether the Apple news signal and the market benchmark story make sense before anyone relies on them.</p>
    <div class="topline" id="meta"></div>
  </header>
  <main>
    <div class="stack">
      <section class="panel state-card" id="state-panel">
        <div class="state-pill warn" id="review-state">Loading review…</div>
        <h2>What am I looking at?</h2>
        <p id="state-explainer" class="section-note">Only AI/ML/NLP evidence belongs here: article relevance, embeddings/topics, FinBERT sentence sentiment, ticker-date NLP aggregates, and market-wide HMM regime context.</p>
        <div class="hero-grid" id="metrics"></div>
      </section>

      <nav class="tab-bar" aria-label="Semantic dashboard tabs">
        <button class="tab-button active" type="button" role="tab" aria-selected="true" aria-controls="summary-gate-tab" data-tab-target="summary-gate-tab">Summary / Gate Status</button>
        <button class="tab-button" type="button" role="tab" aria-selected="false" aria-controls="article-review-tab" data-tab-target="article-review-tab">Article Review</button>
        <button class="tab-button" type="button" role="tab" aria-selected="false" aria-controls="finbert-sentence-review-tab" data-tab-target="finbert-sentence-review-tab">FinBERT Sentence Review</button>
        <button class="tab-button" type="button" role="tab" aria-selected="false" aria-controls="topic-relevance-tab" data-tab-target="topic-relevance-tab">Topic / Relevance Pipeline</button>
        <button class="tab-button" type="button" role="tab" aria-selected="false" aria-controls="semantic-aggregate-tab" data-tab-target="semantic-aggregate-tab">Ticker-Date Semantic Aggregates</button>
        <button class="tab-button" type="button" role="tab" aria-selected="false" aria-controls="hmm-regime-tab" data-tab-target="hmm-regime-tab">HMM Regime</button>
      </nav>

      <section class="panel tab-panel" id="summary-gate-tab" role="tabpanel">
        <div>
          <h2>Summary / Gate Status</h2>
          <p class="readiness-line" id="readiness-line">Loading run readiness…</p>
        </div>
        <div class="hero-grid" id="summary-cards"></div>
        <div id="missing-sections"></div>
        <div class="gate-grid" id="gate-cards"></div>
      </section>

      <section class="panel tab-panel hidden" id="article-review-tab" role="tabpanel" aria-hidden="true">
        <div>
          <h2>Article Review</h2>
          <p class="section-note">Accepted AAPL article groups appear first. Articles with contamination, weak ticker evidence, or non-AAPL focus are separated below so they cannot be mistaken for clean evidence.</p>
        </div>
        <div id="article-review-content"></div>
      </section>

      <section class="panel tab-panel hidden" id="finbert-sentence-review-tab" role="tabpanel" aria-hidden="true">
        <div>
          <h2>FinBERT Sentence Review</h2>
          <p class="section-note">Review one scored sentence/chunk at a time. Each row now shows a Sentence sentiment label, positive/negative/neutral probabilities, sentiment score, and the exact text FinBERT scored.</p>
        </div>
        <div id="finbert-sentence-review-content"></div>
        <details class="panel" id="advanced-section">
          <summary>Advanced evidence and raw rows</summary>
          <div class="body" id="advanced-content">
            <p class="section-note">This section stays collapsed by default so the dashboard remains easy to scan. It is only here when you need the raw preprocessing, embeddings, topic labels, relevance-gate rows, FinBERT rows, and ticker-date semantic aggregates for debugging.</p>
            <div id="nlp-pipeline"></div>
          </div>
        </details>
      </section>

      <section class="panel tab-panel hidden" id="topic-relevance-tab" role="tabpanel" aria-hidden="true">
        <div>
          <h2>Topic / Relevance Pipeline</h2>
          <p class="section-note">This tab follows the article from ticker/entity preprocessing through embedding cache, BERTopic label, and pre-FinBERT relevance-gate evidence. If the Pre-FinBERT relevance gate artifact is missing, this tab is not reviewable yet even when embeddings and topics exist.</p>
        </div>
        <div id="topic-relevance-content"></div>
      </section>

      <section class="panel tab-panel hidden" id="semantic-aggregate-tab" role="tabpanel" aria-hidden="true">
        <div>
          <h2>Ticker-Date Semantic Aggregates</h2>
          <p class="section-note">Human-review digest for the final Layer 1 NLP feature rows: one record per <strong>(date, ticker)</strong>, focused on sentiment direction, positive/negative/neutral mix, article/sentence volume, and relevance.</p>
        </div>
        <div id="semantic-aggregate-content"></div>
      </section>

      <section class="panel tab-panel hidden" id="hmm-regime-tab" role="tabpanel" aria-hidden="true">
        <div>
          <h2>HMM Regime</h2>
          <p class="section-note">HMM applies once per market/inference date and is shared context for every ticker/news row on that date. This tab is intentionally benchmark-first: it uses the market benchmark, usually SPY, so reviewers can validate the regime context without confusing it with company-specific article evidence.</p>
        </div>
        <div class="hero-grid" id="hmm-summary-cards"></div>
        <section class="panel chart-shell" id="chart-section">
          <h3>Market benchmark and HMM regime</h3>
          <p class="chart-note">HMM regime is market-wide and date-level, so the default chart uses <strong>SPY</strong> as the benchmark instead of the selected company ticker. The line shows the benchmark price trend; the colored markers and bars show which regime was most likely on each date and how confident the model was.</p>
          <div id="chart-meta" class="chart-meta"></div>
          <div id="chart-container" class="loading">Loading benchmark chart…</div>
        </section>
        <details class="panel" id="hmm-context-section">
          <summary>Model inputs and date-by-date regime rows</summary>
          <div class="body">
            <p class="section-note">These diagnostics stay collapsed by default so the SPY graph is visible first. Open them when you need the model inputs, missing feature summary, and per-date close/probability rows.</p>
            <div class="compact-grid" id="hmm-context-cards"></div>
            <div class="row-list" id="hmm-date-rows"></div>
          </div>
        </details>
        <details class="panel" id="hmm-advanced-section">
          <summary>Advanced HMM evidence and raw rows</summary>
          <div class="body">
            <p class="section-note">This section stays collapsed by default so the dashboard remains easy to scan. It is only here when you need the raw date-level regime, benchmark price, and benchmark/HMM alignment rows for debugging.</p>
            <div id="hmm-pipeline"></div>
          </div>
        </details>
      </section>

      <section class="panel">
        <h2>Why does it matter?</h2>
        <div class="explain-grid">
          <div class="explain">
            <h3>Good sign</h3>
            <p>The page shows a benchmark chart, clear article evidence, and the status stays on <strong>Ready to review</strong>.</p>
          </div>
          <div class="explain">
            <h3>Bad sign</h3>
            <p>If the benchmark is missing, the HMM manifest is incomplete, or the news rows are too thin, the page should tell you that plainly.</p>
          </div>
          <div class="explain">
            <h3>What changes the answer?</h3>
            <p>Look for direct Apple evidence, the market benchmark trend, and whether the HMM metadata says the model was actually ready on the dates shown.</p>
          </div>
        </div>
      </section>

      <p class="footer-note">Tip: if anything in the chart is missing, trust the blocker card instead of guessing. The dashboard is designed to fail loudly when the benchmark or HMM metadata is incomplete.</p>
    </div>
  </main>
  <script>
    const defaults = {defaults_json};
    const metaEl = document.getElementById('meta');
    const metricsEl = document.getElementById('metrics');
    const summaryCardsEl = document.getElementById('summary-cards');
    const gateCardsEl = document.getElementById('gate-cards');
    const readinessLineEl = document.getElementById('readiness-line');
    const missingSectionsEl = document.getElementById('missing-sections');
    const stateEl = document.getElementById('review-state');
    const stateExplainerEl = document.getElementById('state-explainer');
    const chartMetaEl = document.getElementById('chart-meta');
    const chartContainerEl = document.getElementById('chart-container');
    const articleReviewEl = document.getElementById('article-review-content');
    const finbertReviewEl = document.getElementById('finbert-sentence-review-content');
    const topicRelevanceReviewEl = document.getElementById('topic-relevance-content');
    const semanticAggregateReviewEl = document.getElementById('semantic-aggregate-content');
    const nlpPipelineEl = document.getElementById('nlp-pipeline');
    const hmmSummaryCardsEl = document.getElementById('hmm-summary-cards');
    const hmmContextCardsEl = document.getElementById('hmm-context-cards');
    const hmmDateRowsEl = document.getElementById('hmm-date-rows');
    const hmmPipelineEl = document.getElementById('hmm-pipeline');
    const tabButtons = Array.from(document.querySelectorAll('[data-tab-target]'));

    function escapeHtml(value) {{
      return String(value ?? '')
        .replaceAll('&', '&amp;')
        .replaceAll('<', '&lt;')
        .replaceAll('>', '&gt;')
        .replaceAll('"', '&quot;')
        .replaceAll("'", '&#39;');
    }}

    function formatNumber(value, digits = 2) {{
      const number = Number(value);
      if (!Number.isFinite(number)) return 'n/a';
      return number.toFixed(digits);
    }}

    function sentimentClass(label) {{
      if (label === 'positive') return 'sentiment-positive';
      if (label === 'negative') return 'sentiment-negative';
      if (label === 'neutral') return 'sentiment-neutral';
      return 'sentiment-unknown';
    }}

    function sentimentBadge(label) {{
      const safeLabel = label || 'unknown';
      return `<span class="badge ${{sentimentClass(safeLabel)}}">${{escapeHtml(safeLabel)}}</span>`;
    }}

    function sentimentLabelCounts(counts) {{
      const source = counts || {{}};
      return `positive: ${{Number(source.positive || 0)}} · negative: ${{Number(source.negative || 0)}} · neutral: ${{Number(source.neutral || 0)}} · unknown: ${{Number(source.unknown || 0)}}`;
    }}

    function labelState(state) {{
      return state || 'Not enough evidence';
    }}

    function stateClass(state) {{
      if (state === 'Ready to review') return 'good';
      if (state === 'Needs data fix') return 'warn';
      if (state === 'Needs model/pipeline fix') return 'bad';
      return 'warn';
    }}

    function metricCard(label, value, rawName, title) {{
      return `
        <div class="metric" title="${{escapeHtml(title || rawName)}}">
          <div class="label">${{escapeHtml(label)}}</div>
          <div class="value">${{escapeHtml(value)}}</div>
          <div class="raw">raw: ${{escapeHtml(rawName)}}</div>
        </div>`;
    }}

    function deriveReviewState(payload) {{
      const smoke = payload.smoke || {{}};
      if (smoke.status === 'fail') {{
        const failures = Array.isArray(smoke.failures) ? smoke.failures : [];
        const hasHmmFailure = failures.some((failure) => String(failure.stage || '').includes('hmm') || String(failure.stage || '').includes('benchmark'));
        return {{
          state: hasHmmFailure ? 'Needs model/pipeline fix' : 'Needs data fix',
          reason: 'The real-artifact smoke gate failed, so this dashboard is not ready for final human acceptance.'
        }};
      }}
      const summary = payload.summary || {{}};
      const warnings = Array.isArray(payload.warnings) ? payload.warnings : [];
      const hmmContext = payload.hmm_evaluation_context || {{}};
      const benchmarkSeries = Array.isArray(payload.benchmark_market_regime_series) ? payload.benchmark_market_regime_series : [];
      const benchmarkPrices = Array.isArray(payload.benchmark_price_series) ? payload.benchmark_price_series : [];
      const articles = Array.isArray(payload.article_groups) ? payload.article_groups : [];
      const hasArticles = Number(summary.article_count || 0) > 0 && articles.length > 0;
      const hasDates = Number(summary.date_count || 0) > 0;
      const benchmarkMissing = benchmarkSeries.length === 0 || benchmarkPrices.length === 0;
      const manifestMissing = (Array.isArray(hmmContext.source_manifest_keys) && hmmContext.source_manifest_keys.length === 0)
        || (Array.isArray(hmmContext.training_windows) && hmmContext.training_windows.length === 0)
        || (Array.isArray(hmmContext.manifest_summaries) && hmmContext.manifest_summaries.length === 0);
      const modelWarningCodes = new Set([
        'missing_hmm_manifest',
        'missing_training_window_metadata',
        'incomplete_hmm_feature_set',
        'stale_hmm_manifest',
        'hmm_not_evaluated_for_date',
        'missing_hmm_inference_dates',
        'unexpected_hmm_inference_dates',
      ]);
      const dataWarningScopes = new Set(['price_series', 'hmm_regime', 'hmm_evaluation_context']);
      const hasModelWarnings = Array.isArray(hmmContext.warnings) && hmmContext.warnings.some((warning) => modelWarningCodes.has(String(warning)));
      const hasDataWarnings = warnings.some((warning) => dataWarningScopes.has(String(warning.scope || '')));
      if (!hasArticles || !hasDates) {{
        return {{ state: 'Not enough evidence', reason: 'The page does not yet have enough rows to make a calm judgment.' }};
      }}
      if (benchmarkMissing || warnings.some((warning) => warning.scope === 'benchmark_price_series')) {{
        return {{ state: 'Needs data fix', reason: 'The market benchmark chart is missing, so this page should not pretend the HMM view is complete.' }};
      }}
      if (manifestMissing || hasModelWarnings || hasDataWarnings) {{
        return {{ state: 'Needs model/pipeline fix', reason: 'The HMM evidence or upstream pipeline metadata is incomplete.' }};
      }}
      return {{ state: 'Ready to review', reason: 'The page has enough benchmark, HMM, and article evidence to inspect.' }};
    }}

    function renderTopline(payload, reviewState) {{
      const benchmarkTicker = payload.benchmark_ticker || 'SPY';
      const windowText = `${{payload.from_date || defaults.from_date}} → ${{payload.to_date || defaults.to_date}}`;
      metaEl.innerHTML = [
        badge('run_id', payload.run_id || defaults.run_id),
        badge('ticker', payload.ticker || defaults.ticker),
        badge('benchmark', benchmarkTicker),
        badge('window', windowText),
      ].join('');
      stateEl.textContent = reviewState.state;
      stateEl.className = `state-pill ${{stateClass(reviewState.state)}}`;
      stateExplainerEl.textContent = reviewState.reason;
    }}

    function badge(label, value) {{
      return `<span class="badge"><strong>${{escapeHtml(label)}}:</strong> ${{escapeHtml(value)}}</span>`;
    }}

    function compactCard(label, value, rawName) {{
      return `<div class="compact"><div class="k">${{escapeHtml(label)}}</div><div class="v">${{escapeHtml(value)}}</div><div class="k">${{escapeHtml(rawName)}}</div></div>`;
    }}

    function dominanceLabel(regime) {{
      const bear = Number(regime?.prob_bear ?? 0);
      const sideways = Number(regime?.prob_sideways ?? 0);
      const bull = Number(regime?.prob_bull ?? 0);
      if (bear >= sideways && bear >= bull) return 'bear';
      if (bull >= bear && bull >= sideways) return 'bull';
      return 'sideways';
    }}

    function renderMetrics(payload) {{
      const summary = payload.summary || {{}};
      const benchmarkTicker = payload.benchmark_ticker || 'SPY';
      metricsEl.innerHTML = [
        metricCard('Stories scanned', summary.article_count ?? 0, 'article_count', 'How many article rows were loaded for this review.'),
        metricCard('Days in view', summary.date_count ?? 0, 'date_count', 'How many trading dates are available in the review window.'),
        metricCard('Accepted stories', summary.accepted_article_count ?? 0, 'accepted_article_count', 'Stories that look like they belong in the default review path.'),
        metricCard('Flagged stories', summary.flagged_article_count ?? 0, 'flagged_article_count', 'Stories that need a closer look because their evidence is weak or off-topic.'),
        metricCard('Market benchmark', benchmarkTicker, 'benchmark_ticker', 'The benchmark ticker used for the HMM regime chart.'),
      ].join('');
    }}

    function renderHmmOverview(payload) {{
      const hmmContext = payload.hmm_evaluation_context || {{}};
      const benchmarkTicker = payload.benchmark_ticker || 'SPY';
      const benchmarkSeries = Array.isArray(payload.benchmark_market_regime_series) ? payload.benchmark_market_regime_series : [];
      const benchmarkPrices = Array.isArray(payload.benchmark_price_series) ? payload.benchmark_price_series : [];
      const trainingWindows = Array.isArray(hmmContext.training_windows) ? hmmContext.training_windows : [];
      const sourceManifestKeys = Array.isArray(hmmContext.source_manifest_keys) ? hmmContext.source_manifest_keys : [];
      const inputFeatureColumns = Array.isArray(hmmContext.input_feature_columns_used) ? hmmContext.input_feature_columns_used : [];
      const missingFeatureColumns = Array.isArray(hmmContext.dropped_feature_columns) ? hmmContext.dropped_feature_columns : [];
      const warnings = Array.isArray(hmmContext.warnings) ? hmmContext.warnings : [];
      const expectedFeatureColumns = Array.isArray(hmmContext.expected_input_feature_columns) ? hmmContext.expected_input_feature_columns : [];
      if (hmmSummaryCardsEl) {{
        hmmSummaryCardsEl.innerHTML = [
          metricCard('Benchmark', benchmarkTicker, 'benchmark_ticker', 'The market benchmark used for the HMM regime chart and date rows.'),
          metricCard('Date rows', benchmarkSeries.length, 'benchmark_market_regime_series.length', 'How many benchmark/HMM rows are available for review.'),
          metricCard('Benchmark prices', benchmarkPrices.length, 'benchmark_price_series.length', 'How many SPY/S&P 500 price rows are available.'),
          metricCard('Training windows', trainingWindows.length, 'hmm_evaluation_context.training_windows.length', 'How many HMM training-window metadata blocks are present.'),
        ].join('');
      }}
      if (hmmContextCardsEl) {{
        hmmContextCardsEl.innerHTML = [
          compactCard('Scope', hmmContext.scope || 'n/a', 'raw: hmm_evaluation_context.scope'),
          compactCard('Applies to', hmmContext.applies_to || 'n/a', 'raw: hmm_evaluation_context.applies_to'),
          compactCard('Input columns used', inputFeatureColumns.length ? inputFeatureColumns.join(', ') : 'n/a', 'raw: hmm_evaluation_context.input_feature_columns_used'),
          compactCard('Missing feature columns', missingFeatureColumns.length ? missingFeatureColumns.join(', ') : 'none', 'raw: hmm_evaluation_context.dropped_feature_columns'),
          compactCard('Source manifest keys', sourceManifestKeys.length ? sourceManifestKeys.join(', ') : 'n/a', 'raw: hmm_evaluation_context.source_manifest_keys'),
          compactCard('Warnings', warnings.length ? warnings.join(', ') : 'none', 'raw: hmm_evaluation_context.warnings'),
          compactCard('Expected feature columns', expectedFeatureColumns.length ? expectedFeatureColumns.join(', ') : 'n/a', 'raw: hmm_evaluation_context.expected_input_feature_columns'),
        ].join('');
      }}
      if (hmmDateRowsEl) {{
        hmmDateRowsEl.innerHTML = benchmarkSeries.length
          ? benchmarkSeries.map((row) => {{
              const regime = row.hmm_regime || {{}};
              const price = row.price || {{}};
              const label = regime.regime || dominanceLabel(regime);
              const closeValue = price.adj_close ?? price.close;
              const rowWarnings = Array.isArray(row.warnings) && row.warnings.length ? row.warnings.join(', ') : 'none';
              return `
                <div class="row-item">
                  <div class="article-title">
                    <span class="badge">${{escapeHtml(row.date || 'n/a')}}</span>
                    <span class="badge ${{label === 'bear' ? 'bad' : label === 'bull' ? 'good' : 'warn'}}">${{escapeHtml(label)}}</span>
                    <span class="badge">confidence ${{formatNumber(regime.confidence, 2)}}</span>
                  </div>
                  <div class="compact-grid">
                    <div class="compact"><div class="k">Benchmark close</div><div class="v">${{formatNumber(closeValue, 2)}}</div><div class="k">raw: close / adj_close</div></div>
                    <div class="compact"><div class="k">Bear prob</div><div class="v">${{formatNumber(regime.prob_bear, 2)}}</div><div class="k">raw: prob_bear</div></div>
                    <div class="compact"><div class="k">Sideways prob</div><div class="v">${{formatNumber(regime.prob_sideways, 2)}}</div><div class="k">raw: prob_sideways</div></div>
                    <div class="compact"><div class="k">Bull prob</div><div class="v">${{formatNumber(regime.prob_bull, 2)}}</div><div class="k">raw: prob_bull</div></div>
                    <div class="compact"><div class="k">Warnings</div><div class="v">${{escapeHtml(rowWarnings)}}</div><div class="k">raw: row.warnings</div></div>
                  </div>
                </div>`;
            }}).join('')
          : '<p class="muted">No benchmark/HMM rows are available for this run.</p>';
      }}
    }}
    function renderSummaryGateStatus(payload) {{
      const readiness = payload.run_readiness || {{}};
      const cards = Array.isArray(payload.summary_cards) ? payload.summary_cards : [];
      const gates = Array.isArray(payload.gate_cards) ? payload.gate_cards : [];
      const missingSections = Array.isArray(payload.missing_pipeline_sections) ? payload.missing_pipeline_sections : [];
      const ready = readiness.ready_for_final_human_acceptance === true;
      const recommendation = readiness.recommendation || (ready ? 'ready for final human acceptance' : 'not ready for final human acceptance');
      readinessLineEl.innerHTML = `
        <strong class="${{ready ? 'good' : 'bad'}}">${{escapeHtml(recommendation)}}</strong>
        <span class="muted"> · human review status: ${{escapeHtml(readiness.human_review_status || 'unknown')}}</span>`;
      summaryCardsEl.innerHTML = cards.map((card) => metricCard(
        card.label || card.field || 'Summary',
        card.value ?? 'n/a',
        card.field || card.label || 'summary',
        card.field || card.label || 'summary'
      )).join('');
      missingSectionsEl.innerHTML = missingSections.length
        ? `<div class="chart-blocker">
            <h3>Human review remains blocked</h3>
            <p>The run is <strong>not ready for final human acceptance</strong> because required NLP, HMM, or price evidence is missing.</p>
            <ul>${{missingSections.map((section) => `<li>${{escapeHtml(section.label || section.key)}}: ${{escapeHtml(section.reason || 'missing required evidence')}}</li>`).join('')}}</ul>
          </div>`
        : `<p class="readiness-line good">Required NLP, HMM, and price evidence is present. Human semantic review can start.</p>`;
      gateCardsEl.innerHTML = gates.map((gate) => {{
        const blocked = gate.status === 'blocked';
        const keys = Array.isArray(gate.missing_or_tried_keys) ? gate.missing_or_tried_keys : [];
        const artifacts = Array.isArray(gate.artifact_keys) ? gate.artifact_keys : [];
        return `
          <div class="gate ${{blocked ? 'bad' : 'good'}}">
            <div class="name">${{escapeHtml(gate.label || gate.key || 'Gate')}}</div>
            <span class="state-pill ${{blocked ? 'bad' : 'good'}}">${{blocked ? 'Blocked' : 'Ready'}}</span>
            <div class="message">rows: ${{Number(gate.row_count || 0)}} · required: ${{gate.required === false ? 'no' : 'yes'}}</div>
            <div class="message">${{escapeHtml(gate.message || '')}}</div>
            ${{keys.length ? `<div class="message">missing/tried: ${{escapeHtml(keys.slice(0, 3).join(', '))}}${{keys.length > 3 ? '…' : ''}}</div>` : ''}}
            ${{!keys.length && artifacts.length ? `<div class="message">artifact: ${{escapeHtml(artifacts[0])}}</div>` : ''}}
          </div>`;
      }}).join('');
    }}

    function renderChart(payload) {{
      const benchmarkTicker = payload.benchmark_ticker || 'SPY';
      const prices = Array.isArray(payload.benchmark_price_series) ? payload.benchmark_price_series : [];
      const rows = Array.isArray(payload.benchmark_market_regime_series) ? payload.benchmark_market_regime_series : [];
      const hmmContext = payload.hmm_evaluation_context || {{}};
      const manifestSummaries = Array.isArray(hmmContext.manifest_summaries) ? hmmContext.manifest_summaries : [];
      const trainingWindows = Array.isArray(hmmContext.training_windows) ? hmmContext.training_windows : [];
      const hasManifest = manifestSummaries.length > 0;
      const hasTrainingWindow = trainingWindows.length > 0;
      const hasRenderablePrice = rows.some((row) => {{
        const price = row.price || {{}};
        const value = Number(price.adj_close ?? price.close);
        return Number.isFinite(value);
      }});
      const hasRenderableProbability = rows.some((row) => {{
        const regime = row.hmm_regime || {{}};
        return ['prob_bear', 'prob_sideways', 'prob_bull'].some((field) => Number.isFinite(Number(regime[field])));
      }});
      const rowWarnings = rows.flatMap((row) => Array.isArray(row.warnings) ? row.warnings : []);
      const missingReasons = [];
      if (!prices.length) missingReasons.push(`No ${{benchmarkTicker}} price rows were available for the chart.`);
      if (!rows.length) missingReasons.push('No HMM regime rows were available for the benchmark dates.');
      if (rows.length && !hasRenderablePrice) missingReasons.push(`The ${{benchmarkTicker}} rows did not contain numeric close or adjusted-close values.`);
      if (rows.length && !hasRenderableProbability) missingReasons.push('The HMM rows did not contain numeric regime probabilities.');
      if (rowWarnings.includes('missing_price')) missingReasons.push('At least one benchmark chart date is missing benchmark price context.');
      if (rowWarnings.includes('all_null_hmm_regime')) missingReasons.push('At least one HMM row has all label/probability fields null.');
      if (!hasManifest) missingReasons.push('The HMM manifest summary is missing, so we cannot verify the training window.');
      if (!hasTrainingWindow) missingReasons.push('The HMM training-window metadata is missing, so the model readiness check is incomplete.');
      if (missingReasons.length) {{
        chartContainerEl.dataset.smokeStatus = 'fail';
        chartMetaEl.innerHTML = [
          badge('benchmark', benchmarkTicker),
          badge('date range', `${{payload.from_date || defaults.from_date}} → ${{payload.to_date || defaults.to_date}}`),
        ].join('');
        chartContainerEl.innerHTML = `
          <div class="chart-blocker">
            <h3>Benchmark chart blocked</h3>
            <p>What am I looking at? A market-wide HMM check that should use the benchmark, not the company ticker.</p>
            <p>Why does it matter? If the benchmark price rows or HMM manifest metadata are missing, the chart would be misleading.</p>
            <p>What would make this good or bad? Good: SPY rows plus a training window and manifest summary. Bad: empty prices, empty regime rows, or missing manifest details.</p>
            <ul>${{missingReasons.map((reason) => `<li>${{escapeHtml(reason)}}</li>`).join('')}}</ul>
          </div>`;
        return;
      }}
      chartContainerEl.dataset.smokeStatus = 'pass';

      const width = 1040;
      const height = 360;
      const left = 64;
      const right = 18;
      const top = 18;
      const priceBottom = 210;
      const probTop = 242;
      const probHeight = 74;
      const priceValues = prices.map((row) => Number(row.adj_close ?? row.close)).filter((value) => Number.isFinite(value));
      const minPrice = priceValues.length ? Math.min(...priceValues) : 0;
      const maxPrice = priceValues.length ? Math.max(...priceValues) : 1;
      const priceRange = Math.max(maxPrice - minPrice, 0.0001);
      const step = rows.length > 1 ? (width - left - right) / (rows.length - 1) : 0;
      const xFor = (index) => left + index * step;
      const yFor = (value) => priceBottom - ((value - minPrice) / priceRange) * (priceBottom - top);
      const priceSeries = rows.map((row, index) => {{
        const price = row.price || {{}};
        const value = Number(price.adj_close ?? price.close);
        return Number.isFinite(value) ? `${{xFor(index)}},${{yFor(value)}}` : null;
      }}).filter(Boolean).join(' ');
      const dominantRegime = (regime) => {{
        const bear = Number(regime?.prob_bear ?? 0);
        const sideways = Number(regime?.prob_sideways ?? 0);
        const bull = Number(regime?.prob_bull ?? 0);
        if (bear >= sideways && bear >= bull) return 'bear';
        if (bull >= bear && bull >= sideways) return 'bull';
        return 'sideways';
      }};
      const regimeColor = (regime) => {{
        const label = dominantRegime(regime);
        if (label === 'bear') return 'rgba(251, 113, 133, 0.16)';
        if (label === 'bull') return 'rgba(74, 222, 128, 0.16)';
        return 'rgba(251, 191, 36, 0.18)';
      }};
      const regimeBorder = (regime) => {{
        const label = dominantRegime(regime);
        if (label === 'bear') return '#fb7185';
        if (label === 'bull') return '#4ade80';
        return '#fbbf24';
      }};
      const probabilityBars = rows.map((row, index) => {{
        const regime = row.hmm_regime || {{}};
        const x = xFor(index) - 10;
        const widthBar = 20;
        const bear = Math.max(Number(regime.prob_bear ?? 0), 0) * probHeight;
        const sideways = Math.max(Number(regime.prob_sideways ?? 0), 0) * probHeight;
        const bull = Math.max(Number(regime.prob_bull ?? 0), 0) * probHeight;
        const yBear = probTop + probHeight - bear;
        const ySideways = yBear - sideways;
        const yBull = ySideways - bull;
        return `
          <rect x="${{x}}" y="${{probTop}}" width="${{widthBar}}" height="${{probHeight}}" rx="4" ry="4" fill="rgba(15, 23, 42, 0.3)" stroke="#22334a" />
          <rect x="${{x}}" y="${{yBear}}" width="${{widthBar}}" height="${{bear}}" fill="#fb7185" opacity="0.92"></rect>
          <rect x="${{x}}" y="${{ySideways}}" width="${{widthBar}}" height="${{sideways}}" fill="#fbbf24" opacity="0.92"></rect>
          <rect x="${{x}}" y="${{yBull}}" width="${{widthBar}}" height="${{bull}}" fill="#4ade80" opacity="0.92"></rect>`;
      }}).join('');
      const regimeBands = rows.map((row, index) => {{
        const price = row.price || {{}};
        const value = Number(price.adj_close ?? price.close);
        const regime = row.hmm_regime || {{}};
        const x = xFor(index);
        const y = Number.isFinite(value) ? yFor(value) : priceBottom;
        const label = dominantRegime(regime);
        const fill = regimeColor(regime);
        const border = regimeBorder(regime);
        const confidence = formatNumber(regime.confidence, 2);
        const warningText = Array.isArray(row.warnings) ? row.warnings.join(', ') : '';
        return `
          <rect x="${{x - 8}}" y="${{top}}" width="16" height="${{priceBottom - top}}" fill="${{fill}}" stroke="${{border}}" stroke-opacity="0.55" opacity="0.9">
            <title>${{row.date}}: ${{label}} regime, confidence ${{confidence}}${{warningText ? `; warnings: ${{warningText}}` : ''}}</title>
          </rect>
          <circle cx="${{x}}" cy="${{y}}" r="5.5" fill="${{border}}" class="price-dot">
            <title>${{row.date}}: ${{benchmarkTicker}} adjusted close ${{formatNumber(price.adj_close ?? price.close, 2)}}; regime ${{label}}; confidence ${{confidence}}</title>
          </circle>`;
      }}).join('');
      const dateLabels = rows.map((row, index) => `<text x="${{xFor(index) - 20}}" y="338">${{String(row.date || '').slice(5)}}</text>`).join('');
      const chartWarnings = Array.isArray(hmmContext.warnings) && hmmContext.warnings.length ? hmmContext.warnings.join(', ') : 'none';
      chartMetaEl.innerHTML = [
        badge('benchmark', benchmarkTicker),
        badge('date range', `${{payload.from_date || defaults.from_date}} → ${{payload.to_date || defaults.to_date}}`),
        badge('manifest', hasManifest ? 'present' : 'missing'),
        badge('training window', hasTrainingWindow ? 'present' : 'missing'),
      ].join('');
      chartContainerEl.innerHTML = `
        <svg class="chart" viewBox="0 0 ${{width}} ${{height}}" role="img" aria-label="${{benchmarkTicker}} price and HMM regime chart">
          <line class="axis" x1="${{left}}" y1="${{priceBottom}}" x2="${{width - right}}" y2="${{priceBottom}}"></line>
          <line class="axis" x1="${{left}}" y1="${{probTop + probHeight}}" x2="${{width - right}}" y2="${{probTop + probHeight}}"></line>
          <text x="8" y="${{top + 12}}">${{formatNumber(maxPrice, 2)}}</text>
          <text x="8" y="${{priceBottom}}">${{formatNumber(minPrice, 2)}}</text>
          <text x="8" y="${{probTop + 14}}">probability</text>
          <text x="8" y="${{probTop + probHeight}}">0</text>
          <text x="8" y="${{probTop + 30}}">0.5</text>
          <text x="8" y="${{probTop + probHeight - 2}}">1.0</text>
          <polyline class="price-line" points="${{priceSeries}}"></polyline>
          ${{regimeBands}}
          ${{probabilityBars}}
          ${{dateLabels}}
        </svg>
        <div class="legend">
          <span class="legend-item"><span class="swatch price"></span>${{escapeHtml(benchmarkTicker)}} adjusted close</span>
          <span class="legend-item"><span class="swatch bear"></span>bear probability</span>
          <span class="legend-item"><span class="swatch sideways"></span>sideways probability</span>
          <span class="legend-item"><span class="swatch bull"></span>bull probability</span>
        </div>
        <p class="chart-note">What am I looking at? The market benchmark price line with HMM regime bands and probabilities on the same date axis. Why does it matter? The regime model is market-wide, so this gives the right context for the day. What would make this good or bad? Good: visible benchmark prices, non-empty probabilities, and a complete manifest. Bad: empty rows, all-null regime values, or missing training metadata.</p>
        <p class="chart-note muted">Model notes: ${{escapeHtml(chartWarnings)}}.</p>`;
    }}

    function renderArticleDetails(article) {{
      const articleStatus = article.article_status || 'flagged';
      const accepted = articleStatus === 'accepted';
      const flags = Array.isArray(article.contamination_flags) ? article.contamination_flags : [];
      const snippets = Array.isArray(article.evidence_snippets) ? article.evidence_snippets : [];
      const tickerHits = Array.isArray(article.requested_ticker_term_hits) ? article.requested_ticker_term_hits : [];
      const preprocessingRows = Array.isArray(article.preprocessing_rows) ? article.preprocessing_rows : [];
      const topicRows = Array.isArray(article.topic_evidence) ? article.topic_evidence : [];
      const relevanceRows = Array.isArray(article.relevance_gate_rows) ? article.relevance_gate_rows : [];
      return `
        <details class="article">
          <summary>
            <span class="article-title">${{escapeHtml(article.headline || article.article_id || 'Article')}}</span>
            <span class="badge ${{accepted ? 'good' : 'warn'}}">${{accepted ? 'Accepted for review' : 'Needs a closer look'}}</span>
            <span class="badge" title="Raw field: article_id">article_id: ${{escapeHtml(article.article_id || 'n/a')}}</span>
            <span class="badge" title="Raw field: article_status">status: ${{escapeHtml(articleStatus)}}</span>
          </summary>
          <div class="body">
            <p class="article-copy">What am I looking at? A single story and the evidence that tells us whether it really belongs in the Apple review. Why does it matter? Good articles should mention Apple directly or show strong Apple-specific context; bad articles should be obviously unrelated or too weak to trust. What would make this good or bad? Good: direct Apple/AAPL support, useful topic labels, and matching relevance-gate rows. Bad: unrelated company mentions, duplicate headlines, or missing provenance.</p>
            <div class="compact-grid">
              <div class="compact"><div class="k">Published</div><div class="v">${{escapeHtml(article.published_at || 'n/a')}}</div><div class="k">raw: published_at</div></div>
              <div class="compact"><div class="k">Source</div><div class="v">${{escapeHtml(article.source || 'n/a')}}</div><div class="k">raw: source</div></div>
              <div class="compact"><div class="k">Normalized headline</div><div class="v">${{escapeHtml(article.normalized_headline || 'n/a')}}</div><div class="k">raw: normalized_headline</div></div>
              <div class="compact"><div class="k">Relevance state</div><div class="v">${{escapeHtml(article.relevance_state || 'n/a')}}</div><div class="k">raw: relevance_state</div></div>
            </div>
            <p class="article-copy"><strong>Evidence snippets:</strong> ${{snippets.length ? snippets.map(escapeHtml).join(' · ') : 'none'}}</p>
            <p class="article-copy"><strong>Ticker evidence:</strong> ${{tickerHits.length ? tickerHits.map(escapeHtml).join(', ') : 'none'}}</p>
            ${{flags.length ? `<p class="article-copy bad"><strong>Flags:</strong> ${{flags.map(escapeHtml).join(', ')}}</p>` : ''}}
            <div class="row-list">
              <details>
                <summary>Technical details for this story</summary>
                <div class="body">
                  <div class="row-item"><strong>Preprocessing rows</strong><pre>${{escapeHtml(JSON.stringify(preprocessingRows, null, 2))}}</pre></div>
                  <div class="row-item"><strong>Topic evidence</strong><pre>${{escapeHtml(JSON.stringify(topicRows, null, 2))}}</pre></div>
                  <div class="row-item"><strong>Relevance-gate rows</strong><pre>${{escapeHtml(JSON.stringify(relevanceRows, null, 2))}}</pre></div>
                </div>
              </details>
            </div>
          </div>
        </details>`;
    }}

    function renderDateGroup(group) {{
      const regime = group.regime || {{}};
      const price = group.price || {{}};
      const marketContext = group.market_regime_context || {{}};
      const articles = Array.isArray(group.articles) ? group.articles : [];
      const acceptedCount = Number(group.accepted_article_count || 0);
      const flaggedCount = Number(group.flagged_article_count || 0);
      const sentenceCount = Number(group.sentence_count || 0);
      const semanticAggregates = Array.isArray(group.semantic_aggregates) ? group.semantic_aggregates : [];
      const regimeLabel = regime.regime || 'not available';
      const closeValue = price ? (price.adj_close ?? price.close) : null;
      const hints = [
        `articles: ${{group.article_count ?? 0}}`,
        `accepted: ${{acceptedCount}}`,
        `flagged: ${{flaggedCount}}`,
        `sentences: ${{sentenceCount}}`,
      ];
      return `
        <details class="date-card">
          <summary>
            <span>${{escapeHtml(group.date || 'n/a')}}</span>
            <span class="badge" title="Raw field: regime">regime: ${{escapeHtml(regimeLabel)}}</span>
            <span class="badge" title="Raw field: article_count">articles: ${{group.article_count ?? 0}}</span>
            <span class="badge" title="Raw field: sentence_count">sentences: ${{sentenceCount}}</span>
            <span class="badge" title="Raw field: close / adj_close">price: ${{formatNumber(closeValue, 2)}}</span>
          </summary>
          <div class="body">
            <p class="article-copy">What am I looking at? One trading day of Apple review evidence. Why does it matter? It shows whether the news rows, benchmark context, and regime label line up for that date. What would make this good or bad? Good: a few clear Apple stories and a readable regime signal. Bad: unrelated news, missing benchmark context, or warnings about price or HMM data.</p>
            <div class="compact-grid">
              <div class="compact"><div class="k">HMM regime</div><div class="v">${{escapeHtml(regimeLabel)}}</div><div class="k">raw: regime</div></div>
              <div class="compact"><div class="k">Confidence</div><div class="v">${{formatNumber(regime.confidence, 2)}}</div><div class="k">raw: confidence</div></div>
              <div class="compact"><div class="k">Benchmark close</div><div class="v">${{formatNumber(closeValue, 2)}}</div><div class="k">raw: close / adj_close</div></div>
              <div class="compact"><div class="k">Market context warnings</div><div class="v">${{Array.isArray(marketContext.warnings) && marketContext.warnings.length ? marketContext.warnings.join(', ') : 'none'}}</div><div class="k">raw: market_regime_context.warnings</div></div>
            </div>
            <div class="compact-grid">
              ${{hints.map((hint) => `<div class="compact"><div class="k">At a glance</div><div class="v">${{escapeHtml(hint)}}</div><div class="k">friendly summary</div></div>`).join('')}}
            </div>
            <p class="article-copy"><strong>Semantic aggregates:</strong> ${{semanticAggregates.length ? `${{semanticAggregates.length}} row(s)` : 'none'}}</p>
            <div class="row-list">
              ${{articles.map(renderArticleDetails).join('') || '<p class="muted">No articles were loaded for this date.</p>'}}
            </div>
          </div>
        </details>`;
    }}

    function renderArticleReviewGroup(group) {{
      const articles = Array.isArray(group.articles) ? group.articles : [];
      const summary = group.summary || {{}};
      return `
        <details class="date-card">
          <summary>
            <span>${{escapeHtml(group.date || 'n/a')}}</span>
            <span class="badge" title="Accepted article count">accepted: ${{Number(summary.accepted_article_count || 0)}}</span>
            <span class="badge" title="Flagged article count">flagged: ${{Number(summary.flagged_article_count || 0)}}</span>
            <span class="badge" title="Sentence count">sentences: ${{Number(summary.sentence_count || 0)}}</span>
            <span class="badge" title="Article count">articles: ${{Number(group.article_count || 0)}}</span>
          </summary>
          <div class="body">
            <p class="article-copy">What am I looking at? A date bucket for ${{escapeHtml(group.date || 'n/a')}}. Why does it matter? It keeps the accepted Apple articles together and makes contamination easy to separate. What would make this good or bad? Good: the accepted bucket contains Apple-focused stories with strong ticker evidence. Bad: the contamination bucket contains unrelated or weak stories that should not be treated as Apple evidence.</p>
            <div class="row-list">
              ${{articles.map(renderArticleDetails).join('') || '<p class="muted">No articles were loaded for this date.</p>'}}
            </div>
          </div>
        </details>`;
    }}

    function renderSentenceRow(row) {{
      const text = row.text || '';
      const hasText = Boolean(String(text).trim());
      const sourceTextField = row.source_text_field || 'n/a';
      const sourceTextOrder = row.source_text_order ?? 'n/a';
      const sentenceIndex = row.sentence_index ?? 'n/a';
      const chunkIndex = row.chunk_index ?? 'n/a';
      const rowGranularity = row.row_granularity || 'n/a';
      const relevanceState = row.relevance_state || 'missing';
      const sentimentLabel = row.sentiment_label || 'unknown';
      const sentimentConfidence = row.sentiment_label_confidence;
      const probabilities = [
        `pos: ${{formatNumber(row.positive_probability, 2)}}`,
        `neg: ${{formatNumber(row.negative_probability, 2)}}`,
        `neu: ${{formatNumber(row.neutral_probability, 2)}}`,
      ].join(' · ');
      return `
        <details class="row-item">
          <summary>
            <span class="article-title">Sentence ${{escapeHtml(sentenceIndex)}} / chunk ${{escapeHtml(chunkIndex)}}</span>
            ${{sentimentBadge(sentimentLabel)}}
            <span class="badge" title="Raw field: sentiment_label_confidence">confidence: ${{formatNumber(sentimentConfidence, 2)}}</span>
            <span class="badge">${{escapeHtml(rowGranularity)}}</span>
            <span class="badge ${{hasText ? 'good' : 'warn'}}">${{hasText ? 'Text available' : 'Text missing'}}</span>
            <span class="badge" title="Raw field: relevance_state">relevance: ${{escapeHtml(relevanceState)}}</span>
          </summary>
          <div class="body">
            ${{hasText ? `<p class="article-copy"><strong>Full scored text:</strong> ${{escapeHtml(text)}}</p>` : `<div class="chart-blocker"><h3>Full scored text unavailable</h3><p>This row came from the scored-news artifact, but the full text field is missing.</p></div>`}}
            ${{row.source_artifact_gap ? `<p class="article-copy bad"><strong>Source artifact gap:</strong> ${{escapeHtml(row.source_artifact_gap)}}</p>` : ''}}
            <div class="compact-grid">
              <div class="compact"><div class="k">Source text field</div><div class="v">${{escapeHtml(sourceTextField)}}</div><div class="k">raw: source_text_field</div></div>
              <div class="compact"><div class="k">Source text order</div><div class="v">${{escapeHtml(sourceTextOrder)}}</div><div class="k">raw: source_text_order</div></div>
              <div class="compact"><div class="k">Sentence sentiment label</div><div class="v ${{sentimentClass(sentimentLabel)}}">${{escapeHtml(sentimentLabel)}}</div><div class="k">raw: sentiment_label</div></div>
              <div class="compact"><div class="k">Label confidence</div><div class="v">${{formatNumber(sentimentConfidence, 3)}}</div><div class="k">raw: sentiment_label_confidence</div></div>
              <div class="compact"><div class="k">Sentiment score</div><div class="v">${{formatNumber(row.sentiment_score, 2)}}</div><div class="k">raw: sentiment_score</div></div>
              <div class="compact"><div class="k">Relevance score</div><div class="v">${{formatNumber(row.relevance_score, 2)}}</div><div class="k">raw: relevance_score</div></div>
            </div>
            <p class="article-copy"><strong>Probabilities:</strong> ${{probabilities}}</p>
            <p class="article-copy"><strong>Sentence/chunk index:</strong> ${{escapeHtml(sentenceIndex)}} / ${{escapeHtml(chunkIndex)}}</p>
          </div>
        </details>`;
    }}

    function renderFinbertArticle(article) {{
      const rows = Array.isArray(article.sentence_rows) ? article.sentence_rows : [];
      const rowCount = rows.length;
      return `
        <details class="article">
          <summary>
            <span class="article-title">${{escapeHtml(article.headline || article.article_id || 'Article')}}</span>
            <span class="badge ${{article.article_status === 'accepted' ? 'good' : 'warn'}}">${{article.article_status === 'accepted' ? 'Accepted for review' : 'Needs a closer look'}}</span>
            <span class="badge" title="Derived from sentence-level sentiment labels">${{escapeHtml(sentimentLabelCounts(article.sentiment_label_counts))}}</span>
            <span class="badge" title="Raw field: article_id">article_id: ${{escapeHtml(article.article_id || 'n/a')}}</span>
            <span class="badge" title="Raw field: sentence rows">rows: ${{rowCount}}</span>
          </summary>
          <div class="body">
            <p class="article-copy">What am I looking at? The full FinBERT-scored text for one article and its sentence-level rows. Why does it matter? Reviewers can verify the exact text that was scored, the source-text field/order, and the sentiment/relevance outputs without guessing. What would make this good or bad? Good: all rows have full text and the ordering matches the scored artifact. Bad: the dashboard has to warn about missing text or a source-artifact gap.</p>
            <div class="compact-grid">
              <div class="compact"><div class="k">Published</div><div class="v">${{escapeHtml(article.published_at || 'n/a')}}</div><div class="k">raw: published_at</div></div>
              <div class="compact"><div class="k">Source</div><div class="v">${{escapeHtml(article.source || 'n/a')}}</div><div class="k">raw: source</div></div>
              <div class="compact"><div class="k">Full text available</div><div class="v">${{article.full_scored_text_available ? 'yes' : 'no'}}</div><div class="k">raw: full_scored_text_available</div></div>
              <div class="compact"><div class="k">Missing text rows</div><div class="v">${{Number(article.missing_text_row_count || 0)}}</div><div class="k">raw: missing_text_row_count</div></div>
            </div>
            ${{article.full_scored_text ? `<p class="article-copy"><strong>Full scored text:</strong> ${{escapeHtml(article.full_scored_text)}}</p>` : ''}}
            ${{article.full_scored_text_warning ? `<div class="chart-blocker"><h3>Full scored text unavailable</h3><p>${{escapeHtml(article.full_scored_text_warning)}}</p><p>${{escapeHtml(article.source_artifact_gap || 'The source artifact is missing full scored text.')}}</p></div>` : ''}}
            <div class="row-list">
              ${{rows.length ? rows.map(renderSentenceRow).join('') : '<p class="muted">No sentence rows were loaded for this article.</p>'}}
            </div>
          </div>
        </details>`;
    }}

    function renderArticleReview(payload) {{
      const review = payload.article_review || {{}};
      const acceptedDateGroups = Array.isArray(review.accepted_date_groups) ? review.accepted_date_groups : [];
      const contaminationDateGroups = Array.isArray(review.contamination_date_groups) ? review.contamination_date_groups : [];
      const acceptedArticles = Number(review.accepted_article_count || 0);
      const contaminationArticles = Number(review.contamination_article_count || 0);
      const flagCounts = review.contamination_flag_counts || {{}};
      const flagSummary = Object.keys(flagCounts).length
        ? Object.entries(flagCounts).map(([flag, count]) => `${{flag}}: ${{count}}`).join(' · ')
        : 'none';
      articleReviewEl.innerHTML = `
        <div class="hero-grid">
          ${{metricCard('Accepted stories', acceptedArticles, 'article_review.accepted_article_count', 'Accepted Apple article groups that remain in the clean review path.')}}
          ${{metricCard('Contamination stories', contaminationArticles, 'article_review.contamination_article_count', 'Flagged or off-topic stories that are intentionally separated from the clean article review path.')}}
          ${{metricCard('Contamination flags', flagSummary, 'article_review.contamination_flag_counts', 'Why the contamination section exists.')}}
        </div>
        <div class="panel">
          <h3>Accepted AAPL article groups</h3>
          <p class="section-note">These groups are the default article-review path. They should read like Apple evidence, not a mixed pile of unrelated rows.</p>
          <div class="date-grid">
            ${{acceptedDateGroups.length ? acceptedDateGroups.map(renderArticleReviewGroup).join('') : '<p class="muted">No accepted article groups were found for this run.</p>'}}
          </div>
        </div>
        <div class="panel">
          <h3>Contamination / no-ticker-evidence articles</h3>
          <p class="section-note">These groups are separated on purpose so reviewers can see what should not be treated as clean Apple evidence.</p>
          <div class="date-grid">
            ${{contaminationDateGroups.length ? contaminationDateGroups.map(renderArticleReviewGroup).join('') : '<p class="muted">No contamination articles were found for this run.</p>'}}
          </div>
        </div>`;
    }}

    function renderFinbertSentenceReview(payload) {{
      const review = payload.finbert_sentence_review || {{}};
      const articles = Array.isArray(review.articles) ? review.articles : [];
      const missingTextWarnings = Array.isArray(review.missing_text_warnings) ? review.missing_text_warnings : [];
      const sourceArtifactGaps = Array.isArray(review.source_artifact_gaps) ? review.source_artifact_gaps : [];
      const rowCount = Number(review.row_count || 0);
      const labelCounts = sentimentLabelCounts(review.sentiment_label_counts);
      finbertReviewEl.innerHTML = `
        <div class="hero-grid">
          ${{metricCard('Articles', articles.length, 'finbert_sentence_review.article_count', 'How many article-level FinBERT review cards are available.')}}
          ${{metricCard('Sentence rows', rowCount, 'finbert_sentence_review.row_count', 'How many sentence/chunk rows were reviewed.')}}
          ${{metricCard('Sentence sentiment labels', labelCounts, 'finbert_sentence_review.sentiment_label_counts', 'Positive/negative/neutral labels derived from FinBERT probabilities.')}}
          ${{metricCard('Missing text rows', missingTextWarnings.length, 'finbert_sentence_review.missing_text_warning_count', 'How many scored rows are missing the full scored sentence text.')}}
        </div>
        ${{sourceArtifactGaps.length ? `<div class="chart-blocker"><h3>Source artifact gap</h3><p>The scored-news artifact is incomplete for at least one row, so the dashboard is telling you that instead of inventing missing text.</p><ul>${{sourceArtifactGaps.map((gap) => `<li>${{escapeHtml(gap.date || 'n/a')}} · ${{escapeHtml(gap.article_id || 'n/a')}} · sentence ${{escapeHtml(gap.sentence_index ?? 'n/a')}} / chunk ${{escapeHtml(gap.chunk_index ?? 'n/a')}}${{gap.source_text_field ? ` · source field: ${{escapeHtml(gap.source_text_field)}}` : ''}}${{gap.source_text_order !== undefined && gap.source_text_order !== null ? ` · source order: ${{escapeHtml(gap.source_text_order)}}` : ''}}</li>`).join('')}}</ul></div>` : ''}}
        <div class="row-list">
          ${{articles.length ? articles.map(renderFinbertArticle).join('') : '<p class="muted">No FinBERT sentence rows were found for this run.</p>'}}
        </div>`;
    }}

    function evidenceStatusClass(status) {{
      if (status === 'accepted') return 'good';
      if (status === 'rejected' || status === 'missing_or_default') return 'bad';
      return 'warn';
    }}

    function renderTopicRelevanceArticle(row) {{
      const flags = Array.isArray(row.missing_evidence_flags) ? row.missing_evidence_flags : [];
      const reasonCodes = Array.isArray(row.reason_codes) ? row.reason_codes : [];
      const tickerEvidence = row.ticker_evidence || {{}};
      const entityEvidence = row.entity_evidence || {{}};
      const embeddingEvidence = Array.isArray(row.embedding_evidence) ? row.embedding_evidence : [];
      const topicEvidence = Array.isArray(row.topic_evidence) ? row.topic_evidence : [];
      const relevanceRows = Array.isArray(row.relevance_gate_rows) ? row.relevance_gate_rows : [];
      const preprocessingRows = Array.isArray(row.preprocessing_rows) ? row.preprocessing_rows : [];
      const tickerHits = Array.isArray(tickerEvidence.requested_ticker_term_hits) ? tickerEvidence.requested_ticker_term_hits : [];
      const sourceTickers = Array.isArray(tickerEvidence.source_tickers) ? tickerEvidence.source_tickers : [];
      const tickerMentions = Array.isArray(tickerEvidence.preprocessing_ticker_mentions) ? tickerEvidence.preprocessing_ticker_mentions : [];
      const entityMentions = Array.isArray(entityEvidence.preprocessing_entity_mentions) ? entityEvidence.preprocessing_entity_mentions : [];
      const gateEntities = Array.isArray(entityEvidence.relevance_gate_entity_mentions) ? entityEvidence.relevance_gate_entity_mentions : [];
      const status = row.evidence_status || 'missing_or_default';
      const statusClassName = evidenceStatusClass(status);
      return `
        <details class="article">
          <summary>
            <span class="article-title">${{escapeHtml(row.headline || row.article_id || 'Article')}}</span>
            <span class="badge ${{statusClassName}}" title="Raw field: evidence_status">${{escapeHtml(status)}}</span>
            <span class="badge" title="Raw field: relevance_decision">gate: ${{escapeHtml(row.relevance_decision || 'missing')}}</span>
            <span class="badge" title="Raw field: relevance_score">score: ${{formatNumber(row.relevance_score, 2)}}</span>
            <span class="badge" title="Raw field: article_id">article_id: ${{escapeHtml(row.article_id || 'n/a')}}</span>
          </summary>
          <div class="body">
            <p class="article-copy">What am I looking at? The evidence trail that decides whether this story belongs to the selected ticker before FinBERT sentiment should count. Why does it matter? Ticker/entity evidence, embeddings, topics, and relevance-gate sub-scores should agree before the row is trusted. What would make this good or bad? Good: direct ticker or entity support, embedding and topic rows, an accepted gate decision, and coherent reason codes. Bad: missing evidence, rejected gate rows, or a default score shown without support.</p>
            ${{flags.length ? `<details class="row-item"><summary>Evidence flags <span class="badge ${{statusClassName}}">${{flags.length}}</span></summary><div class="body"><p class="article-copy">${{escapeHtml(row.relevance_score_interpretation || 'missing/default evidence')}}</p><ul>${{flags.map((flag) => `<li>${{escapeHtml(flag)}}</li>`).join('')}}</ul></div></details>` : ''}}
            <div class="compact-grid">
              <div class="compact"><div class="k">Evidence status</div><div class="v">${{escapeHtml(status)}}</div><div class="k">raw: evidence_status</div></div>
              <div class="compact"><div class="k">Relevance score</div><div class="v">${{formatNumber(row.relevance_score, 3)}}</div><div class="k">raw: relevance_score</div></div>
              <div class="compact"><div class="k">Score interpretation</div><div class="v">${{escapeHtml(row.relevance_score_interpretation || 'n/a')}}</div><div class="k">raw: relevance_score_interpretation</div></div>
              <div class="compact"><div class="k">Ticker / financial / topic scores</div><div class="v">${{formatNumber(row.ticker_relevance_score, 2)}} / ${{formatNumber(row.financial_relevance_score, 2)}} / ${{formatNumber(row.topic_relevance_score, 2)}}</div><div class="k">raw: ticker_relevance_score / financial_relevance_score / topic_relevance_score</div></div>
              <div class="compact"><div class="k">Reason codes</div><div class="v">${{escapeHtml(reasonCodes.length ? reasonCodes.join(', ') : 'none')}}</div><div class="k">raw: reason_codes</div></div>
              <div class="compact"><div class="k">Missing evidence flags</div><div class="v">${{escapeHtml(flags.length ? flags.join(', ') : 'none')}}</div><div class="k">raw: missing_evidence_flags</div></div>
            </div>
            <div class="compact-grid">
              <div class="compact"><div class="k">Requested ticker hits</div><div class="v">${{escapeHtml(tickerHits.length ? tickerHits.join(', ') : 'none')}}</div><div class="k">raw: ticker_evidence.requested_ticker_term_hits</div></div>
              <div class="compact"><div class="k">Preprocessing ticker mentions</div><div class="v">${{escapeHtml(tickerMentions.length ? tickerMentions.join(', ') : 'none')}}</div><div class="k">raw: preprocessing ticker_mentions</div></div>
              <div class="compact"><div class="k">Source ticker tags</div><div class="v">${{escapeHtml(sourceTickers.length ? sourceTickers.join(', ') : 'none')}}</div><div class="k">raw: ticker_evidence.source_tickers</div></div>
              <div class="compact"><div class="k">Entity mentions</div><div class="v">${{escapeHtml(entityMentions.length ? entityMentions.join(', ') : 'none')}}</div><div class="k">raw: preprocessing entity_mentions</div></div>
              <div class="compact"><div class="k">Gate entities</div><div class="v">${{escapeHtml(gateEntities.length ? gateEntities.join(', ') : 'none')}}</div><div class="k">raw: relevance_gate entity_evidence</div></div>
              <div class="compact"><div class="k">Embedding rows</div><div class="v">${{embeddingEvidence.length}}</div><div class="k">raw: embedding_evidence</div></div>
              <div class="compact"><div class="k">Topic rows</div><div class="v">${{topicEvidence.length}}</div><div class="k">raw: topic_evidence</div></div>
              <div class="compact"><div class="k">Relevance-gate rows</div><div class="v">${{relevanceRows.length}}</div><div class="k">raw: relevance_gate_rows</div></div>
            </div>
            <details class="row-item">
              <summary>Embedding cache and BERTopic metadata</summary>
              <div class="body">
                <div class="row-item"><strong>Embedding evidence</strong><pre>${{escapeHtml(JSON.stringify(embeddingEvidence, null, 2))}}</pre></div>
                <div class="row-item"><strong>Topic evidence</strong><pre>${{escapeHtml(JSON.stringify(topicEvidence, null, 2))}}</pre></div>
              </div>
            </details>
            <details class="row-item">
              <summary>Ticker/entity and relevance-gate raw rows</summary>
              <div class="body">
                <div class="row-item"><strong>Preprocessing rows</strong><pre>${{escapeHtml(JSON.stringify(preprocessingRows, null, 2))}}</pre></div>
                <div class="row-item"><strong>Relevance-gate rows</strong><pre>${{escapeHtml(JSON.stringify(relevanceRows, null, 2))}}</pre></div>
              </div>
            </details>
          </div>
        </details>`;
    }}

    function renderTopicRelevanceReview(payload) {{
      const review = payload.topic_relevance_review || {{}};
      const summary = review.summary || {{}};
      const dateGroups = Array.isArray(review.date_groups) ? review.date_groups : [];
      const blockers = Array.isArray(review.missing_evidence_blockers) ? review.missing_evidence_blockers : [];
      const blockerPreview = blockers.slice(0, 12);
      const reviewable = summary.reviewable === true;
      topicRelevanceReviewEl.innerHTML = `
        <div class="hero-grid">
          ${{metricCard('Articles', summary.article_count ?? 0, 'topic_relevance_review.summary.article_count', 'Article-level topic/relevance evidence rows.')}}
          ${{metricCard('Relevance-gate rows', summary.relevance_gate_row_count ?? 0, 'topic_relevance_review.summary.relevance_gate_row_count', 'Rows from the pre-FinBERT relevance gate. Zero means this tab cannot be accepted yet.')}}
          ${{metricCard('Embeddings / topic rows', `${{summary.embedding_row_count ?? 0}} / ${{summary.topic_label_row_count ?? 0}}`, 'topic_relevance_review.summary.embedding_row_count / topic_label_row_count', 'ML topic evidence available before the missing relevance-gate decision.')}}
          ${{metricCard('Accepted', summary.accepted_count ?? 0, 'topic_relevance_review.summary.accepted_count', 'Rows accepted by the relevance gate with supporting evidence.')}}
          ${{metricCard('Borderline', summary.borderline_count ?? 0, 'topic_relevance_review.summary.borderline_count', 'Rows that need human attention before trust.')}}
          ${{metricCard('Rejected', summary.rejected_count ?? 0, 'topic_relevance_review.summary.rejected_count', 'Rows rejected by the relevance gate.')}}
          ${{metricCard('Missing/default', summary.missing_or_default_count ?? 0, 'topic_relevance_review.summary.missing_or_default_count', 'Rows missing required topic, embedding, or relevance support.')}}
        </div>
        ${{!reviewable ? `<div class="chart-blocker"><h3>Topic / relevance is not reviewable yet</h3><p>${{escapeHtml(summary.review_explanation || 'Required topic/relevance evidence is missing.')}}</p><details><summary>Show sample affected articles (${{blockers.length}} total)</summary><div class="body"><ul>${{blockerPreview.map((item) => `<li>${{escapeHtml(item.date || 'n/a')}} · ${{escapeHtml(item.article_id || 'n/a')}} · ${{escapeHtml((item.missing_evidence_flags || []).join(', '))}}</li>`).join('')}}</ul></div></details></div>` : ''}}
        <div class="date-grid">
          ${{dateGroups.length ? dateGroups.map((group) => `
            <details class="date-card">
              <summary>
                <span>${{escapeHtml(group.date || 'n/a')}}</span>
                <span class="badge">accepted: ${{Number(group.accepted_count || 0)}}</span>
                <span class="badge">borderline: ${{Number(group.borderline_count || 0)}}</span>
                <span class="badge">rejected: ${{Number(group.rejected_count || 0)}}</span>
                <span class="badge">missing/default: ${{Number(group.missing_or_default_count || 0)}}</span>
              </summary>
              <div class="body">
                <div class="row-list">
                  ${{(Array.isArray(group.articles) ? group.articles : []).map(renderTopicRelevanceArticle).join('') || '<p class="muted">No topic/relevance rows were loaded for this date.</p>'}}
                </div>
              </div>
            </details>`).join('') : '<p class="muted">No topic/relevance rows were loaded for this run.</p>'}}
        </div>`;
    }}

    function semanticAggregateWarnings(payload) {{
      return (Array.isArray(payload.warnings) ? payload.warnings : []).filter((warning) => String(warning.scope || '') === 'sentiment_features');
    }}

    function semanticAggregateValue(value) {{
      if (Array.isArray(value)) return value.length ? value.join(', ') : 'none';
      if (value && typeof value === 'object') return JSON.stringify(value);
      return value ?? 'n/a';
    }}

    function renderSemanticAggregateRow(row) {{
      const features = row.features || {{}};
      const contributingArticleIds = Array.isArray(row.contributing_article_ids) ? row.contributing_article_ids : [];
      const relevanceReasonCodes = Array.isArray(row.relevance_reason_codes) ? row.relevance_reason_codes : [];
      const semanticWarningCodes = Array.isArray(row.semantic_warning_codes) ? row.semantic_warning_codes : [];
      const reviewCards = Array.isArray(row.review_value_cards) ? row.review_value_cards : [];
      const rowGranularity = row.row_granularity || 'n/a';
      const rowGranularityLabel = rowGranularity === 'ticker-date' ? 'Repeated context / aggregate value' : rowGranularity;
      return `
        <details class="row-item">
          <summary>
            <span class="article-title">${{escapeHtml(row.date || 'n/a')}} · ${{escapeHtml(row.ticker || 'n/a')}}</span>
            ${{sentimentBadge(row.sentiment_label || 'unknown')}}
            <span class="badge" title="Raw field: row_granularity">${{escapeHtml(rowGranularityLabel)}}</span>
            <span class="badge" title="Raw field: stage">stage: ${{escapeHtml(row.stage || 'n/a')}}</span>
          </summary>
          <div class="body">
            <p class="article-copy"><strong>Human-review digest:</strong> ${{escapeHtml(row.human_review_summary || 'No aggregate review summary available.')}}</p>
            <div class="compact-grid">
              ${{reviewCards.map((card) => `<div class="compact"><div class="k">${{escapeHtml(card.label)}}</div><div class="v">${{escapeHtml(card.value)}}</div><div class="k">raw: ${{escapeHtml(card.field)}}</div></div>`).join('')}}
              <div class="compact"><div class="k">Row granularity</div><div class="v">${{escapeHtml(rowGranularityLabel)}}</div><div class="k">raw: row_granularity</div></div>
              <div class="compact"><div class="k">Stage</div><div class="v">${{escapeHtml(row.stage || 'n/a')}}</div><div class="k">raw: stage</div></div>
              <div class="compact"><div class="k">Artifact key</div><div class="v">${{escapeHtml(row.artifact_key || 'n/a')}}</div><div class="k">raw: artifact_key</div></div>
              <div class="compact"><div class="k">Contributing articles</div><div class="v">${{escapeHtml(contributingArticleIds.length ? contributingArticleIds.join(', ') : 'none')}}</div><div class="k">raw: contributing_article_ids</div></div>
              <div class="compact"><div class="k">Relevance reason codes</div><div class="v">${{escapeHtml(semanticAggregateValue(relevanceReasonCodes))}}</div><div class="k">raw: relevance_reason_codes</div></div>
              <div class="compact"><div class="k">Semantic warning codes</div><div class="v">${{escapeHtml(semanticAggregateValue(semanticWarningCodes))}}</div><div class="k">raw: semantic_warning_codes</div></div>
            </div>
            <details class="row-item">
              <summary>Raw feature payload</summary>
              <div class="body">
                <div class="row-item"><pre>${{escapeHtml(JSON.stringify(features, null, 2))}}</pre></div>
              </div>
            </details>
          </div>
        </details>`;
    }}

    function renderSemanticAggregateReview(payload) {{
      const review = payload.semantic_aggregate_review || {{}};
      const summary = review.summary || {{}};
      const rows = Array.isArray(review.rows) ? review.rows : [];
      const warnings = semanticAggregateWarnings(payload);
      semanticAggregateReviewEl.innerHTML = `
        <div class="hero-grid">
          ${{metricCard('Aggregate rows', summary.row_count ?? rows.length, 'semantic_aggregate_review.summary.row_count', 'Final ticker-date Layer 1 feature rows available in the review payload.')}}
          ${{metricCard('Dates covered', summary.date_count ?? 0, 'semantic_aggregate_review.summary.date_count', 'Dates with ticker-date NLP aggregate rows.')}}
          ${{metricCard('Missing aggregate warnings', warnings.length, 'warnings[scope=sentiment_features]', 'Warnings for missing or incomplete ticker-date aggregate rows.')}}
          ${{metricCard('Row granularity', 'ticker-date', 'row_granularity', 'These rows are repeated ticker-date context, not article evidence.')}}
        </div>
        ${{warnings.length ? `<div class="chart-blocker"><h3>Missing ticker-date semantic aggregate rows</h3><p>The review payload reported missing or incomplete ticker-date aggregate artifacts, so these rows should not be treated as complete Layer 1 output.</p><ul>${{warnings.map((warning) => `<li>${{escapeHtml(warning.message || warning.reason || 'Missing ticker-date aggregate evidence')}}${{warning.key ? ` · key: ${{escapeHtml(warning.key)}}` : ''}}${{warning.fallback_key ? ` · fallback: ${{escapeHtml(warning.fallback_key)}}` : ''}}${{warning.artifact_key ? ` · artifact: ${{escapeHtml(warning.artifact_key)}}` : ''}}${{warning.manifest_key ? ` · manifest: ${{escapeHtml(warning.manifest_key)}}` : ''}}</li>`).join('')}}</ul></div>` : ''}}
        <p class="section-note">What am I looking at? The final Layer 1 ticker-date NLP outputs. Why does it matter? Layer 2 consumes these aggregates, so they need to stay visually separate from article and sentence evidence. What would make this good or bad? Good: every row is clearly labeled as a ticker-date aggregate with its source, stage, artifact, and repeated context values. Bad: rows are missing or the page makes them look like duplicate article evidence.</p>
        <div class="row-list">
          ${{rows.length ? rows.map(renderSemanticAggregateRow).join('') : '<p class="muted">No ticker-date semantic aggregate rows were loaded for this run.</p>'}}
        </div>`;
    }}

    function setActiveTab(targetId) {{
      tabButtons.forEach((button) => {{
        const isActive = button.dataset.tabTarget === targetId;
        button.classList.toggle('active', isActive);
        button.setAttribute('aria-selected', String(isActive));
        const panel = document.getElementById(button.dataset.tabTarget);
        if (panel) panel.classList.toggle('hidden', !isActive);
        if (panel) panel.setAttribute('aria-hidden', String(!isActive));
      }});
    }}

    tabButtons.forEach((button) => {{
      button.addEventListener('click', () => setActiveTab(button.dataset.tabTarget || 'summary-gate-tab'));
    }});

    function renderNlpPipelineSection(sections) {{
      const orderedSections = [
        ['Ticker/entity preprocessing', 'raw_preprocessing_rows'],
        ['Article embeddings', 'article_embedding_rows'],
        ['BERTopic labels', 'topic_label_rows'],
        ['Pre-FinBERT relevance gate', 'relevance_gate_rows'],
        ['Sentence/chunk FinBERT rows', 'finbert_sentence_rows'],
        ['Ticker-date semantic aggregates', 'semantic_aggregate_rows'],
      ];
      nlpPipelineEl.innerHTML = orderedSections.map(([label, key]) => {{
        const rows = Array.isArray(sections?.[key]) ? sections[key] : [];
        const sample = rows[0] || null;
        return `
          <details>
            <summary>${{escapeHtml(label)}} <span class="badge">rows: ${{rows.length}}</span></summary>
            <div class="body">
              <p class="section-note">What am I looking at? A technical sample from the ${{escapeHtml(label.toLowerCase())}} stage. Why does it matter? It helps debug the pipeline when the human-friendly view says something is missing. What would make this good or bad? Good: rows exist and the sample is coherent. Bad: an empty section or a sample that shows unexpected nulls or keys.</p>
              ${{sample ? `<div class="row-item"><pre>${{escapeHtml(JSON.stringify(sample, null, 2))}}</pre></div>` : '<p class="muted">No rows available.</p>'}}
            </div>
          </details>`;
      }}).join('');
    }}

    function renderHmmPipelineSection(sections) {{
      const orderedSections = [
        ['Date-level HMM regime', 'date_level_regime_rows'],
        ['Stock benchmark rows', 'stock_price_rows'],
        ['Date-aligned benchmark/HMM rows', 'date_aligned_price_hmm_rows'],
      ];
      hmmPipelineEl.innerHTML = orderedSections.map(([label, key]) => {{
        const rows = Array.isArray(sections?.[key]) ? sections[key] : [];
        const sample = rows[0] || null;
        return `
          <details>
            <summary>${{escapeHtml(label)}} <span class="badge">rows: ${{rows.length}}</span></summary>
            <div class="body">
              <p class="section-note">What am I looking at? A technical sample from the ${{escapeHtml(label.toLowerCase())}} stage. Why does it matter? It confirms the HMM tab is backed by real date-level regime rows and benchmark price context. What would make this good or bad? Good: rows exist and the sample is coherent. Bad: an empty section or a sample that shows unexpected nulls or keys.</p>
              ${{sample ? `<div class="row-item"><pre>${{escapeHtml(JSON.stringify(sample, null, 2))}}</pre></div>` : '<p class="muted">No rows available.</p>'}}
            </div>
          </details>`;
      }}).join('');
    }}

    function renderDashboard(payload) {{
      const reviewState = deriveReviewState(payload);
      document.body.dataset.smokeStatus = payload.smoke?.status || 'unknown';
      renderTopline(payload, reviewState);
      renderMetrics(payload);
      renderSummaryGateStatus(payload);
      renderChart(payload);
      renderArticleReview(payload);
      renderFinbertSentenceReview(payload);
      renderTopicRelevanceReview(payload);
      renderSemanticAggregateReview(payload);
      renderHmmOverview(payload);
      renderNlpPipelineSection(payload.pipeline_sections || {{}});
      renderHmmPipelineSection(payload.pipeline_sections || {{}});
      setActiveTab('summary-gate-tab');
    }}

    async function loadReview() {{
      const params = new URLSearchParams(window.location.search);
      if (!params.get('run_id')) params.set('run_id', defaults.run_id);
      if (!params.get('from_date')) params.set('from_date', defaults.from_date);
      if (!params.get('to_date')) params.set('to_date', defaults.to_date);
      if (!params.get('ticker')) params.set('ticker', defaults.ticker);
      const response = await fetch(`/api/review?${{params.toString()}}`);
      const payload = await response.json();
      if (!response.ok) {{
        const error = payload.error || 'Failed to load dashboard payload.';
        document.getElementById('state-panel').innerHTML = `<div class="chart-blocker"><strong>Could not load review data.</strong><p>${{escapeHtml(error)}}</p></div>`;
        chartContainerEl.className = 'chart-blocker';
        chartContainerEl.textContent = error;
        return;
      }}
      renderDashboard(payload);
    }}

    loadReview().catch((error) => {{
      document.getElementById('state-panel').innerHTML = `<div class="chart-blocker"><strong>Could not load dashboard.</strong><p>${{escapeHtml(error.message)}}</p></div>`;
    }});
  </script>
</body>
</html>"""
    return template.format(defaults_json=defaults_json)


if __name__ == "__main__":
    raise SystemExit(main())
