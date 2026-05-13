"""Local read-only web UI for the Layer 0/1 feature audit dashboard."""
from __future__ import annotations

import argparse
import json
from collections.abc import Mapping
from dataclasses import dataclass
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

from loguru import logger

from core.features.dashboard_backend import build_layer1_audit_dashboard_report
from core.features.dashboard_ui import build_layer1_audit_dashboard_ui_payload
from services.r2.writer import R2Writer


@dataclass(frozen=True)
class _DashboardDefaults:
    from_date: str
    to_date: str
    tickers: tuple[str, ...]
    host: str
    port: int
    local_root: Path | None = None


@dataclass(frozen=True)
class _DashboardQuery:
    from_date: str
    to_date: str
    tickers: tuple[str, ...]


class _DashboardHTTPServer(ThreadingHTTPServer):
    """HTTP server carrying the dashboard defaults and R2/local storage config."""

    def __init__(self, server_address: tuple[str, int], defaults: _DashboardDefaults) -> None:
        super().__init__(server_address, _DashboardRequestHandler)
        self.defaults = defaults


class _DashboardRequestHandler(BaseHTTPRequestHandler):
    """Serve the Layer 0/1 QA dashboard shell and report JSON."""

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
        if parsed.path == "/api/report":
            self._handle_report_request(parsed.query)
            return
        if parsed.path == "/health":
            self._send_json({"status": "ok"})
            return
        self._send_json(
            {"error": f"Unknown route: {parsed.path}"},
            status=HTTPStatus.NOT_FOUND,
        )

    def log_message(self, format: str, *args: object) -> None:
        """Route stdlib HTTP logs through Loguru."""
        logger.info("feature-audit-dashboard {} - {}", self.address_string(), format % args)

    def _handle_report_request(self, query_text: str) -> None:
        params = parse_qs(query_text, keep_blank_values=False)
        try:
            query = _query_from_params(params=params, defaults=self.server.defaults)
            payload = _build_dashboard_payload(
                from_date=query.from_date,
                to_date=query.to_date,
                tickers=query.tickers,
                local_root=self.server.defaults.local_root,
            )
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
    """Launch the local read-only Layer 0/1 feature audit dashboard server."""
    args = _parse_args(argv)
    defaults = _DashboardDefaults(
        from_date=str(args.from_date),
        to_date=str(args.to_date),
        tickers=_split_tickers(str(args.tickers)),
        host=str(args.host),
        port=int(args.port),
        local_root=args.local_root,
    )
    if not defaults.tickers:
        raise ValueError("--tickers must contain at least one ticker")

    server = _DashboardHTTPServer((defaults.host, defaults.port), defaults)
    logger.info(
        "Layer 0/1 feature audit dashboard listening on http://{}:{}",
        defaults.host,
        defaults.port,
    )
    logger.info(
        "Read-only QA defaults: window={}..{} tickers={}",
        defaults.from_date,
        defaults.to_date,
        ",".join(defaults.tickers),
    )
    if defaults.local_root is not None:
        logger.info("Using local mock R2 root: {}", defaults.local_root)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        logger.info("Shutting down Layer 0/1 feature audit dashboard.")
    finally:
        server.server_close()
    return 0


def _build_dashboard_payload(
    *,
    from_date: str,
    to_date: str,
    tickers: tuple[str, ...],
    local_root: Path | None,
) -> dict[str, object]:
    writer = R2Writer(local_root=local_root) if local_root is not None else R2Writer()
    report = build_layer1_audit_dashboard_report(
        run_id=f"feature-audit-dashboard-ui-{from_date}-to-{to_date}",
        from_date=from_date,
        to_date=to_date,
        tickers=tickers,
        writer=writer,
    )
    return build_layer1_audit_dashboard_ui_payload(report)


def _query_from_params(
    *,
    params: Mapping[str, list[str]],
    defaults: _DashboardDefaults,
) -> _DashboardQuery:
    from_date = _first_param(params, "from_date") or defaults.from_date
    to_date = _first_param(params, "to_date") or defaults.to_date
    ticker_text = _first_param(params, "tickers") or ",".join(defaults.tickers)
    tickers = _split_tickers(ticker_text)
    if not tickers:
        raise ValueError("tickers must contain at least one non-empty ticker")
    return _DashboardQuery(
        from_date=from_date,
        to_date=to_date,
        tickers=tickers,
    )


def _config_payload(defaults: _DashboardDefaults) -> dict[str, object]:
    return {
        "defaults": {
            "from_date": defaults.from_date,
            "to_date": defaults.to_date,
            "tickers": list(defaults.tickers),
            "host": defaults.host,
            "port": defaults.port,
        }
    }


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Serve the read-only Layer 0/1 feature audit dashboard UI."
    )
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
        "--tickers",
        required=True,
        help="Comma-delimited initial ticker subset, for example AAPL,MSFT.",
    )
    parser.add_argument(
        "--host",
        default="127.0.0.1",
        help="Interface to bind the local dashboard server to.",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8765,
        help="TCP port for the local dashboard server.",
    )
    parser.add_argument(
        "--local-root",
        type=Path,
        default=None,
        help=(
            "Optional local mock R2 root. When omitted, the dashboard uses the configured "
            "R2 credentials or the default local mock store."
        ),
    )
    return parser.parse_args(argv)


def _first_param(params: Mapping[str, list[str]], name: str) -> str | None:
    values = params.get(name, [])
    if not values:
        return None
    value = values[0].strip()
    return value or None


def _split_tickers(value: str) -> tuple[str, ...]:
    normalized: list[str] = []
    seen: set[str] = set()
    for item in value.split(","):
        ticker = item.strip().upper()
        if not ticker or ticker in seen:
            continue
        seen.add(ticker)
        normalized.append(ticker)
    return tuple(normalized)


def _render_dashboard_html(defaults: _DashboardDefaults) -> str:
    defaults_json = json.dumps(
        {
            "fromDate": defaults.from_date,
            "toDate": defaults.to_date,
            "tickers": list(defaults.tickers),
        }
    )
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Layer 0/1 Feature Audit Dashboard</title>
  <style>
    :root {{
      --bg: #f6f1e7;
      --panel: rgba(255, 251, 244, 0.88);
      --panel-strong: rgba(255, 248, 238, 0.97);
      --ink: #1d2a27;
      --muted: #556867;
      --line: rgba(36, 62, 56, 0.14);
      --accent: #0f766e;
      --accent-soft: rgba(15, 118, 110, 0.12);
      --pass: #1f8f59;
      --warn: #c67b17;
      --fail: #bc3f2f;
      --pass-soft: rgba(31, 143, 89, 0.14);
      --warn-soft: rgba(198, 123, 23, 0.14);
      --fail-soft: rgba(188, 63, 47, 0.14);
      --shadow: 0 16px 44px rgba(41, 49, 47, 0.08);
      --radius: 22px;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: "IBM Plex Sans", "Avenir Next", "Segoe UI", sans-serif;
      color: var(--ink);
      background:
        radial-gradient(circle at top left, rgba(15, 118, 110, 0.12), transparent 32%),
        radial-gradient(circle at top right, rgba(198, 123, 23, 0.12), transparent 28%),
        linear-gradient(180deg, #fcfaf5 0%, var(--bg) 100%);
      min-height: 100vh;
    }}
    .shell {{
      width: min(1440px, calc(100vw - 32px));
      margin: 0 auto;
      padding: 28px 0 48px;
      animation: reveal 320ms ease-out;
    }}
    @keyframes reveal {{
      from {{ opacity: 0; transform: translateY(12px); }}
      to {{ opacity: 1; transform: translateY(0); }}
    }}
    .hero {{
      background: linear-gradient(135deg, rgba(255, 253, 248, 0.98), rgba(244, 249, 247, 0.95));
      border: 1px solid rgba(28, 59, 54, 0.08);
      border-radius: 28px;
      padding: 28px;
      box-shadow: var(--shadow);
      position: relative;
      overflow: hidden;
    }}
    .hero::after {{
      content: "";
      position: absolute;
      inset: auto -4rem -4rem auto;
      width: 220px;
      height: 220px;
      background: radial-gradient(circle, rgba(15, 118, 110, 0.18), transparent 70%);
      pointer-events: none;
    }}
    .eyebrow {{
      display: inline-flex;
      align-items: center;
      gap: 10px;
      padding: 8px 12px;
      border-radius: 999px;
      background: rgba(15, 118, 110, 0.08);
      color: var(--accent);
      font-size: 0.84rem;
      font-weight: 700;
      letter-spacing: 0.06em;
      text-transform: uppercase;
    }}
    h1 {{
      margin: 18px 0 12px;
      font-family: "Iowan Old Style", "Palatino Linotype", serif;
      font-size: clamp(2rem, 4vw, 3.4rem);
      line-height: 1.02;
      max-width: 12ch;
    }}
    .hero p {{
      margin: 0;
      max-width: 80ch;
      color: var(--muted);
      line-height: 1.65;
    }}
    .notice {{
      margin-top: 18px;
      display: grid;
      gap: 12px;
      grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
    }}
    .status-help, .panel {{
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: var(--radius);
      box-shadow: var(--shadow);
    }}
    .status-help {{
      padding: 18px;
    }}
    .status-help strong {{
      display: block;
      margin-bottom: 8px;
      font-size: 0.95rem;
    }}
    .chip-row, .filters, .summary-grid, .family-grid, .dual-grid {{
      display: grid;
      gap: 16px;
    }}
    .summary-grid {{
      margin-top: 22px;
      grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
    }}
    .stat-card {{
      padding: 18px;
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 20px;
      box-shadow: var(--shadow);
    }}
    .stat-card small {{
      display: block;
      color: var(--muted);
      text-transform: uppercase;
      letter-spacing: 0.06em;
      font-weight: 700;
      margin-bottom: 10px;
    }}
    .stat-card strong {{
      font-size: 1.9rem;
    }}
    .filters {{
      margin: 22px 0;
      grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
    }}
    label {{
      display: grid;
      gap: 8px;
      font-size: 0.92rem;
      font-weight: 600;
      color: var(--muted);
    }}
    input, select {{
      width: 100%;
      border-radius: 14px;
      border: 1px solid rgba(36, 62, 56, 0.14);
      padding: 12px 14px;
      font: inherit;
      color: var(--ink);
      background: rgba(255, 255, 255, 0.9);
    }}
    button {{
      align-self: end;
      border: 0;
      border-radius: 14px;
      padding: 13px 16px;
      font: inherit;
      font-weight: 700;
      color: white;
      background: linear-gradient(135deg, #0f766e, #1f8f59);
      cursor: pointer;
      box-shadow: 0 10px 28px rgba(15, 118, 110, 0.18);
    }}
    .panel {{
      padding: 20px;
      margin-top: 18px;
    }}
    .panel-head {{
      display: flex;
      align-items: flex-start;
      justify-content: space-between;
      gap: 16px;
      margin-bottom: 16px;
    }}
    .panel-head h2 {{
      margin: 0;
      font-size: 1.25rem;
      font-family: "Iowan Old Style", "Palatino Linotype", serif;
    }}
    .panel-head p {{
      margin: 6px 0 0;
      color: var(--muted);
      line-height: 1.55;
    }}
    .family-grid {{
      grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
    }}
    .family-card {{
      padding: 18px;
      border-radius: 18px;
      border: 1px solid var(--line);
      background: rgba(255, 255, 255, 0.66);
    }}
    .badge {{
      display: inline-flex;
      align-items: center;
      gap: 8px;
      border-radius: 999px;
      padding: 7px 12px;
      font-size: 0.82rem;
      font-weight: 800;
      letter-spacing: 0.05em;
      text-transform: uppercase;
    }}
    .status-pass {{ background: var(--pass-soft); color: var(--pass); }}
    .status-warn {{ background: var(--warn-soft); color: var(--warn); }}
    .status-fail {{ background: var(--fail-soft); color: var(--fail); }}
    .key-metrics {{
      display: grid;
      gap: 8px;
      margin-top: 12px;
      color: var(--muted);
      font-size: 0.92rem;
    }}
    .legend {{
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
    }}
    .multi-filter {{
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      margin-top: 12px;
    }}
    .filter-pill {{
      border-radius: 999px;
      border: 1px solid rgba(36, 62, 56, 0.14);
      padding: 8px 12px;
      background: rgba(255, 255, 255, 0.82);
      cursor: pointer;
      font: inherit;
      color: var(--ink);
    }}
    .filter-pill.active {{
      background: var(--accent-soft);
      border-color: rgba(15, 118, 110, 0.28);
      color: var(--accent);
    }}
    .heatmap-wrap {{
      overflow: auto;
      border-radius: 18px;
      border: 1px solid var(--line);
      background: rgba(255, 255, 255, 0.7);
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
    }}
    th, td {{
      padding: 10px 12px;
      border-bottom: 1px solid rgba(36, 62, 56, 0.08);
      text-align: left;
      vertical-align: top;
      font-size: 0.92rem;
    }}
    th {{
      position: sticky;
      top: 0;
      z-index: 2;
      background: rgba(252, 249, 244, 0.94);
      backdrop-filter: blur(12px);
    }}
    .heat-cell {{
      min-width: 54px;
      text-align: center;
      font-weight: 700;
      border-radius: 12px;
    }}
    .heat-cell.status-pass {{ background: var(--pass-soft); }}
    .heat-cell.status-warn {{ background: var(--warn-soft); }}
    .heat-cell.status-fail {{ background: var(--fail-soft); }}
    .rate-bars {{
      display: grid;
      gap: 12px;
    }}
    .rate-row {{
      display: grid;
      gap: 8px;
    }}
    .rate-meta {{
      display: flex;
      justify-content: space-between;
      gap: 12px;
      font-size: 0.92rem;
    }}
    .stack {{
      width: 100%;
      height: 16px;
      border-radius: 999px;
      overflow: hidden;
      background: rgba(36, 62, 56, 0.08);
      display: flex;
    }}
    .stack span {{ height: 100%; }}
    .stack .missing {{ background: #c67b17; }}
    .stack .null {{ background: #d2b48c; }}
    .stack .invalid {{ background: #bc3f2f; }}
    .dual-grid {{
      grid-template-columns: repeat(auto-fit, minmax(320px, 1fr));
    }}
    .chart-card {{
      border: 1px solid var(--line);
      border-radius: 18px;
      padding: 16px;
      background: rgba(255, 255, 255, 0.72);
    }}
    .chart-frame {{
      width: 100%;
      min-height: 280px;
      border-radius: 16px;
      background: linear-gradient(180deg, rgba(240, 247, 245, 0.9), rgba(255, 255, 255, 0.6));
      border: 1px solid rgba(36, 62, 56, 0.08);
      padding: 10px;
    }}
    svg {{
      width: 100%;
      height: 260px;
      display: block;
    }}
    .cards {{
      display: grid;
      gap: 14px;
      grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
    }}
    .formula-card {{
      padding: 18px;
      border-radius: 18px;
      border: 1px solid var(--line);
      background: rgba(255, 255, 255, 0.76);
    }}
    .formula-card pre {{
      margin: 12px 0 0;
      white-space: pre-wrap;
      font-family: "IBM Plex Mono", "SFMono-Regular", monospace;
      font-size: 0.85rem;
      line-height: 1.5;
      color: #304542;
    }}
    .mono {{
      font-family: "IBM Plex Mono", "SFMono-Regular", monospace;
      font-size: 0.85rem;
    }}
    .error-box {{
      margin-top: 18px;
      padding: 16px;
      border-radius: 16px;
      border: 1px solid rgba(188, 63, 47, 0.2);
      background: rgba(188, 63, 47, 0.08);
      color: var(--fail);
      display: none;
    }}
    .loading {{
      color: var(--muted);
      font-size: 0.92rem;
    }}
    @media (max-width: 760px) {{
      .shell {{ width: min(100vw - 20px, 100%); padding-top: 16px; }}
      .hero {{ padding: 22px; }}
      th, td {{ padding: 8px 10px; }}
    }}
  </style>
</head>
<body>
  <div class="shell">
    <section class="hero">
      <span class="eyebrow">Layer 0/1 QA Only</span>
      <h1>Live Feature Audit Dashboard</h1>
      <p id="readOnlyNotice"></p>
      <div class="notice" id="statusHelp"></div>
    </section>

    <section class="summary-grid" id="summaryGrid"></section>

    <section class="panel">
      <div class="panel-head">
        <div>
          <h2>Selection Controls</h2>
          <p>Change the audit window and ticker subset, then refine the UI locally by family, feature, and focus date.</p>
        </div>
        <div class="loading" id="loadingState">Waiting for first load.</div>
      </div>
      <form class="filters" id="queryForm">
        <label>From date
          <input type="date" id="fromDate" required>
        </label>
        <label>To date
          <input type="date" id="toDate" required>
        </label>
        <label>Ticker subset
          <input type="text" id="tickerInput" placeholder="AAPL,MSFT,SPY" required>
        </label>
        <label>Feature filter
          <input type="search" id="featureSearch" placeholder="returns, regime, nlp">
        </label>
        <label>Focus date
          <select id="focusDate"></select>
        </label>
        <button type="submit">Refresh Dashboard</button>
      </form>
      <div id="familyFilters" class="multi-filter"></div>
      <div class="error-box" id="errorBox"></div>
    </section>

    <section class="panel">
      <div class="panel-head">
        <div>
          <h2>Feature-Family Status</h2>
          <p>PASS/WARN/FAIL cards summarize completeness, null-rate, invalid-rate, and outlier pressure for each Layer 1 feature family.</p>
        </div>
        <div class="legend">
          <span class="badge status-pass">PASS</span>
          <span class="badge status-warn">WARN</span>
          <span class="badge status-fail">FAIL</span>
        </div>
      </div>
      <div class="family-grid" id="familyGrid"></div>
    </section>

    <section class="panel">
      <div class="panel-head">
        <div>
          <h2>Completeness Heatmap</h2>
          <p>Each cell is one stored Layer 1 feature value for a selected (date, ticker) row. PASS means present and valid, WARN means optional or skipped, FAIL means missing required or invalid.</p>
        </div>
      </div>
      <div class="heatmap-wrap" id="heatmapWrap"></div>
    </section>

    <section class="panel">
      <div class="panel-head">
        <div>
          <h2>Null-Rate Bars</h2>
          <p>Bars are stacked as missing, null, and invalid shares. Use this to see which features or families are degrading even before they become hard FAILs.</p>
        </div>
      </div>
      <div class="dual-grid">
        <div class="chart-card">
          <h3>By Feature</h3>
          <div class="rate-bars" id="featureRates"></div>
        </div>
        <div class="chart-card">
          <h3>By Family</h3>
          <div class="rate-bars" id="familyRates"></div>
        </div>
      </div>
    </section>

    <section class="panel">
      <div class="panel-head">
        <div>
          <h2>Raw vs Computed Spot Checks</h2>
          <p>For deterministic market features, the dashboard plots stored Layer 1 values against point-in-time-safe recomputations from raw Layer 0 OHLCV.</p>
        </div>
      </div>
      <div class="filters" style="margin-top:0;">
        <label>Spot-check feature
          <select id="spotFeature"></select>
        </label>
        <label>Spot-check ticker
          <select id="spotTicker"></select>
        </label>
      </div>
      <div class="chart-card">
        <div class="chart-frame" id="spotChart"></div>
      </div>
    </section>

    <section class="panel">
      <div class="panel-head">
        <div>
          <h2>Formula Audit Cards</h2>
          <p>Cards show raw inputs, substituted calculations, the stored Layer 1 value, and the final PASS/WARN/FAIL decision for the selected window.</p>
        </div>
      </div>
      <div class="cards" id="formulaCards"></div>
    </section>

    <section class="panel">
      <div class="panel-head">
        <div>
          <h2>Outlier Scatter and Table</h2>
          <p>Scatter points mark invalid-range and extreme-value flags. Use the table to inspect dates, tickers, and bounds for each flagged record.</p>
        </div>
      </div>
      <div class="filters" style="margin-top:0;">
        <label>Outlier feature
          <select id="outlierFeature"></select>
        </label>
      </div>
      <div class="chart-card">
        <div class="chart-frame" id="outlierChart"></div>
      </div>
      <div class="heatmap-wrap" id="outlierTable" style="margin-top:16px;"></div>
    </section>
  </div>

  <script>
    const defaults = {defaults_json};
    const state = {{
      payload: null,
      activeFamilies: new Set(),
    }};

    const statusClass = (value) => `status-${{value || "warn"}}`;
    const upper = (value) => String(value || "").toUpperCase();
    const escapeHtml = (value) => String(value ?? "")
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;")
      .replaceAll('"', "&quot;")
      .replaceAll("'", "&#39;");
    const escapeAttr = (value) => escapeHtml(value);
    const toFiniteNumber = (value) => {{
      if (value === null || value === undefined || value === "") {{
        return null;
      }}
      const numeric = Number(value);
      return Number.isFinite(numeric) ? numeric : null;
    }};
    const formatNumber = (value) => {{
      const numeric = toFiniteNumber(value);
      if (numeric === null) {{
        return "n/a";
      }}
      return numeric.toFixed(6);
    }};
    const clampPercent = (value) => Math.max(0, Math.min(100, Number(value || 0) * 100));
    const optionMarkup = (value, label) =>
      `<option value="${{escapeAttr(value)}}">${{escapeHtml(label)}}</option>`;

    function showError(message) {{
      const box = document.getElementById("errorBox");
      box.textContent = message;
      box.style.display = "block";
    }}

    function clearError() {{
      const box = document.getElementById("errorBox");
      box.textContent = "";
      box.style.display = "none";
    }}

    function setLoading(message) {{
      document.getElementById("loadingState").textContent = message;
    }}

    async function loadPayload() {{
      clearError();
      setLoading("Loading audit payload...");
      const params = new URLSearchParams({{
        from_date: document.getElementById("fromDate").value,
        to_date: document.getElementById("toDate").value,
        tickers: document.getElementById("tickerInput").value,
      }});
      try {{
        const response = await fetch(`/api/report?${{params.toString()}}`);
        const data = await response.json();
        if (!response.ok) {{
          throw new Error(data.error || `HTTP ${{response.status}}`);
        }}
        state.payload = data;
        hydrateFilters(data);
        renderAll();
        setLoading(`Loaded ${{data.report.rows_loaded}} rows for ${{data.report.from_date}} to ${{data.report.to_date}}.`);
      }} catch (error) {{
        showError(error.message);
        setLoading("Load failed.");
      }}
    }}

    function hydrateFilters(payload) {{
      const controls = payload.controls;
      const familyHost = document.getElementById("familyFilters");
      if (!state.activeFamilies.size) {{
        for (const family of controls.available_families) {{
          state.activeFamilies.add(family.family);
        }}
      }}
      familyHost.innerHTML = "";
      for (const family of controls.available_families) {{
        const button = document.createElement("button");
        button.type = "button";
        button.className = `filter-pill ${{state.activeFamilies.has(family.family) ? "active" : ""}}`;
        button.textContent = family.family_label;
        button.onclick = () => {{
          if (state.activeFamilies.has(family.family)) {{
            state.activeFamilies.delete(family.family);
          }} else {{
            state.activeFamilies.add(family.family);
          }}
          hydrateFilters(payload);
          renderAll();
        }};
        familyHost.appendChild(button);
      }}

      const focusDate = document.getElementById("focusDate");
      focusDate.innerHTML = `<option value="">All dates in window</option>` + controls.available_dates
        .map((date) => optionMarkup(date, date))
        .join("");
      if (!focusDate.value || !controls.available_dates.includes(focusDate.value)) {{
        focusDate.value = controls.default_focus_date || "";
      }}

      const spotFeature = document.getElementById("spotFeature");
      spotFeature.innerHTML = controls.available_spot_check_features
        .map((feature) => optionMarkup(feature, feature))
        .join("");
      if (!spotFeature.value || !controls.available_spot_check_features.includes(spotFeature.value)) {{
        spotFeature.value = controls.default_spot_check_feature || controls.available_spot_check_features[0] || "";
      }}

      const tickers = controls.available_tickers;
      const spotTicker = document.getElementById("spotTicker");
      spotTicker.innerHTML = `<option value="">All selected tickers</option>` + tickers
        .map((ticker) => optionMarkup(ticker, ticker))
        .join("");

      const outlierFeature = document.getElementById("outlierFeature");
      outlierFeature.innerHTML = `<option value="">All outlier features</option>` + controls.available_outlier_features
        .map((feature) => optionMarkup(feature, feature))
        .join("");
      if (!outlierFeature.value || (
          outlierFeature.value && !controls.available_outlier_features.includes(outlierFeature.value)
      )) {{
        outlierFeature.value = controls.default_outlier_feature || "";
      }}
    }}

    function featureSearchText() {{
      return document.getElementById("featureSearch").value.trim().toLowerCase();
    }}

    function selectedFocusDate() {{
      return document.getElementById("focusDate").value;
    }}

    function familyActive(family) {{
      return state.activeFamilies.has(family);
    }}

    function renderAll() {{
      if (!state.payload) {{
        return;
      }}
      renderMeta();
      renderSummary();
      renderFamilyPanels();
      renderHeatmap();
      renderNullRates();
      renderSpotChecks();
      renderFormulaCards();
      renderOutliers();
    }}

    function renderMeta() {{
      const meta = state.payload.meta;
      document.getElementById("readOnlyNotice").textContent = meta.read_only_notice;
      document.getElementById("statusHelp").innerHTML = meta.status_help.map((item) => `
        <div class="status-help">
          <span class="badge ${{statusClass(item.status)}}">${{escapeHtml(item.label)}}</span>
          <strong>${{escapeHtml(item.description)}}</strong>
          <span class="mono">${{escapeHtml(meta.qa_scope)}}</span>
        </div>
      `).join("");
    }}

    function renderSummary() {{
      const report = state.payload.report;
      const summary = report.summary || {{}};
      const warnings = report.load_warnings || [];
      document.getElementById("summaryGrid").innerHTML = `
        <article class="stat-card">
          <small>Run ID</small>
          <strong class="mono">${{escapeHtml(report.run_id)}}</strong>
        </article>
        <article class="stat-card">
          <small>Rows Loaded</small>
          <strong>${{report.rows_loaded}}</strong>
        </article>
        <article class="stat-card">
          <small>Family FAIL</small>
          <strong>${{summary.family_fail_count || 0}}</strong>
        </article>
        <article class="stat-card">
          <small>Spot Check FAIL</small>
          <strong>${{summary.spot_check_fail_count || 0}}</strong>
        </article>
        <article class="stat-card">
          <small>Outliers</small>
          <strong>${{summary.outlier_count || 0}}</strong>
        </article>
        <article class="stat-card">
          <small>Load Warnings</small>
          <strong>${{warnings.length}}</strong>
        </article>
      `;
    }}

    function renderFamilyPanels() {{
      const families = state.payload.family_panels.filter((item) => familyActive(item.family));
      document.getElementById("familyGrid").innerHTML = families.map((item) => `
        <article class="family-card">
          <span class="badge ${{statusClass(item.status)}}">${{upper(item.status)}}</span>
          <h3>${{escapeHtml(item.family_label)}}</h3>
          <div class="key-metrics">
            <span>Features: <strong>${{item.feature_count}}</strong></span>
            <span>Missing rate: <strong>${{(item.missing_rate * 100).toFixed(1)}}%</strong></span>
            <span>Null rate: <strong>${{(item.null_rate * 100).toFixed(1)}}%</strong></span>
            <span>Invalid rate: <strong>${{(item.invalid_rate * 100).toFixed(1)}}%</strong></span>
            <span>Outliers: <strong>${{item.outlier_count}}</strong></span>
          </div>
        </article>
      `).join("");
    }}

    function renderHeatmap() {{
      const search = featureSearchText();
      const rows = state.payload.heatmap.rows.filter((item) => {{
        if (!familyActive(item.family)) {{
          return false;
        }}
        if (!search) {{
          return true;
        }}
        return item.feature_name.toLowerCase().includes(search) ||
          item.family_label.toLowerCase().includes(search);
      }});
      const columns = state.payload.heatmap.columns;
      const head = `
        <thead>
          <tr>
            <th>Feature</th>
            <th>Family</th>
            <th>Status</th>
            ${{columns.map((column) => `<th class="mono">${{escapeHtml(column.date)}}<br>${{escapeHtml(column.ticker)}}</th>`).join("")}}
          </tr>
        </thead>
      `;
      const body = `
        <tbody>
          ${{rows.map((row) => `
            <tr>
              <td class="mono">${{escapeHtml(row.feature_name)}}</td>
              <td>${{escapeHtml(row.family_label)}}</td>
              <td><span class="badge ${{statusClass(row.status)}}">${{upper(row.status)}}</span></td>
              ${{row.cells.map((cell) => `
                <td class="heat-cell ${{statusClass(cell.status)}}" title="${{escapeAttr(cell.message || "")}} | ${{escapeAttr(cell.value_label)}}">
                  ${{upper(cell.status).charAt(0)}}
                </td>
              `).join("")}}
            </tr>
          `).join("")}}
        </tbody>
      `;
      document.getElementById("heatmapWrap").innerHTML = `<table>${{head}}${{body}}</table>`;
    }}

    function rateMarkup(rows, labelKey) {{
      return rows.map((row) => `
        <div class="rate-row">
          <div class="rate-meta">
            <strong class="mono">${{escapeHtml(row[labelKey])}}</strong>
            <span><span class="badge ${{statusClass(row.status)}}">${{upper(row.status)}}</span></span>
          </div>
          <div class="stack" title="${{escapeAttr(`Missing ${{(row.missing_rate * 100).toFixed(1)}}%, Null ${{(row.null_rate * 100).toFixed(1)}}%, Invalid ${{(row.invalid_rate * 100).toFixed(1)}}%`)}}">
            <span class="missing" style="width:${{clampPercent(row.missing_rate)}}%"></span>
            <span class="null" style="width:${{clampPercent(row.null_rate)}}%"></span>
            <span class="invalid" style="width:${{clampPercent(row.invalid_rate)}}%"></span>
          </div>
        </div>
      `).join("");
    }}

    function linePath(points, valueKey, x, y) {{
      return points
        .map((point, index) => {{
          const numeric = toFiniteNumber(point[valueKey]);
          return numeric === null ? null : `${{x(index)}} ${{y(numeric)}}`;
        }})
        .filter((segment) => segment !== null)
        .map((segment, index) => `${{index === 0 ? "M" : "L"}} ${{segment}}`)
        .join(" ");
    }}

    function renderNullRates() {{
      const features = state.payload.null_rates.by_feature.filter((item) => familyActive(item.family));
      const families = state.payload.null_rates.by_family.filter((item) => familyActive(item.family));
      document.getElementById("featureRates").innerHTML = rateMarkup(features.slice(0, 18), "feature_name");
      document.getElementById("familyRates").innerHTML = rateMarkup(families, "family_label");
    }}

    function lineChartMarkup(points) {{
      if (!points.length) {{
        return `<div class="loading">No spot-check rows for the selected filters.</div>`;
      }}
      const values = points
        .flatMap((point) => [toFiniteNumber(point.stored_value), toFiniteNumber(point.expected_value)])
        .filter((value) => value !== null);
      if (!values.length) {{
        return `<div class="loading">Selected rows contain WARN-only spot checks without numeric stored/expected values.</div>`;
      }}
      const min = Math.min(...values);
      const max = Math.max(...values);
      const spread = Math.max(max - min, 1e-9);
      const x = (index) => 40 + (index / Math.max(points.length - 1, 1)) * 720;
      const y = (value) => 230 - ((value - min) / spread) * 180;
      const storedPath = linePath(points, "stored_value", x, y);
      const expectedPath = linePath(points, "expected_value", x, y);
      const dots = points.map((point, index) => `
        <g>
          ${{toFiniteNumber(point.stored_value) !== null ? `<circle cx="${{x(index)}}" cy="${{y(toFiniteNumber(point.stored_value))}}" r="5" fill="#bc3f2f"><title>${{escapeHtml(point.date)}} stored=${{formatNumber(point.stored_value)}}</title></circle>` : ""}}
          ${{toFiniteNumber(point.expected_value) !== null ? `<circle cx="${{x(index)}}" cy="${{y(toFiniteNumber(point.expected_value))}}" r="5" fill="#0f766e"><title>${{escapeHtml(point.date)}} expected=${{formatNumber(point.expected_value)}}</title></circle>` : ""}}
        </g>
      `).join("");
      const labels = points.map((point, index) => `
        <text x="${{x(index)}}" y="252" text-anchor="middle" font-size="11" fill="#556867">${{escapeHtml(point.date.slice(5))}}</text>
      `).join("");
      return `
        <svg viewBox="0 0 800 260" role="img" aria-label="Stored versus recomputed feature values">
          <line x1="40" y1="230" x2="760" y2="230" stroke="rgba(36,62,56,0.18)" />
          <line x1="40" y1="36" x2="40" y2="230" stroke="rgba(36,62,56,0.18)" />
          <path d="${{expectedPath}}" fill="none" stroke="#0f766e" stroke-width="3" stroke-linejoin="round" />
          <path d="${{storedPath}}" fill="none" stroke="#bc3f2f" stroke-width="3" stroke-linejoin="round" />
          ${{dots}}
          ${{labels}}
          <text x="40" y="24" font-size="12" fill="#556867">max ${{formatNumber(max)}}</text>
          <text x="40" y="244" font-size="12" fill="#556867">min ${{formatNumber(min)}}</text>
        </svg>
      `;
    }}

    function renderSpotChecks() {{
      const selectedFeature = document.getElementById("spotFeature").value;
      const selectedTicker = document.getElementById("spotTicker").value;
      const focusDate = selectedFocusDate();
      const series = state.payload.spot_checks.series.filter((item) => {{
        if (selectedFeature && item.feature_name !== selectedFeature) {{
          return false;
        }}
        if (selectedTicker && item.ticker !== selectedTicker) {{
          return false;
        }}
        return true;
      }});
      const points = series.flatMap((item) => item.points.map((point) => ({{
        ...point,
        ticker: item.ticker,
        feature_name: item.feature_name,
      }}))).filter((point) => !focusDate || point.date === focusDate);
      document.getElementById("spotChart").innerHTML = lineChartMarkup(points);
    }}

    function renderFormulaCards() {{
      const selectedFeature = document.getElementById("spotFeature").value;
      const selectedTicker = document.getElementById("spotTicker").value;
      const focusDate = selectedFocusDate();
      const cards = state.payload.formula_cards.filter((item) => {{
        if (selectedFeature && item.feature_name !== selectedFeature) {{
          return false;
        }}
        if (selectedTicker && item.ticker !== selectedTicker) {{
          return false;
        }}
        if (focusDate && item.date !== focusDate) {{
          return false;
        }}
        return true;
      }});
      document.getElementById("formulaCards").innerHTML = cards.slice(0, 18).map((item) => `
        <article class="formula-card">
          <div class="panel-head" style="margin-bottom:10px;">
            <div>
              <h3 style="margin:0;">${{escapeHtml(item.title)}}</h3>
              <p style="margin-top:4px;">Stored ${{formatNumber(item.stored_value)}} vs recomputed ${{formatNumber(item.expected_value)}}</p>
            </div>
            <span class="badge ${{statusClass(item.status)}}">${{upper(item.status)}}</span>
          </div>
          <div class="mono">Formula: ${{escapeHtml(item.formula)}}</div>
          <pre>${{escapeHtml(item.calculation)}}</pre>
          <pre>${{escapeHtml(item.point_in_time_note)}}</pre>
          ${{item.message ? `<pre>${{escapeHtml(item.message)}}</pre>` : ""}}
        </article>
      `).join("");
    }}

    function scatterMarkup(points) {{
      if (!points.length) {{
        return `<div class="loading">No outlier records for the selected filters.</div>`;
      }}
      const values = points
        .map((point) => toFiniteNumber(point.value))
        .filter((value) => value !== null);
      if (!values.length) {{
        return `<div class="loading">Selected outlier rows do not contain numeric values.</div>`;
      }}
      const min = Math.min(...values);
      const max = Math.max(...values);
      const spread = Math.max(max - min, 1e-9);
      const x = (index) => 40 + (index / Math.max(points.length - 1, 1)) * 720;
      const y = (value) => 230 - ((value - min) / spread) * 180;
      return `
        <svg viewBox="0 0 800 260" role="img" aria-label="Outlier scatter plot">
          <line x1="40" y1="230" x2="760" y2="230" stroke="rgba(36,62,56,0.18)" />
          <line x1="40" y1="36" x2="40" y2="230" stroke="rgba(36,62,56,0.18)" />
          ${{points.map((point, index) => `
            <g>
              <circle cx="${{x(index)}}" cy="${{y(toFiniteNumber(point.value) ?? min)}}" r="7" class="${{statusClass(point.status)}}" fill="${{point.rule_type === "range_violation" ? "#bc3f2f" : "#c67b17"}}">
                <title>${{escapeHtml(point.date)}} ${{escapeHtml(point.ticker)}} ${{escapeHtml(point.feature_name)}} ${{formatNumber(point.value)}}</title>
              </circle>
              <text x="${{x(index)}}" y="250" text-anchor="middle" font-size="11" fill="#556867">${{escapeHtml(point.date.slice(5))}}</text>
            </g>
          `).join("")}}
          <text x="40" y="24" font-size="12" fill="#556867">max ${{formatNumber(max)}}</text>
          <text x="40" y="244" font-size="12" fill="#556867">min ${{formatNumber(min)}}</text>
        </svg>
      `;
    }}

    function renderOutliers() {{
      const feature = document.getElementById("outlierFeature").value;
      const focusDate = selectedFocusDate();
      const points = state.payload.outliers.points.filter((item) => {{
        if (!familyActive(item.family)) {{
          return false;
        }}
        if (feature && item.feature_name !== feature) {{
          return false;
        }}
        if (focusDate && item.date !== focusDate) {{
          return false;
        }}
        return true;
      }});
      document.getElementById("outlierChart").innerHTML = scatterMarkup(points);
      document.getElementById("outlierTable").innerHTML = `
        <table>
          <thead>
            <tr>
              <th>Date</th>
              <th>Ticker</th>
              <th>Feature</th>
              <th>Flag</th>
              <th>Value</th>
              <th>Bounds</th>
            </tr>
          </thead>
          <tbody>
            ${{points.map((item) => `
              <tr>
                <td class="mono">${{escapeHtml(item.date)}}</td>
                <td class="mono">${{escapeHtml(item.ticker)}}</td>
                <td class="mono">${{escapeHtml(item.feature_name)}}</td>
                <td><span class="badge ${{statusClass(item.status)}}">${{item.rule_type === "range_violation" ? "INVALID RANGE" : "EXTREME VALUE"}}</span></td>
                <td>${{formatNumber(item.value)}}</td>
                <td class="mono">${{formatNumber(item.lower_bound)}} → ${{formatNumber(item.upper_bound)}}</td>
              </tr>
            `).join("")}}
          </tbody>
        </table>
      `;
    }}

    document.getElementById("queryForm").addEventListener("submit", (event) => {{
      event.preventDefault();
      loadPayload();
    }});
    document.getElementById("featureSearch").addEventListener("input", renderAll);
    document.getElementById("focusDate").addEventListener("change", renderAll);
    document.getElementById("spotFeature").addEventListener("change", renderAll);
    document.getElementById("spotTicker").addEventListener("change", renderAll);
    document.getElementById("outlierFeature").addEventListener("change", renderAll);

    document.getElementById("fromDate").value = defaults.fromDate;
    document.getElementById("toDate").value = defaults.toDate;
    document.getElementById("tickerInput").value = defaults.tickers.join(",");
    loadPayload();
  </script>
</body>
</html>"""


if __name__ == "__main__":
    raise SystemExit(main())
