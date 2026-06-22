"""Live read-only browser dashboard for Layer 1 semantic review."""
from __future__ import annotations

import argparse
import json
import os
import sys
from collections.abc import Mapping, Sequence
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

from loguru import logger


def _resolve_repo_root() -> Path:
    """Return the repository root for local runs and Modal-mounted runs."""
    env_root = os.getenv("AI_STOCK_TRADER_REPO_ROOT")
    if env_root:
        return Path(env_root).resolve()
    resolved = Path(__file__).resolve()
    return resolved.parents[2] if len(resolved.parents) > 2 else resolved.parent


_REPO_ROOT = _resolve_repo_root()
sys.path.insert(0, str(_REPO_ROOT))

from core.features.semantic_review_dashboard import (  # noqa: E402
    DEFAULT_SEMANTIC_REVIEW_ARTIFACT_DIR,
    SemanticReviewDashboardConfig,
    SemanticReviewFilters,
    build_semantic_review_payload,
)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """Parse command-line arguments for the semantic-review dashboard."""
    parser = argparse.ArgumentParser(
        description="Serve a live read-only Layer 1 semantic-review dashboard."
    )
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--from-date", default=None, metavar="YYYY-MM-DD")
    parser.add_argument("--to-date", default=None, metavar="YYYY-MM-DD")
    parser.add_argument("--ticker", default=None)
    parser.add_argument("--layer0-run-id", default=None)
    parser.add_argument("--layer1-run-id", default=None)
    parser.add_argument(
        "--artifact-dir",
        type=Path,
        default=DEFAULT_SEMANTIC_REVIEW_ARTIFACT_DIR,
        help="Local directory containing evidence JSON/CSV artifacts.",
    )
    parser.add_argument("--evidence-json", type=Path, default=None)
    parser.add_argument("--review-csv", type=Path, default=None)
    parser.add_argument("--accuracy-report", type=Path, default=None)
    parser.add_argument("--local-r2-root", type=Path, default=None)
    parser.add_argument(
        "--no-r2",
        action="store_true",
        help="Disable R2/local object-store fallback and only read local artifact files.",
    )
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    """Start the live dashboard server and block until interrupted."""
    args = parse_args(argv)
    config = SemanticReviewDashboardConfig(
        run_id=str(args.run_id).strip(),
        from_date=str(args.from_date).strip() if args.from_date else None,
        to_date=str(args.to_date).strip() if args.to_date else None,
        ticker=str(args.ticker).strip().upper() if args.ticker else None,
        layer0_run_id=str(args.layer0_run_id).strip() if args.layer0_run_id else None,
        layer1_run_id=str(args.layer1_run_id).strip() if args.layer1_run_id else None,
        artifact_dir=args.artifact_dir,
        evidence_json_path=args.evidence_json,
        review_csv_path=args.review_csv,
        accuracy_report_path=args.accuracy_report,
        use_r2=not bool(args.no_r2),
        local_r2_root=args.local_r2_root,
    )
    server = create_semantic_review_server(args.host, int(args.port), config)
    logger.info("Layer 1 semantic-review dashboard listening on http://{}:{}", args.host, args.port)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        logger.info("Layer 1 semantic-review dashboard stopping")
    finally:
        server.server_close()
    return 0


def create_semantic_review_server(
    host: str,
    port: int,
    config: SemanticReviewDashboardConfig,
) -> ThreadingHTTPServer:
    """Create a configured dashboard HTTP server."""
    handler = create_semantic_review_handler(config)
    return ThreadingHTTPServer((host, port), handler)


def create_semantic_review_handler(
    config: SemanticReviewDashboardConfig,
) -> type[BaseHTTPRequestHandler]:
    """Return a request handler bound to one dashboard configuration."""

    class SemanticReviewRequestHandler(BaseHTTPRequestHandler):
        server_version = "SemanticReviewDashboard/1.0"

        def do_GET(self) -> None:
            """Serve the dashboard HTML or JSON API responses."""
            parsed = urlparse(self.path)
            if parsed.path in {"/", "/index.html"}:
                self._send_html(_DASHBOARD_HTML)
                return
            if parsed.path in {"/health", "/api/health"}:
                self._send_json({"status": "ok", "run_id": config.run_id})
                return
            if parsed.path == "/api/review":
                try:
                    payload = build_semantic_review_payload(
                        config,
                        filters=_filters_from_query(parse_qs(parsed.query)),
                    )
                except ValueError as exc:
                    self._send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
                    return
                self._send_json(payload)
                return
            self._send_json({"error": "not found"}, status=HTTPStatus.NOT_FOUND)

        def log_message(self, format: str, *args: Any) -> None:
            """Route HTTP request logs through the project logger."""
            logger.debug("{} - {}", self.address_string(), format % args)

        def _send_html(self, body: str) -> None:
            encoded = body.encode("utf-8")
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

        def _send_json(
            self,
            payload: Mapping[str, object],
            *,
            status: HTTPStatus = HTTPStatus.OK,
        ) -> None:
            encoded = json.dumps(payload, sort_keys=True).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

    return SemanticReviewRequestHandler


def _filters_from_query(query: Mapping[str, Sequence[str]]) -> SemanticReviewFilters:
    return SemanticReviewFilters(
        date=_first_query_value(query, "date"),
        from_date=_first_query_value(query, "from_date"),
        to_date=_first_query_value(query, "to_date"),
        ticker=_first_query_value(query, "ticker"),
        search=_first_query_value(query, "search"),
        min_relevance=_optional_float(_first_query_value(query, "min_relevance")),
        review_status=_first_query_value(query, "review_status"),
    )


def _first_query_value(query: Mapping[str, Sequence[str]], key: str) -> str | None:
    values = query.get(key)
    if not values:
        return None
    value = str(values[0]).strip()
    return value or None


def _optional_float(value: str | None) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except ValueError as exc:
        raise ValueError(f"Expected numeric min_relevance, got {value}") from exc


_DASHBOARD_HTML = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Layer 1 Semantic Review</title>
  <style>
    :root {
      color-scheme: light;
      --ink: #182026;
      --muted: #5d6974;
      --line: #d9e0e5;
      --panel: #f7f9fa;
      --accent: #116a70;
      --warn: #9b5b00;
      --bad: #a03535;
      --good: #20704d;
      font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    }
    body { margin: 0; color: var(--ink); background: #ffffff; }
    header {
      border-bottom: 1px solid var(--line);
      padding: 18px 24px 14px;
      background: #f5f7f8;
    }
    h1 { margin: 0 0 6px; font-size: 20px; font-weight: 700; letter-spacing: 0; }
    .notice { color: var(--muted); font-size: 13px; max-width: 1100px; }
    main { padding: 18px 24px 32px; }
    .toolbar {
      display: grid;
      grid-template-columns: repeat(6, minmax(120px, 1fr));
      gap: 10px;
      align-items: end;
      margin-bottom: 16px;
    }
    label { display: grid; gap: 4px; color: var(--muted); font-size: 12px; font-weight: 650; }
    input, select, button {
      border: 1px solid var(--line);
      border-radius: 6px;
      padding: 8px 10px;
      font: inherit;
      font-size: 13px;
      background: #fff;
      color: var(--ink);
      min-width: 0;
    }
    button { background: var(--accent); border-color: var(--accent); color: #fff; cursor: pointer; }
    .summary {
      display: grid;
      grid-template-columns: repeat(6, minmax(120px, 1fr));
      gap: 10px;
      margin-bottom: 16px;
    }
    .metric { border: 1px solid var(--line); border-radius: 8px; padding: 10px; background: var(--panel); }
    .metric span { display: block; color: var(--muted); font-size: 11px; text-transform: uppercase; }
    .metric strong { display: block; margin-top: 4px; font-size: 14px; overflow-wrap: anywhere; }
    .status-pass, .status-proceed, .polarity-positive { color: var(--good); }
    .status-fail, .status-do_not_proceed, .polarity-negative { color: var(--bad); }
    .status-needs_human_review, .status-pending, .polarity-neutral { color: var(--warn); }
    .layout { display: grid; grid-template-columns: 280px 1fr; gap: 16px; align-items: start; }
    .panel { border: 1px solid var(--line); border-radius: 8px; background: #fff; }
    .panel h2 { margin: 0; padding: 11px 12px; border-bottom: 1px solid var(--line); font-size: 14px; }
    .panel-body { padding: 12px; }
    .gate { display: grid; grid-template-columns: 22px 1fr; gap: 8px; padding: 7px 0; border-bottom: 1px solid #eef1f3; font-size: 13px; }
    .gate:last-child { border-bottom: 0; }
    .rows { display: grid; gap: 10px; }
    .row {
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 12px;
      display: grid;
      gap: 9px;
      background: #fff;
    }
    .row-head { display: flex; justify-content: space-between; gap: 14px; align-items: start; }
    .headline { font-weight: 700; overflow-wrap: anywhere; }
    .meta { color: var(--muted); font-size: 12px; }
    .grid { display: grid; grid-template-columns: repeat(4, minmax(130px, 1fr)); gap: 8px; }
    .cell { background: var(--panel); border-radius: 6px; padding: 8px; font-size: 12px; }
    .cell span { display: block; color: var(--muted); margin-bottom: 3px; }
    details { font-size: 12px; }
    code { overflow-wrap: anywhere; }
    .empty { color: var(--muted); padding: 16px; border: 1px dashed var(--line); border-radius: 8px; }
    @media (max-width: 960px) {
      .toolbar, .summary, .layout, .grid { grid-template-columns: 1fr; }
      main, header { padding-left: 14px; padding-right: 14px; }
    }
  </style>
</head>
<body>
  <header>
    <h1>Layer 1 Semantic Review</h1>
    <div class="notice" id="notice">Loading read-only evidence...</div>
  </header>
  <main>
    <section class="toolbar">
      <label>Date <select id="date"><option value="">All</option></select></label>
      <label>Ticker <select id="ticker"><option value="">All</option></select></label>
      <label>Search <input id="search" type="search" placeholder="headline, source, note"></label>
      <label>Min relevance <input id="min_relevance" type="number" min="0" max="1" step="0.05"></label>
      <label>Status <select id="review_status"><option value="">All</option></select></label>
      <button id="reload" type="button">Reload</button>
    </section>
    <section class="summary" id="summary"></section>
    <section class="layout">
      <aside class="panel">
        <h2>Gates</h2>
        <div class="panel-body" id="gates"></div>
      </aside>
      <section class="rows" id="rows"></section>
    </section>
  </main>
  <script>
    const controls = ["date", "ticker", "search", "min_relevance", "review_status"];
    const state = { loadedFilters: false };
    for (const id of controls) document.getElementById(id).addEventListener("input", loadReview);
    document.getElementById("reload").addEventListener("click", loadReview);
    async function loadReview() {
      const params = new URLSearchParams();
      for (const id of controls) {
        const value = document.getElementById(id).value;
        if (value) params.set(id, value);
      }
      const response = await fetch(`/api/review?${params.toString()}`, { cache: "no-store" });
      const payload = await response.json();
      render(payload);
    }
    function render(payload) {
      document.getElementById("notice").textContent = payload.readonly_notice || "";
      if (!state.loadedFilters) {
        fillOptions("date", payload.available_filters.dates || []);
        fillOptions("ticker", payload.available_filters.tickers || []);
        fillOptions("review_status", payload.available_filters.review_statuses || []);
        state.loadedFilters = true;
      }
      renderSummary(payload.run || {}, payload.load_status);
      renderGates(payload.gates || []);
      renderRows(payload.rows || []);
    }
    function fillOptions(id, values) {
      const select = document.getElementById(id);
      for (const value of values) {
        const option = document.createElement("option");
        option.value = value;
        option.textContent = value;
        select.appendChild(option);
      }
    }
    function renderSummary(run, loadStatus) {
      const items = [
        ["Run", run.run_id],
        ["Window", `${run.from_date || ""} to ${run.to_date || ""}`],
        ["Ticker", run.ticker],
        ["Machine", run.machine_integrity_status],
        ["Human", run.human_semantic_review_status],
        ["Recommendation", run.recommendation_for_issue_202],
        ["Rows", run.review_row_count],
        ["Load", loadStatus],
      ];
      document.getElementById("summary").innerHTML = items.map(([label, value]) => {
        const statusClass = `status-${String(value || "").toLowerCase()}`;
        return `<div class="metric"><span>${escapeHtml(label)}</span><strong class="${statusClass}">${escapeHtml(value ?? "")}</strong></div>`;
      }).join("");
    }
    function renderGates(gates) {
      const target = document.getElementById("gates");
      if (!gates.length) {
        target.innerHTML = '<div class="empty">No gate artifact loaded.</div>';
        return;
      }
      target.innerHTML = gates.map((gate) => {
        const mark = gate.passed ? "PASS" : "FAIL";
        const klass = gate.passed ? "status-pass" : "status-fail";
        return `<div class="gate"><strong class="${klass}">${mark}</strong><div>${escapeHtml(gate.name)}<br><span class="meta">${escapeHtml(JSON.stringify(gate.details || {}))}</span></div></div>`;
      }).join("");
    }
    function renderRows(rows) {
      const target = document.getElementById("rows");
      if (!rows.length) {
        target.innerHTML = '<div class="empty">No review rows match the current filters.</div>';
        return;
      }
      target.innerHTML = rows.map(rowHtml).join("");
    }
    function rowHtml(row) {
      const polarity = `polarity-${row.finbert_polarity}`;
      const keys = Object.entries(row.source_artifact_keys || {}).map(([key, value]) => `<div><strong>${escapeHtml(key)}</strong>: <code>${escapeHtml(value)}</code></div>`).join("");
      return `<article class="row">
        <div class="row-head">
          <div>
            <div class="headline">${escapeHtml(row.raw_headline || "(missing headline)")}</div>
            <div class="meta">${escapeHtml(row.date)} · ${escapeHtml(row.ticker)} · ${escapeHtml(row.raw_source || "")} · ${escapeHtml(row.raw_published_at || "")}</div>
          </div>
          <div class="${polarity}">${escapeHtml(row.finbert_polarity)} (${fmt(row.finbert_score)})</div>
        </div>
        <div>${escapeHtml(row.raw_snippet || "")}</div>
        <div class="grid">
          <div class="cell"><span>FinBERT</span>+ ${fmt(row.finbert_positive)} / - ${fmt(row.finbert_negative)} / 0 ${fmt(row.finbert_neutral)}<br>relevance ${fmt(row.finbert_relevance)}</div>
          <div class="cell"><span>Topic Features</span>topics ${fmt(row.topic_count)}<br>sentences ${fmt(row.topic_sentence_count)}</div>
          <div class="cell"><span>Sentiment Features</span>score ${fmt(row.sentiment_score)}<br>articles ${fmt(row.sentiment_article_count)}</div>
          <div class="cell"><span>HMM Regime</span>${escapeHtml(row.regime_label || "")} conf ${fmt(row.regime_confidence)}<br>bear ${fmt(row.regime_prob_bear)} sideways ${fmt(row.regime_prob_sideways)} bull ${fmt(row.regime_prob_bull)}</div>
        </div>
        <div class="meta">Duplicate group count: ${escapeHtml(row.duplicate_count)} · Notes: ${escapeHtml(row.notes || "")}</div>
        <details><summary>Source artifact keys</summary>${keys}</details>
      </article>`;
    }
    function fmt(value) {
      return value === null || value === undefined ? "" : Number(value).toFixed(4);
    }
    function escapeHtml(value) {
      return String(value).replace(/[&<>"']/g, (char) => ({
        "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;"
      })[char]);
    }
    loadReview();
  </script>
</body>
</html>
"""


if __name__ == "__main__":
    raise SystemExit(main())
