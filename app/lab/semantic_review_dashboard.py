"""Local read-only web UI for the Layer 1 semantic-review dashboard."""
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

from core.features.aapl_evidence import build_layer1_aapl_evidence_report
from core.features.semantic_review_dashboard import build_layer1_semantic_review_dashboard_payload
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
        self._send_json(
            {"error": f"Unknown route: {parsed.path}"},
            status=HTTPStatus.NOT_FOUND,
        )

    def log_message(self, format: str, *args: object) -> None:
        """Route stdlib HTTP logs through Loguru."""
        logger.info("semantic-review-dashboard {} - {}", self.address_string(), format % args)

    def _handle_review_request(self, query_text: str) -> None:
        params = parse_qs(query_text, keep_blank_values=False)
        try:
            query = _query_from_params(params=params, defaults=self.server.defaults)
            payload = _build_dashboard_payload(
                run_id=query.run_id,
                from_date=query.from_date,
                to_date=query.to_date,
                ticker=query.ticker,
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
    server = _DashboardHTTPServer((defaults.host, defaults.port), defaults)
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
    :root {{ color-scheme: dark; }}
    body {{ font-family: system-ui, sans-serif; margin: 0; background: #0f172a; color: #e2e8f0; }}
    header {{ padding: 20px 24px; border-bottom: 1px solid #334155; background: #111827; }}
    main {{ padding: 20px 24px 32px; }}
    .note {{ color: #cbd5e1; margin: 8px 0 0; line-height: 1.5; }}
    .meta {{ display: flex; flex-wrap: wrap; gap: 12px; margin-top: 16px; }}
    .badge {{ background: #1e293b; border: 1px solid #475569; border-radius: 999px; padding: 6px 10px; }}
    .summary {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(160px, 1fr)); gap: 12px; margin: 18px 0; }}
    .card {{ background: #111827; border: 1px solid #334155; border-radius: 14px; padding: 14px; }}
    .date-group {{ margin-top: 16px; }}
    .date-header {{ display: flex; justify-content: space-between; gap: 12px; flex-wrap: wrap; align-items: center; }}
    .date-header h2 {{ margin: 0; font-size: 1.1rem; }}
    .regime {{ color: #fde68a; }}
    details {{ margin-top: 12px; background: #0b1220; border: 1px solid #334155; border-radius: 12px; padding: 10px 12px; }}
    summary {{ cursor: pointer; font-weight: 600; }}
    .article-meta {{ display: flex; flex-wrap: wrap; gap: 8px; margin: 8px 0 0; }}
    .article-meta .badge {{ background: #172033; }}
    .flag {{ color: #fca5a5; }}
    .accepted {{ color: #86efac; }}
    ul {{ margin: 8px 0 0 20px; }}
    .sentence, .evidence-row {{ border-top: 1px solid #1f2937; margin-top: 8px; padding-top: 8px; }}
    .sentence code {{ white-space: pre-wrap; color: #cbd5e1; }}
    .empty {{ color: #94a3b8; font-style: italic; }}
    .warning {{ color: #fbbf24; }}
    .section-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(260px, 1fr)); gap: 12px; margin: 14px 0; }}
    pre {{ white-space: pre-wrap; word-break: break-word; }}
  </style>
</head>
<body>
  <header>
    <h1>Layer 1 semantic-review dashboard</h1>
    <p class="note">Raw ticker/entity preprocessing, article embeddings, BERTopic labels, relevance-gate decisions, sentence-level FinBERT rows, ticker-date semantic aggregates, and date-level HMM regime evidence are separated by pipeline stage.</p>
    <div class="meta" id="meta"></div>
  </header>
  <main>
    <section class="summary" id="summary"></section>
    <section id="pipeline"></section>
    <section id="content"></section>
  </main>
  <script>
    const defaults = {defaults_json};
    const metaEl = document.getElementById('meta');
    const summaryEl = document.getElementById('summary');
    const pipelineEl = document.getElementById('pipeline');
    const contentEl = document.getElementById('content');

    function badge(label, value) {{
      return `<span class="badge"><strong>${{label}}:</strong> ${{value}}</span>`;
    }}

    function renderSummary(summary) {{
      const entries = [
        ['Rows', summary.row_count ?? 0],
        ['Articles', summary.article_count ?? 0],
        ['Dates', summary.date_count ?? 0],
        ['Accepted', summary.accepted_article_count ?? 0],
        ['Flagged', summary.flagged_article_count ?? 0],
        ['Duplicate articles', summary.duplicate_article_count ?? 0],
        ['Repeated headlines', summary.repeated_headline_count ?? 0],
        ['Weak articles', summary.weak_article_count ?? 0],
        ['Preprocess rows', summary.preprocessing_row_count ?? 0],
        ['Embeddings', summary.embedding_row_count ?? 0],
        ['Topic labels', summary.topic_label_row_count ?? 0],
        ['Relevance rows', summary.relevance_gate_row_count ?? 0],
        ['Aggregates', summary.semantic_aggregate_row_count ?? 0],
      ];
      summaryEl.innerHTML = entries.map(([label, value]) => `<div class="card"><div>${{label}}</div><div style="font-size:1.6rem;font-weight:700">${{value}}</div></div>`).join('');
    }}

    function renderMeta(report) {{
      metaEl.innerHTML = [
        badge('run_id', report.run_id),
        badge('ticker', report.ticker),
        badge('window', `${{report.from_date}} → ${{report.to_date}}`),
      ].join('');
    }}

    function renderSentenceRows(sentenceRows) {{
      if (!sentenceRows.length) {{
        return '<p class="empty">No sentence rows available.</p>';
      }}
      return sentenceRows.map((row) => `
        <div class="sentence">
          <div class="article-meta">
            <span class="badge">sentence_index=${{row.sentence_index ?? 'n/a'}}</span>
            <span class="badge">chunk_index=${{row.chunk_index ?? 'n/a'}}</span>
            <span class="badge">granularity=${{row.row_granularity}}</span>
            <span class="badge">sentiment_score=${{formatNumber(row.sentiment_score)}}</span>
            <span class="badge">relevance_score=${{formatNumber(row.relevance_score)}}</span>
          </div>
          <pre>${{escapeHtml(row.text ?? '')}}</pre>
        </div>`).join('');
    }}

    function renderArticle(article) {{
      const accepted = article.article_status === 'accepted';
      const flags = article.contamination_flags || [];
      return `
        <details open>
          <summary>
            ${{escapeHtml(article.headline ?? article.article_id)}}
            <span class="badge ${{accepted ? 'accepted' : 'flag'}}">${{accepted ? 'accepted' : 'flagged'}}</span>
            <span class="badge">article_id=${{article.article_id}}</span>
            <span class="badge">rows=${{article.article_row_count}}</span>
            <span class="badge">duplicate_headlines=${{article.headline_duplicate_count}}</span>
          </summary>
          <div class="article-meta">
            <span class="badge">published_at=${{article.published_at ?? 'n/a'}}</span>
            <span class="badge">source=${{escapeHtml(article.source ?? 'n/a')}}</span>
            <span class="badge">ticker=${{article.ticker}}</span>
            <span class="badge">relevance_state=${{article.relevance_state}}</span>
            <span class="badge">requested_ticker_terms=${{(article.requested_ticker_terms || []).join(', ') || 'n/a'}}</span>
          </div>
          <p class="note">Normalized headline: ${{escapeHtml(article.normalized_headline || '')}}</p>
          <p class="note">Evidence snippets: ${{(article.evidence_snippets || []).length ? (article.evidence_snippets || []).map(escapeHtml).join(' | ') : 'none'}}</p>
          <p class="note">Ticker evidence: ${{(article.requested_ticker_term_hits || []).length ? (article.requested_ticker_term_hits || []).join(', ') : 'none — this article is kept out of the default acceptance path'}}</p>
          ${{flags.length ? `<p class="warning">Flags: ${{flags.join(', ')}}</p>` : ''}}
          <div class="article-meta">
            <span class="badge">preprocessing_rows=${{(article.preprocessing_rows || []).length}}</span>
            <span class="badge">topic_rows=${{(article.topic_evidence || []).length}}</span>
            <span class="badge">relevance_gate_rows=${{(article.relevance_gate_rows || []).length}}</span>
          </div>
          ${{renderSentenceRows(article.sentence_rows || [])}}
        </details>`;
    }}

    function renderPipelineSections(sections) {{
      const labels = [
        ['raw_preprocessing_rows', 'Ticker/entity preprocessing'],
        ['article_embedding_rows', 'Article embeddings'],
        ['topic_label_rows', 'BERTopic labels'],
        ['relevance_gate_rows', 'Pre-FinBERT relevance gate'],
        ['finbert_sentence_rows', 'Sentence/chunk FinBERT rows'],
        ['semantic_aggregate_rows', 'Ticker-date semantic aggregates'],
        ['date_level_regime_rows', 'Date-level HMM regime'],
      ];
      pipelineEl.innerHTML = `
        <section class="card">
          <h2>Pipeline Evidence</h2>
          <p class="note">Human semantic review remains needs_human_review until these completed NLP pipeline sections are inspected and explicitly accepted.</p>
          <div class="section-grid">
            ${{labels.map(([key, label]) => renderPipelineCard(label, sections[key] || [])).join('')}}
          </div>
        </section>`;
    }}

    function renderPipelineCard(label, rows) {{
      const sample = rows[0] || null;
      return `
        <details>
          <summary>${{label}} <span class="badge">rows=${{rows.length}}</span></summary>
          ${{sample ? `<div class="evidence-row"><pre>${{escapeHtml(JSON.stringify(sample, null, 2))}}</pre></div>` : '<p class="empty">No rows available.</p>'}}
        </details>`;
    }}

    function renderDateGroup(group) {{
      const regime = group.regime || null;
      const regimeText = regime
        ? `${{regime.regime || 'unknown'}} (confidence ${{formatNumber(regime.confidence)}}; bear ${{formatNumber(regime.prob_bear)}}, sideways ${{formatNumber(regime.prob_sideways)}}, bull ${{formatNumber(regime.prob_bull)}})`
        : 'regime unavailable';
      return `
        <section class="date-group card">
          <div class="date-header">
            <h2>${{group.date}}</h2>
            <div class="regime">Date-level HMM regime: ${{regimeText}}</div>
          </div>
          <div class="article-meta">
            <span class="badge">articles=${{group.article_count}}</span>
            <span class="badge">accepted=${{group.accepted_article_count}}</span>
            <span class="badge">flagged=${{group.flagged_article_count}}</span>
            <span class="badge">sentence_rows=${{group.sentence_count}}</span>
          </div>
          ${{(group.articles || []).map(renderArticle).join('') || '<p class="empty">No articles for this date.</p>'}}
        </section>`;
    }}

    function formatNumber(value) {{
      return value === null || value === undefined ? 'n/a' : Number(value).toFixed(4);
    }}

    function escapeHtml(text) {{
      return String(text)
        .replaceAll('&', '&amp;')
        .replaceAll('<', '&lt;')
        .replaceAll('>', '&gt;')
        .replaceAll('"', '&quot;')
        .replaceAll("'", '&#39;');
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
        contentEl.innerHTML = `<p class="warning">${{escapeHtml(payload.error || 'Failed to load dashboard payload.')}}</p>`;
        return;
      }}
      renderMeta(payload.report || payload);
      renderSummary(payload.summary || (payload.report && payload.report.summary) || {{}});
      renderPipelineSections(payload.pipeline_sections || {{}});
      contentEl.innerHTML = (payload.date_groups || []).map(renderDateGroup).join('');
      if (!(payload.date_groups || []).length) {{
        contentEl.innerHTML = '<p class="empty">No review rows were found for this run and date range.</p>';
      }}
    }}

    loadReview().catch((error) => {{
      contentEl.innerHTML = `<p class="warning">${{escapeHtml(error.message)}}</p>`;
    }});
  </script>
</body>
</html>"""
    return template.format(defaults_json=defaults_json)


if __name__ == "__main__":
    raise SystemExit(main())
