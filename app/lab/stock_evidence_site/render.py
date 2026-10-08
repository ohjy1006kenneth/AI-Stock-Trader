"""Deterministic server-side HTML rendering for the evidence site.

All user/packet data is escaped with ``html.escape`` (``quote=True``)
before insertion into the document. Rendering is a pure function of a
payload dict plus an explicit ``request`` context, so tests can assert
exact substring behavior without a live server.
"""

from __future__ import annotations

import html
from typing import Any

SITE_NAME = "AI-Stock-Trader Evidence Review"
SITE_PURPOSE = (
    "Read-only, standalone reviewer for a versioned AI-Stock-Trader "
    "Layer-1/stock semantic audit packet. It displays exactly the data "
    "delivered by the local fixture or an independently configured "
    "private packet backend; it never fabricates missing fields and "
    "never treats transport success as semantic readiness."
)

# Fields the live API payload currently omits; shown as explicit
# "not delivered" rather than invented text.
NOT_DELIVERED_NOTE = "not delivered by backend"


def esc(value: Any) -> str:
    """Escape a scalar for safe HTML interpolation (quotes included)."""
    if value is None:
        return ""
    return html.escape(str(value), quote=True)


def unknown_or(value: Any, *, empty: str = "unknown") -> str:
    """Render ``None``/empty/whitespace as an explicit unknown state."""
    if value is None:
        return empty
    text = str(value)
    if text.strip() == "":
        return empty
    return esc(text)


def _fmt_num(value: Any) -> str:
    if value is None or isinstance(value, str) and not value.strip():
        return "unknown"
    try:
        number = float(value)
    except (TypeError, ValueError):
        return esc(value)
    if number.is_integer():
        return str(int(number))
    return f"{number:.4f}".rstrip("0").rstrip(".")


def _badge(label: str, kind: str) -> str:
    return (
        f'<span class="badge badge-{esc(kind)}" aria-label="{esc(label)}">'
        f"{esc(label)}</span>"
    )


def _readiness_badges(readiness: Any) -> str:
    """Render readiness/evidence status and warning badges, unchanged."""
    if not isinstance(readiness, dict):
        return '<span class="badge badge-warn">readiness unavailable</span>'
    parts: list[str] = []
    status = readiness.get("human_review_status")
    if status:
        parts.append(_badge(f"review: {status}", "warn"))
    recommendation = readiness.get("recommendation")
    if recommendation:
        parts.append(_badge(f"{recommendation}", "danger"))
    if "ready_for_final_human_acceptance" in readiness:
        reported = readiness["ready_for_final_human_acceptance"]
        reported_text = "unknown" if reported is None or str(reported).strip() == "" else str(reported)
        parts.append(_badge(
            f"producer-reported ready_for_final_human_acceptance: {reported_text}; not acceptance",
            "warn",
        ))
    if readiness.get("hmm_feature_set_warning"):
        parts.append(_badge(f"HMM: {readiness['hmm_feature_set_warning']}", "warn"))
    return "".join(parts) if parts else (
        '<span class="badge badge-warn">no readiness data delivered</span>'
    )


def _hmm_point_count(ticker_audit: dict[str, Any]) -> Any:
    """Count delivered HMM regime/price points without inference."""
    hmm = ticker_audit.get("hmm")
    if isinstance(hmm, dict):
        rows = hmm.get("regime_rows")
        if isinstance(rows, list):
            return len(rows)
        for key in ("benchmark_market_regime_row_count", "benchmark_price_row_count"):
            if isinstance(hmm.get(key), int):
                return hmm[key]
    readiness = (ticker_audit.get("api") or {}).get("run_readiness") or {}
    audit_state = (
        readiness.get("hmm_chart_auditability")
        or readiness.get("hmm_chart_auditability_state")
        or {}
    )
    if isinstance(audit_state, dict) and isinstance(audit_state.get("point_count"), int):
        return audit_state["point_count"]
    return None


def _hmm_badges(ticker_audit: dict[str, Any]) -> str:
    if not isinstance(ticker_audit, dict):
        return ""
    hmm = ticker_audit.get("hmm")
    points = _hmm_point_count(ticker_audit)
    points_disp = "unknown" if points is None else str(points)
    if not isinstance(hmm, dict):
        hmm = {}
    ticker_name = hmm.get("benchmark_ticker") or "unknown benchmark"
    badge = _badge(f"HMM {ticker_name}: {points_disp} point(s)", "warn")
    if points == 1:
        badge += _badge("HMM single-point audit; audit-only, not a trading signal", "warn")
    if isinstance(hmm.get("evaluation_context"), dict):
        ctx = hmm["evaluation_context"]
        if ctx.get("complete_training_rows_sufficient") is False:
            badge += _badge("HMM training rows insufficient", "warn")
    return badge


def _ticker_section(ticker: str, audit: Any, error: Any = None) -> str:
    if error is not None or not isinstance(audit, dict):
        detail = (
            f'<p>Delivered status: {unknown_or(error.get("status"))}; '
            f'delivered message: {unknown_or(error.get("message"))}</p>'
            if isinstance(error, dict) else
            '<p>Unavailable: no ticker audit or error detail delivered.</p>'
        )
        body = (
            '<p class="empty-state">No packet data delivered for this '
            "ticker. This is a per-ticker no-data error, not a zero-row result.</p>"
            + detail
        )
        return (
            f'<section class="ticker-panel" id="ticker-{esc(ticker)}" '
            f'aria-label="Evidence for {esc(ticker)}">'
            f'<h3>{esc(ticker)}</h3>{body}</section>'
        )

    parts: list[str] = []
    parts.append(f'<h3>{esc(ticker)}</h3>')
    parts.append(
        f'<p class="correlation">requested ticker: <code>{esc(ticker)}</code> '
        f'&middot; served ticker: <code>{unknown_or(audit.get("ticker"))}</code></p>'
    )
    # Readiness lives under the API block in the audit packet.
    api_block = audit.get("api")
    readiness = None
    if isinstance(api_block, dict):
        readiness = api_block.get("run_readiness")
    if not isinstance(readiness, dict):
        readiness = audit.get("run_readiness") or audit.get("readiness") or {}
    parts.append(f'<div class="badges">{_readiness_badges(readiness)}</div>')
    hmm_badges = _hmm_badges(audit)
    if hmm_badges:
        parts.append(f'<div class="badges">{hmm_badges}</div>')

    # Headlines / article counts (source-grounded).
    articles = audit.get("articles") or []
    parts.append(f'<h4>Articles ({len(articles)})</h4>')
    if articles:
        rows = []
        for art in articles:
            if not isinstance(art, dict):
                continue
            headline = unknown_or(art.get("headline"), empty="(no headline delivered)")
            source = unknown_or(art.get("source"), empty="unknown")
            score = _fmt_num(art.get("relevance_score"))
            state = unknown_or(art.get("relevance_state"), empty="unknown")
            rows.append(
                f"<li><span class='headline'>{headline}</span> "
                f"<span class='meta'>source: {source} &middot; "
                f"relevance {score} ({state})</span></li>"
            )
        parts.append(f"<ul class='article-list'>{''.join(rows)}</ul>")
    else:
        parts.append(
            '<p class="empty-state">No article rows delivered for this '
            "ticker.</p>"
        )

    # Sentence/chunk text is NOT in the live payload; mark it honestly.
    parts.append(
        f'<p class="missing-text">Sentence/chunk text: '
        f"<em>{NOT_DELIVERED_NOTE}</em> &mdash; not shown; no text is "
        "synthesized.</p>"
    )

    # Semantic aggregates (delivered feature values, unchanged).
    aggregates = audit.get("semantic_aggregates") or []
    if isinstance(aggregates, list) and aggregates:
        parts.append("<h4>Delivered semantic feature values</h4>")
        agg_rows = []
        for agg in aggregates:
            if not isinstance(agg, dict):
                continue
            values = agg.get("nlp_feature_values")
            if isinstance(values, dict):
                items = "".join(
                    f"<dt>{esc(k)}</dt><dd>{unknown_or(v)}</dd>" for k, v in values.items()
                )
                agg_rows.append(f"<dl class='kv'>{items}</dl>")
        if agg_rows:
            parts.append("".join(agg_rows))
        else:
            parts.append(
                '<p class="empty-state">Aggregate rows present but no '
                "feature values delivered.</p>"
            )
    else:
        parts.append(
            '<p class="empty-state">No semantic aggregate rows delivered '
            "for this ticker.</p>"
        )

    # Relevance gate counts.
    gate = audit.get("relevance_gate")
    if isinstance(gate, dict):
        parts.append("<h4>Relevance gate</h4>")
        parts.append(
            f'<dl class="kv">'
            f"<dt>accepted</dt><dd>{_fmt_num(gate.get('accepted_count'))}</dd>"
            f"<dt>borderline</dt><dd>{_fmt_num(gate.get('borderline_count'))}</dd>"
            f"<dt>rejected</dt><dd>{_fmt_num(gate.get('rejected_count'))}</dd>"
            f"<dt>total rows</dt><dd>{_fmt_num(gate.get('total_rows'))}</dd>"
            f"</dl>"
        )

    # Diagnostic warnings, verbatim.
    warnings = (audit.get("summary") or {}).get("warnings") if isinstance(
        audit.get("summary"), dict
    ) else None
    if warnings:
        parts.append("<h4>Delivered diagnostics</h4>")
        parts.append(
            "<ul class='warnings'>"
            + "".join(f"<li>{esc(w)}</li>" for w in warnings)
            + "</ul>"
        )

    return (
        f'<section class="ticker-panel" id="ticker-{esc(ticker)}" '
        f'aria-label="Evidence for {esc(ticker)}">'
        f"{''.join(parts)}</section>"
    )


def render_payload(request: dict[str, Any], payload: Any) -> str:
    """Render a full audit payload (local fixture or live adapter)."""
    if not isinstance(payload, dict):
        raise ValueError("payload must be a dict")

    packet_id = payload.get("packet_id", payload.get("run_id"))
    git_head = payload.get("git_head")
    generated_at = payload.get("generated_at")
    review_date = payload.get("review_date")
    scope = payload.get("scope")
    r2_mode = payload.get("r2_mode")
    audit_note = payload.get("audit")
    date = request.get("date")

    freshness = payload.get("freshness") or {}
    if isinstance(freshness, dict):
        fresh = freshness.get("is_fresh")
        threshold = freshness.get("threshold_seconds")
        if fresh is True:
            fresh_badge = _badge("fresh (classification only; not readiness)", "warn")
        elif fresh is False:
            fresh_badge = _badge(
                f"historical (threshold {threshold if threshold is not None else 'unknown'}s)",
                "warn",
            )
        else:
            fresh_badge = _badge("unknown freshness", "warn")
    else:
        fresh_badge = _badge("unknown freshness", "warn")

    ticker_audits = payload.get("ticker_audits") or {}
    if not isinstance(ticker_audits, dict):
        ticker_audits = {}
    requested = request.get("tickers") or payload.get("tickers") or []
    if isinstance(requested, str):
        requested = [t.strip() for t in requested.split(",") if t.strip()]
    requested = [str(t) for t in requested]

    tab_links: list[str] = []
    sections: list[str] = []
    ticker_errors = payload.get("ticker_errors") or {}
    selected = bool(requested)
    for t in requested:
        present = t in ticker_audits and t not in ticker_errors
        is_sel = selected and t == requested[0]
        tab_class = "tab active" if is_sel else "tab"
        tab_links.append(
            f'<a class="{tab_class}" href="#ticker-{esc(t)}" '
            f'aria-current="{ "page" if is_sel else "false" }">{esc(t)}'
            + ("" if present else " (no data)")
            + "</a>"
        )
        error = ticker_errors.get(t) if t in ticker_errors else None
        if t in ticker_errors and error is None:
            error = {}
        sections.append(_ticker_section(t, ticker_audits.get(t), error))

    # Mismatch / fail-closed banner when served identity != requested.
    mismatch = payload.get("mismatch")
    if mismatch:
        banner = (
            '<div class="banner banner-danger" role="alert">'
            f"<strong>Identity mismatch &mdash; failed closed.</strong> "
            f"<p>{esc(mismatch)}</p></div>"
        )
    else:
        banner = ""

    historical = (
        '<div class="banner banner-warn" role="note">'
        "This packet is <strong>historical</strong>. Values were generated "
        f"{unknown_or(generated_at)}; do not treat them as "
        "current market state.</div>"
    ) if isinstance(freshness, dict) and freshness.get("is_fresh") is False else ""
    source = payload.get("source") or request.get("source")
    source_label = (
        "local historical preview; not current readiness" if source == "local" else
        "backend; no local substitution" if source == "backend" else "unknown"
    )

    audit_note_html = (
        f"<p class='correlation'>audit note: {esc(audit_note)}</p>"
        if isinstance(audit_note, str)
        else ""
    )
    r2_mode_html = f"<dt>r2 mode</dt><dd>{unknown_or(r2_mode)}</dd>"
    title = SITE_NAME
    return (
        "<!doctype html>\n"
        '<html lang="en">\n'
        "<head>\n"
        '<meta charset="utf-8">\n'
        '<meta name="viewport" content="width=device-width, initial-scale=1">\n'
        f"<title>{esc(title)}</title>\n"
        "<style>\n"
        "body{font-family:system-ui,-apple-system,Segoe UI,Roboto,sans-serif;"
        "margin:0;color:#1a1a1a;background:#fafafa;line-height:1.5}\n"
        "header{background:#111;color:#fff;padding:16px 24px}\n"
        "header h1{margin:0;font-size:1.25rem}\n"
        "header p{margin:4px 0 0;color:#bbb;font-size:.9rem}\n"
        ".container{max-width:1100px;margin:0 auto;padding:16px}\n"
        ".banner{padding:10px 14px;border-radius:6px;margin:12px 0}\n"
        ".banner-warn{background:#fff3cd;border:1px solid #ffe08a}\n"
        ".banner-danger{background:#f8d7da;border:1px solid #f1aeb5}\n"
        ".badges{margin:8px 0;display:flex;flex-wrap:wrap;gap:6px}\n"
        ".badge{display:inline-block;padding:2px 8px;border-radius:10px;"
        "font-size:.8rem;border:1px solid transparent}\n"
        ".badge-ok{background:#d4edda;color:#155724;border-color:#c3e6cb}\n"
        ".badge-warn{background:#fff3cd;color:#856404;border-color:#ffe08a}\n"
        ".badge-danger{background:#f8d7da;color:#721c24;border-color:#f1aeb5}\n"
        ".tabs{display:flex;flex-wrap:wrap;gap:8px;margin:12px 0}\n"
        ".tab{padding:8px 14px;border-radius:6px;background:#eee;text-decoration:none;"
        "color:#1a1a1a}\n"
        ".tab.active{background:#0d6efd;color:#fff}\n"
        ".ticker-panel{background:#fff;border:1px solid #ddd;border-radius:8px;"
        "padding:16px;margin:12px 0}\n"
        ".empty-state,.missing-text{color:#888;font-style:italic}\n"
        ".kv{display:grid;grid-template-columns:1fr 1fr;gap:2px 12px;margin:0}\n"
        ".kv dt{color:#555}\n"
        ".kv dd{margin:0;font-weight:600}\n"
        ".correlation{font-size:.85rem;color:#555}\n"
        "code{background:#f1f1f1;padding:1px 4px;border-radius:3px}\n"
        ".article-list li{margin:6px 0}\n"
        ".article-list .meta{color:#666;font-size:.85rem}\n"
        "footer{padding:16px;color:#888;font-size:.8rem;text-align:center}\n"
        "</style>\n"
        "</head>\n"
        "<body>\n"
        f"<header><h1>{esc(title)}</h1><p>{esc(SITE_PURPOSE)}</p></header>\n"
        '<main class="container">\n'
        "<section class='correlation-panel' aria-label='Packet correlation'>"
        "<h2>Packet correlation</h2>"
        "<dl class='kv'>"
        f"<dt>requested packet / run id</dt><dd><code>{unknown_or(request.get('packet_id'))}</code></dd>"
        f"<dt>served packet / run id</dt><dd><code>{unknown_or(packet_id)}</code></dd>"
        f"<dt>source</dt><dd>{esc(source_label)}</dd>"
        f"<dt>review date</dt><dd>{unknown_or(review_date)}</dd>"
        f"<dt>requested date</dt><dd>{unknown_or(date)}</dd>"
        f"<dt>served date</dt><dd>{unknown_or(payload.get('date'))}</dd>"
        f"<dt>scope</dt><dd>{unknown_or(scope)}</dd>"
        f"<dt>git head</dt><dd><code>{unknown_or(git_head)}</code></dd>"
        f"<dt>generated at</dt><dd>{unknown_or(generated_at)}</dd>"
        f"<dt>freshness</dt><dd>{fresh_badge}</dd>"
        f"{r2_mode_html}"
        "</dl></section>\n"
        f"{audit_note_html}"
        f"{historical}\n"
        f"{banner}\n"
        '<p>API status=pass is transport only, not semantic readiness. '
        'Refresh manually with GET/reload; no automatic polling.</p>'
        '<nav class="tabs" aria-label="Ticker selector">'
        f"{''.join(tab_links)}</nav>\n"
        f"{''.join(sections)}\n"
        "</main>\n"
        f"<footer>{esc(title)} &mdash; read-only; no trades, no writes, no "
        "backfills. Rendered from delivered data only.</footer>\n"
        "</body>\n"
        "</html>\n"
    )


def render_error(request: dict[str, Any], error: Any) -> str:
    """Render an explicit, viewable request/service error state."""
    if isinstance(error, dict):
        status = error.get("status", "error")
        message = error.get("message", "unknown error")
        kind = "danger" if "unavailable" in str(status) else "warn"
    else:
        status = "error"
        message = str(error)
        kind = "warn"
    packet_id = request.get("packet_id")
    date = request.get("date")
    tickers = request.get("tickers")
    if isinstance(tickers, str):
        tickers_disp = tickers
    elif isinstance(tickers, list):
        tickers_disp = ", ".join(tickers)
    else:
        tickers_disp = "unknown"
    return (
        "<!doctype html>\n"
        '<html lang="en">\n'
        "<head><meta charset='utf-8'>"
        '<meta name="viewport" content="width=device-width, initial-scale=1">'
        f"<title>{esc(SITE_NAME)} &mdash; error</title></head>\n"
        "<body>\n"
        f"<header><h1>{esc(SITE_NAME)}</h1></header>\n"
        '<main class="container">\n'
        f'<div class="banner banner-{kind}" role="alert">'
        f"<strong>{esc(status)}</strong><p>{esc(message)}</p></div>\n"
        f"<dl class='kv'><dt>requested packet</dt><dd>{esc(packet_id)}</dd>"
        f"<dt>requested date</dt><dd>{esc(date)}</dd>"
        f"<dt>requested tickers</dt><dd>{esc(tickers_disp)}</dd></dl>\n"
        "<p class='missing-text'>No data is substituted. Retry with a "
        "valid request or restore the backend.</p>\n"
        "</main></body></html>\n"
    )
