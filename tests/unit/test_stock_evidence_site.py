"""Unit tests for the standalone evidence review site (Slice S1 scope).

S1: read-only evidence-source adapter against the real backend contract.
The adapter must correlate served identity (run_id, query dates, tickers,
git_head via per-ticker producer_correlation) with the requested identity
and the authoritative packet, fail closed on missing/conflicting identity,
treat a missing ticker as a per-ticker error (not a zero row), and never
fill invented provenance. The local fixture source is a labeled historical
preview, never a silent fallback for a live request.
"""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import httpx
import pytest
from fastapi.testclient import TestClient

from app.lab.stock_evidence_site import app as site_app
from app.lab.stock_evidence_site.render import (
    SITE_NAME,
    render_error,
    render_payload,
)
from app.lab.stock_evidence_site.sources import (
    DEFAULT_PACKET_ID,
    InvalidPayloadError,
    InvalidSourceError,
    LocalFixtureSource,
    PacketError,
    RequestError,
    TransportError,
    UnknownPacketError,
    get_sources,
    validate_request,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

FIXTURE_PATH = (
    "artifacts/reports/diagnostics/"
    "issue281_post_pr306_current_production_audit_2026-09-04.json"
)

# Authoritative local packet metadata for the fixture packet id.
PACKET_RUN_ID = "issue281-post-pr306-current-prod-20260904-v1"
PACKET_REVIEW_DATE = "2026-09-04"
PACKET_GIT_HEAD = "aaa29eca6fe6e55d3529b4f134471eb6fbe70c85"
PACKET_GENERATED_AT = "2026-09-08T06:00:04.958733+00:00"
PACKET_TICKERS = ("AAPL", "AMD", "NVDA", "MSFT")
PACKET_SCOPE = "one trading date, four target tickers; no broad historical backfill"

# Representative sanitized live backend response shape
# (GET /api/stocks/audit?run_id=...&from_date=...&to_date=...&tickers=...).
# Sanitized: real headlines/tickers reduced to placeholders; metadata shape
# preserved exactly as observed in the live response.
LIVE_QUERY = {
    "from_date": PACKET_REVIEW_DATE,
    "to_date": PACKET_REVIEW_DATE,
    "tickers": ["AAPL"],
}


def _producer_corr(run_id: str = PACKET_RUN_ID,
                   git_head: str | None = PACKET_GIT_HEAD) -> dict[str, Any]:
    """Per-ticker producer correlation block as served by the live API."""
    return {
        "run_id": run_id,
        "from_date": PACKET_REVIEW_DATE,
        "to_date": PACKET_REVIEW_DATE,
        "ticker": "AAPL",
    }


def _review(ticker: str, *, run_id: str = PACKET_RUN_ID,
            git_head: str | None = PACKET_GIT_HEAD) -> dict[str, Any]:
    return {
        "ticker": ticker,
        "packet_metadata": {
            "run_id": run_id,
            "git_head": git_head,
            "review_date": PACKET_REVIEW_DATE,
            "packet_tickers": list(PACKET_TICKERS),
            "selected_ticker": ticker,
            "generated_at": PACKET_GENERATED_AT,
            "scope": PACKET_SCOPE,
        },
        "producer_correlation": {**_producer_corr(run_id, git_head), "ticker": ticker},
        "states": {"run_readiness": {"state": "pass"}},
        "review_counts": {"accepted": 2, "borderline": 1, "rejected": 0},
    }


def make_backend_payload(
    run_id: str = PACKET_RUN_ID,
    from_date: str = PACKET_REVIEW_DATE,
    to_date: str = PACKET_REVIEW_DATE,
    tickers: list[str] | None = None,
    git_head: str | None = PACKET_GIT_HEAD,
    status: str = "pass",
    ok: bool = True,
    ticker_errors: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build a sanitized live-shape response (ok, status, run_id, query,
    payload, ticker_errors, reviews, review_options, review_counts)."""
    tickers = ["AAPL"] if tickers is None else tickers
    return {
        "ok": ok,
        "status": status,
        "run_id": run_id,
        "query": {
            "from_date": from_date,
            "to_date": to_date,
            "tickers": tickers,
        },
        "payload": {
            "tickers": {t: {"review_date": from_date} for t in tickers
                        if t not in (ticker_errors or {})},
            "errors": ticker_errors or {},
        },
        "ticker_errors": ticker_errors or {},
        "reviews": [_review(t, git_head=git_head) for t in tickers
                    if t not in (ticker_errors or {})],
        "review_options": {
            "candidates": [
                {"run_id": PACKET_RUN_ID, "review_date": PACKET_REVIEW_DATE,
                 "scope": PACKET_SCOPE, "git_head": git_head},
            ],
            "default_selection": {
                "run_id": PACKET_RUN_ID, "date": PACKET_REVIEW_DATE,
                "tickers": list(PACKET_TICKERS),
            },
        },
        "review_counts": {t: {"total": 3} for t in tickers
                          if t not in (ticker_errors or {})},
    }


def _request(**overrides: Any) -> Any:
    """A validated AuditRequest-shaped dict (as emitted by validate_request)."""
    base: dict[str, Any] = {
        "packet_id": PACKET_RUN_ID,
        "date": PACKET_REVIEW_DATE,
        "tickers": ["AAPL"],
        "source": "backend",
        "git_head": PACKET_GIT_HEAD,
    }
    base.update(overrides)
    return validate_request(**base)


def _mock_get(response: Any, *, status: int = 200,
              raise_exc: Exception | None = None) -> MagicMock:
    mock = MagicMock()
    if raise_exc is not None:
        mock.__enter__.side_effect = raise_exc
        return mock
    mock.__enter__.return_value = mock
    mock.get.return_value.status_code = status
    mock.get.return_value.content = b"{}"
    mock.get.return_value.json.return_value = response
    return mock


# ---------------------------------------------------------------------------
# Local fixture source (unchanged behavior, labeled preview)
# ---------------------------------------------------------------------------

@pytest.fixture
def local_source() -> LocalFixtureSource:
    """Read the explicitly synthetic tracked unit sample, independent of cwd."""
    sample = Path(__file__).resolve().parents[2] / "data/sample/stock_evidence_local_unit_packet.json"
    local, _ = get_sources(fixture_path=str(sample))
    return local


def test_local_fixture_loads_representative_unit_sample(local_source: LocalFixtureSource) -> None:
    """Preserve the synthetic sample's requested identity through real JSON reading."""
    local = local_source
    payload = local.fetch_audit(_request(source="local"))
    assert payload["packet_id"] == DEFAULT_PACKET_ID
    assert payload["date"] == PACKET_REVIEW_DATE
    assert payload["git_head"] == PACKET_GIT_HEAD
    assert set(payload["ticker_audits"]) == {"AAPL"}


def test_local_fixture_is_labeled_historical_preview(local_source: LocalFixtureSource) -> None:
    """The unit sample remains visibly synthetic and a historical preview."""
    local = local_source
    payload = local.fetch_audit(_request(source="local"))
    assert payload["audit"]
    assert "preview" in payload["audit"].lower() or "historical" in payload["audit"].lower()
    assert "not a trading decision" in payload["audit"]
    assert "Synthetic unit-test sample; not production evidence" in payload["audit"]


def test_local_unknown_packet_raises():
    local, _ = get_sources()
    with pytest.raises(UnknownPacketError):
        local.fetch_audit(_request(packet_id="no-such-packet", source="local"))


def test_local_invalid_source_rejected():
    local, _ = get_sources()
    with pytest.raises(InvalidSourceError):
        local.fetch_audit(_request(source="live"))


def test_local_git_head_conflicts_with_authoritative():
    local, _ = get_sources()
    with patch(
        "app.lab.stock_evidence_site.sources.load_fixture",
        return_value={"run_id": PACKET_RUN_ID, "git_head": "deadbeef" * 5, "date": PACKET_REVIEW_DATE,
                      "ticker_audits": {}},
    ):
        with pytest.raises(PacketError) as exc:
            local.fetch_audit(_request(source="local"))
    assert exc.value.status == 409
    assert "conflict" in exc.value.message


# ---------------------------------------------------------------------------
# Backend adapter: request construction and identity correlation
# ---------------------------------------------------------------------------

def test_backend_sends_run_id_from_equal_dates_and_requested_tickers():
    """The live endpoint takes run_id, from_date, to_date, tickers — the
    selected date must be sent as explicit equal from_date/to_date."""
    _, backend = get_sources(backend_base_url="https://backend.invalid")
    with patch("app.lab.stock_evidence_site.sources.httpx.Client") as client_cls:
        client_cls.return_value = _mock_get(make_backend_payload())
        backend.fetch_audit(_request())
    client = client_cls.return_value
    req = client.get.call_args
    path = req.args[0] if req.args else req.kwargs.get("url")
    assert path == "/api/stocks/audit"
    params = req.kwargs.get("params") or (req.args[1] if len(req.args) > 1 else {})
    assert params["run_id"] == PACKET_RUN_ID
    assert params["from_date"] == PACKET_REVIEW_DATE
    assert params["to_date"] == PACKET_REVIEW_DATE
    assert params["from_date"] == params["to_date"]
    assert params["tickers"] == "AAPL"
    with_ = req.kwargs.get("with", None)
    assert "source" not in params
    assert "packet_id" not in params
    assert with_ is None or "source" not in str(with_)


def test_backend_success_correlates_identity_and_returns_payload():
    _, backend = get_sources(backend_base_url="https://backend.invalid")
    with patch("app.lab.stock_evidence_site.sources.httpx.Client") as client_cls:
        client_cls.return_value = _mock_get(make_backend_payload())
        payload = backend.fetch_audit(_request())
    # Served identity correlated with request; no invented provenance.
    assert payload["run_id"] == PACKET_RUN_ID
    assert payload["review_date"] == PACKET_REVIEW_DATE
    assert payload["git_head"] == PACKET_GIT_HEAD
    assert payload["generated_at"] == PACKET_GENERATED_AT
    assert payload["scope"] == PACKET_SCOPE
    assert payload["ticker_errors"] == {}
    assert payload["reviews"][0]["review_counts"] == {"accepted": 2, "borderline": 1, "rejected": 0}
    # Served audit rows available for rendering; not a silent zero row.
    assert "AAPL" in (payload.get("ticker_audits") or {})
    assert payload["review_counts"] == {"AAPL": {"total": 3}}


def test_backend_fails_closed_on_run_id_conflict():
    _, backend = get_sources(backend_base_url="https://backend.invalid")
    bad = make_backend_payload(run_id="some-other-run")
    with patch("app.lab.stock_evidence_site.sources.httpx.Client") as client_cls:
        client_cls.return_value = _mock_get(bad)
        with pytest.raises(PacketError) as exc:
            backend.fetch_audit(_request())
    assert exc.value.status == 409
    assert "run_id" in exc.value.message


def test_backend_fails_closed_on_date_conflict():
    _, backend = get_sources(backend_base_url="https://backend.invalid")
    bad = make_backend_payload(from_date="2026-09-03")
    with patch("app.lab.stock_evidence_site.sources.httpx.Client") as client_cls:
        client_cls.return_value = _mock_get(bad)
        with pytest.raises(PacketError) as exc:
            backend.fetch_audit(_request())
    assert exc.value.status == 409
    assert "date" in exc.value.message


def test_backend_fails_closed_on_ticker_conflict():
    _, backend = get_sources(backend_base_url="https://backend.invalid")
    bad = make_backend_payload(tickers=["AAPL", "META"])
    with patch("app.lab.stock_evidence_site.sources.httpx.Client") as client_cls:
        client_cls.return_value = _mock_get(bad)
        with pytest.raises(PacketError) as exc:
            backend.fetch_audit(_request())
    assert exc.value.status == 409
    assert "ticker" in exc.value.message


def test_backend_fails_closed_on_git_head_conflict_with_authoritative():
    _, backend = get_sources(backend_base_url="https://backend.invalid")
    bad = make_backend_payload(git_head="deadbeef" * 5)
    with patch("app.lab.stock_evidence_site.sources.httpx.Client") as client_cls:
        client_cls.return_value = _mock_get(bad)
        with pytest.raises(PacketError) as exc:
            backend.fetch_audit(_request())
    assert exc.value.status == 409
    assert "git_head" in exc.value.message


def test_backend_missing_identity_fails_closed_not_invented():
    """Served git_head absent and no authoritative conflict: the adapter
    must not fill default provenance."""
    _, backend = get_sources(backend_base_url="https://backend.invalid")
    missing = make_backend_payload(git_head=None)
    with patch("app.lab.stock_evidence_site.sources.httpx.Client") as client_cls:
        client_cls.return_value = _mock_get(missing)
        with pytest.raises(PacketError) as exc:
            backend.fetch_audit(_request())
    assert exc.value.status == 409
    assert "git_head" in exc.value.message
    assert "missing" in exc.value.message.lower()


def test_backend_missing_requested_ticker_is_error_not_zero_row():
    _, backend = get_sources(backend_base_url="https://backend.invalid")
    partial = make_backend_payload(
        tickers=["AAPL", "TSLA"],
        ticker_errors={"TSLA": {"status": 404, "message": "audit not found"}},
    )
    with patch("app.lab.stock_evidence_site.sources.httpx.Client") as client_cls:
        client_cls.return_value = _mock_get(partial)
        payload = backend.fetch_audit(_request(tickers=["AAPL", "TSLA"]))
    # TSLA is reported as an explicit per-ticker error, never a zero row.
    assert "TSLA" in payload["ticker_errors"]
    assert "TSLA" not in (payload.get("ticker_audits") or {})
    assert payload["ticker_errors"]["TSLA"]["status"] == 404
    # The present ticker still renders with its real evidence.
    assert "AAPL" in (payload.get("ticker_audits") or {})


def test_backend_ok_false_is_failed_not_error_payload():
    _, backend = get_sources(backend_base_url="https://backend.invalid")
    failed = make_backend_payload(ok=False, status="fail")
    with patch("app.lab.stock_evidence_site.sources.httpx.Client") as client_cls:
        client_cls.return_value = _mock_get(failed)
        with pytest.raises(PacketError) as exc:
            backend.fetch_audit(_request())
    assert exc.value.status == 422
    assert "fail" in exc.value.message


def test_backend_transport_failure_raises_transport_error():
    _, backend = get_sources(backend_base_url="https://backend.invalid")
    with patch("app.lab.stock_evidence_site.sources.httpx.Client") as client_cls:
        client_cls.return_value = _mock_get(None, raise_exc=httpx.ConnectError("connection refused"))
        with pytest.raises(TransportError) as exc:
            backend.fetch_audit(_request())
    assert exc.value.status == 502


def test_backend_http_error_maps_to_transport_or_conflict():
    _, backend = get_sources(backend_base_url="https://backend.invalid")
    with patch("app.lab.stock_evidence_site.sources.httpx.Client") as client_cls:
        client_cls.return_value = _mock_get(None, status=500)
        with pytest.raises(TransportError) as exc:
            backend.fetch_audit(_request())
    assert exc.value.status == 502
    assert "500" in exc.value.message


def test_backend_review_options_proxies_live_candidates():
    _, backend = get_sources(backend_base_url="https://backend.invalid")
    live = make_backend_payload()["review_options"]
    with patch("app.lab.stock_evidence_site.sources.httpx.Client") as client_cls:
        client_cls.return_value = _mock_get(live)
        opts = backend.review_options()
    assert "candidates" in opts
    assert opts["default_selection"]["run_id"] == PACKET_RUN_ID
    assert opts["source"] == "backend"


def test_backend_review_options_transport_failure():
    _, backend = get_sources(backend_base_url="https://backend.invalid")
    with patch("app.lab.stock_evidence_site.sources.httpx.Client") as client_cls:
        client_cls.return_value = _mock_get(None, raise_exc=httpx.ConnectError("refused"))
        with pytest.raises(TransportError):
            backend.review_options()


# ---------------------------------------------------------------------------
# Request validation (malformed date, sources, tickers)
# ---------------------------------------------------------------------------

def test_validate_request_rejects_malformed_date():
    with pytest.raises(RequestError) as exc:
        validate_request(PACKET_RUN_ID, "09/04/2026", "AAPL", "backend")
    assert exc.value.status == 400
    assert "date" in exc.value.message


def test_validate_request_rejects_invalid_source():
    with pytest.raises(InvalidSourceError) as exc:
        validate_request(PACKET_RUN_ID, PACKET_REVIEW_DATE, "AAPL", "live")
    assert exc.value.status == 400
    assert "source" in exc.value.message


def test_validate_request_rejects_empty_tickers():
    with pytest.raises(RequestError) as exc:
        validate_request(PACKET_RUN_ID, PACKET_REVIEW_DATE, " , ", "backend")
    assert exc.value.status == 400


def test_validate_request_accepts_backend_source():
    req = validate_request(PACKET_RUN_ID, PACKET_REVIEW_DATE, "AAPL", "backend")
    assert req.source == "backend"
    assert req.tickers == ["AAPL"]


# ---------------------------------------------------------------------------
# Rendering (frozen render/app code — regression coverage)
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("block,key,bad", [
    ("producer_correlation", "run_id", "other"),
    ("producer_correlation", "from_date", "2026-09-03"),
    ("producer_correlation", "to_date", "2026-09-03"),
    ("producer_correlation", "ticker", "AAPL"),
    ("producer_correlation", "from_date", None),
    ("producer_correlation", "run_id", None),
    ("packet_metadata", "git_head", "bad"),
    ("packet_metadata", "git_head", None),
    ("packet_metadata", "run_id", "other"),
    ("packet_metadata", "review_date", "2026-09-03"),
    ("packet_metadata", "selected_ticker", "AAPL"),
    ("packet_metadata", "packet_tickers", ["AAPL"]),
    ("packet_metadata", "packet_tickers", [{}]),
    ("packet_metadata", "review_date", None),
])
def test_second_review_identity_is_checked(block: str, key: str, bad: Any) -> None:
    """A correct first ticker must never mask contradictory second evidence."""
    _, backend = get_sources(backend_base_url="https://backend.invalid")
    payload = make_backend_payload(tickers=list(PACKET_TICKERS))
    payload["reviews"][1][block][key] = bad
    with patch.object(backend, "_get_json", return_value=payload):
        with pytest.raises(PacketError) as exc:
            backend.fetch_audit(_request(tickers=list(PACKET_TICKERS)))
    assert exc.value.status == 409


@pytest.mark.parametrize("bad_date", ["2026-02-30", "2026-13-04", "20260904"])
def test_impossible_calendar_date(bad_date: str) -> None:
    """Reject both bad format and impossible dates."""
    with pytest.raises(RequestError):
        _request(date=bad_date)


@pytest.mark.parametrize("value", [None, [], {}, {"reviews": "bad"}])
def test_malformed_backend_payload(value: Any) -> None:
    """Malformed envelopes produce actionable packet errors."""
    _, backend = get_sources(backend_base_url="https://backend.invalid")
    with patch.object(backend, "_get_json", return_value=value):
        with pytest.raises(PacketError):
            backend.fetch_audit(_request())


def test_unreported_missing_review_and_all_empty() -> None:
    """Missing evidence gets an error even if backend omitted ticker_errors."""
    _, backend = get_sources(backend_base_url="https://backend.invalid")
    payload = make_backend_payload(tickers=list(PACKET_TICKERS))
    payload["reviews"].pop()
    with patch.object(backend, "_get_json", return_value=payload):
        result = backend.fetch_audit(_request(tickers=list(PACKET_TICKERS)))
    assert "MSFT" in result["ticker_errors"]
    assert "MSFT" not in result["ticker_audits"]
    payload["reviews"] = []
    with patch.object(backend, "_get_json", return_value=payload):
        with pytest.raises(PacketError) as exc:
            backend.fetch_audit(_request(tickers=list(PACKET_TICKERS)))
    assert exc.value.status == 422


@pytest.mark.parametrize("failure", ["json", "timeout", "http"])
def test_native_httpx_errors(failure: str) -> None:
    """Exercise real HTTP response handling, not invented response attributes."""
    _, backend = get_sources(backend_base_url="https://backend.invalid")

    def handler(request: httpx.Request) -> httpx.Response:
        if failure == "timeout":
            raise httpx.ReadTimeout("timeout", request=request)
        if failure == "http":
            return httpx.Response(503, request=request)
        return httpx.Response(200, content=b"not-json", request=request)

    with patch.object(backend, "_client", return_value=httpx.Client(
        base_url="https://backend.invalid", transport=httpx.MockTransport(handler)
    )):
        with pytest.raises(PacketError) as exc:
            backend.fetch_audit(_request())
    assert exc.value.status == (422 if failure == "json" else 502)


def test_native_four_ticker_response_preserves_provenance() -> None:
    """Native transport correlates all four tickers and preserves null metadata."""
    _, backend = get_sources(backend_base_url="https://backend.invalid")
    payload = make_backend_payload(tickers=list(PACKET_TICKERS))
    for review in payload["reviews"]:
        review["packet_metadata"]["generated_at"] = None
        review["packet_metadata"]["scope"] = None

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.params["tickers"] == "AAPL,AMD,NVDA,MSFT"
        assert request.url.params["from_date"] == request.url.params["to_date"]
        return httpx.Response(200, json=payload, request=request)

    with patch.object(backend, "_client", return_value=httpx.Client(
        base_url="https://backend.invalid", transport=httpx.MockTransport(handler)
    )):
        result = backend.fetch_audit(_request(tickers=list(PACKET_TICKERS)))
    assert set(result["ticker_audits"]) == set(PACKET_TICKERS)
    assert result["generated_at"] is None
    assert result["scope"] is None


@pytest.mark.parametrize("field", ["generated_at", "scope"])
@pytest.mark.parametrize("index", [0, 1])
@pytest.mark.parametrize("unknown", ["missing", None, ""])
def test_metadata_unknown_takes_precedence(field: str, index: int, unknown: Any) -> None:
    """Any unknown provenance defeats even otherwise conflicting populated values."""
    _, backend = get_sources(backend_base_url="https://backend.invalid")
    payload = make_backend_payload(tickers=list(PACKET_TICKERS))
    meta = payload["reviews"][index]["packet_metadata"]
    if unknown == "missing":
        del meta[field]
    else:
        meta[field] = unknown
    payload["reviews"][2]["packet_metadata"][field] = "different populated value"
    with patch.object(backend, "_get_json", return_value=payload):
        result = backend.fetch_audit(_request(tickers=list(PACKET_TICKERS)))
    assert result[field] is None
    assert result["reviews"] == payload["reviews"]


@pytest.mark.parametrize("field", ["generated_at", "scope"])
@pytest.mark.parametrize("value", [None, "unanimous served value"])
def test_metadata_unanimous_values_preserved(field: str, value: Any) -> None:
    """Unanimous populated and null provenance are preserved without substitution."""
    _, backend = get_sources(backend_base_url="https://backend.invalid")
    payload = make_backend_payload(tickers=list(PACKET_TICKERS))
    for review in payload["reviews"]:
        review["packet_metadata"][field] = value
    with patch.object(backend, "_get_json", return_value=payload):
        result = backend.fetch_audit(_request(tickers=list(PACKET_TICKERS)))
    assert result[field] == value
    assert result["reviews"] == payload["reviews"]


@pytest.mark.parametrize("field", ["generated_at", "scope"])
def test_metadata_populated_conflict_fails_closed(field: str) -> None:
    """A later populated disagreement names its field in an actionable 422."""
    _, backend = get_sources(backend_base_url="https://backend.invalid")
    payload = make_backend_payload(tickers=list(PACKET_TICKERS))
    payload["reviews"][1]["packet_metadata"][field] = "different populated value"
    with patch.object(backend, "_get_json", return_value=payload):
        with pytest.raises(InvalidPayloadError) as exc:
            backend.fetch_audit(_request(tickers=list(PACKET_TICKERS)))
    assert exc.value.status == 422
    assert field in exc.value.message


def test_local_unit_sample_generated_at_preserved(local_source: LocalFixtureSource) -> None:
    """Representative unit sample timestamp is not borrowed from the backend."""
    local = local_source
    result = local.fetch_audit(_request(source="local", tickers=list(PACKET_TICKERS)))
    assert result["generated_at"] == "2026-09-08T06:05:01.278755+00:00"
    assert result["generated_at"] != PACKET_GENERATED_AT
    assert result["freshness"]["is_fresh"] is False
    assert set(result["ticker_audits"]) == set(PACKET_TICKERS)


def test_render_payload_representative_unit_sample(local_source: LocalFixtureSource) -> None:
    """Render identity and ticker sections from the synthetic unit sample."""
    local = local_source
    req = local.fetch_audit(_request(source="local"))
    ctx = {
        "packet_id": DEFAULT_PACKET_ID,
        "date": PACKET_REVIEW_DATE,
        "tickers": ",".join(PACKET_TICKERS),
        "source": "local",
    }
    html = render_payload(ctx, req)
    assert SITE_NAME in html
    assert PACKET_GIT_HEAD in html
    for ticker in PACKET_TICKERS:
        assert f'id="ticker-{ticker}"' in html


def test_render_error_shows_explicit_state():
    html = render_error(
        {"packet_id": DEFAULT_PACKET_ID, "date": PACKET_REVIEW_DATE,
         "tickers": "AAPL"},
        {"status": "502 Bad Gateway", "message": "backend unavailable"},
    )
    assert SITE_NAME in html
    assert "502 Bad Gateway" in html
    assert "No data is substituted" in html


def test_render_payload_marks_missing_ticker_as_error_not_zero_row():
    backend_payload = make_backend_payload(
        tickers=["AAPL", "TSLA"],
        ticker_errors={"TSLA": {"status": 404, "message": "audit not found"}},
    )
    # Mirror the adapter's normalization for the renderer contract.
    ctx = {
        "packet_id": PACKET_RUN_ID,
        "date": PACKET_REVIEW_DATE,
        "tickers": "AAPL,TSLA",
        "source": "backend",
    }
    html = render_payload(ctx, backend_payload)
    assert "audit not found" in html
    assert "no data" in html.lower()


def _render_case(audit: dict[str, Any] | None = None, **metadata: Any) -> str:
    """Render admitted evidence with explicit test metadata, without a network."""
    return render_payload(
        {"packet_id": "requested-run", "date": "2026-09-03",
         "tickers": "AAPL", "source": "backend"},
        {"packet_id": "served-run", "date": PACKET_REVIEW_DATE,
         "ticker_audits": {"AAPL": audit if audit is not None else {"ticker": "AAPL"}},
         **metadata},
    )


def test_render_error_precedence_and_escaping() -> None:
    """Delivered ticker errors suppress even otherwise present evidence."""
    html = _render_case(
        {"ticker": "AAPL", "articles": [{"headline": "SHOULD NOT APPEAR"}]},
        ticker_errors={"AAPL": {"status": 404, "message": '<script>"audit"</script>'}},
    )
    assert "Delivered status: 404" in html
    assert "&lt;script&gt;&quot;audit&quot;&lt;/script&gt;" in html
    assert "<script>" not in html
    assert "SHOULD NOT APPEAR" not in html
    assert "Articles (0)" not in html
    assert "per-ticker no-data error" in html


def test_render_missing_audit_and_delivered_empty_are_distinct() -> None:
    """Only a delivered audit can truthfully have an empty article list."""
    missing = _render_case(ticker_audits={})
    assert "Unavailable: no ticker audit or error detail delivered" in missing
    assert "Articles (0)" not in missing
    empty = _render_case({"ticker": "AAPL", "articles": []})
    assert "Articles (0)" in empty
    assert "No article rows delivered" in empty
    assert "per-ticker no-data error" not in empty


@pytest.mark.parametrize("unknown", [None, "", "   "])
def test_render_unknown_metadata_and_aggregates(unknown: Any) -> None:
    """Null and blank values remain unknown, but real zero remains zero."""
    html = _render_case(
        {"ticker": "AAPL", "semantic_aggregates": [
            {"nlp_feature_values": {"unknown_value": unknown, "real_zero": 0}}
        ]},
        packet_id=unknown, git_head=unknown, generated_at=unknown,
        review_date=unknown, scope=unknown, date=unknown,
    )
    for label in ["review date", "served date", "scope", "generated at"]:
        assert f"<dt>{label}</dt><dd>unknown</dd>" in html
    for label in ["served packet / run id", "git head"]:
        assert f"<dt>{label}</dt><dd><code>unknown</code></dd>" in html
    assert "<dt>unknown_value</dt><dd>unknown</dd>" in html
    assert "<dt>real_zero</dt><dd>0</dd>" in html


def test_render_requested_and_served_identity_separate() -> None:
    """Served identity never borrows the request's expected producer fields."""
    html = _render_case(generated_at="served-time", git_head="served-head")
    assert "requested packet / run id</dt><dd><code>requested-run" in html
    assert "served packet / run id</dt><dd><code>served-run" in html
    assert "requested date</dt><dd>2026-09-03" in html
    assert "served date</dt><dd>2026-09-04" in html
    assert "generated at</dt><dd>served-time" in html
    assert "git head</dt><dd><code>served-head" in html
    missing = render_payload({"packet_id": "request-only", "git_head": "expected-only"}, {})
    assert "served packet / run id</dt><dd><code>unknown" in missing
    assert "expected-only" not in missing


@pytest.mark.parametrize("fresh", [True, False, None, "absent"])
def test_render_freshness_is_delivered_classification_only(fresh: Any) -> None:
    """No clock-based classification or unconditional historical warning."""
    freshness = {"threshold_seconds": 86400}
    if fresh != "absent":
        freshness["is_fresh"] = fresh
    html = _render_case(freshness=freshness, generated_at="served-time")
    assert ("This packet is <strong>historical</strong>" in html) == (fresh is False)
    if fresh is False:
        assert "historical (threshold 86400s)" in html
    elif fresh is True:
        assert "fresh (classification only; not readiness)" in html
        assert "historical" not in html
    else:
        assert "unknown freshness" in html
        assert "historical" not in html
    assert "generated at</dt><dd>served-time" in html


@pytest.mark.parametrize("freshness", [None, {}])
def test_render_absent_freshness_unknown(freshness: Any) -> None:
    """Missing freshness is unknown, regardless of the timestamp."""
    assert "unknown freshness" in _render_case(freshness=freshness)


@pytest.mark.parametrize("source", ["local", "backend"])
def test_render_source_and_reported_readiness_not_acceptance(source: str) -> None:
    """Even a producer true flag cannot present historical evidence as accepted."""
    html = _render_case(
        {"ticker": "AAPL", "api": {"run_readiness": {
            "ready_for_final_human_acceptance": True
        }}}, source=source, freshness={"is_fresh": False},
    )
    assert ("local historical preview; not current readiness" in html) == (source == "local")
    assert ("backend; no local substitution" in html) == (source == "backend")
    assert "producer-reported ready_for_final_human_acceptance: True; not acceptance" in html
    assert 'class="badge badge-ok"' not in html
    assert "API status=pass is transport only" in html
    assert "Refresh manually with GET/reload" in html


@pytest.mark.parametrize("count", [0, 1, 4, None])
def test_render_hmm_warning_only_for_delivered_one_point(count: int | None) -> None:
    """Unknown and multi-point HMM evidence cannot claim a one-point audit."""
    hmm = {} if count is None else {"regime_rows": [{} for _ in range(count)]}
    html = _render_case({"ticker": "AAPL", "hmm": hmm})
    assert ("HMM single-point audit" in html) == (count == 1)
    assert f"{count if count is not None else 'unknown'} point(s)" in html
    assert "has only one point" not in html


@pytest.mark.parametrize("reported", [False, None, ""])
def test_render_reported_readiness_retains_false_and_unknown(reported: Any) -> None:
    """A false or unknown producer flag is not omitted or upgraded to acceptance."""
    html = _render_case({"ticker": "AAPL", "api": {"run_readiness": {
        "ready_for_final_human_acceptance": reported
    }}})
    expected = "False" if reported is False else "unknown"
    assert f"producer-reported ready_for_final_human_acceptance: {expected}; not acceptance" in html
    assert 'class="badge badge-ok"' not in html


def test_render_readiness_delivered_hmm_count() -> None:
    """Backend readiness count can supply delivered single-point evidence."""
    html = _render_case({"ticker": "AAPL", "api": {"run_readiness": {
        "hmm_chart_auditability_state": {"point_count": 1}
    }}})
    assert "HMM single-point audit" in html


def test_render_all_four_tickers_visible_and_anchor_accessible(
    local_source: LocalFixtureSource,
) -> None:
    """Every anchor targets a visible section without requiring JavaScript."""
    local = local_source
    payload = local.fetch_audit(_request(source="local", tickers=list(PACKET_TICKERS)))
    html = render_payload({"tickers": list(PACKET_TICKERS), "source": "local"}, payload)
    for ticker in PACKET_TICKERS:
        assert f'href="#ticker-{ticker}"' in html
        assert f'id="ticker-{ticker}"' in html
        assert f'aria-label="Evidence for {ticker}"' in html
    assert "aria-hidden" not in html
    assert "display:none" not in html


def test_render_evidence_escaping_and_dimensionless_scores() -> None:
    """Escaping changes markup only; scores retain their existing formatting."""
    html = _render_case({"ticker": "AAPL", "articles": [
        {"headline": "<b>news</b>", "source": '"wire"',
         "relevance_score": 0.12345, "relevance_state": "accepted"},
        {"relevance_score": None}
    ], "relevance_gate": {"accepted_count": None, "borderline_count": 0}})
    assert "&lt;b&gt;news&lt;/b&gt;" in html
    assert "&quot;wire&quot;" in html
    assert "relevance 0.1235 (accepted)" in html
    assert "relevance unknown (unknown)" in html
    assert "<dt>accepted</dt><dd>unknown</dd>" in html
    assert "<dt>borderline</dt><dd>0</dd>" in html
    assert "not delivered by backend" in html


@pytest.fixture
def http_site(
    monkeypatch: pytest.MonkeyPatch, local_source: LocalFixtureSource,
) -> Iterator[tuple[TestClient, MagicMock, MagicMock]]:
    """Inject explicit read-only sources; never access a live backend."""
    local = local_source
    backend = MagicMock()
    backend.fetch_audit.return_value = {
        "packet_id": PACKET_RUN_ID, "date": PACKET_REVIEW_DATE,
        "generated_at": "backend-served-time", "git_head": PACKET_GIT_HEAD,
        "freshness": {"is_fresh": False, "threshold_seconds": 86400},
        "ticker_audits": {t: {"ticker": t, "articles": []} for t in PACKET_TICKERS},
    }
    backend.review_options.return_value = {"source": "backend", "candidates": []}
    local_spy = MagicMock(wraps=local)
    monkeypatch.setattr(site_app, "_local", local_spy)
    monkeypatch.setattr(site_app, "_backend", backend)
    with TestClient(site_app.app) as client:
        yield client, local_spy, backend


def test_local_missing_file_fails_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An absent explicit packet returns errors, never the successful unit sample."""
    absent = tmp_path / "absent-packet.json"
    assert not absent.exists()
    local = LocalFixtureSource(fixture_path=str(absent))
    with pytest.raises(UnknownPacketError) as exc:
        local.fetch_audit(_request(source="local"))
    assert exc.value.status == 404
    assert "no cached/latest substitution" in exc.value.message
    backend = MagicMock()
    monkeypatch.setattr(site_app, "_local", local)
    monkeypatch.setattr(site_app, "_backend", backend)
    params = {"run_id": PACKET_RUN_ID, "date": PACKET_REVIEW_DATE,
              "tickers": "AAPL", "source": "local"}
    with TestClient(site_app.app) as client:
        response = client.get("/api/stocks/audit", params=params)
        assert response.status_code == 404
        assert response.json() == {"error": exc.value.message}
        params["packet_id"] = params.pop("run_id")
        root = client.get("/", params=params)
        assert root.status_code == 200
        assert "HTTP 404" in root.text
        assert "local fixture packet not found" in root.text
        assert "No data is substituted" in root.text
        assert 'id="ticker-AAPL"' not in root.text
    backend.fetch_audit.assert_not_called()
    backend.review_options.assert_not_called()
    assert not absent.exists()


def test_http_healthz(http_site: Any) -> None:
    """Liveness requires neither packet source."""
    client, local, backend = http_site
    response = client.get("/healthz")
    assert response.status_code == 200
    assert response.json() == {"status": "ok", "service": "stock-evidence-site"}
    local.fetch_audit.assert_not_called()
    backend.fetch_audit.assert_not_called()


@pytest.mark.parametrize("source", ["local", "backend"])
def test_http_root_and_json_sources(http_site: Any, source: str) -> None:
    """Root and JSON preserve each selected source's served provenance."""
    client, local, backend = http_site
    params = {"date": PACKET_REVIEW_DATE, "tickers": ",".join(PACKET_TICKERS), "source": source}
    response = client.get("/", params={"packet_id": PACKET_RUN_ID, **params})
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/html")
    for ticker in PACKET_TICKERS:
        assert f'id="ticker-{ticker}"' in response.text
    expected_time = "backend-served-time" if source == "backend" else "2026-09-08T06:05:01.278755+00:00"
    assert expected_time in response.text
    response = client.get("/api/stocks/audit", params={"run_id": PACKET_RUN_ID, **params})
    assert response.status_code == 200
    assert response.json()["source"] == source
    assert response.json()["generated_at"] == expected_time
    selected, other = (backend, local) if source == "backend" else (local, backend)
    assert selected.fetch_audit.call_count == 2
    other.fetch_audit.assert_not_called()


@pytest.mark.parametrize("source", ["local", "backend"])
def test_http_review_options(http_site: Any, source: str) -> None:
    """Review options route to only the explicitly selected source."""
    client, local, backend = http_site
    response = client.get("/api/stocks/review-options", params={"source": source})
    assert response.status_code == 200
    selected, other = (backend, local) if source == "backend" else (local, backend)
    if source == "backend":
        assert response.json() == backend.review_options.return_value
    else:
        assert response.json()["source"] == "local"
    selected.review_options.assert_called_once()
    other.review_options.assert_not_called()


@pytest.mark.parametrize("field,value", [
    ("run_id", "invalid-run"), ("date", "2026-02-30"), ("source", "invalid-source")
])
def test_http_invalid_requests(http_site: Any, field: str, value: str) -> None:
    """Validation errors are viewable HTML200 and explicit JSON errors."""
    client, local, backend = http_site
    params = {"run_id": PACKET_RUN_ID, "date": PACKET_REVIEW_DATE,
              "tickers": "AAPL", "source": "local", field: value}
    response = client.get("/api/stocks/audit", params=params)
    assert response.status_code in (400, 404)
    assert "error" in response.json()
    params["packet_id"] = params.pop("run_id")
    root = client.get("/", params=params)
    assert root.status_code == 200
    assert "No data is substituted" in root.text
    assert f"HTTP {response.status_code}" in root.text
    backend.fetch_audit.assert_not_called()


@pytest.mark.parametrize("status", [502, 503])
def test_http_backend_unavailable_no_local_fallback(http_site: Any, status: int) -> None:
    """Backend failures retain status for JSON and never invoke the local source."""
    client, local, backend = http_site
    backend.fetch_audit.side_effect = PacketError(status, '<backend unavailable>')
    backend.review_options.side_effect = PacketError(status, '<backend unavailable>')
    params = {"run_id": PACKET_RUN_ID, "date": PACKET_REVIEW_DATE,
              "tickers": "AAPL", "source": "backend"}
    response = client.get("/api/stocks/audit", params=params)
    assert response.status_code == status
    assert response.json() == {"error": "<backend unavailable>"}
    params["packet_id"] = params.pop("run_id")
    root = client.get("/", params=params)
    assert root.status_code == 200
    assert f"HTTP {status}" in root.text
    assert "&lt;backend unavailable&gt;" in root.text
    assert "No data is substituted" in root.text
    options = client.get("/api/stocks/review-options", params={"source": "backend"})
    assert options.status_code == status
    local.fetch_audit.assert_not_called()
    local.review_options.assert_not_called()


def test_http_review_options_invalid_source(http_site: Any) -> None:
    """Unsupported sources fail before querying either candidate provider."""
    client, local, backend = http_site
    response = client.get("/api/stocks/review-options", params={"source": "invalid"})
    assert response.status_code == 400
    assert "invalid source" in response.json()["error"]
    local.review_options.assert_not_called()
    backend.review_options.assert_not_called()
