"""Data sources for the standalone evidence site.

Two read-only sources, selected at request time:

* :class:`LocalFixtureSource` — serves the versioned #281 audit packet JSON
  from a repo-relative path. It is a **labeled historical preview** of a
  bounded packet, never a silent fallback for a live request.
* :class:`PacketBackendAdapter` — calls an *independently configured* private
  packet backend (``STOCK_EVIDENCE_BACKEND`` env var or explicit
  ``backend_base_url``) for ``/api/stocks/review-options`` and
  ``/api/stocks/audit``. Uses ``httpx`` (already a project dependency) so the
  HTTP layer is mockable in unit tests. The private host is never hardcoded.

Both are read-only. Neither writes, backfills, or substitutes a different
run/date/ticker. Served identity (run_id, query dates, tickers, and per-ticker
producer correlation git_head) is correlated with the requested identity and
the authoritative packet, and the adapter **fails closed** on any missing or
conflicting identity. Missing provenance is never filled with a default.
"""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass, field
from datetime import date as calendar_date
from typing import Any

import httpx
from loguru import logger

DEFAULT_PACKET_ID = "issue281-post-pr306-current-prod-20260904-v1"
DEFAULT_DATE = "2026-09-04"
DEFAULT_TICKERS = ("AAPL", "AMD", "MSFT", "NVDA")
DEFAULT_GIT_HEAD = "aaa29eca6fe6e55d3529b4f134471eb6fbe70c85"

# Local fixture = the authoritative bounded #281 production packet.
# The hyphenated artifact is the canonical local historical preview.
_REPO_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "..")
)
DEFAULT_FIXTURE_PATH = os.path.join(
    _REPO_ROOT,
    "artifacts",
    "reports",
    "diagnostics",
    "issue281-post-pr306-current-prod-20260904-v1_audit_2026-09-04.json",
)

# Hard cap: a real packet is ~60KB pretty; 2MB fails closed.
MAX_RESPONSE_BYTES = 2_000_000

_ISO_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


# ---------------------------------------------------------------------------
# Error taxonomy
# ---------------------------------------------------------------------------


class PacketError(Exception):
    """Base error with an HTTP status and a short user-facing message."""

    def __init__(self, status: int, message: str) -> None:
        super().__init__(message)
        self.status = status
        self.message = message


class RequestError(PacketError):
    """Invalid query/run/date (400)."""

    def __init__(self, message: str) -> None:
        super().__init__(400, message)


class InvalidSourceError(PacketError):
    """The selected source is not a supported evidence source (400)."""

    def __init__(self, message: str) -> None:
        super().__init__(400, message)


class UnknownPacketError(PacketError):
    """The requested packet does not exist in the selected source (404)."""

    def __init__(self, message: str) -> None:
        super().__init__(404, message)


class IdentityMismatchError(PacketError):
    """Served identity conflicts with the requested one (409, fail closed)."""

    def __init__(self, message: str) -> None:
        super().__init__(409, message)


class InvalidPayloadError(PacketError):
    """The response is present but failed semantic validation (422)."""

    def __init__(self, message: str) -> None:
        super().__init__(422, message)


class TransportError(PacketError):
    """Transport failure: backend unreachable or returned HTTP >= 500 (502)."""

    def __init__(self, status: int, message: str) -> None:
        super().__init__(status, message)


# ---------------------------------------------------------------------------
# Request validation
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class AuditRequest:
    """A normalized, validated audit request.

    ``git_head`` carries the authoritative packet identity (the git head the
    served packet must correlate to); it is never invented when absent.
    """

    packet_id: str
    date: str
    tickers: list[str]
    source: str  # "local" | "backend"
    git_head: str | None = None
    _extra: dict[str, Any] = field(default_factory=dict, compare=False)

    def as_dict(self) -> dict[str, Any]:
        out = {
            "packet_id": self.packet_id,
            "date": self.date,
            "tickers": list(self.tickers),
            "source": self.source,
            "git_head": self.git_head,
        }
        out.update(self._extra)
        return out


def _parse_tickers(raw: str | list[str] | None) -> list[str]:
    if raw is None:
        return []
    if isinstance(raw, list):
        items = [str(t).strip() for t in raw]
    else:
        items = [t.strip() for t in str(raw).split(",")]
    return [t.upper() for t in items if t]


def validate_request(
    packet_id: str | None,
    date: str | None,
    tickers: str | list[str] | None,
    source: str,
    *,
    git_head: str | None = None,
    **extra: Any,
) -> AuditRequest:
    """Validate the query identity; raise :class:`RequestError` on bad input.

    A malformed date (anything not ``YYYY-MM-DD``) is rejected with a 400 that
    names the date field. An unsupported source name raises
    :class:`InvalidSourceError`.
    """
    if source not in ("local", "backend"):
        raise InvalidSourceError(f"invalid source: {source!r}")
    if not packet_id or not str(packet_id).strip():
        raise RequestError("packet_id (run id) is required")
    if not date or not str(date).strip():
        raise RequestError("date is required")
    date = str(date).strip()
    if not _ISO_DATE_RE.match(date):
        raise RequestError(
            f"date must be YYYY-MM-DD, got {date!r} (malformed date)"
        )
    try:
        calendar_date.fromisoformat(date)
    except ValueError as exc:
        raise RequestError(f"invalid calendar date: {date!r}") from exc
    parsed = _parse_tickers(tickers)
    if not parsed:
        raise RequestError("tickers is required (comma-separated, e.g. AAPL,AMD)")
    return AuditRequest(
        packet_id=str(packet_id).strip(),
        date=date,
        tickers=parsed,
        source=source,
        git_head=git_head,
        _extra=dict(extra),
    )


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def load_fixture(path: str) -> dict[str, Any]:
    """Load a local fixture packet as a dict (read-only).

    Returns a normalized dict of the raw JSON. Missing/unreadable files raise
    :class:`UnknownPacketError`; non-dict top-level raises
    :class:`UnknownPacketError`.
    """
    if not os.path.exists(path):
        raise UnknownPacketError(
            f"local fixture packet not found: {path} "
            "(no cached/latest substitution)"
        )
    try:
        with open(path, encoding="utf-8") as fh:
            data = json.load(fh)
    except (OSError, json.JSONDecodeError) as exc:
        logger.error("fixture read failed: {}", exc)
        raise UnknownPacketError(f"local fixture unreadable: {exc}") from exc
    if not isinstance(data, dict):
        raise UnknownPacketError("local fixture has unexpected shape")
    return data


def _normalize(value: Any) -> Any:
    """Recursively strip payload-compaction markers for display safety."""
    if isinstance(value, dict):
        out: dict[str, Any] = {}
        for k, v in value.items():
            if isinstance(v, dict) and v.get("payload_compaction_marker"):
                out[k] = None  # compacted node: unknown, not fabricated
            else:
                out[k] = _normalize(v)
        return out
    if isinstance(value, list):
        return [_normalize(v) for v in value]
    return value


def _first_present(*values: Any) -> Any:
    for v in values:
        if v is not None:
            return v
    return None


# ---------------------------------------------------------------------------
# Local fixture source (labeled historical preview)
# ---------------------------------------------------------------------------


class LocalFixtureSource:
    """Serve the versioned local audit packet fixture (read-only).

    The local fixture is a **historical preview** of a bounded packet. It is
    never a silent fallback for a live backend request; it only answers
    explicitly ``source="local"`` requests, and it still enforces the same
    fail-closed identity correlation as the backend adapter.
    """

    name = "local"

    def __init__(self, fixture_path: str = DEFAULT_FIXTURE_PATH) -> None:
        self.fixture_path = fixture_path

    def review_options(self) -> dict[str, Any]:
        """List locally available packet candidates (labeled preview)."""
        available = os.path.exists(self.fixture_path)
        return {
            "source": self.name,
            "candidates": [
                {
                    "packet_id": DEFAULT_PACKET_ID,
                    "date": DEFAULT_DATE,
                    "tickers": list(DEFAULT_TICKERS),
                    "available": available,
                }
            ],
        }

    def fetch_audit(self, req: AuditRequest) -> dict[str, Any]:
        """Load and identity-check the local fixture packet (fail closed)."""
        if req.source != self.name:
            # A source we do not serve is rejected, never silently handled.
            raise InvalidSourceError(
                f"source {req.source!r} not supported by local fixture source"
            )

        if req.packet_id != DEFAULT_PACKET_ID:
            raise UnknownPacketError("requested packet is not available in local preview")
        data = load_fixture(self.fixture_path)
        data = _normalize(data)

        # Fail-closed identity correlation against the requested identity and
        # the authoritative packet (git_head must be served and match).
        self._check_identity(data, req)

        audits = data.get("ticker_audits")
        if not isinstance(audits, dict) or not audits:
            raise InvalidPayloadError("no ticker audit rows in packet")

        # A missing requested ticker is a per-ticker error, never a zero row.
        missing = [t for t in req.tickers if t not in audits]
        if missing:
            raise UnknownPacketError(
                "requested ticker(s) missing from packet (per-ticker error, "
                f"not a zero row): {', '.join(missing)}"
            )

        # Labeled historical preview; provenance is served by the packet, not
        # filled with a default.
        audit_note = data.get("audit")
        if not isinstance(audit_note, str) or not audit_note:
            audit_note = "historical preview; not a trading decision"
        else:
            audit_note = f"{audit_note} — historical preview; not a trading decision"

        return {
            "packet_id": data.get("run_id") or req.packet_id,
            "git_head": data.get("git_head"),
            "generated_at": data.get("generated_at"),
            "review_date": data.get("review_date") or req.date,
            "date": req.date,
            "scope": data.get("scope"),
            "r2_mode": data.get("r2_mode"),
            "audit": audit_note,
            "tickers": req.tickers,
            "freshness": {
                "is_fresh": False,
                "threshold_seconds": data.get("freshness_threshold_seconds", 86400),
                "note": "historical packet (labeled preview)",
            },
            "ticker_audits": {t: audits.get(t) for t in req.tickers},
        }

    def _check_identity(
        self, data: dict[str, Any], req: AuditRequest
    ) -> None:
        """Fail closed if the served packet does not match the request."""
        served_run = _first_present(data.get("run_id"), data.get("packet_id"))
        if served_run is None:
            raise IdentityMismatchError(
                "run_id missing from served packet (not filled with a default)"
            )
        if served_run != req.packet_id:
            raise IdentityMismatchError(
                f"run_id conflict: served {served_run!r} != requested "
                f"{req.packet_id!r}"
            )

        served_date = _first_present(
            (data.get("request") or {}).get("date")
            if isinstance(data.get("request"), dict)
            else None,
            data.get("review_date"),
            data.get("date"),
        )
        if served_date is None:
            raise IdentityMismatchError(
                "date missing from served packet (not filled with a default)"
            )
        if served_date != req.date:
            raise IdentityMismatchError(
                f"date conflict: served {served_date!r} != requested {req.date!r}"
            )

        for key, expected in (("run_id", req.packet_id), ("packet_id", req.packet_id),
                              ("review_date", req.date), ("date", req.date)):
            if key in data and data[key] != expected:
                raise IdentityMismatchError(f"local {key} conflict")
        request = data.get("request")
        if isinstance(request, dict):
            for key, expected in (("date", req.date), ("run_id", req.packet_id)):
                if key in request and request[key] != expected:
                    raise IdentityMismatchError(f"local request {key} conflict")
        served_head = data.get("git_head")
        if served_head is None:
            raise IdentityMismatchError(
                "git_head missing from served packet (not filled with a default)"
            )
        if req.git_head is not None and served_head != req.git_head:
            raise IdentityMismatchError(
                f"git_head conflict: served {served_head!r} != authoritative "
                f"{req.git_head!r}"
            )
        if served_head != DEFAULT_GIT_HEAD:
            raise IdentityMismatchError(
                f"git_head conflict: served {served_head!r} != authoritative "
                f"{DEFAULT_GIT_HEAD!r}"
            )


# ---------------------------------------------------------------------------
# Backend adapter
# ---------------------------------------------------------------------------


class PacketBackendAdapter:
    """Adapter for an independently configured private packet backend.

    The base URL comes only from ``STOCK_EVIDENCE_BACKEND`` (env) or the
    ``backend_base_url`` argument; the private host is never hardcoded.
    Uses ``httpx`` so the HTTP layer is mockable. All responses are
    identity-checked fail-closed.
    """

    name = "backend"

    def __init__(self, backend_base_url: str | None = None, timeout: float = 10.0):
        base = (
            backend_base_url
            or os.environ.get("STOCK_EVIDENCE_BACKEND", "").rstrip("/")
        )
        self.base_url = base or None
        self.timeout = timeout

    # -- HTTP layer (mockable via app.lab.stock_evidence_site.sources.httpx) --

    def _client(self) -> httpx.Client:
        if not self.base_url:
            raise TransportError(
                502,
                "backend base URL not configured (set STOCK_EVIDENCE_BACKEND)",
            )
        return httpx.Client(base_url=self.base_url, timeout=self.timeout)

    def _get_json(self, path: str, params: dict[str, str]) -> Any:
        try:
            with self._client() as client:
                resp = client.get(path, params=params)
                if resp.status_code >= 400:
                    raise TransportError(502, f"backend returned HTTP {resp.status_code}")
                if len(resp.content) > MAX_RESPONSE_BYTES:
                    raise InvalidPayloadError("backend payload exceeds size limit")
                return resp.json()
        except httpx.HTTPError as exc:
            raise TransportError(502, "backend connection or timeout failure") from exc
        except ValueError as exc:
            raise InvalidPayloadError("backend returned invalid JSON") from exc

    # -- endpoints --

    def review_options(self) -> dict[str, Any]:
        """Fetch review options from the backend (fail closed)."""
        data = self._get_json("/api/stocks/review-options", {})
        if not isinstance(data, dict) or not isinstance(data.get("candidates"), list):
            raise InvalidPayloadError("backend review options have unexpected shape")
        data = _normalize(data)
        data["source"] = self.name
        return data

    def fetch_audit(self, req: AuditRequest) -> dict[str, Any]:
        """Fetch an audit packet from the backend with fail-closed checks."""
        if req.source != self.name:
            raise InvalidSourceError("backend requires source='backend'")
        params = {
            "run_id": req.packet_id,
            "from_date": req.date,  # selected date sent as explicit equal dates
            "to_date": req.date,
            "tickers": ",".join(req.tickers),
        }
        data = self._get_json("/api/stocks/audit", params)
        if not isinstance(data, dict):
            raise TransportError(
                502, "backend audit payload has unexpected shape"
            )
        data = _normalize(data)
        for key, kind in (("reviews", list), ("ticker_errors", dict),
                          ("payload", dict), ("review_counts", dict),
                          ("review_options", dict)):
            if not isinstance(data.get(key), kind):
                raise InvalidPayloadError(f"backend {key} has unexpected shape")
        if not isinstance(data.get("ok"), bool) or not isinstance(data.get("status"), str):
            raise InvalidPayloadError("backend success state missing or malformed")

        # A backend "fail" status is an invalid payload, not transport.
        if data.get("ok") is False or data.get("status") == "fail":
            raise InvalidPayloadError(
                f"backend reported fail (status={data.get('status')!r})"
            )

        self._check_identity(data, req)

        ticker_audits = self._build_ticker_audits(data, req)
        missing = [t for t in req.tickers if t not in ticker_audits]
        ticker_errors = dict(data["ticker_errors"])
        for ticker in missing:
            ticker_errors.setdefault(ticker, {"status": 404, "message": "audit review missing"})

        if not any(t in ticker_audits for t in req.tickers):
            raise InvalidPayloadError("no audit rows returned for any ticker")

        return {
            "run_id": data.get("run_id"),
            "review_date": self._served_review_date(data, req),
            "git_head": self._served_git_head(data),
            "generated_at": self._served_generated_at(data),
            "scope": self._served_scope(data),
            "ticker_errors": ticker_errors,
            "reviews": list(data.get("reviews") or []),
            "review_counts": dict(data.get("review_counts") or {}),
            "review_options": data.get("review_options"),
            "ticker_audits": ticker_audits,
            "date": req.date,
            "tickers": req.tickers,
        }

    # -- identity / provenance correlation --

    def _check_identity(
        self, data: dict[str, Any], req: AuditRequest
    ) -> None:
        served_run = _first_present(data.get("run_id"))
        if served_run is None:
            raise IdentityMismatchError(
                "run_id missing from served response (not filled with a default)"
            )
        if served_run != req.packet_id:
            raise IdentityMismatchError(
                f"run_id conflict: served {served_run!r} != requested "
                f"{req.packet_id!r}"
            )

        query = data.get("query") or {}
        if not isinstance(query, dict):
            query = {}
        for key in ("from_date", "to_date"):
            served = query.get(key)
            if served is None:
                raise IdentityMismatchError(
                    f"{key} missing from served query (not filled with a default)"
                )
            if served != req.date:
                raise IdentityMismatchError(
                    f"date conflict: served {key} {served!r} != requested "
                    f"{req.date!r}"
                )

        served_tickers = query.get("tickers")
        if served_tickers != req.tickers:
            raise IdentityMismatchError(
                f"ticker conflict: served {served_tickers!r} != requested "
                f"{req.tickers!r}"
            )

        # Per-ticker producer correlation must carry a git_head that matches
        # the authoritative packet head; missing or conflicting = fail closed.
        if req.git_head is not None and req.git_head != DEFAULT_GIT_HEAD:
            raise IdentityMismatchError("requested git_head conflicts with authoritative packet")
        seen: set[str] = set()
        for review in data["reviews"]:
            if not isinstance(review, dict):
                raise InvalidPayloadError("backend review has unexpected shape")
            ticker = review.get("ticker")
            if not isinstance(ticker, str) or ticker not in req.tickers or ticker in seen:
                raise IdentityMismatchError("review ticker missing, duplicate, or conflicting")
            seen.add(ticker)
            for block in ("producer_correlation", "packet_metadata"):
                identity = review.get(block)
                if not isinstance(identity, dict):
                    raise IdentityMismatchError(f"{block} identity missing")
                expected_fields = (
                    (("run_id", req.packet_id), ("from_date", req.date),
                     ("to_date", req.date), ("ticker", ticker))
                    if block == "producer_correlation" else
                    (("run_id", req.packet_id), ("review_date", req.date),
                     ("git_head", DEFAULT_GIT_HEAD), ("selected_ticker", ticker))
                )
                for key, expected in expected_fields:
                    if identity.get(key) != expected:
                        state = "missing" if identity.get(key) is None else "conflict"
                        raise IdentityMismatchError(f"{ticker} {block} {key} {state}")
                if block == "packet_metadata":
                    packet_tickers = identity.get("packet_tickers")
                    if (not isinstance(packet_tickers, list)
                            or not all(isinstance(t, str) for t in packet_tickers)
                            or len(packet_tickers) != len(DEFAULT_TICKERS)
                            or set(packet_tickers) != set(DEFAULT_TICKERS)):
                        raise IdentityMismatchError(f"{ticker} packet_tickers missing or conflict")
                for key, expected in (("date", req.date), ("ticker", ticker),
                                      ("git_head", DEFAULT_GIT_HEAD), ("review_date", req.date)):
                    if key in identity and identity[key] != expected:
                        raise IdentityMismatchError(f"{ticker} {block} {key} conflict")
        for key, expected in (("git_head", DEFAULT_GIT_HEAD), ("review_date", req.date),
                              ("packet_id", req.packet_id), ("date", req.date)):
            if key in data and data[key] != expected:
                raise IdentityMismatchError(f"top-level {key} conflict")
        self._served_git_head(data, required=bool(data["reviews"]))

    def _served_git_head(self, data: dict[str, Any], required: bool = False) -> Any:
        """Correlate the served git_head across per-ticker producer blocks."""
        head: Any = None
        for review in data.get("reviews") or []:
            if not isinstance(review, dict):
                continue
            corr = review.get("producer_correlation")
            if isinstance(corr, dict):
                if corr.get("git_head") is not None:
                    head = corr.get("git_head")
                    break
            meta = review.get("packet_metadata")
            if isinstance(meta, dict) and meta.get("git_head") is not None:
                head = meta.get("git_head")
                break
        if head is None:
            head = data.get("git_head")
        if head is None and required:
            raise IdentityMismatchError(
                "git_head missing from served response (not filled with a default)"
            )
        # Correlate against the authoritative packet head.
        if head is not None and head != DEFAULT_GIT_HEAD:
            raise IdentityMismatchError(
                f"git_head conflict: served {head!r} != authoritative "
                f"{DEFAULT_GIT_HEAD!r}"
            )
        return head

    def _served_review_date(self, data: dict[str, Any], req: AuditRequest) -> Any:
        for review in data.get("reviews") or []:
            if isinstance(review, dict):
                corr = review.get("producer_correlation")
                if isinstance(corr, dict) and corr.get("review_date"):
                    return corr.get("review_date")
                meta = review.get("packet_metadata")
                if isinstance(meta, dict) and meta.get("review_date"):
                    return meta.get("review_date")
        return req.date

    def _served_generated_at(self, data: dict[str, Any]) -> Any:
        return self._served_metadata_consensus(data, "generated_at")

    def _served_scope(self, data: dict[str, Any]) -> Any:
        return self._served_metadata_consensus(data, "scope")

    def _served_metadata_consensus(self, data: dict[str, Any], field: str) -> Any:
        """Return unanimous populated provenance, with unknown taking precedence."""
        values = []
        for review in data.get("reviews") or []:
            meta = review.get("packet_metadata") if isinstance(review, dict) else None
            value = meta.get(field) if isinstance(meta, dict) else None
            if not value:
                return None
            values.append(value)
        if not values:
            return None
        if any(value != values[0] for value in values[1:]):
            raise InvalidPayloadError(f"backend packet_metadata {field} conflict across reviews")
        return values[0]

    def _build_ticker_audits(
        self, data: dict[str, Any], req: AuditRequest
    ) -> dict[str, Any]:
        """Map each requested ticker to its served evidence row.

        A ticker absent from ``reviews`` (and/or listed in ``ticker_errors``)
        is *not* given a zero row — it is simply absent here and surfaced via
        ``ticker_errors`` instead.
        """
        reviews = data.get("reviews") or []
        by_ticker: dict[str, Any] = {}
        for review in reviews:
            if isinstance(review, dict):
                t = review.get("ticker")
                if isinstance(t, str):
                    by_ticker[t.upper()] = review
        payload = data.get("payload") or {}
        payload_tickers = (
            payload.get("tickers") if isinstance(payload, dict) else None
        ) or {}
        out: dict[str, Any] = {}
        for t in req.tickers:
            if t in by_ticker and t not in data["ticker_errors"]:
                row = dict(by_ticker[t])
                if isinstance(payload_tickers, dict) and t in payload_tickers:
                    row.setdefault("_payload", payload_tickers[t])
                out[t] = row
        return out


# ---------------------------------------------------------------------------
# Source factory
# ---------------------------------------------------------------------------


def get_sources(
    fixture_path: str = DEFAULT_FIXTURE_PATH,
    backend_base_url: str | None = None,
) -> tuple[LocalFixtureSource, PacketBackendAdapter]:
    """Construct both sources (adapter base URL from env/argument)."""
    return (
        LocalFixtureSource(fixture_path),
        PacketBackendAdapter(backend_base_url),
    )
