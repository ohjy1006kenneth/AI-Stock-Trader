from __future__ import annotations

import csv
import io
import re
import zipfile
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date as Date
from typing import Any

import requests

SUPPORTED_TICKERS_ZIP_URL = "https://apimedia.tiingo.com/docs/tiingo/daily/supported_tickers.zip"
_ACTIVE_END_DATE_VALUES = {"", "null", "none", "nan", "active", "present"}
_SECURITY_ID_PART_PATTERN = re.compile(r"[^A-Z0-9_-]+")


@dataclass(frozen=True)
class TiingoSecurity:
    """One Tiingo security identity row used to key historical price archives."""

    ticker: str
    perma_ticker: str | None
    start_date: str | None = None
    end_date: str | None = None
    exchange: str | None = None
    asset_type: str | None = None
    name: str | None = None

    @property
    def security_id(self) -> str:
        """Return the stable archive key for this security."""
        if self.perma_ticker:
            return _normalize_security_id_part(self.perma_ticker)

        start = _normalize_security_id_part(self.start_date or "unknown-start")
        end = _normalize_security_id_part(self.end_date or "active")
        ticker = _normalize_security_id_part(self.ticker)
        return f"{ticker}_{start}_{end}"

    def to_reference_row(self) -> dict[str, str | None]:
        """Serialize this security for the raw reference mapping artifact."""
        return {
            "ticker": self.ticker,
            "perma_ticker": self.perma_ticker,
            "security_id": self.security_id,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "exchange": self.exchange,
            "asset_type": self.asset_type,
            "name": self.name,
        }


class TiingoSecurityMaster:
    """Resolve historical tickers to stable Tiingo security identities."""

    def __init__(self, securities: Sequence[TiingoSecurity]) -> None:
        """Index security rows by normalized ticker."""
        if not securities:
            raise ValueError("TiingoSecurityMaster requires at least one security")
        self._securities = tuple(securities)
        self._by_ticker: dict[str, list[TiingoSecurity]] = {}
        for security in self._securities:
            self._by_ticker.setdefault(security.ticker, []).append(security)

        for ticker, rows in self._by_ticker.items():
            self._by_ticker[ticker] = sorted(rows, key=_security_sort_key)

    @classmethod
    def from_rows(cls, rows: Iterable[Mapping[str, Any]]) -> TiingoSecurityMaster:
        """Build a security master from vendor-neutral mapping rows."""
        securities = [security_from_mapping(row) for row in rows]
        return cls(securities)

    @classmethod
    def from_supported_tickers_zip(cls, payload: bytes) -> TiingoSecurityMaster:
        """Build a security master from Tiingo's supported-tickers ZIP payload."""
        return cls.from_rows(_read_supported_tickers_zip(payload))

    @classmethod
    def fetch_supported_tickers(
        cls,
        session: requests.Session | None = None,
        url: str = SUPPORTED_TICKERS_ZIP_URL,
        timeout_seconds: int = 30,
    ) -> TiingoSecurityMaster:
        """Fetch and parse Tiingo's supported-tickers ZIP into a security master."""
        active_session = session or requests.Session()
        response = active_session.get(url, timeout=timeout_seconds)
        response.raise_for_status()
        return cls.from_supported_tickers_zip(response.content)

    def resolve(self, ticker: str, as_of_date: str | None = None) -> TiingoSecurity:
        """Resolve one ticker to the best matching Tiingo security row."""
        candidates = self.resolve_all(ticker)

        if as_of_date is not None:
            _validate_date(as_of_date, "as_of_date")
            dated = [security for security in candidates if _security_active_on(security, as_of_date)]
            if dated:
                return dated[-1]

        return candidates[-1]

    def resolve_all(self, ticker: str) -> list[TiingoSecurity]:
        """Resolve a ticker to every matching historical Tiingo security row."""
        normalized = _normalize_ticker(ticker)
        candidates = self._by_ticker.get(normalized)
        if not candidates:
            raise KeyError(f"Ticker {normalized} is not present in Tiingo security master")
        return list(candidates)

    def resolve_many(self, tickers: Iterable[str]) -> list[TiingoSecurity]:
        """Resolve many tickers while preserving sorted deterministic output."""
        resolved: set[TiingoSecurity] = set()
        for ticker in tickers:
            for security in self.resolve_all(ticker):
                resolved.add(security)
        return sorted(resolved, key=_security_reference_sort_key)

    def to_reference_rows(self, securities: Iterable[TiingoSecurity] | None = None) -> list[dict[str, str | None]]:
        """Serialize selected security rows for R2 reference storage."""
        source = self._securities if securities is None else tuple(securities)
        return [security.to_reference_row() for security in sorted(source, key=_security_reference_sort_key)]


def security_from_mapping(row: Mapping[str, Any]) -> TiingoSecurity:
    """Parse one Tiingo security mapping row into a normalized security object."""
    ticker = _coerce_required_text(_first_present(row, ("ticker", "symbol")), "ticker")
    return TiingoSecurity(
        ticker=_normalize_ticker(ticker),
        perma_ticker=_coerce_optional_text(
            _first_present(row, ("permaTicker", "perma_ticker", "permaticker", "permaTickerId"))
        ),
        start_date=_coerce_optional_date(_first_present(row, ("startDate", "start_date")), "startDate"),
        end_date=_coerce_optional_date(_first_present(row, ("endDate", "end_date")), "endDate"),
        exchange=_coerce_optional_text(_first_present(row, ("exchange", "exchangeCode"))),
        asset_type=_coerce_optional_text(_first_present(row, ("assetType", "asset_type"))),
        name=_coerce_optional_text(_first_present(row, ("name", "companyName", "description"))),
    )


def _read_supported_tickers_zip(payload: bytes) -> list[dict[str, str]]:
    """Read the first CSV file from Tiingo's supported-tickers ZIP payload."""
    with zipfile.ZipFile(io.BytesIO(payload)) as archive:
        csv_names = sorted(name for name in archive.namelist() if name.lower().endswith(".csv"))
        if not csv_names:
            raise ValueError("Tiingo supported-tickers ZIP did not contain a CSV file")
        with archive.open(csv_names[0]) as csv_file:
            text = io.TextIOWrapper(csv_file, encoding="utf-8", newline="")
            return list(csv.DictReader(text))


def _first_present(row: Mapping[str, Any], fields: tuple[str, ...]) -> Any | None:
    """Return the first present non-empty value from a row."""
    for field in fields:
        value = row.get(field)
        if value is not None and str(value).strip():
            return value
    return None


def _coerce_required_text(value: Any | None, field_name: str) -> str:
    """Coerce a required text field."""
    text = _coerce_optional_text(value)
    if text is None:
        raise ValueError(f"Missing required Tiingo security field: {field_name}")
    return text


def _coerce_optional_text(value: Any | None) -> str | None:
    """Coerce optional text while treating blank values as missing."""
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _coerce_optional_date(value: Any | None, field_name: str) -> str | None:
    """Coerce optional YYYY-MM-DD dates while treating active end dates as missing."""
    text = _coerce_optional_text(value)
    if text is None or text.lower() in _ACTIVE_END_DATE_VALUES:
        return None
    return _validate_date(text, field_name)


def _validate_date(value: str, field_name: str) -> str:
    """Validate and normalize a YYYY-MM-DD date string."""
    try:
        return Date.fromisoformat(value).isoformat()
    except ValueError as exc:
        raise ValueError(f"{field_name} must be YYYY-MM-DD: {value}") from exc


def _normalize_ticker(ticker: str) -> str:
    """Normalize ticker text for lookups."""
    normalized = ticker.strip().upper().replace(".", "-")
    if not normalized:
        raise ValueError("ticker cannot be empty")
    return normalized


def _normalize_security_id_part(value: str) -> str:
    """Normalize text for safe R2 key parts."""
    normalized = _SECURITY_ID_PART_PATTERN.sub("-", value.strip().upper()).strip("-")
    if not normalized:
        raise ValueError(f"security identifier part cannot be empty: {value!r}")
    return normalized


def _security_active_on(security: TiingoSecurity, as_of_date: str) -> bool:
    """Return True when a security row covers the given date."""
    starts_before = security.start_date is None or security.start_date <= as_of_date
    ends_after = security.end_date is None or as_of_date <= security.end_date
    return starts_before and ends_after


def _security_sort_key(security: TiingoSecurity) -> tuple[str, str, str]:
    """Sort older inactive rows before current rows for deterministic resolution."""
    return (
        security.start_date or "0000-00-00",
        security.end_date or "9999-99-99",
        security.security_id,
    )


def _security_reference_sort_key(security: TiingoSecurity) -> tuple[str, str, str, str]:
    """Sort security-reference rows without collapsing shared stable identities."""
    return (
        security.security_id,
        security.start_date or "0000-00-00",
        security.end_date or "9999-99-99",
        security.ticker,
    )
