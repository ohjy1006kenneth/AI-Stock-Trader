from __future__ import annotations

import json
import os
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv
from loguru import logger

FRED_API_KEY_ENV = "FRED_API_KEY"
FRED_BASE_URL_ENV = "FRED_BASE_URL"
DEFAULT_FRED_BASE_URL = "https://api.stlouisfed.org/fred"
FRED_OBSERVATIONS_ENDPOINT = "/series/observations"
DEFAULT_FRED_PAGE_LIMIT = 1000
FRED_ENV_FILE = Path(__file__).resolve().parents[2] / "config" / "fred.env"
DEFAULT_FRED_CONFIG_PATH = Path(__file__).resolve().parents[2] / "config" / "fred_series.json"


@dataclass(frozen=True)
class FredClientConfig:
    """Configuration for FRED HTTP clients."""

    api_key: str
    base_url: str = DEFAULT_FRED_BASE_URL
    timeout_seconds: int = 30
    max_retries: int = 2
    retry_sleep_seconds: float = 1.0

    @classmethod
    def from_env(cls) -> FredClientConfig:
        """Build FRED config from environment variables or config/fred.env."""
        _load_local_fred_env_file()
        api_key = os.getenv(FRED_API_KEY_ENV)
        if not api_key:
            raise ValueError(f"Missing required FRED environment variable: {FRED_API_KEY_ENV}")
        return cls(
            api_key=api_key,
            base_url=os.getenv(FRED_BASE_URL_ENV) or DEFAULT_FRED_BASE_URL,
        )


@dataclass(frozen=True)
class FredSeriesSpec:
    """Configured FRED series metadata."""

    series_id: str
    description: str | None = None


@dataclass(frozen=True)
class FredArchiveConfig:
    """Configured FRED macro archive series and date range."""

    default_start_date: str
    default_end_date: str
    series: tuple[FredSeriesSpec, ...]

    @property
    def series_ids(self) -> tuple[str, ...]:
        """Return configured series IDs in deterministic order."""
        return tuple(series.series_id for series in self.series)


@dataclass(frozen=True)
class FredPage:
    """One page of normalized FRED observations."""

    rows: list[dict[str, Any]]
    offset: int
    limit: int


class FredMacroFetcher:
    """Fetch and normalize FRED macro/rate observations."""

    def __init__(
        self,
        config: FredClientConfig,
        session: requests.Session | None = None,
    ) -> None:
        """Store client configuration and HTTP session."""
        self.config = config
        self.session = session or requests.Session()

    def fetch_series_page(
        self,
        *,
        series_id: str,
        start_date: str,
        end_date: str,
        realtime_start: str | None = None,
        realtime_end: str | None = None,
        limit: int = DEFAULT_FRED_PAGE_LIMIT,
        offset: int = 0,
    ) -> FredPage:
        """Fetch one page of FRED observations for one series/date/realtime range."""
        normalized_series_id = _normalize_series_id(series_id)
        start = _validate_date(start_date, "start_date")
        end = _validate_date(end_date, "end_date")
        realtime_start_value = _validate_date(realtime_start or start, "realtime_start")
        realtime_end_value = _validate_date(realtime_end or end, "realtime_end")
        if start > end:
            raise ValueError("start_date must be <= end_date")
        if realtime_start_value > realtime_end_value:
            raise ValueError("realtime_start must be <= realtime_end")
        if limit <= 0:
            raise ValueError("limit must be positive")
        if offset < 0:
            raise ValueError("offset must be non-negative")

        payload = self._request_json(
            params={
                "series_id": normalized_series_id,
                "observation_start": start,
                "observation_end": end,
                "realtime_start": realtime_start_value,
                "realtime_end": realtime_end_value,
                "sort_order": "asc",
                "file_type": "json",
                "output_type": 1,
                "limit": limit,
                "offset": offset,
                "api_key": self.config.api_key,
            }
        )
        observations = _extract_observations(payload)
        rows = normalize_fred_observations(observations, series_id=normalized_series_id)
        return FredPage(rows=rows, offset=offset, limit=limit)

    def fetch_series_observations(
        self,
        *,
        series_id: str,
        start_date: str,
        end_date: str,
        realtime_start: str | None = None,
        realtime_end: str | None = None,
        limit: int = DEFAULT_FRED_PAGE_LIMIT,
        max_pages: int | None = None,
    ) -> list[dict[str, Any]]:
        """Fetch all observations for one FRED series, chunking the realtime range."""
        realtime_start_value = _validate_date(
            realtime_start or start_date, "realtime_start"
        )
        realtime_end_value = _validate_date(realtime_end or end_date, "realtime_end")
        rows: list[dict[str, Any]] = []
        seen: set[str] = set()

        for chunk_index, (chunk_start, chunk_end) in enumerate(
            _split_realtime_range(realtime_start_value, realtime_end_value)
        ):
            offset = 0
            pages = 0
            while True:
                try:
                    page = self.fetch_series_page(
                        series_id=series_id,
                        start_date=start_date,
                        end_date=end_date,
                        realtime_start=chunk_start,
                        realtime_end=chunk_end,
                        limit=limit,
                        offset=offset,
                    )
                except requests.HTTPError as exc:
                    if chunk_index == 0 and offset == 0 and _is_alfred_missing_error(exc):
                        logger.warning(
                            "FRED series {} missing from ALFRED; falling back to snapshot",
                            series_id,
                        )
                        return self._fetch_series_snapshot(
                            series_id=series_id,
                            start_date=start_date,
                            end_date=end_date,
                            limit=limit,
                            max_pages=max_pages,
                        )
                    raise
                if not page.rows:
                    break

                for row in page.rows:
                    key = _observation_key(row)
                    if key in seen:
                        continue
                    seen.add(key)
                    rows.append(row)

                if len(page.rows) < page.limit:
                    break

                offset += page.limit
                pages += 1
                if max_pages is not None and pages >= max_pages:
                    break

        return rows

    def _fetch_series_snapshot(
        self,
        *,
        series_id: str,
        start_date: str,
        end_date: str,
        limit: int,
        max_pages: int | None,
    ) -> list[dict[str, Any]]:
        """Fetch current-vintage observations for a series not tracked in ALFRED."""
        today = date.today().isoformat()
        offset = 0
        pages = 0
        rows: list[dict[str, Any]] = []
        seen: set[str] = set()
        while True:
            page = self.fetch_series_page(
                series_id=series_id,
                start_date=start_date,
                end_date=end_date,
                realtime_start=today,
                realtime_end=today,
                limit=limit,
                offset=offset,
            )
            if not page.rows:
                break
            for row in page.rows:
                key = _observation_key(row)
                if key in seen:
                    continue
                seen.add(key)
                rows.append(row)
            if len(page.rows) < page.limit:
                break
            offset += page.limit
            pages += 1
            if max_pages is not None and pages >= max_pages:
                break
        return rows

    def fetch_all_macro_observations(
        self,
        *,
        series_ids: Sequence[str],
        start_date: str,
        end_date: str,
        realtime_start: str | None = None,
        realtime_end: str | None = None,
        limit: int = DEFAULT_FRED_PAGE_LIMIT,
    ) -> list[dict[str, Any]]:
        """Fetch all configured FRED series observations for a date/realtime range."""
        normalized_series_ids = _normalize_series_ids(series_ids)
        rows: list[dict[str, Any]] = []
        for series_id in normalized_series_ids:
            rows.extend(
                self.fetch_series_observations(
                    series_id=series_id,
                    start_date=start_date,
                    end_date=end_date,
                    realtime_start=realtime_start,
                    realtime_end=realtime_end,
                    limit=limit,
                )
            )
        return rows

    def _request_json(self, params: Mapping[str, Any]) -> Any:
        """Request one FRED payload with bounded retries for transient failures."""
        url = f"{self.config.base_url.rstrip('/')}{FRED_OBSERVATIONS_ENDPOINT}"
        last_error: requests.RequestException | None = None
        for attempt in range(self.config.max_retries + 1):
            try:
                response = self.session.get(
                    url,
                    params=dict(params),
                    timeout=self.config.timeout_seconds,
                )
                response.raise_for_status()
                return response.json()
            except requests.RequestException as exc:
                last_error = exc
                if attempt >= self.config.max_retries or not _is_retryable_error(exc):
                    raise
                time.sleep(self.config.retry_sleep_seconds)
        if last_error is not None:
            raise last_error
        raise RuntimeError("FRED request failed without an exception")


def load_fred_archive_config(path: Path = DEFAULT_FRED_CONFIG_PATH) -> FredArchiveConfig:
    """Load configured FRED series IDs and default date range from JSON config."""
    payload = json.loads(path.read_text())
    if not isinstance(payload, Mapping):
        raise ValueError("FRED config must be a JSON object")

    start_date = _required_config_date(payload, "default_start_date")
    end_date = _required_config_end_date(payload)
    series_payload = payload.get("series")
    if not isinstance(series_payload, list) or not series_payload:
        raise ValueError("FRED config series must be a non-empty list")

    seen: set[str] = set()
    series: list[FredSeriesSpec] = []
    for index, item in enumerate(series_payload):
        if not isinstance(item, Mapping):
            raise ValueError(f"FRED config series item {index} must be an object")
        series_id_value = item.get("id")
        if not isinstance(series_id_value, str):
            raise TypeError(f"FRED config series item {index} id must be a string")
        series_id = _normalize_series_id(series_id_value)
        if series_id in seen:
            raise ValueError(f"Duplicate FRED series configured: {series_id}")
        seen.add(series_id)
        description = item.get("description")
        if description is not None and not isinstance(description, str):
            raise TypeError(f"FRED config series {series_id} description must be a string")
        series.append(FredSeriesSpec(series_id=series_id, description=description))

    return FredArchiveConfig(
        default_start_date=start_date,
        default_end_date=end_date,
        series=tuple(series),
    )


def normalize_fred_observations(
    observations: Sequence[Mapping[str, Any]],
    *,
    series_id: str,
    retrieved_at: datetime | None = None,
) -> list[dict[str, Any]]:
    """Normalize raw FRED observations while preserving the original payload."""
    normalized_series_id = _normalize_series_id(series_id)
    retrieval_time = (retrieved_at or datetime.now(UTC)).isoformat()
    normalized_rows: list[dict[str, Any]] = []

    for index, observation in enumerate(observations):
        if not isinstance(observation, Mapping):
            raise ValueError(
                f"FRED observation {index} must be an object, "
                f"got {type(observation).__name__}"
            )
        raw = dict(observation)
        observation_date = _required_date(raw, "date", "observation_date")
        realtime_start = _required_date(raw, "realtime_start", "realtime_start")
        realtime_end = _required_date(raw, "realtime_end", "realtime_end")
        value = _required_value(raw, "value", index)

        normalized_rows.append(
            {
                "source": "fred",
                "series_id": normalized_series_id,
                "observation_date": observation_date,
                "realtime_start": realtime_start,
                "realtime_end": realtime_end,
                "retrieved_at": retrieval_time,
                "value": value,
                "is_missing": value is None,
                "raw": raw,
            }
        )

    return normalized_rows


def _extract_observations(payload: Any) -> list[Mapping[str, Any]]:
    """Extract observation objects from a FRED JSON payload."""
    if not isinstance(payload, Mapping):
        raise ValueError("FRED response must be a JSON object")
    observations = payload.get("observations")
    if observations is None:
        raise ValueError("FRED response must contain observations")
    if not isinstance(observations, list):
        raise ValueError("FRED response observations must be a list")

    rows: list[Mapping[str, Any]] = []
    for index, observation in enumerate(observations):
        if not isinstance(observation, Mapping):
            raise ValueError(
                f"FRED observation {index} must be an object, "
                f"got {type(observation).__name__}"
            )
        rows.append(observation)
    return rows


def _required_config_date(payload: Mapping[str, Any], field_name: str) -> str:
    """Return a required config date in YYYY-MM-DD format."""
    value = payload.get(field_name)
    if not isinstance(value, str):
        raise TypeError(f"FRED config {field_name} must be a string")
    return _validate_date(value, field_name)


def _required_config_end_date(payload: Mapping[str, Any]) -> str:
    """Return a required config end date or the latest sentinel."""
    value = payload.get("default_end_date")
    if not isinstance(value, str):
        raise TypeError("FRED config default_end_date must be a string")
    stripped = value.strip().lower()
    if stripped == "latest":
        return stripped
    return _validate_date(stripped, "default_end_date")


def _normalize_series_ids(series_ids: Sequence[str]) -> tuple[str, ...]:
    """Validate and normalize a non-empty sequence of FRED series IDs."""
    if not series_ids:
        raise ValueError("series_ids must contain at least one series")
    return tuple(_normalize_series_id(series_id) for series_id in series_ids)


def _normalize_series_id(series_id: str) -> str:
    """Normalize one FRED series ID."""
    if not isinstance(series_id, str):
        raise TypeError("series_id must be a string")
    cleaned = series_id.strip().upper()
    if not cleaned:
        raise ValueError("series_id cannot be empty")
    return cleaned


def _required_date(row: Mapping[str, Any], field_name: str, output_name: str) -> str:
    """Return a required YYYY-MM-DD date from a FRED row."""
    value = row.get(field_name)
    if value is None:
        raise ValueError(f"Missing required FRED field: {output_name}")
    if not isinstance(value, str):
        raise TypeError(f"FRED field {output_name} must be a string date")
    return _validate_date(value, output_name)


def _required_value(row: Mapping[str, Any], field_name: str, index: int) -> float | None:
    """Return a required FRED value, preserving '.' as missing data."""
    value = row.get(field_name)
    if value is None:
        raise ValueError("Missing required FRED field: value")
    if not isinstance(value, str):
        raise TypeError("FRED field value must be a string")
    stripped = value.strip()
    if stripped in {"", "."}:
        return None
    try:
        return float(stripped)
    except ValueError as exc:
        raise ValueError(f"FRED observation {index} value must be numeric or '.': {value}") from exc


def _validate_date(value: str, field_name: str) -> str:
    """Validate and normalize a YYYY-MM-DD date string."""
    try:
        return date.fromisoformat(value.strip().split("T", maxsplit=1)[0]).isoformat()
    except ValueError as exc:
        raise ValueError(f"{field_name} must be YYYY-MM-DD: {value}") from exc


# FRED's observations endpoint caps a single request at 2000 vintage dates within
# the realtime window. For daily series (e.g. DGS10) one vintage is roughly one
# business day, so we chunk by ~4 calendar years to stay comfortably under the cap.
FRED_REALTIME_CHUNK_DAYS = 4 * 365


def _split_realtime_range(start: str, end: str) -> list[tuple[str, str]]:
    """Split [start, end] into sub-ranges small enough for FRED's vintage-date limit."""
    start_date = date.fromisoformat(start)
    end_date = date.fromisoformat(end)
    if start_date > end_date:
        raise ValueError("realtime_start must be <= realtime_end")
    chunks: list[tuple[str, str]] = []
    cursor = start_date
    while cursor <= end_date:
        chunk_end = min(
            end_date,
            date.fromordinal(cursor.toordinal() + FRED_REALTIME_CHUNK_DAYS - 1),
        )
        chunks.append((cursor.isoformat(), chunk_end.isoformat()))
        cursor = date.fromordinal(chunk_end.toordinal() + 1)
    return chunks


def _observation_key(row: Mapping[str, Any]) -> str:
    """Return a deterministic key for deduplicating normalized FRED observations."""
    return "|".join(
        [
            str(row.get("series_id") or ""),
            str(row.get("observation_date") or ""),
            str(row.get("realtime_start") or ""),
            str(row.get("realtime_end") or ""),
            json.dumps(row.get("raw") or {}, sort_keys=True, separators=(",", ":")),
        ]
    )


def _is_alfred_missing_error(error: requests.HTTPError) -> bool:
    """Return True when a 400 means the series lacks ALFRED vintage history."""
    response = getattr(error, "response", None)
    if response is None or response.status_code != 400:
        return False
    try:
        body = response.text or ""
    except Exception:
        return False
    return "does not exist in ALFRED" in body


def _is_retryable_error(error: requests.RequestException) -> bool:
    """Return True when a request error is likely transient."""
    response = getattr(error, "response", None)
    status_code = getattr(response, "status_code", None)
    return status_code in {429, 500, 502, 503, 504} or isinstance(
        error,
        (requests.ConnectionError, requests.Timeout),
    )


def _load_local_fred_env_file() -> None:
    """Load local FRED settings from config/fred.env when the file exists."""
    load_dotenv(dotenv_path=FRED_ENV_FILE, override=False)
