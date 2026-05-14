from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import date as Date

MACRO_SNAPSHOT_DATE_COLUMN = "snapshot_date"


def annotate_macro_snapshot_rows(
    rows: Sequence[Mapping[str, object]],
    *,
    snapshot_date: str,
) -> list[dict[str, object]]:
    """Stamp one snapshot date onto every macro archive row."""
    normalized_snapshot_date = _normalize_iso_date(snapshot_date)
    return [
        dict(row, **{MACRO_SNAPSHOT_DATE_COLUMN: normalized_snapshot_date})
        for row in rows
    ]


def build_latest_available_macro_snapshot(
    rows: Sequence[Mapping[str, object]],
    *,
    snapshot_date: str,
    series_ids: Sequence[str] | None = None,
) -> list[dict[str, object]]:
    """Return the latest point-in-time-safe row per series for one snapshot date."""
    normalized_snapshot_date = _normalize_iso_date(snapshot_date)
    requested_series = _normalize_series_ids(series_ids)
    latest_by_series: dict[str, dict[str, object]] = {}
    for row in rows:
        series_id = _normalize_series_id(row.get("series_id"))
        if series_id is None:
            continue
        if requested_series and series_id not in requested_series:
            continue
        if not _row_is_available_as_of(row, normalized_snapshot_date):
            continue
        candidate = dict(row)
        candidate[MACRO_SNAPSHOT_DATE_COLUMN] = normalized_snapshot_date
        current = latest_by_series.get(series_id)
        if current is None or _row_priority(candidate) > _row_priority(current):
            latest_by_series[series_id] = candidate
    return sorted(latest_by_series.values(), key=_row_sort_key)


def macro_snapshot_is_usable(
    rows: Sequence[Mapping[str, object]],
    *,
    snapshot_date: str,
    required_series_ids: Sequence[str] | None = None,
) -> bool:
    """Return True when one stored macro shard is already a usable run-date snapshot."""
    normalized_snapshot_date = _normalize_iso_date(snapshot_date)
    requested_series = _normalize_series_ids(required_series_ids)
    present_series: set[str] = set()
    for row in rows:
        explicit_snapshot_date = row.get(MACRO_SNAPSHOT_DATE_COLUMN)
        if explicit_snapshot_date is not None:
            try:
                if _normalize_iso_date(str(explicit_snapshot_date)) != normalized_snapshot_date:
                    return False
            except ValueError:
                return False

        series_id = _normalize_series_id(row.get("series_id"))
        if series_id is None:
            return False
        if series_id in present_series:
            return False
        if not _row_is_available_as_of(row, normalized_snapshot_date):
            return False
        present_series.add(series_id)

    if not present_series:
        return False
    return not requested_series or requested_series.issubset(present_series)


def _row_is_available_as_of(row: Mapping[str, object], snapshot_date: str) -> bool:
    """Return True when a macro row was public on or before the snapshot date."""
    observation_date = _coerce_iso_date(row.get("observation_date"))
    realtime_start = _coerce_iso_date(row.get("realtime_start"))
    if observation_date is None or realtime_start is None:
        return False
    return observation_date <= snapshot_date and realtime_start <= snapshot_date


def _row_priority(row: Mapping[str, object]) -> tuple[str, str, str, str]:
    """Return the ordering used to select the latest available row per series."""
    return (
        _coerce_iso_date(row.get("observation_date")) or "",
        _coerce_iso_date(row.get("realtime_start")) or "",
        _coerce_iso_date(row.get("realtime_end")) or "",
        str(row.get("retrieved_at") or ""),
    )


def _row_sort_key(row: Mapping[str, object]) -> tuple[str, str, str, str, str]:
    """Return the deterministic serialization order for snapshot rows."""
    return (
        str(row.get("series_id") or ""),
        _coerce_iso_date(row.get("observation_date")) or "",
        _coerce_iso_date(row.get("realtime_start")) or "",
        _coerce_iso_date(row.get("realtime_end")) or "",
        str(row.get("retrieved_at") or ""),
    )


def _normalize_series_ids(series_ids: Sequence[str] | None) -> set[str]:
    if not series_ids:
        return set()
    return {
        normalized
        for series_id in series_ids
        if (normalized := _normalize_series_id(series_id)) is not None
    }


def _normalize_series_id(value: object) -> str | None:
    text = str(value or "").strip().upper()
    return text or None


def _normalize_iso_date(value: str) -> str:
    return Date.fromisoformat(value.strip()).isoformat()


def _coerce_iso_date(value: object) -> str | None:
    text = str(value or "").strip()
    if not text:
        return None
    candidate = text.split("T", maxsplit=1)[0].split(" ", maxsplit=1)[0]
    try:
        return Date.fromisoformat(candidate).isoformat()
    except ValueError:
        return None
