"""Layer 1 sector/factor features derived from point-in-time archived inputs.

Sector membership is inferred from the latest fundamentals record whose
`availability_date` is strictly before each target date. The sector-to-ETF map
is loaded from repository config so computation logic stays data-driven.

Missing sector classifications, unmapped sector names, absent sector ETF price
histories, or insufficient same-sector peers resolve to null feature values
instead of provider calls or guessed defaults.
"""
from __future__ import annotations

import importlib
import json
import math
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

from core.contracts.schemas import FeatureRecord

if TYPE_CHECKING:
    import pandas as pd

DEFAULT_SECTOR_ETF_CONFIG_PATH = (
    Path(__file__).resolve().parents[2] / "config" / "sector_etf_mapping.json"
)

SECTOR_FEATURE_COLUMNS: tuple[str, ...] = (
    "sector_etf_ret",
    "stock_vs_sector",
    "sector_momentum",
    "sector_relative_strength",
)

_SECTOR_LOOKBACK_RETURNS_1D = 1
_SECTOR_LOOKBACK_MOMENTUM = 21
_SECTOR_LOOKBACK_RELATIVE_STRENGTH = 63
_WHITESPACE_RE = re.compile(r"\s+")


@dataclass(frozen=True)
class SectorEtfConfig:
    """Configurable sector normalization and ETF mapping."""

    sector_field_names: tuple[str, ...]
    sector_aliases: Mapping[str, str]
    sector_to_etf: Mapping[str, str]


def load_sector_etf_config(
    path: Path = DEFAULT_SECTOR_ETF_CONFIG_PATH,
) -> SectorEtfConfig:
    """Load the repository-owned sector-to-ETF mapping config."""
    payload = json.loads(path.read_text(encoding="utf-8"))
    field_names_raw = payload.get("sector_field_names", ())
    if not isinstance(field_names_raw, list) or not field_names_raw:
        raise ValueError("sector_field_names must be a non-empty list")
    field_names = tuple(_normalize_field_name(value) for value in field_names_raw)
    if any(not value for value in field_names):
        raise ValueError("sector_field_names cannot contain empty values")

    aliases_raw = payload.get("sector_aliases", {})
    if not isinstance(aliases_raw, dict):
        raise ValueError("sector_aliases must be an object")
    sector_aliases = {
        _normalize_sector_label(key): _normalize_sector_label(value)
        for key, value in aliases_raw.items()
    }
    if any(not key or not value for key, value in sector_aliases.items()):
        raise ValueError("sector_aliases cannot contain empty keys or values")

    sector_to_etf_raw = payload.get("sector_to_etf", {})
    if not isinstance(sector_to_etf_raw, dict) or not sector_to_etf_raw:
        raise ValueError("sector_to_etf must be a non-empty object")
    sector_to_etf = {
        _normalize_sector_label(key): _normalize_ticker(value)
        for key, value in sector_to_etf_raw.items()
    }
    if any(not key or not value for key, value in sector_to_etf.items()):
        raise ValueError("sector_to_etf cannot contain empty keys or values")

    return SectorEtfConfig(
        sector_field_names=field_names,
        sector_aliases=sector_aliases,
        sector_to_etf=sector_to_etf,
    )


def compute_sector_features(
    *,
    ohlcv_by_ticker: Mapping[str, pd.DataFrame],
    fundamentals_by_ticker: Mapping[str, pd.DataFrame],
    target_dates_by_ticker: Mapping[str, Sequence[str]] | None = None,
    sector_price_frames: Mapping[str, pd.DataFrame] | None = None,
    sector_config: SectorEtfConfig | None = None,
    min_peers_for_relative_strength: int = 2,
) -> dict[str, pd.DataFrame]:
    """Return sector/factor feature frames keyed by ticker.

    `sector_relative_strength` is a percentile rank of the ticker's point-in-
    time-safe 63-day return among same-sector peers present in the current
    compute scope for that date. When fewer than `min_peers_for_relative_strength`
    same-sector peers have usable data, the rank is left null.
    """
    pd = _require_pandas()
    if min_peers_for_relative_strength <= 0:
        raise ValueError("min_peers_for_relative_strength must be positive")

    config = sector_config or load_sector_etf_config()
    normalized_target_dates = _normalize_target_dates_by_ticker(
        ohlcv_by_ticker=ohlcv_by_ticker,
        target_dates_by_ticker=target_dates_by_ticker,
    )
    if not normalized_target_dates:
        return {}

    etf_features = _build_sector_etf_features(
        pd,
        sector_price_frames=sector_price_frames or {},
    )

    combined_frames: list[pd.DataFrame] = []
    for ticker, target_dates in sorted(normalized_target_dates.items()):
        bars = ohlcv_by_ticker.get(ticker)
        if bars is None:
            combined_frames.append(_empty_frame(pd, ticker=ticker))
            continue

        ticker_frame = pd.DataFrame({"date": list(target_dates)})
        ticker_frame["ticker"] = ticker
        ticker_frame = ticker_frame.merge(
            _point_in_time_sector_assignments(
                pd,
                fundamentals=fundamentals_by_ticker.get(ticker),
                target_dates=target_dates,
                sector_config=config,
            ),
            on="date",
            how="left",
        )
        ticker_frame = ticker_frame.merge(
            _stock_return_features(pd, bars),
            on="date",
            how="left",
        )
        ticker_frame = ticker_frame.merge(
            etf_features,
            left_on=["date", "_sector_etf_ticker"],
            right_on=["date", "_sector_etf_ticker"],
            how="left",
        )
        ticker_frame["stock_vs_sector"] = (
            ticker_frame["_stock_return_1d"] - ticker_frame["sector_etf_ret"]
        )
        combined_frames.append(ticker_frame)

    if not combined_frames:
        return {}

    combined = pd.concat(combined_frames, ignore_index=True)
    combined["sector_relative_strength"] = float("nan")

    eligible = combined[
        combined["_sector_key"].notna() & combined["_stock_return_63d"].notna()
    ]
    for _, group in eligible.groupby(["date", "_sector_key"], sort=True, dropna=False):
        if len(group.index) < min_peers_for_relative_strength:
            continue
        ranks = group["_stock_return_63d"].rank(method="average", pct=True)
        combined.loc[group.index, "sector_relative_strength"] = ranks.to_numpy()

    results: dict[str, pd.DataFrame] = {}
    for ticker, ticker_frame in combined.groupby("ticker", sort=True, dropna=False):
        results[str(ticker)] = (
            ticker_frame[["date", "ticker", "sector_etf_ret", "stock_vs_sector",
                          "sector_momentum", "sector_relative_strength"]]
            .sort_values("date")
            .reset_index(drop=True)
        )
    return results


def sector_features_to_records(features: pd.DataFrame) -> list[FeatureRecord]:
    """Convert a sector-features frame into FeatureRecord instances."""
    records: list[FeatureRecord] = []
    for row in features.to_dict(orient="records"):
        feature_values = {
            name: _normalize_feature_value(row.get(name)) for name in SECTOR_FEATURE_COLUMNS
        }
        records.append(
            FeatureRecord(
                date=str(row["date"]),
                ticker=str(row["ticker"]),
                features=feature_values,
            )
        )
    return records


def _normalize_target_dates_by_ticker(
    *,
    ohlcv_by_ticker: Mapping[str, pd.DataFrame],
    target_dates_by_ticker: Mapping[str, Sequence[str]] | None,
) -> dict[str, tuple[str, ...]]:
    """Return sorted unique target dates for each ticker in scope."""
    normalized: dict[str, tuple[str, ...]] = {}
    for ticker, bars in sorted(ohlcv_by_ticker.items()):
        if target_dates_by_ticker is None:
            if "date" not in bars.columns:
                raise ValueError("OHLCV frame must include a date column")
            dates = tuple(
                sorted({str(value) for value in bars["date"].astype(str).tolist()})
            )
        else:
            dates = tuple(
                sorted(
                    {
                        str(value).strip()
                        for value in target_dates_by_ticker.get(ticker, ())
                        if str(value).strip()
                    }
                )
            )
        normalized[ticker] = dates
    return normalized


def _build_sector_etf_features(
    pd: Any,
    *,
    sector_price_frames: Mapping[str, pd.DataFrame],
) -> pd.DataFrame:
    """Return one `(date, ETF)` lookup frame for sector ETF features."""
    frames: list[pd.DataFrame] = []
    for etf_ticker, bars in sorted(sector_price_frames.items()):
        if "date" not in bars.columns or "adj_close" not in bars.columns:
            raise ValueError("Sector ETF OHLCV frame must include date and adj_close columns")
        frame = (
            bars.sort_values("date")
            .drop_duplicates("date")
            .reset_index(drop=True)[["date", "adj_close"]]
            .copy()
        )
        adj_close = frame["adj_close"].astype(float)
        frame["_sector_etf_ticker"] = _normalize_ticker(etf_ticker)
        frame["sector_etf_ret"] = adj_close.pct_change(
            _SECTOR_LOOKBACK_RETURNS_1D,
            fill_method=None,
        ).shift(1)
        frame["sector_momentum"] = adj_close.pct_change(
            _SECTOR_LOOKBACK_MOMENTUM,
            fill_method=None,
        ).shift(1)
        frames.append(
            frame[["date", "_sector_etf_ticker", "sector_etf_ret", "sector_momentum"]]
        )
    if not frames:
        return pd.DataFrame(
            columns=["date", "_sector_etf_ticker", "sector_etf_ret", "sector_momentum"]
        )
    return pd.concat(frames, ignore_index=True)


def _point_in_time_sector_assignments(
    pd: Any,
    *,
    fundamentals: pd.DataFrame | None,
    target_dates: Sequence[str],
    sector_config: SectorEtfConfig,
) -> pd.DataFrame:
    """Return point-in-time sector assignments for the requested target dates."""
    if not target_dates:
        return pd.DataFrame(columns=["date", "_sector_key", "_sector_etf_ticker"])

    if fundamentals is None or len(fundamentals.index) == 0:
        return pd.DataFrame(
            {
                "date": list(target_dates),
                "_sector_key": [None] * len(target_dates),
                "_sector_etf_ticker": [None] * len(target_dates),
            }
        )

    required = {"availability_date", "raw_json"}
    missing = sorted(required - set(fundamentals.columns))
    if missing:
        raise ValueError(f"Fundamentals frame missing required columns: {missing}")

    assignments: list[tuple[str, str]] = []
    for _, row in fundamentals.sort_values("availability_date").iterrows():
        availability_date = str(row.get("availability_date", "")).strip()
        if not availability_date:
            continue
        sector_key = _extract_sector_key(row, sector_config)
        if sector_key is None:
            continue
        assignments.append((availability_date, sector_key))

    rows: list[dict[str, Any]] = []
    latest_sector: str | None = None
    assignment_index = 0
    sorted_assignments = sorted(assignments, key=lambda item: item[0])
    for target_date in target_dates:
        while (
            assignment_index < len(sorted_assignments)
            and sorted_assignments[assignment_index][0] < target_date
        ):
            latest_sector = sorted_assignments[assignment_index][1]
            assignment_index += 1
        sector_etf_ticker = (
            sector_config.sector_to_etf.get(latest_sector) if latest_sector is not None else None
        )
        rows.append(
            {
                "date": target_date,
                "_sector_key": latest_sector,
                "_sector_etf_ticker": sector_etf_ticker,
            }
        )
    return pd.DataFrame(rows)


def _extract_sector_key(
    row: Any,
    sector_config: SectorEtfConfig,
) -> str | None:
    """Return the normalized sector key for one fundamentals row, when present."""
    raw_mapping = _decode_raw_json(row.get("raw_json"))
    direct_mapping = {
        key: row.get(key) for key in getattr(row, "index", ()) if isinstance(key, str)
    }
    for field_name in sector_config.sector_field_names:
        direct_value = direct_mapping.get(field_name)
        normalized = _normalize_sector_value(direct_value, sector_config)
        if normalized is not None:
            return normalized

        if raw_mapping is None:
            continue
        for key, value in raw_mapping.items():
            if _normalize_field_name(key) != field_name:
                continue
            normalized = _normalize_sector_value(value, sector_config)
            if normalized is not None:
                return normalized
    return None


def _stock_return_features(pd: Any, bars: pd.DataFrame) -> pd.DataFrame:
    """Return the point-in-time-safe stock return lookups used by sector features."""
    if "date" not in bars.columns or "adj_close" not in bars.columns:
        raise ValueError("OHLCV frame must include date and adj_close columns")
    frame = (
        bars.sort_values("date")
        .drop_duplicates("date")
        .reset_index(drop=True)[["date", "adj_close"]]
        .copy()
    )
    adj_close = frame["adj_close"].astype(float)
    frame["_stock_return_1d"] = adj_close.pct_change(
        _SECTOR_LOOKBACK_RETURNS_1D,
        fill_method=None,
    ).shift(1)
    frame["_stock_return_63d"] = (
        adj_close.pct_change(
            _SECTOR_LOOKBACK_RELATIVE_STRENGTH,
            fill_method=None,
        ).shift(1)
    )
    return frame[["date", "_stock_return_1d", "_stock_return_63d"]]


def _empty_frame(pd: Any, *, ticker: str) -> pd.DataFrame:
    """Return an empty feature frame with canonical sector columns."""
    return pd.DataFrame(columns=["date", "ticker", *SECTOR_FEATURE_COLUMNS]).assign(
        ticker=ticker
    )[["date", "ticker", *SECTOR_FEATURE_COLUMNS]]


def _normalize_field_name(value: Any) -> str:
    """Return a normalized sector-field identifier."""
    text = str(value).strip()
    return text if text else ""


def _normalize_sector_value(
    value: Any,
    sector_config: SectorEtfConfig,
) -> str | None:
    """Return the config-normalized sector key when the raw value is usable."""
    normalized = _normalize_sector_label(value)
    if not normalized:
        return None
    normalized = sector_config.sector_aliases.get(normalized, normalized)
    if normalized not in sector_config.sector_to_etf:
        return None
    return normalized


def _normalize_sector_label(value: Any) -> str:
    """Normalize raw sector text for config lookups."""
    if value is None:
        return ""
    text = str(value).strip().lower().replace("&", "and")
    return _WHITESPACE_RE.sub(" ", text)


def _normalize_ticker(value: Any) -> str:
    """Return an uppercase ticker string or raise on empties."""
    text = str(value).strip().upper()
    if not text:
        raise ValueError("ticker values cannot be empty")
    return text


def _decode_raw_json(raw_json: Any) -> dict[str, Any] | None:
    """Parse the archived fundamentals raw_json field into a mapping."""
    if raw_json is None:
        return None
    if isinstance(raw_json, Mapping):
        return dict(raw_json)
    if not isinstance(raw_json, str):
        return None
    stripped = raw_json.strip()
    if not stripped:
        return None
    try:
        parsed = json.loads(stripped)
    except json.JSONDecodeError:
        return None
    return parsed if isinstance(parsed, dict) else None


def _normalize_feature_value(value: Any) -> float | int | bool | None:
    """Convert a pandas/numpy scalar to a FeatureRecord-compatible primitive."""
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(numeric) or math.isinf(numeric):
        return None
    if isinstance(value, int) and not isinstance(value, bool):
        return int(value)
    return numeric


def _require_pandas() -> Any:
    """Import pandas/pyarrow lazily with a clear dependency error."""
    try:
        import pandas as pd

        importlib.import_module("pyarrow")
    except ModuleNotFoundError as exc:
        raise ModuleNotFoundError(
            "pandas and pyarrow are required for sector feature computation."
        ) from exc
    return pd
