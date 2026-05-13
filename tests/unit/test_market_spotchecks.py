from __future__ import annotations

import io
import math
import statistics
from pathlib import Path

import pandas as pd
import pytest

from core.contracts.schemas import FeatureRecord
from core.features.market_spotchecks import build_market_feature_spot_checks
from services.r2.paths import raw_price_path
from tests.fixtures.layer1_audit_support import local_writer


def test_build_market_feature_spot_checks_recomputes_with_prior_bars_only(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Spot checks use only bars strictly before the feature date and emit formula cards."""
    writer = local_writer(tmp_path, monkeypatch)
    bars = _hand_computable_bars()
    writer.put_object(raw_price_path("AAPL"), _parquet_bytes(bars))

    feature_date = str(bars.iloc[27]["date"])
    expected_values = _expected_market_values(bars, feature_date=feature_date)
    record = FeatureRecord(
        date=feature_date,
        ticker="AAPL",
        features=expected_values,
    )

    checks, cards = build_market_feature_spot_checks(records=[record], writer=writer)

    assert len(checks) == 5
    assert len(cards) == 5
    assert all(check.status == "pass" for check in checks)

    by_name = {check.feature_name: check for check in checks}
    assert by_name["returns_1d"].expected_value == pytest.approx(expected_values["returns_1d"])
    assert by_name["returns_5d"].expected_value == pytest.approx(expected_values["returns_5d"])
    assert by_name["realized_vol_21d"].expected_value == pytest.approx(
        expected_values["realized_vol_21d"]
    )
    assert by_name["volume_ratio_20"].expected_value == pytest.approx(
        expected_values["volume_ratio_20"]
    )
    assert by_name["rsi_14"].expected_value == pytest.approx(expected_values["rsi_14"])
    assert by_name["returns_1d"].point_in_time_safe is True
    assert by_name["returns_1d"].source_window_end < feature_date
    assert by_name["returns_1d"].raw_inputs["window_dates"][-1] == by_name["returns_1d"].source_window_end
    assert str(bars.iloc[-1]["date"]) not in by_name["returns_1d"].raw_inputs["window_dates"]

    card_by_name = {card.feature_name: card for card in cards}
    assert "adj_close" in card_by_name["returns_1d"].calculation
    assert "sqrt(252)" in card_by_name["realized_vol_21d"].calculation
    assert "avg_gain_14 / avg_loss_14" in card_by_name["rsi_14"].calculation


def test_build_market_feature_spot_checks_warns_on_missing_raw_archive(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Missing raw OHLCV archives are surfaced as explicit WARN spot checks."""
    writer = local_writer(tmp_path, monkeypatch)
    record = FeatureRecord(
        date="2024-02-08",
        ticker="MSFT",
        features={"returns_1d": 0.01},
    )

    checks, cards = build_market_feature_spot_checks(
        records=[record],
        writer=writer,
        feature_names=("returns_1d",),
    )

    assert len(checks) == 1
    assert checks[0].status == "warn"
    assert checks[0].missing_reason == "Raw Layer 0 OHLCV archive is missing for this ticker."
    assert cards[0].status == "warn"
    assert cards[0].calculation == "Raw Layer 0 OHLCV archive is missing for this ticker."


def test_build_market_feature_spot_checks_fails_on_stored_mismatch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Stored values outside tolerance fail the spot-check comparison."""
    writer = local_writer(tmp_path, monkeypatch)
    bars = _hand_computable_bars()
    writer.put_object(raw_price_path("AAPL"), _parquet_bytes(bars))

    feature_date = str(bars.iloc[27]["date"])
    expected_values = _expected_market_values(bars, feature_date=feature_date)
    record = FeatureRecord(
        date=feature_date,
        ticker="AAPL",
        features={"returns_1d": float(expected_values["returns_1d"]) + 0.5},
    )

    checks, cards = build_market_feature_spot_checks(
        records=[record],
        writer=writer,
        feature_names=("returns_1d",),
    )

    assert checks[0].status == "fail"
    assert checks[0].absolute_difference is not None
    assert checks[0].absolute_difference > 0.1
    assert cards[0].status == "fail"
    assert "differs from recomputation" in cards[0].message


def _hand_computable_bars() -> pd.DataFrame:
    """Return a deterministic OHLCV history with extra future bars after the feature date."""
    dates = pd.bdate_range("2024-01-02", periods=32)
    rows: list[dict[str, object]] = []
    for index, date_value in enumerate(dates):
        close = 100.0 + index * 1.2 + (index % 3) * 0.35
        if index >= 30:
            close += 25.0 * (index - 29)
        volume = 1_000_000 + index * 25_000 + (index % 4) * 5_000
        rows.append(
            {
                "date": date_value.date().isoformat(),
                "ticker": "AAPL",
                "open": close - 0.4,
                "high": close + 0.8,
                "low": close - 0.9,
                "close": close,
                "adj_close": close,
                "volume": volume,
                "dollar_volume": close * volume,
            }
        )
    return pd.DataFrame(rows)


def _expected_market_values(bars: pd.DataFrame, *, feature_date: str) -> dict[str, float]:
    """Return manual deterministic spot-check values for one feature date."""
    row_index = int(bars.index[bars["date"] == feature_date][0])
    closes = [float(value) for value in bars["adj_close"].tolist()]
    volumes = [float(value) for value in bars["volume"].tolist()]

    returns_1d = closes[row_index - 1] / closes[row_index - 2] - 1.0
    returns_5d = closes[row_index - 1] / closes[row_index - 6] - 1.0

    vol_window = closes[row_index - 22 : row_index]
    daily_returns = [
        vol_window[index] / vol_window[index - 1] - 1.0 for index in range(1, len(vol_window))
    ]
    realized_vol_21d = statistics.stdev(daily_returns) * math.sqrt(252)

    volume_window = volumes[row_index - 20 : row_index]
    volume_ratio_20 = volume_window[-1] / statistics.fmean(volume_window)

    rsi_window = closes[row_index - 15 : row_index]
    deltas = [rsi_window[index] - rsi_window[index - 1] for index in range(1, len(rsi_window))]
    gains = [max(delta, 0.0) for delta in deltas]
    losses = [max(-delta, 0.0) for delta in deltas]
    avg_gain = statistics.fmean(gains)
    avg_loss = statistics.fmean(losses)
    rsi_14 = 100.0 if avg_loss == 0.0 else 100.0 - 100.0 / (1.0 + avg_gain / avg_loss)

    return {
        "returns_1d": returns_1d,
        "returns_5d": returns_5d,
        "realized_vol_21d": realized_vol_21d,
        "volume_ratio_20": volume_ratio_20,
        "rsi_14": rsi_14,
    }


def _parquet_bytes(frame: pd.DataFrame) -> bytes:
    """Serialize a DataFrame to Parquet bytes for the local mock R2 writer."""
    buffer = io.BytesIO()
    frame.to_parquet(buffer, index=False)
    return buffer.getvalue()
