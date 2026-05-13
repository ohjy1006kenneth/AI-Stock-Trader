from __future__ import annotations

from core.contracts.schemas import FeatureRecord
from core.features.io import feature_records_to_parquet_bytes
from services.r2.paths import layer1_ticker_history_path
from services.r2.writer import R2Writer
from tests.fixtures.layer1_audit_support import seed_layer1_audit_fixture


def seed_layer1_dashboard_fixture(writer: R2Writer) -> dict[str, object]:
    """Seed multi-row Layer 1 histories for dashboard backend tests."""
    audit_fixture = seed_layer1_audit_fixture(writer)
    base_features = dict(audit_fixture["history_record"].features)

    aapl_records = [
        _record("2024-05-06", "AAPL", base_features, returns_1d=0.010),
        _record(
            "2024-05-07",
            "AAPL",
            base_features,
            returns_1d=0.011,
            nlp_sentiment_score=None,
        ),
        _record("2024-05-08", "AAPL", base_features, returns_1d=0.750),
    ]
    msft_records = [
        _record("2024-05-06", "MSFT", base_features, returns_1d=0.012, rsi_14=120.0),
        _record(
            "2024-05-07",
            "MSFT",
            {name: value for name, value in base_features.items() if name != "beta_60d"},
            returns_1d=0.013,
        ),
        _record("2024-05-08", "MSFT", base_features, returns_1d=0.014),
    ]

    writer.put_object(
        layer1_ticker_history_path("AAPL"),
        feature_records_to_parquet_bytes(aapl_records),
    )
    writer.put_object(
        layer1_ticker_history_path("MSFT"),
        feature_records_to_parquet_bytes(msft_records),
    )
    return {
        "from_date": "2024-05-06",
        "to_date": "2024-05-08",
        "tickers": ("AAPL", "MSFT"),
    }


def _record(
    date: str,
    ticker: str,
    features: dict[str, float | int | str | bool | None],
    **overrides: float | int | str | bool | None,
) -> FeatureRecord:
    """Return one FeatureRecord using a shared baseline feature dictionary."""
    return FeatureRecord(
        date=date,
        ticker=ticker,
        features={
            **features,
            **overrides,
        },
    )
