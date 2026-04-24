from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from core.contracts.schemas import NewsSentimentRecord
from core.features.sentiment_features import (
    SENTIMENT_AGGREGATE_COLUMNS,
    SourceCredibilityConfig,
    aggregate_sentiment_by_ticker_day,
    load_source_credibility_config,
    sentiment_aggregates_to_records,
)


def _row(
    *,
    date: str = "2024-04-10",
    ticker: str = "AAPL",
    source: str = "Reuters",
    sentiment_positive: float = 0.8,
    sentiment_negative: float = 0.1,
    sentiment_neutral: float = 0.1,
    sentiment_score: float = 0.7,
    relevance_score: float = 1.0,
) -> dict[str, object]:
    """Build one scored article row."""
    return {
        "date": date,
        "ticker": ticker,
        "source": source,
        "sentiment_positive": sentiment_positive,
        "sentiment_negative": sentiment_negative,
        "sentiment_neutral": sentiment_neutral,
        "sentiment_score": sentiment_score,
        "relevance_score": relevance_score,
    }


def test_aggregate_sentiment_by_ticker_day_uses_source_credibility_weights() -> None:
    """Higher-credibility sources contribute more to ticker-day sentiment."""
    scored_news = pd.DataFrame(
        [
            _row(source="Reuters", sentiment_positive=0.9, sentiment_score=0.8),
            _row(source="Personal Blog", sentiment_positive=0.1, sentiment_score=-0.8),
        ]
    )
    config = SourceCredibilityConfig(
        default_source_weight=1.0,
        source_weights={"reuters": 3.0, "personal blog": 1.0},
    )

    aggregates = aggregate_sentiment_by_ticker_day(
        scored_news,
        credibility_config=config,
    )

    assert list(aggregates.columns) == list(SENTIMENT_AGGREGATE_COLUMNS)
    assert len(aggregates) == 1
    assert aggregates.loc[0, "sentiment_positive"] == pytest.approx(0.7)
    assert aggregates.loc[0, "sentiment_score"] == pytest.approx(0.4)


def test_aggregate_sentiment_by_ticker_day_multiplies_relevance_weight() -> None:
    """Article relevance scales the configured source credibility weight."""
    scored_news = pd.DataFrame(
        [
            _row(source="Reuters", sentiment_score=1.0, relevance_score=1.0),
            _row(source="Reuters", sentiment_score=-1.0, relevance_score=0.25),
        ]
    )
    config = SourceCredibilityConfig(
        default_source_weight=1.0,
        source_weights={"reuters": 2.0},
    )

    aggregates = aggregate_sentiment_by_ticker_day(
        scored_news,
        credibility_config=config,
    )

    assert aggregates.loc[0, "sentiment_score"] == pytest.approx(0.6)
    assert aggregates.loc[0, "relevance_score"] == pytest.approx(0.625)


def test_aggregate_sentiment_by_ticker_day_uses_default_weight_for_unknown_source() -> None:
    """Unknown sources use the configured default source credibility weight."""
    scored_news = pd.DataFrame(
        [
            _row(source="Reuters", sentiment_score=1.0),
            _row(source="Unknown Wire", sentiment_score=-1.0),
        ]
    )
    config = SourceCredibilityConfig(
        default_source_weight=0.5,
        source_weights={"reuters": 1.5},
    )

    aggregates = aggregate_sentiment_by_ticker_day(
        scored_news,
        credibility_config=config,
    )

    assert aggregates.loc[0, "sentiment_score"] == pytest.approx(0.5)


def test_aggregate_sentiment_by_ticker_day_groups_by_date_and_ticker() -> None:
    """Aggregation emits one canonical row per date and ticker."""
    scored_news = pd.DataFrame(
        [
            _row(date="2024-04-10", ticker="AAPL", sentiment_score=0.5),
            _row(date="2024-04-10", ticker="MSFT", sentiment_score=-0.5),
            _row(date="2024-04-11", ticker="AAPL", sentiment_score=0.2),
        ]
    )

    aggregates = aggregate_sentiment_by_ticker_day(
        scored_news,
        credibility_config=SourceCredibilityConfig(
            default_source_weight=1.0,
            source_weights={},
        ),
    )

    assert aggregates[["date", "ticker"]].to_dict(orient="records") == [
        {"date": "2024-04-10", "ticker": "AAPL"},
        {"date": "2024-04-10", "ticker": "MSFT"},
        {"date": "2024-04-11", "ticker": "AAPL"},
    ]


def test_aggregate_sentiment_by_ticker_day_empty_input_returns_canonical_frame() -> None:
    """Empty scored-news input returns a canonical empty aggregate frame."""
    scored_news = pd.DataFrame(columns=list(SENTIMENT_AGGREGATE_COLUMNS))

    aggregates = aggregate_sentiment_by_ticker_day(
        scored_news,
        credibility_config=SourceCredibilityConfig(
            default_source_weight=1.0,
            source_weights={},
        ),
    )

    assert len(aggregates) == 0
    assert list(aggregates.columns) == list(SENTIMENT_AGGREGATE_COLUMNS)


def test_aggregate_sentiment_by_ticker_day_rejects_missing_columns() -> None:
    """Missing contract-aligned columns fail closed."""
    scored_news = pd.DataFrame([{"date": "2024-04-10", "ticker": "AAPL"}])

    with pytest.raises(ValueError, match="sentiment_score"):
        aggregate_sentiment_by_ticker_day(
            scored_news,
            credibility_config=SourceCredibilityConfig(
                default_source_weight=1.0,
                source_weights={},
            ),
        )


def test_aggregate_sentiment_by_ticker_day_rejects_bad_probability() -> None:
    """FinBERT probability columns must stay inside the model probability range."""
    scored_news = pd.DataFrame([_row(sentiment_positive=1.2)])

    with pytest.raises(ValueError, match="sentiment_positive"):
        aggregate_sentiment_by_ticker_day(
            scored_news,
            credibility_config=SourceCredibilityConfig(
                default_source_weight=1.0,
                source_weights={},
            ),
        )


def test_aggregate_sentiment_by_ticker_day_handles_nan_relevance() -> None:
    """NaN relevance does not prevent source-weighted sentiment aggregation."""
    scored_news = pd.DataFrame([_row(relevance_score=float("nan"))])

    aggregates = aggregate_sentiment_by_ticker_day(
        scored_news,
        credibility_config=SourceCredibilityConfig(
            default_source_weight=1.0,
            source_weights={"reuters": 2.0},
        ),
    )

    assert aggregates.loc[0, "sentiment_score"] == pytest.approx(0.7)
    assert pd.isna(aggregates.loc[0, "relevance_score"])


def test_load_source_credibility_config_normalizes_source_keys(tmp_path: Path) -> None:
    """Config source names are normalized so runtime rows can vary casing."""
    config_path = tmp_path / "source_credibility.json"
    config_path.write_text(
        '{"default_source_weight": 1.0, "source_weights": {" Reuters ": 1.25}}'
    )

    config = load_source_credibility_config(config_path)

    assert config.default_source_weight == pytest.approx(1.0)
    assert config.source_weights == {"reuters": pytest.approx(1.25)}


def test_sentiment_aggregates_to_records_matches_contract() -> None:
    """Aggregate rows convert into NewsSentimentRecord without schema extras."""
    scored_news = pd.DataFrame([_row()])
    aggregates = aggregate_sentiment_by_ticker_day(
        scored_news,
        credibility_config=SourceCredibilityConfig(
            default_source_weight=1.0,
            source_weights={},
        ),
    )

    records = sentiment_aggregates_to_records(aggregates)

    assert len(records) == 1
    assert isinstance(records[0], NewsSentimentRecord)
    assert records[0].date == "2024-04-10"
    assert records[0].ticker == "AAPL"
    assert records[0].sentiment_score == pytest.approx(0.7)
