from __future__ import annotations

import json
from collections.abc import Sequence
from pathlib import Path

import pandas as pd
import pytest

from core.contracts.schemas import NewsSentimentRecord
from core.features.news_preprocessing import records_to_news_sentiment_frame
from core.features.sentiment_features import (
    SENTIMENT_AGGREGATE_COLUMNS,
    SentimentScore,
    SourceCredibilityConfig,
    aggregate_sentiment_by_ticker_day,
    load_source_credibility_config,
    score_news_sentiment,
    sentiment_aggregates_to_records,
    sentiment_feature_records_from_scored_news,
    sentiment_feature_records_to_frame,
)


def _row(
    *,
    date: str = "2024-04-10",
    ticker: str = "AAPL",
    article_id: str = "article-1",
    sentence_index: int = 0,
    chunk_index: int = 0,
    source: str | None = "Reuters",
    published_at: str | None = None,
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
        "article_id": article_id,
        "sentence_index": sentence_index,
        "chunk_index": chunk_index,
        "source": source,
        "published_at": published_at or f"{date}T14:30:00Z",
        "sentiment_positive": sentiment_positive,
        "sentiment_negative": sentiment_negative,
        "sentiment_neutral": sentiment_neutral,
        "sentiment_score": sentiment_score,
        "relevance_score": relevance_score,
    }


def _relevance_gate_row(
    *,
    date: str = "2024-04-10",
    ticker: str = "AAPL",
    article_id: str = "article-1",
    sentence_index: int = 0,
    chunk_index: int = 0,
    relevance_decision: str = "accepted",
    relevance_score: float = 1.0,
    ticker_relevance_score: float = 1.0,
    financial_relevance_score: float = 1.0,
    topic_relevance_score: float = 1.0,
    **extra: object,
) -> dict[str, object]:
    """Build one relevance-gate audit row."""
    row = {
        "date": date,
        "ticker": ticker,
        "article_id": article_id,
        "sentence_index": sentence_index,
        "chunk_index": chunk_index,
        "relevance_decision": relevance_decision,
        "relevance_score": relevance_score,
        "ticker_relevance_score": ticker_relevance_score,
        "financial_relevance_score": financial_relevance_score,
        "topic_relevance_score": topic_relevance_score,
        "reason_codes": json.dumps(["legacy_relevance_evidence"], sort_keys=True),
    }
    row.update(extra)
    return row


class _FakeScorer:
    """Deterministic test scorer."""

    def score(self, texts: Sequence[str]) -> Sequence[SentimentScore]:
        """Return positive scores for bullish text and negative otherwise."""
        scores: list[SentimentScore] = []
        for text in texts:
            if "beats" in text.lower():
                scores.append(SentimentScore(positive=0.8, negative=0.1, neutral=0.1))
            else:
                scores.append(SentimentScore(positive=0.2, negative=0.6, neutral=0.2))
        return scores


class _BadScorer:
    """Test scorer that returns the wrong output cardinality."""

    def score(self, texts: Sequence[str]) -> Sequence[SentimentScore]:
        """Return too few scores."""
        return []


def test_score_news_sentiment_distinguishes_missing_and_explicit_relevance() -> None:
    """Preprocessed news records keep explicit relevance separate from missing fallback."""
    records = [
        NewsSentimentRecord(
            date="2024-04-10",
            ticker="AAPL",
            text="Apple beats expectations.",
            article_id="a1",
            sentence_index=0,
            source="Reuters",
        ),
        NewsSentimentRecord(
            date="2024-04-10",
            ticker="MSFT",
            text="Microsoft misses expectations.",
            article_id="a2",
            sentence_index=0,
            source="Reuters",
            relevance_score=0.25,
        ),
        NewsSentimentRecord(
            date="2024-04-10",
            ticker="SPY",
            text="S&P 500 rises as Fed holds rates.",
            article_id="a3",
            sentence_index=0,
            source="Reuters",
            relevance_score=0.9,
        ),
    ]

    scored = score_news_sentiment(records, scorer=_FakeScorer(), batch_size=1)

    assert [record.ticker for record in scored] == ["AAPL", "MSFT", "SPY"]
    assert scored[0].sentiment_positive == pytest.approx(0.8)
    assert scored[0].sentiment_score == pytest.approx(0.7)
    assert scored[1].sentiment_negative == pytest.approx(0.6)
    assert scored[1].sentiment_score == pytest.approx(-0.4)
    assert scored[0].relevance_score == pytest.approx(1.0)
    assert scored[1].relevance_score == pytest.approx(0.25)
    assert scored[2].relevance_score == pytest.approx(0.9)

    aggregates = sentiment_feature_records_from_scored_news(
        records_to_news_sentiment_frame(scored),
        credibility_config=SourceCredibilityConfig(
            default_source_weight=1.0,
            source_weights={},
        ),
    )
    by_ticker = {record.ticker: record for record in aggregates}
    assert by_ticker["AAPL"].features["nlp_relevance_score"] == pytest.approx(1.0)
    assert by_ticker["AAPL"].features["nlp_sentiment_score"] == pytest.approx(0.7)
    assert by_ticker["MSFT"].features["nlp_relevance_score"] == pytest.approx(0.25)
    assert by_ticker["SPY"].features["nlp_relevance_score"] == pytest.approx(0.9)
    assert json.loads(str(by_ticker["AAPL"].features["nlp_semantic_warning_codes"])) == [
        "missing_relevance_evidence",
        "missing_topic_artifact",
    ]


def test_score_news_sentiment_requires_explicit_relevance_evidence() -> None:
    """Direct ticker/entity evidence gets full weight while generic context stays conservative."""
    records = [
        NewsSentimentRecord(
            date="2024-04-10",
            ticker="AAPL",
            headline="Apple says AAPL revenue improved.",
            text="Apple says AAPL revenue improved.",
            article_id="direct-aapl",
            sentence_index=0,
            source="Reuters",
        ),
        NewsSentimentRecord(
            date="2024-04-10",
            ticker="AAPL",
            headline="Intel and Snap rally as ETF inflows lift tech.",
            text="Intel and Snap rally as ETF inflows lift tech.",
            article_id="context-aapl",
            sentence_index=0,
            source="Reuters",
        ),
        NewsSentimentRecord(
            date="2024-04-10",
            ticker="MSFT",
            headline="AI and tech stocks rally on market hopes.",
            text="AI and tech stocks rally on market hopes.",
            article_id="context-msft",
            sentence_index=0,
            source="Reuters",
        ),
        NewsSentimentRecord(
            date="2024-04-10",
            ticker="NVDA",
            headline="Weekend weather update.",
            text="Weekend weather update.",
            article_id="noise-nvda",
            sentence_index=0,
            source="Reuters",
        ),
    ]

    scored = score_news_sentiment(records, scorer=_FakeScorer(), batch_size=2)
    relevance_by_article = {record.article_id: record.relevance_score for record in scored}

    assert relevance_by_article["direct-aapl"] == pytest.approx(1.0)
    assert relevance_by_article["context-aapl"] == pytest.approx(0.25)
    assert relevance_by_article["context-msft"] == pytest.approx(0.25)
    assert relevance_by_article["noise-nvda"] is None


def test_sentiment_feature_records_accept_legacy_relevance_gate_without_optional_target_fields() -> None:
    """Legacy relevance-gate rows should still attach and leave optional target fields empty."""
    scored_news = pd.DataFrame([
        _row(article_id="legacy-aapl", sentence_index=0, relevance_score=0.75),
    ])
    relevance_gate = pd.DataFrame(
        [
            _relevance_gate_row(
                article_id="legacy-aapl",
                relevance_decision="accepted",
                relevance_score=0.75,
                ticker_relevance_score=1.0,
                financial_relevance_score=0.8,
                topic_relevance_score=0.6,
                reason_codes=json.dumps(["legacy_relevance_evidence"], sort_keys=True),
            )
        ]
    )

    records = sentiment_feature_records_from_scored_news(
        scored_news,
        relevance_gate=relevance_gate,
        credibility_config=SourceCredibilityConfig(
            default_source_weight=1.0,
            source_weights={},
        ),
    )

    assert len(records) == 1
    features = records[0].features
    assert features["nlp_relevance_category"] is None
    assert features["nlp_target_impact_direction"] is None
    assert features["nlp_target_impact_magnitude"] is None
    assert features["nlp_target_impact_horizon"] is None
    assert features["nlp_target_impact_confidence"] is None
    assert features["nlp_causal_channel"] is None
    assert features["nlp_document_sentiment"] is None
    assert features["nlp_article_contamination_ratio"] is None
    assert features["nlp_article_contamination_count"] is None
    assert features["nlp_article_signal_count"] is None
    assert features["nlp_article_contribution_weight"] is None


def test_sentiment_feature_records_propagate_optional_target_impact_fields() -> None:
    """Optional relevance-gate target-impact fields should flow into sentiment features."""
    scored_news = pd.DataFrame([
        _row(article_id="target-aapl", sentence_index=0, relevance_score=0.9),
    ])
    relevance_gate = pd.DataFrame(
        [
            _relevance_gate_row(
                article_id="target-aapl",
                relevance_decision="accepted",
                relevance_score=0.9,
                ticker_relevance_score=1.0,
                financial_relevance_score=0.85,
                topic_relevance_score=0.7,
                relevance_category="direct_target_event",
                target_context_score=0.88,
                document_sentiment="bullish",
                target_company_impact_direction="positive",
                target_company_impact_magnitude="high",
                impact_horizon="short_term",
                causal_channel="product_device",
                target_impact_confidence=0.93,
                article_contamination_ratio=0.12,
                article_contamination_count=1,
                article_signal_count=3,
                article_contribution_weight=0.6,
                reason_codes=json.dumps(
                    ["legacy_relevance_evidence", "article_contribution_capped"],
                    sort_keys=True,
                ),
            )
        ]
    )

    records = sentiment_feature_records_from_scored_news(
        scored_news,
        relevance_gate=relevance_gate,
        credibility_config=SourceCredibilityConfig(
            default_source_weight=1.0,
            source_weights={},
        ),
    )

    assert len(records) == 1
    features = records[0].features
    assert features["nlp_relevance_category"] == "direct_target_event"
    assert features["nlp_target_impact_direction"] == "positive"
    assert features["nlp_target_impact_magnitude"] == "high"
    assert features["nlp_target_impact_horizon"] == "short_term"
    assert features["nlp_target_impact_confidence"] == 0.93
    assert features["nlp_causal_channel"] == "product_device"
    assert features["nlp_document_sentiment"] == "bullish"
    assert features["nlp_article_contamination_ratio"] == 0.12
    assert features["nlp_article_contamination_count"] == 1
    assert features["nlp_article_signal_count"] == 3
    assert features["nlp_article_contribution_weight"] == 0.6
    assert "article_contribution_capped" in json.loads(str(features["nlp_relevance_reason_codes"]))


def test_score_news_sentiment_skips_rows_without_text_or_headline() -> None:
    """Rows that cannot be scored are omitted from the scored output."""
    records = [NewsSentimentRecord(date="2024-04-10", ticker="AAPL")]

    assert score_news_sentiment(records, scorer=_FakeScorer()) == []


def test_score_news_sentiment_rejects_scorer_cardinality_mismatch() -> None:
    """Model providers must return exactly one score per input text."""
    records = [NewsSentimentRecord(date="2024-04-10", ticker="AAPL", text="beats")]

    with pytest.raises(ValueError, match="wrong number"):
        score_news_sentiment(records, scorer=_BadScorer())


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


def test_aggregate_sentiment_by_ticker_day_excludes_contamination_from_direct_weight() -> None:
    """Direct ticker sentiment only uses explicit evidence and downweights context."""
    scored_news = pd.DataFrame(
        [
            _row(
                article_id="direct",
                source="Reuters",
                sentiment_positive=1.0,
                sentiment_negative=0.0,
                sentiment_neutral=0.0,
                sentiment_score=1.0,
                relevance_score=1.0,
            ),
            _row(
                article_id="context",
                source="Reuters",
                sentiment_positive=0.0,
                sentiment_negative=1.0,
                sentiment_neutral=0.0,
                sentiment_score=-1.0,
                relevance_score=0.25,
            ),
            _row(
                article_id="noise",
                source="Reuters",
                sentiment_positive=0.0,
                sentiment_negative=1.0,
                sentiment_neutral=0.0,
                sentiment_score=-1.0,
                relevance_score=float("nan"),
            ),
        ]
    )

    records = sentiment_feature_records_from_scored_news(
        scored_news,
        credibility_config=SourceCredibilityConfig(
            default_source_weight=1.0,
            source_weights={"reuters": 1.0},
        ),
    )

    assert len(records) == 1
    assert records[0].ticker == "AAPL"
    assert records[0].features["nlp_sentence_count"] == 3
    assert records[0].features["nlp_sentiment_score"] == pytest.approx(0.6)
    assert records[0].features["nlp_relevance_score"] == pytest.approx(0.625)


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


def test_aggregate_sentiment_by_ticker_day_buckets_by_configured_timezone() -> None:
    """Published timestamps are bucketed in the configured trading timezone."""
    scored_news = pd.DataFrame(
        [
            _row(
                date="2024-04-11",
                published_at="2024-04-11T02:30:00Z",
                sentiment_score=0.4,
            ),
            _row(
                date="2024-04-10",
                published_at="2024-04-10T15:00:00Z",
                sentiment_score=0.2,
            ),
        ]
    )

    aggregates = aggregate_sentiment_by_ticker_day(
        scored_news,
        credibility_config=SourceCredibilityConfig(
            default_source_weight=1.0,
            source_weights={},
        ),
        bucket_timezone="America/New_York",
    )

    assert aggregates[["date", "ticker"]].to_dict(orient="records") == [
        {"date": "2024-04-10", "ticker": "AAPL"}
    ]
    assert aggregates.loc[0, "sentiment_score"] == pytest.approx(0.3)


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
    """NaN relevance is treated as non-direct evidence, not full-weight support."""
    scored_news = pd.DataFrame([_row(relevance_score=float("nan"))])

    aggregates = aggregate_sentiment_by_ticker_day(
        scored_news,
        credibility_config=SourceCredibilityConfig(
            default_source_weight=1.0,
            source_weights={"reuters": 2.0},
        ),
    )

    assert pd.isna(aggregates.loc[0, "sentiment_score"])
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


def test_sentiment_feature_records_from_scored_news_matches_contract() -> None:
    """Scored news rows aggregate into ticker-day FeatureRecord rows."""
    scored_news = pd.DataFrame(
        [
            _row(article_id="a1", sentiment_score=0.7, sentiment_positive=0.8),
            _row(article_id="a1", sentiment_score=0.5, sentiment_positive=0.7),
            _row(article_id="a2", sentiment_score=-0.2, sentiment_positive=0.2),
        ]
    )

    records = sentiment_feature_records_from_scored_news(
        scored_news,
        credibility_config=SourceCredibilityConfig(
            default_source_weight=1.0,
            source_weights={},
        ),
    )

    assert len(records) == 1
    assert records[0].date == "2024-04-10"
    assert records[0].ticker == "AAPL"
    assert records[0].features["nlp_article_count"] == 2
    assert records[0].features["nlp_sentence_count"] == 3
    assert records[0].features["nlp_sentiment_score"] == pytest.approx(1.0 / 3.0)
    assert json.loads(records[0].features["nlp_semantic_warning_codes"]) == [
        "missing_relevance_evidence",
        "missing_topic_artifact",
    ]


def test_sentiment_feature_records_combine_topics_relevance_and_source_weights() -> None:
    """Semantic aggregation combines FinBERT, topics, relevance, and source weights."""
    scored_news = pd.DataFrame(
        [
            _row(
                article_id="aapl-specific",
                source="Reuters",
                sentiment_positive=0.9,
                sentiment_negative=0.05,
                sentiment_neutral=0.05,
                sentiment_score=0.85,
                relevance_score=0.9,
            )
            | {"sentence_index": 0, "chunk_index": 0},
            _row(
                article_id="broad-market",
                source="Personal Blog",
                sentiment_positive=0.2,
                sentiment_negative=0.6,
                sentiment_neutral=0.2,
                sentiment_score=-0.4,
                relevance_score=0.4,
            )
            | {"sentence_index": 0, "chunk_index": 0},
        ]
    )
    topic_labels = pd.DataFrame(
        [
            _topic_row("aapl-specific", topic_id=7, topic_probability=0.8),
            _topic_row("broad-market", topic_id=3, topic_probability=0.5),
        ]
    )
    relevance_gate = pd.DataFrame(
        [
            _relevance_row("aapl-specific", decision="accepted", relevance_score=0.9),
            _relevance_row("broad-market", decision="borderline", relevance_score=0.4),
        ]
    )

    records = sentiment_feature_records_from_scored_news(
        scored_news,
        topic_labels=topic_labels,
        relevance_gate=relevance_gate,
        credibility_config=SourceCredibilityConfig(
            default_source_weight=1.0,
            source_weights={"reuters": 2.0, "personal blog": 0.5},
        ),
    )

    features = records[0].features
    topic_summary = json.loads(features["nlp_topic_sentiment_summary"])
    source_summary = json.loads(features["nlp_source_weight_summary"])

    assert features["nlp_sentiment_score"] == pytest.approx(0.725)
    assert features["nlp_sentiment_topic_score"] == pytest.approx(0.7688311688311689)
    assert features["nlp_sentiment_topic_count"] == 2
    assert features["nlp_sentiment_dominant_topic_id"] == 7
    assert features["nlp_sentiment_dominant_topic_score"] == pytest.approx(0.85)
    assert features["nlp_relevance_accepted_count"] == 1
    assert features["nlp_relevance_borderline_count"] == 1
    assert features["nlp_effective_weight_sum"] == pytest.approx(2.0)
    assert json.loads(features["nlp_contributing_article_ids"]) == [
        "aapl-specific",
        "broad-market",
    ]
    assert json.loads(features["nlp_semantic_warning_codes"]) == []
    assert topic_summary[0]["topic_id"] == 7
    assert topic_summary[0]["article_count"] == 1
    assert source_summary == [
        {
            "article_count": 1,
            "sentence_count": 1,
            "source": "Personal Blog",
            "source_weight": 0.5,
        },
        {
            "article_count": 1,
            "sentence_count": 1,
            "source": "Reuters",
            "source_weight": 2.0,
        },
    ]


def test_sentiment_feature_records_warn_on_missing_optional_topic_evidence() -> None:
    """Missing optional topic rows produce null topic features and warning metadata."""
    scored_news = pd.DataFrame([_row(source=None) | {"sentence_index": 0, "chunk_index": 0}])
    relevance_gate = pd.DataFrame([_relevance_row("article-1")])

    records = sentiment_feature_records_from_scored_news(
        scored_news,
        topic_labels=pd.DataFrame(
            columns=["date", "ticker", "article_id", "topic_id", "topic_probability"]
        ),
        relevance_gate=relevance_gate,
        credibility_config=SourceCredibilityConfig(
            default_source_weight=1.0,
            source_weights={},
        ),
    )

    features = records[0].features

    assert features["nlp_sentiment_topic_score"] is None
    assert features["nlp_sentiment_dominant_topic_id"] is None
    assert features["nlp_missing_source_count"] == 1
    assert features["nlp_missing_topic_count"] == 1
    assert json.loads(features["nlp_semantic_warning_codes"]) == [
        "missing_source_default_weight",
        "missing_topic_artifact",
    ]


def test_sentiment_feature_records_to_frame_serializes_features() -> None:
    """Sentiment FeatureRecords serialize to the shared feature-shard layout."""
    scored_news = pd.DataFrame([_row()])
    records = sentiment_feature_records_from_scored_news(
        scored_news,
        credibility_config=SourceCredibilityConfig(
            default_source_weight=1.0,
            source_weights={},
        ),
    )

    frame = sentiment_feature_records_to_frame(records)

    serialized = frame.to_dict(orient="records")
    features = json.loads(serialized[0]["features"])

    assert serialized[0]["date"] == "2024-04-10"
    assert serialized[0]["ticker"] == "AAPL"
    assert features["nlp_article_count"] == 1
    assert features["nlp_relevance_score"] == 1.0
    assert features["nlp_sentence_count"] == 1
    assert features["nlp_sentiment_score"] == 0.7
    assert features["nlp_sentiment_strength"] == 0.8


def _topic_row(
    article_id: str,
    *,
    topic_id: int,
    topic_probability: float,
) -> dict[str, object]:
    """Build one topic-label row."""
    return {
        "date": "2024-04-10",
        "ticker": "AAPL",
        "article_id": article_id,
        "normalized_headline": article_id,
        "text": article_id,
        "article_sentence_count": 1,
        "embedding_cache_key": f"embedding-{article_id}",
        "topic_model": "test-topic",
        "topic_model_version": "test-version",
        "topic_id": topic_id,
        "topic_probability": topic_probability,
    }


def _relevance_row(
    article_id: str,
    *,
    decision: str = "accepted",
    relevance_score: float = 1.0,
) -> dict[str, object]:
    """Build one relevance-gate audit row."""
    return {
        "date": "2024-04-10",
        "ticker": "AAPL",
        "article_id": article_id,
        "sentence_index": 0,
        "chunk_index": 0,
        "headline": article_id,
        "text": article_id,
        "source": "Reuters",
        "published_at": "2024-04-10T14:30:00Z",
        "relevance_decision": decision,
        "relevance_score": relevance_score,
        "ticker_relevance_score": 1.0,
        "financial_relevance_score": 1.0,
        "topic_relevance_score": 0.8,
        "reason_codes": "[]",
        "ticker_evidence": "{}",
        "entity_evidence": "[]",
        "topic_id": 7,
        "topic_probability": 0.8,
        "embedding_cache_key": f"embedding-{article_id}",
        "has_embedding": True,
    }
