from __future__ import annotations

import re
from datetime import UTC

from core.features.news_preprocessing import (
    NewsPreprocessingConfig,
    news_sentiment_frame_to_records,
    preprocess_news_articles,
    records_to_news_sentiment_frame,
    split_article_chunks,
    split_article_sentences,
)


def test_split_article_chunks_preserves_source_field_provenance() -> None:
    """Sentence/chunk rows preserve their source field instead of deduping provenance away."""
    article = {
        "headline": "Apple moves higher.",
        "summary": "Apple moves higher.",
        "content": "Apple moves higher.",
    }

    chunks = split_article_chunks(article)
    sentences = split_article_sentences(article)

    assert [chunk.source_field for chunk in chunks] == ["headline", "summary", "content"]
    assert [chunk.source_order for chunk in chunks] == [0, 0, 0]
    assert [chunk.chunk_index for chunk in chunks] == [0, 1, 2]
    assert [chunk.text for chunk in chunks] == ["Apple moves higher."] * 3
    assert sentences == ["Apple moves higher."]


def test_preprocess_news_articles_emits_sentence_records_for_multiple_alias_tickers() -> None:
    """Raw Alpaca symbols are normalized and expanded into per-ticker sentence rows."""
    articles = [
        {
            "id": 1001,
            "headline": "Apple and Berkshire rally.",
            "summary": "Apple Inc. reported earnings.",
            "content": "Berkshire also gained!",
            "source": "benzinga",
            "url": "https://example.test/article",
            "created_at": "2024-01-02T12:00:00+00:00",
            "symbols": ["AAPL", "BRK.B", "FB", "NOTIN"],
        }
    ]

    records = preprocess_news_articles(
        articles,
        as_of_date="2024-01-02",
        point_in_time_tickers=["AAPL", "BRK-B", "META"],
    )

    assert {(record.ticker, record.sentence_index) for record in records} == {
        ("AAPL", 0),
        ("BRK-B", 0),
        ("META", 0),
        ("AAPL", 1),
        ("BRK-B", 1),
        ("META", 1),
        ("AAPL", 2),
        ("BRK-B", 2),
        ("META", 2),
    }
    assert all(record.article_id == "1001" for record in records)
    assert all(record.headline == "Apple and Berkshire rally." for record in records)
    assert all(record.published_at is not None for record in records)
    assert records[0].published_at is not None
    assert records[0].published_at.isoformat() == "2024-01-02T12:00:00+00:00"


def test_split_article_sentences_cleans_provider_html_and_preserves_meaningful_text() -> None:
    """Provider HTML is stripped before splitting without losing useful article text."""
    article = {
        "headline": (
            '<div><a href="https://example.test/apple">Apple</a> shares rise &nbsp; '
            "on <strong>AI</strong> push.</div>"
        ),
        "summary": (
            '<p>Analyst calls it a "strong quarter" &#8212; target rises to $210.</p>'
            '<blockquote class="twitter-tweet"><p>Ignore this tweet embed.</p></blockquote>'
        ),
        "content": (
            '<div><p>AAPL traded at <span>$189.20</span>.</p>'
            '<script>window.widget = true;</script>'
            '<div class="widget">widget boilerplate</div>'
            '<p>Source: Benzinga</p></div>'
        ),
        "symbols": ["AAPL"],
    }

    sentences = split_article_sentences(article)
    records = preprocess_news_articles(
        [article],
        as_of_date="2024-01-02",
        point_in_time_tickers=["AAPL"],
    )

    assert sentences == [
        "Apple shares rise on AI push.",
        'Analyst calls it a "strong quarter" — target rises to $210.',
        "AAPL traded at $189.20.",
        "Source: Benzinga",
    ]
    assert [record.text for record in records] == sentences
    assert all("<" not in sentence and ">" not in sentence for sentence in sentences)
    assert all("twitter" not in sentence.lower() for sentence in sentences)
    assert all("widget" not in sentence.lower() for sentence in sentences)
    assert all("&nbsp;" not in sentence for sentence in sentences)


def test_preprocess_news_articles_tags_tickers_entities_and_provenance() -> None:
    """AAPL, competitor-only, broad-market, and irrelevant article examples are handled cleanly."""
    articles = [
        {
            "id": 1001,
            "headline": "Apple releases quarterly results.",
            "summary": "Apple reported quarterly earnings.",
            "content": "Full article text for Apple earnings.",
            "source": "benzinga",
            "url": "https://example.test/apple",
            "created_at": "2024-01-02T12:00:00+00:00",
            "symbols": ["AAPL"],
        },
        {
            "id": 1002,
            "headline": "Microsoft announces new product.",
            "summary": "Microsoft expanded its product line.",
            "content": "Full article text for Microsoft product.",
            "source": "benzinga",
            "url": "https://example.test/microsoft",
            "created_at": "2024-01-02T13:00:00+00:00",
            "symbols": ["MSFT"],
        },
        {
            "id": 1003,
            "headline": "S&P 500 rises as Fed holds rates.",
            "summary": "Federal Reserve signaled patience.",
            "content": "Broad market trading stayed orderly.",
            "source": "benzinga",
            "url": "https://example.test/market",
            "created_at": "2024-01-02T14:00:00+00:00",
            "symbols": ["SPY"],
        },
        {
            "id": 1004,
            "headline": "Local bakery opens a new store.",
            "summary": "Neighborhood foot traffic increased.",
            "content": "Irrelevant local news.",
            "source": "benzinga",
            "url": "https://example.test/irrelevant",
            "created_at": "2024-01-02T15:00:00+00:00",
            "symbols": [],
        },
    ]

    records = preprocess_news_articles(
        articles,
        as_of_date="2024-01-02",
        point_in_time_tickers=None,
    )

    assert {record.ticker for record in records} == {"AAPL", "MSFT", "SPY"}
    assert all(record.article_id in {"1001", "1002", "1003"} for record in records)
    assert all(record.source_text_provenance["article_id"] == record.article_id for record in records)
    assert all(record.source_text_provenance["chunk_index"] == record.chunk_index for record in records)

    aapl_records = [record for record in records if record.ticker == "AAPL"]
    assert {record.source_text_field for record in aapl_records} == {"headline", "summary", "content"}
    assert all(record.normalized_headline == "apple releases quarterly results" for record in aapl_records)
    assert all(record.ticker_mentions == ("AAPL",) for record in aapl_records)
    assert all("Apple" in record.entity_mentions for record in aapl_records)
    assert aapl_records[0].headline == "Apple releases quarterly results."
    assert aapl_records[0].published_at is not None
    assert aapl_records[0].published_at.isoformat() == "2024-01-02T12:00:00+00:00"
    assert aapl_records[0].source_text_provenance["raw_headline"] == "Apple releases quarterly results."

    msft_records = [record for record in records if record.ticker == "MSFT"]
    assert all("Microsoft" in record.entity_mentions for record in msft_records)
    assert all(record.ticker_mentions == ("MSFT",) for record in msft_records)

    spy_records = [record for record in records if record.ticker == "SPY"]
    assert all(any(entity in record.entity_mentions for entity in {"S&P 500", "Federal Reserve"}) for record in spy_records)
    assert all(record.ticker_mentions == ("SPY",) for record in spy_records)


def test_split_article_sentences_preserves_headings_lists_and_quote_punctuation() -> None:
    """Headings, list items, abbreviations, and quoted sentences stay readable."""
    article = {
        "headline": '<h1><a href="https://example.test/aapl">AAPL</a> gains on AI rollout.</h1>',
        "summary": '<p>Analyst says, "Buy now." Revenue may accelerate.</p>',
        "content": (
            "<h2>Key points</h2>"
            "<p>U.S. regulators review the deal.</p>"
            "<ul><li>Revenue beat expectations.</li><li>Margins improved.</li></ul>"
        ),
        "symbols": ["AAPL"],
    }

    sentences = split_article_sentences(article)

    assert sentences == [
        "AAPL gains on AI rollout.",
        'Analyst says, "Buy now."',
        "Revenue may accelerate.",
        "Key points",
        "U.S. regulators review the deal.",
        "Revenue beat expectations.",
        "Margins improved.",
    ]
    assert all("<" not in sentence and ">" not in sentence for sentence in sentences)
    assert all("aapl" not in sentence.lower() or sentence == "AAPL gains on AI rollout." for sentence in sentences)


def test_split_article_sentences_handles_abbreviations_and_deduplicates() -> None:
    """Sentence splitting avoids common abbreviation splits and removes duplicate text."""
    article = {
        "headline": "U.S. stocks rose.",
        "summary": "Apple Inc. reported earnings. Shares rose!",
        "content": "Apple Inc. reported earnings. Shares rose!",
    }

    sentences = split_article_sentences(article)

    assert sentences == [
        "U.S. stocks rose.",
        "Apple Inc. reported earnings.",
        "Shares rose!",
    ]


def test_preprocess_news_articles_chunks_oversized_paragraphs_without_truncation() -> None:
    """Oversized blocks are split into bounded chunks without dropping content."""
    paragraph = " ".join(
        [
            "Alpha beta gamma delta epsilon zeta eta theta iota kappa lambda mu",
            "nu xi omicron pi rho sigma tau upsilon phi chi psi omega",
        ]
        * 6
    )
    article = {
        "id": "chunk-limit",
        "content": f"<p>{paragraph}</p>",
        "symbols": ["AAPL"],
    }
    config = NewsPreprocessingConfig(
        target_chunk_chars=80,
        max_chunk_chars=120,
        fallback_chunk_chars=60,
    )

    records = preprocess_news_articles(
        [article],
        as_of_date="2024-01-02",
        point_in_time_tickers=["AAPL"],
        config=config,
    )

    assert len(records) > 1
    assert all(len(record.text or "") <= 120 for record in records)
    reconstructed = re.sub(r"\s+", " ", " ".join(record.text or "" for record in records)).strip()
    expected = re.sub(r"\s+", " ", paragraph).strip()
    assert reconstructed == expected
    assert [record.sentence_index for record in records] == list(range(len(records)))


def test_preprocess_news_articles_filters_empty_text_and_missing_universe_symbols() -> None:
    """Articles without allowed point-in-time ticker tags are skipped."""
    records = preprocess_news_articles(
        [
            {"id": "empty-text", "headline": " ", "symbols": ["AAPL"]},
            {"id": "not-allowed", "headline": "Microsoft rallied.", "symbols": ["MSFT"]},
        ],
        as_of_date="2024-01-02",
        point_in_time_tickers=["AAPL"],
        config=NewsPreprocessingConfig(min_sentence_chars=3),
    )

    assert records == []


def test_news_sentiment_frame_round_trips_records_with_provenance() -> None:
    """Preprocessed records serialize to a frame and back without losing provenance."""
    records = preprocess_news_articles(
        [
            {
                "id": "round-trip",
                "headline": "Apple launches product.",
                "created_at": "2024-01-02T12:00:00+00:00",
                "symbols": ["AAPL"],
            }
        ],
        as_of_date="2024-01-02",
        point_in_time_tickers=["AAPL"],
    )

    frame = records_to_news_sentiment_frame(records)
    restored = news_sentiment_frame_to_records(frame)

    assert restored == records
    assert restored[0].published_at is not None
    assert restored[0].published_at.tzinfo == UTC
    assert restored[0].ticker_mentions == ("AAPL",)
    assert restored[0].entity_mentions == ("Apple",)
    assert set(frame.columns) >= {
        "normalized_headline",
        "chunk_index",
        "source_text_field",
        "source_text_provenance",
        "ticker_mentions",
        "entity_mentions",
    }
