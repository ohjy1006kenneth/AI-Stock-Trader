from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from core.features.news_preprocessing import preprocess_news_articles
from core.features.news_relevance import (
    RELEVANCE_GATE_COLUMNS,
    apply_news_relevance_gate,
)

TARGET_TICKERS = ("AAPL", "AMD", "NVDA", "MSFT")


def test_news_relevance_gate_target_conditioned_categories_and_contamination_controls() -> None:
    """Target-conditioned relevance survives direct business-impact rows and downweights listicles."""
    articles = [
        {
            "id": "aapl-direct",
            "headline": "Apple App Store legal ruling boosts iPhone device sales",
            "summary": "Analysts say the product update could help investor sentiment and services revenue.",
            "created_at": "2024-01-02T12:00:00+00:00",
            "source": "benzinga",
            "symbols": list(TARGET_TICKERS),
        },
        {
            "id": "amd-supply",
            "headline": "AMD warns semiconductor supply chain and input costs may pressure margins",
            "summary": "The chipmaker said manufacturing and foundry issues could weigh on results.",
            "created_at": "2024-01-02T12:05:00+00:00",
            "source": "benzinga",
            "symbols": list(TARGET_TICKERS),
        },
        {
            "id": "nvda-direct",
            "headline": "NVIDIA GPU demand and datacenter AI-chip sales stay strong",
            "summary": "Analysts raised price targets after the device and compute update.",
            "created_at": "2024-01-02T12:10:00+00:00",
            "source": "benzinga",
            "symbols": list(TARGET_TICKERS),
        },
        {
            "id": "msft-direct",
            "headline": "Microsoft Azure, OpenAI and Copilot enterprise software demand lifts outlook",
            "summary": "The cloud and product update supports investor sentiment.",
            "created_at": "2024-01-02T12:15:00+00:00",
            "source": "benzinga",
            "symbols": list(TARGET_TICKERS),
        },
        {
            "id": "mag7-listicle",
            "headline": "Markets rally as investors pick five large-cap names",
            "summary": "Apple, Microsoft, NVIDIA and AMD appear in a roundup of the Mag 7 trade.",
            "content": (
                "Treasury yields and the S&P 500 drove the broader market tone. "
                "The article compares a basket of stocks and only briefly names Apple, Microsoft, NVIDIA and AMD. "
                "Wall Street traders focused on inflation, rates and market rotation rather than one company."
            ),
            "created_at": "2024-01-02T12:20:00+00:00",
            "source": "benzinga",
            "symbols": list(TARGET_TICKERS),
        },
    ]

    records = preprocess_news_articles(
        articles,
        as_of_date="2024-01-02",
        point_in_time_tickers=TARGET_TICKERS,
    )

    result = apply_news_relevance_gate(
        records,
        embeddings=_embedding_frame(article["id"] for article in articles),
        topic_labels=_topic_label_frame(article["id"] for article in articles),
    )

    audit = result.audit_frame
    assert list(audit.columns) == list(RELEVANCE_GATE_COLUMNS)

    aapl = _rows_for_article(audit, "aapl-direct")
    amd = _rows_for_article(audit, "amd-supply")
    nvda = _rows_for_article(audit, "nvda-direct")
    msft = _rows_for_article(audit, "msft-direct")
    listicle = _rows_for_article(audit, "mag7-listicle")

    aapl_targets = [row for row in aapl if row["ticker"] == "AAPL"]
    amd_targets = [row for row in amd if row["ticker"] == "AMD"]
    nvda_targets = [row for row in nvda if row["ticker"] == "NVDA"]
    msft_targets = [row for row in msft if row["ticker"] == "MSFT"]

    assert any(row["relevance_category"] == "direct_target_event" for row in aapl_targets)
    assert any(
        row["relevance_category"] in {"supplier_or_input_cost_exposure", "direct_target_event"}
        for row in amd_targets
    )
    assert any(row["relevance_category"] == "direct_target_event" for row in nvda_targets)
    assert any(row["relevance_category"] == "direct_target_event" for row in msft_targets)

    assert any(row["relevance_decision"] in {"accepted", "borderline"} for row in aapl_targets)
    assert any(row["relevance_decision"] in {"accepted", "borderline"} for row in amd_targets)
    assert any(row["relevance_decision"] in {"accepted", "borderline"} for row in nvda_targets)
    assert any(row["relevance_decision"] in {"accepted", "borderline"} for row in msft_targets)

    assert max(row["article_contamination_ratio"] for row in listicle) > 0.5
    assert max(row["article_contribution_weight"] for row in listicle) < 1.0
    assert any("article_contribution_capped" in _reason_codes(row) for row in listicle)
    assert any(row["relevance_decision"] != "accepted" for row in listicle)

    assert {record.article_id for record in result.finbert_records} >= {
        "aapl-direct",
        "amd-supply",
        "nvda-direct",
        "msft-direct",
    }
    assert all(record.relevance_score is not None for record in result.finbert_records)


def test_news_relevance_gate_keeps_openai_story_from_becoming_chip_relevant() -> None:
    """Microsoft/OpenAI product coverage should not promote AMD/NVDA without chip demand evidence."""
    articles = [
        {
            "id": "msft-openai-no-chip",
            "headline": "Microsoft expands OpenAI partnership across Azure enterprise software",
            "summary": "The update focuses on cloud subscriptions and developer tools.",
            "created_at": "2024-01-02T12:00:00+00:00",
            "source": "benzinga",
            "symbols": list(TARGET_TICKERS),
        },
        {
            "id": "nvda-only",
            "headline": "NVIDIA launches new GPU for AI datacenter customers",
            "summary": "The chip launch highlights compute demand and device sales.",
            "created_at": "2024-01-02T12:05:00+00:00",
            "source": "benzinga",
            "symbols": list(TARGET_TICKERS),
        },
    ]

    records = preprocess_news_articles(
        articles,
        as_of_date="2024-01-02",
        point_in_time_tickers=TARGET_TICKERS,
    )

    result = apply_news_relevance_gate(
        records,
        embeddings=_embedding_frame(article["id"] for article in articles),
        topic_labels=_topic_label_frame(article["id"] for article in articles),
    )

    audit = result.audit_frame
    msft_story = _rows_for_article(audit, "msft-openai-no-chip")
    nvda_story = _rows_for_article(audit, "nvda-only")

    assert any(row["ticker"] == "MSFT" and row["relevance_category"] != "irrelevant" for row in msft_story)
    assert all(row["relevance_decision"] == "rejected" for row in msft_story if row["ticker"] in {"NVDA", "AMD"})
    assert all(row["relevance_decision"] == "rejected" for row in nvda_story if row["ticker"] in {"AAPL", "AMD", "MSFT"})
    assert any(row["ticker"] == "NVDA" and row["relevance_category"] == "direct_target_event" for row in nvda_story)
    assert any(row["ticker"] == "NVDA" and row["relevance_decision"] in {"accepted", "borderline"} for row in nvda_story)


def test_news_relevance_gate_does_not_fully_promote_unknown_rows() -> None:
    """Rows without ticker/entity and financial evidence are rejected with audit reasons."""
    records = preprocess_news_articles(
        [
            {
                "id": "unknown",
                "headline": "Community event opens downtown",
                "summary": "The schedule includes local speakers.",
                "created_at": "2024-01-02T12:00:00+00:00",
                "source": "benzinga",
                "symbols": ["AAPL"],
            }
        ],
        as_of_date="2024-01-02",
        point_in_time_tickers=("AAPL",),
    )

    result = apply_news_relevance_gate(records)

    assert result.finbert_records == []
    assert set(result.audit_frame["relevance_decision"]) == {"rejected"}
    assert result.audit_frame["relevance_score"].max() < 1.0
    assert "low_financial_relevance" in _reason_codes(result.audit_frame.iloc[0])


def test_news_relevance_gate_filters_aapl_contamination_pattern() -> None:
    """Weak non-AAPL article tags do not flow into AAPL FinBERT scoring."""
    articles = json.loads(
        Path("tests/fixtures/news_relevance_gate_articles.json").read_text(encoding="utf-8")
    )
    records = preprocess_news_articles(
        articles,
        as_of_date="2024-01-02",
        point_in_time_tickers=("AAPL",),
    )

    result = apply_news_relevance_gate(
        records,
        embeddings=_embedding_frame(article["id"] for article in articles),
        topic_labels=_topic_label_frame(article["id"] for article in articles),
    )

    audit = result.audit_frame
    aapl_rows = audit.loc[(audit["article_id"] == "aapl-specific") & (audit["ticker"] == "AAPL")]
    assert list(audit.columns) == list(RELEVANCE_GATE_COLUMNS)
    assert aapl_rows["relevance_decision"].isin({"accepted", "borderline"}).any()
    assert "assignment_classification:broad_market" in _reason_codes(
        audit.loc[audit["article_id"] == "broad-market"].iloc[0]
    )
    assert "assignment_classification:broad_market" in _reason_codes(
        audit.loc[audit["article_id"] == "broad-market"].iloc[0]
    )
    assert "low_ticker_relevance" in _reason_codes(
        audit.loc[audit["article_id"] == "broad-market"].iloc[0]
    )
    assert "causal_channel:comparison_context" in _reason_codes(
        audit.loc[audit["article_id"] == "competitor-only"].iloc[0]
    )
    assert audit.loc[audit["article_id"] == "competitor-only", "relevance_decision"].iloc[0] == "rejected"
    assert {record.article_id for record in result.finbert_records} >= {"aapl-specific"}
    assert all(record.relevance_score is not None for record in result.finbert_records)
    assert audit.loc[audit["article_id"] == "broad-market", "ticker_relevance_score"].iloc[0] == 0.0


def test_news_relevance_gate_excludes_snap_vision_pro_comparison_from_aapl_signal() -> None:
    """Snap story with only a Vision Pro comparison must not become AAPL signal."""
    articles = [
        {
            "id": "snap-vision-pro-comparison",
            "headline": "'Sad' That No One Will Tell Snap CEO The Truth About Horrendous Product Design",
            "summary": (
                "The story is about Snap and says its product price compares with Apple's "
                "Vision Pro as a price comparison only."
            ),
            "content": (
                "Snap shares are down and the article criticizes Snap's wearable product. "
                "Apple appears only as a Vision Pro price comparison, with no direct Apple "
                "business claim."
            ),
            "created_at": "2024-01-02T12:00:00+00:00",
            "source": "benzinga",
            "symbols": ["SNAP", "AAPL"],
        }
    ]
    records = preprocess_news_articles(
        articles,
        as_of_date="2024-01-02",
        point_in_time_tickers=("AAPL",),
    )

    result = apply_news_relevance_gate(
        records,
        embeddings=_embedding_frame(article["id"] for article in articles),
        topic_labels=_topic_label_frame(article["id"] for article in articles),
    )

    aapl_rows = _rows_for_article(result.audit_frame, "snap-vision-pro-comparison")
    incidental_rows = [row for row in aapl_rows if row["relevance_category"] == "incidental_comparison"]
    assert incidental_rows
    assert all(row["relevance_decision"] == "rejected" for row in aapl_rows)
    assert all(row["target_context_score"] <= 0.08 for row in incidental_rows)
    assert all(row["target_company_impact_direction"] == "none" for row in incidental_rows)
    assert all(row["article_signal_count"] == 0 for row in aapl_rows)
    assert all(record.article_id != "snap-vision-pro-comparison" for record in result.finbert_records)
    assert any("incidental_comparison_excluded_from_signal" in _reason_codes(row) for row in incidental_rows)


def _topic_label_frame(article_ids) -> pd.DataFrame:
    """Return topic-label rows for relevance-gate tests."""
    return pd.DataFrame(
        [
            {
                "date": "2024-01-02",
                "ticker": ticker,
                "article_id": article_id,
                "normalized_headline": article_id,
                "text": article_id,
                "article_sentence_count": 2,
                "embedding_cache_key": f"embedding-{article_id}",
                "topic_model": "test-topic",
                "topic_model_version": "test",
                "topic_id": 1,
                "topic_probability": 0.70,
            }
            for article_id in article_ids
            for ticker in TARGET_TICKERS
        ]
    )


def _embedding_frame(article_ids) -> pd.DataFrame:
    """Return embedding rows for relevance-gate tests."""
    return pd.DataFrame(
        [
            {
                "date": "2024-01-02",
                "article_id": article_id,
                "normalized_headline": article_id,
                "text": article_id,
                "article_sentence_count": 2,
                "embedding_model": "test-embedding",
                "embedding_revision": "test",
                "embedding_cache_key": f"embedding-{article_id}",
                "embedding_json": "[0.1,0.2]",
            }
            for article_id in article_ids
        ]
    )


def _rows_for_article(frame: pd.DataFrame, article_id: str) -> list[dict[str, object]]:
    """Return dict rows for one article id."""
    return frame.loc[frame["article_id"] == article_id].to_dict(orient="records")


def _reason_codes(row: pd.Series | dict[str, object]) -> set[str]:
    """Decode relevance reason codes from one audit row."""
    return set(json.loads(str(row["reason_codes"])))
