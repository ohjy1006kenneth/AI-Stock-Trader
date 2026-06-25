from __future__ import annotations

from core.features.catalog import feature_catalog, validate_feature_value
from core.features.order_book_features import ORDER_BOOK_FEATURE_COLUMNS
from core.features.sector_features import SECTOR_FEATURE_COLUMNS
from core.features.sentiment_features import SENTIMENT_FEATURE_COLUMNS


def test_feature_catalog_registers_sector_features() -> None:
    """All sector feature columns exist in the canonical Layer 1 catalog."""
    catalog = feature_catalog()

    for feature_name in SECTOR_FEATURE_COLUMNS:
        assert feature_name in catalog
        assert catalog[feature_name].owner == "sector"
        assert catalog[feature_name].kind == "number"
        assert catalog[feature_name].required is True


def test_sector_relative_strength_has_bounded_catalog_rule() -> None:
    """Sector-relative strength stays constrained to the documented percentile range."""
    rule = feature_catalog()["sector_relative_strength"]

    assert rule.minimum == 0.0
    assert rule.maximum == 1.0
    assert validate_feature_value("sector_relative_strength", -0.01, rule) is not None
    assert validate_feature_value("sector_relative_strength", 1.01, rule) is not None
    assert validate_feature_value("sector_relative_strength", 0.5, rule) is None


def test_feature_catalog_registers_optional_order_book_features() -> None:
    """Order-book columns remain optional and use bounded numeric rules."""
    catalog = feature_catalog()

    for feature_name in ORDER_BOOK_FEATURE_COLUMNS:
        assert feature_name in catalog
        assert catalog[feature_name].owner == "order_book"
        assert catalog[feature_name].required is False

    assert (
        validate_feature_value("l2_bid_ask_spread", -0.01, catalog["l2_bid_ask_spread"])
        is not None
    )
    assert (
        validate_feature_value("l2_book_imbalance", 1.1, catalog["l2_book_imbalance"])
        is not None
    )
    assert (
        validate_feature_value("l2_book_imbalance", 0.2, catalog["l2_book_imbalance"])
        is None
    )


def test_feature_catalog_registers_sentiment_semantic_features() -> None:
    """All sentiment semantic aggregation columns exist in the Layer 1 catalog."""
    catalog = feature_catalog()

    for feature_name in SENTIMENT_FEATURE_COLUMNS:
        assert feature_name in catalog
        assert catalog[feature_name].owner in {"sentiment", "nlp"}
        assert catalog[feature_name].required is False

    assert (
        validate_feature_value(
            "nlp_sentiment_topic_score",
            -1.1,
            catalog["nlp_sentiment_topic_score"],
        )
        is not None
    )
    assert (
        validate_feature_value(
            "nlp_topic_sentiment_summary",
            '[{"topic_id":1}]',
            catalog["nlp_topic_sentiment_summary"],
        )
        is None
    )
