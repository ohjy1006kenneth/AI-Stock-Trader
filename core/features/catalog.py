"""Shared Layer 1 feature catalog and validation helpers."""
from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Literal

from core.features.fundamentals_features import FUNDAMENTAL_FEATURE_COLUMNS
from core.features.macro_features import MACRO_FEATURE_COLUMNS
from core.features.market_features import MARKET_FEATURE_COLUMNS
from core.features.order_book_features import ORDER_BOOK_FEATURE_COLUMNS
from core.features.regime_detection import HMM_REGIME_FEATURE_COLUMNS
from core.features.sector_features import SECTOR_FEATURE_COLUMNS
from core.features.sentiment_features import SENTIMENT_FEATURE_COLUMNS
from core.features.text_topics import TOPIC_FEATURE_COLUMNS


@dataclass(frozen=True)
class FeatureRule:
    """Catalog rule for one named Layer 1 feature."""

    owner: str
    kind: Literal["number", "string", "boolean"]
    required: bool
    nullable: bool = True
    minimum: float | None = None
    maximum: float | None = None
    allowed_values: tuple[str, ...] = ()


@dataclass(frozen=True)
class FeatureFamilySpec:
    """Audit/report-oriented feature family grouping."""

    key: str
    label: str
    feature_names: tuple[str, ...]


def _unique_feature_names(feature_names: tuple[str, ...]) -> tuple[str, ...]:
    """Return a stable tuple preserving the first occurrence of each feature name."""
    seen: set[str] = set()
    ordered: list[str] = []
    for feature_name in feature_names:
        if feature_name in seen:
            continue
        seen.add(feature_name)
        ordered.append(feature_name)
    return tuple(ordered)


FEATURE_FAMILY_SPECS: tuple[FeatureFamilySpec, ...] = (
    FeatureFamilySpec(
        key="market",
        label="Market",
        feature_names=_unique_feature_names(
            (*MARKET_FEATURE_COLUMNS, *SECTOR_FEATURE_COLUMNS, *ORDER_BOOK_FEATURE_COLUMNS)
        ),
    ),
    FeatureFamilySpec(
        key="macro_context",
        label="Macro/Context",
        feature_names=MACRO_FEATURE_COLUMNS,
    ),
    FeatureFamilySpec(
        key="fundamentals_earnings",
        label="Fundamentals/Earnings",
        feature_names=FUNDAMENTAL_FEATURE_COLUMNS,
    ),
    FeatureFamilySpec(
        key="nlp_topic",
        label="NLP/Topic",
        feature_names=_unique_feature_names(
            (
                *TOPIC_FEATURE_COLUMNS,
                *SENTIMENT_FEATURE_COLUMNS,
            )
        ),
    ),
    FeatureFamilySpec(
        key="regime",
        label="Regime",
        feature_names=HMM_REGIME_FEATURE_COLUMNS,
    ),
)


def feature_catalog() -> dict[str, FeatureRule]:
    """Return the canonical Layer 1 feature catalog."""
    rules: dict[str, FeatureRule] = {}
    for name in MARKET_FEATURE_COLUMNS:
        rules[name] = FeatureRule(owner="market", kind="number", required=True)
    for name in ORDER_BOOK_FEATURE_COLUMNS:
        rules[name] = FeatureRule(owner="order_book", kind="number", required=False)
    for name in SECTOR_FEATURE_COLUMNS:
        if name == "sector_relative_strength":
            continue
        rules[name] = FeatureRule(owner="sector", kind="number", required=True)
    for name in (
        "realized_vol_5d",
        "realized_vol_21d",
        "vol_ratio_5_21",
        "atr_14",
        "volume_ratio_20",
    ):
        rules[name] = FeatureRule(owner="market", kind="number", required=True, minimum=0.0)
    rules["golden_cross_50_200"] = FeatureRule(
        owner="market",
        kind="number",
        required=True,
        minimum=0.0,
        maximum=1.0,
    )
    rules["rsi_14"] = FeatureRule(
        owner="market",
        kind="number",
        required=True,
        minimum=0.0,
        maximum=100.0,
    )
    rules["sector_relative_strength"] = FeatureRule(
        owner="sector",
        kind="number",
        required=True,
        minimum=0.0,
        maximum=1.0,
    )
    rules["l2_bid_ask_spread"] = FeatureRule(
        owner="order_book",
        kind="number",
        required=False,
        minimum=0.0,
    )
    rules["l2_quoted_spread_bps"] = FeatureRule(
        owner="order_book",
        kind="number",
        required=False,
        minimum=0.0,
    )
    rules["l2_book_imbalance"] = FeatureRule(
        owner="order_book",
        kind="number",
        required=False,
        minimum=-1.0,
        maximum=1.0,
    )
    rules["l2_snapshot_count"] = FeatureRule(
        owner="order_book",
        kind="number",
        required=False,
        minimum=0.0,
    )

    for name in FUNDAMENTAL_FEATURE_COLUMNS:
        rules[name] = FeatureRule(owner="fundamentals", kind="number", required=True)
    rules["days_to_next_earnings"] = FeatureRule(
        owner="fundamentals",
        kind="number",
        required=True,
        minimum=0.0,
    )
    rules["pre_earnings_flag"] = FeatureRule(
        owner="fundamentals",
        kind="number",
        required=True,
        minimum=0.0,
        maximum=1.0,
    )
    rules["post_earnings_flag"] = FeatureRule(
        owner="fundamentals",
        kind="number",
        required=True,
        minimum=0.0,
        maximum=1.0,
    )

    for name in MACRO_FEATURE_COLUMNS:
        rules[name] = FeatureRule(owner="macro", kind="number", required=True)
    for name in (
        "fed_funds_rate",
        "treasury_3m",
        "treasury_2y",
        "treasury_10y",
        "vix_level",
        "dollar_index",
        "cpi_level",
        "high_yield_spread",
    ):
        rules[name] = FeatureRule(owner="macro", kind="number", required=True, minimum=0.0)

    rules["nlp_sentence_count"] = FeatureRule(
        owner="nlp",
        kind="number",
        required=False,
        minimum=0.0,
    )
    rules["nlp_topic_count"] = FeatureRule(
        owner="topics",
        kind="number",
        required=False,
        minimum=0.0,
    )
    rules["nlp_dominant_topic_id"] = FeatureRule(owner="topics", kind="number", required=False)
    rules["nlp_dominant_topic_probability"] = FeatureRule(
        owner="topics",
        kind="number",
        required=False,
        minimum=0.0,
        maximum=1.0,
    )
    rules["nlp_mean_topic_probability"] = FeatureRule(
        owner="topics",
        kind="number",
        required=False,
        minimum=0.0,
        maximum=1.0,
    )

    for name in (
        "nlp_sentiment_positive",
        "nlp_sentiment_negative",
        "nlp_sentiment_neutral",
        "nlp_sentiment_strength",
    ):
        rules[name] = FeatureRule(
            owner="sentiment",
            kind="number",
            required=False,
            minimum=0.0,
            maximum=1.0,
        )
    rules["nlp_sentiment_score"] = FeatureRule(
        owner="sentiment",
        kind="number",
        required=False,
        minimum=-1.0,
        maximum=1.0,
    )
    rules["nlp_sentiment_std"] = FeatureRule(
        owner="sentiment",
        kind="number",
        required=False,
        minimum=0.0,
    )
    rules["nlp_article_count"] = FeatureRule(
        owner="sentiment",
        kind="number",
        required=False,
        minimum=0.0,
    )
    rules["nlp_relevance_score"] = FeatureRule(
        owner="sentiment",
        kind="number",
        required=False,
        minimum=0.0,
    )
    for name in (
        "nlp_source_weight_mean",
        "nlp_source_weight_sum",
        "nlp_effective_weight_sum",
        "nlp_sentiment_topic_count",
        "nlp_relevance_accepted_count",
        "nlp_relevance_borderline_count",
        "nlp_missing_source_count",
        "nlp_missing_topic_count",
        "nlp_missing_relevance_evidence_count",
    ):
        rules[name] = FeatureRule(
            owner="sentiment",
            kind="number",
            required=False,
            minimum=0.0,
        )
    for name in (
        "nlp_sentiment_topic_score",
        "nlp_sentiment_dominant_topic_score",
    ):
        rules[name] = FeatureRule(
            owner="sentiment",
            kind="number",
            required=False,
            minimum=-1.0,
            maximum=1.0,
        )
    rules["nlp_sentiment_dominant_topic_id"] = FeatureRule(
        owner="sentiment",
        kind="number",
        required=False,
    )
    rules["nlp_sentiment_dominant_topic_probability"] = FeatureRule(
        owner="sentiment",
        kind="number",
        required=False,
        minimum=0.0,
        maximum=1.0,
    )
    for name in (
        "nlp_contributing_article_ids",
        "nlp_topic_sentiment_summary",
        "nlp_source_weight_summary",
        "nlp_relevance_reason_codes",
        "nlp_semantic_warning_codes",
    ):
        rules[name] = FeatureRule(owner="sentiment", kind="string", required=False)

    rules["regime_label"] = FeatureRule(
        owner="regime",
        kind="string",
        required=True,
        nullable=False,
        allowed_values=("bear", "sideways", "bull"),
    )
    rules["regime_confidence"] = FeatureRule(
        owner="regime",
        kind="number",
        required=True,
        minimum=0.0,
        maximum=1.0,
    )
    for name in ("regime_prob_bear", "regime_prob_sideways", "regime_prob_bull"):
        rules[name] = FeatureRule(
            owner="regime",
            kind="number",
            required=True,
            minimum=0.0,
            maximum=1.0,
        )
    return rules


def feature_family_map() -> dict[str, FeatureFamilySpec]:
    """Return the audit/report family assignment for each cataloged feature name."""
    mapping: dict[str, FeatureFamilySpec] = {}
    for spec in FEATURE_FAMILY_SPECS:
        for feature_name in spec.feature_names:
            mapping[feature_name] = spec
    return mapping


def validate_feature_value(
    feature_name: str,
    value: object,
    rule: FeatureRule,
) -> str | None:
    """Return an error message when `value` violates the feature rule."""
    if value is None:
        if rule.nullable:
            return None
        return f"{feature_name}: null value is not allowed"
    if rule.kind == "string":
        if not isinstance(value, str):
            return f"{feature_name}: expected string, got {type(value).__name__}"
        if rule.allowed_values and value not in rule.allowed_values:
            return f"{feature_name}: {value!r} not in {sorted(rule.allowed_values)}"
        return None
    if rule.kind == "boolean":
        if not isinstance(value, bool):
            return f"{feature_name}: expected boolean, got {type(value).__name__}"
        return None

    numeric = to_float_or_none(value)
    if numeric is None:
        return f"{feature_name}: expected numeric value, got {value!r}"
    if rule.minimum is not None and numeric < rule.minimum:
        return f"{feature_name}: {numeric} < minimum {rule.minimum}"
    if rule.maximum is not None and numeric > rule.maximum:
        return f"{feature_name}: {numeric} > maximum {rule.maximum}"
    return None


def to_float_or_none(value: object) -> float | None:
    """Return a finite float for numeric values, otherwise None."""
    if value is None or isinstance(value, bool):
        return None
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(numeric) or math.isinf(numeric):
        return None
    return numeric
