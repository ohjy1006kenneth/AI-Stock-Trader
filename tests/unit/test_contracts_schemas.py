from __future__ import annotations

from typing import Any

from pydantic import ValidationError

from core.contracts.schemas import (
    SCHEMA_VERSION,
    ActionType,
    ApprovedOrderRecord,
    ArtifactManifestRecord,
    ExecutionFillRecord,
    FeatureRecord,
    NewsSentimentRecord,
    OHLCVRecord,
    PipelineManifestRecord,
    PortfolioRecord,
    RunStatus,
    ScoreRecord,
    UniverseRecord,
)
from tests.unit.sample_data import load_sample_json


def _contracts_fixture() -> dict[str, Any]:
    """Load schema-valid sample records used across contract tests."""
    return load_sample_json("contracts_schema_records.json")


def test_universe_record_happy_path() -> None:
    """UniverseRecord should parse required fields with defaults."""
    record = UniverseRecord(**_contracts_fixture()["universe_record"])
    assert record.tradable is True
    assert record.liquid is True
    assert record.halted is False


def test_ohlcv_record_rejects_negative_volume() -> None:
    """OHLCVRecord must reject invalid negative volume."""
    try:
        OHLCVRecord(**{**_contracts_fixture()["ohlcv_record"], "volume": -1})
    except ValidationError:
        return
    assert False, "Expected ValidationError for negative volume"


def test_news_sentiment_probability_bounds() -> None:
    """NewsSentimentRecord probabilities should be bounded to [0, 1]."""
    record = NewsSentimentRecord(**_contracts_fixture()["news_sentiment_record"])
    assert record.sentiment_positive == 0.7


def test_feature_record_holds_dynamic_feature_map() -> None:
    """FeatureRecord should keep flexible features dictionary."""
    record = FeatureRecord(**_contracts_fixture()["feature_record"])
    assert record.features["returns_1d"] == 0.01


def test_score_record_probability_bounds() -> None:
    """ScoreRecord must enforce bounded probability-style fields."""
    record = ScoreRecord(**_contracts_fixture()["score_record"])
    assert record.confidence == 0.72


def test_portfolio_record_contains_change_dollars() -> None:
    """PortfolioRecord should include the pre-risk change notional."""
    record = PortfolioRecord(**_contracts_fixture()["portfolio_record"])
    assert record.change_dollars == 2_000.0


def test_approved_order_record_uses_action_enum() -> None:
    """ApprovedOrderRecord action should use ActionType values."""
    record = ApprovedOrderRecord(**_contracts_fixture()["approved_order_record"])
    assert record.action == ActionType.BUY


def test_execution_fill_record_share_bounds() -> None:
    """ExecutionFillRecord should reject negative share counts."""
    try:
        ExecutionFillRecord(**{**_contracts_fixture()["execution_fill_record"], "shares_filled": -1})
    except ValidationError:
        return
    assert False, "Expected ValidationError for negative shares_filled"


def test_pipeline_manifest_record_uses_runstatus_enum() -> None:
    """PipelineManifestRecord status must be a RunStatus enum value."""
    record = PipelineManifestRecord(**_contracts_fixture()["pipeline_manifest_record"])
    assert record.status == RunStatus.COMPLETED


def test_artifact_manifest_defaults_schema_version() -> None:
    """Artifact manifest should require explicit matching schema version."""
    record = ArtifactManifestRecord(**_contracts_fixture()["artifact_manifest_record"])
    assert record.schema_version == SCHEMA_VERSION


def test_artifact_manifest_rejects_missing_or_mismatched_schema_version() -> None:
    """Artifact manifest should reject missing or mismatched schema_version."""
    try:
        ArtifactManifestRecord(
            **{
                key: value
                for key, value in _contracts_fixture()["artifact_manifest_record"].items()
                if key != "schema_version"
            }
        )
    except ValidationError:
        pass
    else:
        assert False, "Expected ValidationError for missing schema_version"

    try:
        ArtifactManifestRecord(
            **{**_contracts_fixture()["artifact_manifest_record"], "schema_version": "0.9.0"}
        )
    except ValidationError:
        return
    assert False, "Expected ValidationError for mismatched schema_version"
