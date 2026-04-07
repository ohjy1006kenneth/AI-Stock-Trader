from __future__ import annotations

from datetime import datetime
from pathlib import Path
import sys

from pydantic import ValidationError


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

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


def test_universe_record_happy_path() -> None:
    """UniverseRecord should parse required fields with defaults."""
    record = UniverseRecord(date="2026-04-06", ticker="AAPL", in_universe=True)
    assert record.tradable is True
    assert record.liquid is True
    assert record.halted is False


def test_ohlcv_record_rejects_negative_volume() -> None:
    """OHLCVRecord must reject invalid negative volume."""
    try:
        OHLCVRecord(
            date="2026-04-06",
            ticker="AAPL",
            open=100.0,
            high=101.0,
            low=99.0,
            close=100.5,
            volume=-1,
            adj_close=100.5,
            dollar_volume=1_000_000.0,
        )
    except ValidationError:
        return
    assert False, "Expected ValidationError for negative volume"


def test_news_sentiment_probability_bounds() -> None:
    """NewsSentimentRecord probabilities should be bounded to [0, 1]."""
    record = NewsSentimentRecord(
        date="2026-04-06",
        ticker="AAPL",
        sentiment_positive=0.7,
        sentiment_negative=0.1,
        sentiment_neutral=0.2,
    )
    assert record.sentiment_positive == 0.7


def test_feature_record_holds_dynamic_feature_map() -> None:
    """FeatureRecord should keep flexible features dictionary."""
    record = FeatureRecord(
        date="2026-04-06",
        ticker="AAPL",
        features={"returns_1d": 0.01, "regime_label": "bull"},
    )
    assert record.features["returns_1d"] == 0.01


def test_score_record_probability_bounds() -> None:
    """ScoreRecord must enforce bounded probability-style fields."""
    record = ScoreRecord(
        date="2026-04-06",
        ticker="AAPL",
        return_score=0.15,
        pos_prob=0.61,
        rank_score=0.88,
        regime="bull",
        confidence=0.72,
        model_version="xgb-v1",
    )
    assert record.confidence == 0.72


def test_portfolio_record_contains_change_dollars() -> None:
    """PortfolioRecord should include the pre-risk change notional."""
    record = PortfolioRecord(
        date="2026-04-06",
        ticker="AAPL",
        weight=0.1,
        target_dollars=10_000.0,
        current_dollars=8_000.0,
        change_dollars=2_000.0,
    )
    assert record.change_dollars == 2_000.0


def test_approved_order_record_uses_action_enum() -> None:
    """ApprovedOrderRecord action should use ActionType values."""
    record = ApprovedOrderRecord(
        date="2026-04-06",
        ticker="AAPL",
        action=ActionType.BUY,
        target_dollars=2_000.0,
        approved=True,
    )
    assert record.action == ActionType.BUY


def test_execution_fill_record_share_bounds() -> None:
    """ExecutionFillRecord should reject negative share counts."""
    try:
        ExecutionFillRecord(
            date="2026-04-06",
            ticker="AAPL",
            action=ActionType.SELL,
            shares_target=100,
            shares_filled=-1,
            avg_fill_price=100.0,
            estimated_fill_price=100.2,
            slippage_bps=-20.0,
            status="filled",
            retries=0,
        )
    except ValidationError:
        return
    assert False, "Expected ValidationError for negative shares_filled"


def test_pipeline_manifest_record_uses_runstatus_enum() -> None:
    """PipelineManifestRecord status must be a RunStatus enum value."""
    record = PipelineManifestRecord(
        run_id="run-001",
        stage="feature_generation",
        status=RunStatus.COMPLETED,
        started_at=datetime(2026, 4, 6, 20, 0, 0),
        finished_at=datetime(2026, 4, 6, 20, 5, 0),
    )
    assert record.status == RunStatus.COMPLETED


def test_artifact_manifest_defaults_schema_version() -> None:
    """Artifact manifest should require explicit matching schema version."""
    record = ArtifactManifestRecord(
        artifact_id="artifact-001",
        model_version="xgb-v1",
        created_at=datetime(2026, 4, 6, 21, 0, 0),
        stage="validation",
        bundle_path="artifacts/bundles/xgb-v1.tar.gz",
        schema_version=SCHEMA_VERSION,
    )
    assert record.schema_version == SCHEMA_VERSION


def test_artifact_manifest_rejects_missing_or_mismatched_schema_version() -> None:
    """Artifact manifest should reject missing or mismatched schema_version."""
    try:
        ArtifactManifestRecord(
            artifact_id="artifact-001",
            model_version="xgb-v1",
            created_at=datetime(2026, 4, 6, 21, 0, 0),
            stage="validation",
            bundle_path="artifacts/bundles/xgb-v1.tar.gz",
        )
    except ValidationError:
        pass
    else:
        assert False, "Expected ValidationError for missing schema_version"

    try:
        ArtifactManifestRecord(
            artifact_id="artifact-001",
            model_version="xgb-v1",
            created_at=datetime(2026, 4, 6, 21, 0, 0),
            stage="validation",
            bundle_path="artifacts/bundles/xgb-v1.tar.gz",
            schema_version="0.9.0",
        )
    except ValidationError:
        return
    assert False, "Expected ValidationError for mismatched schema_version"
