from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator


SCHEMA_VERSION = "1.0.0"


class ActionType(str, Enum):
    """Allowed high-level order actions."""

    BUY = "BUY"
    SELL = "SELL"
    HOLD = "HOLD"
    REJECT = "REJECT"


class RunStatus(str, Enum):
    """Allowed pipeline/job states."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    BLOCKED = "blocked"


class UniverseRecord(BaseModel):
    """Point-in-time eligibility state for a ticker on a given date."""

    model_config = ConfigDict(extra="forbid")

    date: str
    ticker: str
    in_universe: bool
    tradable: bool = True
    liquid: bool = True
    halted: bool = False
    data_quality_ok: bool = True
    reason: str | None = None


class OHLCVRecord(BaseModel):
    """Adjusted OHLCV bar for one ticker on one date."""

    model_config = ConfigDict(extra="forbid")

    date: str
    ticker: str
    open: float
    high: float
    low: float
    close: float
    volume: int = Field(ge=0)
    adj_close: float
    dollar_volume: float = Field(ge=0)


class NewsSentimentRecord(BaseModel):
    """Article-level or aggregated ticker-day sentiment record."""

    model_config = ConfigDict(extra="forbid")

    date: str
    ticker: str
    headline: str | None = None
    source: str | None = None
    published_at: datetime | None = None
    sentiment_positive: float | None = Field(default=None, ge=0.0, le=1.0)
    sentiment_negative: float | None = Field(default=None, ge=0.0, le=1.0)
    sentiment_neutral: float | None = Field(default=None, ge=0.0, le=1.0)
    sentiment_score: float | None = None
    relevance_score: float | None = None


class FeatureRecord(BaseModel):
    """Aligned feature row for one date/ticker pair."""

    model_config = ConfigDict(extra="forbid")

    date: str
    ticker: str
    features: dict[str, float | int | str | bool | None] = Field(default_factory=dict)


class ScoreRecord(BaseModel):
    """Predictive model output for one ticker on one date."""

    model_config = ConfigDict(extra="forbid")

    date: str
    ticker: str
    return_score: float
    pos_prob: float = Field(ge=0.0, le=1.0)
    rank_score: float
    regime: str | None = None
    confidence: float = Field(ge=0.0, le=1.0)
    model_version: str


class PortfolioRecord(BaseModel):
    """Pre-risk portfolio target for one ticker on one date."""

    model_config = ConfigDict(extra="forbid")

    date: str
    ticker: str
    weight: float
    target_dollars: float
    current_dollars: float
    change_dollars: float
    selection_reason: str | None = None


class ApprovedOrderRecord(BaseModel):
    """Risk-approved order intent."""

    model_config = ConfigDict(extra="forbid")

    date: str
    ticker: str
    action: ActionType
    target_dollars: float
    approved: bool
    rules_triggered: list[str] = Field(default_factory=list)
    reason: str | None = None


class ExecutionFillRecord(BaseModel):
    """Realized execution outcome for one ticker/order."""

    model_config = ConfigDict(extra="forbid")

    date: str
    ticker: str
    action: ActionType
    shares_target: int = Field(ge=0)
    shares_filled: int = Field(ge=0)
    avg_fill_price: float | None = Field(default=None, ge=0.0)
    estimated_fill_price: float | None = Field(default=None, ge=0.0)
    slippage_bps: float | None = None
    status: str
    retries: int = Field(ge=0, default=0)


class PipelineManifestRecord(BaseModel):
    """Machine-readable completion state for a pipeline stage or job."""

    model_config = ConfigDict(extra="forbid")

    run_id: str
    stage: str
    status: RunStatus
    started_at: datetime
    finished_at: datetime | None = None
    input_path: str | None = None
    output_path: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class ArtifactManifestRecord(BaseModel):
    """Canonical published artifact/bundle metadata."""

    model_config = ConfigDict(extra="forbid")

    artifact_id: str
    model_version: str
    created_at: datetime
    stage: str
    metrics_path: str | None = None
    diagnostics_path: str | None = None
    bundle_path: str
    schema_version: str
    approved: bool = False

    @field_validator("schema_version")
    @classmethod
    def validate_schema_version(cls, value: str) -> str:
        """Require explicit schema_version and enforce current schema for baseline."""
        if value != SCHEMA_VERSION:
            raise ValueError(
                f"schema_version must equal {SCHEMA_VERSION} for this baseline release"
            )
        return value