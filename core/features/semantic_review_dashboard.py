"""UI payload helpers for the Layer 1 semantic-review dashboard."""
from __future__ import annotations

from collections.abc import Mapping

from core.features.aapl_evidence import (
    Layer1SemanticReviewReport,
    _build_payload_from_report,
    build_layer1_semantic_review_dashboard_smoke_result,
)


def build_layer1_semantic_review_dashboard_payload(
    report: Layer1SemanticReviewReport | Mapping[str, object],
) -> dict[str, object]:
    """Return the JSON payload rendered by the semantic-review dashboard UI."""
    return _build_payload_from_report(report)


def validate_layer1_semantic_review_dashboard_payload(
    payload: Mapping[str, object],
) -> dict[str, object]:
    """Return the smoke gate result for a dashboard payload."""
    return build_layer1_semantic_review_dashboard_smoke_result(payload)
