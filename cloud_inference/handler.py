from __future__ import annotations

import os
import uuid
from datetime import datetime, timezone
from typing import Any

from cloud_inference.artifact_loader import load_bundle
from cloud_inference.contracts import unwrap_hf_request
from cloud_inference.feature_adapter import build_predictive_sample
from cloud_training.model_architecture.policy import build_policy_predictions


class EndpointHandler:
    def __init__(self, model_dir: str | None = None, **_: Any):
        explicit_path = os.getenv("PREDICTIVE_BUNDLE_PATH") or model_dir
        self.bundle = load_bundle(explicit_path)

    def __call__(self, data: dict[str, Any]) -> dict[str, Any]:
        payload, request_id = unwrap_hf_request(data)
        portfolio = payload["portfolio"]
        universe = payload["universe"]
        scored_universe = []
        for item in universe:
            sample = build_predictive_sample(item, universe)
            score = self.bundle.predict_from_sample(sample)
            scored_universe.append({"ticker": item["ticker"], "score": score})

        as_of_date = max(str(item["history"][-1]["date"]) for item in universe if item.get("history"))
        policy_payload = build_policy_predictions(
            as_of_date=as_of_date,
            scored_universe=scored_universe,
            portfolio=portfolio,
        )
        return {
            "model_version": self.bundle.artifact_name,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "request_id": request_id or str(uuid.uuid4()),
            "predictions": policy_payload["predictions"],
        }
