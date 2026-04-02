# Hugging Face Cloud Oracle Deployment

This issue defines the deployable Hugging Face runtime as a **custom handler package**, not a catalog model.

## Canonical deployment shape

The Cloud Oracle deploys as a small file package containing:

- root `handler.py`
  - exposes `EndpointHandler` for Hugging Face Inference Endpoints
- root `requirements.txt`
  - currently no third-party dependencies are needed
- `cloud_inference/`
  - request unwrap/validation
  - artifact loading
  - response construction
- `cloud_training/model_architecture/hybrid_model.py`
  - inference-time feature extraction + artifact-backed predictor
- `artifacts/bundles/*.bundle.json`
  - bundled servable artifact produced by Issue #3
- `artifacts/bundles/manifest.json`
  - packaged default bundle + shipped bundle inventory for reproducible refreshes
- `bundle_pin.json`
  - explicit packaged pin so directory-based runtime loading does not drift by filename sort order
- `config/cloud_oracle_request.schema.json`
- `config/cloud_oracle_response.schema.json`

This is the runtime that must be deployed so Hugging Face executes **our code against our bundled artifact**.

## Build the package

From repo root:

```bash
.venv/bin/python cloud_inference/build_hf_deployment.py
```

Or from a local snapshot/export of the canonical model repo after approval:

```bash
.venv/bin/python cloud_inference/build_hf_deployment.py \
  --model-repo-dir <model-repo-snapshot>
```

Output directory:

```text
artifacts/deployments/huggingface_oracle/
```

## Deploy on Hugging Face

Use the contents of `artifacts/deployments/huggingface_oracle/` as the custom Inference Endpoint repository contents.

Expected Hugging Face entrypoint behavior:
- it finds root `handler.py`
- it instantiates `EndpointHandler`
- handler prefers the packaged `bundle_pin.json` / `artifacts/bundles/manifest.json` default when loading from a deployment directory
- if the deployment package was built from `--model-repo-dir`, the packaged default came from `endpoints/oracle/ready.json` in that canonical model repo snapshot
- if no packaged pin/manifest is present, it falls back to the latest `artifacts/bundles/*.bundle.json` by filename sort order
- optional env override: `PREDICTIVE_BUNDLE_PATH=artifacts/bundles/<bundle>.bundle.json`

## Request/response contract

Accepted transport forms:

```json
{
  "inputs": {
    "portfolio": {"cash": 10000, "positions": []},
    "universe": []
  },
  "request_id": "req-123"
}
```

or bare payload:

```json
{
  "portfolio": {"cash": 10000, "positions": []},
  "universe": []
}
```

The Pi edge currently uses the wrapped `inputs` transport.

Universe items may also optionally include:
- `market_history` = benchmark/SPY-style OHLCV history for macro feature fallback
- `context` = lightweight per-ticker fundamentals/context fields
- `precomputed_features` = preferred numeric feature payload aligned to the bundle feature schema

Preferred production path for the first serious XGBoost model is to send `precomputed_features` from a cloud-side feature pipeline. The Oracle can fall back to deriving market/macro features from OHLCV history and zero-filling missing text/context fields, but that fallback is mainly for compatibility and smoke validation.

Response shape:

```json
{
  "model_version": "predictive_signal_bundle_v1_...",
  "generated_at": "2026-03-29T00:00:00+00:00",
  "request_id": "req-123",
  "predictions": [
    {
      "ticker": "AAPL",
      "target_weight": 0.123,
      "confidence": 0.81,
      "signal_type": "long"
    }
  ]
}
```

## Why this shape

This keeps deployment bounded and explicit:
- no catalog/base model ambiguity
- no dependency on unrelated repo areas
- exact reuse of the completed `cloud_inference/handler.py` + servable bundle path
- reproducible package build from the repo itself
