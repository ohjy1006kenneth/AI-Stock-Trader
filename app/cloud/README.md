# app/cloud

Cloud Oracle inference package.

Owner: Hosted inference service boundary.

Responsibilities:
- Load approved artifacts and serve inference
- Validate input and output contracts for edge callers

Out of scope:
- Model training and backtesting
- Broker order execution

This folder contains the serving-side code that loads trained artifacts, adapts runtime inputs into model-ready features, and returns structured outputs to the edge runtime.

This is for scoring and contract handling, not heavy training.
