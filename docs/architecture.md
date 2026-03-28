# Cloud-Native Deep Learning Architecture

## Roles
- `trading-quant-researcher` strictly owns `cloud_training/`.
- `trading-portfolio-strategist` and `trading-executor-reporter` strictly own `pi_edge/`.
- `trading` remains the orchestrator and canonical project owner.

## Separation of concerns
- `cloud_training/` = offline research, feature work, PyTorch model definition, training, cloud backtesting.
- `cloud_inference/` = hosted inference handler for the Hugging Face endpoint.
- `pi_edge/` = lightweight orchestrator on the Raspberry Pi: fetch market/news inputs, call hosted inference, execute via Alpaca, report results.

## Boundary
- Decision intelligence moves to the cloud model.
- The edge should only orchestrate data fetch, network inference calls, trade execution, and reporting.
