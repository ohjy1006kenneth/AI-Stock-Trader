# Setup Guides

## Edge configuration
- Configure Alpaca credentials in `config/alpaca.env`.
- Keep `config/sp500_constituents.json` as the local runtime universe snapshot.
- Configure Hugging Face access in `config/alpaca.env` with:
  - `HF_INFERENCE_URL`
  - `HF_API_TOKEN`

## Cloud / edge contract
The Pi edge sends one portfolio-level batch payload containing:
- top-level `portfolio`
- top-level `universe`

The Cloud Oracle returns one wrapped response object containing:
- `model_version`
- `generated_at`
- `request_id`
- `predictions`

Contract schemas live at:
- `config/cloud_oracle_request.schema.json`
- `config/cloud_oracle_response.schema.json`

## Architecture rules
- Cloud computes indicators and model features from raw OHLCV and news.
- Pi stays lightweight and does not compute training-side technical features.
- Pi trusts the cloud policy output and does not apply a separate confidence cutoff.
- If Oracle target weights sum to more than 1.0, the payload is invalid and the edge must reject it.
- Omitted tickers are treated as target weight 0.0 by downstream edge logic.
