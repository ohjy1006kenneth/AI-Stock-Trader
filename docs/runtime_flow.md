# Runtime Flow

This document defines the operational day flow.

## After market close

1. Pull market and news data
2. Build features
3. Run cloud inference
4. Construct portfolio targets
5. Apply hard risk controls
6. Persist approved proposal

## Next session open

1. Reconcile account state
2. Translate targets to orders
3. Execute and monitor fills
4. Persist execution logs
5. Send run summary
