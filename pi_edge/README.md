# pi_edge

Lightweight edge runtime.

This folder is meant to run on the Raspberry Pi / edge machine and should stay lightweight.

Responsibilities:
- fetch market and account context
- call the cloud inference endpoint
- translate target weights into paper orders
- execute through Alpaca paper trading
- reconcile positions and generate reports

AI-heavy workloads should not run here.
