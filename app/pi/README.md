# app/pi

Lightweight Edge Pi runtime.

Owner: Edge orchestration and execution boundary.

Responsibilities:
- Run daily orchestration flow
- Call cloud inference and execute approved orders
- Emit runtime summaries and alerts

Out of scope:
- Heavy ML training/inference workloads
- Contract/schema ownership

This folder is meant to run on the Raspberry Pi and should stay small, deterministic, and operationally boring.

Responsibilities:
- fetch market and account context
- call the cloud inference endpoint
- translate target weights into paper orders
- execute through Alpaca paper trading
- reconcile positions and generate reports

AI-heavy workloads should not run here.
