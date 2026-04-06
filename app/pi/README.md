# app/pi

Lightweight Edge Pi runtime.

This folder is meant to run on the Raspberry Pi and should stay small, deterministic, and operationally boring.

Responsibilities:
- fetch market and account context
- call the cloud inference endpoint
- translate target weights into paper orders
- execute through Alpaca paper trading
- reconcile positions and generate reports

AI-heavy workloads should not run here.
