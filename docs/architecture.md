# Architecture

This document defines the end-to-end system architecture for AI Stock Trader.

## Deployment split

- Cloud Lab: heavy feature generation, training, validation, packaging
- Cloud Oracle: hosted inference and response contracts
- Edge Pi: orchestration, broker execution, and reporting

## Layered algorithm baseline

0. Data and universe selection
1. Feature generation
2. Regime detection and prediction
3. Portfolio construction
4. Risk engine
5. Execution engine

## Design principles

- Keep edge runtime deterministic and lightweight
- Keep heavy compute in cloud workloads
- Keep state in object storage
- Keep contracts explicit between layers
