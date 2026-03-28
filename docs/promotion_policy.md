# Maintenance and Promotion Workflow

## 1. Research Proposal
- Scholar Researcher proposes factor/source update.

## 2. Validation
- Backtest Validator reviews robustness, bias controls, and realism.

## 3. Code Update
- Code Maintainer prepares minimal deterministic code changes.

## 4. Approval Gate
- Changes affecting strategy logic require explicit approval after validation.

## 5. Promotion
- Only validated and approved changes reach production scripts.

Rules:
- no silent strategy drift
- no daily formula reinvention
- no direct ledger edits outside executor
- no LLM in the hot path
