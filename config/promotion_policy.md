# Maintenance and Promotion Policy

## 1. Research Proposal
- Trigger: new paper, source, or factor idea
- Owner: Scholar Researcher
- Output: formula proposal with exact definitions and implementation notes
- Requirement: trustworthy source citation and explicit assumptions

## 2. Backtest Validation
- Owner: Backtest Validator
- Requirement:
  - no look-ahead bias
  - no survivorship bias where possible
  - in-sample vs out-of-sample separation
  - realistic cost and slippage assumptions
  - benchmark comparison
- Output: approve / reject / revise verdict

## 3. Code Update
- Owner: Code Maintainer
- Input: approved research + validator verdict
- Output: minimal code diff, tests, change note

## 4. Approval Gate
- Strategy logic is not promoted unless:
  - research record exists
  - backtest verdict is approval or conditional approval
  - code tests pass
  - change note is written

## 5. Production Promotion
- Update version markers in config and registry
- Archive prior version assumptions
- Record activation date and affected scripts
- Daily operation may only use approved production factors and rules
