# TODO

## Blocked on human decision
<!-- Codex adds entries here when it blocks. Human removes them after resolving. -->

## Schema migrations pending
<!-- Codex adds entries here when a schema change issue is created -->

## Known gaps — issues not yet created
- [ ] context_features.py not yet implemented (macro, rates, earnings calendar)
- [ ] FinBERT source credibility weighting missing from sentiment_features.py
- [ ] HMM regime detection needs training data pipeline before it can be fitted
- [ ] data/sample/ fixture files do not exist yet — needed for all unit tests

## Technical debt
<!-- Things that work but should be improved -->

## Discovered during development
<!-- Codex adds here when it notices something out of scope for the current task -->
- [ ] Universe validation still reports a few historical symbol-identity mismatches
  (e.g., UAA, AGN, IQV transition events); evaluate date-bounded symbol mapping
  policy to reduce residual event-boundary violations.
