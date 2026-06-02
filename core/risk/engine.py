"""Layer 4 hard-rule risk engine.

Applies deterministic position, exposure, sector, and liquidity constraints
to a list of PortfolioRecord proposals and returns ApprovedOrderRecords. No
ML is involved; all rules are threshold-based and auditable.

Usage::

    engine = RiskEngine(RiskConfig(max_position_pct=0.08))
    approved = engine.apply(
        proposals,
        equity=100_000.0,
        sector_map={"AAPL": "Technology", "JPM": "Financials"},
        adv_map={"AAPL": 5_000_000.0, "JPM": 2_000_000.0},
    )
"""
from __future__ import annotations

from collections import defaultdict
from collections.abc import Sequence
from dataclasses import dataclass

from core.contracts.schemas import ActionType, ApprovedOrderRecord, PortfolioRecord
from core.risk.rules import check_adv_cap, check_max_position, check_min_position


@dataclass(frozen=True)
class RiskConfig:
    """Configurable thresholds for all risk rules."""

    max_position_pct: float = 0.10
    """Maximum single-position weight as a fraction of total equity (default 10 %)."""

    min_position_dollars: float = 100.0
    """Minimum non-zero position size; smaller orders are rejected (default $100)."""

    max_gross_exposure: float = 1.0
    """Maximum sum of |target_dollars| / equity (default 1.0 = fully invested long-only)."""

    max_sector_pct: float = 0.30
    """Maximum weight of any single GICS sector as a fraction of equity (default 30 %)."""

    max_adv_participation: float = 0.01
    """Maximum order size as a fraction of average daily volume (default 1 %)."""


class RiskEngine:
    """Stateless risk engine that evaluates proposals against configurable hard rules.

    The engine runs in two passes:
    1. Per-proposal rules (position size, min size, ADV cap).
    2. Portfolio-level rules (gross exposure, sector concentration) applied to
       surviving proposals in descending |target_dollars| order, so the largest
       positions are approved first and smaller ones are cut when limits are full.
    """

    def __init__(self, config: RiskConfig | None = None) -> None:
        self.config = config or RiskConfig()

    def apply(
        self,
        proposals: Sequence[PortfolioRecord],
        *,
        equity: float,
        sector_map: dict[str, str] | None = None,
        adv_map: dict[str, float] | None = None,
    ) -> list[ApprovedOrderRecord]:
        """Evaluate proposals and return one ApprovedOrderRecord per proposal.

        Args:
            proposals: Pre-risk portfolio targets from Layer 3.
            equity: Total account equity in dollars.
            sector_map: Optional ticker → sector string. Sector concentration
                rules are skipped when not provided.
            adv_map: Optional ticker → average daily dollar volume. ADV cap
                is skipped when not provided.

        Returns:
            One ApprovedOrderRecord per input proposal. Rejected proposals
            carry ``approved=False`` and the names of the rules that fired.
        """
        sector_map = sector_map or {}
        adv_map = adv_map or {}

        # Pass 1: per-proposal rules
        survivors: list[tuple[PortfolioRecord, list[str]]] = []
        rejected: list[ApprovedOrderRecord] = []

        for proposal in proposals:
            fired: list[str] = []

            rule = check_max_position(proposal, equity, self.config.max_position_pct)
            if rule:
                fired.append(rule)

            rule = check_min_position(proposal, self.config.min_position_dollars)
            if rule:
                fired.append(rule)

            adv = adv_map.get(proposal.ticker, 0.0)
            rule = check_adv_cap(proposal, adv, self.config.max_adv_participation)
            if rule:
                fired.append(rule)

            if fired:
                rejected.append(_make_order(proposal, approved=False, rules=fired))
            else:
                survivors.append((proposal, []))

        # Pass 2: portfolio-level rules applied in descending size order
        survivors.sort(key=lambda item: abs(item[0].target_dollars), reverse=True)

        approved: list[ApprovedOrderRecord] = []
        gross_dollars = 0.0
        sector_dollars: dict[str, float] = defaultdict(float)

        for proposal, rules in survivors:
            extra_rules: list[str] = []

            # Gross exposure check
            if equity > 0:
                projected_gross = gross_dollars + abs(proposal.target_dollars)
                if projected_gross / equity > self.config.max_gross_exposure:
                    extra_rules.append("max_gross_exposure")

            # Sector concentration check
            sector = sector_map.get(proposal.ticker)
            if sector and equity > 0:
                projected_sector = sector_dollars[sector] + abs(proposal.target_dollars)
                if projected_sector / equity > self.config.max_sector_pct:
                    extra_rules.append("max_sector_pct")

            if extra_rules:
                rejected.append(_make_order(proposal, approved=False, rules=extra_rules))
            else:
                gross_dollars += abs(proposal.target_dollars)
                if sector:
                    sector_dollars[sector] += abs(proposal.target_dollars)
                approved.append(_make_order(proposal, approved=True, rules=[]))

        # Maintain original input order in the output
        ticker_to_order: dict[str, ApprovedOrderRecord] = {}
        for order in rejected + approved:
            ticker_to_order[order.ticker] = order

        return [ticker_to_order[p.ticker] for p in proposals if p.ticker in ticker_to_order]


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _make_order(
    proposal: PortfolioRecord,
    *,
    approved: bool,
    rules: list[str],
) -> ApprovedOrderRecord:
    """Convert a PortfolioRecord into an ApprovedOrderRecord."""
    if not approved:
        action = ActionType.REJECT
        reason = "; ".join(rules) if rules else "rejected"
    elif proposal.change_dollars > 0:
        action = ActionType.BUY
        reason = None
    elif proposal.change_dollars < 0:
        action = ActionType.SELL
        reason = None
    else:
        action = ActionType.HOLD
        reason = None

    return ApprovedOrderRecord(
        date=proposal.date,
        ticker=proposal.ticker,
        action=action,
        target_dollars=proposal.target_dollars,
        approved=approved,
        rules_triggered=list(rules),
        reason=reason,
    )
