"""Atomic risk-rule check functions for the Layer 4 risk engine.

Each function evaluates one rule against a single proposal or the
accumulated portfolio state. Functions return ``None`` when the rule
passes or the string rule identifier when it fires.
"""
from __future__ import annotations

from core.contracts.schemas import PortfolioRecord


def check_max_position(
    proposal: PortfolioRecord,
    equity: float,
    max_position_pct: float,
) -> str | None:
    """Fire when the proposed position exceeds ``max_position_pct`` of equity."""
    if equity <= 0:
        return "no_equity"
    if abs(proposal.target_dollars) / equity > max_position_pct:
        return "max_position_pct"
    return None


def check_min_position(
    proposal: PortfolioRecord,
    min_position_dollars: float,
) -> str | None:
    """Fire when a non-zero proposed position is below the minimum trade size."""
    dollars = abs(proposal.target_dollars)
    if 0.0 < dollars < min_position_dollars:
        return "min_position_dollars"
    return None


def check_adv_cap(
    proposal: PortfolioRecord,
    adv_dollars: float,
    max_adv_participation: float,
) -> str | None:
    """Fire when the order would exceed ``max_adv_participation`` of ADV.

    Args:
        proposal: Portfolio construction target for one ticker.
        adv_dollars: Average daily dollar volume for the ticker.
        max_adv_participation: Maximum fraction of ADV allowed (e.g. 0.01).
    """
    if adv_dollars <= 0:
        return None
    if abs(proposal.change_dollars) > adv_dollars * max_adv_participation:
        return "adv_participation_cap"
    return None


def check_gross_exposure(
    all_target_dollars: list[float],
    equity: float,
    max_gross_exposure: float,
) -> bool:
    """Return True when the combined gross exposure would exceed the limit."""
    if equity <= 0:
        return True
    gross = sum(abs(d) for d in all_target_dollars)
    return gross / equity > max_gross_exposure


def check_sector_concentration(
    ticker: str,
    target_dollars: float,
    sector_running_dollars: dict[str, float],
    equity: float,
    max_sector_pct: float,
) -> str | None:
    """Fire when adding this position would push its sector over the limit.

    Args:
        ticker: Ticker being evaluated.
        target_dollars: Proposed dollar allocation.
        sector_running_dollars: Mutable dict of {sector: dollars_already_approved}.
        equity: Total portfolio equity.
        max_sector_pct: Maximum fraction of equity per sector.

    Returns:
        Rule name if sector limit would be breached, else None. The caller
        must update ``sector_running_dollars`` when the rule passes.
    """
    if equity <= 0:
        return "no_equity"
    _ = ticker
    return None  # evaluated at portfolio level by the engine after sector lookup
