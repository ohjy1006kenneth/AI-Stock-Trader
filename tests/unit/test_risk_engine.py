"""Unit tests for the Layer 4 risk engine."""
from __future__ import annotations

from core.contracts.schemas import ActionType, ApprovedOrderRecord, PortfolioRecord
from core.risk.engine import RiskConfig, RiskEngine
from core.risk.rules import check_adv_cap, check_max_position, check_min_position

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _proposal(
    ticker: str,
    *,
    target_dollars: float,
    current_dollars: float = 0.0,
    date: str = "2025-01-10",
    weight: float | None = None,
) -> PortfolioRecord:
    change = target_dollars - current_dollars
    if weight is None:
        weight = target_dollars / 100_000.0
    return PortfolioRecord(
        date=date,
        ticker=ticker,
        weight=weight,
        target_dollars=target_dollars,
        current_dollars=current_dollars,
        change_dollars=change,
    )


# ---------------------------------------------------------------------------
# Rule-level unit tests
# ---------------------------------------------------------------------------


class TestCheckMaxPosition:
    def test_passes_within_limit(self) -> None:
        p = _proposal("AAPL", target_dollars=5_000.0)
        assert check_max_position(p, equity=100_000.0, max_position_pct=0.10) is None

    def test_fires_at_limit_breach(self) -> None:
        p = _proposal("AAPL", target_dollars=11_000.0)
        assert check_max_position(p, equity=100_000.0, max_position_pct=0.10) == "max_position_pct"

    def test_fires_on_zero_equity(self) -> None:
        p = _proposal("AAPL", target_dollars=1.0)
        assert check_max_position(p, equity=0.0, max_position_pct=0.10) == "no_equity"

    def test_uses_absolute_value(self) -> None:
        p = _proposal("AAPL", target_dollars=-15_000.0)
        assert check_max_position(p, equity=100_000.0, max_position_pct=0.10) == "max_position_pct"


class TestCheckMinPosition:
    def test_passes_when_above_minimum(self) -> None:
        p = _proposal("AAPL", target_dollars=500.0)
        assert check_min_position(p, min_position_dollars=100.0) is None

    def test_fires_when_below_minimum(self) -> None:
        p = _proposal("AAPL", target_dollars=50.0)
        assert check_min_position(p, min_position_dollars=100.0) == "min_position_dollars"

    def test_passes_when_zero_target(self) -> None:
        p = _proposal("AAPL", target_dollars=0.0)
        assert check_min_position(p, min_position_dollars=100.0) is None

    def test_passes_exactly_at_minimum(self) -> None:
        p = _proposal("AAPL", target_dollars=100.0)
        assert check_min_position(p, min_position_dollars=100.0) is None


class TestCheckAdvCap:
    def test_passes_within_adv(self) -> None:
        p = _proposal("AAPL", target_dollars=100.0, current_dollars=0.0)
        assert check_adv_cap(p, adv_dollars=100_000.0, max_adv_participation=0.01) is None

    def test_fires_when_over_adv(self) -> None:
        p = _proposal("AAPL", target_dollars=2_000.0, current_dollars=0.0)
        assert (
            check_adv_cap(p, adv_dollars=100_000.0, max_adv_participation=0.01)
            == "adv_participation_cap"
        )

    def test_passes_when_adv_zero(self) -> None:
        p = _proposal("AAPL", target_dollars=1_000.0)
        assert check_adv_cap(p, adv_dollars=0.0, max_adv_participation=0.01) is None

    def test_uses_change_dollars_not_target(self) -> None:
        # change = 500 - 400 = 100; 100 / 100_000 = 0.001 < 0.01 → pass
        p = _proposal("AAPL", target_dollars=500.0, current_dollars=400.0)
        assert check_adv_cap(p, adv_dollars=100_000.0, max_adv_participation=0.01) is None


# ---------------------------------------------------------------------------
# RiskEngine integration tests
# ---------------------------------------------------------------------------


class TestRiskEngine:
    def _engine(self, **kwargs: object) -> RiskEngine:
        return RiskEngine(RiskConfig(**kwargs))

    def test_approves_clean_proposals(self) -> None:
        engine = self._engine()
        proposals = [
            _proposal("AAPL", target_dollars=5_000.0),
            _proposal("MSFT", target_dollars=4_000.0),
        ]
        results = engine.apply(proposals, equity=100_000.0)
        assert all(r.approved for r in results)

    def test_rejects_oversized_position(self) -> None:
        engine = self._engine(max_position_pct=0.05)
        proposals = [_proposal("AAPL", target_dollars=10_000.0)]
        results = engine.apply(proposals, equity=100_000.0)
        assert results[0].approved is False
        assert "max_position_pct" in results[0].rules_triggered

    def test_rejects_below_minimum_size(self) -> None:
        engine = self._engine(min_position_dollars=200.0)
        proposals = [_proposal("AAPL", target_dollars=50.0)]
        results = engine.apply(proposals, equity=100_000.0)
        assert results[0].approved is False
        assert "min_position_dollars" in results[0].rules_triggered

    def test_rejects_adv_breach(self) -> None:
        engine = self._engine(max_adv_participation=0.01)
        proposals = [_proposal("TINY", target_dollars=2_000.0)]
        results = engine.apply(
            proposals,
            equity=100_000.0,
            adv_map={"TINY": 10_000.0},  # 2000 / 10000 = 20% > 1%
        )
        assert results[0].approved is False
        assert "adv_participation_cap" in results[0].rules_triggered

    def test_rejects_gross_exposure_overflow(self) -> None:
        # Raise max_position_pct so both proposals survive per-proposal checks;
        # combined 60k + 50k = 110k on 100k equity exceeds the 100% gross limit.
        engine = self._engine(max_gross_exposure=1.0, max_position_pct=0.70)
        proposals = [
            _proposal("AAPL", target_dollars=60_000.0),
            _proposal("MSFT", target_dollars=50_000.0),
        ]
        results = engine.apply(proposals, equity=100_000.0)
        approved = [r for r in results if r.approved]
        rejected = [r for r in results if not r.approved]
        assert len(approved) == 1
        assert len(rejected) == 1
        assert "max_gross_exposure" in rejected[0].rules_triggered

    def test_rejects_sector_concentration(self) -> None:
        engine = self._engine(max_sector_pct=0.25, max_position_pct=1.0)
        proposals = [
            _proposal("AAPL", target_dollars=20_000.0),
            _proposal("MSFT", target_dollars=20_000.0),
        ]
        sector_map = {"AAPL": "Technology", "MSFT": "Technology"}
        results = engine.apply(proposals, equity=100_000.0, sector_map=sector_map)
        approved = [r for r in results if r.approved]
        rejected = [r for r in results if not r.approved]
        assert len(approved) == 1
        assert len(rejected) == 1
        assert "max_sector_pct" in rejected[0].rules_triggered

    def test_output_preserves_input_order(self) -> None:
        engine = self._engine()
        tickers = ["Z", "A", "M", "B"]
        proposals = [_proposal(t, target_dollars=1_000.0) for t in tickers]
        results = engine.apply(proposals, equity=100_000.0)
        assert [r.ticker for r in results] == tickers

    def test_buy_action_on_increase(self) -> None:
        engine = self._engine()
        proposals = [_proposal("AAPL", target_dollars=5_000.0, current_dollars=1_000.0)]
        results = engine.apply(proposals, equity=100_000.0)
        assert results[0].action == ActionType.BUY

    def test_sell_action_on_decrease(self) -> None:
        engine = self._engine()
        proposals = [_proposal("AAPL", target_dollars=1_000.0, current_dollars=5_000.0)]
        results = engine.apply(proposals, equity=100_000.0)
        assert results[0].action == ActionType.SELL

    def test_hold_action_when_no_change(self) -> None:
        engine = self._engine()
        proposals = [_proposal("AAPL", target_dollars=5_000.0, current_dollars=5_000.0)]
        results = engine.apply(proposals, equity=100_000.0)
        assert results[0].action == ActionType.HOLD

    def test_rejected_action_is_reject(self) -> None:
        engine = self._engine(max_position_pct=0.01)
        proposals = [_proposal("AAPL", target_dollars=5_000.0)]
        results = engine.apply(proposals, equity=100_000.0)
        assert results[0].action == ActionType.REJECT

    def test_empty_proposals_returns_empty(self) -> None:
        engine = self._engine()
        assert engine.apply([], equity=100_000.0) == []

    def test_missing_sector_map_skips_sector_rule(self) -> None:
        engine = self._engine(max_sector_pct=0.01)
        proposals = [
            _proposal("AAPL", target_dollars=5_000.0),
            _proposal("MSFT", target_dollars=5_000.0),
        ]
        results = engine.apply(proposals, equity=100_000.0)
        assert all(r.approved for r in results)

    def test_multiple_rules_can_fire_on_same_proposal(self) -> None:
        engine = self._engine(max_position_pct=0.01, min_position_dollars=10_000.0)
        proposals = [_proposal("AAPL", target_dollars=5_000.0)]
        results = engine.apply(proposals, equity=100_000.0)
        assert not results[0].approved
        assert len(results[0].rules_triggered) >= 1

    def test_all_proposals_rejected_on_no_equity(self) -> None:
        engine = self._engine()
        proposals = [_proposal("AAPL", target_dollars=1_000.0)]
        results = engine.apply(proposals, equity=0.0)
        assert not results[0].approved

    def test_approved_order_record_is_valid_schema(self) -> None:
        engine = self._engine()
        proposals = [_proposal("AAPL", target_dollars=1_000.0)]
        results = engine.apply(proposals, equity=100_000.0)
        record = results[0]
        assert isinstance(record, ApprovedOrderRecord)
        assert record.ticker == "AAPL"
        assert isinstance(record.rules_triggered, list)
