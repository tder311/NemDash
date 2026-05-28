"""Unit tests for the BESS dispatch optimiser.

Synthetic price series with known optimal patterns — these guard the LP body
against silent regressions in the modelling choices (RTE split, cyclic, no
arbitrage when uneconomic, etc.).
"""

import numpy as np
import pandas as pd
import pytest

from app.optimiser import DispatchInputs, optimise_dispatch


def _prices(values):
    idx = pd.date_range("2026-01-01", periods=len(values), freq="30min")
    return pd.Series(values, index=idx, dtype=float)


def test_worked_example_revenue_and_pattern():
    """The 4-interval worked example: revenue and dispatch pattern should match by-hand."""
    r = optimise_dispatch(
        _prices([50.0, 200.0, 100.0, 30.0]),
        DispatchInputs(power_mw=10, energy_mwh=20, eff_rt=1.0, cyclic=True),
    )
    assert r.solver_status == "Optimal"
    assert r.total_revenue == pytest.approx(1100.0, abs=1e-6)
    # Charge during cheap intervals, discharge during dear ones.
    assert r.schedule.loc[0, "charge_mw"] > 0
    assert r.schedule.loc[0, "discharge_mw"] == pytest.approx(0)
    assert r.schedule.loc[1, "discharge_mw"] > 0
    assert r.schedule.loc[2, "discharge_mw"] > 0
    assert r.schedule.loc[3, "charge_mw"] > 0
    # Cyclic boundary -> end SOC == starting SOC.
    assert r.schedule["soc_mwh"].iloc[-1] == pytest.approx(0.5 * 20, abs=1e-6)


def test_cyclic_off_lets_battery_drain_at_horizon():
    """Free end-SOC ≥ cyclic revenue (LP gains a final discharge of bonus revenue)."""
    cyclic = optimise_dispatch(
        _prices([50.0, 200.0, 100.0, 30.0]),
        DispatchInputs(power_mw=10, energy_mwh=20, eff_rt=1.0, cyclic=True),
    )
    free = optimise_dispatch(
        _prices([50.0, 200.0, 100.0, 30.0]),
        DispatchInputs(power_mw=10, energy_mwh=20, eff_rt=1.0, cyclic=False),
    )
    assert free.total_revenue >= cyclic.total_revenue - 1e-6
    assert free.schedule["soc_mwh"].iloc[-1] <= cyclic.schedule["soc_mwh"].iloc[-1] + 1e-6


def test_rte_loss_reduces_revenue():
    """Adding round-trip losses must not increase revenue for the same prices."""
    perfect = optimise_dispatch(
        _prices([50.0, 200.0, 100.0, 30.0]),
        DispatchInputs(power_mw=10, energy_mwh=20, eff_rt=1.0, cyclic=True),
    )
    lossy = optimise_dispatch(
        _prices([50.0, 200.0, 100.0, 30.0]),
        DispatchInputs(power_mw=10, energy_mwh=20, eff_rt=0.7, cyclic=True),
    )
    assert lossy.total_revenue < perfect.total_revenue


def test_no_arbitrage_when_spread_below_loss_threshold():
    """Narrow spread + lossy RTE: LP should refuse to act (or barely)."""
    r = optimise_dispatch(
        _prices([80.0, 82.0, 79.0, 81.0]),
        DispatchInputs(power_mw=10, energy_mwh=20, eff_rt=0.5, cyclic=True),
    )
    assert r.total_revenue == pytest.approx(0.0, abs=1e-6)
    assert r.schedule["charge_mw"].sum() < 1e-3
    assert r.schedule["discharge_mw"].sum() < 1e-3


def test_negative_prices_get_charged_into():
    """A negative price is 'paid to take energy' — the LP should charge there."""
    r = optimise_dispatch(
        _prices([-50.0, 100.0, 50.0, 0.0]),
        DispatchInputs(power_mw=10, energy_mwh=20, eff_rt=1.0, cyclic=True),
    )
    assert r.schedule.loc[0, "charge_mw"] > 0


def test_input_validation():
    px = _prices([50.0, 100.0])
    with pytest.raises(ValueError):
        optimise_dispatch(px, DispatchInputs(power_mw=0, energy_mwh=10))
    with pytest.raises(ValueError):
        optimise_dispatch(px, DispatchInputs(power_mw=10, energy_mwh=10, eff_rt=1.5))
    with pytest.raises(ValueError):
        optimise_dispatch(pd.Series(dtype=float), DispatchInputs(power_mw=10, energy_mwh=10))
