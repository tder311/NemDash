"""Unit tests for the parametric bid-band calculator.

The bid curve is the LP's optimal discharge/charge as a function of the local
spot price — these tests verify the curve has the properties the LP's
convexity and intertemporal structure imply.
"""

import numpy as np
import pandas as pd
import pytest

from app.bid_bands import (
    NEM_MPC,
    NEM_MPF,
    compute_bid_curves,
    compute_kink_grid,
)
from app.optimiser import DispatchInputs


def _prices(values):
    idx = pd.date_range("2026-01-01", periods=len(values), freq="30min")
    return pd.Series(values, index=idx, dtype=float)


# Small grid + horizon so the test suite stays under a second even with re-solves.
SMALL_GRID = [-500.0, 0.0, 50.0, 200.0, 1000.0, 10000.0]


def test_bid_curve_structure():
    res = compute_bid_curves(
        _prices([50.0, 200.0, 100.0, 30.0]),
        DispatchInputs(power_mw=10, energy_mwh=20, eff_rt=0.9, cyclic=True),
        horizon_intervals=4,
        price_grid=SMALL_GRID,
    )
    assert res.horizon_intervals == 4
    assert len(res.curves) == 4
    assert res.n_lp_solves == 4 * len(SMALL_GRID)
    for c in res.curves:
        assert len(c.grid) == len(SMALL_GRID)
        # Each row is (price, discharge, charge)
        assert all(len(row) == 3 for row in c.grid)


def test_discharge_monotone_in_price():
    """LP convexity: discharge[i] is non-decreasing as price[i] rises."""
    res = compute_bid_curves(
        _prices([80.0, 80.0, 80.0, 80.0]),
        DispatchInputs(power_mw=10, energy_mwh=20, eff_rt=0.9, cyclic=True),
        horizon_intervals=2,
        price_grid=SMALL_GRID,
    )
    for curve in res.curves:
        discharges = [d for (_p, d, _c) in curve.grid]
        for a, b in zip(discharges, discharges[1:]):
            assert a <= b + 1e-6, f"discharge decreased: {discharges}"


def test_charge_anti_monotone_in_price():
    """LP convexity: charge[i] is non-increasing as price[i] rises."""
    res = compute_bid_curves(
        _prices([80.0, 80.0, 80.0, 80.0]),
        DispatchInputs(power_mw=10, energy_mwh=20, eff_rt=0.9, cyclic=True),
        horizon_intervals=2,
        price_grid=SMALL_GRID,
    )
    for curve in res.curves:
        charges = [c for (_p, _d, c) in curve.grid]
        for a, b in zip(charges, charges[1:]):
            assert a + 1e-6 >= b, f"charge increased with price: {charges}"


def test_extreme_high_price_means_max_discharge():
    """At MPC-grade local price, the LP should discharge to the power limit."""
    res = compute_bid_curves(
        _prices([50.0, 50.0, 50.0, 50.0]),
        DispatchInputs(power_mw=10, energy_mwh=20, eff_rt=1.0, cyclic=False),
        horizon_intervals=1,
        price_grid=[50.0, 10000.0],
    )
    discharge_at_high = res.curves[0].grid[-1][1]
    assert discharge_at_high == pytest.approx(10.0, abs=1e-6)


def test_extreme_low_price_means_max_charge():
    """At MPF-grade local price, the LP should charge to the power limit."""
    res = compute_bid_curves(
        _prices([50.0, 50.0, 50.0, 50.0]),
        DispatchInputs(power_mw=10, energy_mwh=20, eff_rt=1.0, cyclic=False),
        horizon_intervals=1,
        price_grid=[-1000.0, 50.0],
    )
    charge_at_low = res.curves[0].grid[0][2]
    assert charge_at_low == pytest.approx(10.0, abs=1e-6)


def test_empty_prices_raises():
    with pytest.raises(ValueError):
        compute_bid_curves(
            pd.Series(dtype=float),
            DispatchInputs(power_mw=10, energy_mwh=20),
        )


def test_tranches_sum_to_endpoints_and_are_non_negative():
    """Discharge tranches sum to max discharge; charge tranches sum to max charge.
    Both must be non-negative (the tranches helper clamps to zero defensively)."""
    res = compute_bid_curves(
        _prices([50.0, 200.0, 100.0, 30.0]),
        DispatchInputs(power_mw=10, energy_mwh=20, eff_rt=0.9, cyclic=True),
        horizon_intervals=2,
        price_grid=SMALL_GRID,
    )
    for curve in res.curves:
        dt = curve.discharge_tranches()
        ct = curve.charge_tranches()
        assert all(t >= -1e-9 for t in dt)
        assert all(t >= -1e-9 for t in ct)
        # Sum of discharge tranches = max discharge (the highest-band cumulative value).
        max_discharge = curve.grid[-1][1]
        assert sum(dt) == pytest.approx(max_discharge, abs=1e-6)
        # Sum of charge tranches = max charge (the lowest-band cumulative value).
        max_charge = curve.grid[0][2]
        assert sum(ct) == pytest.approx(max_charge, abs=1e-6)


def test_start_offset_picks_the_right_intervals():
    """start_offset shifts which intervals get curves computed; LP still sees all prices."""
    res = compute_bid_curves(
        _prices([50.0, 200.0, 100.0, 30.0]),
        DispatchInputs(power_mw=10, energy_mwh=20, eff_rt=1.0, cyclic=True),
        horizon_intervals=2,
        price_grid=SMALL_GRID,
        start_offset=2,
    )
    # We should get curves for the 3rd and 4th intervals (indices 2 and 3).
    assert res.horizon_intervals == 2
    assert res.curves[0].interval_datetime == pd.Timestamp("2026-01-01 01:00")
    assert res.curves[0].forecast_price == pytest.approx(100.0)
    assert res.curves[1].forecast_price == pytest.approx(30.0)


def test_start_offset_beyond_horizon_raises():
    with pytest.raises(ValueError):
        compute_bid_curves(
            _prices([50.0, 200.0]),
            DispatchInputs(power_mw=10, energy_mwh=20),
            start_offset=5,
        )


# --------------------------------------------------------------------------- #
# Kink grid (merit-order density)
# --------------------------------------------------------------------------- #


def test_kink_grid_returns_10_bands_with_mpf_mpc_anchors():
    """Standard case: dense bid distribution → 8 interior centroids + MPF + MPC."""
    # Three dense clusters around $0 (coal), $80 (mid-merit), $300 (peaker).
    rng = np.random.default_rng(0)
    prices = np.concatenate([
        rng.normal(0, 5, 100),
        rng.normal(80, 5, 100),
        rng.normal(300, 10, 100),
    ])
    mws = np.full(len(prices), 50.0)
    bids = pd.DataFrame({"price": prices, "mw": mws})
    grid = compute_kink_grid(bids, k=8)
    assert len(grid) == 10
    assert grid[0] == NEM_MPF
    assert grid[-1] == NEM_MPC
    # Interior centroids should fall within the relevant range.
    for c in grid[1:-1]:
        assert -200 <= c <= 2000


def test_kink_grid_centroids_track_dense_clusters():
    """Centroids should sit near the actual cluster means, not somewhere random."""
    rng = np.random.default_rng(42)
    prices = np.concatenate([
        rng.normal(50, 2, 200),     # tight cluster at $50
        rng.normal(500, 5, 200),    # tight cluster at $500
    ])
    mws = np.full(len(prices), 100.0)
    bids = pd.DataFrame({"price": prices, "mw": mws})
    grid = compute_kink_grid(bids, k=2)
    interior = grid[1:-1]
    assert len(interior) == 2
    # The two centroids should be close to $50 and $500.
    assert min(interior) == pytest.approx(50, abs=10)
    assert max(interior) == pytest.approx(500, abs=15)


def test_kink_grid_mw_weight_pulls_centroids_to_heavy_volume():
    """A small high-MW cluster should outweigh many low-MW points."""
    bids = pd.DataFrame({
        "price": [10.0] * 100 + [200.0] * 5,    # 100 points at $10, 5 at $200
        "mw":    [1.0]  * 100 + [1000.0] * 5,    # but the $200 points carry 50x the MW
    })
    grid = compute_kink_grid(bids, k=2)
    interior = grid[1:-1]
    # Heavier (MW-weighted) cluster should anchor one centroid near $200.
    assert any(abs(c - 200) < 20 for c in interior)


def test_kink_grid_empty_returns_just_anchors():
    grid = compute_kink_grid(pd.DataFrame({"price": [], "mw": []}))
    assert grid == [NEM_MPF, NEM_MPC]


def test_kink_grid_filters_out_of_range_prices():
    """A massive cluster outside [range_lo, range_hi] should not pull centroids."""
    bids = pd.DataFrame({
        "price": [-5000.0] * 50 + [50.0] * 50 + [50000.0] * 50,
        "mw":    [100.0]   * 50 + [10.0] * 50 + [100.0]   * 50,
    })
    grid = compute_kink_grid(bids, k=2)
    interior = grid[1:-1]
    # Only the $50 cluster sits in [-200, 2000]; both centroids should land near it.
    assert all(-200 <= c <= 2000 for c in interior)
