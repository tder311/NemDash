"""Parametric bid-band calculator for NEM BESS dispatch.

For each interval in the forecast horizon, traces the LP's optimal discharge
(and charge) quantity as the *local* spot price varies — holding every other
interval at its forecast value. The resulting step function IS the optimal
bid curve in AEMO's "MW offered at this price band" format, because the LP's
natural intertemporal coordination is baked in.

This is the textbook self-scheduling-under-intertemporal-constraints approach
used for hydro / BESS / pumped-storage bidding.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional, Tuple

import pandas as pd

from .optimiser import DispatchInputs, optimise_dispatch

# Default NEM-flavoured grid spanning floor → cap. The 10-band cap mirrors
# AEMO's PRICEBAND1..PRICEBAND10 structure, though the actual prices an
# operator submits are free within [MPF, MPC].
DEFAULT_PRICE_GRID: List[float] = [
    -1000.0, -50.0, 0.0, 50.0, 100.0, 200.0, 500.0, 1000.0, 5000.0, 16600.0,
]


@dataclass
class IntervalBidCurve:
    """LP-derived supply/demand response at one interval."""

    interval_datetime: pd.Timestamp
    forecast_price: float
    # One row per price in the grid: (band_price, discharge_mw, charge_mw).
    # discharge_mw is non-decreasing, charge_mw is non-increasing across the grid.
    grid: List[Tuple[float, float, float]] = field(default_factory=list)

    def discharge_curve(self) -> List[Tuple[float, float]]:
        """Return only (price, discharge_mw) pairs — the offer curve."""
        return [(p, d) for (p, d, _c) in self.grid]

    def charge_curve(self) -> List[Tuple[float, float]]:
        """Return only (price, charge_mw) pairs — the bid (buy) curve."""
        return [(p, c) for (p, _d, c) in self.grid]

    def discharge_tranches(self) -> List[float]:
        """MW to offer AT each band (BANDAVAIL on the offer side).

        Tranches are the differences between consecutive cumulative discharge
        values: the first band gets what's offered at the lowest price; each
        subsequent band gets the incremental MW that comes online at that
        price threshold. Tranches sum to the maximum discharge MW.
        """
        ds = [d for (_p, d, _c) in self.grid]
        tranches = [max(0.0, ds[0])]
        for i in range(1, len(ds)):
            tranches.append(max(0.0, ds[i] - ds[i - 1]))
        return tranches

    def charge_tranches(self) -> List[float]:
        """MW to bid AT each band (BANDAVAIL on the load side).

        For the load curve (charge), the LP wants more MW as price drops. The
        tranche at band k = additional MW that comes in as price falls below
        band k+1 down to band k. The highest band gets the residual (typically 0).
        """
        cs = [c for (_p, _d, c) in self.grid]
        tranches: List[float] = []
        for i in range(len(cs) - 1):
            tranches.append(max(0.0, cs[i] - cs[i + 1]))
        tranches.append(max(0.0, cs[-1]))
        return tranches


@dataclass
class BidBandResult:
    """Bid curves over a horizon plus the inputs they were computed against."""

    curves: List[IntervalBidCurve]
    inputs: DispatchInputs
    horizon_intervals: int
    price_grid: List[float]
    n_lp_solves: int


def compute_bid_curves(
    prices: pd.Series,
    inputs: DispatchInputs,
    horizon_intervals: Optional[int] = None,
    price_grid: Optional[List[float]] = None,
    start_offset: int = 0,
) -> BidBandResult:
    """Compute parametric bid curves for ``horizon_intervals`` intervals starting at ``start_offset``.

    The LP always sees the FULL ``prices`` series (so its intertemporal
    coordination is correct); ``start_offset`` and ``horizon_intervals`` only
    control which intervals get their bid curves computed.

    Parameters
    ----------
    prices
        Forecast price series indexed by ``interval_datetime``.
    inputs
        Battery configuration (same dataclass the optimiser uses).
    horizon_intervals
        How many intervals to build curves for (defaults to min(48, len-start_offset)).
        Solves = horizon_intervals × len(price_grid), so keep it bounded.
    price_grid
        Candidate prices to sweep (defaults to NEM-flavoured 10-band grid).
    start_offset
        Interval index to start computing curves at (0 = first forecast interval).
        Used to compute one day's worth at a time over a multi-day horizon.

    Returns
    -------
    BidBandResult with one IntervalBidCurve per interval and the price grid used.
    """
    if prices is None or len(prices) == 0:
        raise ValueError("empty price series")
    if price_grid is None:
        price_grid = list(DEFAULT_PRICE_GRID)
    if not price_grid:
        raise ValueError("price_grid is empty")
    if start_offset < 0:
        raise ValueError("start_offset must be >= 0")

    prices = prices.dropna().sort_index()
    if start_offset >= len(prices):
        raise ValueError(f"start_offset {start_offset} is beyond forecast horizon ({len(prices)} intervals)")

    available = len(prices) - start_offset
    H = min(horizon_intervals if horizon_intervals is not None else 48, available)

    curves: List[IntervalBidCurve] = []
    for i in range(start_offset, start_offset + H):
        interval_dt = prices.index[i]
        base_price = float(prices.iloc[i])
        grid: List[Tuple[float, float, float]] = []
        for p in price_grid:
            modified = prices.copy()
            modified.iloc[i] = p
            result = optimise_dispatch(modified, inputs)
            row = result.schedule.iloc[i]
            grid.append((float(p), float(row["discharge_mw"]), float(row["charge_mw"])))
        curves.append(
            IntervalBidCurve(
                interval_datetime=interval_dt,
                forecast_price=base_price,
                grid=grid,
            )
        )

    return BidBandResult(
        curves=curves,
        inputs=inputs,
        horizon_intervals=H,
        price_grid=list(price_grid),
        n_lp_solves=H * len(price_grid),
    )
