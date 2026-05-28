"""Parametric bid-band calculator for NEM BESS dispatch.

For each interval in the forecast horizon, traces the LP's optimal discharge
(and charge) quantity as the *local* spot price varies — holding every other
interval at its forecast value. The resulting step function IS the optimal
bid curve in AEMO's "MW offered at this price band" format, because the LP's
natural intertemporal coordination is baked in.

This is the textbook self-scheduling-under-intertemporal-constraints approach
used for hydro / BESS / pumped-storage bidding.

The price grid itself can be derived from real regional bids using
``compute_kink_grid`` — k-means clustering of bid prices weighted by MW
availability picks out the supply-curve kinks where price-setting actually
transitions, giving a much sharper grid than a hardcoded list.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional, Tuple

import numpy as np
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


# --------------------------------------------------------------------------- #
# Merit-order-density-derived price grid
# --------------------------------------------------------------------------- #

# AEMO floor and cap anchors. MPC drifts upward over time (CPI-linked); update
# annually if the auto-grid starts looking off-by-one at the tail.
NEM_MPF: float = -1000.0
NEM_MPC: float = 16600.0


async def fetch_regional_bid_distribution(
    db, region: str, lookback_days: int = 7
) -> pd.DataFrame:
    """Return long-format (price, mw) rows for every band offer in ``region``
    over the last ``lookback_days``.

    Joins ``bid_per_offer`` (BANDAVAIL per interval) with ``bid_day_offer``
    (PRICEBAND per day) on (duid, settlementdate, offerdate), filters DUIDs
    by ``generator_info.region``, then unpivots the 10 bands into long form.
    """
    short = region[:-1] if region.endswith("1") else region  # NSW1 -> NSW
    lookback_days = int(lookback_days)
    band_cols_p = ", ".join(f"bdo.priceband{i}" for i in range(1, 11))
    band_cols_a = ", ".join(f"bpo.bandavail{i}" for i in range(1, 11))
    sql = f"""
        SELECT {band_cols_p}, {band_cols_a}
        FROM bid_per_offer bpo
        JOIN bid_day_offer bdo
          ON bdo.duid = bpo.duid
         AND bdo.settlementdate = bpo.settlementdate::date
         AND bdo.offerdate = bpo.offerdate
        JOIN generator_info gi ON gi.duid = bpo.duid
        WHERE gi.region = $1
          AND bpo.settlementdate >= NOW() - INTERVAL '{lookback_days} days'
    """
    async with db._pool.acquire() as conn:
        rows = await conn.fetch(sql, short)

    if not rows:
        return pd.DataFrame(columns=["price", "mw"])

    wide = pd.DataFrame([dict(r) for r in rows])
    parts = []
    for i in range(1, 11):
        p = wide[[f"priceband{i}", f"bandavail{i}"]].rename(
            columns={f"priceband{i}": "price", f"bandavail{i}": "mw"}
        )
        parts.append(p)
    long = pd.concat(parts, ignore_index=True)
    long = long.dropna(subset=["price", "mw"])
    long = long[long["mw"] > 0]
    return long.reset_index(drop=True)


def compute_kink_grid(
    bid_distribution: pd.DataFrame,
    k: int = 8,
    mpf: float = NEM_MPF,
    mpc: float = NEM_MPC,
    range_lo: float = -200.0,
    range_hi: float = 2000.0,
) -> List[float]:
    """Cluster regional bid prices weighted by MW; return MPF + k centroids + MPC = 10 bands.

    The k-means clustering with sample_weight=mw places centroids at the
    bid-MW *density* — i.e. where the cumulative supply curve has the most
    weight. The interior is restricted to ``[range_lo, range_hi]`` (cents
    BESS competes in), and MPF/MPC bookend the grid for safety.
    """
    if bid_distribution is None or bid_distribution.empty:
        return [mpf, mpc]

    in_range = bid_distribution[
        (bid_distribution["price"] >= range_lo)
        & (bid_distribution["price"] <= range_hi)
        & (bid_distribution["mw"] > 0)
    ]
    if in_range.empty:
        return [mpf, mpc]

    # Fewer distinct prices than clusters -> use the distinct prices directly.
    unique_prices = sorted(in_range["price"].unique().tolist())
    if len(unique_prices) <= k:
        return [mpf] + [round(p, 1) for p in unique_prices] + [mpc]

    from sklearn.cluster import KMeans

    X = in_range["price"].to_numpy().reshape(-1, 1).astype(float)
    w = in_range["mw"].to_numpy().astype(float)
    km = KMeans(n_clusters=k, n_init=10, random_state=42)
    km.fit(X, sample_weight=w)
    centroids = sorted(float(c) for c in km.cluster_centers_.flatten())
    # Snap to nearest whole dollar for readability; AEMO accepts cents but
    # operators almost always pick round numbers.
    centroids = [round(c, 0) for c in centroids]
    return [mpf] + centroids + [mpc]


async def derived_grid(db, region: str, lookback_days: int = 7) -> List[float]:
    """End-to-end: pull recent regional bids and return the kink-derived 10-band grid."""
    bids = await fetch_regional_bid_distribution(db, region, lookback_days)
    return compute_kink_grid(bids)
