"""Single-asset BESS arbitrage dispatch optimiser.

Solves a linear program (PuLP + CBC):

    max  Σ price[t] · (discharge[t] - charge[t]) · dt
    s.t. SOC dynamics with round-trip losses,
         0 <= charge[t], discharge[t] <= P_max,
         0 <= SOC[t] <= E_max,
         optional cyclic boundary (SOC[T] == SOC[0]).

Used to (a) recommend dispatch over the model's 7-day price forecast, and
(b) compute the "cost of forecast error" by comparing forecast-driven dispatch
against perfect-foresight dispatch on realised PUBLIC prices.

This is a pure LP (no binaries) because with round-trip efficiency < 1 the
solver naturally avoids simultaneous charge+discharge — it would waste energy.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

import numpy as np
import pandas as pd

SETTLEMENT_HOURS: float = 0.5  # 30-min trading intervals


@dataclass
class DispatchInputs:
    """Battery + horizon parameters for the LP."""

    power_mw: float            # max charge/discharge power (MW)
    energy_mwh: float          # storage capacity (MWh) — duration = energy/power
    eff_rt: float = 0.85       # round-trip efficiency in (0, 1]
    soc0_frac: float = 0.5     # initial SOC as a fraction of energy_mwh
    cyclic: bool = True        # require end-of-horizon SOC == start SOC


@dataclass
class DispatchResult:
    """Solved dispatch schedule and headline numbers."""

    schedule: pd.DataFrame     # interval_datetime, charge_mw, discharge_mw, soc_mwh, net_mw, price, revenue
    total_revenue: float       # $ over the horizon
    n_cycles: float            # equivalent full cycles
    solver_status: str
    inputs: Optional[DispatchInputs] = None


def optimise_dispatch(prices: pd.Series, inputs: DispatchInputs) -> DispatchResult:
    """Find the revenue-maximising charge/discharge schedule for a price series.

    Parameters
    ----------
    prices
        Series indexed by ``interval_datetime`` (30-min, period-ending), values
        in $/MWh. Must be non-empty and sorted.
    inputs
        Battery configuration (see ``DispatchInputs``).

    Returns
    -------
    DispatchResult with the schedule, total revenue, equivalent cycles, and
    solver status.
    """
    import pulp

    # --- input prep ----------------------------------------------------------
    if prices is None or len(prices) == 0:
        raise ValueError("empty price series")
    prices = prices.dropna().sort_index()
    T = len(prices)
    dt = SETTLEMENT_HOURS
    P_max = float(inputs.power_mw)
    E_max = float(inputs.energy_mwh)
    if P_max <= 0 or E_max <= 0:
        raise ValueError("power_mw and energy_mwh must be positive")
    if not 0 < inputs.eff_rt <= 1:
        raise ValueError("eff_rt must be in (0, 1]")
    # Symmetric round-trip loss: charge stores eff_leg · charge,
    # discharge draws discharge / eff_leg.  eff_leg^2 = eff_rt.
    eff_leg = inputs.eff_rt ** 0.5
    soc0 = inputs.soc0_frac * E_max
    p = prices.to_numpy()

    # --- LP body -------------------------------------------------------------
    m = pulp.LpProblem("bess_arb", pulp.LpMaximize)

    # >>> YOUR CONTRIBUTION (learning mode) <<<
    #
    # GOAL IN ONE LINE: write the LP that maximises arbitrage revenue. The
    # maths is already in your head as a Modo dispatch person — this is really
    # just expressing it in PuLP. About 8-10 lines.
    #
    # WORKED EXAMPLE — 4 intervals, p = [50, 200, 100, 30] $/MWh, P_max=10,
    # E_max=20, RTE=1, cyclic=True:
    #   The LP should charge in the cheap intervals (50, 30), discharge in the
    #   dear ones (200, 100), respecting capacity, and end at the same SOC it
    #   started. CBC finds the exact split.
    #
    # PuLP CHEAT SHEET — only three patterns needed:
    #
    #     x = pulp.LpVariable("x", lowBound=0, upBound=10)        # one bounded var
    #     m += x + y == 5, "init"                                 # add an equality
    #     m += pulp.lpSum(p[i] * x[i] for i in range(T))          # set the objective
    #
    # Because m is LpMaximize, the objective is maximised — write revenue
    # directly, no negation.
    #
    # RECIPE — five steps, mostly transcription, ~1-3 lines each:
    #
    #   Step 1 — three variable lists (charge & discharge length T; soc length T+1):
    #       charge    = [pulp.LpVariable(f"c_{i}", 0, P_max) for i in range(T)]
    #       discharge = [pulp.LpVariable(f"d_{i}", 0, P_max) for i in range(T)]
    #       soc       = [pulp.LpVariable(f"s_{i}", 0, E_max) for i in range(T + 1)]
    #
    #   Step 2 — initial state:
    #       m += soc[0] == soc0, "init"
    #
    #   Step 3 — SOC dynamics (one constraint per interval, in a loop):
    #       for i in range(T):
    #           m += soc[i+1] == soc[i] + (eff_leg * charge[i] - discharge[i] / eff_leg) * dt, f"dyn_{i}"
    #
    #   Step 4 — cyclic boundary (only if inputs.cyclic):
    #       if inputs.cyclic:
    #           m += soc[T] == soc0, "cyclic"
    #
    #   Step 5 — objective (revenue over the horizon, $):
    #       m += pulp.lpSum(p[i] * (discharge[i] - charge[i]) * dt for i in range(T))
    #
    # DESIGN CHOICES — yours; the recipe above is the "symmetric + cyclic" default:
    #   * RTE SPLIT (Step 3 dynamics): symmetric splits the round-trip loss
    #     across both legs (sqrt(eff_rt) each, applied as eff_leg). The one-leg
    #     alternative puts the whole loss on discharge — replace
    #         eff_leg * charge[i]    with   charge[i]
    #         discharge[i] / eff_leg with   discharge[i] / inputs.eff_rt
    #     One-leg makes charging slightly more attractive (no entry tax), so
    #     arbitrage triggers at a smaller price spread.
    #   * CYCLIC BOUNDARY (Step 4): ON = honest backtest (battery can't give
    #     away free energy at horizon end). OFF = one bonus discharge of
    #     revenue at the tail — truer for "operate over this week, then stop"
    #     planning, but inflates apparent revenue in a rolling backtest.
    #   * No binary forbidding simultaneous charge+discharge: with eff_rt < 1
    #     the LP would never choose both at once (wastes energy and money), so
    #     the constraint is redundant and we keep it a fast pure LP.

    charge = [pulp.LpVariable(f"c_{i}", 0, P_max) for i in range(T)]
    discharge = [pulp.LpVariable(f"d_{i}", 0, P_max) for i in range(T)]
    soc = [pulp.LpVariable(f"s_{i}", 0, E_max) for i in range(T + 1)]

    m += soc[0] == soc0, "init"

    for i in range(T):
        m += soc[i+1] == soc[i] + (eff_leg * charge[i] - discharge[i] / eff_leg) * dt, f"dyn_{i}"

    if inputs.cyclic: 
        m+= soc[T] == soc0, "cyclic"

    m += pulp.lpSum(p[i] * (discharge[i] - charge[i]) * dt for i in range(T))

    # --- solve + extract -----------------------------------------------------
    m.solve(pulp.PULP_CBC_CMD(msg=0))
    status = pulp.LpStatus[m.status]

    charge_v = np.array([v.varValue or 0.0 for v in charge])         # noqa: F821
    discharge_v = np.array([v.varValue or 0.0 for v in discharge])    # noqa: F821
    soc_v = np.array([v.varValue or 0.0 for v in soc])                # noqa: F821
    net = discharge_v - charge_v
    revenue = p * net * dt

    schedule = pd.DataFrame(
        {
            "interval_datetime": prices.index,
            "charge_mw": charge_v,
            "discharge_mw": discharge_v,
            "soc_mwh": soc_v[1:],   # SOC at end of each interval
            "net_mw": net,
            "price": p,
            "revenue": revenue,
        }
    )
    total = float(revenue.sum())
    # Equivalent full cycles = discharge throughput / E_max (one cycle = E_max in, E_max out).
    n_cycles = float(discharge_v.sum() * dt) / E_max if E_max > 0 else 0.0

    return DispatchResult(
        schedule=schedule,
        total_revenue=total,
        n_cycles=n_cycles,
        solver_status=status,
        inputs=inputs,
    )
