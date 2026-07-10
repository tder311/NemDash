"""Joint least-squares backsolve of unit generation from predispatch constraints.

Each predispatch (run, interval) publishes solved LHS values for ~900 constraints,
where LHS = sum(factor*unit_MW) + sum(factor*interconnector_flow) + sum(factor*region_demand).
Stacking all constraints for one (run, interval) gives an over/under-determined linear
system A*g = b in the unknown unit-generation vector g, solved with bounds via
scipy.optimize.lsq_linear.

Identifiability is a first-class output. A unit's MW is *estimable* only if its unit
vector is orthogonal to the null space of A; a unit that appears solely as part of a
fixed linear combination (e.g. two units whose sum is pinned but whose split is not)
is structurally unidentifiable and is flagged, never silently reported as a number.
"""

import numpy as np
import pandas as pd
from scipy.optimize import lsq_linear

from .database import NEMDatabase, SENTINEL_MMSDM_TRADETYPE, SENTINEL_MMSDM_VERSION

OUTPUT_COLUMNS = [
    "run_datetime", "interval_datetime", "duid",
    "mw_inferred", "quality", "n_equations", "system_residual",
]
GROUP_KEYS = ["run_datetime", "interval_datetime"]
TERM_COLUMNS = ["constraintid", "term_type", "term_id", "factor"]

# Upper bound used when no availability (MAXAVAIL) is supplied for a unit.
DEFAULT_MAX_MW = 100_000.0

# Singular values below SINGULAR_RTOL * max span the null space of A.
SINGULAR_RTOL = 1e-9
# A unit whose unit vector projects more than NULL_TOL onto the null space is unidentifiable.
NULL_TOL = 1e-6
# Estimable units whose pseudoinverse row-norm (noise amplification) exceeds this are 'weak'.
WEAK_SENSITIVITY = 10.0

# Inferred rows are only trusted up to this lead (interval - run) for user-facing series/stats.
SHORT_LEAD_HOURS = 2.0
# A unit is considered to track realised generation when its observed correlation clears this.
TRACKING_CORR_THRESHOLD = 0.6

TRACKING_COLUMNS = ["duid", "n", "corr", "mae", "quality", "median_n_equations", "tracking"]
SERIES_COLUMNS = ["interval_datetime", "mw_inferred", "mw_realised"]


def _empty_output() -> pd.DataFrame:
    """Empty result frame with the canonical output schema."""
    return pd.DataFrame(columns=OUTPUT_COLUMNS)


def solve_unit_generation(
    lhs_frame: pd.DataFrame,
    terms: pd.DataFrame,
    ic_flows: pd.DataFrame,
    region_demand: pd.DataFrame,
    bounds: pd.DataFrame = None,
    default_max_mw: float = DEFAULT_MAX_MW,
) -> pd.DataFrame:
    """Jointly backsolve unit MW per (run, interval) from stacked constraint equations.

    lhs_frame: run_datetime, interval_datetime, constraintid, lhs (NaN lhs rows dropped).
    terms: constraintid, term_type ('duid'|'interconnector'|'region'), term_id, factor.
    ic_flows: run_datetime, interval_datetime, interconnectorid, mwflow.
    region_demand: run_datetime, interval_datetime, regionid, demand (empty => region terms
        are unresolvable, so every region-term constraint is dropped from its system).
    bounds: optional duid, maxavail upper bounds -- per (interval_datetime, duid) when an
        interval_datetime column is present, else per duid; missing units get default_max_mw.

    A constraint whose known (interconnector/region) terms cannot all be resolved is dropped
    from its system rather than zero-filled -- a wrong substitution would poison every unit.
    Returns one row per (run, interval, duid) with mw_inferred, an identifiability quality
    flag, the count of equations constraining that unit, and the system's residual norm.
    """
    if lhs_frame.empty or terms.empty:
        return _empty_output()

    usable_lhs = lhs_frame.dropna(subset=["lhs"])
    if usable_lhs.empty:
        return _empty_output()

    merged = usable_lhs.merge(terms[TERM_COLUMNS], on="constraintid", how="inner")
    if merged.empty:
        return _empty_output()

    duid_rows = _build_duid_system_rows(merged, ic_flows, region_demand)
    if duid_rows.empty:
        return _empty_output()

    bounds_lookup = _build_bounds_lookup(bounds)
    results = []
    for (run, interval), grp in duid_rows.groupby(GROUP_KEYS, sort=False):
        results.append(_solve_interval(run, interval, grp, bounds_lookup, default_max_mw))
    if not results:
        return _empty_output()
    return pd.concat(results, ignore_index=True)[OUTPUT_COLUMNS]


def _build_bounds_lookup(bounds: pd.DataFrame) -> tuple:
    """(cap lookup dict, keyed_by_interval flag); keys are (interval, duid) or plain duid."""
    if bounds is None or bounds.empty:
        return {}, False
    caps = pd.to_numeric(bounds["maxavail"], errors="coerce")
    if "interval_datetime" in bounds.columns:
        return dict(zip(zip(bounds["interval_datetime"], bounds["duid"]), caps)), True
    return dict(zip(bounds["duid"], caps)), False


def _build_duid_system_rows(
    merged: pd.DataFrame, ic_flows: pd.DataFrame, region_demand: pd.DataFrame
) -> pd.DataFrame:
    """Resolve known terms, drop unresolvable constraints, and return DUID rows with per-constraint b.

    Output columns: run_datetime, interval_datetime, constraintid, duid, factor, b.
    """
    known_sum, unresolvable = _resolve_known_terms(merged, ic_flows, region_demand)

    key_cols = GROUP_KEYS + ["constraintid"]
    constraint_b = merged[key_cols + ["lhs"]].drop_duplicates(subset=key_cols)
    constraint_b = constraint_b.merge(known_sum, on=key_cols, how="left")
    constraint_b["known_sum"] = pd.to_numeric(constraint_b["known_sum"], errors="coerce").fillna(0.0)
    constraint_b["b"] = constraint_b["lhs"] - constraint_b["known_sum"]

    if not unresolvable.empty:
        constraint_b = constraint_b.merge(
            unresolvable.assign(_bad=True), on=key_cols, how="left"
        )
        constraint_b = constraint_b[constraint_b["_bad"].isna()]

    duid_rows = merged[merged["term_type"] == "duid"].merge(
        constraint_b[key_cols + ["b"]], on=key_cols, how="inner"
    )
    duid_rows = duid_rows.rename(columns={"term_id": "duid"})
    return duid_rows[key_cols + ["duid", "factor", "b"]]


def _resolve_known_terms(merged, ic_flows, region_demand):
    """Per (run, interval, constraintid): summed known-term value and the set with a missing known.

    Interconnector terms substitute mwflow; region terms substitute demand. A term whose value
    is absent makes its whole constraint instance unresolvable (returned in the second frame).
    """
    key_cols = GROUP_KEYS + ["constraintid"]
    ic_resolved = _substitute_terms(
        merged[merged["term_type"] == "interconnector"], ic_flows,
        "interconnectorid", "mwflow", key_cols,
    )
    region_resolved = _substitute_terms(
        merged[merged["term_type"] == "region"], region_demand,
        "regionid", "demand", key_cols,
    )
    frames = [f for f in (ic_resolved, region_resolved) if not f.empty]
    if not frames:
        empty_known = pd.DataFrame(columns=key_cols + ["known_sum"])
        empty_bad = pd.DataFrame(columns=key_cols)
        return empty_known, empty_bad
    resolved = pd.concat(frames, ignore_index=True)

    unresolvable = resolved.loc[resolved["known_value"].isna(), key_cols].drop_duplicates()
    known_sum = (
        resolved.dropna(subset=["known_value"])
        .groupby(key_cols, as_index=False)["known_value"].sum()
        .rename(columns={"known_value": "known_sum"})
    )
    return known_sum, unresolvable


def _substitute_terms(term_rows, values, value_id_col, value_col, key_cols):
    """Attach factor*value to each known-term row; unmatched rows get NaN known_value."""
    out_cols = key_cols + ["known_value"]
    if term_rows.empty:
        return pd.DataFrame(columns=out_cols)
    if values is None or values.empty:
        return term_rows.assign(known_value=np.nan)[out_cols]

    joined = term_rows.merge(
        values[GROUP_KEYS + [value_id_col, value_col]],
        left_on=GROUP_KEYS + ["term_id"],
        right_on=GROUP_KEYS + [value_id_col],
        how="left",
    )
    joined["known_value"] = joined["factor"] * joined[value_col]
    return joined[out_cols]


def _solve_interval(run, interval, grp, bounds_lookup, default_max_mw) -> pd.DataFrame:
    """Assemble and solve one (run, interval) system; return per-DUID rows with quality flags."""
    pivot = grp.pivot_table(index="constraintid", columns="duid", values="factor", aggfunc="sum", fill_value=0.0)
    duids = list(pivot.columns)
    a_matrix = pivot.to_numpy(dtype=float)

    b_by_constraint = grp.drop_duplicates("constraintid").set_index("constraintid")["b"]
    b_vector = b_by_constraint.reindex(pivot.index).to_numpy(dtype=float)

    caps, by_interval = bounds_lookup
    keys = [(interval, d) if by_interval else d for d in duids]
    upper = np.array([_unit_upper_bound(k, caps, default_max_mw) for k in keys])
    solution = lsq_linear(a_matrix, b_vector, bounds=(np.zeros(len(duids)), upper))
    residual = float(np.linalg.norm(a_matrix @ solution.x - b_vector))

    quality = _classify_units(a_matrix)
    n_equations = (a_matrix != 0.0).sum(axis=0)

    return pd.DataFrame({
        "run_datetime": run,
        "interval_datetime": interval,
        "duid": duids,
        "mw_inferred": solution.x,
        "quality": quality,
        "n_equations": n_equations.astype(int),
        "system_residual": residual,
    })


def _unit_upper_bound(key, caps, default_max_mw) -> float:
    """MAXAVAIL upper bound for one unit (or interval-unit) key, else the large default cap."""
    cap = caps.get(key, default_max_mw)
    if pd.isna(cap) or cap <= 0:
        return default_max_mw
    return float(cap)


def _classify_units(a_matrix: np.ndarray) -> list:
    """Per-column identifiability flag: 'good' | 'weak' | 'unidentifiable'.

    A unit is unidentifiable when its unit vector has a non-trivial projection onto the null
    space of A (its value is not separable from other units). Estimable units are 'weak' when
    the pseudoinverse amplifies b-noise into the estimate above WEAK_SENSITIVITY, else 'good'.
    """
    n_cols = a_matrix.shape[1]
    _, singular, vt = np.linalg.svd(a_matrix, full_matrices=True)
    smax = singular.max() if singular.size else 0.0
    cutoff = smax * SINGULAR_RTOL

    singular_full = np.zeros(n_cols)
    singular_full[: singular.size] = singular
    null_mask = singular_full <= cutoff
    nonnull_mask = ~null_mask

    null_projection = np.linalg.norm(vt[null_mask, :], axis=0)
    inv_singular = np.zeros(n_cols)
    inv_singular[nonnull_mask] = 1.0 / singular_full[nonnull_mask]
    sensitivity = np.sqrt(((vt * inv_singular[:, None]) ** 2).sum(axis=0))

    quality = np.where(null_projection > NULL_TOL, "unidentifiable",
                       np.where(sensitivity > WEAK_SENSITIVITY, "weak", "good"))
    return quality.tolist()


def select_short_lead_latest_run(
    inferred: pd.DataFrame, max_lead_hours: float = SHORT_LEAD_HOURS
) -> pd.DataFrame:
    """One row per (duid, interval_datetime): the latest run within max_lead_hours of that interval.

    Rows with a non-positive or over-long lead (interval - run) are dropped first, since a
    later/closer run is always the more reliable forecast of that interval's generation.
    """
    if inferred.empty:
        return inferred.copy()
    lead = inferred["interval_datetime"] - inferred["run_datetime"]
    within = inferred[(lead > pd.Timedelta(0)) & (lead <= pd.Timedelta(hours=max_lead_hours))]
    if within.empty:
        return within.reset_index(drop=True)
    within = within.sort_values("run_datetime")
    return within.drop_duplicates(subset=["duid", "interval_datetime"], keep="last").reset_index(drop=True)


def aggregate_realised_30min(dispatch: pd.DataFrame) -> pd.DataFrame:
    """Mean 5-min scadavalue per 30-min bucket, per DUID (bucket = period-ending, via ceil)."""
    out_columns = ["interval_datetime", "duid", "mw_realised"]
    if dispatch.empty:
        return pd.DataFrame(columns=out_columns)
    out = dispatch.copy()
    out["interval_datetime"] = out["settlementdate"].dt.ceil("30min")
    return (
        out.groupby(["interval_datetime", "duid"], as_index=False)["scadavalue"]
        .mean()
        .rename(columns={"scadavalue": "mw_realised"})
    )


def compute_unit_tracking(inferred: pd.DataFrame, realised: pd.DataFrame) -> pd.DataFrame:
    """Per-DUID observed tracking quality: n, corr, mae, quality mode, median n_equations, tracking gate.

    Scored on short-lead/latest-run rows only, matching what a user sees in the paired series --
    a unit's 'good' solver flag does not guarantee it tracks realised MW (e.g. BESS charging
    violates the solver's g>=0 bound), so trust is gated on this observed correlation instead.
    """
    selected = select_short_lead_latest_run(inferred)
    if selected.empty or realised.empty:
        return pd.DataFrame(columns=TRACKING_COLUMNS)

    merged = selected.merge(realised, on=["interval_datetime", "duid"], how="inner")
    if merged.empty:
        return pd.DataFrame(columns=TRACKING_COLUMNS)

    rows = []
    for duid, group in merged.groupby("duid"):
        # Constant series have undefined correlation; guard so numpy never warns on the zero stddev.
        varying = len(group) >= 2 and group["mw_inferred"].std() > 0 and group["mw_realised"].std() > 0
        corr = group["mw_inferred"].corr(group["mw_realised"]) if varying else np.nan
        mae = (group["mw_inferred"] - group["mw_realised"]).abs().mean()
        rows.append({
            "duid": duid,
            "n": len(group),
            "corr": corr,
            "mae": mae,
            "quality": group["quality"].mode().iloc[0],
            "median_n_equations": group["n_equations"].median(),
            "tracking": bool(pd.notna(corr) and corr >= TRACKING_CORR_THRESHOLD),
        })
    return pd.DataFrame(rows).sort_values("corr", ascending=False, na_position="last").reset_index(drop=True)


def build_paired_series(inferred: pd.DataFrame, realised: pd.DataFrame) -> pd.DataFrame:
    """Short-lead inferred MW outer-joined with realised 30-min MW for one DUID, by interval.

    Outer join so a gap in either source shows up as a null point rather than being dropped,
    letting the chart render the full window even where one side has no data.
    """
    selected = select_short_lead_latest_run(inferred)
    if selected.empty and realised.empty:
        return pd.DataFrame(columns=SERIES_COLUMNS)
    merged = selected.merge(realised, on=["interval_datetime", "duid"], how="outer")
    return merged.sort_values("interval_datetime")[SERIES_COLUMNS].reset_index(drop=True)


def aggregate_bounds_to_30min(bids: pd.DataFrame) -> pd.DataFrame:
    """Max MAXAVAIL per (30-min interval, duid) from 5-min bid rows (period-ending, via ceil)."""
    out = bids.copy()
    out["interval_datetime"] = out["settlementdate"].dt.ceil("30min")
    return out.groupby(["interval_datetime", "duid"], as_index=False)["maxavail"].max()


TERMS_OUTPUT_COLUMNS = ["constraintid", "term_type", "term_id", "factor"]


# TradeType codes whose trader/region factors are coefficients on energy MW (verified against
# a real 2026-05-15 NEMDE day: ENOF=generator, LDOF=scheduled load, BDOF=bidirectional/BESS,
# DROF=wholesale demand response; every R*/L* code is an FCAS variable). The MMSDM sentinel
# marker counts as energy because that feed was ENERGY-bidtype filtered at source.
ENERGY_TRADETYPES = frozenset({"ENOF", "LDOF", "BDOF", "DROF", SENTINEL_MMSDM_TRADETYPE})


def drop_non_energy_constraints(terms: pd.DataFrame) -> pd.DataFrame:
    """Drop every constraint that has >=1 trader/region term with a non-energy tradetype.

    Such a factor multiplies the unit's FCAS variable, not its energy MW, so the constraint's
    published LHS includes contributions the solver cannot substitute -- keeping any of its
    terms would poison every system containing it (same logic as the region-demand exclusion).
    Interconnector terms carry a NULL tradetype and never trigger the exclusion. Precondition:
    the caller has narrowed terms to at most one version per constraintid (exclusion is
    keyed on constraintid, so a stale FCAS version would otherwise veto a clean current one).
    """
    if terms.empty:
        return terms
    fcas = (
        terms["term_type"].isin(["duid", "region"])
        & terms["tradetype"].notna()
        & ~terms["tradetype"].isin(ENERGY_TRADETYPES)
    )
    excluded = set(terms.loc[fcas, "constraintid"])
    return terms[~terms["constraintid"].isin(excluded)]


def select_terms_for_run_date(all_terms: pd.DataFrame, run_date) -> pd.DataFrame:
    """Per constraintid, keep only the term rows for the version effective at run_date.

    A version is a candidate if its effective_date <= run_date; the winner is the one with the
    latest effective_date (ties broken by the highest version number, then by the latest
    first_seen -- the version we actually knew about soonest). A constraintid with no dated
    candidate falls back to its sentinel MMSDM row (version == SENTINEL_MMSDM_VERSION), which
    carries no effective_date and is only ever a last resort. Selected versions containing any
    FCAS-tradetype term are then excluded whole (see drop_non_energy_constraints).
    """
    if all_terms.empty:
        return pd.DataFrame(columns=TERMS_OUTPUT_COLUMNS)
    run_date = pd.Timestamp(run_date).normalize()

    dated = all_terms[all_terms["effective_date"].notna()].copy()
    dated["effective_date"] = pd.to_datetime(dated["effective_date"])
    dated = dated[dated["effective_date"] <= run_date]

    winners = pd.DataFrame(columns=["constraintid", "version"])
    if not dated.empty:
        version_keys = dated[["constraintid", "version", "effective_date", "first_seen"]].drop_duplicates(
            subset=["constraintid", "version"]
        )
        version_keys["first_seen"] = pd.to_datetime(version_keys["first_seen"])
        version_keys = version_keys.sort_values(["constraintid", "effective_date", "version", "first_seen"])
        winners = version_keys.groupby("constraintid", as_index=False).tail(1)[["constraintid", "version"]]

    dated_selected = dated.merge(winners, on=["constraintid", "version"], how="inner")

    covered = set(winners["constraintid"])
    sentinel = all_terms[
        (all_terms["version"] == SENTINEL_MMSDM_VERSION) & (~all_terms["constraintid"].isin(covered))
    ]

    frames = [f for f in (dated_selected, sentinel) if not f.empty]
    if not frames:
        return pd.DataFrame(columns=TERMS_OUTPUT_COLUMNS)
    selected = drop_non_energy_constraints(pd.concat(frames, ignore_index=True))
    return selected[TERMS_OUTPUT_COLUMNS].reset_index(drop=True)


# --- DB-aware input fetchers (async; everything above is pure and DataFrame-in/DataFrame-out) ---

# Fuels shown as per-unit heatmaps (individually identifiable enough to be worth a row each).
UNIT_FUEL_SOURCES = ("Coal", "Gas", "Battery")
# Fuels shown as fleet aggregates only -- too numerous/small to chart per-unit.
FLEET_FUEL_SOURCES = ("Wind", "Solar")

GENERATION_FORECAST_UNIT_INFO_COLUMNS = ["station_name", "fuel_source", "technology_type", "capacity_mw"]


def build_generation_forecast(rows: pd.DataFrame, region: str) -> dict:
    """Shape latest-run inferred generation into per-unit series (Coal/Gas/Battery) and fleet
    aggregates (Wind/Solar) for one region.

    rows: run_datetime, interval_datetime, duid, mw_inferred, quality, station_name, region,
        fuel_source, technology_type, capacity_mw -- may span multiple runs; only the latest is used.
    A unit's series holds only the intervals where it was inferable that interval -- gaps are
    honest absence, never interpolated or zero-filled. Units are ordered by capacity descending.
    """
    if rows.empty:
        return {"units": [], "fleets": []}

    latest = rows[rows["run_datetime"] == rows["run_datetime"].max()]
    regional = latest[latest["region"] == region]

    units = _build_unit_series(regional[regional["fuel_source"].isin(UNIT_FUEL_SOURCES)])
    fleets = _build_fleet_series(regional[regional["fuel_source"].isin(FLEET_FUEL_SOURCES)])
    return {"units": units, "fleets": fleets}


def _build_unit_series(df: pd.DataFrame) -> list:
    """One entry per DUID: static generator_info fields plus its inferable-interval-only series."""
    if df.empty:
        return []
    info = df.drop_duplicates("duid").set_index("duid")[GENERATION_FORECAST_UNIT_INFO_COLUMNS]
    info = info.sort_values("capacity_mw", ascending=False)

    series_by_duid = {
        duid: [
            {"interval_datetime": row.interval_datetime, "mw": row.mw_inferred, "quality": row.quality}
            for row in group.sort_values("interval_datetime").itertuples(index=False)
        ]
        for duid, group in df.groupby("duid", sort=False)
    }
    return [
        {
            "duid": duid,
            "station_name": meta["station_name"],
            "fuel_source": meta["fuel_source"],
            "technology_type": meta["technology_type"],
            "capacity_mw": float(meta["capacity_mw"]),
            "series": series_by_duid[duid],
        }
        for duid, meta in info.iterrows()
    ]


def _build_fleet_series(df: pd.DataFrame) -> list:
    """Per fuel source (Wind/Solar): summed MW and unit/capacity coverage per interval, plus fleet totals."""
    if df.empty:
        return []
    fleets = []
    for fuel, group in df.groupby("fuel_source", sort=False):
        fleet_units = group.drop_duplicates("duid")
        per_interval = (
            group.groupby("interval_datetime")
            .agg(mw_sum=("mw_inferred", "sum"), n_units=("duid", "nunique"), capacity_inferable=("capacity_mw", "sum"))
            .sort_index()
        )
        fleets.append({
            "fuel_source": fuel,
            "n_units_total": int(fleet_units["duid"].nunique()),
            "capacity_total": float(fleet_units["capacity_mw"].sum()),
            "series": [
                {
                    "interval_datetime": row.Index,
                    "mw_sum": float(row.mw_sum),
                    "n_units": int(row.n_units),
                    "capacity_inferable": float(row.capacity_inferable),
                }
                for row in per_interval.itertuples()
            ],
        })
    return fleets


async def fetch_terms(db: NEMDatabase, run_date) -> pd.DataFrame:
    """Constraint equation terms in force at run_date (see select_terms_for_run_date)."""
    async with db._pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT constraintid, version, effective_date, term_type, term_id, factor, first_seen, tradetype "
            "FROM constraint_equation_terms"
        )
    all_terms = pd.DataFrame([dict(r) for r in rows])
    if all_terms.empty:
        return pd.DataFrame(columns=TERMS_OUTPUT_COLUMNS)
    return select_terms_for_run_date(all_terms, run_date)


async def fetch_bounds(db: NEMDatabase, start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
    """Per-(30-min interval, duid) MAXAVAIL bounds from stored bid_per_offer rows."""
    async with db._pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT settlementdate, duid, maxavail FROM bid_per_offer
            WHERE settlementdate > $1 AND settlementdate <= $2 AND maxavail IS NOT NULL
        """, start.to_pydatetime(), (end + pd.Timedelta(days=1)).to_pydatetime())
    bids = pd.DataFrame([dict(r) for r in rows])
    if bids.empty:
        return bids
    bids["settlementdate"] = pd.to_datetime(bids["settlementdate"])
    return aggregate_bounds_to_30min(bids)
