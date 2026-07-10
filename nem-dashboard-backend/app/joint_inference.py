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
    bounds: optional duid, maxavail (per-unit upper bound); missing units get default_max_mw.

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


def _build_bounds_lookup(bounds: pd.DataFrame) -> dict:
    """Map duid -> MAXAVAIL upper bound; empty when no bounds supplied."""
    if bounds is None or bounds.empty:
        return {}
    return dict(zip(bounds["duid"], pd.to_numeric(bounds["maxavail"], errors="coerce")))


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

    upper = np.array([_unit_upper_bound(d, bounds_lookup, default_max_mw) for d in duids])
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


def _unit_upper_bound(duid, bounds_lookup, default_max_mw) -> float:
    """MAXAVAIL upper bound for a unit, falling back to the large default cap."""
    cap = bounds_lookup.get(duid, default_max_mw)
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
