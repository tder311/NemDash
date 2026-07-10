"""Backsolve single-unknown-DUID constraint equations for inferred unit generation.

A predispatch constraint's solved LHS = sum(factor * term) over DUID,
interconnector, and region-demand terms. With LHS and all-but-one term known,
the remaining DUID term's MW is solvable directly: mw = (lhs - known_sum) / factor.
"""

import pandas as pd

OUTPUT_COLUMNS = ["run_datetime", "interval_datetime", "duid", "mw_inferred", "constraintid", "n_terms"]
GROUP_KEYS = ["run_datetime", "interval_datetime", "constraintid"]


def infer_unit_generation(
    constraints: pd.DataFrame, interconnectors: pd.DataFrame, terms: pd.DataFrame
) -> pd.DataFrame:
    """Backsolve MW for constraints with LHS known and exactly one unknown DUID term.

    constraints: run_datetime, interval_datetime, constraintid, lhs (NaN lhs rows are unusable).
    interconnectors: run_datetime, interval_datetime, interconnectorid, mwflow.
    terms: constraintid, term_type ('duid'|'interconnector'|'region'), term_id, factor.
    Region terms (no stored forecast region demand) make a constraint unusable for v1 and are
    excluded entirely. Multiple constraints inferring the same (run, interval, duid) are all kept.
    """
    empty = pd.DataFrame(columns=OUTPUT_COLUMNS)
    usable_constraints = constraints.dropna(subset=["lhs"])
    if usable_constraints.empty or terms.empty:
        return empty

    n_terms_by_constraint = terms.groupby("constraintid").size().reset_index(name="n_terms")
    region_constraintids = set(terms.loc[terms["term_type"] == "region", "constraintid"])
    usable_terms = terms[~terms["constraintid"].isin(region_constraintids)]

    merged = usable_constraints.merge(usable_terms, on="constraintid", how="inner")
    if merged.empty:
        return empty

    known_sum, unresolved = _substitute_interconnector_terms(merged, interconnectors)
    solvable = _select_single_unknown_duid(merged, known_sum, unresolved)
    if solvable.empty:
        return empty

    solvable = solvable.assign(mw_inferred=(solvable["lhs"] - solvable["known_sum"]) / solvable["factor"])
    solvable = solvable.rename(columns={"term_id": "duid"}).merge(
        n_terms_by_constraint, on="constraintid", how="left"
    )
    return solvable[OUTPUT_COLUMNS].reset_index(drop=True)


def _substitute_interconnector_terms(merged: pd.DataFrame, interconnectors: pd.DataFrame):
    """Per (run, interval, constraintid): summed known interconnector value, and groups with an
    unresolved (no matching mwflow) interconnector term."""
    ic_term_rows = merged[merged["term_type"] == "interconnector"]
    if ic_term_rows.empty:
        return pd.DataFrame(columns=GROUP_KEYS + ["known_sum"]), pd.DataFrame(columns=GROUP_KEYS)
    if interconnectors.empty:
        # No interconnector flow data at all -- every interconnector-term instance is unresolved.
        unresolved = ic_term_rows[GROUP_KEYS].drop_duplicates().reset_index(drop=True)
        return pd.DataFrame(columns=GROUP_KEYS + ["known_sum"]), unresolved

    ic_rows = ic_term_rows.merge(
        interconnectors[["run_datetime", "interval_datetime", "interconnectorid", "mwflow"]],
        left_on=GROUP_KEYS[:2] + ["term_id"],
        right_on=GROUP_KEYS[:2] + ["interconnectorid"],
        how="left",
    )
    ic_rows["known_value"] = ic_rows["factor"] * ic_rows["mwflow"]
    unresolved = ic_rows.loc[ic_rows["mwflow"].isna(), GROUP_KEYS].drop_duplicates()
    known_sum = (
        ic_rows.groupby(GROUP_KEYS, as_index=False)["known_value"].sum().rename(columns={"known_value": "known_sum"})
    )
    return known_sum, unresolved


def _select_single_unknown_duid(merged: pd.DataFrame, known_sum: pd.DataFrame, unresolved: pd.DataFrame) -> pd.DataFrame:
    """DUID term rows whose constraint instance has exactly one unknown DUID and fully-known interconnectors."""
    duid_rows = merged[merged["term_type"] == "duid"]
    duid_counts = duid_rows.groupby(GROUP_KEYS).size().reset_index(name="n_duid_unknown")

    solvable = duid_rows.merge(duid_counts, on=GROUP_KEYS, how="left")
    solvable = solvable[solvable["n_duid_unknown"] == 1]

    if known_sum.empty:
        solvable = solvable.assign(known_sum=0.0)
    else:
        solvable = solvable.merge(known_sum, on=GROUP_KEYS, how="left")
        solvable["known_sum"] = solvable["known_sum"].fillna(0.0)

    if not unresolved.empty:
        solvable = solvable.merge(unresolved.assign(_unresolved=True), on=GROUP_KEYS, how="left")
        solvable = solvable[solvable["_unresolved"].isna()]

    return solvable
