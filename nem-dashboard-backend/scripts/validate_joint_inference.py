"""Validate joint least-squares unit inference (app.joint_inference) against realised dispatch.

For a window of settled trading days, downloads every PD7Day run from NEMWEB Current
(retains ~2 months; 3 runs/day at ~07:00/~12:40/~17:40), parses the FULL
CONSTRAINTSOLUTION and INTERCONNECTORSOLUTION tables in-memory (all constraints, not
the binding-only DB subset), keeps each run's first forward hours, and jointly solves
per (run, interval) for unit MW. Inferred MW is compared against AEMO's realised
TOTALCLEARED from Next_Day_Dispatch, aggregated to 30-min (period-ending).

Region terms: the live PD7Day file carries NO region table (verified 2026-07-10:
CASESOLUTION, MARKET_SUMMARY, CONSTRAINTSOLUTION, INTERCONNECTORSOLUTION, PRICESOLUTION
only -- PRICESOLUTION is prices, no demand), so region-term substitution is impossible
from this feed. The stored ENERGY-bidtype terms snapshot contains zero region terms,
so excluding them costs zero equations; region-term constraints would be dropped by
the solver anyway (unresolvable known term).

Calibration: constraints with ZERO duid terms should reconstruct exactly
(LHS ~= sum(factor*mwflow) from the same file); their error distribution is reported
before any solve is trusted.

Run from the backend directory (``nem-dashboard-backend/``):

    python -m scripts.validate_joint_inference --start 2026-06-19 --end 2026-07-09

Add ``--persist`` to also upsert solved good/weak rows into ``inferred_unit_generation``
(seeds history for the unit-inference API/chart; unidentifiable rows are still dropped).
"""

import argparse
import asyncio
import io
import json
import os
import re
import zipfile
from typing import Dict, List, Set

import httpx
import pandas as pd
from dotenv import load_dotenv

from app.database import NEMDatabase
from app.joint_inference import (
    aggregate_bounds_to_30min,
    fetch_bounds,
    fetch_terms,
    solve_unit_generation,
)
from app.nem_predispatch_client import NEMPredispatchClient
from scripts.validate_unit_inference import (
    aggregate_realised_to_30min,
    compute_validation_metrics,
    fetch_realised_dispatch,
)

BASE_URL = "https://www.nemweb.com.au"
PD7DAY_PATH = "Reports/Current/PD7Day/"
PD7DAY_FILE_RE = r"PUBLIC_PD7DAY_(\d{14})_\d+\.zip"
MAX_LEAD_HOURS = 4
REPORT_PATH = "/tmp/joint_inference_validation.json"


def filter_forward_window(df: pd.DataFrame, max_lead_hours: int = MAX_LEAD_HOURS) -> pd.DataFrame:
    """Keep rows within a run's first forward hours (run < interval <= run + lead)."""
    lead = pd.Timedelta(hours=max_lead_hours)
    mask = (df["interval_datetime"] > df["run_datetime"]) & (
        df["interval_datetime"] <= df["run_datetime"] + lead
    )
    return df[mask].reset_index(drop=True)


def compute_calibration_errors(
    lhs_frame: pd.DataFrame, terms: pd.DataFrame, ic_flows: pd.DataFrame
) -> pd.DataFrame:
    """Reconstruction error LHS - sum(factor*mwflow) for constraints with zero DUID terms.

    Only fully-resolvable instances (every interconnector term matched a flow) are scored;
    the result is the direct evidence that stored factors match the file's LHS convention.
    """
    duid_cids = set(terms.loc[terms["term_type"] == "duid", "constraintid"])
    ic_terms = terms[(terms["term_type"] == "interconnector") & ~terms["constraintid"].isin(duid_cids)]
    scored = lhs_frame.merge(ic_terms[["constraintid", "term_id", "factor"]], on="constraintid", how="inner")
    if scored.empty:
        return pd.DataFrame(columns=["run_datetime", "interval_datetime", "constraintid", "lhs", "reconstruction_error"])

    scored = scored.merge(
        ic_flows[["run_datetime", "interval_datetime", "interconnectorid", "mwflow"]],
        left_on=["run_datetime", "interval_datetime", "term_id"],
        right_on=["run_datetime", "interval_datetime", "interconnectorid"],
        how="left",
    )
    scored["term_value"] = scored["factor"] * scored["mwflow"]
    keys = ["run_datetime", "interval_datetime", "constraintid"]
    agg = scored.groupby(keys, as_index=False).agg(
        lhs=("lhs", "first"), known_sum=("term_value", "sum"), n_missing=("mwflow", lambda s: s.isna().sum())
    )
    agg = agg[agg["n_missing"] == 0]
    agg["reconstruction_error"] = agg["lhs"] - agg["known_sum"]
    return agg[keys + ["lhs", "reconstruction_error"]].reset_index(drop=True)


def summarise_calibration(calibration: pd.DataFrame) -> Dict:
    """Distribution stats of the zero-DUID-constraint reconstruction error."""
    if calibration.empty:
        return {"n": 0}
    err = calibration["reconstruction_error"].abs()
    return {
        "n": int(len(err)),
        "median_abs_error_mw": float(err.median()),
        "p90_abs_error_mw": float(err.quantile(0.9)),
        "max_abs_error_mw": float(err.max()),
        "share_within_1mw": float((err <= 1.0).mean()),
    }


def summarise_quality(inferred: pd.DataFrame) -> Dict:
    """Row and distinct-DUID counts per identifiability quality class."""
    rows = inferred["quality"].value_counts().to_dict()
    duids = inferred.groupby("quality")["duid"].nunique().to_dict()
    return {
        "rows_per_class": {k: int(v) for k, v in rows.items()},
        "duids_per_class": {k: int(v) for k, v in duids.items()},
    }


async def list_pd7day_runs(client: httpx.AsyncClient, start: pd.Timestamp, end: pd.Timestamp) -> List[str]:
    """PD7Day Current filenames whose run token falls within [start, end + 1 day)."""
    resp = await client.get(f"{BASE_URL}/{PD7DAY_PATH}")
    resp.raise_for_status()
    out = []
    for name in sorted(set(re.findall(PD7DAY_FILE_RE.replace("(", "(?:"), resp.text))):
        token = re.search(r"PD7DAY_(\d{14})_", name).group(1)
        run_dt = pd.Timestamp(f"{token[:8]} {token[8:10]}:{token[10:12]}:{token[12:]}")
        if start <= run_dt < end + pd.Timedelta(days=1):
            out.append(name)
    return out


async def fetch_run_tables(client: httpx.AsyncClient, filename: str):
    """Download one PD7Day run zip and parse its constraint + interconnector tables."""
    resp = await client.get(f"{BASE_URL}/{PD7DAY_PATH}{filename}")
    resp.raise_for_status()
    with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
        text = z.read(z.namelist()[0]).decode("utf-8", "ignore")
    parser = NEMPredispatchClient()
    return parser._parse_constraint_csv(text), parser._parse_interconnector_csv(text)


async def fetch_realised_for_days(days: List[pd.Timestamp], target_duids: Set[str]) -> pd.DataFrame:
    """Realised 30-min TOTALCLEARED per (interval, duid) across the validation days."""
    frames = []
    async with httpx.AsyncClient(timeout=180.0, follow_redirects=True) as client:
        for day in days:
            try:
                frames.append(await fetch_realised_dispatch(client, day, target_duids))
            except (httpx.HTTPError, SystemExit) as e:
                print(f"  WARNING: skipping Next_Day_Dispatch for {day.date()}: {e}")
    if not frames:
        raise ValueError("No Next_Day_Dispatch data fetched for any validation day.")
    realised_5min = pd.concat(frames, ignore_index=True)
    return aggregate_realised_to_30min(realised_5min)


def build_report(calibration_summary, quality_summary, metrics, inferred, n_runs) -> Dict:
    """Assemble the printable/serialisable validation report."""
    report = {
        "n_pd7day_runs": n_runs,
        "n_inferred_rows": int(len(inferred)),
        "n_systems": int(inferred.groupby(["run_datetime", "interval_datetime"]).ngroups) if not inferred.empty else 0,
        "median_system_residual": float(inferred["system_residual"].median()) if not inferred.empty else None,
        "calibration_zero_duid_constraints": calibration_summary,
        "quality_classes": quality_summary,
    }
    if metrics.empty:
        report["good_units"] = {"n_duids": 0, "note": "no good-quality units overlapped realised dispatch"}
        return report
    report["good_units"] = {
        "n_duids": int(len(metrics)),
        "median_corr": float(metrics["corr"].median()),
        "median_mae_mw": float(metrics["mae"].median()),
        "top_30_by_n": metrics.head(30).to_dict(orient="records"),
    }
    return report


def _print_report(report: Dict) -> None:
    print(json.dumps({k: v for k, v in report.items() if k != "good_units"}, indent=2, default=str))
    good = report["good_units"]
    if "top_30_by_n" not in good:
        print("No good-quality units to score.")
        return
    print(f"\n'good' units: {good['n_duids']} DUIDs, median corr {good['median_corr']:.3f}, "
          f"median MAE {good['median_mae_mw']:.1f} MW")
    print(f"{'DUID':<14}{'n':>6}{'corr':>10}{'MAE (MW)':>12}")
    for row in good["top_30_by_n"]:
        corr = f"{row['corr']:.3f}" if pd.notna(row["corr"]) else "n/a"
        print(f"{row['duid']:<14}{row['n']:>6}{corr:>10}{row['mae']:>12.2f}")


async def validate(start_str: str, end_str: str, persist: bool = False) -> None:
    load_dotenv()
    start = pd.Timestamp(start_str).normalize()
    end = pd.Timestamp(end_str).normalize()
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        raise SystemExit("DATABASE_URL is not set (check your .env).")

    db = NEMDatabase(db_url)
    await db.initialize()
    try:
        # One date-aware term selection for the whole window (its `end` date) -- constraint
        # equations rarely change within a validation window of a few weeks.
        terms = await fetch_terms(db, end)
        bounds = await fetch_bounds(db, start, end)
        print(f"Loaded {len(terms)} terms, {len(bounds)} (interval, duid) MAXAVAIL bounds")

        inferred_frames, calibration_frames = [], []
        async with httpx.AsyncClient(timeout=180.0, follow_redirects=True) as client:
            run_files = await list_pd7day_runs(client, start, end)
            print(f"Found {len(run_files)} PD7Day runs in window")
            for i, filename in enumerate(run_files, 1):
                constraints, ic_flows = await fetch_run_tables(client, filename)
                constraints = filter_forward_window(constraints.dropna(subset=["lhs"]))
                ic_flows = filter_forward_window(ic_flows)
                calibration_frames.append(compute_calibration_errors(constraints, terms, ic_flows))
                solved = solve_unit_generation(
                    constraints[["run_datetime", "interval_datetime", "constraintid", "lhs"]],
                    terms, ic_flows, pd.DataFrame(columns=["run_datetime", "interval_datetime", "regionid", "demand"]),
                    bounds=bounds,
                )
                inferred_frames.append(solved)
                print(f"  [{i}/{len(run_files)}] {filename}: {len(constraints)} constraint rows -> {len(solved)} unit rows")

        inferred = pd.concat(inferred_frames, ignore_index=True)
        calibration = pd.concat(calibration_frames, ignore_index=True)
        calibration_summary = summarise_calibration(calibration)
        quality_summary = summarise_quality(inferred) if not inferred.empty else {}
        print(f"\nCalibration (zero-DUID constraints): {calibration_summary}")

        if persist and not inferred.empty:
            persisted = await db.insert_inferred_unit_generation(inferred)
            print(f"Persisted {persisted} good/weak rows to inferred_unit_generation "
                  f"(of {len(inferred)} solved rows)")

        good = inferred[inferred["quality"] == "good"]
        days = list(pd.date_range(start, end, freq="D"))
        realised = await fetch_realised_for_days(days, set(good["duid"].unique()))
        metrics = compute_validation_metrics(good[["interval_datetime", "duid", "mw_inferred"]], realised)

        report = build_report(calibration_summary, quality_summary, metrics, inferred, len(run_files))
        _print_report(report)
        with open(REPORT_PATH, "w") as f:
            json.dump(report, f, indent=2, default=str)
        print(f"\nReport written to {REPORT_PATH}")
    finally:
        await db.close()


def main() -> None:
    ap = argparse.ArgumentParser(description="Validate app.joint_inference against realised dispatch.")
    ap.add_argument("--start", required=True, help="first trading day, YYYY-MM-DD")
    ap.add_argument("--end", required=True, help="last trading day, YYYY-MM-DD (must be settled)")
    ap.add_argument("--persist", action="store_true", help="write solved good/weak rows to inferred_unit_generation")
    args = ap.parse_args()
    asyncio.run(validate(args.start, args.end, persist=args.persist))


if __name__ == "__main__":
    main()
