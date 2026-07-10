"""Validate inferred unit generation (app.unit_inference) against realised dispatch.

For a recent settled trading day, runs the single-unknown-DUID constraint
backsolve on stored short-lead predispatch data (runs within ~2h of the
interval they forecast), then compares the inferred MW against AEMO's
realised dispatch from NEMWEB's Next_Day_Dispatch report -- the evidence for
whether this feature's inference is usable at all.

Run from the backend directory (``nem-dashboard-backend/``):

    python -m scripts.validate_unit_inference --day 2026-07-09

Source: ``https://www.nemweb.com.au/Reports/Current/Next_Day_Dispatch/
PUBLIC_NEXT_DAY_DISPATCH_{YYYYMMDD}_*.zip`` (filename dated the trading day
it covers, published the following morning), table ``DISPATCH,UNIT_SOLUTION``
(verified against a live 2026-07-09 file), 5-min INITIALMW/TOTALCLEARED per
DUID. TOTALCLEARED (AEMO's cleared dispatch target) is compared against
``mw_inferred``, aggregated to 30-min by ceiling each 5-min settlementdate to
its enclosing half-hour -- matching predispatch's period-ending convention.
"""

import argparse
import asyncio
import io
import os
import zipfile
from csv import reader as csv_reader
from typing import List, Optional, Set

import httpx
import pandas as pd
from dotenv import load_dotenv

from app.database import NEMDatabase
from app.unit_inference import infer_unit_generation

BASE_URL = "https://www.nemweb.com.au"
NEXT_DAY_DISPATCH_PATH = "Reports/Current/Next_Day_Dispatch"
SHORT_LEAD_HOURS = 2


def parse_next_day_dispatch_csv(text: str, target_duids: Set[str]) -> pd.DataFrame:
    """Extract DISPATCH,UNIT_SOLUTION rows for target_duids -> settlementdate, duid, initialmw, totalcleared."""
    headers: Optional[List[str]] = None
    rows: List[List[str]] = []
    for line in text.splitlines():
        if line.startswith("I,DISPATCH,UNIT_SOLUTION"):
            headers = [h.strip() for h in next(csv_reader([line]))[4:]]
        elif line.startswith("D,DISPATCH,UNIT_SOLUTION") and headers is not None:
            if not any(duid in line for duid in target_duids):
                continue
            rows.append(next(csv_reader([line]))[4:])

    columns = ["settlementdate", "duid", "initialmw", "totalcleared"]
    empty = pd.DataFrame(columns=columns)
    if not headers or not rows:
        return empty

    df = pd.DataFrame(rows, columns=headers[: len(rows[0])])
    df = df[df["DUID"].isin(target_duids)]
    if "INTERVENTION" in df.columns:
        df = df[df["INTERVENTION"].astype(str).str.strip() == "0"]
    if df.empty:
        return empty

    out = pd.DataFrame({
        "settlementdate": pd.to_datetime(df["SETTLEMENTDATE"], errors="coerce"),
        "duid": df["DUID"],
        "initialmw": pd.to_numeric(df["INITIALMW"], errors="coerce"),
        "totalcleared": pd.to_numeric(df["TOTALCLEARED"], errors="coerce"),
    })
    return out.dropna(subset=["settlementdate"]).reset_index(drop=True)


def aggregate_realised_to_30min(df: pd.DataFrame) -> pd.DataFrame:
    """Mean 5-min realised values per 30-min bucket, per DUID (bucket = period-ending, via ceil)."""
    out = df.copy()
    out["interval_datetime"] = out["settlementdate"].dt.ceil("30min")
    return out.groupby(["interval_datetime", "duid"], as_index=False)[["initialmw", "totalcleared"]].mean()


def compute_validation_metrics(inferred: pd.DataFrame, realised: pd.DataFrame) -> pd.DataFrame:
    """Per-DUID n / correlation / MAE between mw_inferred and realised totalcleared."""
    columns = ["duid", "n", "corr", "mae"]
    if inferred.empty or realised.empty:
        return pd.DataFrame(columns=columns)

    merged = inferred.merge(realised, on=["interval_datetime", "duid"], how="inner")
    if merged.empty:
        return pd.DataFrame(columns=columns)

    rows = []
    for duid, group in merged.groupby("duid"):
        corr = group["mw_inferred"].corr(group["totalcleared"]) if len(group) >= 2 else float("nan")
        mae = (group["mw_inferred"] - group["totalcleared"]).abs().mean()
        rows.append({"duid": duid, "n": len(group), "corr": corr, "mae": mae})
    return pd.DataFrame(rows).sort_values("n", ascending=False).reset_index(drop=True)


async def fetch_short_lead_inputs(db: NEMDatabase, day: pd.Timestamp):
    """Constraints (LHS not null, short lead), interconnectors, and terms for one trading day."""
    start = day
    end = day + pd.Timedelta(days=1)
    async with db._pool.acquire() as conn:
        constraint_rows = await conn.fetch("""
            SELECT run_datetime, interval_datetime, constraintid, lhs
            FROM predispatch_constraint
            WHERE lhs IS NOT NULL
              AND interval_datetime >= $1 AND interval_datetime < $2
              AND interval_datetime > run_datetime
              AND interval_datetime <= run_datetime + ($3 || ' hours')::INTERVAL
        """, start, end, str(SHORT_LEAD_HOURS))
        interconnector_rows = await conn.fetch("""
            SELECT run_datetime, interval_datetime, interconnectorid, mwflow
            FROM predispatch_interconnector
            WHERE interval_datetime >= $1 AND interval_datetime < $2
        """, start, end)
        term_rows = await conn.fetch(
            "SELECT constraintid, version, term_type, term_id, factor FROM constraint_equation_terms"
        )
    constraints = pd.DataFrame([dict(r) for r in constraint_rows])
    interconnectors = pd.DataFrame([dict(r) for r in interconnector_rows])
    terms = pd.DataFrame([dict(r) for r in term_rows])
    return constraints, interconnectors, terms


async def fetch_realised_dispatch(client: httpx.AsyncClient, day: pd.Timestamp, target_duids: Set[str]) -> pd.DataFrame:
    """Download one day's Next_Day_Dispatch zip and extract UNIT_SOLUTION rows for target_duids."""
    resp = await client.get(f"{BASE_URL}/{NEXT_DAY_DISPATCH_PATH}/")
    resp.raise_for_status()
    day_token = day.strftime("%Y%m%d")
    matches = [n for n in _extract_hrefs(resp.text) if f"NEXT_DAY_DISPATCH_{day_token}_" in n]
    if not matches:
        raise SystemExit(f"No Next_Day_Dispatch file found for {day_token}")
    filename = matches[0]

    file_resp = await client.get(f"{BASE_URL}/{NEXT_DAY_DISPATCH_PATH}/{filename}")
    file_resp.raise_for_status()
    with zipfile.ZipFile(io.BytesIO(file_resp.content)) as z:
        text = z.read(z.namelist()[0]).decode("utf-8", "ignore")
    return parse_next_day_dispatch_csv(text, target_duids)


def _extract_hrefs(html: str) -> List[str]:
    """Filenames referenced by HREF in a NEMWEB directory listing page."""
    import re
    return re.findall(r'HREF="([^"]+\.zip)"', html, re.IGNORECASE)


def _print_report(metrics: pd.DataFrame) -> None:
    if metrics.empty:
        print("No overlapping (interval, duid) points between inferred and realised data -- nothing to report.")
        return
    print(f"{'DUID':<12}{'n':>6}{'corr':>10}{'MAE (MW)':>12}")
    for _, row in metrics.iterrows():
        corr_str = f"{row['corr']:.3f}" if pd.notna(row["corr"]) else "n/a"
        print(f"{row['duid']:<12}{row['n']:>6}{corr_str:>10}{row['mae']:>12.2f}")
    print("-" * 40)
    print(f"{'OVERALL':<12}{metrics['n'].sum():>6}{metrics['corr'].mean():>10.3f}{metrics['mae'].mean():>12.2f}")


async def validate(day_str: str) -> None:
    load_dotenv()
    day = pd.Timestamp(day_str).normalize()
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        raise SystemExit("DATABASE_URL is not set (check your .env).")

    db = NEMDatabase(db_url)
    await db.initialize()
    try:
        constraints, interconnectors, terms = await fetch_short_lead_inputs(db, day)
    finally:
        await db.close()

    print(f"Loaded: {len(constraints)} constraint rows, {len(interconnectors)} interconnector rows, {len(terms)} terms")
    inferred = infer_unit_generation(constraints, interconnectors, terms)
    print(f"Inferred {len(inferred)} (run, interval, duid, constraintid) rows across {inferred['duid'].nunique() if not inferred.empty else 0} DUIDs")
    if inferred.empty:
        print("Nothing inferred -- no constraint had exactly one unknown DUID term with fully-known other terms.")
        return

    target_duids = set(inferred["duid"].unique())
    async with httpx.AsyncClient(timeout=180.0, follow_redirects=True) as client:
        realised_5min = await fetch_realised_dispatch(client, day, target_duids)
    print(f"Fetched {len(realised_5min)} realised 5-min rows for {len(target_duids)} target DUIDs")

    realised_30min = aggregate_realised_to_30min(realised_5min)
    metrics = compute_validation_metrics(inferred, realised_30min)
    _print_report(metrics)


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Validate app.unit_inference against realised dispatch for one settled trading day."
    )
    ap.add_argument("--day", required=True, help="trading day, YYYY-MM-DD (must be fully settled)")
    args = ap.parse_args()
    asyncio.run(validate(args.day))


if __name__ == "__main__":
    main()
