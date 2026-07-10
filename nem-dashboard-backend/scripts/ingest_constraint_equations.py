"""Ingest constraint equation terms (LHS = sum(factor*term)) from AEMO's MMSDM monthly archive.

Run from the backend directory (``nem-dashboard-backend/``):

    python -m scripts.ingest_constraint_equations --dry-run
    python -m scripts.ingest_constraint_equations
    python -m scripts.ingest_constraint_equations --year 2026 --month 5

Source: ``https://www.nemweb.com.au/Data_Archive/Wholesale_Electricity/MMSDM/{YYYY}/
MMSDM_{YYYY}_{MM}/MMSDM_Historical_Data_SQLLoader/DATA/
PUBLIC_ARCHIVE#{TABLE}#FILE01#{YYYYMM}010000.zip`` (``#`` percent-encoded as ``%23``),
verified against the 2026-05 archive for four tables: ``SPDCONNECTIONPOINTCONSTRAINT``
(connection-point terms, EFFECTIVEDATE/VERSIONNO/GENCONID/FACTOR/BIDTYPE),
``SPDINTERCONNECTORCONSTRAINT`` (interconnector terms, no BIDTYPE column),
``SPDREGIONCONSTRAINT`` (region terms, has BIDTYPE), and ``DUDETAILSUMMARY``
(CONNECTIONPOINTID -> DUID, effective-dated; the current mapping's END_DATE
sentinel is 2999/12/31). GENCONID is AEMO's constraint ID, i.e. ``constraintid``
elsewhere in this codebase.

Only ENERGY-bidtype terms are kept (FCAS constraints are out of scope for MW
backsolving), and only each constraint's latest (EFFECTIVEDATE, VERSIONNO) --
v1 stores no effective-date history, so a later run simply overwrites earlier
terms in place (see ``constraint_equation_terms``'s UNIQUE(constraintid,
term_type, term_id) and its ON CONFLICT DO UPDATE).

Connection points with no *current* DUDETAILSUMMARY row (~1-2% in practice --
e.g. retired/renamed plant) cannot be mapped to a DUID and are dropped.
MMSDM lags real-time by ~1-2 months; the default (no --year/--month) finds
the latest month with a published archive by listing the year-index page(s).
"""

import argparse
import asyncio
import io
import os
import re
import zipfile
from csv import reader as csv_reader
from datetime import date
from typing import List, Optional, Tuple

import httpx
import pandas as pd
from dotenv import load_dotenv

from app.database import NEMDatabase

BASE_URL = "https://www.nemweb.com.au"
ARCHIVE_DIR = "Data_Archive/Wholesale_Electricity/MMSDM"
MONTH_DIR_RE = r"MMSDM_(\d{4})_(\d{2})"
SOURCE_TABLES = (
    "DUDETAILSUMMARY",
    "SPDCONNECTIONPOINTCONSTRAINT",
    "SPDINTERCONNECTORCONSTRAINT",
    "SPDREGIONCONSTRAINT",
)


def archive_url(year: int, month: int, table: str) -> str:
    """MMSDM monthly PUBLIC_ARCHIVE zip URL for one table."""
    ym_dir = f"{year:04d}_{month:02d}"
    ym_file = f"{year:04d}{month:02d}"
    return (
        f"{BASE_URL}/{ARCHIVE_DIR}/{year:04d}/MMSDM_{ym_dir}/MMSDM_Historical_Data_SQLLoader/DATA/"
        f"PUBLIC_ARCHIVE%23{table}%23FILE01%23{ym_file}010000.zip"
    )


def month_index_url(year: int) -> str:
    """URL of the MMSDM year-index page listing that year's monthly archive folders."""
    return f"{BASE_URL}/{ARCHIVE_DIR}/{year}/"


def parse_available_months(html: str) -> List[Tuple[int, int]]:
    """(year, month) tuples found in an MMSDM year-index page, sorted ascending."""
    return sorted({(int(y), int(m)) for y, m in re.findall(MONTH_DIR_RE, html)})


def _extract_table_rows(text: str, table_marker: str) -> Tuple[Optional[List[str]], List[List[str]]]:
    """Generic I/D row extractor for MMS CSVs (4 leading record-type/table/version fields dropped)."""
    headers: Optional[List[str]] = None
    rows: List[List[str]] = []
    for line in text.splitlines():
        if line.startswith(f"I,{table_marker}"):
            headers = next(csv_reader([line]))[4:]
        elif line.startswith(f"D,{table_marker}") and headers is not None:
            rows.append(next(csv_reader([line]))[4:])
    return headers, rows


def parse_dudetailsummary_csv(text: str) -> pd.DataFrame:
    """Latest-active CONNECTIONPOINTID -> DUID mapping (max END_DATE per connection point).

    END_DATE's "no end yet" sentinel is 2999/12/31, outside pandas' Timestamp range, so this
    compares the fixed-width "YYYY/MM/DD HH:MM:SS" strings directly rather than parsing dates.
    """
    headers, rows = _extract_table_rows(text, "PARTICIPANT_REGISTRATION,DUDETAILSUMMARY")
    empty = pd.DataFrame(columns=["connectionpointid", "duid"])
    if not headers or not rows:
        return empty

    df = pd.DataFrame(rows, columns=headers[: len(rows[0])])
    latest_idx = df.groupby("CONNECTIONPOINTID")["END_DATE"].idxmax()
    latest = df.loc[latest_idx]
    return latest.rename(
        columns={"CONNECTIONPOINTID": "connectionpointid", "DUID": "duid"}
    )[["connectionpointid", "duid"]].reset_index(drop=True)


def parse_spd_connection_point_csv(text: str) -> pd.DataFrame:
    """SPDCONNECTIONPOINTCONSTRAINT ENERGY-bidtype rows -> connectionpointid, constraintid, factor, effectivedate, versionno."""
    headers, rows = _extract_table_rows(text, "SPDCPC")
    columns = ["connectionpointid", "constraintid", "factor", "effectivedate", "versionno"]
    empty = pd.DataFrame(columns=columns)
    if not headers or not rows:
        return empty

    df = pd.DataFrame(rows, columns=headers[: len(rows[0])])
    df = df[df["BIDTYPE"] == "ENERGY"]
    if df.empty:
        return empty
    return pd.DataFrame({
        "connectionpointid": df["CONNECTIONPOINTID"],
        "constraintid": df["GENCONID"],
        "factor": pd.to_numeric(df["FACTOR"], errors="coerce"),
        "effectivedate": pd.to_datetime(df["EFFECTIVEDATE"], errors="coerce"),
        "versionno": pd.to_numeric(df["VERSIONNO"], errors="coerce").astype(int),
    }).reset_index(drop=True)


def parse_spd_interconnector_csv(text: str) -> pd.DataFrame:
    """SPDINTERCONNECTORCONSTRAINT rows -> interconnectorid, constraintid, factor, effectivedate, versionno (no BIDTYPE column)."""
    headers, rows = _extract_table_rows(text, "SPDICC")
    columns = ["interconnectorid", "constraintid", "factor", "effectivedate", "versionno"]
    empty = pd.DataFrame(columns=columns)
    if not headers or not rows:
        return empty

    df = pd.DataFrame(rows, columns=headers[: len(rows[0])])
    return pd.DataFrame({
        "interconnectorid": df["INTERCONNECTORID"],
        "constraintid": df["GENCONID"],
        "factor": pd.to_numeric(df["FACTOR"], errors="coerce"),
        "effectivedate": pd.to_datetime(df["EFFECTIVEDATE"], errors="coerce"),
        "versionno": pd.to_numeric(df["VERSIONNO"], errors="coerce").astype(int),
    }).reset_index(drop=True)


def parse_spd_region_csv(text: str) -> pd.DataFrame:
    """SPDREGIONCONSTRAINT ENERGY-bidtype rows -> regionid, constraintid, factor, effectivedate, versionno."""
    headers, rows = _extract_table_rows(text, "SPDRC")
    columns = ["regionid", "constraintid", "factor", "effectivedate", "versionno"]
    empty = pd.DataFrame(columns=columns)
    if not headers or not rows:
        return empty

    df = pd.DataFrame(rows, columns=headers[: len(rows[0])])
    df = df[df["BIDTYPE"] == "ENERGY"]
    if df.empty:
        return empty
    return pd.DataFrame({
        "regionid": df["REGIONID"],
        "constraintid": df["GENCONID"],
        "factor": pd.to_numeric(df["FACTOR"], errors="coerce"),
        "effectivedate": pd.to_datetime(df["EFFECTIVEDATE"], errors="coerce"),
        "versionno": pd.to_numeric(df["VERSIONNO"], errors="coerce").astype(int),
    }).reset_index(drop=True)


def latest_version_only(df: pd.DataFrame) -> pd.DataFrame:
    """Keep only rows at each constraintid's max (effectivedate, versionno) -- no history for v1."""
    if df.empty:
        return df
    max_effdate = df.groupby("constraintid")["effectivedate"].transform("max")
    at_latest_date = df[df["effectivedate"] == max_effdate]
    max_ver = at_latest_date.groupby("constraintid")["versionno"].transform("max")
    return at_latest_date[at_latest_date["versionno"] == max_ver]


def build_constraint_equation_terms(
    cpc_df: pd.DataFrame, icc_df: pd.DataFrame, rc_df: pd.DataFrame, dud_df: pd.DataFrame
) -> pd.DataFrame:
    """Combine the three SPD term sources into one terms table, DUID-mapped and latest-version-only.

    Connection points with no current DUDETAILSUMMARY mapping are dropped (see module docstring).
    """
    term_columns = ["constraintid", "term_type", "term_id", "factor", "effectivedate", "versionno"]

    duid_terms = cpc_df.merge(dud_df, on="connectionpointid", how="inner").rename(columns={"duid": "term_id"})
    duid_terms["term_type"] = "duid"

    ic_terms = icc_df.rename(columns={"interconnectorid": "term_id"})
    ic_terms["term_type"] = "interconnector"

    region_terms = rc_df.rename(columns={"regionid": "term_id"})
    region_terms["term_type"] = "region"

    frames = [
        df[term_columns] for df in (duid_terms, ic_terms, region_terms) if not df.empty
    ]
    if not frames:
        return pd.DataFrame(columns=["constraintid", "version", "term_type", "term_id", "factor"])
    combined = pd.concat(frames, ignore_index=True)
    combined = latest_version_only(combined)
    combined = combined.drop_duplicates(subset=["constraintid", "term_type", "term_id"], keep="first")
    return combined.rename(columns={"versionno": "version"})[
        ["constraintid", "version", "term_type", "term_id", "factor"]
    ].reset_index(drop=True)


async def find_latest_month(client: httpx.AsyncClient, today: Optional[date] = None) -> Tuple[int, int]:
    """Most recent (year, month) with a published MMSDM archive, checking this and last year's index."""
    today = today or date.today()
    months: List[Tuple[int, int]] = []
    for year in (today.year, today.year - 1):
        resp = await client.get(month_index_url(year))
        if resp.status_code == 200:
            months.extend(parse_available_months(resp.text))
    if not months:
        raise SystemExit("No MMSDM month archives found on the index page(s).")
    return max(months)


async def fetch_table_csv(client: httpx.AsyncClient, year: int, month: int, table: str) -> str:
    """Download one MMSDM monthly table zip and return its single CSV's text."""
    resp = await client.get(archive_url(year, month, table))
    resp.raise_for_status()
    with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
        name = z.namelist()[0]
        return z.read(name).decode("utf-8", "ignore")


async def build_terms_for_month(client: httpx.AsyncClient, year: int, month: int) -> pd.DataFrame:
    """Download all four source tables for one month and build the terms table."""
    dud_text = await fetch_table_csv(client, year, month, "DUDETAILSUMMARY")
    cpc_text = await fetch_table_csv(client, year, month, "SPDCONNECTIONPOINTCONSTRAINT")
    icc_text = await fetch_table_csv(client, year, month, "SPDINTERCONNECTORCONSTRAINT")
    rc_text = await fetch_table_csv(client, year, month, "SPDREGIONCONSTRAINT")
    return build_constraint_equation_terms(
        parse_spd_connection_point_csv(cpc_text),
        parse_spd_interconnector_csv(icc_text),
        parse_spd_region_csv(rc_text),
        parse_dudetailsummary_csv(dud_text),
    )


def _print_dry_run_report(terms: pd.DataFrame) -> None:
    print(f"distinct constraints: {terms['constraintid'].nunique():,}")
    for term_type, group in terms.groupby("term_type"):
        print(f"  {term_type}: {len(group):,} terms")
    print(terms.head(10).to_string(index=False))


async def ingest(dry_run: bool, year: Optional[int], month: Optional[int]) -> None:
    load_dotenv()
    async with httpx.AsyncClient(timeout=180.0, follow_redirects=True) as client:
        if year is None or month is None:
            year, month = await find_latest_month(client)
        print(f"Using MMSDM {year:04d}-{month:02d}")
        terms = await build_terms_for_month(client, year, month)

    if dry_run:
        _print_dry_run_report(terms)
        return

    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        raise SystemExit("DATABASE_URL is not set (check your .env).")
    db = NEMDatabase(db_url)
    await db.initialize()
    try:
        inserted = await db.insert_constraint_equation_terms(terms)
        print(f"Inserted/updated {inserted:,} constraint equation terms.")
    finally:
        await db.close()


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Ingest constraint equation terms from AEMO's MMSDM monthly archive."
    )
    ap.add_argument("--dry-run", action="store_true", help="download+parse only, print term counts, insert nothing")
    ap.add_argument("--year", type=int, default=None, help="MMSDM archive year (default: latest available)")
    ap.add_argument("--month", type=int, default=None, help="MMSDM archive month (default: latest available)")
    args = ap.parse_args()
    asyncio.run(ingest(args.dry_run, args.year, args.month))


if __name__ == "__main__":
    main()
