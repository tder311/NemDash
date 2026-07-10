"""Ingest date-effective constraint equation terms from AEMO's NEMDE case-file archive.

Run from the backend directory (``nem-dashboard-backend/``):

    python -m scripts.ingest_nemde_constraints --dry-run --start 2026-05-15 --end 2026-05-15
    python -m scripts.ingest_nemde_constraints --start 2026-05-01 --end 2026-05-31

Source: one ~170MB zip per day of 288 five-minute ``.loaded`` XML case files,
``NemSpdOutputs_{YYYYMMDD}_loaded.zip`` (verified against 2026-05-15). Each XML's
``GenericConstraintCollection`` holds one ``<GenericConstraint ConstraintID
VersionNo EffectiveDate ...>`` per constraint, with a nested ``LHSFactorCollection``
of ``<TraderFactor Factor TradeType TraderID>`` (TraderID IS the DUID),
``<InterconnectorFactor Factor InterconnectorID>``, and ``<RegionFactor Factor
TradeType RegionID>``. All three are stored regardless of TradeType -- unlike the
legacy MMSDM ingest's ENERGY-bidtype filter, this keeps FCAS-only terms too;
region terms are already excluded downstream by the solver whenever no
region_demand is supplied, so this is a deliberate (documented) scope widening
for duid/interconnector terms that may warrant revisiting.

A day's constraint set turns over intraday (verified: ~5-10% of constraints
differ between consecutive 2-hour samples on 2026-05-15), but a constraint
living under 30 minutes can't bind a half-hourly predispatch solve, so sampling
every 2 hours (--samples-per-day 12, the default) catches every version that
matters while reading ~4% of a day's 288 members. Each day's zip is streamed to
a temp file and deleted before the next day is fetched -- only one day is ever
on disk (see scripts/backfill_predispatch.py for the same pattern).

Members are parsed with ``xml.etree.ElementTree.iterparse`` and only
``GenericConstraint`` elements are extracted; every element is cleared as soon
as its end tag is seen so the trader/case/region solutions that make up most of
each 12MB file are never retained.

``--dry-run`` downloads+parses one day only and prints per-type term counts,
distinct constraints, and (if DATABASE_URL is set) new-vs-already-known
(constraintid, version) pairs; it never writes.
"""

import argparse
import asyncio
import os
import tempfile
import xml.etree.ElementTree as ET
import zipfile
from typing import List, Optional

import httpx
import pandas as pd
from dotenv import load_dotenv

from app.database import NEMDatabase, SENTINEL_MMSDM_VERSION

BASE_URL = "https://www.nemweb.com.au"
ARCHIVE_DIR = "Data_Archive/Wholesale_Electricity/NEMDE"
DEFAULT_SAMPLES_PER_DAY = 12
TERM_COLUMNS = ["constraintid", "version", "effective_date", "term_type", "term_id", "factor"]
FACTOR_TAGS = {
    "TraderFactor": ("duid", "TraderID"),
    "InterconnectorFactor": ("interconnector", "InterconnectorID"),
    "RegionFactor": ("region", "RegionID"),
}


def day_zip_url(day: pd.Timestamp) -> str:
    """NEMDE day-zip URL for one date."""
    return (
        f"{BASE_URL}/{ARCHIVE_DIR}/{day.year:04d}/NEMDE_{day.year:04d}_{day.month:02d}/"
        f"NEMDE_Market_Data/NEMDE_Files/NemSpdOutputs_{day.strftime('%Y%m%d')}_loaded.zip"
    )


def select_sample_indices(n_members: int, samples_per_day: int) -> List[int]:
    """Evenly-spaced member indices across a day's (sorted) five-minute members."""
    if samples_per_day >= n_members:
        return list(range(n_members))
    step = n_members / samples_per_day
    return sorted({int(i * step) for i in range(samples_per_day)})


def _local_tag(tag: str) -> str:
    """Strip any XML namespace prefix ('{uri}Tag' -> 'Tag') from an ElementTree tag."""
    return tag.rsplit("}", 1)[-1]


def parse_generic_constraint(elem) -> List[dict]:
    """One <GenericConstraint> element -> its LHS term rows (duid/interconnector/region)."""
    constraintid = elem.get("ConstraintID")
    version = int(elem.get("VersionNo"))
    effective_date = pd.to_datetime(elem.get("EffectiveDate")).date()
    lhs = elem.find("LHSFactorCollection")
    if lhs is None:
        return []
    rows = []
    for child in lhs:
        mapping = FACTOR_TAGS.get(_local_tag(child.tag))
        if mapping is None:
            continue
        term_type, id_attr = mapping
        rows.append({
            "constraintid": constraintid,
            "version": version,
            "effective_date": effective_date,
            "term_type": term_type,
            "term_id": child.get(id_attr),
            "factor": float(child.get("Factor")),
        })
    return rows


# iterparse fires child "end" events before their parent's, so these must survive
# uncleared until GenericConstraint's own end event reads them.
_LHS_DESCENDANT_TAGS = {"LHSFactorCollection", "TraderFactor", "InterconnectorFactor", "RegionFactor"}


def parse_member_xml(fileobj) -> pd.DataFrame:
    """Stream-parse one .loaded XML member, keeping only GenericConstraint LHS terms.

    Every element is cleared as soon as its end tag is seen (except the LHSFactorCollection
    subtree, read only once its GenericConstraint parent's end event fires), so the tree never
    accumulates the trader/case/region solutions that make up most of the file's ~12MB.
    """
    rows: List[dict] = []
    for _, elem in ET.iterparse(fileobj, events=("end",)):
        tag = _local_tag(elem.tag)
        if tag == "GenericConstraint":
            rows.extend(parse_generic_constraint(elem))
            elem.clear()
        elif tag not in _LHS_DESCENDANT_TAGS:
            elem.clear()
    return pd.DataFrame(rows, columns=TERM_COLUMNS)


def dedupe_versions(terms: pd.DataFrame) -> pd.DataFrame:
    """One row per (constraintid, version, term_type, term_id) -- collapses repeat sightings
    of an already-seen version across the day's samples."""
    if terms.empty:
        return terms
    return terms.drop_duplicates(
        subset=["constraintid", "version", "term_type", "term_id"]
    ).reset_index(drop=True)


async def fetch_day_to_file(client: httpx.AsyncClient, url: str, path: str) -> None:
    """Stream one day's zip to a file on disk (never buffered whole in memory)."""
    async with client.stream("GET", url) as resp:
        resp.raise_for_status()
        with open(path, "wb") as f:
            async for chunk in resp.aiter_bytes():
                f.write(chunk)


def parse_day_zip(path: str, samples_per_day: int) -> pd.DataFrame:
    """Extract+parse a sampled subset of one day's members, deduped across samples."""
    frames = []
    with zipfile.ZipFile(path) as z:
        members = sorted(z.namelist())
        for idx in select_sample_indices(len(members), samples_per_day):
            with z.open(members[idx]) as f:
                frames.append(parse_member_xml(f))
    combined = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=TERM_COLUMNS)
    return dedupe_versions(combined)


async def fetch_known_versions(db: NEMDatabase) -> set:
    """Distinct (constraintid, version) pairs already stored (excludes the MMSDM sentinel)."""
    async with db._pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT DISTINCT constraintid, version FROM constraint_equation_terms WHERE version != $1",
            SENTINEL_MMSDM_VERSION,
        )
    return {(r["constraintid"], r["version"]) for r in rows}


def _print_dry_run_report(terms: pd.DataFrame, known: Optional[set]) -> None:
    print(f"distinct constraints: {terms['constraintid'].nunique():,}")
    for term_type, group in terms.groupby("term_type"):
        print(f"  {term_type}: {len(group):,} terms")

    version_keys = set(zip(terms["constraintid"], terms["version"]))
    if known is None:
        print(f"distinct (constraint, version) pairs: {len(version_keys):,} "
              f"(no DATABASE_URL -- skipping known-version comparison)")
    else:
        new = version_keys - known
        print(f"distinct (constraint, version) pairs: {len(version_keys):,} "
              f"({len(new):,} new, {len(version_keys) - len(new):,} already known)")
    print(terms.head(10).to_string(index=False))


async def ingest(start_str: str, end_str: str, samples_per_day: int, dry_run: bool) -> None:
    load_dotenv()
    start = pd.Timestamp(start_str).normalize()
    end = pd.Timestamp(end_str).normalize()
    days = list(pd.date_range(start, end, freq="D"))
    if dry_run:
        days = days[:1]

    db = None
    db_url = os.environ.get("DATABASE_URL")
    if not dry_run:
        if not db_url:
            raise SystemExit("DATABASE_URL is not set (check your .env).")
        db = NEMDatabase(db_url)
        await db.initialize()
    elif db_url:
        db = NEMDatabase(db_url)
        await db.initialize()

    failed: List[str] = []
    try:
        async with httpx.AsyncClient(timeout=600.0, follow_redirects=True) as client:
            print(f"{len(days)} day(s) to process, samples_per_day={samples_per_day}")
            for day in days:
                url = day_zip_url(day)
                tmp = tempfile.NamedTemporaryFile(suffix=".zip", delete=False)
                tmp.close()
                try:
                    await fetch_day_to_file(client, url, tmp.name)
                    terms = parse_day_zip(tmp.name, samples_per_day)
                except Exception as e:
                    print(f"{day.date()}: FAILED ({e})")
                    failed.append(str(day.date()))
                    continue
                finally:
                    os.remove(tmp.name)

                if dry_run:
                    known = await fetch_known_versions(db) if db is not None else None
                    _print_dry_run_report(terms, known)
                    return

                inserted = await db.insert_constraint_equation_terms(terms)
                print(f"{day.date()}: sampled {len(terms):,} term rows -> inserted/updated {inserted:,}")
    finally:
        if db is not None:
            await db.close()

    if failed:
        print(f"\n{len(failed)} day(s) failed: {', '.join(failed)}")
        raise SystemExit(1)


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Ingest date-effective constraint equation terms from AEMO's NEMDE case-file archive."
    )
    ap.add_argument("--start", required=True, help="first day, YYYY-MM-DD")
    ap.add_argument("--end", required=True, help="last day, YYYY-MM-DD")
    ap.add_argument(
        "--samples-per-day", type=int, default=DEFAULT_SAMPLES_PER_DAY,
        help="evenly-spaced 5-min members sampled per day (default: 12, i.e. every 2h)",
    )
    ap.add_argument("--dry-run", action="store_true", help="download+parse one day only, print term counts, insert nothing")
    args = ap.parse_args()
    asyncio.run(ingest(args.start, args.end, args.samples_per_day, args.dry_run))


if __name__ == "__main__":
    main()
