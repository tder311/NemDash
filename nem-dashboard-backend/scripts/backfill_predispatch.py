"""Backfill ``predispatch_price``/``predispatch_interconnector``/``predispatch_constraint``
from NEMWEB PredispatchIS weekly archives.

Run from the backend directory (``nem-dashboard-backend/``):

    python -m scripts.backfill_predispatch            # all available weeks
    python -m scripts.backfill_predispatch --weeks 4
    python -m scripts.backfill_predispatch --dry-run
    python -m scripts.backfill_predispatch --tables network --dry-run

Index: ``https://www.nemweb.com.au/Reports/Archive/PredispatchIS_Reports/``
holds one ~277MB zip per Sunday-Saturday week
(``PUBLIC_PREDISPATCHIS_YYYYMMDD_YYYYMMDD.zip``). Each contains one inner zip
per predispatch run (~336/week), ``PUBLIC_PREDISPATCHIS_YYYYMMDDHHMM_*.zip``,
whose CSV carries the run's full multi-hour half-hourly forecast in three
tables (verified against the 2025-06-22 week and a live 2026-07-09 run):
``PREDISPATCH,REGION_PRICES`` (RRP), ``PREDISPATCH,INTERCONNECTOR_SOLN``
(flows vs limits, ~6 interconnectors/interval), and
``PREDISPATCH,CONSTRAINT_SOLUTION`` (RHS/marginal value/violation degree,
~900 constraints/interval, ~1-2% binding). INTERVENTION column present on all
three tables.

run_datetime is the inner filename's YYYYMMDDHHMM token (the run's first
forecast interval; verified ~30min after the run's actual publication time).
Thinning selects ~4 runs/day BY FILENAME before extraction, so ~92% of members
are never parsed. Each weekly zip is streamed to a temp file and deleted
before the next week is fetched -- only one week is ever on disk.

``--tables`` selects which table groups to backfill (comma-separated):
``prices`` (predispatch_price) and/or ``network`` (predispatch_interconnector
+ predispatch_constraint, the constraint table binding-only). Re-running
``prices`` is harmless -- inserts are idempotent upserts.
"""

import argparse
import asyncio
import io
import os
import re
import tempfile
import zipfile
from csv import reader as csv_reader
from typing import Dict, List, Optional, Tuple

import httpx
import pandas as pd
from dotenv import load_dotenv

from app.database import NEMDatabase, filter_binding_constraints

BASE_URL = "https://www.nemweb.com.au"
ARCHIVE_PATH = "Reports/Archive/PredispatchIS_Reports"
WEEK_FILE_RE = r"PUBLIC_PREDISPATCHIS_\d{8}_\d{8}\.zip"
MEMBER_TOKEN_RE = r"PUBLIC_PREDISPATCHIS_(\d{12})_"
NEM_REGIONS = ("NSW1", "QLD1", "SA1", "TAS1", "VIC1")
DEFAULT_TARGET_TIMES = ("04:30", "10:30", "16:30", "22:30")
APPROX_WEEK_MB = 277
# Ballpark bytes/row (REAL/TEXT/TIMESTAMP columns + btree index overhead), for dry-run sizing only.
BYTES_PER_ROW_ESTIMATE = 120

TABLE_GROUPS = {"prices": ["prices"], "network": ["interconnector", "constraint"]}
INSERT_METHODS = {
    "prices": "insert_predispatch_price",
    "interconnector": "insert_predispatch_interconnector",
    "constraint": "insert_predispatch_constraint",
}
EMPTY_COLUMNS = {
    "prices": ["run_datetime", "interval_datetime", "regionid", "rrp"],
    "interconnector": [
        "run_datetime", "interval_datetime", "interconnectorid",
        "mwflow", "exportlimit", "importlimit", "marginalvalue",
    ],
    "constraint": [
        "run_datetime", "interval_datetime", "constraintid",
        "rhs", "marginalvalue", "violationdegree",
    ],
}


def resolve_table_keys(tables_arg: str) -> List[str]:
    """Expand a comma-separated --tables arg (e.g. "prices,network") into concrete table keys."""
    keys: List[str] = []
    for group in tables_arg.split(","):
        keys.extend(TABLE_GROUPS[group.strip()])
    return keys


def list_week_files(index_html: str) -> List[str]:
    """Weekly archive filenames from the index page HTML, sorted (oldest first)."""
    return sorted(set(re.findall(WEEK_FILE_RE, index_html)))


def week_url(filename: str) -> str:
    """Full download URL for one weekly archive file."""
    return f"{BASE_URL}/{ARCHIVE_PATH}/{filename}"


def member_run_datetime(name: str) -> pd.Timestamp:
    """Inner member name -> run_datetime from its YYYYMMDDHHMM token."""
    token = re.search(MEMBER_TOKEN_RE, name).group(1)
    return pd.Timestamp(f"{token[:8]} {token[8:10]}:{token[10:12]}")


def select_members(
    members: List[str], target_times: Tuple[str, ...] = DEFAULT_TARGET_TIMES
) -> List[str]:
    """Per calendar day, the member nearest each target run time (by filename only)."""
    df = pd.DataFrame({"name": members})
    df["run_datetime"] = df["name"].map(member_run_datetime)
    df = df.sort_values("run_datetime")
    day = df["run_datetime"].dt.normalize()
    keep = set()
    for time_str in target_times:
        target = day + pd.Timedelta(f"{time_str}:00")
        dist = (df["run_datetime"] - target).abs()
        nearest = df.loc[dist.groupby(day.values).idxmin(), "name"]
        keep.update(nearest)
    return df.loc[df["name"].isin(keep), "name"].tolist()


def parse_predispatch_csv(text: str) -> pd.DataFrame:
    """Extract PREDISPATCH,REGION_PRICES D rows -> interval_datetime, regionid, rrp."""
    headers: Optional[List[str]] = None
    rows: List[List[str]] = []
    for line in text.splitlines():
        if line.startswith("I,PREDISPATCH,REGION_PRICES"):
            headers = [h.strip() for h in next(csv_reader([line]))[4:]]
        elif line.startswith("D,PREDISPATCH,REGION_PRICES") and headers is not None:
            rows.append(next(csv_reader([line]))[4:])

    empty = pd.DataFrame(columns=["interval_datetime", "regionid", "rrp"])
    if not headers or not rows:
        return empty

    df = pd.DataFrame(rows, columns=headers[: len(rows[0])])
    df = df[df["REGIONID"].isin(NEM_REGIONS)]
    if "INTERVENTION" in df.columns:
        df = df[df["INTERVENTION"].astype(str).str.strip() == "0"]
    if df.empty:
        return empty

    out = pd.DataFrame(
        {
            "interval_datetime": pd.to_datetime(df["DATETIME"], errors="coerce"),
            "regionid": df["REGIONID"],
            "rrp": pd.to_numeric(df["RRP"], errors="coerce"),
        }
    )
    return out.dropna(subset=["interval_datetime", "rrp"]).reset_index(drop=True)


def build_run_dataframe(text: str, run_datetime: pd.Timestamp) -> pd.DataFrame:
    """Parse one run's CSV and attach its run_datetime."""
    columns = ["run_datetime", "interval_datetime", "regionid", "rrp"]
    parsed = parse_predispatch_csv(text)
    if parsed.empty:
        return pd.DataFrame(columns=columns)
    parsed.insert(0, "run_datetime", run_datetime)
    return parsed[columns]


def parse_predispatch_interconnector_csv(text: str) -> pd.DataFrame:
    """Extract PREDISPATCH,INTERCONNECTOR_SOLN D rows -> interval_datetime, interconnectorid,
    mwflow, exportlimit, importlimit, marginalvalue."""
    headers: Optional[List[str]] = None
    rows: List[List[str]] = []
    for line in text.splitlines():
        if line.startswith("I,PREDISPATCH,INTERCONNECTOR_SOLN"):
            headers = [h.strip() for h in next(csv_reader([line]))[4:]]
        elif line.startswith("D,PREDISPATCH,INTERCONNECTOR_SOLN") and headers is not None:
            rows.append(next(csv_reader([line]))[4:])

    empty = pd.DataFrame(columns=EMPTY_COLUMNS["interconnector"][1:])
    if not headers or not rows:
        return empty

    df = pd.DataFrame(rows, columns=headers[: len(rows[0])])
    if "INTERVENTION" in df.columns:
        df = df[df["INTERVENTION"].astype(str).str.strip() == "0"]
    if df.empty:
        return empty

    out = pd.DataFrame({
        "interval_datetime": pd.to_datetime(df["DATETIME"], errors="coerce"),
        "interconnectorid": df["INTERCONNECTORID"],
        "mwflow": pd.to_numeric(df["MWFLOW"], errors="coerce"),
        "exportlimit": pd.to_numeric(df["EXPORTLIMIT"], errors="coerce"),
        "importlimit": pd.to_numeric(df["IMPORTLIMIT"], errors="coerce"),
        "marginalvalue": pd.to_numeric(df["MARGINALVALUE"], errors="coerce"),
    })
    return out.dropna(subset=["interval_datetime", "interconnectorid"]).reset_index(drop=True)


def parse_predispatch_constraint_csv(text: str) -> pd.DataFrame:
    """Extract PREDISPATCH,CONSTRAINT_SOLUTION D rows -> interval_datetime, constraintid, rhs,
    marginalvalue, violationdegree (unfiltered; binding-only filter applied downstream)."""
    headers: Optional[List[str]] = None
    rows: List[List[str]] = []
    for line in text.splitlines():
        if line.startswith("I,PREDISPATCH,CONSTRAINT_SOLUTION"):
            headers = [h.strip() for h in next(csv_reader([line]))[4:]]
        elif line.startswith("D,PREDISPATCH,CONSTRAINT_SOLUTION") and headers is not None:
            rows.append(next(csv_reader([line]))[4:])

    empty = pd.DataFrame(columns=EMPTY_COLUMNS["constraint"][1:])
    if not headers or not rows:
        return empty

    df = pd.DataFrame(rows, columns=headers[: len(rows[0])])
    if "INTERVENTION" in df.columns:
        df = df[df["INTERVENTION"].astype(str).str.strip() == "0"]
    if df.empty:
        return empty

    out = pd.DataFrame({
        "interval_datetime": pd.to_datetime(df["DATETIME"], errors="coerce"),
        "constraintid": df["CONSTRAINTID"],
        "rhs": pd.to_numeric(df["RHS"], errors="coerce"),
        "marginalvalue": pd.to_numeric(df["MARGINALVALUE"], errors="coerce"),
        "violationdegree": pd.to_numeric(df["VIOLATIONDEGREE"], errors="coerce"),
    })
    return out.dropna(subset=["interval_datetime", "constraintid"]).reset_index(drop=True)


def build_run_interconnector_dataframe(text: str, run_datetime: pd.Timestamp) -> pd.DataFrame:
    """Parse one run's CSV interconnector rows and attach its run_datetime."""
    columns = EMPTY_COLUMNS["interconnector"]
    parsed = parse_predispatch_interconnector_csv(text)
    if parsed.empty:
        return pd.DataFrame(columns=columns)
    parsed.insert(0, "run_datetime", run_datetime)
    return parsed[columns]


def build_run_constraint_dataframe(text: str, run_datetime: pd.Timestamp) -> pd.DataFrame:
    """Parse one run's CSV constraint rows (binding-or-violated only) and attach its run_datetime."""
    columns = EMPTY_COLUMNS["constraint"]
    parsed = filter_binding_constraints(parse_predispatch_constraint_csv(text))
    if parsed.empty:
        return pd.DataFrame(columns=columns)
    parsed.insert(0, "run_datetime", run_datetime)
    return parsed[columns]


BUILDERS = {
    "prices": build_run_dataframe,
    "interconnector": build_run_interconnector_dataframe,
    "constraint": build_run_constraint_dataframe,
}


async def fetch_week_to_file(client: httpx.AsyncClient, url: str, path: str) -> None:
    """Stream one weekly zip to a file on disk (never buffered whole in memory)."""
    async with client.stream("GET", url) as resp:
        resp.raise_for_status()
        with open(path, "wb") as f:
            async for chunk in resp.aiter_bytes():
                f.write(chunk)


def parse_week_zip(path: str, table_keys: List[str], verbose: bool = False) -> Dict[str, pd.DataFrame]:
    """Thin a weekly zip's members by filename, then extract+parse only the requested tables."""
    frames: Dict[str, List[pd.DataFrame]] = {key: [] for key in table_keys}
    with zipfile.ZipFile(path) as outer:
        members = [n for n in outer.namelist() if n.lower().endswith(".zip")]
        for name in select_members(members):
            with zipfile.ZipFile(io.BytesIO(outer.read(name))) as inner:
                text = inner.read(inner.namelist()[0]).decode("utf-8", "ignore")
            run_datetime = member_run_datetime(name)
            counts = []
            for key in table_keys:
                df = BUILDERS[key](text, run_datetime)
                frames[key].append(df)
                counts.append(f"{key}={len(df)}")
            if verbose:
                print(f"  {name}: {', '.join(counts)}")
    return {
        key: (pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame(columns=EMPTY_COLUMNS[key]))
        for key, dfs in frames.items()
    }


def _print_dry_run_report(filename: str, table_frames: Dict[str, pd.DataFrame]) -> None:
    """Per-table row counts for the sampled week plus a full-year volume estimate."""
    print(f"{filename} (dry-run, no insert):")
    for key, df in table_frames.items():
        if df.empty:
            print(f"  {key}: 0 rows")
            continue
        n_days = df["run_datetime"].dt.normalize().nunique()
        per_day = len(df) / n_days
        year_rows = per_day * 365
        year_mb = year_rows * BYTES_PER_ROW_ESTIMATE / (1024 * 1024)
        print(
            f"  {key}: {df['run_datetime'].nunique()} runs, {len(df):,} rows "
            f"(~{per_day:,.0f}/day; ~{year_rows:,.0f} rows/yr, ~{year_mb:,.0f}MB/yr)"
        )
        print(df.head(5).to_string(index=False))


async def backfill(weeks: Optional[int], dry_run: bool, tables_arg: str) -> None:
    load_dotenv()
    table_keys = resolve_table_keys(tables_arg)

    db = None
    if not dry_run:
        db_url = os.environ.get("DATABASE_URL")
        if not db_url:
            raise SystemExit("DATABASE_URL is not set (check your .env).")
        db = NEMDatabase(db_url)
        await db.initialize()

    failed: List[str] = []
    try:
        async with httpx.AsyncClient(timeout=600.0, follow_redirects=True) as client:
            resp = await client.get(f"{BASE_URL}/{ARCHIVE_PATH}/")
            resp.raise_for_status()
            week_files = list_week_files(resp.text)
            if not week_files:
                raise SystemExit("No weekly archives found on the index page.")
            if weeks is not None:
                week_files = week_files[-weeks:]
            if dry_run:
                week_files = week_files[:1]
            print(
                f"{len(week_files)} weekly archive(s) to process, tables={table_keys} "
                f"(~{len(week_files) * APPROX_WEEK_MB / 1024:.1f} GB total transfer)"
            )

            for filename in week_files:
                tmp = tempfile.NamedTemporaryFile(suffix=".zip", delete=False)
                tmp.close()
                try:
                    await fetch_week_to_file(client, week_url(filename), tmp.name)
                    table_frames = parse_week_zip(tmp.name, table_keys, verbose=dry_run)
                except Exception as e:
                    print(f"{filename}: FAILED ({e})")
                    failed.append(filename)
                    continue
                finally:
                    os.remove(tmp.name)

                if dry_run:
                    _print_dry_run_report(filename, table_frames)
                    return

                for key, df in table_frames.items():
                    inserted = await getattr(db, INSERT_METHODS[key])(df)
                    print(f"{filename} [{key}]: thinned to {len(df):,} rows -> inserted {inserted:,}")
    finally:
        if db is not None:
            await db.close()

    if failed:
        print(f"\n{len(failed)} week(s) failed: {', '.join(failed)}")
        raise SystemExit(1)


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Backfill predispatch_price/predispatch_interconnector/predispatch_constraint "
        "from PredispatchIS weekly archives."
    )
    ap.add_argument("--weeks", type=int, default=None, help="last N weeks only (default: all)")
    ap.add_argument("--dry-run", action="store_true", help="download+thin+parse one week, insert nothing")
    ap.add_argument(
        "--tables", default="prices,network",
        help="comma-separated table groups: prices, network (default: prices,network)",
    )
    args = ap.parse_args()
    asyncio.run(backfill(args.weeks, args.dry_run, args.tables))


if __name__ == "__main__":
    main()
