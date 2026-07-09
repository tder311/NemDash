"""Backfill ``predispatch_price`` from AEMO's MMS monthly archives.

Run from the backend directory (``nem-dashboard-backend/``):

    python -m scripts.backfill_predispatch --months 12
    python -m scripts.backfill_predispatch --dry-run

Downloads the ``PREDISPATCHPRICE`` (PREDISPATCH,REGION_PRICES) table from AEMO's
MMSDM historical archive at
``https://www.nemweb.com.au/Data_Archive/Wholesale_Electricity/MMSDM/{YYYY}/MMSDM_{YYYY}_{MM}/
MMSDM_Historical_Data_SQLLoader/DATA/PUBLIC_ARCHIVE#PREDISPATCHPRICE#FILE01#{YYYYMM}010000.zip``
(verified via directory listing; filenames use a literal ``#`` which must be
percent-encoded as ``%23`` in the request URL).

IMPORTANT: this archived table only retains PERIODID=01 per run (the run's
immediate next 30-min interval) -- not the full multi-hour PD lookahead curve.
Verified empirically: ~240 rows/day (48 runs x 5 regions), RRP1..RRP8 always
blank. So each row already is exactly one (run_datetime, interval_datetime)
pair with a ~30min lead time; "thinning" below selects which runs to keep,
not which lead times.

PREDISPATCHSEQNO = YYYYMMDDPP where YYYYMMDD is the trading day (starts 04:00)
and PP (01-48) is the run number within it. Verified against a downloaded
sample (2026-05): run_datetime = trading_day 04:00 + (PP-1)*30min lines up
with LASTCHANGED (within ~2min), and interval_datetime (DATETIME column) is
always exactly run_datetime + 30min, consecutive seqnos stepping by 30min.
"""

import argparse
import asyncio
import io
import os
import zipfile
from csv import reader as csv_reader
from datetime import date, timedelta
from typing import List, Optional, Tuple

import httpx
import pandas as pd
from dotenv import load_dotenv

from app.database import NEMDatabase

BASE_URL = "https://www.nemweb.com.au"
ARCHIVE_DIR = "Data_Archive/Wholesale_Electricity/MMSDM"
NEM_REGIONS = ("NSW1", "QLD1", "SA1", "TAS1", "VIC1")
DEFAULT_TARGET_RUN_HOURS = (4, 10, 16, 22)


def archive_url(year: int, month: int) -> str:
    """Build the MMSDM monthly PREDISPATCHPRICE archive URL."""
    ym_dir = f"{year:04d}_{month:02d}"
    ym_file = f"{year:04d}{month:02d}"
    return (
        f"{BASE_URL}/{ARCHIVE_DIR}/{year:04d}/MMSDM_{ym_dir}/MMSDM_Historical_Data_SQLLoader/DATA/"
        f"PUBLIC_ARCHIVE%23PREDISPATCHPRICE%23FILE01%23{ym_file}010000.zip"
    )


def last_n_complete_months(n: int, today: Optional[date] = None) -> List[Tuple[int, int]]:
    """The n calendar months before the current (incomplete) one, oldest first."""
    today = today or date.today()
    cur = today.replace(day=1)
    months = []
    for _ in range(n):
        cur = (cur - timedelta(days=1)).replace(day=1)
        months.append((cur.year, cur.month))
    months.reverse()
    return months


def seqno_to_run_datetime(seqno: pd.Series) -> pd.Series:
    """PREDISPATCHSEQNO YYYYMMDDPP -> trading-day(04:00) + (PP-1)*30min = run_datetime."""
    trading_day = pd.to_datetime(seqno.str[:8], format="%Y%m%d")
    run_no = seqno.str[8:10].astype(int)
    return trading_day + pd.Timedelta(hours=4) + pd.to_timedelta((run_no - 1) * 30, unit="m")


def select_thinned_runs(
    run_datetime: pd.Series, target_hours: Tuple[int, ...] = DEFAULT_TARGET_RUN_HOURS
) -> pd.Series:
    """Mask keeping, per calendar day, the run nearest each target hour."""
    unique_runs = pd.Series(pd.unique(run_datetime)).sort_values().reset_index(drop=True)
    calendar_day = unique_runs.dt.normalize()
    keep = set()
    for day, group in unique_runs.groupby(calendar_day):
        for hour in target_hours:
            target = day + pd.Timedelta(hours=hour)
            nearest = group.iloc[(group - target).abs().to_numpy().argmin()]
            keep.add(nearest)
    return run_datetime.isin(keep)


def parse_predispatch_csv(text: str) -> pd.DataFrame:
    """Extract PREDISPATCH,REGION_PRICES D rows -> seqno, interval_datetime, regionid, rrp."""
    headers: Optional[List[str]] = None
    rows: List[List[str]] = []
    for line in text.splitlines():
        if line.startswith("I,PREDISPATCH,REGION_PRICES"):
            headers = [h.strip() for h in next(csv_reader([line]))[4:]]
        elif line.startswith("D,PREDISPATCH,REGION_PRICES") and headers is not None:
            rows.append(next(csv_reader([line]))[4:])

    empty = pd.DataFrame(columns=["seqno", "interval_datetime", "regionid", "rrp"])
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
            "seqno": df["PREDISPATCHSEQNO"],
            "interval_datetime": pd.to_datetime(df["DATETIME"], errors="coerce"),
            "regionid": df["REGIONID"],
            "rrp": pd.to_numeric(df["RRP"], errors="coerce"),
        }
    )
    return out.dropna(subset=["interval_datetime", "rrp"]).reset_index(drop=True)


def build_month_dataframe(
    text: str, target_hours: Tuple[int, ...] = DEFAULT_TARGET_RUN_HOURS
) -> pd.DataFrame:
    """Parse a month's CSV, derive run_datetime, and thin to ~4 runs/day."""
    columns = ["run_datetime", "interval_datetime", "regionid", "rrp"]
    parsed = parse_predispatch_csv(text)
    if parsed.empty:
        return pd.DataFrame(columns=columns)
    parsed["run_datetime"] = seqno_to_run_datetime(parsed["seqno"])
    mask = select_thinned_runs(parsed["run_datetime"], target_hours)
    return parsed.loc[mask, columns].reset_index(drop=True)


async def fetch_month_zip(client: httpx.AsyncClient, year: int, month: int) -> bytes:
    """Download one month's PREDISPATCHPRICE archive zip."""
    resp = await client.get(archive_url(year, month))
    resp.raise_for_status()
    return resp.content


def extract_csv_text(zip_bytes: bytes) -> str:
    """Read the single CSV inside a monthly archive zip."""
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as z:
        name = z.namelist()[0]
        return z.read(name).decode("utf-8", "ignore")


async def backfill(months: int, dry_run: bool) -> None:
    load_dotenv()
    month_list = last_n_complete_months(months)
    if dry_run:
        month_list = month_list[:1]

    db = None
    if not dry_run:
        db_url = os.environ.get("DATABASE_URL")
        if not db_url:
            raise SystemExit("DATABASE_URL is not set (check your .env).")
        db = NEMDatabase(db_url)
        await db.initialize()

    failed: List[str] = []
    try:
        async with httpx.AsyncClient(timeout=180.0, follow_redirects=True) as client:
            for year, month in month_list:
                label = f"{year:04d}-{month:02d}"
                try:
                    blob = await fetch_month_zip(client, year, month)
                    text = extract_csv_text(blob)
                    df = build_month_dataframe(text)
                except Exception as e:
                    print(f"{label}: FAILED ({e})")
                    failed.append(label)
                    continue

                if dry_run:
                    print(f"{label}: parsed+thinned {len(df):,} rows (dry-run, no insert)")
                    print(df.head(10).to_string(index=False))
                    return

                inserted = await db.insert_predispatch_price(df)
                print(f"{label}: parsed+thinned {len(df):,} rows -> inserted {inserted:,}")
    finally:
        if db is not None:
            await db.close()

    if failed:
        print(f"\n{len(failed)} month(s) failed: {', '.join(failed)}")
        raise SystemExit(1)


def main() -> None:
    ap = argparse.ArgumentParser(description="Backfill predispatch_price from MMS monthly archives.")
    ap.add_argument("--months", type=int, default=12, help="number of complete months to backfill")
    ap.add_argument("--dry-run", action="store_true", help="download+parse+thin one month, insert nothing")
    args = ap.parse_args()
    asyncio.run(backfill(args.months, args.dry_run))


if __name__ == "__main__":
    main()
