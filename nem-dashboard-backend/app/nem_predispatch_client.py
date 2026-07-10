"""NEM 7-day Pre-dispatch price client.

Fetches AEMO's PD7Day forecast (RRP at 30-min resolution, ~7 days ahead) from
NEMWEB's PD7Day reports. Published every ~6 hours. Used to overlay AEMO's
official 7-day pre-dispatch price against the model's forecast.
"""

import csv
import io
import logging
import re
import zipfile
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import httpx
import pandas as pd

logger = logging.getLogger(__name__)

CURRENT_PATH = "Reports/Current/PD7Day/"
ARCHIVE_PATH = "Reports/Archive/PD7Day/"
CURRENT_FILE_RE = r"PUBLIC_PD7DAY_\d+_\d+\.zip"
ARCHIVE_FILE_RE = r"PUBLIC_PD7DAY_\d{8}\.zip"

# Numeric columns pulled from each PD7Day table (lowercased, post-parse).
INTERCONNECTOR_NUMERIC_COLS = ["mwflow", "exportlimit", "importlimit", "marginalvalue"]
CONSTRAINT_NUMERIC_COLS = ["rhs", "marginalvalue", "violationdegree", "lhs"]


class NEMPredispatchClient:
    """Client for fetching PD7Day PRICESOLUTION/INTERCONNECTORSOLUTION/CONSTRAINTSOLUTION from NEMWEB."""

    def __init__(self, base_url: str = "https://www.nemweb.com.au"):
        self.base_url = base_url.rstrip("/")

    async def _fetch_latest_zip_content(self) -> Optional[bytes]:
        """Download the most recent PD7Day run's zip bytes (single download)."""
        url = f"{self.base_url}/{CURRENT_PATH}"
        try:
            async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
                resp = await client.get(url)
                resp.raise_for_status()
                files = re.findall(CURRENT_FILE_RE, resp.text)
                if not files:
                    logger.warning("No PREDISPATCHIS files found")
                    return None
                latest = sorted(files)[-1]
                file_resp = await client.get(f"{url}{latest}")
                file_resp.raise_for_status()
                return file_resp.content
        except Exception as e:
            logger.error(f"Error fetching pre-dispatch data: {e}")
            return None

    async def get_latest_predispatch(self) -> Optional[pd.DataFrame]:
        """Fetch the most recent PD7Day run's 7-day RRP forecast."""
        content = await self._fetch_latest_zip_content()
        if content is None:
            return None
        return self._parse_zip(content)

    async def get_latest_predispatch_all(self) -> Optional[Dict[str, Optional[pd.DataFrame]]]:
        """Fetch the most recent PD7Day run's price, interconnector, and constraint tables.

        Single download; all three tables are parsed from the same CSV.
        """
        content = await self._fetch_latest_zip_content()
        if content is None:
            return None
        try:
            with zipfile.ZipFile(io.BytesIO(content)) as z:
                name = z.namelist()[0]
                text = z.read(name).decode("utf-8", "ignore")
        except Exception as e:
            logger.error(f"Error parsing pre-dispatch zip: {e}")
            return None
        return {
            "prices": self._parse_csv(text),
            "interconnector": self._parse_interconnector_csv(text),
            "constraint": self._parse_constraint_csv(text),
        }

    async def list_archive_files(self) -> List[Tuple[str, datetime]]:
        """List (filename, file_date) in the pre-dispatch archive directory."""
        url = f"{self.base_url}/{ARCHIVE_PATH}"
        async with httpx.AsyncClient(timeout=60.0, follow_redirects=True) as client:
            resp = await client.get(url)
            resp.raise_for_status()
        names = sorted(set(re.findall(ARCHIVE_FILE_RE, resp.text)))
        out: List[Tuple[str, datetime]] = []
        for name in names:
            m = re.search(r"(\d{8})", name)
            if m:
                out.append((name, datetime.strptime(m.group(1), "%Y%m%d")))
        return out

    async def get_archive_predispatch_file(self, filename: str) -> Optional[pd.DataFrame]:
        """Download one archive file (zip of per-run zips) and parse every run."""
        url = f"{self.base_url}/{ARCHIVE_PATH}{filename}"
        try:
            async with httpx.AsyncClient(timeout=180.0, follow_redirects=True) as client:
                resp = await client.get(url)
                resp.raise_for_status()
                blob = resp.content
        except Exception as e:
            logger.error(f"Error downloading archive {filename}: {e}")
            return None

        frames = []
        try:
            with zipfile.ZipFile(io.BytesIO(blob)) as outer:
                for inner_name in outer.namelist():
                    if not inner_name.lower().endswith(".zip"):
                        continue
                    df = self._parse_zip(outer.read(inner_name))
                    if df is not None and not df.empty:
                        frames.append(df)
        except zipfile.BadZipFile as e:
            logger.error(f"Bad archive zip {filename}: {e}")
            return None

        if not frames:
            return None
        return pd.concat(frames, ignore_index=True)

    def _parse_zip(self, content: bytes) -> Optional[pd.DataFrame]:
        """Read a single-CSV zip and parse its REGION_PRICES table."""
        try:
            with zipfile.ZipFile(io.BytesIO(content)) as z:
                name = z.namelist()[0]
                text = z.read(name).decode("utf-8", "ignore")
            return self._parse_csv(text)
        except Exception as e:
            logger.error(f"Error parsing pre-dispatch zip: {e}")
            return None

    def _extract_table(self, text: str, table: str) -> Optional[pd.DataFrame]:
        """Extract raw I/D rows for one PD7DAY table -> DataFrame with lowercase columns."""
        prefix_i = f"I,PD7DAY,{table}"
        prefix_d = f"D,PD7DAY,{table}"
        headers: Optional[List[str]] = None
        rows: List[List[str]] = []
        for line in text.splitlines():
            if line.startswith(prefix_i):
                # skip record type, report, table, version (4 fields)
                headers = [h.lower().strip() for h in next(csv.reader([line]))[4:]]
            elif line.startswith(prefix_d) and headers is not None:
                rows.append(next(csv.reader([line]))[4:])
        if not headers or not rows:
            return None
        return pd.DataFrame(rows, columns=headers[: len(rows[0])])

    def _filter_non_intervention(self, df: pd.DataFrame) -> pd.DataFrame:
        """Keep only intervention=0 rows (avoid duplicate intervals from intervention runs)."""
        if "intervention" not in df.columns:
            return df
        return df[df["intervention"].astype(str).str.strip().isin(["0", "0.0", ""])]

    def _parse_csv(self, text: str) -> Optional[pd.DataFrame]:
        """Extract PD7DAY PRICESOLUTION rows -> run_datetime, interval_datetime, regionid, rrp."""
        df = self._extract_table(text, "PRICESOLUTION")
        if df is None:
            logger.warning("No PD7DAY PRICESOLUTION rows found")
            return None

        df = self._filter_non_intervention(df)
        df["interval_datetime"] = pd.to_datetime(df["interval_datetime"], errors="coerce")
        df["run_datetime"] = pd.to_datetime(df["run_datetime"], errors="coerce")
        df["rrp"] = pd.to_numeric(df["rrp"], errors="coerce")
        df = df.dropna(subset=["interval_datetime", "regionid", "rrp"])
        out = df[["run_datetime", "interval_datetime", "regionid", "rrp"]].drop_duplicates(
            subset=["run_datetime", "interval_datetime", "regionid"], keep="last"
        )
        return out.reset_index(drop=True)

    def _parse_interconnector_csv(self, text: str) -> Optional[pd.DataFrame]:
        """Extract PD7DAY INTERCONNECTORSOLUTION rows -> run_datetime, interval_datetime, interconnectorid, mwflow, exportlimit, importlimit, marginalvalue."""
        df = self._extract_table(text, "INTERCONNECTORSOLUTION")
        if df is None:
            logger.warning("No PD7DAY INTERCONNECTORSOLUTION rows found")
            return None

        df = self._filter_non_intervention(df)
        df[INTERCONNECTOR_NUMERIC_COLS] = df[INTERCONNECTOR_NUMERIC_COLS].apply(pd.to_numeric, errors="coerce")
        df["interval_datetime"] = pd.to_datetime(df["interval_datetime"], errors="coerce")
        df["run_datetime"] = pd.to_datetime(df["run_datetime"], errors="coerce")
        df = df.dropna(subset=["interval_datetime", "interconnectorid"])
        out = df[["run_datetime", "interval_datetime", "interconnectorid"] + INTERCONNECTOR_NUMERIC_COLS]
        out = out.drop_duplicates(
            subset=["run_datetime", "interval_datetime", "interconnectorid"], keep="last"
        )
        return out.reset_index(drop=True)

    def _parse_constraint_csv(self, text: str) -> Optional[pd.DataFrame]:
        """Extract PD7DAY CONSTRAINTSOLUTION rows -> run_datetime, interval_datetime, constraintid, rhs, marginalvalue, violationdegree, lhs."""
        df = self._extract_table(text, "CONSTRAINTSOLUTION")
        if df is None:
            logger.warning("No PD7DAY CONSTRAINTSOLUTION rows found")
            return None

        df = self._filter_non_intervention(df)
        df[CONSTRAINT_NUMERIC_COLS] = df[CONSTRAINT_NUMERIC_COLS].apply(pd.to_numeric, errors="coerce")
        df["interval_datetime"] = pd.to_datetime(df["interval_datetime"], errors="coerce")
        df["run_datetime"] = pd.to_datetime(df["run_datetime"], errors="coerce")
        df = df.dropna(subset=["interval_datetime", "constraintid"])
        out = df[["run_datetime", "interval_datetime", "constraintid"] + CONSTRAINT_NUMERIC_COLS]
        out = out.drop_duplicates(
            subset=["run_datetime", "interval_datetime", "constraintid"], keep="last"
        )
        return out.reset_index(drop=True)
