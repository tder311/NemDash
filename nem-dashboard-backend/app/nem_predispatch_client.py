"""NEM Pre-dispatch price client.

Fetches AEMO PREDISPATCH region prices (the RRP forecast out to ~end of next
trading day) from NEMWEB's PredispatchIS reports. Used to overlay AEMO's own
short-term price forecast against the model's forecast.
"""

import csv
import io
import logging
import re
import zipfile
from datetime import datetime
from typing import List, Optional, Tuple

import httpx
import pandas as pd

logger = logging.getLogger(__name__)

CURRENT_PATH = "Reports/Current/PredispatchIS_Reports/"
ARCHIVE_PATH = "Reports/Archive/PredispatchIS_Reports/"
CURRENT_FILE_RE = r"PUBLIC_PREDISPATCHIS_\d+_\d+\.zip"
ARCHIVE_FILE_RE = r"PUBLIC_PREDISPATCHIS_\d{8}\.zip"


class NEMPredispatchClient:
    """Client for fetching PREDISPATCH REGION_PRICES (RRP forecast) from NEMWEB."""

    def __init__(self, base_url: str = "https://www.nemweb.com.au"):
        self.base_url = base_url.rstrip("/")

    async def get_latest_predispatch(self) -> Optional[pd.DataFrame]:
        """Fetch the most recent pre-dispatch run's RRP forecast."""
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
                return self._parse_zip(file_resp.content)
        except Exception as e:
            logger.error(f"Error fetching pre-dispatch data: {e}")
            return None

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

    def _parse_csv(self, text: str) -> Optional[pd.DataFrame]:
        """Extract REGION_PRICES rows -> run_datetime, interval_datetime, regionid, rrp."""
        headers: Optional[List[str]] = None
        rows: List[List[str]] = []
        for line in text.splitlines():
            if line.startswith("I,PREDISPATCH,REGION_PRICES"):
                # skip record type, report, table, version (4 fields)
                headers = [h.lower().strip() for h in next(csv.reader([line]))[4:]]
            elif line.startswith("D,PREDISPATCH,REGION_PRICES") and headers is not None:
                rows.append(next(csv.reader([line]))[4:])

        if not headers or not rows:
            logger.warning("No PREDISPATCH REGION_PRICES rows found")
            return None

        df = pd.DataFrame(rows, columns=headers[: len(rows[0])])
        # Non-intervention runs only (avoid duplicate intervals).
        if "intervention" in df.columns:
            df = df[df["intervention"].astype(str).str.strip().isin(["0", "0.0", ""])]
        df = df.rename(columns={"datetime": "interval_datetime", "lastchanged": "run_datetime"})
        df["interval_datetime"] = pd.to_datetime(df["interval_datetime"], errors="coerce")
        df["run_datetime"] = pd.to_datetime(df["run_datetime"], errors="coerce")
        df["rrp"] = pd.to_numeric(df["rrp"], errors="coerce")
        df = df.dropna(subset=["interval_datetime", "regionid", "rrp"])
        out = df[["run_datetime", "interval_datetime", "regionid", "rrp"]].drop_duplicates(
            subset=["run_datetime", "interval_datetime", "regionid"], keep="last"
        )
        return out.reset_index(drop=True)
