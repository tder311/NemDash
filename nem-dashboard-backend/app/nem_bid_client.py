"""
NEM Bid Data Client

Downloads and parses Bidmove_Complete files from NEMWEB containing
BIDDAYOFFER (price bands) and BIDPEROFFER (quantity bands per interval)
for all generators in the NEM.
"""

import httpx
import pandas as pd
from datetime import datetime, timedelta
from typing import Optional, Tuple
import logging
import zipfile
import io
import re

logger = logging.getLogger(__name__)


class NEMBidClient:
    """Client for fetching bid data from NEMWEB Bidmove_Complete reports."""

    def __init__(self, base_url: str = "https://www.nemweb.com.au"):
        self.base_url = base_url.rstrip('/')

    async def get_daily_bids(self, date: datetime) -> Optional[Tuple[pd.DataFrame, pd.DataFrame]]:
        """Fetch bid data for a single date (all DUIDs, ENERGY only).

        Tries Current directory first (recent ~3 days), then Archive.

        Args:
            date: The date to fetch data for.

        Returns:
            Tuple of (biddayoffer_df, bidperoffer_df) or None if unavailable.
        """
        target_date = date.date() if hasattr(date, 'date') else date

        # Try Current directory first
        result = await self._fetch_from_current(target_date)
        if result is not None:
            return result

        # Fall back to Archive
        result = await self._fetch_from_archive(target_date)
        if result is not None:
            return result

        logger.warning(f"No bid data found for {target_date} in Current or Archive")
        return None

    async def _fetch_from_current(self, target_date) -> Optional[Tuple[pd.DataFrame, pd.DataFrame]]:
        """Fetch bid data from Current directory (recent ~3 days)."""
        try:
            current_url = f"{self.base_url}/Reports/Current/Bidmove_Complete/"
            date_str = target_date.strftime("%Y%m%d")

            async with httpx.AsyncClient(timeout=120.0, headers={"User-Agent": "NEM-Dashboard/1.0"}) as client:
                response = await client.get(current_url)
                response.raise_for_status()

                # Find matching files for the target date
                # Real filenames: PUBLIC_BIDMOVE_COMPLETE_20260221_0000000504578183.zip
                pattern = rf'(PUBLIC_BIDMOVE_COMPLETE_{date_str}_\d{{16}}\.zip)'
                matches = list(set(re.findall(pattern, response.text)))

                if not matches:
                    logger.debug(f"No Bidmove_Complete files found in Current for {date_str}")
                    return None

                # Use the latest version
                latest_file = sorted(matches)[-1]
                file_url = f"{current_url}{latest_file}"
                logger.info(f"Fetching bid data from Current: {latest_file}")

                file_response = await client.get(file_url)
                file_response.raise_for_status()

                return self._parse_bidmove_zip(file_response.content, target_date)

        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                logger.debug(f"Bidmove Current directory not available")
            else:
                logger.warning(f"HTTP error fetching bids from Current for {target_date}: {e}")
            return None
        except Exception as e:
            logger.debug(f"Could not fetch bids from Current for {target_date}: {e}")
            return None

    async def _fetch_from_archive(self, target_date) -> Optional[Tuple[pd.DataFrame, pd.DataFrame]]:
        """Fetch bid data from Archive directory (monthly ZIP files containing daily ZIPs)."""
        try:
            archive_url = f"{self.base_url}/Reports/Archive/Bidmove_Complete/"
            year_month = target_date.strftime("%Y%m")

            async with httpx.AsyncClient(timeout=180.0, headers={"User-Agent": "NEM-Dashboard/1.0"}) as client:
                # List the archive directory to find the monthly file
                response = await client.get(archive_url)
                response.raise_for_status()

                # Monthly archive files look like PUBLIC_BIDMOVE_COMPLETE_20250102.zip
                # where the date varies but contains the YYYYMM prefix
                pattern = rf'(PUBLIC_BIDMOVE_COMPLETE_{year_month}\d{{2}}\.zip)'
                matches = list(set(re.findall(pattern, response.text)))

                if not matches:
                    logger.debug(f"No Bidmove_Complete archive found for {year_month}")
                    return None

                archive_filename = sorted(matches)[-1]
                file_url = f"{archive_url}{archive_filename}"
                logger.info(f"Fetching bid data from Archive: {archive_filename}")

                file_response = await client.get(file_url)
                file_response.raise_for_status()

                # Archive is a monthly ZIP containing daily ZIPs
                return self._parse_bidmove_zip(file_response.content, target_date)

        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                logger.debug(f"Bid archive not found for {target_date}")
            else:
                logger.warning(f"HTTP error fetching bid archive for {target_date}: {e}")
            return None
        except Exception as e:
            logger.debug(f"Could not fetch bids from Archive for {target_date}: {e}")
            return None

    def _parse_bidmove_zip(self, zip_content: bytes, target_date) -> Optional[Tuple[pd.DataFrame, pd.DataFrame]]:
        """Parse a Bidmove_Complete ZIP file.

        Returns tuple of (biddayoffer_df, bidperoffer_df).
        """
        try:
            with zipfile.ZipFile(io.BytesIO(zip_content)) as zf:
                csv_files = [f for f in zf.namelist() if f.upper().endswith('.CSV')]

                if not csv_files:
                    # May be nested ZIP (archive format)
                    inner_zips = [f for f in zf.namelist() if f.upper().endswith('.ZIP')]
                    for inner_name in inner_zips:
                        date_str = target_date.strftime("%Y%m%d")
                        if date_str in inner_name:
                            inner_content = zf.read(inner_name)
                            return self._parse_bidmove_zip(inner_content, target_date)
                    logger.warning("No CSV files found in bid ZIP")
                    return None

                # Parse the CSV file(s)
                all_day_records = []
                all_per_records = []

                for csv_file in csv_files:
                    csv_content = zf.read(csv_file)
                    day_records, per_records = self._parse_bid_csv(csv_content, target_date)
                    all_day_records.extend(day_records)
                    all_per_records.extend(per_records)

                day_df = pd.DataFrame(all_day_records) if all_day_records else pd.DataFrame()
                per_df = pd.DataFrame(all_per_records) if all_per_records else pd.DataFrame()

                if not day_df.empty:
                    logger.info(f"Parsed {len(day_df)} BIDDAYOFFER records for {target_date}")
                if not per_df.empty:
                    logger.info(f"Parsed {len(per_df)} BIDPEROFFER records for {target_date}")

                if day_df.empty and per_df.empty:
                    return None

                return (day_df, per_df)

        except zipfile.BadZipFile:
            logger.error(f"Invalid ZIP file for bid data {target_date}")
            return None
        except Exception as e:
            logger.error(f"Error parsing bid ZIP for {target_date}: {e}")
            return None

    def _parse_bid_csv(self, csv_content: bytes, target_date) -> Tuple[list, list]:
        """Parse bid CSV content, extracting BIDDAYOFFER and BIDPEROFFER records.

        Uses I (info) header rows to dynamically determine column positions.
        Filters to ENERGY bid type only.
        """
        csv_text = csv_content.decode('utf-8', errors='replace')
        lines = csv_text.split('\n')

        # Column mappings discovered from I rows
        day_columns = {}
        per_columns = {}

        day_records = []
        per_records = []

        for line in lines:
            line = line.strip()
            if not line:
                continue

            parts = line.split(',')
            if len(parts) < 4:
                continue

            record_type = parts[0].strip('"')
            table_group = parts[1].strip('"') if len(parts) > 1 else ''
            sub_table = parts[2].strip('"') if len(parts) > 2 else ''

            # Parse header rows to get column positions
            if record_type == 'I' and table_group in ('BID', 'BIDS'):
                col_names = [p.strip('"').strip() for p in parts]
                if 'BIDDAYOFFER_D' in sub_table:
                    day_columns = {name: idx for idx, name in enumerate(col_names)}
                    logger.debug(f"BIDDAYOFFER header: {len(col_names)} columns")
                elif 'BIDPEROFFER_D' in sub_table:
                    per_columns = {name: idx for idx, name in enumerate(col_names)}
                    logger.debug(f"BIDPEROFFER header: {len(col_names)} columns")

            # Parse data rows
            elif record_type == 'D' and table_group in ('BID', 'BIDS'):
                if 'BIDDAYOFFER_D' in sub_table and day_columns:
                    record = self._parse_biddayoffer(parts, day_columns)
                    if record is not None:
                        day_records.append(record)
                elif 'BIDPEROFFER_D' in sub_table and per_columns:
                    record = self._parse_bidperoffer(parts, per_columns)
                    if record is not None:
                        per_records.append(record)

        logger.info(f"CSV parse result: {len(day_records)} day offers, {len(per_records)} per-interval offers")
        return day_records, per_records

    def _parse_biddayoffer(self, parts: list, columns: dict) -> Optional[dict]:
        """Parse a single BIDDAYOFFER_D data row."""
        try:
            # Filter to ENERGY only
            bidtype_idx = columns.get('BIDTYPE')
            if bidtype_idx is not None and bidtype_idx < len(parts):
                bidtype = parts[bidtype_idx].strip('"').strip()
                if bidtype != 'ENERGY':
                    return None

            duid = self._get_field(parts, columns, 'DUID')
            if not duid:
                return None

            settlementdate = self._get_field(parts, columns, 'SETTLEMENTDATE')
            offerdate = self._get_field(parts, columns, 'OFFERDATE')

            record = {
                'duid': duid,
                'settlementdate': pd.to_datetime(settlementdate) if settlementdate else None,
                'offerdate': pd.to_datetime(offerdate) if offerdate else None,
            }

            # Price bands 1-10
            for i in range(1, 11):
                record[f'priceband{i}'] = self._get_float(parts, columns, f'PRICEBAND{i}')

            record['minimumload'] = self._get_float(parts, columns, 'MINIMUMLOAD')

            # Ramp rates
            for key in ['T1', 'T2', 'T3', 'T4']:
                record[key.lower()] = self._get_float(parts, columns, key)

            if record['settlementdate'] is None:
                return None

            return record

        except Exception as e:
            logger.debug(f"Error parsing BIDDAYOFFER line: {e}")
            return None

    def _parse_bidperoffer(self, parts: list, columns: dict) -> Optional[dict]:
        """Parse a single BIDPEROFFER_D data row."""
        try:
            # Filter to ENERGY only
            bidtype_idx = columns.get('BIDTYPE')
            if bidtype_idx is not None and bidtype_idx < len(parts):
                bidtype = parts[bidtype_idx].strip('"').strip()
                if bidtype != 'ENERGY':
                    return None

            duid = self._get_field(parts, columns, 'DUID')
            if not duid:
                return None

            # Use INTERVAL_DATETIME if available, otherwise SETTLEMENTDATE
            interval_dt = self._get_field(parts, columns, 'INTERVAL_DATETIME')
            settlement_dt = self._get_field(parts, columns, 'SETTLEMENTDATE')
            timestamp = interval_dt or settlement_dt

            offerdate = self._get_field(parts, columns, 'OFFERDATE')

            record = {
                'duid': duid,
                'settlementdate': pd.to_datetime(timestamp) if timestamp else None,
                'offerdate': pd.to_datetime(offerdate) if offerdate else None,
            }

            # Band availability 1-10
            for i in range(1, 11):
                record[f'bandavail{i}'] = self._get_float(parts, columns, f'BANDAVAIL{i}')

            record['maxavail'] = self._get_float(parts, columns, 'MAXAVAIL')
            record['fixedload'] = self._get_float(parts, columns, 'FIXEDLOAD')
            record['rocup'] = self._get_float(parts, columns, 'ROCUP')
            record['rocdown'] = self._get_float(parts, columns, 'ROCDOWN')
            record['pasaavailability'] = self._get_float(parts, columns, 'PASAAVAILABILITY')

            if record['settlementdate'] is None:
                return None

            return record

        except Exception as e:
            logger.debug(f"Error parsing BIDPEROFFER line: {e}")
            return None

    def _get_field(self, parts: list, columns: dict, name: str) -> Optional[str]:
        """Get a string field from a parsed CSV line using column mapping."""
        idx = columns.get(name)
        if idx is not None and idx < len(parts):
            val = parts[idx].strip('"').strip()
            return val if val else None
        return None

    def _get_float(self, parts: list, columns: dict, name: str) -> Optional[float]:
        """Get a float field from a parsed CSV line using column mapping."""
        val = self._get_field(parts, columns, name)
        if val is not None:
            try:
                return float(val)
            except (ValueError, TypeError):
                pass
        return None
