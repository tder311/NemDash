"""
NEM Price and Interconnector Data Client
"""

import asyncio
import httpx
import pandas as pd
from datetime import datetime, timedelta
from typing import Optional
import logging
import zipfile
import io
import re

logger = logging.getLogger(__name__)

# NEM Region mapping
REGION_MAPPING = {
    '1': 'NSW',
    '2': 'VIC', 
    '3': 'QLD',
    '4': 'SA',
    '5': 'TAS',
    'NSW1': 'NSW',
    'VIC1': 'VIC',
    'QLD1': 'QLD',
    'SA1': 'SA',
    'TAS1': 'TAS'
}

class NEMPriceClient:
    def __init__(self, base_url: str = "https://www.nemweb.com.au"):
        self.base_url = base_url.rstrip('/')
    
    async def get_current_dispatch_prices(self) -> Optional[pd.DataFrame]:
        """Fetch current dispatch prices from NEMWEB DispatchIS_Reports"""
        try:
            dispatch_price_url = f"{self.base_url}/Reports/Current/DispatchIS_Reports/"
            
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.get(dispatch_price_url)
                response.raise_for_status()
                
                # Parse directory listing for latest dispatch price file
                latest_file = self._parse_latest_dispatch_price_file(response.text)
                if not latest_file:
                    logger.warning("No dispatch price file found")
                    return None
                
                # Download the dispatch price file
                file_url = f"{dispatch_price_url}{latest_file}"
                logger.info(f"Fetching dispatch price file: {latest_file}")
                file_response = await client.get(file_url)
                file_response.raise_for_status()
                
                return self._parse_dispatch_price_zip(file_response.content)
                
        except Exception as e:
            logger.error(f"Error fetching dispatch prices: {e}")
            return None
    
    async def get_trading_prices(self) -> Optional[pd.DataFrame]:
        """Fetch trading prices (30-minute intervals)"""
        try:
            trading_url = f"{self.base_url}/Reports/Current/TradingIS_Reports/"
            
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.get(trading_url)
                response.raise_for_status()
                
                # Parse directory listing for latest trading file
                latest_file = self._parse_latest_trading_file(response.text)
                if not latest_file:
                    logger.warning("No trading price file found")
                    return None
                
                # Download the trading file
                file_url = f"{trading_url}{latest_file}"
                logger.info(f"Fetching trading price file: {latest_file}")
                file_response = await client.get(file_url)
                file_response.raise_for_status()
                
                return self._parse_trading_price_zip(file_response.content)
                
        except Exception as e:
            logger.error(f"Error fetching trading prices: {e}")
            return None

    async def get_daily_prices(self, date: datetime) -> Optional[pd.DataFrame]:
        """Fetch daily price history from Public Prices.

        Tries Current directory first (last ~7-14 days), then falls back to
        Archive directory (monthly ZIP files) for older dates.

        NEMWEB PUBLIC_PRICES files use market day boundaries (04:05 to 04:00 next day).
        To get a complete calendar day, we need data from two market days.
        """
        try:
            target_date = date.date() if hasattr(date, 'date') else date

            # Try Current directory first (recent data)
            df = await self._get_daily_prices_from_current(target_date)
            if df is not None and not df.empty:
                return df

            # Fallback to Archive for older data
            df = await self._get_daily_prices_from_archive(target_date)
            if df is not None and not df.empty:
                return df

            logger.warning(f"No public prices found for {target_date} in Current or Archive")
            return None

        except Exception as e:
            logger.error(f"Error fetching daily prices for {date}: {e}")
            return None

    async def _get_daily_prices_from_current(self, target_date) -> Optional[pd.DataFrame]:
        """Fetch daily prices from Current directory (last ~7-14 days)."""
        try:
            prev_date = target_date - timedelta(days=1)
            public_prices_url = f"{self.base_url}/Reports/Current/Public_Prices/"

            all_dfs = []

            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.get(public_prices_url)
                response.raise_for_status()
                directory_html = response.text

                # Fetch both the previous day's file (for 00:00-04:00) and target day's file (for 04:05-23:55)
                for fetch_date in [prev_date, target_date]:
                    date_str = fetch_date.strftime("%Y%m%d")
                    pattern = f"PUBLIC_PRICES_{date_str}0000_\\d{{14}}\\.zip"
                    matches = re.findall(pattern, directory_html)

                    if not matches:
                        continue

                    latest_file = sorted(matches)[-1]  # Get latest version
                    file_url = f"{public_prices_url}{latest_file}"
                    logger.info(f"Fetching daily prices file: {latest_file}")

                    file_response = await client.get(file_url)
                    file_response.raise_for_status()

                    df = self._parse_public_prices_zip(file_response.content)
                    if df is not None and not df.empty:
                        all_dfs.append(df)

            if not all_dfs:
                return None

            return self._filter_to_target_date(all_dfs, target_date)

        except Exception as e:
            logger.debug(f"Could not fetch from Current for {target_date}: {e}")
            return None

    async def _get_daily_prices_from_archive(self, target_date) -> Optional[pd.DataFrame]:
        """Fetch daily prices from Archive (monthly ZIP files).

        Archive files are monthly ZIPs named PUBLIC_PRICES_YYYYMM01.zip
        containing nested daily ZIPs for each day.
        """
        try:
            prev_date = target_date - timedelta(days=1)
            archive_url = f"{self.base_url}/Reports/Archive/Public_Prices/"

            # Determine which monthly archives we need (might span two months)
            months_needed = set()
            for d in [prev_date, target_date]:
                month_key = d.strftime("%Y%m")
                months_needed.add(month_key)

            all_dfs = []

            async with httpx.AsyncClient(timeout=120.0) as client:
                for month_key in months_needed:
                    archive_filename = f"PUBLIC_PRICES_{month_key}01.zip"
                    file_url = f"{archive_url}{archive_filename}"

                    try:
                        logger.info(f"Fetching archive file: {archive_filename}")
                        response = await client.get(file_url)
                        response.raise_for_status()

                        # Archive is a monthly ZIP containing nested daily ZIPs
                        dfs = self._parse_archive_monthly_zip(response.content, target_date, prev_date)
                        all_dfs.extend(dfs)

                    except httpx.HTTPStatusError as e:
                        if e.response.status_code == 404:
                            logger.debug(f"Archive not found: {archive_filename}")
                        else:
                            logger.warning(f"Error fetching archive {archive_filename}: {e}")
                        continue

            if not all_dfs:
                return None

            return self._filter_to_target_date(all_dfs, target_date)

        except Exception as e:
            logger.debug(f"Could not fetch from Archive for {target_date}: {e}")
            return None

    def _parse_archive_monthly_zip(self, zip_content: bytes, target_date, prev_date) -> list:
        """Parse monthly archive ZIP containing nested daily ZIPs."""
        dfs = []
        try:
            with zipfile.ZipFile(io.BytesIO(zip_content)) as outer_zip:
                # Look for daily ZIPs matching our target dates
                for inner_name in outer_zip.namelist():
                    if not inner_name.endswith('.zip'):
                        continue

                    # Check if this inner ZIP is for one of our target dates
                    # Pattern: PUBLIC_PRICES_YYYYMMDD0000_*.zip
                    for check_date in [prev_date, target_date]:
                        date_str = check_date.strftime("%Y%m%d")
                        if f"PUBLIC_PRICES_{date_str}0000" in inner_name:
                            try:
                                inner_content = outer_zip.read(inner_name)
                                df = self._parse_public_prices_zip(inner_content)
                                if df is not None and not df.empty:
                                    dfs.append(df)
                            except Exception as e:
                                logger.debug(f"Error parsing {inner_name}: {e}")
                            break
        except Exception as e:
            logger.error(f"Error parsing archive monthly ZIP: {e}")

        return dfs

    def _filter_to_target_date(self, dfs: list, target_date) -> Optional[pd.DataFrame]:
        """Filter combined dataframes to target calendar day."""
        combined_df = pd.concat(dfs, ignore_index=True)

        # Filter to only the target calendar day
        combined_df['settlementdate'] = pd.to_datetime(combined_df['settlementdate'])
        target_date_str = target_date.strftime('%Y-%m-%d')
        filtered_df = combined_df[
            combined_df['settlementdate'].dt.date.astype(str) == target_date_str
        ].copy()

        # Remove duplicates (same timestamp from both files at 04:00 boundary)
        filtered_df = filtered_df.drop_duplicates(
            subset=['settlementdate', 'region'],
            keep='last'
        ).reset_index(drop=True)

        logger.info(f"Retrieved {len(filtered_df)} price records for {target_date}")
        return filtered_df if not filtered_df.empty else None

    async def get_monthly_archive_prices(self, year: int, month: int) -> Optional[pd.DataFrame]:
        """Fetch all PUBLIC prices for an entire month from Archive.

        Downloads the monthly archive once and extracts all daily data.
        Much more efficient than fetching day-by-day.

        Args:
            year: Year (e.g., 2025)
            month: Month (1-12)

        Returns:
            DataFrame with all price records for the month, or None if not found
        """
        try:
            archive_url = f"{self.base_url}/Reports/Archive/Public_Prices/"
            archive_filename = f"PUBLIC_PRICES_{year}{month:02d}01.zip"
            file_url = f"{archive_url}{archive_filename}"

            async with httpx.AsyncClient(timeout=120.0) as client:
                logger.info(f"Fetching monthly archive: {archive_filename}")
                response = await client.get(file_url)
                response.raise_for_status()

                # Parse all daily ZIPs from the monthly archive
                all_dfs = []
                with zipfile.ZipFile(io.BytesIO(response.content)) as outer_zip:
                    for inner_name in outer_zip.namelist():
                        if not inner_name.endswith('.zip'):
                            continue
                        try:
                            inner_content = outer_zip.read(inner_name)
                            df = self._parse_public_prices_zip(inner_content)
                            if df is not None and not df.empty:
                                all_dfs.append(df)
                        except Exception as e:
                            logger.debug(f"Error parsing {inner_name}: {e}")
                            continue

                if not all_dfs:
                    logger.warning(f"No price data found in archive {archive_filename}")
                    return None

                combined_df = pd.concat(all_dfs, ignore_index=True)
                combined_df['settlementdate'] = pd.to_datetime(combined_df['settlementdate'])

                # Remove duplicates
                combined_df = combined_df.drop_duplicates(
                    subset=['settlementdate', 'region'],
                    keep='last'
                ).reset_index(drop=True)

                logger.info(f"Retrieved {len(combined_df)} price records from {archive_filename}")
                return combined_df

        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                logger.debug(f"Archive not found: {archive_filename}")
            else:
                logger.warning(f"Error fetching archive {archive_filename}: {e}")
            return None
        except Exception as e:
            logger.error(f"Error fetching monthly archive {year}-{month:02d}: {e}")
            return None

    async def get_all_current_dispatch_prices(
        self,
        since: Optional[datetime] = None,
        request_delay: float = 0.05
    ) -> Optional[pd.DataFrame]:
        """Fetch dispatch price files from Current directory.

        Args:
            since: Only fetch files with timestamps after this datetime.
                   If None, fetches all files (~288 files, ~3 days).
            request_delay: Delay between requests in seconds (default 0.05s to avoid rate limiting)

        Returns:
            DataFrame with dispatch price data, or None if no files found/error
        """
        try:
            dispatch_url = f"{self.base_url}/Reports/Current/DispatchIS_Reports/"
            headers = {"User-Agent": "NEM-Dashboard/1.0"}

            async with httpx.AsyncClient(timeout=60.0, headers=headers) as client:
                # 1. Get directory listing
                response = await client.get(dispatch_url)
                response.raise_for_status()

                # 2. Find all dispatch price files
                pattern = r'PUBLIC_DISPATCHIS_(\d{12})_\d{16}\.zip'
                matches = re.findall(pattern, response.text)

                if not matches:
                    logger.warning("No dispatch price files found in Current directory")
                    return None

                # Build list of (filename, timestamp) tuples
                # Use a set to dedupe - HTML shows each filename twice (in href and link text)
                seen_files = set()
                all_files = []
                for match in re.finditer(r'(PUBLIC_DISPATCHIS_(\d{12})_\d{16}\.zip)', response.text):
                    filename = match.group(1)
                    if filename in seen_files:
                        continue
                    seen_files.add(filename)
                    timestamp_str = match.group(2)  # YYYYMMDDHHmm
                    try:
                        file_timestamp = datetime.strptime(timestamp_str, '%Y%m%d%H%M')
                        all_files.append((filename, file_timestamp))
                    except ValueError:
                        continue

                # 3. Filter files by timestamp if since is provided
                if since:
                    files_to_fetch = [
                        (f, t) for f, t in all_files if t > since
                    ]
                    logger.info(f"Dispatch prices: {len(all_files)} total files, {len(files_to_fetch)} newer than {since}")
                else:
                    files_to_fetch = all_files
                    logger.info(f"Fetching all {len(all_files)} dispatch price files sequentially")

                if not files_to_fetch:
                    logger.info("No new dispatch price files to fetch")
                    return None

                # Sort by timestamp (oldest first)
                files_to_fetch.sort(key=lambda x: x[1])

                # 4. Fetch files sequentially with small delay to avoid rate limiting
                all_dfs = []
                for i, (filename, _) in enumerate(files_to_fetch):
                    file_url = f"{dispatch_url}{filename}"
                    try:
                        file_response = await client.get(file_url)
                        file_response.raise_for_status()
                        df = self._parse_dispatch_price_zip(file_response.content)
                        if df is not None and not df.empty:
                            all_dfs.append(df)
                    except httpx.HTTPStatusError as e:
                        if e.response.status_code == 403:
                            logger.error(f"403 Forbidden fetching {filename}")
                        else:
                            logger.debug(f"HTTP error fetching {filename}: {e}")
                    except Exception as e:
                        logger.debug(f"Error fetching {filename}: {e}")

                    # Small delay between requests to avoid rate limiting
                    if i < len(files_to_fetch) - 1:
                        await asyncio.sleep(request_delay)

                    # Progress logging every 100 files
                    if (i + 1) % 100 == 0:
                        logger.info(f"Dispatch price backfill progress: {i + 1}/{len(files_to_fetch)} files")

                if not all_dfs:
                    logger.warning("No valid data found in any dispatch price files")
                    return None

                # 5. Concatenate all DataFrames
                combined_df = pd.concat(all_dfs, ignore_index=True)

                # 6. Deduplicate by (settlementdate, region) - keep last occurrence
                combined_df = combined_df.drop_duplicates(
                    subset=['settlementdate', 'region'],
                    keep='last'
                )

                logger.info(f"Successfully fetched {len(combined_df)} dispatch price records from {len(all_dfs)} files")
                return combined_df

        except Exception as e:
            logger.error(f"Error fetching all current dispatch prices: {e}")
            return None

    async def get_all_current_trading_prices(
        self,
        since: Optional[datetime] = None,
        request_delay: float = 0.05
    ) -> Optional[pd.DataFrame]:
        """Fetch trading price files from Current directory.

        Args:
            since: Only fetch files with timestamps after this datetime.
            request_delay: Delay between requests in seconds (default 0.05s to avoid rate limiting)

        Returns:
            DataFrame with trading price data, or None if no files found/error
        """
        try:
            trading_url = f"{self.base_url}/Reports/Current/TradingIS_Reports/"
            headers = {"User-Agent": "NEM-Dashboard/1.0"}

            async with httpx.AsyncClient(timeout=60.0, headers=headers) as client:
                response = await client.get(trading_url)
                response.raise_for_status()

                # Find all trading price files
                # Use a set to dedupe - HTML shows each filename twice (in href and link text)
                seen_files = set()
                all_files = []
                for match in re.finditer(r'(PUBLIC_TRADINGIS_(\d{12})_\d{16}\.zip)', response.text):
                    filename = match.group(1)
                    if filename in seen_files:
                        continue
                    seen_files.add(filename)
                    timestamp_str = match.group(2)
                    try:
                        file_timestamp = datetime.strptime(timestamp_str, '%Y%m%d%H%M')
                        all_files.append((filename, file_timestamp))
                    except ValueError:
                        continue

                if not all_files:
                    logger.warning("No trading price files found in Current directory")
                    return None

                # Filter by timestamp if provided
                if since:
                    files_to_fetch = [(f, t) for f, t in all_files if t > since]
                    logger.info(f"Trading prices: {len(all_files)} total files, {len(files_to_fetch)} newer than {since}")
                else:
                    files_to_fetch = all_files
                    logger.info(f"Fetching all {len(all_files)} trading price files sequentially")

                if not files_to_fetch:
                    logger.info("No new trading price files to fetch")
                    return None

                files_to_fetch.sort(key=lambda x: x[1])

                # Fetch files sequentially with small delay to avoid rate limiting
                all_dfs = []
                for i, (filename, _) in enumerate(files_to_fetch):
                    file_url = f"{trading_url}{filename}"
                    try:
                        file_response = await client.get(file_url)
                        file_response.raise_for_status()
                        df = self._parse_trading_price_zip(file_response.content)
                        if df is not None and not df.empty:
                            all_dfs.append(df)
                    except httpx.HTTPStatusError as e:
                        if e.response.status_code == 403:
                            logger.error(f"403 Forbidden fetching {filename}")
                        else:
                            logger.debug(f"HTTP error fetching {filename}: {e}")
                    except Exception as e:
                        logger.debug(f"Error fetching {filename}: {e}")

                    # Small delay between requests to avoid rate limiting
                    if i < len(files_to_fetch) - 1:
                        await asyncio.sleep(request_delay)

                    # Progress logging every 100 files
                    if (i + 1) % 100 == 0:
                        logger.info(f"Trading price backfill progress: {i + 1}/{len(files_to_fetch)} files")

                if not all_dfs:
                    logger.warning("No valid data found in any trading price files")
                    return None

                combined_df = pd.concat(all_dfs, ignore_index=True)
                combined_df = combined_df.drop_duplicates(
                    subset=['settlementdate', 'region'],
                    keep='last'
                )

                logger.info(f"Successfully fetched {len(combined_df)} trading price records from {len(all_dfs)} files")
                return combined_df

        except Exception as e:
            logger.error(f"Error fetching all current trading prices: {e}")
            return None

    def _parse_latest_dispatch_price_file(self, html_content: str) -> Optional[str]:
        """Parse directory listing for latest dispatch price file"""
        # Pattern for dispatch price files
        pattern = r'PUBLIC_DISPATCHIS_\d{12}_\d{16}\.zip'
        matches = re.findall(pattern, html_content)
        return sorted(matches)[-1] if matches else None
    
    def _parse_latest_trading_file(self, html_content: str) -> Optional[str]:
        """Parse directory listing for latest trading file"""
        # Pattern for trading files
        pattern = r'PUBLIC_TRADINGIS_\d{12}_\d{16}\.zip'
        matches = re.findall(pattern, html_content)
        return sorted(matches)[-1] if matches else None

    def _parse_dispatch_price_zip(self, zip_content: bytes) -> Optional[pd.DataFrame]:
        """Parse dispatch price ZIP file"""
        try:
            with zipfile.ZipFile(io.BytesIO(zip_content)) as zip_file:
                csv_files = [f for f in zip_file.namelist() if f.endswith('.CSV')]
                if not csv_files:
                    return None
                
                csv_content = zip_file.read(csv_files[0])
                return self._parse_price_csv(csv_content, 'DISPATCH')
                
        except Exception as e:
            logger.error(f"Error parsing dispatch price ZIP: {e}")
            return None
    
    def _parse_trading_price_zip(self, zip_content: bytes) -> Optional[pd.DataFrame]:
        """Parse trading price ZIP file"""
        try:
            with zipfile.ZipFile(io.BytesIO(zip_content)) as zip_file:
                csv_files = [f for f in zip_file.namelist() if f.endswith('.CSV')]
                if not csv_files:
                    return None
                
                csv_content = zip_file.read(csv_files[0])
                return self._parse_price_csv(csv_content, 'TRADING')
                
        except Exception as e:
            logger.error(f"Error parsing trading price ZIP: {e}")
            return None

    def _parse_public_prices_zip(self, zip_content: bytes) -> Optional[pd.DataFrame]:
        """Parse public prices ZIP file"""
        try:
            with zipfile.ZipFile(io.BytesIO(zip_content)) as zip_file:
                csv_files = [f for f in zip_file.namelist() if f.endswith('.CSV')]
                if not csv_files:
                    return None
                
                csv_content = zip_file.read(csv_files[0])
                return self._parse_price_csv(csv_content, 'PUBLIC')
                
        except Exception as e:
            logger.error(f"Error parsing public prices ZIP: {e}")
            return None
    
    def _parse_price_csv(self, csv_content: bytes, price_type: str) -> Optional[pd.DataFrame]:
        """Parse price CSV content - handles dispatch, trading, and public prices"""
        try:
            csv_text = csv_content.decode('utf-8')
            lines = csv_text.split('\n')

            # Look for price records - different types have different patterns
            price_lines = []
            regionsum_lines = []  # For demand data

            if price_type == 'DISPATCH':
                # For dispatch, look for PRICE records which contain actual RRP values
                pattern = 'D,DISPATCH,PRICE'
                regionsum_pattern = 'D,DISPATCH,REGIONSUM'
            elif price_type == 'TRADING':
                # Trading has actual price records with RRP
                pattern = 'D,TRADING,PRICE'
                regionsum_pattern = 'D,TRADING,REGIONSUM'
            else:  # PUBLIC - uses DREGION records
                pattern = 'D,DREGION,'
                regionsum_pattern = None

            for line in lines:
                if pattern in line:
                    price_lines.append(line)
                if regionsum_pattern and regionsum_pattern in line:
                    regionsum_lines.append(line)

            if not price_lines:
                logger.warning(f"No {price_type} price records found")
                return None

            # Parse REGIONSUM for demand data (DISPATCH and TRADING)
            demand_by_region = {}
            for line in regionsum_lines:
                parts = line.split(',')
                if len(parts) >= 10:
                    try:
                        region_id = parts[6].strip('"')
                        region = REGION_MAPPING.get(region_id, region_id)
                        demand = self._safe_float(parts[9])  # Column 9 is TOTALDEMAND
                        demand_by_region[region] = demand
                    except Exception as e:
                        logger.warning(f"Error parsing regionsum line: {e}")

            # Parse price data - format varies by type
            data = []

            if price_type == 'TRADING':
                # Trading price format: D,TRADING,PRICE,3,"2025/08/29 13:55:00",1,SA1,167,-98.93,0,0,"2025/08/29 13:50:12",-98.93,...
                # Columns: 0=D, 1=TRADING, 2=PRICE, 3=version, 4=settlementdate, 5=runno, 6=regionid, 7=periodid, 8=RRP, ...
                for line in price_lines:
                    parts = line.split(',')
                    if len(parts) >= 9:
                        try:
                            settlement_date = parts[4].strip('"')
                            region_id = parts[6].strip('"')  # Column 6 is REGIONID (NSW1, VIC1, etc.)
                            region = REGION_MAPPING.get(region_id, region_id)  # Map to display names
                            rrp_value = self._safe_float(parts[8])  # Column 8 is RRP (Regional Reference Price)

                            # Get demand from REGIONSUM if available
                            data.append({
                                'settlementdate': settlement_date,
                                'region': region,
                                'price': rrp_value,
                                'totaldemand': demand_by_region.get(region, 0.0),
                                'price_type': price_type
                            })

                        except Exception as e:
                            logger.warning(f"Error parsing trading price line: {e}")
                            continue

            elif price_type == 'DISPATCH':
                # Dispatch PRICE format varies by version:
                # Version 3: D,DISPATCH,PRICE,3,"date",runno,regionid,dispatchinterval,RRP,...
                # Version 5: D,DISPATCH,PRICE,5,"date",runno,regionid,dispatchinterval,INTERVENTION,RRP,...
                # We need to detect the version and adjust the RRP column index accordingly
                for line in price_lines:
                    parts = line.split(',')
                    if len(parts) >= 10:
                        try:
                            version = int(parts[3])
                            settlement_date = parts[4].strip('"')
                            region_id = parts[6].strip('"')  # Column 6 is REGIONID (NSW1, VIC1, etc.)
                            region = REGION_MAPPING.get(region_id, region_id)  # Map to display names

                            # Version 5 has INTERVENTION column at index 8, pushing RRP to index 9
                            # Version 3 and earlier have RRP at index 8
                            rrp_index = 9 if version >= 5 else 8
                            rrp_value = self._safe_float(parts[rrp_index])

                            # Get demand from REGIONSUM if available
                            data.append({
                                'settlementdate': settlement_date,
                                'region': region,
                                'price': rrp_value,
                                'totaldemand': demand_by_region.get(region, 0.0),
                                'price_type': price_type
                            })

                        except Exception as e:
                            logger.warning(f"Error parsing dispatch price line: {e}")
                            continue

            else:  # PUBLIC prices
                # Public price format: D,DREGION,,2,"2025/09/01 03:00:00",1,NSW1,0,107.84888,0,107.84888,0,0,7136.43,...
                # Columns: 0=D, 1=DREGION, 2=blank, 3=version, 4=settlementdate, 5=runno, 6=regionid, 7=intervention, 8=RRP, 9=EEP, 10=ROP, 11=APCFLAG, 12=MARKETSUSPENDEDFLAG, 13=TOTALDEMAND,...
                for line in price_lines:
                    parts = line.split(',')
                    if len(parts) >= 14:
                        try:
                            settlement_date = parts[4].strip('"')
                            region_id = parts[6].strip('"')
                            region = REGION_MAPPING.get(region_id, region_id)
                            rrp_value = self._safe_float(parts[8])  # Column 8 is RRP
                            demand_value = self._safe_float(parts[13])  # Column 13 is TOTALDEMAND
                            
                            data.append({
                                'settlementdate': settlement_date,
                                'region': region,
                                'price': rrp_value,
                                'totaldemand': demand_value,
                                'price_type': price_type
                            })
                        except Exception as e:
                            logger.warning(f"Error parsing public price line: {e}")
                            continue
            
            if data:
                df = pd.DataFrame(data)
                df['settlementdate'] = pd.to_datetime(df['settlementdate'])
                logger.info(f"Successfully parsed {len(df)} {price_type} price records")
                return df
                
        except Exception as e:
            logger.error(f"Error parsing {price_type} price CSV: {e}")

        return None

    def _safe_float(self, value: str) -> float:
        """Safely convert string to float"""
        try:
            return float(value.strip('"')) if value and value.strip() else 0.0
        except (ValueError, TypeError):
            return 0.0