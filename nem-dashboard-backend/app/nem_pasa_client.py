"""
NEM PASA (Projected Assessment of System Adequacy) Client

Fetches PDPASA and STPASA data from AEMO NEMWEB.
- PDPASA: Pre-Dispatch PASA (short-term, ~6 hours ahead)
- STPASA: Short Term PASA (medium-term, ~6 days ahead)
"""

import httpx
import pandas as pd
from typing import Optional
import logging
import zipfile
import io
import re
import csv

logger = logging.getLogger(__name__)


class NEMPASAClient:
    """Client for fetching PDPASA and STPASA data from NEMWEB."""

    def __init__(self, base_url: str = "https://www.nemweb.com.au"):
        self.base_url = base_url.rstrip('/')

    async def get_latest_pdpasa(self) -> Optional[pd.DataFrame]:
        """Fetch the latest PDPASA data from NEMWEB.

        Returns:
            DataFrame with PDPASA REGIONSOLUTION data, or None on error
        """
        try:
            pdpasa_url = f"{self.base_url}/Reports/Current/PDPASA/"

            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.get(pdpasa_url)
                response.raise_for_status()

                # Find all PDPASA files
                files = re.findall(r'PUBLIC_PDPASA_\d+_\d+\.zip', response.text)
                if not files:
                    logger.warning("No PDPASA files found")
                    return None

                # Get the most recent file
                latest_file = sorted(files)[-1]
                file_url = f"{pdpasa_url}{latest_file}"

                logger.info(f"Fetching PDPASA file: {latest_file}")
                file_response = await client.get(file_url)
                file_response.raise_for_status()

                return self._parse_pasa_zip(file_response.content, 'PDPASA')

        except Exception as e:
            logger.error(f"Error fetching PDPASA data: {e}")
            return None

    async def get_latest_stpasa(self) -> Optional[pd.DataFrame]:
        """Fetch the latest STPASA data from NEMWEB.

        Returns:
            DataFrame with STPASA REGIONSOLUTION data, or None on error
        """
        try:
            stpasa_url = f"{self.base_url}/Reports/Current/Short_Term_PASA_Reports/"

            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.get(stpasa_url)
                response.raise_for_status()

                # Find all STPASA files (pattern: PUBLIC_STPASA_YYYYMMDDHHMM_sequence.zip)
                files = re.findall(r'PUBLIC_STPASA_\d+_\d+\.zip', response.text)
                if not files:
                    logger.warning("No STPASA files found")
                    return None

                # Get the most recent file
                latest_file = sorted(files)[-1]
                file_url = f"{stpasa_url}{latest_file}"

                logger.info(f"Fetching STPASA file: {latest_file}")
                file_response = await client.get(file_url)
                file_response.raise_for_status()

                return self._parse_pasa_zip(file_response.content, 'STPASA')

        except Exception as e:
            logger.error(f"Error fetching STPASA data: {e}")
            return None

    def _parse_pasa_zip(self, content: bytes, pasa_type: str) -> Optional[pd.DataFrame]:
        """Parse PASA ZIP file and extract REGIONSOLUTION data.

        Args:
            content: ZIP file content
            pasa_type: 'PDPASA' or 'STPASA'

        Returns:
            DataFrame with regional solution data
        """
        try:
            with zipfile.ZipFile(io.BytesIO(content)) as z:
                csv_filename = z.namelist()[0]
                with z.open(csv_filename) as csv_file:
                    file_content = csv_file.read().decode('utf-8')

            lines = file_content.split('\n')

            # Parse REGIONSOLUTION table
            region_headers = None
            region_data = []
            current_table = None
            run_datetime = None

            for line in lines:
                # Detect REGIONSOLUTION header line
                if line.startswith(f'I,{pasa_type},REGIONSOLUTION'):
                    parts = line.split(',')
                    region_headers = parts[3:]  # Skip record type, table name, version
                    current_table = 'REGIONSOLUTION'
                    continue
                elif line.startswith('I,'):
                    current_table = None
                    continue

                # Parse REGIONSOLUTION data lines
                if line.startswith(f'D,{pasa_type},REGIONSOLUTION') and current_table == 'REGIONSOLUTION':
                    reader = csv.reader([line])
                    for row in reader:
                        region_data.append(row[3:])  # Skip record type, table name, version
                        # Extract run_datetime from first data row
                        if run_datetime is None and len(row) > 3:
                            run_datetime = row[3]  # RUN_DATETIME is typically first column

            if not region_headers or not region_data:
                logger.warning(f"Could not find {pasa_type} REGIONSOLUTION data")
                return None

            # Create DataFrame
            df = pd.DataFrame(region_data, columns=region_headers[:len(region_data[0])])

            # Standardize column names to lowercase
            df.columns = df.columns.str.lower().str.strip()

            # Convert datetime columns
            if 'interval_datetime' in df.columns:
                df['interval_datetime'] = pd.to_datetime(df['interval_datetime'])
            if 'run_datetime' in df.columns:
                df['run_datetime'] = pd.to_datetime(df['run_datetime'])

            # Convert numeric columns
            numeric_cols = [
                'demand10', 'demand50', 'demand90', 'reservereq', 'capacityreq',
                'unconstrainedcapacity', 'constrainedcapacity', 'surpluscapacity',
                'surplusreserve', 'lorcondition', 'aggregatecapacityavailable',
                'aggregatepasaavailability', 'calculatedlor1level', 'calculatedlor2level',
                'netinterchangeunderscarcity', 'totalintermittentgeneration'
            ]

            for col in numeric_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')

            # Remove duplicate rows (keep first occurrence)
            if 'interval_datetime' in df.columns and 'regionid' in df.columns:
                df = df.drop_duplicates(subset=['interval_datetime', 'regionid'], keep='first')
                df = df.sort_values('interval_datetime')

            logger.info(f"Parsed {len(df)} {pasa_type} records")
            return df

        except Exception as e:
            logger.error(f"Error parsing {pasa_type} ZIP: {e}")
            return None

    @staticmethod
    def get_lor_description(lor_level: int) -> str:
        """Get human-readable description of LOR level.

        Args:
            lor_level: LOR condition level (0-3)

        Returns:
            Description string
        """
        descriptions = {
            0: "No LOR",
            1: "LOR1 - Low Reserve Condition",
            2: "LOR2 - Lack of Reserve 2",
            3: "LOR3 - Lack of Reserve 3 (Load Shedding Imminent)"
        }
        return descriptions.get(int(lor_level), f"Unknown ({lor_level})")
