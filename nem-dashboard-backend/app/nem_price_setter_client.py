"""
NEM Price Setter Data Client

Downloads and parses NemPriceSetter XML files from the NEMDE archive
to identify which generators set the regional reference price at each
5-minute dispatch interval.
"""

import httpx
import pandas as pd
from datetime import datetime
from typing import Optional, List, Dict
import logging
import zipfile
import io
from xml.etree import ElementTree as ET

logger = logging.getLogger(__name__)

# Minimum absolute value of the Increase coefficient to consider a PriceSetting
# record a genuine price setter (vs a constraint artifact). Records with
# abs(Increase) below this threshold are stored but excluded from metrics.
INCREASE_THRESHOLD = 0.01

REGION_MAPPING = {
    'NSW1': 'NSW',
    'VIC1': 'VIC',
    'QLD1': 'QLD',
    'SA1': 'SA',
    'TAS1': 'TAS',
}


class NEMPriceSetterClient:
    """Client for fetching NemPriceSetter data from NEMDE archive."""

    def __init__(self, base_url: str = "https://www.nemweb.com.au"):
        self.base_url = base_url.rstrip('/')

    async def get_daily_price_setter(self, date: datetime) -> Optional[pd.DataFrame]:
        """Fetch NemPriceSetter XML data for a single date.

        Downloads the daily ZIP from the NEMDE archive, parses all 288 XML
        files inside, and returns Energy market price setter records.

        Args:
            date: The date to fetch data for.

        Returns:
            DataFrame with columns: period_id, region, price, duid, increase, band_price, band_no
            or None if data is unavailable.
        """
        try:
            target_date = date.date() if hasattr(date, 'date') else date
            year = target_date.year
            month = f"{target_date.month:02d}"
            date_str = target_date.strftime("%Y%m%d")

            url = (
                f"{self.base_url}/Data_Archive/Wholesale_Electricity/NEMDE/"
                f"{year}/NEMDE_{year}_{month}/NEMDE_Market_Data/NEMDE_Files/"
                f"NemPriceSetter_{date_str}_xml.zip"
            )

            async with httpx.AsyncClient(timeout=60.0) as client:
                response = await client.get(url)
                if response.status_code == 404:
                    logger.warning(f"No price setter data for {date_str}")
                    return None
                response.raise_for_status()

                return self._parse_price_setter_zip(response.content)

        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                logger.warning(f"Price setter archive not found for {date_str}")
            else:
                logger.error(f"HTTP error fetching price setter for {date}: {e}")
            return None
        except Exception as e:
            logger.error(f"Error fetching price setter data for {date}: {e}")
            return None

    def _parse_price_setter_zip(self, zip_content: bytes) -> Optional[pd.DataFrame]:
        """Parse a daily NemPriceSetter ZIP containing 288 XML files."""
        try:
            all_records = []

            with zipfile.ZipFile(io.BytesIO(zip_content)) as zf:
                xml_files = [f for f in zf.namelist() if f.endswith('.xml')]

                if not xml_files:
                    logger.warning("No XML files found in price setter ZIP")
                    return None

                for xml_name in xml_files:
                    try:
                        xml_content = zf.read(xml_name)
                        records = self._parse_price_setter_xml(xml_content)
                        all_records.extend(records)
                    except Exception as e:
                        logger.debug(f"Error parsing {xml_name}: {e}")
                        continue

            if not all_records:
                logger.warning("No price setter records extracted")
                return None

            df = pd.DataFrame(all_records)
            df['period_id'] = pd.to_datetime(df['period_id'], utc=True)
            # Convert to naive AEST (UTC+10) to match existing NEM data handling
            df['period_id'] = df['period_id'].dt.tz_convert('Australia/Brisbane').dt.tz_localize(None)

            # Deduplicate: keep one record per (period_id, region, duid)
            df = df.drop_duplicates(subset=['period_id', 'region', 'duid'], keep='first')

            logger.info(f"Parsed {len(df)} price setter records from {len(xml_files)} intervals")
            return df

        except Exception as e:
            logger.error(f"Error parsing price setter ZIP: {e}")
            return None

    def _parse_price_setter_xml(self, xml_content: bytes) -> List[Dict]:
        """Parse a single 5-min interval XML file.

        Extracts Energy market price setter records where DispatchedMarket
        is 'ENOF' (energy offer), which identifies the generators whose
        bids directly determined the regional reference price.
        """
        records = []

        root = ET.fromstring(xml_content)

        for ps in root.iter('PriceSetting'):
            market = ps.get('Market')
            dispatched_market = ps.get('DispatchedMarket')

            # Only energy market, only direct energy offer price setters
            if market != 'Energy' or dispatched_market != 'ENOF':
                continue

            region_id = ps.get('RegionID')
            region = REGION_MAPPING.get(region_id)
            if not region:
                continue

            # Extract DUID from Unit field
            # Single DUID: "GSTONE5"
            # Multi-unit: "GSTONE5,ENOF,2,GSTONE6,ENOF,2" (triplets)
            # Interconnector: "T-V-MNSP1,TAS1"
            unit_str = ps.get('Unit', '')
            duid = unit_str.split(',')[0].strip()
            if not duid:
                continue

            try:
                price = float(ps.get('Price', 0))
            except (ValueError, TypeError):
                continue

            try:
                increase = float(ps.get('Increase', 0))
            except (ValueError, TypeError):
                increase = 0.0

            try:
                band_price = float(ps.get('RRNBandPrice', 0))
            except (ValueError, TypeError):
                band_price = None

            try:
                band_no = int(ps.get('BandNo', 0))
            except (ValueError, TypeError):
                band_no = None

            records.append({
                'period_id': ps.get('PeriodID'),
                'region': region,
                'price': price,
                'duid': duid,
                'increase': increase,
                'band_price': band_price,
                'band_no': band_no,
            })

        return records
