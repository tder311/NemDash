"""
NEM Price and Interconnector Data Client
"""

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
    
    async def get_interconnector_flows(self) -> Optional[pd.DataFrame]:
        """Fetch current interconnector flows from Dispatch IRSR"""
        try:
            irsr_url = f"{self.base_url}/Reports/Current/Dispatch_IRSR/"
            
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.get(irsr_url)
                response.raise_for_status()
                
                # Parse directory listing for latest IRSR file
                latest_file = self._parse_latest_irsr_file(response.text)
                if not latest_file:
                    logger.warning("No IRSR file found")
                    return None
                
                # Download the IRSR file
                file_url = f"{irsr_url}{latest_file}"
                logger.info(f"Fetching IRSR file: {latest_file}")
                file_response = await client.get(file_url)
                file_response.raise_for_status()
                
                return self._parse_irsr_zip(file_response.content)
                
        except Exception as e:
            logger.error(f"Error fetching interconnector flows: {e}")
            return None
    
    async def get_daily_prices(self, date: datetime) -> Optional[pd.DataFrame]:
        """Fetch daily price history from Public Prices"""
        try:
            date_str = date.strftime("%Y%m%d")
            public_prices_url = f"{self.base_url}/Reports/Current/Public_Prices/"
            
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.get(public_prices_url)
                response.raise_for_status()
                
                # Look for file matching the date
                pattern = f"PUBLIC_PRICES_{date_str}0000_\\d{{14}}\\.zip"
                matches = re.findall(pattern, response.text)
                
                if not matches:
                    logger.warning(f"No public prices file found for {date_str}")
                    return None
                
                latest_file = sorted(matches)[-1]  # Get latest version
                file_url = f"{public_prices_url}{latest_file}"
                logger.info(f"Fetching daily prices file: {latest_file}")
                
                file_response = await client.get(file_url)
                file_response.raise_for_status()
                
                return self._parse_public_prices_zip(file_response.content)
                
        except Exception as e:
            logger.error(f"Error fetching daily prices for {date}: {e}")
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
    
    def _parse_latest_irsr_file(self, html_content: str) -> Optional[str]:
        """Parse directory listing for latest IRSR file"""
        # Try multiple patterns as IRSR files may have different naming conventions
        patterns = [
            r'PUBLIC_IRSR_\d{12}_\d{16}\.zip',
            r'PUBLIC_DISPATCH_IRSR_\d{12}_\d{16}\.zip',
            r'PUBLIC_DISPATCHIRSR_\d{12}_\d{16}\.zip',
        ]
        for pattern in patterns:
            matches = re.findall(pattern, html_content)
            if matches:
                return sorted(matches)[-1]

        # Log available files for debugging if no match found
        all_zips = re.findall(r'PUBLIC_[A-Z_]+_\d{12}_\d{16}\.zip', html_content)
        if all_zips:
            logger.debug(f"Available ZIP files in IRSR directory: {set(f.split('_')[1] for f in all_zips[:5])}")

        return None
    
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
    
    def _parse_irsr_zip(self, zip_content: bytes) -> Optional[pd.DataFrame]:
        """Parse IRSR (interconnector) ZIP file"""
        try:
            with zipfile.ZipFile(io.BytesIO(zip_content)) as zip_file:
                csv_files = [f for f in zip_file.namelist() if f.endswith('.CSV')]
                if not csv_files:
                    return None
                
                csv_content = zip_file.read(csv_files[0])
                return self._parse_interconnector_csv(csv_content)
                
        except Exception as e:
            logger.error(f"Error parsing IRSR ZIP: {e}")
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
                # Dispatch PRICE format: D,DISPATCH,PRICE,3,"2025/08/29 13:55:00",1,NSW1,0,12.5,1,...
                # Columns: 0=D, 1=DISPATCH, 2=PRICE, 3=version, 4=settlementdate, 5=runno, 6=regionid, 7=dispatchinterval, 8=RRP, 9=EEP,...
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
    
    def _parse_interconnector_csv(self, csv_content: bytes) -> Optional[pd.DataFrame]:
        """Parse interconnector CSV content"""
        try:
            csv_text = csv_content.decode('utf-8')
            lines = csv_text.split('\n')
            
            # Look for interconnector flow records
            flow_lines = []
            for line in lines:
                if 'D,INTERCONNECTORRES,' in line:
                    flow_lines.append(line)
            
            if not flow_lines:
                logger.warning("No interconnector flow records found")
                return None
            
            # Parse interconnector data
            data = []
            for line in flow_lines:
                parts = line.split(',')
                if len(parts) >= 10:
                    try:
                        data.append({
                            'settlementdate': parts[4].strip('"'),
                            'interconnector': parts[5].strip('"'),
                            'meteredmwflow': self._safe_float(parts[6]),
                            'mwflow': self._safe_float(parts[7]),
                            'mwloss': self._safe_float(parts[8]),
                            'marginalvalue': self._safe_float(parts[9])
                        })
                    except Exception as e:
                        logger.warning(f"Error parsing interconnector line: {e}")
                        continue
            
            if data:
                df = pd.DataFrame(data)
                df['settlementdate'] = pd.to_datetime(df['settlementdate'])
                logger.info(f"Successfully parsed {len(df)} interconnector records")
                return df
                
        except Exception as e:
            logger.error(f"Error parsing interconnector CSV: {e}")
        
        return None
    
    def _safe_float(self, value: str) -> float:
        """Safely convert string to float"""
        try:
            return float(value.strip('"')) if value and value.strip() else 0.0
        except (ValueError, TypeError):
            return 0.0