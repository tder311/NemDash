import httpx
import pandas as pd
from datetime import datetime
from typing import Optional
import logging
import zipfile
import io

logger = logging.getLogger(__name__)

class NEMDispatchClient:
    def __init__(self, base_url: str = "https://www.nemweb.com.au"):
        self.base_url = base_url.rstrip('/')
    
    async def get_current_dispatch_data(self) -> Optional[pd.DataFrame]:
        """Fetch current dispatch data from NEM"""
        try:
            dispatch_url = f"{self.base_url}/REPORTS/CURRENT/Dispatch_SCADA/"
            
            async with httpx.AsyncClient(timeout=30.0) as client:
                # Get directory listing
                response = await client.get(dispatch_url)
                response.raise_for_status()
                
                # Parse the directory listing to find the latest dispatch file
                latest_file = self._parse_latest_dispatch_file(response.text)
                if not latest_file:
                    logger.warning("No dispatch file found")
                    return None
                
                # Download the actual dispatch file (ZIP)
                file_url = f"{dispatch_url}{latest_file}"
                logger.info(f"Fetching dispatch file: {latest_file}")
                file_response = await client.get(file_url)
                file_response.raise_for_status()
                
                return self._parse_dispatch_zip(file_response.content)
                
        except Exception as e:
            logger.error(f"Error fetching dispatch data: {e}")
            return None
    
    async def get_historical_dispatch_data(self, date: datetime) -> Optional[pd.DataFrame]:
        """Fetch historical dispatch data for a specific date"""
        try:
            date_str = date.strftime("%Y%m%d")
            archive_url = f"{self.base_url}/Reports/Archive/Dispatch_SCADA/{date.year}/DISPATCH_SCADA_{date_str}.zip"
            
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.get(archive_url)
                response.raise_for_status()
                
                # Extract and parse the CSV from the ZIP file
                with zipfile.ZipFile(io.BytesIO(response.content)) as zip_file:
                    csv_files = [f for f in zip_file.namelist() if f.endswith('.CSV')]
                    if not csv_files:
                        logger.warning(f"No CSV files found in archive for {date_str}")
                        return None
                    
                    # Read the first CSV file found
                    csv_content = zip_file.read(csv_files[0])
                    return self._parse_dispatch_csv(csv_content)
                    
        except Exception as e:
            logger.error(f"Error fetching historical dispatch data for {date}: {e}")
            return None
    
    def _parse_latest_dispatch_file(self, html_content: str) -> Optional[str]:
        """Parse the NEM directory listing to find the latest dispatch file"""
        import re
        
        # Look for dispatch ZIP files in the HTML - updated pattern for real NEM files
        pattern = r'PUBLIC_DISPATCHSCADA_\d{12}_\d{16}\.zip'
        matches = re.findall(pattern, html_content)
        
        if matches:
            # Return the latest file (they're timestamped)
            return sorted(matches)[-1]
        
        return None
    
    def _parse_dispatch_zip(self, zip_content: bytes) -> Optional[pd.DataFrame]:
        """Parse NEM dispatch ZIP file containing CSV data"""
        try:
            # Extract CSV from ZIP file
            with zipfile.ZipFile(io.BytesIO(zip_content)) as zip_file:
                csv_files = [f for f in zip_file.namelist() if f.endswith('.CSV')]
                if not csv_files:
                    logger.warning("No CSV files found in dispatch ZIP")
                    return None
                
                # Read the first CSV file found
                csv_content = zip_file.read(csv_files[0])
                logger.info(f"Found CSV file in ZIP: {csv_files[0]}")
                return self._parse_dispatch_csv(csv_content)
                
        except Exception as e:
            logger.error(f"Error parsing dispatch ZIP: {e}")
            return None

    def _parse_dispatch_csv(self, csv_content: bytes) -> Optional[pd.DataFrame]:
        """Parse NEM dispatch CSV content - updated for real NEM format"""
        try:
            # Convert bytes to string and handle the NEM CSV format
            csv_text = csv_content.decode('utf-8')
            
            # NEM CSV files have multiple record types, we want DISPATCH records
            lines = csv_text.split('\n')
            dispatch_lines = []
            
            # Look for DISPATCH,UNIT_SCADA records (the actual format)
            for line in lines:
                if line.startswith('D,DISPATCH,UNIT_SCADA'):
                    dispatch_lines.append(line)
            
            if not dispatch_lines:
                logger.warning("No DISPATCH,UNIT_SCADA records found in CSV")
                # Let's also check what records are actually present
                sample_lines = [line for line in lines[:10] if line.strip()]
                logger.info(f"Sample CSV lines: {sample_lines}")
                return None
            
            logger.info(f"Found {len(dispatch_lines)} dispatch records")
            
            # Parse the dispatch data according to actual NEM format
            # Format: D,DISPATCH,UNIT_SCADA,1,"SETTLEMENTDATE",DUID,SCADAVALUE,"LASTCHANGED"
            data = []
            for line in dispatch_lines:
                parts = line.split(',')
                if len(parts) >= 8:  # Ensure we have enough columns for actual format
                    try:
                        # Clean quoted values
                        settlement_date = parts[4].strip('"')
                        duid = parts[5].strip()
                        scada_value = self._safe_float(parts[6])
                        last_changed = parts[7].strip('"') if len(parts) > 7 else ""
                        
                        data.append({
                            'settlementdate': settlement_date,
                            'duid': duid,
                            'scadavalue': scada_value,
                            # Set other fields to default values since they're not in this data format
                            'uigf': 0.0,
                            'totalcleared': 0.0,
                            'ramprate': 0.0,
                            'availability': 0.0,
                            'raise1sec': 0.0,
                            'lower1sec': 0.0
                        })
                    except Exception as e:
                        logger.warning(f"Error parsing line: {line[:100]}... - {e}")
                        continue
            
            if data:
                df = pd.DataFrame(data)
                df['settlementdate'] = pd.to_datetime(df['settlementdate'])
                logger.info(f"Successfully parsed {len(df)} dispatch records")
                return df
            
        except Exception as e:
            logger.error(f"Error parsing dispatch CSV: {e}")
        
        return None
    
    def _safe_float(self, value: str) -> float:
        """Safely convert string to float, returning 0.0 for empty/invalid values"""
        try:
            return float(value) if value and value.strip() else 0.0
        except (ValueError, TypeError):
            return 0.0
    
