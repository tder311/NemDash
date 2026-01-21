import asyncio
import logging
import os
from datetime import datetime, timedelta
from typing import Optional
from pathlib import Path

import pandas as pd

from .nem_client import NEMDispatchClient
from .nem_price_client import NEMPriceClient
from .database import NEMDatabase

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataIngester:
    def __init__(self, db_url: str, nem_base_url: str = "https://www.nemweb.com.au"):
        """Initialize the data ingester.

        Args:
            db_url: PostgreSQL database URL (e.g., 'postgresql://user:pass@localhost:5432/nem_dashboard')
            nem_base_url: Base URL for NEMWEB API
        """
        self.db = NEMDatabase(db_url)
        self.nem_client = NEMDispatchClient(nem_base_url)
        self.price_client = NEMPriceClient(nem_base_url)
        self.is_running = False
        
    async def initialize(self):
        """Initialize the database"""
        await self.db.initialize()
        logger.info("Database initialized")
    
    async def ingest_current_data(self) -> bool:
        """Fetch and ingest current dispatch data, prices, and interconnector flows"""
        success = True
        
        try:
            # Fetch dispatch data
            logger.info("Fetching current dispatch data...")
            dispatch_df = await self.nem_client.get_current_dispatch_data()
            
            if dispatch_df is not None and not dispatch_df.empty:
                records_inserted = await self.db.insert_dispatch_data(dispatch_df)
                logger.info(f"Inserted {records_inserted} dispatch records")
            else:
                logger.warning("No current dispatch data available")
                success = False
            
            # Fetch dispatch prices
            logger.info("Fetching current dispatch prices...")
            price_df = await self.price_client.get_current_dispatch_prices()
            
            if price_df is not None and not price_df.empty:
                price_records = await self.db.insert_price_data(price_df)
                logger.info(f"Inserted {price_records} dispatch price records")
            else:
                logger.warning("No current dispatch price data available")
            
            # Fetch trading prices
            logger.info("Fetching current trading prices...")
            trading_df = await self.price_client.get_trading_prices()
            
            if trading_df is not None and not trading_df.empty:
                trading_records = await self.db.insert_price_data(trading_df)
                logger.info(f"Inserted {trading_records} trading price records")
            else:
                logger.warning("No current trading price data available")
            
            # Fetch today's and yesterday's public prices (complete historical data)
            today = datetime.now()
            yesterday = today - timedelta(days=1)
            
            # Try to fetch yesterday's public prices (for early morning hours)
            logger.info("Fetching yesterday's public prices for complete data...")
            yesterday_df = await self.price_client.get_daily_prices(yesterday)
            
            if yesterday_df is not None and not yesterday_df.empty:
                yesterday_records = await self.db.insert_price_data(yesterday_df)
                logger.info(f"Inserted {yesterday_records} public price records for yesterday")
            else:
                logger.warning("No public price data available for yesterday")
            
            # Fetch today's public prices
            logger.info("Fetching today's public prices...")
            public_df = await self.price_client.get_daily_prices(today)
            
            if public_df is not None and not public_df.empty:
                public_records = await self.db.insert_price_data(public_df)
                logger.info(f"Inserted {public_records} public price records for today")
            else:
                logger.warning("No public price data available for today")
            
            # Fetch interconnector flows
            logger.info("Fetching current interconnector flows...")
            interconnector_df = await self.price_client.get_interconnector_flows()
            
            if interconnector_df is not None and not interconnector_df.empty:
                interconnector_records = await self.db.insert_interconnector_data(interconnector_df)
                logger.info(f"Inserted {interconnector_records} interconnector flow records")
            else:
                logger.warning("No current interconnector flow data available")
            
            return success
                
        except Exception as e:
            logger.error(f"Error ingesting current data: {e}")
            return False
    
    async def ingest_historical_data(self, start_date: datetime, end_date: Optional[datetime] = None) -> int:
        """Fetch and ingest historical dispatch data for a date range"""
        if end_date is None:
            end_date = start_date
            
        total_records = 0
        current_date = start_date
        
        while current_date <= end_date:
            try:
                logger.info(f"Fetching historical data for {current_date.strftime('%Y-%m-%d')}")
                df = await self.nem_client.get_historical_dispatch_data(current_date)
                
                if df is not None and not df.empty:
                    records_inserted = await self.db.insert_dispatch_data(df)
                    total_records += records_inserted
                    logger.info(f"Inserted {records_inserted} records for {current_date.strftime('%Y-%m-%d')}")
                else:
                    logger.warning(f"No data available for {current_date.strftime('%Y-%m-%d')}")
                
                # Small delay to avoid overwhelming the API
                await asyncio.sleep(1)
                
            except Exception as e:
                logger.error(f"Error ingesting data for {current_date}: {e}")
            
            current_date += timedelta(days=1)
        
        logger.info(f"Historical ingestion complete. Total records: {total_records}")
        return total_records
    
    async def ingest_historical_prices(self, start_date: datetime, end_date: Optional[datetime] = None) -> int:
        """Fetch and ingest historical price data (PUBLIC_PRICES) for a date range"""
        if end_date is None:
            end_date = start_date

        total_records = 0
        current_date = start_date

        while current_date <= end_date:
            try:
                logger.info(f"Fetching historical prices for {current_date.strftime('%Y-%m-%d')}")
                price_df = await self.price_client.get_daily_prices(current_date)

                if price_df is not None and not price_df.empty:
                    records_inserted = await self.db.insert_price_data(price_df)
                    total_records += records_inserted
                    logger.info(f"Inserted {records_inserted} price records for {current_date.strftime('%Y-%m-%d')}")
                else:
                    logger.warning(f"No price data available for {current_date.strftime('%Y-%m-%d')}")

                # Small delay to avoid overwhelming the API
                await asyncio.sleep(1)

            except Exception as e:
                logger.error(f"Error ingesting price data for {current_date}: {e}")

            current_date += timedelta(days=1)

        logger.info(f"Historical price ingestion complete. Total records: {total_records}")
        return total_records

    async def backfill_missing_data(self, days_back: int = 30, max_gaps_per_run: int = 10) -> int:
        """Automatically backfill missing historical price data on startup"""
        logger.info(f"Starting automatic backfill check for last {days_back} days...")

        try:
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days_back)

            # Find missing dates
            missing_dates = await self.db.get_missing_dates(start_date, end_date, price_type='PUBLIC')

            if not missing_dates:
                logger.info("No missing dates found - data is complete")
                return 0

            logger.info(f"Found {len(missing_dates)} missing dates in the last {days_back} days")

            # Limit the number of gaps to fill per run to avoid long startup times
            dates_to_fill = missing_dates[:max_gaps_per_run]
            if len(missing_dates) > max_gaps_per_run:
                logger.info(f"Limiting backfill to {max_gaps_per_run} dates per run")

            total_records = 0
            for date in dates_to_fill:
                try:
                    logger.info(f"Backfilling prices for {date.strftime('%Y-%m-%d')}")
                    price_df = await self.price_client.get_daily_prices(date)

                    if price_df is not None and not price_df.empty:
                        records = await self.db.insert_price_data(price_df)
                        total_records += records
                        logger.info(f"Backfilled {records} price records for {date.strftime('%Y-%m-%d')}")

                    # Delay between requests to respect NEMWEB rate limits
                    await asyncio.sleep(1)

                except Exception as e:
                    logger.error(f"Error backfilling {date.strftime('%Y-%m-%d')}: {e}")
                    continue

            logger.info(f"Backfill complete. Total records added: {total_records}")
            return total_records

        except Exception as e:
            logger.error(f"Error during backfill: {e}")
            return 0

    async def backfill_dispatch_prices(self) -> int:
        """Backfill DISPATCH prices from Current directory since last PUBLIC price.

        Only fetches files newer than the latest PUBLIC price timestamp,
        reducing the number of files from ~288 (3 days) to typically ~100-200
        (since 4am today). Uses concurrent requests for faster fetching.
        """
        logger.info("Starting DISPATCH price backfill from Current directory...")

        try:
            # Get the latest PUBLIC price timestamp to avoid fetching unnecessary data
            latest_public = await self.db.get_latest_price_timestamp('PUBLIC')

            if latest_public:
                logger.info(f"Latest PUBLIC price: {latest_public}, fetching DISPATCH since then")
            else:
                logger.info("No PUBLIC prices found, fetching all available DISPATCH data")

            # Fetch only files newer than latest PUBLIC timestamp
            df = await self.price_client.get_all_current_dispatch_prices(since=latest_public)

            if df is not None and not df.empty:
                records = await self.db.insert_price_data(df)
                logger.info(f"Backfilled {records} DISPATCH price records from Current directory")
                return records
            else:
                logger.info("No new DISPATCH price data to backfill")
                return 0

        except Exception as e:
            logger.error(f"Error during DISPATCH price backfill: {e}")
            return 0

    async def backfill_dispatch_data(self, days_back: int = 7) -> int:
        """Backfill dispatch SCADA data for generation history charts.

        Fetches dispatch data from:
        1. NEMWEB Current directory (~3 days rolling window) for recent data
        2. NEMWEB Archive for older historical data (up to days_back)

        This enables historical generation by fuel source charts for up to 7 days.
        """
        total_records = 0

        try:
            # Step 1: Fetch recent data from Current directory (~3 days)
            logger.info("Fetching dispatch data from Current directory...")
            df = await self.nem_client.get_all_current_dispatch_data(since=None)

            if df is not None and not df.empty:
                records = await self.db.insert_dispatch_data(df)
                total_records += records
                logger.info(f"Backfilled {records} dispatch records from Current directory")

            # Step 2: Backfill older data from historical archives
            # Archives are available with ~2 day delay, so we can fill gaps older than ~3 days
            logger.info(f"Checking for missing dispatch data in last {days_back} days...")

            end_date = datetime.now()
            start_date = end_date - timedelta(days=days_back)

            # Find dates that need backfilling from archives
            missing_dates = await self._get_missing_dispatch_dates(start_date, end_date)

            if missing_dates:
                logger.info(f"Found {len(missing_dates)} dates to backfill from archives")

                for date in missing_dates:
                    try:
                        logger.info(f"Backfilling dispatch data for {date.strftime('%Y-%m-%d')} from archive...")
                        archive_df = await self.nem_client.get_historical_dispatch_data(date)

                        if archive_df is not None and not archive_df.empty:
                            records = await self.db.insert_dispatch_data(archive_df)
                            total_records += records
                            logger.info(f"Backfilled {records} dispatch records for {date.strftime('%Y-%m-%d')}")

                        # Delay between requests to respect NEMWEB rate limits
                        await asyncio.sleep(1)

                    except Exception as e:
                        logger.warning(f"Could not backfill dispatch for {date.strftime('%Y-%m-%d')}: {e}")
                        continue
            else:
                logger.info("No missing dispatch dates found - data is complete")

            logger.info(f"Dispatch backfill complete. Total records added: {total_records}")
            return total_records

        except Exception as e:
            logger.error(f"Error during dispatch data backfill: {e}")
            return total_records

    async def _get_missing_dispatch_dates(self, start_date: datetime, end_date: datetime) -> list:
        """Find dates with no or incomplete dispatch data in the specified range.

        Archives have ~2 day delay, so we only check dates at least 2 days old.
        """
        # Archives are posted with ~2 day delay
        archive_cutoff = datetime.now() - timedelta(days=2)

        # Only check dates that would be available in archives
        check_end = min(end_date, archive_cutoff)

        if start_date >= check_end:
            return []

        # Get dates that have sufficient dispatch data
        existing_dates = await self.db.get_dispatch_dates_with_data(start_date, check_end)

        # Generate all dates in range and find missing ones
        missing = []
        current = start_date.replace(hour=0, minute=0, second=0, microsecond=0)

        while current <= check_end:
            date_str = current.strftime('%Y-%m-%d')
            if date_str not in existing_dates:
                missing.append(current)
            current += timedelta(days=1)

        return missing

    async def run_continuous_ingestion(self, interval_minutes: int = 5):
        """Run continuous data ingestion"""
        self.is_running = True
        logger.info(f"Starting continuous ingestion with {interval_minutes} minute intervals")

        # Backfill missing historical PUBLIC price data on startup
        backfill_days = int(os.getenv('BACKFILL_DAYS_ON_STARTUP', '30'))
        await self.backfill_missing_data(days_back=backfill_days)

        # Backfill DISPATCH prices from Current directory (~3 days)
        # This bridges the gap between 4am (when PUBLIC prices end) and now
        await self.backfill_dispatch_prices()

        # Backfill dispatch SCADA data for generation history charts
        dispatch_backfill_days = int(os.getenv('DISPATCH_BACKFILL_DAYS', '7'))
        await self.backfill_dispatch_data(days_back=dispatch_backfill_days)

        # Initial data fetch
        await self.ingest_current_data()
        
        while self.is_running:
            try:
                await asyncio.sleep(interval_minutes * 60)
                if self.is_running:
                    await self.ingest_current_data()
            except Exception as e:
                logger.error(f"Error in continuous ingestion: {e}")
                await asyncio.sleep(60)  # Wait 1 minute before retrying
    
    def stop_continuous_ingestion(self):
        """Stop continuous data ingestion"""
        self.is_running = False
        logger.info("Stopping continuous ingestion")
    
    async def get_data_summary(self):
        """Get summary of ingested data"""
        return await self.db.get_data_summary()
    
    async def cleanup(self):
        """Clean up resources"""
        logger.info("Data ingester cleaned up")

# Sample generator information for common NEM units
SAMPLE_GENERATOR_INFO = [
    {"duid": "ADPCC1", "station_name": "Adelaide Desalination Plant", "region": "SA", "fuel_source": "Solar", "technology_type": "Solar PV", "capacity_mw": 1.2},
    {"duid": "AGLHAL", "station_name": "Hallett Wind Farm", "region": "SA", "fuel_source": "Wind", "technology_type": "Wind", "capacity_mw": 94.5},
    {"duid": "AGLSOM", "station_name": "AGL Somerton", "region": "VIC", "fuel_source": "Gas", "technology_type": "Gas Turbine", "capacity_mw": 160},
    {"duid": "ANGASG1", "station_name": "Angaston Gas", "region": "SA", "fuel_source": "Gas", "technology_type": "Gas Turbine", "capacity_mw": 50},
    {"duid": "APD01", "station_name": "Port Stanvac", "region": "SA", "fuel_source": "Diesel", "technology_type": "Reciprocating Engine", "capacity_mw": 56},
    {"duid": "ARWF1", "station_name": "Ararat Wind Farm", "region": "VIC", "fuel_source": "Wind", "technology_type": "Wind", "capacity_mw": 240},
    {"duid": "BALBG1", "station_name": "Ballarat Base Hospital", "region": "VIC", "fuel_source": "Gas", "technology_type": "Gas Turbine", "capacity_mw": 1.0},
    {"duid": "BARRON1", "station_name": "Barron Gorge", "region": "QLD", "fuel_source": "Hydro", "technology_type": "Hydro", "capacity_mw": 66},
    {"duid": "BASTYAN", "station_name": "Bastyan", "region": "TAS", "fuel_source": "Hydro", "technology_type": "Hydro", "capacity_mw": 82},
    {"duid": "BBTHREE1", "station_name": "BB1 Unit 1", "region": "NSW", "fuel_source": "Coal", "technology_type": "Steam Turbine", "capacity_mw": 350}
]

async def update_sample_generator_info(db: NEMDatabase):
    """Update database with sample generator information"""
    await db.update_generator_info(SAMPLE_GENERATOR_INFO)
    logger.info(f"Updated {len(SAMPLE_GENERATOR_INFO)} generator info records")


async def import_generator_info_from_csv(db: NEMDatabase, csv_path: str = None):
    """
    Import generator info from GenInfo.csv if available.
    Falls back to sample generator info if CSV not found.
    """
    # Try to find GenInfo.csv in common locations
    if csv_path is None:
        possible_paths = [
            Path(__file__).parent.parent / 'data' / 'GenInfo.csv',
            Path('./data/GenInfo.csv'),
        ]
        for path in possible_paths:
            if path.exists():
                csv_path = str(path)
                break

    if csv_path is None or not Path(csv_path).exists():
        logger.warning("GenInfo.csv not found, using sample generator info only")
        await update_sample_generator_info(db)
        return

    logger.info(f"Importing generator info from {csv_path}")

    try:
        # Read the CSV file
        df = pd.read_csv(csv_path, encoding='utf-8-sig')  # Handle BOM
        df.columns = df.columns.str.strip()  # Clean column names

        # Filter for existing plants with DUIDs
        df_valid = df[
            (df['DUID'].notna()) &
            (df['DUID'] != '') &
            (df['Asset Type'].str.contains('Existing', na=False))
        ].copy()

        logger.info(f"Found {len(df_valid)} existing generators with DUIDs in CSV")

        # Process generators
        generators = []
        for _, row in df_valid.iterrows():
            duid = str(row['DUID']).strip()
            if not duid or duid == 'nan':
                continue

            # Map region (remove the '1' suffix)
            region = str(row['Region']).replace('1', '').strip()

            # Clean fuel type mapping
            fuel_type_raw = str(row['Fuel Type']).strip()
            if 'Solar' in fuel_type_raw:
                fuel_source = 'Solar'
            elif 'Wind' in fuel_type_raw:
                fuel_source = 'Wind'
            elif 'Water' in fuel_type_raw or 'Hydro' in fuel_type_raw:
                fuel_source = 'Hydro'
            elif 'Gas' in fuel_type_raw or 'Coal Mine Gas' in fuel_type_raw:
                fuel_source = 'Gas'
            elif 'Coal' in fuel_type_raw:
                fuel_source = 'Coal'
            elif 'Other' in fuel_type_raw and 'Battery' in str(row.get('Technology Type', '')):
                fuel_source = 'Battery'
            elif 'Diesel' in fuel_type_raw:
                fuel_source = 'Diesel'
            else:
                fuel_source = 'Other'

            # Clean technology type
            tech_type_raw = str(row.get('Technology Type', '')).strip()
            if 'Solar PV' in tech_type_raw:
                technology_type = 'Solar PV'
            elif 'Wind Turbine' in tech_type_raw:
                technology_type = 'Wind'
            elif 'Storage - Battery' in tech_type_raw:
                technology_type = 'Battery Storage'
            elif 'Hydro' in tech_type_raw:
                technology_type = 'Hydro'
            elif 'Gas Turbine' in tech_type_raw:
                technology_type = 'Gas Turbine'
            elif 'Steam Turbine' in tech_type_raw:
                if fuel_source == 'Coal':
                    technology_type = 'Coal Steam'
                else:
                    technology_type = 'Gas Steam'
            elif 'Reciprocating Engine' in tech_type_raw:
                technology_type = 'Reciprocating Engine'
            else:
                technology_type = tech_type_raw if tech_type_raw else 'Unknown'

            # Get capacity (try different columns)
            capacity = 0.0
            for cap_col in ['Nameplate Capacity (MW)', 'Aggregated Upper Nameplate Capacity (MW)', 'Upper Nameplate Capacity (MW)']:
                if cap_col in row and pd.notna(row[cap_col]):
                    try:
                        cap_str = str(row[cap_col]).strip().replace(' - ', '-')
                        if '-' in cap_str:
                            # Handle range like "200.00 - 400.00"
                            cap_parts = cap_str.split('-')
                            capacity = float(cap_parts[-1].strip())
                        else:
                            capacity = float(cap_str)
                        break
                    except (ValueError, IndexError):
                        continue

            if capacity == 0.0:
                capacity = 100.0  # Default

            # Clean site name
            station_name = str(row.get('Site Name', duid)).strip()
            if not station_name or station_name == 'nan':
                station_name = duid

            generators.append({
                'duid': duid,
                'station_name': station_name,
                'region': region,
                'fuel_source': fuel_source,
                'technology_type': technology_type,
                'capacity_mw': capacity
            })

        if generators:
            await db.update_generator_info(generators)
            logger.info(f"Imported {len(generators)} generator records from GenInfo.csv")
        else:
            logger.warning("No valid generators found in CSV, using sample data")
            await update_sample_generator_info(db)

    except Exception as e:
        logger.error(f"Error importing GenInfo.csv: {e}, falling back to sample data")
        await update_sample_generator_info(db)