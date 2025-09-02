import asyncio
import logging
from datetime import datetime, timedelta
from typing import Optional
import time
from pathlib import Path

from .nem_client import NEMDispatchClient
from .nem_price_client import NEMPriceClient
from .database import NEMDatabase

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataIngester:
    def __init__(self, db_path: str, nem_base_url: str = "https://www.nemweb.com.au"):
        self.db = NEMDatabase(db_path)
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
    
    async def run_continuous_ingestion(self, interval_minutes: int = 5):
        """Run continuous data ingestion"""
        self.is_running = True
        logger.info(f"Starting continuous ingestion with {interval_minutes} minute intervals")
        
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