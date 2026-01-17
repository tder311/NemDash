from fastapi import FastAPI, HTTPException, BackgroundTasks, Query
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from typing import Optional, List
import os
import logging
from pathlib import Path
import asyncio

from .database import NEMDatabase
from .data_ingester import DataIngester, update_sample_generator_info
from .models import (
    DispatchDataResponse,
    GenerationByFuelResponse,
    DataSummaryResponse,
    DUIDListResponse,
    PriceDataResponse,
    InterconnectorDataResponse,
    FuelMixRecord,
    RegionFuelMixResponse,
    RegionPriceHistoryResponse,
    RegionSummaryResponse,
    DataCoverageResponse
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Global variables
db: NEMDatabase = None
data_ingester: DataIngester = None
background_task: Optional[asyncio.Task] = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application lifecycle"""
    global db, data_ingester, background_task

    # Skip initialization if database is already set (e.g., in tests)
    if db is not None:
        logger.info("Database already initialized (test mode), skipping lifespan startup")
        yield
        return

    # Startup
    db_path = os.getenv('DATABASE_PATH', './data/nem_dispatch.db')
    nem_base_url = os.getenv('NEM_API_BASE_URL', 'https://www.nemweb.com.au')
    update_interval = int(os.getenv('UPDATE_INTERVAL_MINUTES', '5'))

    db = NEMDatabase(db_path)
    data_ingester = DataIngester(db_path, nem_base_url)

    # Initialize database
    await data_ingester.initialize()

    # Update sample generator info
    await update_sample_generator_info(db)

    # Start background data ingestion
    background_task = asyncio.create_task(
        data_ingester.run_continuous_ingestion(update_interval)
    )

    logger.info("NEM Dashboard API started")

    yield

    # Shutdown
    if background_task:
        data_ingester.stop_continuous_ingestion()
        background_task.cancel()
        try:
            await background_task
        except asyncio.CancelledError:
            pass

    if data_ingester:
        await data_ingester.cleanup()

    logger.info("NEM Dashboard API stopped")

app = FastAPI(
    title="NEM Dispatch Data API",
    description="API for accessing NEM (National Electricity Market) dispatch data",
    version="1.0.0",
    lifespan=lifespan
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://localhost:8050"],  # Add your frontend URLs
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
async def root():
    """Root endpoint"""
    return {"message": "NEM Dispatch Data API", "version": "1.0.0"}

@app.get("/health")
async def health():
    """Health check endpoint"""
    return {"status": "healthy", "database": "connected", "timestamp": datetime.now().isoformat()}

@app.get("/api/dispatch/latest", response_model=DispatchDataResponse)
async def get_latest_dispatch_data(
    limit: int = Query(default=1000, ge=1, le=5000, description="Maximum number of records to return")
):
    """Get the latest dispatch data"""
    try:
        df = await db.get_latest_dispatch_data(limit)
        
        if df.empty:
            return DispatchDataResponse(data=[], count=0, message="No data available")
        
        # Convert DataFrame to list of dictionaries
        records = df.to_dict('records')
        
        # Convert datetime objects to ISO strings
        for record in records:
            if 'settlementdate' in record:
                record['settlementdate'] = record['settlementdate'].isoformat()
        
        return DispatchDataResponse(
            data=records,
            count=len(records),
            message=f"Retrieved {len(records)} latest dispatch records"
        )
        
    except Exception as e:
        logger.error(f"Error getting latest dispatch data: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/dispatch/range", response_model=DispatchDataResponse)
async def get_dispatch_data_by_range(
    start_date: datetime = Query(description="Start date (ISO format)"),
    end_date: datetime = Query(description="End date (ISO format)"),
    duid: Optional[str] = Query(default=None, description="Specific DUID to filter by")
):
    """Get dispatch data for a date range"""
    try:
        df = await db.get_dispatch_data_by_date_range(start_date, end_date, duid)
        
        if df.empty:
            return DispatchDataResponse(data=[], count=0, message="No data available for the specified range")
        
        records = df.to_dict('records')
        
        # Convert datetime objects to ISO strings
        for record in records:
            if 'settlementdate' in record:
                record['settlementdate'] = record['settlementdate'].isoformat()
        
        return DispatchDataResponse(
            data=records,
            count=len(records),
            message=f"Retrieved {len(records)} dispatch records for date range"
        )
        
    except Exception as e:
        logger.error(f"Error getting dispatch data by range: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/generation/by-fuel", response_model=GenerationByFuelResponse)
async def get_generation_by_fuel_type(
    start_date: datetime = Query(description="Start date (ISO format)"),
    end_date: datetime = Query(description="End date (ISO format)")
):
    """Get aggregated generation data by fuel type"""
    try:
        df = await db.get_generation_by_fuel_type(start_date, end_date)
        
        if df.empty:
            return GenerationByFuelResponse(data=[], count=0, message="No data available for the specified range")
        
        records = df.to_dict('records')
        
        # Convert datetime objects to ISO strings
        for record in records:
            if 'settlementdate' in record:
                record['settlementdate'] = record['settlementdate'].isoformat()
        
        return GenerationByFuelResponse(
            data=records,
            count=len(records),
            message=f"Retrieved {len(records)} generation records by fuel type"
        )
        
    except Exception as e:
        logger.error(f"Error getting generation by fuel type: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/duids", response_model=DUIDListResponse)
async def get_unique_duids():
    """Get list of all unique DUIDs"""
    try:
        duids = await db.get_unique_duids()
        
        return DUIDListResponse(
            duids=duids,
            count=len(duids),
            message=f"Retrieved {len(duids)} unique DUIDs"
        )
        
    except Exception as e:
        logger.error(f"Error getting unique DUIDs: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/summary", response_model=DataSummaryResponse)
async def get_data_summary():
    """Get summary statistics about the data"""
    try:
        summary = await db.get_data_summary()
        
        return DataSummaryResponse(
            total_records=summary['total_records'],
            unique_duids=summary['unique_duids'],
            earliest_date=summary['earliest_date'],
            latest_date=summary['latest_date'],
            fuel_breakdown=summary['fuel_breakdown']
        )
        
    except Exception as e:
        logger.error(f"Error getting data summary: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/ingest/current")
async def trigger_current_ingestion(background_tasks: BackgroundTasks):
    """Manually trigger current data ingestion"""
    try:
        success = await data_ingester.ingest_current_data()
        
        if success:
            return {"message": "Current data ingestion completed successfully"}
        else:
            raise HTTPException(status_code=500, detail="Failed to ingest current data")
            
    except Exception as e:
        logger.error(f"Error triggering current ingestion: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/ingest/historical")
async def trigger_historical_ingestion(
    start_date: datetime = Query(description="Start date for historical data (ISO format)"),
    end_date: Optional[datetime] = Query(default=None, description="End date for historical data (ISO format)"),
    background_tasks: BackgroundTasks = None
):
    """Manually trigger historical data ingestion"""
    try:
        # Run historical ingestion in background to avoid timeout
        background_tasks.add_task(
            data_ingester.ingest_historical_data,
            start_date,
            end_date
        )
        
        return {"message": f"Historical data ingestion started for {start_date.date()}"}
        
    except Exception as e:
        logger.error(f"Error triggering historical ingestion: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/ingest/historical-prices")
async def trigger_historical_price_ingestion(
    start_date: datetime = Query(description="Start date for historical price data (ISO format)"),
    end_date: Optional[datetime] = Query(default=None, description="End date for historical price data (ISO format)"),
    background_tasks: BackgroundTasks = None
):
    """Manually trigger historical price data ingestion (PUBLIC_PRICES)"""
    try:
        # Run historical price ingestion in background to avoid timeout
        background_tasks.add_task(
            data_ingester.ingest_historical_prices,
            start_date,
            end_date
        )
        
        return {"message": f"Historical price data ingestion started for {start_date.date()} to {end_date.date() if end_date else start_date.date()}"}
        
    except Exception as e:
        logger.error(f"Error triggering historical price ingestion: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/prices/latest", response_model=PriceDataResponse)
async def get_latest_prices(
    price_type: str = Query(default="DISPATCH", description="Price type: DISPATCH, TRADING, or PUBLIC")
):
    """Get latest price data by type"""
    try:
        df = await db.get_latest_prices(price_type)
        
        if df.empty:
            return PriceDataResponse(data=[], count=0, message=f"No {price_type.lower()} price data available")
        
        records = df.to_dict('records')
        for record in records:
            if 'settlementdate' in record:
                record['settlementdate'] = record['settlementdate'].isoformat()
        
        return PriceDataResponse(
            data=records,
            count=len(records),
            message=f"Retrieved {len(records)} {price_type.lower()} price records"
        )
        
    except Exception as e:
        logger.error(f"Error getting latest prices: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/prices/history", response_model=PriceDataResponse)
async def get_price_history(
    start_date: datetime = Query(description="Start date (ISO format)"),
    end_date: datetime = Query(description="End date (ISO format)"),
    region: Optional[str] = Query(default=None, description="Region filter"),
    price_type: str = Query(default="DISPATCH", description="Price type: DISPATCH, TRADING, or PUBLIC")
):
    """Get price history for date range"""
    try:
        df = await db.get_price_history(start_date, end_date, region, price_type)
        
        if df.empty:
            return PriceDataResponse(data=[], count=0, message="No price data available for the specified range")
        
        records = df.to_dict('records')
        for record in records:
            if 'settlementdate' in record:
                record['settlementdate'] = record['settlementdate'].isoformat()
        
        return PriceDataResponse(
            data=records,
            count=len(records),
            message=f"Retrieved {len(records)} price records"
        )
        
    except Exception as e:
        logger.error(f"Error getting price history: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/interconnectors/latest", response_model=InterconnectorDataResponse)
async def get_latest_interconnector_flows():
    """Get latest interconnector flow data"""
    try:
        df = await db.get_latest_interconnector_flows()
        
        if df.empty:
            return InterconnectorDataResponse(data=[], count=0, message="No interconnector flow data available")
        
        records = df.to_dict('records')
        for record in records:
            if 'settlementdate' in record:
                record['settlementdate'] = record['settlementdate'].isoformat()
        
        return InterconnectorDataResponse(
            data=records,
            count=len(records),
            message=f"Retrieved {len(records)} interconnector flow records"
        )
        
    except Exception as e:
        logger.error(f"Error getting latest interconnector flows: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/interconnectors/history", response_model=InterconnectorDataResponse)
async def get_interconnector_history(
    start_date: datetime = Query(description="Start date (ISO format)"),
    end_date: datetime = Query(description="End date (ISO format)"),
    interconnector: Optional[str] = Query(default=None, description="Interconnector name filter")
):
    """Get interconnector flow history"""
    try:
        df = await db.get_interconnector_history(start_date, end_date, interconnector)
        
        if df.empty:
            return InterconnectorDataResponse(data=[], count=0, message="No interconnector data available for the specified range")
        
        records = df.to_dict('records')
        for record in records:
            if 'settlementdate' in record:
                record['settlementdate'] = record['settlementdate'].isoformat()
        
        return InterconnectorDataResponse(
            data=records,
            count=len(records),
            message=f"Retrieved {len(records)} interconnector flow records"
        )
        
    except Exception as e:
        logger.error(f"Error getting interconnector history: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/generators/filter", response_model=DispatchDataResponse)
async def get_generators_by_region_fuel(
    region: Optional[str] = Query(default=None, description="Region filter (NSW, VIC, QLD, SA, TAS)"),
    fuel_source: Optional[str] = Query(default=None, description="Fuel source filter")
):
    """Get generators filtered by region and/or fuel source"""
    try:
        df = await db.get_generators_by_region_fuel(region, fuel_source)
        
        if df.empty:
            return DispatchDataResponse(data=[], count=0, message="No generators found for the specified filters")
        
        records = df.to_dict('records')
        for record in records:
            if 'settlementdate' in record:
                record['settlementdate'] = record['settlementdate'].isoformat()
        
        return DispatchDataResponse(
            data=records,
            count=len(records),
            message=f"Retrieved {len(records)} generator records"
        )
        
    except Exception as e:
        logger.error(f"Error getting filtered generators: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Region-specific endpoints for state drilldown
@app.get("/api/region/{region}/generation/current", response_model=RegionFuelMixResponse)
async def get_region_current_generation(region: str):
    """Get current generation breakdown by fuel type for a specific region"""
    valid_regions = ['NSW', 'VIC', 'QLD', 'SA', 'TAS']
    region = region.upper()

    if region not in valid_regions:
        raise HTTPException(status_code=400, detail=f"Invalid region. Must be one of: {', '.join(valid_regions)}")

    try:
        df = await db.get_region_fuel_mix(region)

        if df.empty:
            return RegionFuelMixResponse(
                region=region,
                settlementdate=None,
                total_generation=0,
                fuel_mix=[],
                message=f"No generation data available for {region}"
            )

        # Convert to response format
        fuel_mix = []
        for _, row in df.iterrows():
            fuel_mix.append(FuelMixRecord(
                fuel_source=row['fuel_source'],
                generation_mw=round(row['generation_mw'], 2) if row['generation_mw'] else 0,
                percentage=round(row['percentage'], 1) if row['percentage'] else 0,
                unit_count=int(row['unit_count'])
            ))

        total_generation = df['generation_mw'].sum()
        settlementdate = df['settlementdate'].iloc[0] if not df.empty else None

        return RegionFuelMixResponse(
            region=region,
            settlementdate=str(settlementdate) if settlementdate else None,
            total_generation=round(total_generation, 2),
            fuel_mix=fuel_mix,
            message=f"Retrieved fuel mix for {region}"
        )

    except Exception as e:
        logger.error(f"Error getting region fuel mix for {region}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/region/{region}/prices/history", response_model=RegionPriceHistoryResponse)
async def get_region_price_history(
    region: str,
    hours: int = Query(default=24, ge=1, le=168, description="Hours of history (1-168)"),
    price_type: str = Query(default="DISPATCH", description="Price type: DISPATCH, TRADING, or PUBLIC")
):
    """Get price history for a specific region over the last N hours"""
    valid_regions = ['NSW', 'VIC', 'QLD', 'SA', 'TAS']
    region = region.upper()

    if region not in valid_regions:
        raise HTTPException(status_code=400, detail=f"Invalid region. Must be one of: {', '.join(valid_regions)}")

    try:
        df = await db.get_region_price_history(region, hours, price_type)

        if df.empty:
            return RegionPriceHistoryResponse(
                region=region,
                data=[],
                count=0,
                hours=hours,
                price_type=price_type,
                message=f"No price history available for {region}"
            )

        records = df.to_dict('records')
        for record in records:
            if 'settlementdate' in record:
                record['settlementdate'] = record['settlementdate'].isoformat()

        return RegionPriceHistoryResponse(
            region=region,
            data=records,
            count=len(records),
            hours=hours,
            price_type=price_type,
            message=f"Retrieved {len(records)} price records for {region}"
        )

    except Exception as e:
        logger.error(f"Error getting price history for {region}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/region/{region}/summary", response_model=RegionSummaryResponse)
async def get_region_summary(region: str):
    """Get summary statistics for a specific region"""
    valid_regions = ['NSW', 'VIC', 'QLD', 'SA', 'TAS']
    region = region.upper()

    if region not in valid_regions:
        raise HTTPException(status_code=400, detail=f"Invalid region. Must be one of: {', '.join(valid_regions)}")

    try:
        summary = await db.get_region_summary(region)

        return RegionSummaryResponse(
            region=summary['region'],
            latest_price=summary['latest_price'],
            total_demand=summary['total_demand'],
            price_timestamp=summary['price_timestamp'],
            total_generation=summary['total_generation'],
            generator_count=summary['generator_count'],
            message=f"Retrieved summary for {region}"
        )

    except Exception as e:
        logger.error(f"Error getting summary for {region}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/data/coverage", response_model=DataCoverageResponse)
async def get_data_coverage(
    table: str = Query(default="price_data", description="Table to check: price_data, dispatch_data, interconnector_data")
):
    """Get data coverage information for backfill planning"""
    valid_tables = ['price_data', 'dispatch_data', 'interconnector_data']

    if table not in valid_tables:
        raise HTTPException(status_code=400, detail=f"Invalid table. Must be one of: {', '.join(valid_tables)}")

    try:
        coverage = await db.get_data_coverage(table)

        return DataCoverageResponse(
            table=table,
            earliest_date=coverage['earliest_date'],
            latest_date=coverage['latest_date'],
            total_records=coverage['total_records'],
            days_with_data=coverage['days_with_data'],
            message=f"Data coverage for {table}"
        )

    except Exception as e:
        logger.error(f"Error getting data coverage: {e}")
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True)