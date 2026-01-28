from fastapi import FastAPI, HTTPException, Query, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
import io
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Optional
import os
import logging
import asyncio

from .database import NEMDatabase, calculate_aggregation_minutes, to_aest_isoformat
from .data_ingester import DataIngester, import_generator_info_from_csv
from .models import (
    DispatchDataResponse,
    GenerationByFuelResponse,
    DataSummaryResponse,
    DUIDListResponse,
    PriceDataResponse,
    FuelMixRecord,
    RegionFuelMixResponse,
    RegionPriceHistoryResponse,
    RegionGenerationHistoryResponse,
    RegionSummaryResponse,
    DataCoverageResponse,
    DatabaseHealthResponse,
    PASADataResponse,
    RegionDataRangeResponse
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
    db_url = os.getenv('DATABASE_URL')
    if not db_url:
        raise RuntimeError(
            "DATABASE_URL environment variable is required. "
            "Example: postgresql://postgres:localdev@localhost:5432/nem_dashboard"
        )
    nem_base_url = os.getenv('NEM_API_BASE_URL', 'https://www.nemweb.com.au')
    update_interval = int(os.getenv('UPDATE_INTERVAL_MINUTES', '5'))

    data_ingester = DataIngester(db_url, nem_base_url)

    # Initialize database
    await data_ingester.initialize()

    # Use the data_ingester's database instance for consistency
    db = data_ingester.db

    # Import generator info from CSV (falls back to sample data if not found)
    await import_generator_info_from_csv(db)

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

    # Close database connection pool (for PostgreSQL)
    if db:
        await db.close()

    logger.info("NEM Dashboard API stopped")

app = FastAPI(
    title="NEM Dispatch Data API",
    description="API for accessing NEM (National Electricity Market) dispatch data",
    version="1.0.0",
    lifespan=lifespan
)

# Add CORS middleware
allowed_origins = os.getenv('ALLOWED_ORIGINS', 'http://localhost:3000,http://localhost:8050').split(',')
app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,
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


@app.get("/api/time-range-options")
async def get_time_range_options():
    """Get available time range options for the frontend.

    Returns a list of predefined time ranges with their corresponding
    aggregation intervals for optimal data visualization.
    """
    options = [
        {"label": "24 hours", "hours": 24, "aggregation_minutes": calculate_aggregation_minutes(24)},
        {"label": "48 hours", "hours": 48, "aggregation_minutes": calculate_aggregation_minutes(48)},
        {"label": "7 days", "hours": 168, "aggregation_minutes": calculate_aggregation_minutes(168)},
        {"label": "14 days", "hours": 336, "aggregation_minutes": calculate_aggregation_minutes(336)},
        {"label": "30 days", "hours": 720, "aggregation_minutes": calculate_aggregation_minutes(720)},
        {"label": "60 days", "hours": 1440, "aggregation_minutes": calculate_aggregation_minutes(1440)},
        {"label": "90 days", "hours": 2160, "aggregation_minutes": calculate_aggregation_minutes(2160)},
        {"label": "365 days", "hours": 8760, "aggregation_minutes": calculate_aggregation_minutes(8760)},
    ]
    return {"options": options}


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
        
        # Convert datetime objects to ISO strings with AEST timezone
        for record in records:
            if 'settlementdate' in record:
                record['settlementdate'] = to_aest_isoformat(record['settlementdate'])

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

        # Convert datetime objects to ISO strings with AEST timezone
        for record in records:
            if 'settlementdate' in record:
                record['settlementdate'] = to_aest_isoformat(record['settlementdate'])

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

        # Convert datetime objects to ISO strings with AEST timezone
        for record in records:
            if 'settlementdate' in record:
                record['settlementdate'] = to_aest_isoformat(record['settlementdate'])

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
                record['settlementdate'] = to_aest_isoformat(record['settlementdate'])

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
                record['settlementdate'] = to_aest_isoformat(record['settlementdate'])

        return PriceDataResponse(
            data=records,
            count=len(records),
            message=f"Retrieved {len(records)} price records"
        )
        
    except Exception as e:
        logger.error(f"Error getting price history: {e}")
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
                record['settlementdate'] = to_aest_isoformat(record['settlementdate'])

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


@app.get("/api/region/{region}/generation/history", response_model=RegionGenerationHistoryResponse)
async def get_region_generation_history(
    region: str,
    hours: Optional[int] = Query(default=None, ge=1, le=8760, description="Hours of history (1-8760, i.e., up to 365 days)"),
    start_date: Optional[datetime] = Query(default=None, description="Start date for custom range (ISO format)"),
    end_date: Optional[datetime] = Query(default=None, description="End date for custom range (ISO format)"),
    aggregation: Optional[int] = Query(default=None, ge=5, le=10080, description="Aggregation interval in minutes (auto-calculated if not specified)")
):
    """Get historical generation by fuel source for a specific region.

    Can be queried by either:
    - hours: Get the last N hours of data (backwards from now)
    - start_date/end_date: Get data for a specific date range

    Auto-aggregation levels when aggregation is not specified:
    - < 48h: 5 min (raw data)
    - 48h - 7d: 30 min
    - 7d - 30d: 60 min (hourly)
    - 30d - 90d: 1440 min (daily)
    - > 90d: 10080 min (weekly)
    """
    valid_regions = ['NSW', 'VIC', 'QLD', 'SA', 'TAS']
    region = region.upper()

    if region not in valid_regions:
        raise HTTPException(status_code=400, detail=f"Invalid region. Must be one of: {', '.join(valid_regions)}")

    # Determine hours from date range or default
    if start_date and end_date:
        if end_date <= start_date:
            raise HTTPException(status_code=400, detail="end_date must be after start_date")
        delta = end_date - start_date
        hours = int(delta.total_seconds() / 3600)
        if hours < 1:
            hours = 1
        if hours > 8760:
            hours = 8760
    elif hours is None:
        hours = 24  # Default to 24 hours

    # Use auto-calculated aggregation if not specified
    if aggregation is None:
        aggregation = calculate_aggregation_minutes(hours)

    try:
        if start_date and end_date:
            df = await db.get_region_generation_history_by_dates(region, start_date, end_date, aggregation)
        else:
            df = await db.get_region_generation_history(region, hours, aggregation)

        if df.empty:
            return RegionGenerationHistoryResponse(
                region=region,
                data=[],
                count=0,
                hours=hours,
                aggregation_minutes=aggregation,
                message=f"No generation history available for {region}"
            )

        records = df.to_dict('records')
        for record in records:
            if 'period' in record:
                record['period'] = to_aest_isoformat(record['period'])

        return RegionGenerationHistoryResponse(
            region=region,
            data=records,
            count=len(records),
            hours=hours,
            aggregation_minutes=aggregation,
            message=f"Retrieved {len(records)} generation history records for {region}"
        )

    except Exception as e:
        logger.error(f"Error getting generation history for {region}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/region/{region}/prices/history", response_model=RegionPriceHistoryResponse)
async def get_region_price_history(
    region: str,
    hours: Optional[int] = Query(default=None, ge=1, le=8760, description="Hours of history (1-8760, i.e., up to 365 days)"),
    start_date: Optional[datetime] = Query(default=None, description="Start date for custom range (ISO format)"),
    end_date: Optional[datetime] = Query(default=None, description="End date for custom range (ISO format)"),
    price_type: str = Query(default="DISPATCH", description="Price type: DISPATCH, TRADING, PUBLIC, or MERGED")
):
    """Get price history for a specific region.

    Can be queried by either:
    - hours: Get the last N hours of data (backwards from now)
    - start_date/end_date: Get data for a specific date range

    The MERGED price type intelligently combines PUBLIC (official settlement) prices
    with DISPATCH (real-time) prices. PUBLIC prices are preferred where available,
    with DISPATCH filling gaps from the latest PUBLIC timestamp to current time.

    For extended ranges (>7 days), data is automatically aggregated:
    - 7d - 30d: hourly averages
    - 30d - 90d: daily averages
    - > 90d: weekly averages
    """
    valid_regions = ['NSW', 'VIC', 'QLD', 'SA', 'TAS']
    valid_price_types = ['DISPATCH', 'TRADING', 'PUBLIC', 'MERGED']
    region = region.upper()
    price_type = price_type.upper()

    if region not in valid_regions:
        raise HTTPException(status_code=400, detail=f"Invalid region. Must be one of: {', '.join(valid_regions)}")

    if price_type not in valid_price_types:
        raise HTTPException(status_code=400, detail=f"Invalid price_type. Must be one of: {', '.join(valid_price_types)}")

    # Determine hours from date range or default
    if start_date and end_date:
        if end_date <= start_date:
            raise HTTPException(status_code=400, detail="end_date must be after start_date")
        delta = end_date - start_date
        hours = int(delta.total_seconds() / 3600)
        if hours < 1:
            hours = 1
        if hours > 8760:
            hours = 8760
    elif hours is None:
        hours = 24  # Default to 24 hours

    # Calculate aggregation for response metadata
    aggregation_minutes = calculate_aggregation_minutes(hours)

    try:
        # For extended ranges with MERGED, use aggregated price history
        if start_date and end_date:
            df = await db.get_aggregated_price_history_by_dates(region, start_date, end_date)
        elif price_type == 'MERGED' or aggregation_minutes > 30:
            df = await db.get_aggregated_price_history(region, hours)
        else:
            df = await db.get_region_price_history(region, hours, price_type)

        if df.empty:
            return RegionPriceHistoryResponse(
                region=region,
                data=[],
                count=0,
                hours=hours,
                price_type=price_type,
                aggregation_minutes=aggregation_minutes,
                message=f"No price history available for {region}"
            )

        records = df.to_dict('records')
        for record in records:
            if 'settlementdate' in record:
                record['settlementdate'] = to_aest_isoformat(record['settlementdate'])

        return RegionPriceHistoryResponse(
            region=region,
            data=records,
            count=len(records),
            hours=hours,
            price_type=price_type,
            aggregation_minutes=aggregation_minutes,
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


@app.get("/api/region/{region}/data-range", response_model=RegionDataRangeResponse)
async def get_region_data_range(region: str):
    """Get the available date range for a specific region's price data.

    Returns the earliest and latest dates with price data for the region.
    Useful for setting date picker bounds in the frontend.
    """
    valid_regions = ['NSW', 'VIC', 'QLD', 'SA', 'TAS']
    region = region.upper()

    if region not in valid_regions:
        raise HTTPException(status_code=400, detail=f"Invalid region. Must be one of: {', '.join(valid_regions)}")

    try:
        # Get data coverage from price_data table for this region
        coverage = await db.get_region_data_range(region)

        return RegionDataRangeResponse(
            region=region,
            earliest_date=coverage.get('earliest_date'),
            latest_date=coverage.get('latest_date'),
            message=f"Data range for {region}"
        )

    except Exception as e:
        logger.error(f"Error getting data range for {region}: {e}")
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


@app.get("/api/database/health", response_model=DatabaseHealthResponse)
async def get_database_health(
    hours_back: int = Query(default=168, ge=1, le=8760, description="Hours of history to check for gaps (1-8760)")
):
    """Get comprehensive database health including record counts and gap detection.

    Returns:
    - Record counts for all tables (dispatch_data, price_data, interconnector_data, generator_info)
    - Date ranges for each table
    - Detected gaps in 5-minute interval data within the specified time range
    """
    try:
        health = await db.get_database_health(hours_back)

        return DatabaseHealthResponse(
            tables=health['tables'],
            gaps=health['gaps'],
            checked_hours=health['checked_hours'],
            checked_at=health['checked_at']
        )

    except Exception as e:
        logger.error(f"Error getting database health: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# PASA (Projected Assessment of System Adequacy) endpoints
@app.get("/api/pasa/pdpasa/{region}", response_model=PASADataResponse)
async def get_region_pdpasa(region: str):
    """Get the latest PDPASA (Pre-Dispatch PASA) forecast for a specific region.

    PDPASA provides short-term reserve forecasts (approximately 6 hours ahead)
    at 30-minute intervals. Includes demand forecasts, available capacity,
    surplus reserve, and LOR (Lack of Reserve) conditions.

    LOR Levels:
    - 0: No LOR - Adequate reserves
    - 1: LOR1 - Low Reserve Condition
    - 2: LOR2 - Lack of Reserve 2
    - 3: LOR3 - Lack of Reserve 3 (Load Shedding Imminent)
    """
    valid_regions = ['NSW1', 'VIC1', 'QLD1', 'SA1', 'TAS1']
    region = region.upper()

    # Allow both formats: NSW or NSW1
    if region in ['NSW', 'VIC', 'QLD', 'SA', 'TAS']:
        region = region + '1'

    if region not in valid_regions:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid region. Must be one of: {', '.join(valid_regions)} (or without the '1' suffix)"
        )

    try:
        data = await db.get_latest_pdpasa(region)

        if not data:
            return PASADataResponse(
                data=[],
                run_datetime=None,
                region=region,
                count=0,
                message=f"No PDPASA data available for {region}"
            )

        # Convert datetime objects to ISO strings with AEST timezone
        for record in data:
            if 'run_datetime' in record and record['run_datetime']:
                record['run_datetime'] = to_aest_isoformat(record['run_datetime'])
            if 'interval_datetime' in record and record['interval_datetime']:
                record['interval_datetime'] = to_aest_isoformat(record['interval_datetime'])
            if 'created_at' in record and record['created_at']:
                record['created_at'] = record['created_at'].isoformat()

        run_datetime = data[0].get('run_datetime') if data else None

        return PASADataResponse(
            data=data,
            run_datetime=run_datetime,
            region=region,
            count=len(data),
            message=f"Retrieved {len(data)} PDPASA records for {region}"
        )

    except Exception as e:
        logger.error(f"Error getting PDPASA data for {region}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/pasa/stpasa/{region}", response_model=PASADataResponse)
async def get_region_stpasa(region: str):
    """Get the latest STPASA (Short Term PASA) forecast for a specific region.

    STPASA provides medium-term reserve forecasts (approximately 6 days ahead)
    at 30-minute intervals. Includes demand forecasts, available capacity,
    surplus reserve, and LOR (Lack of Reserve) conditions.

    LOR Levels:
    - 0: No LOR - Adequate reserves
    - 1: LOR1 - Low Reserve Condition
    - 2: LOR2 - Lack of Reserve 2
    - 3: LOR3 - Lack of Reserve 3 (Load Shedding Imminent)
    """
    valid_regions = ['NSW1', 'VIC1', 'QLD1', 'SA1', 'TAS1']
    region = region.upper()

    # Allow both formats: NSW or NSW1
    if region in ['NSW', 'VIC', 'QLD', 'SA', 'TAS']:
        region = region + '1'

    if region not in valid_regions:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid region. Must be one of: {', '.join(valid_regions)} (or without the '1' suffix)"
        )

    try:
        data = await db.get_latest_stpasa(region)

        if not data:
            return PASADataResponse(
                data=[],
                run_datetime=None,
                region=region,
                count=0,
                message=f"No STPASA data available for {region}"
            )

        # Convert datetime objects to ISO strings with AEST timezone
        for record in data:
            if 'run_datetime' in record and record['run_datetime']:
                record['run_datetime'] = to_aest_isoformat(record['run_datetime'])
            if 'interval_datetime' in record and record['interval_datetime']:
                record['interval_datetime'] = to_aest_isoformat(record['interval_datetime'])
            if 'created_at' in record and record['created_at']:
                record['created_at'] = record['created_at'].isoformat()

        run_datetime = data[0].get('run_datetime') if data else None

        return PASADataResponse(
            data=data,
            run_datetime=run_datetime,
            region=region,
            count=len(data),
            message=f"Retrieved {len(data)} STPASA records for {region}"
        )

    except Exception as e:
        logger.error(f"Error getting STPASA data for {region}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# CSV Export endpoints
@app.get("/api/export/available-options")
async def get_export_options():
    """Get available export options and data ranges for the downloads page.

    Returns:
    - Available regions
    - Available fuel sources
    - PASA types
    - Data ranges for each exportable data type
    """
    try:
        fuel_sources = await db.get_unique_fuel_sources()
        data_ranges = await db.get_export_data_ranges()

        return {
            "regions": ["NSW", "VIC", "QLD", "SA", "TAS"],
            "fuel_sources": fuel_sources,
            "pasa_types": ["pdpasa", "stpasa"],
            "data_ranges": data_ranges
        }
    except Exception as e:
        logger.error(f"Error getting export options: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/export/prices")
async def export_prices_csv(
    start_date: datetime = Query(..., description="Start date (ISO format)"),
    end_date: datetime = Query(..., description="End date (ISO format)"),
    regions: Optional[str] = Query(default=None, description="Comma-separated regions (e.g., NSW,VIC)")
):
    """Export price data as CSV file.

    Returns a CSV file with columns: settlementdate, region, price, totaldemand, price_type
    """
    try:
        region_list = regions.split(',') if regions else None
        df = await db.export_price_data(start_date, end_date, region_list)

        stream = io.StringIO()
        df.to_csv(stream, index=False)
        stream.seek(0)

        filename = f"nem_prices_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}.csv"
        response = StreamingResponse(
            iter([stream.getvalue()]),
            media_type="text/csv"
        )
        response.headers["Content-Disposition"] = f"attachment; filename={filename}"
        return response

    except Exception as e:
        logger.error(f"Error exporting prices: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/export/generation")
async def export_generation_csv(
    start_date: datetime = Query(..., description="Start date (ISO format)"),
    end_date: datetime = Query(..., description="End date (ISO format)"),
    regions: Optional[str] = Query(default=None, description="Comma-separated regions (e.g., NSW,VIC)"),
    fuel_sources: Optional[str] = Query(default=None, description="Comma-separated fuel sources (e.g., Coal,Gas)")
):
    """Export generation data as CSV file.

    Returns a CSV file with columns: settlementdate, duid, station_name, region,
    fuel_source, technology_type, generation_mw, totalcleared, availability
    """
    try:
        region_list = regions.split(',') if regions else None
        fuel_list = fuel_sources.split(',') if fuel_sources else None
        df = await db.export_generation_data(start_date, end_date, region_list, fuel_list)

        stream = io.StringIO()
        df.to_csv(stream, index=False)
        stream.seek(0)

        filename = f"nem_generation_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}.csv"
        response = StreamingResponse(
            iter([stream.getvalue()]),
            media_type="text/csv"
        )
        response.headers["Content-Disposition"] = f"attachment; filename={filename}"
        return response

    except Exception as e:
        logger.error(f"Error exporting generation: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/export/pasa")
async def export_pasa_csv(
    pasa_type: str = Query(..., description="PASA type: pdpasa or stpasa"),
    regions: Optional[str] = Query(default=None, description="Comma-separated regions (e.g., NSW,VIC)")
):
    """Export the latest PASA forecast data as CSV file.

    PASA data is forecast data - this exports the most recent forecast run.

    Returns a CSV file with columns: run_datetime, interval_datetime, regionid,
    demand10, demand50, demand90, reservereq, capacityreq, aggregatecapacityavailable,
    aggregatepasaavailability, surplusreserve, lorcondition, calculatedlor1level, calculatedlor2level
    """
    if pasa_type not in ['pdpasa', 'stpasa']:
        raise HTTPException(status_code=400, detail="pasa_type must be 'pdpasa' or 'stpasa'")

    try:
        region_list = regions.split(',') if regions else None
        df = await db.export_latest_pasa_data(pasa_type, region_list)

        stream = io.StringIO()
        df.to_csv(stream, index=False)
        stream.seek(0)

        timestamp = datetime.now().strftime('%Y%m%d_%H%M')
        filename = f"nem_{pasa_type}_forecast_{timestamp}.csv"
        response = StreamingResponse(
            iter([stream.getvalue()]),
            media_type="text/csv"
        )
        response.headers["Content-Disposition"] = f"attachment; filename={filename}"
        return response

    except Exception as e:
        logger.error(f"Error exporting PASA data: {e}")
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True)