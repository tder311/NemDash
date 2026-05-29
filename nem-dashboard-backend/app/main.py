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
    RegionDataRangeResponse,
    DailyMetricsResponse,
    MetricsSummaryResponse,
    BidBandResponse,
    DUIDSearchResponse,
    PriceForecastResponse,
)
from .forecaster import (
    REGIONS,
    HORIZON_INTERVALS,
    PriceForecaster,
    generate_forecast,
    default_model_path,
    forecast_price_series,
    train_and_save,
)
from .optimiser import DispatchInputs, optimise_dispatch
from .bid_bands import compute_bid_curves, DEFAULT_PRICE_GRID, derived_grid
from . import agent as nem_agent
from pydantic import BaseModel
from typing import List, Dict, Any

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
_forecaster: Optional[PriceForecaster] = None
_openai_client = None


def _get_openai_client():
    """Lazily build the async OpenAI client (so the app boots without a key)."""
    global _openai_client
    if _openai_client is None:
        from openai import AsyncOpenAI
        # Reads OPENAI_API_KEY (and optional OPENAI_BASE_URL) from the env.
        kwargs = {}
        base_url = os.getenv("OPENAI_BASE_URL")
        if base_url:
            kwargs["base_url"] = base_url
        _openai_client = AsyncOpenAI(**kwargs)
    return _openai_client


class ChatRequest(BaseModel):
    messages: List[Dict[str, Any]]  # [{role: "user"|"assistant", content: ...}]


def _get_forecaster() -> Optional[PriceForecaster]:
    """Lazily load the trained price model from disk (cached after first load)."""
    global _forecaster
    if _forecaster is None:
        path = default_model_path()
        if os.path.exists(path):
            _forecaster = PriceForecaster.load(path)
    return _forecaster


# In-process state for the retrain-on-demand flow (fire-and-poll).
_training_state: dict = {
    "status": "idle",  # idle | running | done | error
    "started_at": None,
    "finished_at": None,
    "metrics": None,
    "n_rows": None,
    "error": None,
}


async def _run_retrain(days: int):
    """Background task: retrain, hot-swap the cached model, record status."""
    global _forecaster
    _training_state.update(status="running", started_at=datetime.now().isoformat(),
                           finished_at=None, error=None)
    try:
        result = await train_and_save(db, days=days)
        _forecaster = result["model"]  # hot-reload — no restart needed
        _training_state.update(status="done", finished_at=datetime.now().isoformat(),
                               metrics=result["metrics"], n_rows=result["n_rows"], error=None)
        logger.info(f"Retrain complete: {result['n_rows']} rows, metrics={result['metrics']}")
    except Exception as e:
        logger.error(f"Retrain failed: {e}")
        _training_state.update(status="error", finished_at=datetime.now().isoformat(), error=str(e))

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

@app.post("/api/ingest/backfill-dispatch-prices")
async def trigger_dispatch_price_backfill(background_tasks: BackgroundTasks):
    """Refetch DISPATCH price files from NEMWEB Current/ since the latest PUBLIC price.

    Used to fill DISPATCH gaps caused by transient NEMWEB 403s during the
    initial polling pass. Idempotent — inserts collide on the unique constraint.
    """
    background_tasks.add_task(data_ingester.backfill_dispatch_prices)
    return {"message": "DISPATCH price backfill started"}


@app.post("/api/ingest/refetch-current-scada")
async def trigger_current_scada_refetch(
    since: datetime = Query(description="Refetch SCADA files newer than this timestamp (ISO)"),
    background_tasks: BackgroundTasks = None,
):
    """Refetch dispatch SCADA files from NEMWEB Current/Dispatch_SCADA newer than `since`.

    Fills SCADA gaps caused by transient NEMWEB 403s during the initial polling
    pass. Idempotent — inserts collide on the unique constraint.
    """
    async def _refetch():
        df = await data_ingester.nem_client.get_all_current_dispatch_data(since=since)
        if df is not None and not df.empty:
            inserted = await data_ingester.db.insert_dispatch_data(df)
            logger.info(f"Refetched {inserted} SCADA records since {since}")
        else:
            logger.info(f"No SCADA records to refetch since {since}")

    background_tasks.add_task(_refetch)
    return {"message": f"SCADA refetch started for files newer than {since.isoformat()}"}


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

@app.get("/api/metrics/daily", response_model=DailyMetricsResponse)
async def get_daily_metrics(
    region: str = Query(description="Region code (NSW, VIC, QLD, SA, TAS)"),
    start_date: datetime = Query(description="Start date (ISO format)"),
    end_date: datetime = Query(description="End date (ISO format)")
):
    """Get precalculated daily capture rates and TB spreads for a region."""
    valid_regions = ['NSW', 'VIC', 'QLD', 'SA', 'TAS']
    region = region.upper()
    if region not in valid_regions:
        raise HTTPException(status_code=400, detail=f"Invalid region. Must be one of: {', '.join(valid_regions)}")
    try:
        data = await db.get_daily_metrics(region, start_date, end_date)
        return DailyMetricsResponse(
            region=region,
            data=data,
            count=len(data),
            start_date=start_date.date().isoformat(),
            end_date=end_date.date().isoformat(),
            message=f"Retrieved {len(data)} days of metrics for {region}"
        )
    except Exception as e:
        logger.error(f"Error getting daily metrics for {region}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/metrics/summary", response_model=MetricsSummaryResponse)
async def get_metrics_summary(
    region: str = Query(description="Region code (NSW, VIC, QLD, SA, TAS)")
):
    """Get averaged capture rates and TB spreads for multiple lookback periods."""
    valid_regions = ['NSW', 'VIC', 'QLD', 'SA', 'TAS']
    region = region.upper()
    if region not in valid_regions:
        raise HTTPException(status_code=400, detail=f"Invalid region. Must be one of: {', '.join(valid_regions)}")

    try:
        from datetime import date, timedelta
        end = date.today() - timedelta(days=1)  # yesterday (most recent complete day)
        lookbacks = {
            '24h': 1,
            '7d': 7,
            '30d': 30,
            '365d': 365
        }

        periods = {}
        for label, days in lookbacks.items():
            start = end - timedelta(days=days - 1)
            summary = await db.get_metrics_summary(region, start, end)
            periods[label] = summary

        return MetricsSummaryResponse(
            region=region,
            periods=periods,
            message=f"Summary metrics for {region}"
        )
    except Exception as e:
        logger.error(f"Error getting metrics summary for {region}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/ingest/calculate-metrics")
async def trigger_metrics_calculation(
    start_date: datetime = Query(description="Start date (ISO format)"),
    end_date: Optional[datetime] = Query(default=None),
    background_tasks: BackgroundTasks = None
):
    """Trigger backfill calculation of daily metrics (capture rates and TB spreads)."""
    background_tasks.add_task(
        data_ingester.backfill_daily_metrics,
        start_date,
        end_date
    )
    return {"message": f"Metrics calculation started from {start_date.date()}"}


@app.post("/api/ingest/backfill-price-setter")
async def trigger_price_setter_backfill(
    start_date: datetime = Query(description="Start date (ISO format)"),
    end_date: Optional[datetime] = Query(default=None),
    background_tasks: BackgroundTasks = None
):
    """Trigger backfill of NemPriceSetter data and price setting metrics."""
    background_tasks.add_task(
        data_ingester.backfill_price_setter_data,
        start_date,
        end_date
    )
    return {"message": f"Price setter backfill started from {start_date.date()}"}


@app.post("/api/ingest/backfill-pasa")
async def trigger_pasa_backfill(
    start_date: datetime = Query(description="Start date (ISO format)"),
    end_date: Optional[datetime] = Query(default=None),
    background_tasks: BackgroundTasks = None
):
    """Trigger backfill of historical PD/ST PASA from the NEMWEB archive."""
    background_tasks.add_task(
        data_ingester.backfill_pasa_data,
        start_date,
        end_date
    )
    return {"message": f"PASA backfill started from {start_date.date()}"}


@app.post("/api/ingest/backfill-predispatch")
async def trigger_predispatch_backfill(
    start_date: datetime = Query(description="Start date (ISO format)"),
    end_date: Optional[datetime] = Query(default=None),
    background_tasks: BackgroundTasks = None
):
    """Trigger backfill of historical pre-dispatch price from the NEMWEB archive."""
    background_tasks.add_task(
        data_ingester.backfill_predispatch_data,
        start_date,
        end_date
    )
    return {"message": f"Pre-dispatch backfill started from {start_date.date()}"}


@app.post("/api/ingest/recalculate-price-setter-metrics")
async def trigger_price_setter_metrics_recalc(
    start_date: datetime = Query(description="Start date (ISO format)"),
    end_date: Optional[datetime] = Query(default=None),
    background_tasks: BackgroundTasks = None
):
    """Recalculate price setter daily metrics from existing raw data (no download)."""
    background_tasks.add_task(
        data_ingester.recalculate_price_setter_metrics,
        start_date,
        end_date
    )
    return {"message": f"Price setter metrics recalculation started from {start_date.date()}"}


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


@app.get("/api/forecast/prices", response_model=PriceForecastResponse)
async def get_price_forecast(region: str):
    """7-day-ahead 30-min price forecast for a region, driven by latest PASA."""
    region = region.upper()
    if region not in REGIONS:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown region '{region}'. Expected one of {REGIONS}.",
        )

    model = _get_forecaster()
    if model is None:
        raise HTTPException(
            status_code=503,
            detail="Price model not trained yet. Run: python -m scripts.train_forecaster",
        )

    try:
        data = await generate_forecast(db, region, model)
        return PriceForecastResponse(
            region=region,
            data=data,
            count=len(data),
            horizon_intervals=HORIZON_INTERVALS,
            model_trained_at=model.card.trained_at or None,
            message=f"{len(data)}-interval price forecast for {region}",
        )
    except Exception as e:
        logger.error(f"Error generating price forecast for {region}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/forecast/retrain")
async def retrain_forecast(
    days: int = Query(default=365, description="Training window length in days"),
    background_tasks: BackgroundTasks = None,
):
    """Kick off a model retrain (background); poll /api/forecast/status for progress."""
    if _training_state["status"] == "running":
        raise HTTPException(status_code=409, detail="A retrain is already in progress.")
    background_tasks.add_task(_run_retrain, days)
    return {"message": f"Retraining started on the last {days} days."}


@app.get("/api/forecast/status")
async def forecast_status():
    """Current training status plus the live model's trained-at timestamp."""
    model = _get_forecaster()
    return {**_training_state, "model_trained_at": model.card.trained_at if model else None}


@app.get("/api/predispatch/prices")
async def get_predispatch_prices(region: str):
    """Latest AEMO pre-dispatch price (RRP) forecast for a region, for overlay."""
    region = region.upper()
    if region not in REGIONS:
        raise HTTPException(status_code=400, detail=f"Unknown region '{region}'.")

    rows = await db.get_latest_predispatch_price(region)

    def iso(v):
        return v.isoformat() if hasattr(v, "isoformat") else v

    data = [{"interval_datetime": iso(r["interval_datetime"]), "rrp": r["rrp"]} for r in rows]
    return {
        "region": region,
        "data": data,
        "count": len(data),
        "run_datetime": iso(rows[0]["run_datetime"]) if rows else None,
        "message": f"Latest pre-dispatch price forecast for {region} ({len(data)} intervals)",
    }


@app.get("/api/optimise/dispatch")
async def optimise_dispatch_endpoint(
    region: str,
    power_mw: float = Query(default=100.0, gt=0, description="Battery max power (MW)"),
    energy_mwh: float = Query(default=200.0, gt=0, description="Battery capacity (MWh)"),
    eff_rt: float = Query(default=0.85, gt=0, le=1.0, description="Round-trip efficiency"),
    cycle_cost_per_mwh: float = Query(
        default=0.0, ge=0, description="Degradation cost per MWh discharged ($/MWh)"
    ),
    cyclic: bool = Query(default=True, description="Force end-of-horizon SOC == start"),
):
    """Optimise BESS dispatch against the model's 7-day forecast for `region`."""
    region = region.upper()
    if region not in REGIONS:
        raise HTTPException(status_code=400, detail=f"Unknown region '{region}'.")

    model = _get_forecaster()
    if model is None:
        raise HTTPException(
            status_code=503,
            detail="Price model not trained yet. Run: python -m scripts.train_forecaster",
        )

    try:
        prices = await forecast_price_series(db, region, model)
        if prices.empty:
            raise HTTPException(status_code=503, detail="No forward PASA inputs available.")

        cfg = DispatchInputs(
            power_mw=power_mw,
            energy_mwh=energy_mwh,
            eff_rt=eff_rt,
            cycle_cost_per_mwh=cycle_cost_per_mwh,
            cyclic=cyclic,
        )
        result = optimise_dispatch(prices, cfg)

        schedule = [
            {
                "interval_datetime": row.interval_datetime.isoformat()
                if hasattr(row.interval_datetime, "isoformat")
                else str(row.interval_datetime),
                "charge_mw": round(float(row.charge_mw), 3),
                "discharge_mw": round(float(row.discharge_mw), 3),
                "soc_mwh": round(float(row.soc_mwh), 3),
                "net_mw": round(float(row.net_mw), 3),
                "price": round(float(row.price), 2),
                "revenue": round(float(row.revenue), 2),
            }
            for row in result.schedule.itertuples(index=False)
        ]
        return {
            "region": region,
            "inputs": {
                "power_mw": power_mw,
                "energy_mwh": energy_mwh,
                "duration_h": energy_mwh / power_mw if power_mw else None,
                "eff_rt": eff_rt,
                "cycle_cost_per_mwh": cycle_cost_per_mwh,
                "cyclic": cyclic,
            },
            "total_revenue": round(result.total_revenue, 2),
            "n_cycles": round(result.n_cycles, 3),
            "solver_status": result.solver_status,
            "schedule": schedule,
            "count": len(schedule),
            "message": f"Optimal dispatch for {region}: ${result.total_revenue:,.0f} over {len(schedule)} intervals",
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error optimising dispatch for {region}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/bid-bands")
async def bid_bands_endpoint(
    region: str,
    power_mw: float = Query(default=100.0, gt=0),
    energy_mwh: float = Query(default=200.0, gt=0),
    eff_rt: float = Query(default=0.85, gt=0, le=1.0),
    cycle_cost_per_mwh: float = Query(default=0.0, ge=0),
    cyclic: bool = Query(default=True),
    day_offset: int = Query(
        default=0, ge=0, le=6,
        description="Which day of the 7-day forecast to compute bands for (0 = first day)",
    ),
    grid_mode: str = Query(
        default="kinks",
        description="'kinks' = derive grid from regional bid merit-order density (recommended); 'static' = use a hardcoded NEM-flavoured grid",
    ),
    lookback_days: int = Query(
        default=7, ge=1, le=30,
        description="Window of recent bids to inform the kink grid (ignored if grid_mode='static')",
    ),
):
    """Compute parametric bid bands for one day of the forecast.

    Sweeps the AEMO-flavoured 10-band price grid at each of the 48 30-min
    intervals in the chosen day; the LP sees the full 7-day forecast for
    intertemporal correctness. Each curve is returned both as the raw
    cumulative response (`grid`) and as bid-stack `tranches` (BANDAVAIL MW
    per band) on the discharge (offer) and charge (load) sides.
    """
    region = region.upper()
    if region not in REGIONS:
        raise HTTPException(status_code=400, detail=f"Unknown region '{region}'.")
    model = _get_forecaster()
    if model is None:
        raise HTTPException(status_code=503, detail="Price model not trained yet.")

    try:
        prices = await forecast_price_series(db, region, model)
        if prices.empty:
            raise HTTPException(status_code=503, detail="No forward PASA inputs available.")

        start_offset = day_offset * 48  # 48 × 30-min intervals per day
        if start_offset >= len(prices):
            raise HTTPException(
                status_code=400,
                detail=f"day_offset {day_offset} is beyond the {len(prices)}-interval forecast.",
            )

        cfg = DispatchInputs(
            power_mw=power_mw,
            energy_mwh=energy_mwh,
            eff_rt=eff_rt,
            cycle_cost_per_mwh=cycle_cost_per_mwh,
            cyclic=cyclic,
        )

        if grid_mode == "kinks":
            grid = await derived_grid(db, region, lookback_days)
            if len(grid) < 2:  # fallback if no bids returned
                grid = list(DEFAULT_PRICE_GRID)
        else:
            grid = list(DEFAULT_PRICE_GRID)

        result = await asyncio.to_thread(
            compute_bid_curves, prices, cfg, 48, grid, start_offset
        )

        curves = [
            {
                "interval_datetime": c.interval_datetime.isoformat()
                if hasattr(c.interval_datetime, "isoformat")
                else str(c.interval_datetime),
                "forecast_price": round(c.forecast_price, 2),
                "grid": [
                    {
                        "price": round(p, 2),
                        "discharge_mw": round(d, 3),
                        "charge_mw": round(ch, 3),
                    }
                    for (p, d, ch) in c.grid
                ],
                "discharge_tranches": [round(t, 3) for t in c.discharge_tranches()],
                "charge_tranches": [round(t, 3) for t in c.charge_tranches()],
            }
            for c in result.curves
        ]
        return {
            "region": region,
            "inputs": {
                "power_mw": power_mw,
                "energy_mwh": energy_mwh,
                "duration_h": energy_mwh / power_mw if power_mw else None,
                "eff_rt": eff_rt,
                "cycle_cost_per_mwh": cycle_cost_per_mwh,
                "cyclic": cyclic,
            },
            "day_offset": day_offset,
            "grid_mode": grid_mode,
            "lookback_days": lookback_days if grid_mode == "kinks" else None,
            "horizon_intervals": result.horizon_intervals,
            "price_grid": result.price_grid,
            "n_lp_solves": result.n_lp_solves,
            "curves": curves,
            "message": (
                f"Bid bands for {region} day {day_offset}"
                f" (grid={grid_mode}): "
                f"{result.horizon_intervals} intervals × "
                f"{len(result.price_grid)} bands = {result.n_lp_solves} LP solves"
            ),
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error computing bid bands for {region}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/chat")
async def chat(req: ChatRequest):
    """Conversational NEM analyst — streams SSE (text + tool-call status).

    Body: {"messages": [{"role": "user", "content": "what are prices now?"}, ...]}.
    The agent calls read-only data tools and never reports a figure from memory.
    """
    if not os.getenv("OPENAI_API_KEY"):
        raise HTTPException(
            status_code=503,
            detail="Chat is not configured: set OPENAI_API_KEY on the backend.",
        )
    if not req.messages:
        raise HTTPException(status_code=400, detail="messages must be non-empty.")

    client = _get_openai_client()
    # Copy so the agent's in-place mutation doesn't leak across requests.
    messages = [dict(m) for m in req.messages]

    async def event_source():
        async for ev in nem_agent.stream_chat(client, db, messages):
            yield f"event: {ev['event']}\ndata: {ev['data']}\n\n"

    return StreamingResponse(
        event_source(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


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


@app.get("/api/export/metrics")
async def export_metrics_csv(
    start_date: datetime = Query(..., description="Start date (ISO format)"),
    end_date: datetime = Query(..., description="End date (ISO format)"),
    regions: Optional[str] = Query(default=None, description="Comma-separated regions (e.g., NSW,VIC)")
):
    """Export daily metrics (capture rates, capture prices, TB spreads) as CSV."""
    try:
        region_list = regions.split(',') if regions else None
        df = await db.export_daily_metrics(start_date, end_date, region_list)

        stream = io.StringIO()
        df.to_csv(stream, index=False)
        stream.seek(0)

        timestamp = datetime.now().strftime('%Y%m%d_%H%M')
        filename = f"nem_daily_metrics_{timestamp}.csv"
        response = StreamingResponse(
            iter([stream.getvalue()]),
            media_type="text/csv"
        )
        response.headers["Content-Disposition"] = f"attachment; filename={filename}"
        return response

    except Exception as e:
        logger.error(f"Error exporting daily metrics: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ---- Bid Band Endpoints ----

@app.get("/api/bids/{duid}", response_model=BidBandResponse)
async def get_bid_bands(
    duid: str,
    date: str = Query(..., description="Date in YYYY-MM-DD format")
):
    """Get combined bid band data (price bands + quantity bands) for a DUID on a specific date."""
    try:
        target_date = datetime.strptime(date, '%Y-%m-%d').date()
        duid = duid.upper()

        data = await db.get_bid_bands_for_duid(duid, target_date)

        # Extract price bands from the first record
        price_bands = [None] * 10
        if data:
            for i in range(10):
                price_bands[i] = data[0].get(f'priceband{i+1}')

        # Convert datetimes to AEST ISO format
        for record in data:
            if 'settlementdate' in record and record['settlementdate'] is not None:
                record['settlementdate'] = to_aest_isoformat(record['settlementdate'])

        return BidBandResponse(
            duid=duid,
            date=date,
            data=data,
            count=len(data),
            price_bands=price_bands,
            message=f"Retrieved {len(data)} bid intervals for {duid} on {date}"
        )

    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD.")
    except Exception as e:
        logger.error(f"Error fetching bid bands for {duid}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/duids/search", response_model=DUIDSearchResponse)
async def search_duids(
    q: str = Query(..., description="Search query (DUID or station name)", min_length=2)
):
    """Search for DUIDs by name or station name."""
    try:
        results = await db.search_duids(q)
        return DUIDSearchResponse(
            results=results,
            count=len(results),
            message=f"Found {len(results)} matching DUIDs"
        )
    except Exception as e:
        logger.error(f"Error searching DUIDs: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/ingest/backfill-bids")
async def trigger_bid_backfill(
    start_date: datetime = Query(..., description="Start date (ISO format)"),
    end_date: Optional[datetime] = Query(default=None, description="End date (ISO format, default=yesterday)"),
    background_tasks: BackgroundTasks = None
):
    """Trigger backfill of bid data for a date range."""
    background_tasks.add_task(data_ingester.backfill_bid_data, start_date, end_date)
    return {"message": f"Bid data backfill started from {start_date.date()}"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True)