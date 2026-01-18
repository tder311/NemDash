# NEM Dashboard Backend

FastAPI backend service for the NEM Dashboard application. Provides REST API endpoints and continuous data ingestion from AEMO's NEMWEB portal.

## Features

- **Real-time Data Ingestion**: Automatic 5-minute fetch cycles from NEMWEB
- **REST API**: 14+ endpoints for dispatch, price, and interconnector data
- **Async Architecture**: Non-blocking I/O with asyncpg and httpx
- **Generator Classification**: Comprehensive generator metadata with fuel types
- **PostgreSQL Database**: Production-ready database with connection pooling

## Quick Start

### Prerequisites

- Python 3.8+
- PostgreSQL 15+ (or Docker)

### Start PostgreSQL with Docker

```bash
# Start PostgreSQL container
docker-compose up -d

# Wait for PostgreSQL to be ready
docker-compose exec postgres pg_isready -U postgres
```

### Install and Run

```bash
# Install dependencies
pip install -r requirements.txt

# Configure environment
cp .env.example .env
# Edit .env if needed (default works with docker-compose)

# Start the server
python run.py
```

The server starts at http://localhost:8000 with automatic data ingestion enabled.

## Project Structure

```
nem-dashboard-backend/
├── app/
│   ├── __init__.py           # Package initialization
│   ├── main.py               # FastAPI application, routes, lifecycle
│   ├── database.py           # PostgreSQL database operations (NEMDatabase)
│   ├── models.py             # Pydantic response schemas
│   ├── nem_client.py         # Dispatch data client (NEMDispatchClient)
│   ├── nem_price_client.py   # Price/interconnector client (NEMPriceClient)
│   └── data_ingester.py      # Data pipeline orchestration (DataIngester)
├── data/
│   └── GenInfo.csv           # Generator metadata (optional)
├── scripts/
│   ├── setup_postgres.sh     # PostgreSQL setup script
│   └── migrate_to_postgres.py# Migration from SQLite (if needed)
├── run.py                    # Application entry point
├── import_geninfo_csv.py     # Generator data import utility
├── requirements.txt          # Python dependencies
├── docker-compose.yml        # PostgreSQL container config
└── .env.example              # Environment configuration template
```

## Configuration

Create a `.env` file (or copy from `.env.example`):

```bash
# Server settings
HOST=0.0.0.0
PORT=8000
RELOAD=True
LOG_LEVEL=info

# Database (PostgreSQL required)
DATABASE_URL=postgresql://postgres:localdev@localhost:5432/nem_dashboard

# NEMWEB API
NEM_API_BASE_URL=https://www.nemweb.com.au

# Data ingestion interval (minutes)
UPDATE_INTERVAL_MINUTES=5

# Days to backfill on startup
BACKFILL_DAYS_ON_STARTUP=30
```

## API Endpoints

### Health & Status
| Endpoint | Method | Description |
|----------|--------|-------------|
| `/` | GET | Root health check |
| `/health` | GET | Detailed health status |
| `/api/summary` | GET | Database statistics |

### Dispatch Data
| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/dispatch/latest` | GET | Latest SCADA data (limit param) |
| `/api/dispatch/range` | GET | Date range queries (optional duid filter) |

### Price Data
| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/prices/latest` | GET | Latest prices by type (DISPATCH/TRADING/PUBLIC) |
| `/api/prices/history` | GET | Historical prices (date range, optional region) |

### Interconnectors
| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/interconnectors/latest` | GET | Latest interconnector flows |
| `/api/interconnectors/history` | GET | Historical flows (date range) |

### Generators
| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/generators/filter` | GET | Filter by region and/or fuel_source |
| `/api/duids` | GET | List all generator DUIDs |
| `/api/generation/by-fuel` | GET | Aggregated generation by fuel type |

### Manual Ingestion
| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/ingest/current` | POST | Trigger current data fetch |
| `/api/ingest/historical` | POST | Fetch historical dispatch data |
| `/api/ingest/historical-prices` | POST | Fetch historical prices |

See [../docs/API.md](../docs/API.md) for complete API documentation with request/response examples.

## Core Modules

### main.py - FastAPI Application

- FastAPI app with async lifespan management
- CORS middleware for frontend origins
- Background task for continuous data ingestion
- All REST API endpoint definitions

```python
# Startup: Initialize DB, load sample generators, start ingestion
# Shutdown: Gracefully stop ingestion task
@asynccontextmanager
async def lifespan(app: FastAPI):
    await db.initialize()
    await update_sample_generator_info()
    task = asyncio.create_task(data_ingester.run_continuous_ingestion())
    yield
    data_ingester.stop_continuous_ingestion()
```

### database.py - NEMDatabase Class

Async PostgreSQL operations with `asyncpg`:

| Method | Description |
|--------|-------------|
| `initialize()` | Create tables with proper schema and indexes |
| `insert_dispatch_data(df)` | Batch insert dispatch SCADA records |
| `insert_price_data(df)` | Insert price records (any type) |
| `insert_interconnector_data(df)` | Insert interconnector flow records |
| `get_latest_dispatch_data(limit)` | Query most recent dispatch data |
| `get_dispatch_data_by_date_range()` | Range queries with optional DUID filter |
| `get_generation_by_fuel_type()` | Aggregation with generator_info JOIN |
| `get_latest_prices(price_type)` | Latest prices by type |
| `get_price_history()` | Historical price range queries |
| `get_data_summary()` | Database statistics |
| `update_generator_info()` | Upsert generator metadata |

### nem_client.py - NEMDispatchClient

Fetches dispatch SCADA data from NEMWEB:

```python
get_current_dispatch_data()    # Latest 5-min data from /REPORTS/CURRENT/Dispatch_SCADA/
get_historical_dispatch_data() # Archives from /Reports/Archive/Dispatch_SCADA/{year}/
```

### nem_price_client.py - NEMPriceClient

Fetches price and interconnector data:

| Method | Source | Data Type |
|--------|--------|-----------|
| `get_current_dispatch_prices()` | DispatchIS_Reports | 5-min prices |
| `get_trading_prices()` | TradingIS_Reports | 30-min prices |
| `get_daily_prices(date)` | Public_Prices | Daily PUBLIC prices |
| `get_interconnector_flows()` | Dispatch_IRSR | Power flows |

### data_ingester.py - DataIngester

Orchestrates continuous data ingestion:

```python
run_continuous_ingestion(interval=5)  # Main loop every 5 minutes
ingest_current_data()                  # Fetches dispatch, prices, flows
ingest_historical_data(start, end)     # Batch historical dispatch
ingest_historical_prices(start, end)   # Batch historical prices
```

## Database Schema

### dispatch_data
- Unique: (settlementdate, duid) ON CONFLICT DO UPDATE
- Columns: id, settlementdate, duid, scadavalue, uigf, totalcleared, ramprate, availability, raise1sec, lower1sec, created_at

### price_data
- Unique: (settlementdate, region, price_type) ON CONFLICT DO UPDATE
- Columns: id, settlementdate, region, price, totaldemand, price_type, created_at

### interconnector_data
- Unique: (settlementdate, interconnector) ON CONFLICT DO UPDATE
- Columns: id, settlementdate, interconnector, meteredmwflow, mwflow, mwloss, marginalvalue, created_at

### generator_info
- Primary key: duid
- Columns: duid, station_name, region, fuel_source, technology_type, capacity_mw, updated_at

## NEM Data Format

NEMWEB delivers data as ZIP files containing CSV:

```
# Record format (filter rows starting with "D,")
D,DISPATCH,UNIT_SCADA,1,"2025/01/15 10:30:00",BASTYAN,82.5,"2025/01/15 10:30:05"
```

- Settlement dates: "YYYY/MM/DD HH:MM:SS" format
- Data rows prefixed with "D,"
- ZIP archives containing single CSV file

## Generator Data Import

Import authoritative generator metadata:

```bash
python import_geninfo_csv.py
```

Requirements:
- Place `GenInfo.csv` in `data/` directory
- Required columns: duid, station_name, region, fuel_source, technology_type, capacity_mw

## Development

### Testing

```bash
# Set DATABASE_URL for tests
export DATABASE_URL=postgresql://postgres:localdev@localhost:5432/nem_dashboard

# Run tests
pytest tests/
```

### Testing Endpoints

```bash
# Health check
curl http://localhost:8000/health

# Database summary
curl http://localhost:8000/api/summary

# Latest trading prices
curl "http://localhost:8000/api/prices/latest?price_type=TRADING"

# Latest dispatch (limited)
curl "http://localhost:8000/api/dispatch/latest?limit=10"

# Filtered generators
curl "http://localhost:8000/api/generators/filter?region=NSW&fuel_source=Coal"
```

### Adding New Endpoints

1. Define Pydantic model in `models.py`
2. Add database method in `database.py`
3. Create endpoint in `main.py` with error handling

### Adding New Data Sources

1. Create fetch method in `nem_client.py` or `nem_price_client.py`
2. Parse NEM CSV format (filter "D," rows)
3. Add database table/method if needed
4. Update `DataIngester.ingest_current_data()`
5. Create API endpoint

## CORS Configuration

Allowed origins in `main.py`:
- `http://localhost:3000` - React dev server
- `http://localhost:8050` - Alternative dev port

Add production domains to `allow_origins` list.

## Dependencies

```
fastapi>=0.104.0      # Web framework
uvicorn>=0.24.0       # ASGI server
asyncpg>=0.29.0       # Async PostgreSQL
httpx>=0.25.0         # Async HTTP client
pandas>=2.0.0         # Data processing
python-dotenv>=1.0.0  # Environment config
pydantic>=2.0.0       # Data validation
```

## Troubleshooting

| Issue | Solution |
|-------|----------|
| No data appearing | Check NEMWEB connectivity, verify ingestion logs |
| Connection refused | Ensure PostgreSQL is running (`docker-compose up -d`) |
| Stale data | Check ingestion task, manually trigger `/api/ingest/current` |
| Memory issues | Process smaller date ranges for historical imports |
