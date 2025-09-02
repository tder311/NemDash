# NEM Dashboard Backend

FastAPI backend service for the NEM (National Electricity Market) Dashboard that fetches real-time electricity market data from AEMO's NEMWEB portal.

## Features

- **Real-time NEM Data**: Fetches dispatch, price, and interconnector data every 5 minutes
- **SQLite Database**: Local storage with automatic schema management
- **REST API**: Clean endpoints for frontend integration
- **Generator Classification**: Comprehensive generator information with fuel types and regions
- **Async Architecture**: Non-blocking data fetching and processing

## Quick Start

1. **Install dependencies**:
   ```bash
   pip install fastapi uvicorn aiofiles aiohttp pandas sqlite3 python-dotenv
   ```

2. **Run the backend**:
   ```bash
   python run.py
   ```

3. **Import generator data** (optional, for better classification):
   ```bash
   python import_geninfo_csv.py
   ```

The backend will start on `http://localhost:8000` with automatic data fetching every 5 minutes.

## API Endpoints

### Core Data Endpoints

- `GET /api/latest-prices/{price_type}` - Latest regional prices (DISPATCH or TRADING)
- `GET /api/price-history` - Historical price data with date range filtering
- `GET /api/interconnector-flows` - Current interconnector power flows
- `GET /api/generators` - Generator data with region/fuel filtering

### Health & Info

- `GET /health` - Service health check
- `GET /api/data-info` - Database statistics and last update times

## Data Sources

**NEMWEB Portal**: https://nemweb.com.au/
- Dispatch SCADA data (5-minute intervals)
- Trading prices (30-minute intervals) 
- Interconnector flows
- Generator information

## Database Schema

### Core Tables
- `dispatch_data`: Real-time generator output (SCADA values)
- `price_data`: Regional electricity prices
- `interconnector_data`: Power flows between states
- `generator_info`: Generator classifications and metadata

### Key Columns
- `scadavalue`: Current generator output (MW)
- `capacity_mw`: Generator nameplate capacity
- `fuel_source`: Coal, Gas, Wind, Solar, Hydro, Battery, etc.
- `region`: NSW, VIC, QLD, SA, TAS

## Configuration

Environment variables (optional):
```bash
# Server settings
API_PORT=8000
API_HOST=0.0.0.0

# Database
DB_PATH=./data/nem_dispatch.db

# Data fetching
FETCH_INTERVAL=300  # seconds
```

## Data Flow

1. **Fetch**: Downloads CSV files from NEMWEB every 5 minutes
2. **Parse**: Extracts relevant data using pandas
3. **Store**: Updates SQLite database with new records
4. **Serve**: Provides data via REST API endpoints

## Generator Classification

The system includes comprehensive generator classification based on:
- DUID (Dispatchable Unit Identifier)
- Fuel source mapping (Coal → Coal, Solar PV → Solar, etc.)
- Regional assignment (remove '1' suffix from AEMO regions)
- Technology type standardization

Import `data/GenInfo.csv` for authoritative generator metadata:
```bash
python import_geninfo_csv.py
```

## Monitoring

Check service status:
```bash
curl http://localhost:8000/health
curl http://localhost:8000/api/data-info
```

Logs show:
- Data fetch status and record counts
- API request handling
- Database operations
- Error conditions

## Development

**File Structure**:
```
app/
├── database.py          # SQLite database operations
├── nem_client.py        # Core NEMWEB data fetching
├── nem_price_client.py  # Price and interconnector data
└── data_service.py      # Business logic layer

data/
├── nem_dispatch.db      # SQLite database
└── GenInfo.csv          # Generator reference data

run.py                   # Application entry point
import_geninfo_csv.py    # Generator data import utility
```

**Testing**:
- Backend runs on port 8000
- Check `/health` endpoint for status
- Monitor logs for data fetch cycles
- Database updates every 5 minutes

The backend is designed to run continuously, automatically fetching and serving fresh NEM market data.