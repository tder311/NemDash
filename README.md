# NEM Dashboard

A full-stack application for visualizing real-time and historical Australian electricity market data from AEMO's NEMWEB portal.

## Overview

The National Electricity Market (NEM) Dashboard provides real-time monitoring and historical analysis of Australia's electricity market across five regions: New South Wales (NSW), Victoria (VIC), Queensland (QLD), South Australia (SA), and Tasmania (TAS).

### Key Features

- **Live Prices**: Real-time electricity prices updated every 30 seconds with interactive map visualization
- **State Drilldown**: Click any region for detailed price history, fuel mix, and generation data
- **Extended Time Ranges**: View data from 6 hours to 365 days with automatic aggregation
- **Price History**: Historical price charts with multi-region comparison
- **Market Metrics**: TB spreads, capture prices, and price-setter frequency by region
- **Generator Data**: SCADA dispatch data with fuel source classification
- **Dark Mode**: Full dark/light theme support

## Architecture

**Stack**: FastAPI backend + React frontend + PostgreSQL database

Continuous NEMWEB ingestion runs as its own process (`run_worker.py`), separate
from the API that serves requests, so ingestion never competes with request
serving. In production these are three deployables sharing one Postgres:

- **Ingestion worker** (`run_worker.py`) — the sole continuous writer:
  polls NEMWEB on a loop and applies raw-data retention.
- **NemDash API** (this repo's `app/main.py`) — reads Postgres for the
  dashboard SPA; also still constructs a `DataIngester` for manual
  `/api/ingest/*` backfill endpoints (a known exception to single-writer,
  documented in code).
- **Forecasting API** (future, separate repo) — reads Postgres for
  forecast/optimiser/bidding endpoints; never writes.

```
┌───────────────────────────────┐        ┌──────────────────────────────┐
│      Ingestion Worker          │        │        NemDash API           │
│    (run_worker.py, own         │───────▶│  (app/main.py, FastAPI)      │
│     process/service)           │ writes │  http://localhost:8000       │
│  • NEMDispatchClient/          │        │  • REST API (market-watching │
│    NEMPriceClient/... (poll)   │        │    endpoints)                │
│  • DataIngester                │        │  • Manual /api/ingest/*      │
│    .run_continuous_ingestion() │        │    backfills (also writes)   │
└───────────────┬────────────────┘        └───────────────┬──────────────┘
                │ writes                                   │ reads
                ▼                                          ▼
┌───────────────────────────────────────────────────────────────────────┐
│                          PostgreSQL Database                          │
└───────────────────────────────────────────────────────────────────────┘
                ▲                                          ▲
                │ writes                                   │ reads only
                │                                          │
┌───────────────┴────────────────┐        ┌───────────────┴──────────────┐
│         AEMO NEMWEB             │        │   Forecasting API (future,   │
│   https://www.nemweb.com.au     │        │   separate repo)             │
│  • Dispatch SCADA (5-min)       │        │  • Own SPA + own FastAPI     │
│  • Trading Prices (30-min)      │        │  • Never writes, never       │
│  • Interconnector Flows         │        │    imports NemDash code      │
│  • Public Price Archives        │        └───────────────────────────────┘
└───────────────────────────────────┘

Frontend (React, http://localhost:3000) talks to the NemDash API above.
```

## Quick Start

### Prerequisites

- Python 3.8+ with pip
- Node.js 14+ with npm
- PostgreSQL 15+ (or Docker)
- Git

### Installation

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd curitiba
   ```

2. **Start PostgreSQL** (using Docker)
   ```bash
   cd nem-dashboard-backend
   docker-compose up -d
   ```

3. **Verify environment and install dependencies**
   ```bash
   make check      # Verify Python and Node.js are installed
   make install    # Install all dependencies (backend + frontend)
   ```

   Or install manually:
   ```bash
   # Backend
   cd nem-dashboard-backend
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   pip install -r requirements.txt
   cp .env.example .env

   # Frontend
   cd ../nem-dashboard-frontend
   npm install
   ```

### Running the Application

**One-Command Start (macOS):**
```bash
make dev
```
This opens two Terminal windows - one for backend (localhost:8000) and one for frontend (localhost:3000).

**Manual Start (any platform):**

Terminal 1 - Backend:
```bash
make run-backend
# Or: cd nem-dashboard-backend && python run.py
```

Terminal 2 - Frontend:
```bash
make run-frontend
# Or: cd nem-dashboard-frontend && npm start
```

### Verify Installation

```bash
# Check if servers are running
make health

# Or manually:
curl http://localhost:8000/health
curl http://localhost:8000/api/summary
```

### All Available Commands

Run `make help` to see all available commands:

```
Setup:       make install, make install-backend, make install-frontend
Development: make dev, make run-backend, make run-frontend
Database:    make db, make db-stop
Verification: make check, make check-deps, make health
Build:       make build, make test, make clean
```

## Project Structure

```
curitiba/
├── README.md                      # This file
├── Makefile                       # Development commands
├── scripts/
│   └── dev.sh                     # macOS script to open two Terminal windows
├── nem-dashboard-backend/
│   ├── README.md                  # Backend documentation
│   ├── docker-compose.yml         # PostgreSQL container config
│   ├── app/
│   │   ├── main.py               # FastAPI app & endpoints
│   │   ├── database.py           # PostgreSQL operations (asyncpg)
│   │   ├── models.py             # Pydantic schemas
│   │   ├── nem_client.py         # Dispatch data client
│   │   ├── nem_price_client.py   # Price/flow client
│   │   └── data_ingester.py      # Data pipeline
│   ├── scripts/
│   │   ├── migrate_to_postgres.py # SQLite migration tool
│   │   └── setup_postgres.sh      # PostgreSQL setup script
│   ├── run.py                    # API entry point
│   ├── run_worker.py             # Ingestion worker entry point (own process)
│   ├── import_geninfo_csv.py     # Generator data import
│   ├── requirements.txt          # Python dependencies
│   └── data/                     # Reference data (GenInfo.csv)
└── nem-dashboard-frontend/
    ├── README.md                  # Frontend documentation
    ├── src/
    │   ├── App.js                # Main container
    │   └── components/           # React components
    ├── public/
    │   └── australia-map.svg     # Map visualization
    └── package.json              # Node dependencies
```

## Configuration

### Backend Environment Variables

Create a `.env` file in `nem-dashboard-backend/`:

| Variable | Default | Description |
|----------|---------|-------------|
| `HOST` | `0.0.0.0` | Server bind address |
| `PORT` | `8000` | Server port |
| `RELOAD` | `True` | Auto-reload on code changes |
| `LOG_LEVEL` | `info` | Logging level |
| `DATABASE_URL` | `postgresql://postgres:localdev@localhost:5432/nem_dashboard` | PostgreSQL connection URL |
| `NEM_API_BASE_URL` | `https://www.nemweb.com.au` | NEMWEB base URL |
| `UPDATE_INTERVAL_MINUTES` | `5` | Data fetch interval |
| `BACKFILL_DAYS_ON_STARTUP` | `30` | Days to backfill on startup |

### Frontend Configuration

The frontend uses a proxy configuration in `package.json` to route API requests to the backend:
```json
"proxy": "http://localhost:8000"
```

No additional environment variables are required for development.

---

## Backend Architecture

### Data Flow Pipeline

1. **NEMDispatchClient** (nem_client.py) - Downloads CSV/ZIP files from NEMWEB
2. **NEMPriceClient** (nem_price_client.py) - Fetches price and interconnector data
3. **DataIngester** (data_ingester.py) - Orchestrates continuous data ingestion (5-min intervals)
4. **NEMDatabase** (database.py) - PostgreSQL operations with async asyncpg
5. **FastAPI** (main.py) - REST API endpoints with CORS enabled

### Key Database Tables

- **dispatch_data** - Generator SCADA values (5-min intervals)
  - Unique constraint: (settlementdate, duid)
  - Key columns: scadavalue, uigf, totalcleared
- **price_data** - Regional electricity prices (dispatch/trading/public)
  - Unique constraint: (settlementdate, region, price_type)
  - Three price types: DISPATCH (5-min), TRADING (30-min), PUBLIC (historical)
- **interconnector_data** - Power flows between states
  - Unique constraint: (settlementdate, interconnector)
- **generator_info** - Generator metadata (region, fuel_source, capacity_mw)

### NEM Data Format Parsing

The NEMWEB CSV format is non-standard:
- Files are ZIP-compressed with CSV inside
- CSV records prefixed with record types (e.g., "D,DISPATCH,UNIT_SCADA")
- Parse by filtering specific record types, not standard CSV headers
- Settlement dates in format: "YYYY/MM/DD HH:MM:SS"
- Example: `D,DISPATCH,UNIT_SCADA,1,"2025/01/15 10:30:00",DUID,123.45,"2025/01/15 10:30:05"`

### Background Data Ingestion

- `lifespan` context manager in main.py handles startup/shutdown
- `DataIngester.run_continuous_ingestion()` runs as asyncio background task
- Default 5-minute update interval (configurable via UPDATE_INTERVAL_MINUTES env var)
- **Automatic backfill on startup**: Fills missing price data for the last 30 days
- Fetches: dispatch data, dispatch prices, trading prices, interconnector flows, and daily public prices

### API Endpoints

**Health & Status:**
- `GET /` - Root endpoint (returns API name and version)
- `GET /health` - Health check (returns status, database connection, timestamp)

**Dispatch Data:**
- `GET /api/dispatch/latest` - Latest SCADA data (limit parameter)
- `GET /api/dispatch/range?start_date=...&end_date=...&duid=...` - Dispatch data for date range

**Price Data:**
- `GET /api/prices/latest?price_type=DISPATCH|TRADING|PUBLIC` - Latest prices by type
- `GET /api/prices/history?start_date=...&end_date=...&region=...&price_type=...` - Historical range queries

**Interconnector Data:**
- `GET /api/interconnectors/latest` - Current interconnector flows
- `GET /api/interconnectors/history?start_date=...&end_date=...&interconnector=...` - Interconnector flow history

**Generator Data:**
- `GET /api/generators/filter?region=NSW&fuel_source=Coal` - Filtered generator data
- `GET /api/duids` - List of all unique generator DUIDs

**Analysis & Summary:**
- `GET /api/generation/by-fuel?start_date=...&end_date=...` - Aggregated generation by fuel type
- `GET /api/summary` - Database summary statistics
- `GET /api/data/coverage?table=price_data` - Data coverage for backfill planning

**Data Ingestion (Manual Triggers):**
- `POST /api/ingest/current` - Trigger current data ingestion
- `POST /api/ingest/historical?start_date=...&end_date=...` - Trigger historical dispatch ingestion
- `POST /api/ingest/historical-prices?start_date=...&end_date=...` - Trigger historical price ingestion

**Region-Specific Endpoints (State Drilldown):**
- `GET /api/region/{region}/generation/current` - Current fuel mix breakdown for a region
- `GET /api/region/{region}/generation/history?hours=24` - Generation history with auto-aggregation
- `GET /api/region/{region}/prices/history?hours=24&price_type=MERGED` - Price history for a region
- `GET /api/region/{region}/summary` - Summary statistics for a region (price, demand, generation)

### Price Type Distinctions

- **DISPATCH**: 5-minute interval real-time prices
- **TRADING**: 30-minute interval prices (standard market interval)
- **PUBLIC**: Historical daily price data from NEMWEB archives
- **MERGED**: Combines PUBLIC (where available) with DISPATCH (for recent gaps)

---

## Frontend Architecture

### Component Structure

- **App.js** - Main container with dark mode toggle and tab navigation
- **LivePricesPage.js** - Real-time prices with map visualization
  - Fetches trading prices for display
  - Polls every 30 seconds
  - Falls back to empty data on error
  - **Supports state drilldown**: Click on a region to navigate to StateDetailPage
- **StateDetailPage.js** - Detailed view for a specific NEM region
  - Price history chart (Plotly, configurable time range: 6h-365d)
  - Fuel mix donut chart (current generation by fuel type)
  - Summary cards (price, demand, generation, generator count)
  - Fuel breakdown table
  - Auto-refresh every 60 seconds
  - Back button to return to overview
- **PriceHistoryPage.js** - Historical price charts
- **RegionSidebar.js** - Regional price cards with hover and click effects
- **RegionCard.js** - Individual region card component (price display, styling)
- **AustraliaMap.js** - SVG map with region highlighting and click navigation
- **InterconnectorFlow.js** - Power flow visualization

### State Management

- Local component state with useState/useEffect hooks
- No global state management (Redux/Context not used)
- Dark mode prop passed from App.js to child components

### Data Fetching Pattern

```javascript
// Standard pattern used throughout
const fetchData = async () => {
  try {
    const response = await axios.get('/api/endpoint');
    setData(response.data.data || []);
    setLastUpdated(new Date().toLocaleTimeString());
  } catch (error) {
    console.error('Error:', error);
    // Always provide fallback sample data
    setData(sampleData);
  }
};
```

### Styling Conventions

- CSS modules per component (e.g., LivePricesPage.css)
- Dark mode support via className conditionals: `className={darkMode ? 'dark' : 'light'}`
- Consistent color scheme for NEM regions:
  - NSW: Blue (#1f77b4)
  - VIC: Orange (#ff7f0e)
  - QLD: Green (#2ca02c)
  - SA: Red (#d62728)
  - TAS: Purple (#9467bd)

---

## Data Aggregation

Extended time range queries use automatic aggregation to maintain performance:

| Time Range | Aggregation Level |
|------------|-------------------|
| < 48 hours | 5 min (raw data) |
| 48h - 7 days | 30 min averages |
| 7d - 30 days | Hourly averages |
| 30d - 90 days | Daily averages |
| > 90 days | Weekly averages |

---

## Development Patterns

### Adding New API Endpoint

1. Define response model in `app/models.py`
2. Add database query method in `database.py`
3. Create endpoint in `main.py` with proper error handling
4. Follow existing pattern: async def, HTTPException for errors, log errors

### Adding New NEM Data Source

1. Create client method in nem_client.py or nem_price_client.py
2. Parse CSV/ZIP following NEM format conventions
3. Add database table/insertion method if new table needed
4. Update DataIngester.ingest_current_data() to fetch new data source
5. Create corresponding API endpoint in main.py

### Adding Frontend Component

1. Create component file in src/components/
2. Create matching CSS file
3. Follow useState/useEffect pattern for data fetching
4. Add darkMode prop support
5. Provide error fallback with sample data

---

## Implementation Details

### Database UNIQUE Constraints

All data tables use `ON CONFLICT DO UPDATE` to handle duplicate records gracefully. When inserting data, duplicates based on unique constraints (settlementdate + identifying column) will update existing records rather than error.

### Async/Await Throughout Backend

Entire backend is async: asyncpg for database, httpx for HTTP requests, FastAPI async endpoints. Do not mix sync database operations.

### CORS Configuration

CORS middleware configured to allow frontend origins (localhost:3000, localhost:8050). Add new origins to allow_origins list in main.py if deploying to different ports/domains.

---

## Generator Classification

Generator metadata enriches dispatch data with fuel source, region, and capacity information. The system includes sample generator data in data_ingester.py. For complete data:

```bash
python import_geninfo_csv.py
```

This imports from `data/GenInfo.csv` which should contain columns: duid, station_name, region, fuel_source, technology_type, capacity_mw.

---

## Testing

### Backend Testing

```bash
cd nem-dashboard-backend
export DATABASE_URL=postgresql://postgres:localdev@localhost:5432/nem_dashboard
pytest tests/
```

Manual API testing:
```bash
curl http://localhost:8000/health
curl http://localhost:8000/api/summary
curl http://localhost:8000/api/prices/latest
```

### Frontend Testing

```bash
cd nem-dashboard-frontend
npm test
```

---

## Known Limitations

1. Generator info sample data is limited to 10 common units (run `import_geninfo_csv.py` for full dataset)
2. Frontend has no authentication/authorization
3. No data retention policy - database grows indefinitely
4. Interconnector data parsing may fail if NEMWEB changes file naming conventions

---

## Deployment Considerations

- Backend requires Python 3.8+ with asyncio support
- Frontend requires Node.js 14+ for React 18
- PostgreSQL 15+ required (use docker-compose for local development)
- NEMWEB may rate-limit requests - respect 5-minute intervals
- Add reverse proxy (nginx) for production frontend serving
- Configure proper CORS origins in main.py for production domains
- Continuous ingestion runs as its own deployed process (`python run_worker.py`),
  separate from the API service (`app/main.py`) — see Architecture above

---

## License

[Add license information]

## Contributing

[Add contribution guidelines]

## Support

For issues or questions, please [add support information].
