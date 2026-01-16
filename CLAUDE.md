# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

NEM (National Electricity Market) Dashboard - A full-stack application for visualizing real-time and historical Australian electricity market data from AEMO's NEMWEB portal.

**Architecture**: FastAPI backend + React frontend with SQLite database

## Development Commands

### Backend (FastAPI)
```bash
cd nem-dashboard-backend
python run.py                           # Start backend server (http://localhost:8000)
python import_geninfo_csv.py            # Import generator metadata from CSV
```

### Frontend (React)
```bash
cd nem-dashboard-frontend
npm start                               # Start development server (http://localhost:3000)
npm run build                           # Build production bundle
npm test                                # Run tests
```

### Full Stack Development
1. Start backend first: `cd nem-dashboard-backend && python run.py`
2. Start frontend: `cd nem-dashboard-frontend && npm start`
3. Frontend proxies API requests to backend via `proxy` setting in package.json

## Quick Development Start

### One-Command Start (macOS)
```bash
make dev                    # Opens two Terminal windows for backend and frontend
```

### First-Time Setup
```bash
make check                  # Verify Python and Node.js are installed
make install                # Install all dependencies
make dev                    # Start development servers
```

### Available Make Commands
Run `make help` to see all commands. Key targets:

| Command | Description |
|---------|-------------|
| `make dev` | Start both servers in separate Terminal windows (macOS) |
| `make install` | Install all dependencies |
| `make run-backend` | Start backend only (blocking) |
| `make run-frontend` | Start frontend only (blocking) |
| `make check` | Verify Python and Node.js installed |
| `make check-deps` | Verify all dependencies installed |
| `make health` | Check if servers are running |
| `make build` | Build frontend for production |
| `make test` | Run frontend tests |
| `make clean` | Remove build artifacts |
| `make import-generators` | Import generator metadata from CSV |

## Backend Architecture

### Data Flow Pipeline
1. **NEMDispatchClient** (nem_client.py) - Downloads CSV/ZIP files from NEMWEB
2. **NEMPriceClient** (nem_price_client.py) - Fetches price and interconnector data
3. **DataIngester** (data_ingester.py) - Orchestrates continuous data ingestion (5-min intervals)
4. **NEMDatabase** (database.py) - SQLite operations with async aiosqlite
5. **FastAPI** (main.py) - REST API endpoints with CORS enabled

### Key Database Tables
- `dispatch_data` - Generator SCADA values (5-min intervals)
  - Unique constraint: (settlementdate, duid)
  - Key columns: scadavalue, uigf, totalcleared
- `price_data` - Regional electricity prices (dispatch/trading/public)
  - Unique constraint: (settlementdate, region, price_type)
  - Three price types: DISPATCH (5-min), TRADING (30-min), PUBLIC (historical)
- `interconnector_data` - Power flows between states
  - Unique constraint: (settlementdate, interconnector)
- `generator_info` - Generator metadata (region, fuel_source, capacity_mw)

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
- **Automatic backfill on startup**: Fills missing price data for the last 30 days (configurable via BACKFILL_DAYS_ON_STARTUP)
- Fetches: dispatch data, dispatch prices, trading prices, interconnector flows, and daily public prices

### API Endpoint Patterns

**General Endpoints:**
- `/api/dispatch/latest` - Latest SCADA data (limit parameter)
- `/api/prices/latest?price_type=DISPATCH|TRADING|PUBLIC` - Latest prices by type
- `/api/prices/history?start_date=...&end_date=...&price_type=...` - Historical range queries
- `/api/interconnectors/latest` - Current interconnector flows
- `/api/generators/filter?region=NSW&fuel_source=Coal` - Filtered generator data
- `/api/data/coverage?table=price_data` - Data coverage for backfill planning

**Region-Specific Endpoints (State Drilldown):**
- `/api/region/{region}/generation/current` - Current fuel mix breakdown for a region
- `/api/region/{region}/prices/history?hours=24&price_type=DISPATCH` - Price history for a region
- `/api/region/{region}/summary` - Summary statistics for a region (price, demand, generation)

## Frontend Architecture

### Component Structure
- **App.js** - Main container with dark mode toggle and tab navigation
- **LivePricesPage.js** - Real-time prices with map visualization
  - Fetches trading prices for display
  - Polls every 30 seconds
  - Falls back to empty data on error
  - **Supports state drilldown**: Click on a region to navigate to StateDetailPage
- **StateDetailPage.js** - Detailed view for a specific NEM region
  - Price history chart (Plotly, configurable time range: 6h-7d)
  - Fuel mix donut chart (current generation by fuel type)
  - Summary cards (price, demand, generation, generator count)
  - Fuel breakdown table
  - Auto-refresh every 60 seconds
  - Back button to return to overview
- **PriceHistoryPage.js** - Historical price charts
- **RegionSidebar.js** - Regional price cards with hover and click effects
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

## Environment Configuration

### Backend (.env in nem-dashboard-backend/)
```
HOST=0.0.0.0
PORT=8000
RELOAD=True
LOG_LEVEL=info
DATABASE_PATH=./data/nem_dispatch.db
NEM_API_BASE_URL=https://www.nemweb.com.au
UPDATE_INTERVAL_MINUTES=5
BACKFILL_DAYS_ON_STARTUP=30
```

### Frontend
- Proxy configured in package.json: `"proxy": "http://localhost:8000"`
- No environment variables required for development

## Testing Strategy

### Backend Testing
- Manual API testing via curl or browser
- Check health: `curl http://localhost:8000/health`
- Check data summary: `curl http://localhost:8000/api/summary`
- Monitor logs for data ingestion cycles

### Frontend Testing
- React Testing Library configured
- Test command: `npm test`
- Focus on component rendering and user interactions

## Common Development Patterns

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

## Important Implementation Details

### Database UNIQUE Constraints
All data tables use `ON CONFLICT REPLACE` to handle duplicate records gracefully. When inserting data, duplicates based on unique constraints (settlementdate + identifying column) will replace existing records rather than error.

### Async/Await Throughout Backend
Entire backend is async: aiosqlite for database, httpx for HTTP requests, FastAPI async endpoints. Do not mix sync database operations.

### CORS Configuration
CORS middleware configured to allow frontend origins (localhost:3000, localhost:8050). Add new origins to allow_origins list in main.py if deploying to different ports/domains.

### Price Type Distinctions
- **DISPATCH**: 5-minute interval real-time prices
- **TRADING**: 30-minute interval prices (standard market interval)
- **PUBLIC**: Historical daily price data from NEMWEB archives
Use appropriate price_type when querying price data.

## Generator Classification

Generator metadata enriches dispatch data with fuel source, region, and capacity information. The system includes sample generator data in data_ingester.py. For complete data:

```bash
python import_geninfo_csv.py
```

This imports from `data/GenInfo.csv` which should contain columns: duid, station_name, region, fuel_source, technology_type, capacity_mw.

## Known Limitations

1. Generator info sample data is limited to 10 common units (run `import_geninfo_csv.py` for full dataset)
2. Frontend has no authentication/authorization
3. SQLite database not suitable for high-concurrency production use (designed for easy migration to PostgreSQL)
4. No data retention policy - database grows indefinitely
5. Interconnector data parsing may fail if NEMWEB changes file naming conventions

## Deployment Considerations

- Backend requires Python 3.8+ with asyncio support
- Frontend requires Node.js 14+ for React 18
- Database file location must be writable
- NEMWEB may rate-limit requests - respect 5-minute intervals
- Consider using PostgreSQL for production instead of SQLite
- Add reverse proxy (nginx) for production frontend serving
