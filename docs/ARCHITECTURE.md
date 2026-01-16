# NEM Dashboard Architecture

This document provides a detailed overview of the NEM Dashboard system architecture, data flow, and component interactions.

## Table of Contents

- [System Overview](#system-overview)
- [Backend Architecture](#backend-architecture)
- [Frontend Architecture](#frontend-architecture)
- [Data Pipeline](#data-pipeline)
- [Database Design](#database-design)
- [API Design](#api-design)
- [Security Considerations](#security-considerations)
- [Scalability & Performance](#scalability--performance)

---

## System Overview

### High-Level Architecture

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                              NEM Dashboard                                    │
│                                                                              │
│  ┌─────────────────────────┐         ┌─────────────────────────────────────┐ │
│  │     React Frontend      │         │         FastAPI Backend             │ │
│  │    (localhost:3000)     │  HTTP   │        (localhost:8000)             │ │
│  │                         │◄───────►│                                     │ │
│  │  • LivePricesPage       │   REST  │  • REST API Endpoints               │ │
│  │  • PriceHistoryPage     │   API   │  • Async Data Processing            │ │
│  │  • Interactive Map      │         │  • Background Ingestion Task        │ │
│  │  • Region Cards         │         │                                     │ │
│  └─────────────────────────┘         └──────────────┬──────────────────────┘ │
│                                                     │                        │
└─────────────────────────────────────────────────────┼────────────────────────┘
                                                      │
                     ┌────────────────────────────────┼────────────────────────┐
                     │                                │                        │
                     ▼                                ▼                        │
           ┌─────────────────┐             ┌─────────────────────┐             │
           │  SQLite Database │             │    AEMO NEMWEB      │             │
           │                  │             │                     │             │
           │  • dispatch_data │ ◄──────────│  • Dispatch SCADA   │             │
           │  • price_data    │    Fetch    │  • Trading Prices   │             │
           │  • interconnector│    Every    │  • Dispatch Prices  │             │
           │  • generator_info│    5 min    │  • Interconnector   │             │
           └─────────────────┘             └─────────────────────┘             │
```

### Technology Stack

| Layer | Technology | Purpose |
|-------|------------|---------|
| Frontend | React 18 | UI components and state management |
| Visualization | Plotly.js | Interactive charts |
| HTTP Client | Axios | API communication |
| Backend | FastAPI | REST API framework |
| Async Runtime | asyncio | Non-blocking I/O |
| HTTP Client | httpx | Async HTTP requests to NEMWEB |
| Database | SQLite + aiosqlite | Async database operations |
| Data Processing | Pandas | CSV parsing and data manipulation |

---

## Backend Architecture

### Module Structure

```
nem-dashboard-backend/
├── app/
│   ├── __init__.py           # Package initialization
│   ├── main.py               # FastAPI application & endpoints
│   ├── database.py           # Database operations (NEMDatabase)
│   ├── models.py             # Pydantic response schemas
│   ├── nem_client.py         # Dispatch data client (NEMDispatchClient)
│   ├── nem_price_client.py   # Price/flow client (NEMPriceClient)
│   └── data_ingester.py      # Data pipeline orchestration
├── run.py                    # Application entry point
└── import_geninfo_csv.py     # Generator metadata import utility
```

### Component Responsibilities

#### main.py - FastAPI Application

```python
# Key responsibilities:
# 1. Application lifecycle management (startup/shutdown)
# 2. CORS middleware configuration
# 3. Route definitions for all API endpoints
# 4. Background task orchestration
# 5. Error handling and logging

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: Initialize database, start ingestion
    await db.initialize()
    await update_sample_generator_info()
    ingestion_task = asyncio.create_task(
        data_ingester.run_continuous_ingestion()
    )
    yield
    # Shutdown: Stop ingestion gracefully
    data_ingester.stop_continuous_ingestion()
```

#### database.py - NEMDatabase Class

```python
class NEMDatabase:
    """Async SQLite database operations using aiosqlite."""

    # Key methods:
    async def initialize()           # Create tables with proper schema
    async def insert_dispatch_data() # Batch insert dispatch records
    async def insert_price_data()    # Insert price records by type
    async def insert_interconnector_data()  # Insert flow records
    async def get_latest_dispatch_data()    # Query latest SCADA
    async def get_dispatch_data_by_date_range()  # Range queries
    async def get_generation_by_fuel_type()  # Aggregation with JOIN
    async def get_data_summary()     # Database statistics
```

#### nem_client.py - NEMDispatchClient Class

```python
class NEMDispatchClient:
    """Fetches dispatch SCADA data from NEMWEB."""

    # Data sources:
    # - Current: /REPORTS/CURRENT/Dispatch_SCADA/
    # - Archive: /Reports/Archive/Dispatch_SCADA/{year}/

    async def get_current_dispatch_data()    # Latest 5-min data
    async def get_historical_dispatch_data() # Date-specific archives
```

#### nem_price_client.py - NEMPriceClient Class

```python
class NEMPriceClient:
    """Fetches price and interconnector data from NEMWEB."""

    # Data sources:
    # - Dispatch prices: /Reports/Current/DispatchIS_Reports/
    # - Trading prices: /Reports/Current/TradingIS_Reports/
    # - Public prices: /Reports/Current/Public_Prices/
    # - Interconnector: /Reports/Current/Dispatch_IRSR/

    async def get_current_dispatch_prices()  # 5-min prices
    async def get_trading_prices()           # 30-min prices
    async def get_daily_prices()             # Daily PUBLIC prices
    async def get_interconnector_flows()     # Power flows
```

#### data_ingester.py - DataIngester Class

```python
class DataIngester:
    """Orchestrates continuous data ingestion from NEMWEB."""

    async def run_continuous_ingestion(interval_minutes=5):
        while self.is_running:
            await self.ingest_current_data()
            await asyncio.sleep(interval_minutes * 60)

    async def ingest_current_data():
        # Fetches: dispatch, dispatch prices, trading prices,
        #          daily prices, interconnector flows
        # Inserts all data into respective database tables

    async def ingest_historical_data(start_date, end_date):
        # Iterates through date range fetching archived data
```

### Request-Response Flow

```
Client Request
      │
      ▼
┌─────────────────┐
│  FastAPI Router │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Path Operation │  (e.g., get_latest_prices)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   NEMDatabase   │  (async database query)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  SQLite + Pandas│  (query execution, DataFrame conversion)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Pydantic Schema │  (response serialization)
└────────┬────────┘
         │
         ▼
   JSON Response
```

---

## Frontend Architecture

### Component Hierarchy

```
App.js
├── State: darkMode, activeTab
├── Header (title, dark mode toggle)
├── Tab Navigation
│
├── [activeTab === 'live']
│   └── LivePricesPage.js
│       ├── State: prices, lastUpdated, loading, hoveredRegion
│       ├── useEffect: fetchPrices (30s interval)
│       │
│       ├── RegionSidebar.js
│       │   ├── Props: regions, darkMode, onRegionHover
│       │   └── Renders region cards with prices
│       │
│       └── AustraliaMap.js
│           ├── Props: hoveredRegion, darkMode
│           └── Renders SVG map with highlighting
│
└── [activeTab === 'history']
    └── PriceHistoryPage.js
        ├── State: priceData, loading
        ├── useEffect: fetchHistory
        │
        └── Plotly Chart
            └── 5 line series (one per region)
```

### State Management Pattern

The application uses local component state exclusively (no Redux/Context):

```javascript
// Data fetching pattern used throughout
const [data, setData] = useState([]);
const [loading, setLoading] = useState(true);
const [lastUpdated, setLastUpdated] = useState(null);

useEffect(() => {
  const fetchData = async () => {
    try {
      const response = await axios.get('/api/endpoint');
      setData(response.data.data || []);
      setLastUpdated(new Date().toLocaleTimeString());
    } catch (error) {
      console.error('Error:', error);
      setData(fallbackData);  // Graceful degradation
    } finally {
      setLoading(false);
    }
  };

  fetchData();
  const interval = setInterval(fetchData, 30000);  // Polling
  return () => clearInterval(interval);
}, []);
```

### Data Flow

```
                    ┌─────────────────────────────────────────────────┐
                    │                    App.js                        │
                    │  ┌─────────────────────────────────────────────┐ │
                    │  │  darkMode: boolean                          │ │
                    │  │  activeTab: 'live' | 'history'              │ │
                    │  └─────────────────────────────────────────────┘ │
                    └───────────────────┬─────────────────────────────┘
                                        │ props: darkMode
                    ┌───────────────────┼───────────────────┐
                    │                   │                   │
                    ▼                   ▼                   ▼
          ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐
          │ LivePricesPage  │  │ RegionSidebar   │  │ AustraliaMap    │
          │                 │  │                 │  │                 │
          │ prices: []      │──│ regions: []     │  │ hoveredRegion   │
          │ hoveredRegion   │◄─│ onRegionHover   │──│                 │
          └────────┬────────┘  └─────────────────┘  └─────────────────┘
                   │
                   │ axios.get('/api/prices/latest')
                   ▼
          ┌─────────────────┐
          │  Backend API    │
          └─────────────────┘
```

### Styling Architecture

```
CSS Structure:
├── index.css          # Global styles, CSS variables, body styling
├── App.css            # App container, header, navigation
└── components/
    ├── LivePricesPage.css      # Live page layout, animations
    ├── PriceHistoryPage.css    # Chart container styles
    ├── RegionSidebar.css       # Sidebar, card hover effects
    ├── RegionCard.css          # Individual card styling
    └── InterconnectorFlow.css  # Flow line animations
```

**Dark Mode Implementation:**
```javascript
// App.js
<div className={`app ${darkMode ? 'dark' : 'light'}`}>

// Component
<div className={`component ${darkMode ? 'dark' : 'light'}`}>
```

```css
/* Light mode (default) */
.component.light {
  background: #ffffff;
  color: #333333;
}

/* Dark mode */
.component.dark {
  background: #1a1a1a;
  color: #f5f5f5;
}
```

---

## Data Pipeline

### Continuous Ingestion Flow

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        DataIngester.run_continuous_ingestion()               │
│                                                                             │
│   ┌─────────────────┐                                                       │
│   │  while running  │◄──────────────────────────────────────────────────┐   │
│   └────────┬────────┘                                                   │   │
│            │                                                            │   │
│            ▼                                                            │   │
│   ┌─────────────────────────────────────────────────────────────────┐   │   │
│   │                   ingest_current_data()                          │   │   │
│   │                                                                  │   │   │
│   │  ┌────────────────┐  ┌────────────────┐  ┌────────────────────┐ │   │   │
│   │  │NEMDispatchClient  │NEMPriceClient  │  │NEMPriceClient     │ │   │   │
│   │  │.get_current_    │  │.get_current_  │  │.get_trading_      │ │   │   │
│   │  │ dispatch_data() │  │ dispatch_     │  │ prices()          │ │   │   │
│   │  │                 │  │ prices()      │  │                   │ │   │   │
│   │  └───────┬─────────┘  └───────┬───────┘  └─────────┬─────────┘ │   │   │
│   │          │                    │                    │           │   │   │
│   │  ┌────────────────┐  ┌────────────────┐                        │   │   │
│   │  │NEMPriceClient  │  │NEMPriceClient  │                        │   │   │
│   │  │.get_daily_     │  │.get_inter-    │                        │   │   │
│   │  │ prices()       │  │ connector_    │                        │   │   │
│   │  │                │  │ flows()       │                        │   │   │
│   │  └───────┬────────┘  └───────┬───────┘                        │   │   │
│   │          │                   │                                 │   │   │
│   │          ▼                   ▼                                 │   │   │
│   │  ┌─────────────────────────────────────────────────────────┐   │   │   │
│   │  │                   NEMDatabase                            │   │   │   │
│   │  │  .insert_dispatch_data()      .insert_price_data()      │   │   │   │
│   │  │  .insert_interconnector_data()                          │   │   │   │
│   │  └─────────────────────────────────────────────────────────┘   │   │   │
│   └─────────────────────────────────────────────────────────────────┘   │   │
│            │                                                            │   │
│            ▼                                                            │   │
│   ┌─────────────────┐                                                   │   │
│   │ asyncio.sleep   │  (5 minutes default)                              │   │
│   │ (interval)      │───────────────────────────────────────────────────┘   │
│   └─────────────────┘                                                       │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### NEM Data Format Parsing

NEMWEB provides data in a non-standard CSV format inside ZIP files:

```
ZIP file structure:
├── PUBLIC_DISPATCHSCADA_20250115103000_0000000123456789.zip
    └── PUBLIC_DISPATCHSCADA_20250115103000_0000000123456789.CSV

CSV format (NEM-specific):
C,NEMP.WORLD,file_info,...
I,DISPATCH,UNIT_SCADA,1,SETTLEMENTDATE,DUID,SCADAVALUE,LASTCHANGED
D,DISPATCH,UNIT_SCADA,1,"2025/01/15 10:30:00",BASTYAN,82.5,"2025/01/15 10:30:05"
D,DISPATCH,UNIT_SCADA,1,"2025/01/15 10:30:00",AGLSOM,125.3,"2025/01/15 10:30:05"
...
```

**Parsing logic:**
1. Download ZIP file from NEMWEB
2. Extract CSV from ZIP
3. Filter rows starting with "D," (data rows)
4. Parse specific columns based on record type
5. Convert to pandas DataFrame
6. Return for database insertion

---

## Database Design

### Entity-Relationship Diagram

```
┌────────────────────────────────────────────────────────────────────────┐
│                          generator_info                                 │
│  ┌──────────────────────────────────────────────────────────────────┐  │
│  │ duid (PK)        | station_name | region | fuel_source |         │  │
│  │ technology_type  | capacity_mw  | updated_at                     │  │
│  └──────────────────────────────────────────────────────────────────┘  │
└────────────────────────────────────────────────────────────────────────┘
                                    │
                                    │ duid (FK reference)
                                    ▼
┌────────────────────────────────────────────────────────────────────────┐
│                          dispatch_data                                  │
│  ┌──────────────────────────────────────────────────────────────────┐  │
│  │ id (PK)          | settlementdate | duid | scadavalue | uigf    │  │
│  │ totalcleared     | ramprate | availability | raise1sec |        │  │
│  │ lower1sec        | created_at                                    │  │
│  │                                                                  │  │
│  │ UNIQUE(settlementdate, duid) ON CONFLICT REPLACE                 │  │
│  └──────────────────────────────────────────────────────────────────┘  │
└────────────────────────────────────────────────────────────────────────┘

┌────────────────────────────────────────────────────────────────────────┐
│                            price_data                                   │
│  ┌──────────────────────────────────────────────────────────────────┐  │
│  │ id (PK)          | settlementdate | region | price | totaldemand │  │
│  │ price_type       | created_at                                     │  │
│  │                                                                   │  │
│  │ UNIQUE(settlementdate, region, price_type) ON CONFLICT REPLACE    │  │
│  └──────────────────────────────────────────────────────────────────┘  │
└────────────────────────────────────────────────────────────────────────┘

┌────────────────────────────────────────────────────────────────────────┐
│                       interconnector_data                               │
│  ┌──────────────────────────────────────────────────────────────────┐  │
│  │ id (PK)          | settlementdate | interconnector | meteredmwflow│  │
│  │ mwflow           | mwloss | marginalvalue | created_at            │  │
│  │                                                                   │  │
│  │ UNIQUE(settlementdate, interconnector) ON CONFLICT REPLACE        │  │
│  └──────────────────────────────────────────────────────────────────┘  │
└────────────────────────────────────────────────────────────────────────┘
```

### Indexing Strategy

```sql
-- dispatch_data indexes
CREATE INDEX idx_dispatch_settlementdate ON dispatch_data(settlementdate);
CREATE INDEX idx_dispatch_duid ON dispatch_data(duid);
CREATE INDEX idx_dispatch_date_duid ON dispatch_data(settlementdate, duid);

-- price_data indexes
CREATE INDEX idx_price_settlementdate ON price_data(settlementdate);
CREATE INDEX idx_price_region ON price_data(region);
CREATE INDEX idx_price_date_region ON price_data(settlementdate, region);

-- interconnector_data indexes
CREATE INDEX idx_interconnector_settlementdate ON interconnector_data(settlementdate);
CREATE INDEX idx_interconnector_id ON interconnector_data(interconnector);
```

### Data Retention

Currently, no automatic data retention is implemented. The database grows indefinitely. For production, consider:

- Daily/weekly cleanup jobs
- Partitioning by date
- Archival to cold storage
- Summary table aggregation

---

## API Design

### Design Principles

1. **REST conventions**: Resource-based URLs, HTTP methods for operations
2. **Consistent responses**: All list endpoints return `{data: [], count: n, message: ""}`
3. **Query parameters**: Filtering via query params, not path segments
4. **ISO 8601 dates**: All datetime parameters and responses use ISO format
5. **Meaningful errors**: HTTPException with descriptive messages

### Endpoint Categories

| Category | Endpoints | Purpose |
|----------|-----------|---------|
| Health | `/`, `/health` | System status |
| Dispatch | `/api/dispatch/*` | Generator SCADA data |
| Prices | `/api/prices/*` | Electricity prices |
| Interconnectors | `/api/interconnectors/*` | Power flows |
| Generators | `/api/generators/*`, `/api/duids` | Generator metadata |
| Analysis | `/api/generation/by-fuel`, `/api/summary` | Aggregations |
| Ingestion | `/api/ingest/*` | Manual data import |

### Response Schema Pattern

```python
# Pydantic response model pattern
class DispatchDataResponse(BaseModel):
    data: List[DispatchRecord]
    count: int
    message: str

# Endpoint pattern
@app.get("/api/dispatch/latest", response_model=DispatchDataResponse)
async def get_latest_dispatch(limit: int = Query(1000, ge=1, le=5000)):
    try:
        data = await db.get_latest_dispatch_data(limit)
        return DispatchDataResponse(
            data=data,
            count=len(data),
            message="Latest dispatch data retrieved successfully"
        )
    except Exception as e:
        logger.error(f"Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))
```

---

## Security Considerations

### Current State

The application currently has **no authentication or authorization**. This is acceptable for local development but must be addressed for production.

### Production Recommendations

1. **API Authentication**
   - Add API key authentication for backend endpoints
   - Consider OAuth2/JWT for user-based authentication

2. **Input Validation**
   - Pydantic models provide type validation
   - Add rate limiting for ingestion endpoints
   - Validate date ranges to prevent excessive queries

3. **CORS Configuration**
   - Currently allows localhost origins only
   - Update `allow_origins` for production domains

4. **Database Security**
   - SQLite file should have restricted permissions
   - Consider PostgreSQL with proper user roles for production

5. **HTTPS**
   - Deploy behind HTTPS-terminating reverse proxy
   - Use secure cookies if implementing sessions

---

## Scalability & Performance

### Current Limitations

| Component | Limitation | Impact |
|-----------|------------|--------|
| SQLite | Single-writer, no connection pooling | Not suitable for concurrent writes |
| In-memory | No caching layer | Repeated queries hit database |
| Single process | No horizontal scaling | Limited throughput |
| Polling | 30-second frontend intervals | Not real-time |

### Production Improvements

1. **Database**
   - Migrate to PostgreSQL for concurrent access
   - Implement connection pooling (asyncpg)
   - Add read replicas for query scaling

2. **Caching**
   - Add Redis for frequently accessed data
   - Cache latest prices/dispatch (5-min TTL)
   - Cache summary statistics (1-hour TTL)

3. **Real-time Updates**
   - Implement WebSockets for live data
   - Server-sent events (SSE) as alternative
   - Reduce polling to fallback only

4. **Horizontal Scaling**
   - Separate ingestion worker from API server
   - Run multiple API instances behind load balancer
   - Use message queue for ingestion tasks

### Monitoring Recommendations

1. **Application Metrics**
   - Request latency (p50, p95, p99)
   - Error rates by endpoint
   - Ingestion success/failure rates

2. **Database Metrics**
   - Query latency
   - Table sizes
   - Index usage

3. **External Dependencies**
   - NEMWEB response times
   - NEMWEB error rates

4. **Alerting**
   - Data freshness (stale data detection)
   - Ingestion failures
   - API error spikes
