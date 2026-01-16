# NEM Dashboard

A full-stack application for visualizing real-time and historical Australian electricity market data from AEMO's NEMWEB portal.

## Overview

The National Electricity Market (NEM) Dashboard provides real-time monitoring and historical analysis of Australia's electricity market across five regions: New South Wales (NSW), Victoria (VIC), Queensland (QLD), South Australia (SA), and Tasmania (TAS).

### Key Features

- **Live Prices**: Real-time electricity prices updated every 30 seconds with interactive map visualization
- **Price History**: 24-hour historical price charts with multi-region comparison
- **Interconnector Flows**: Power flow visualization between states
- **Generator Data**: SCADA dispatch data with fuel source classification
- **Dark Mode**: Full dark/light theme support

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                         NEM Dashboard                                │
├─────────────────────────────┬───────────────────────────────────────┤
│     Frontend (React)        │           Backend (FastAPI)            │
│   http://localhost:3000     │         http://localhost:8000          │
├─────────────────────────────┼───────────────────────────────────────┤
│ • Live Prices Page          │ • Data Ingestion Pipeline              │
│ • Price History Page        │   - NEMDispatchClient (SCADA data)     │
│ • Australia Map SVG         │   - NEMPriceClient (prices/flows)      │
│ • Region Sidebar            │   - DataIngester (orchestration)       │
│ • Interconnector Flows      │ • REST API (14 endpoints)              │
│ • Plotly Charts             │ • SQLite Database (async)              │
└─────────────────────────────┴───────────────────────────────────────┘
                                        │
                                        ▼
                        ┌───────────────────────────────┐
                        │         AEMO NEMWEB           │
                        │  https://www.nemweb.com.au    │
                        │  • Dispatch SCADA (5-min)     │
                        │  • Trading Prices (30-min)    │
                        │  • Interconnector Flows       │
                        │  • Public Price Archives      │
                        └───────────────────────────────┘
```

## Quick Start

### Prerequisites

- Python 3.8+ with pip
- Node.js 14+ with npm
- Git

### Installation

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd san-juan
   ```

2. **Set up the backend**
   ```bash
   cd nem-dashboard-backend

   # Create virtual environment (recommended)
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate

   # Install dependencies
   pip install -r requirements.txt

   # Configure environment (optional - defaults work for development)
   cp .env.example .env
   ```

3. **Set up the frontend**
   ```bash
   cd nem-dashboard-frontend
   npm install
   ```

### Running the Application

**Terminal 1 - Start Backend**
```bash
cd nem-dashboard-backend
python run.py
```
The backend will start at http://localhost:8000 and begin fetching data from NEMWEB automatically.

**Terminal 2 - Start Frontend**
```bash
cd nem-dashboard-frontend
npm start
```
The frontend will start at http://localhost:3000 and open in your browser.

### Verify Installation

```bash
# Check backend health
curl http://localhost:8000/health

# Check data summary
curl http://localhost:8000/api/summary
```

## Project Structure

```
san-juan/
├── README.md                      # This file
├── CLAUDE.md                      # Claude Code development guidance
├── docs/
│   ├── API.md                     # Complete API reference
│   └── ARCHITECTURE.md            # System architecture details
├── nem-dashboard-backend/
│   ├── README.md                  # Backend documentation
│   ├── app/
│   │   ├── main.py               # FastAPI app & endpoints
│   │   ├── database.py           # SQLite operations
│   │   ├── models.py             # Pydantic schemas
│   │   ├── nem_client.py         # Dispatch data client
│   │   ├── nem_price_client.py   # Price/flow client
│   │   └── data_ingester.py      # Data pipeline
│   ├── run.py                    # Application entry point
│   ├── import_geninfo_csv.py     # Generator data import
│   ├── requirements.txt          # Python dependencies
│   └── data/                     # Database & reference data
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
| `DATABASE_PATH` | `./data/nem_dispatch.db` | SQLite database location |
| `NEM_API_BASE_URL` | `https://www.nemweb.com.au` | NEMWEB base URL |
| `UPDATE_INTERVAL_MINUTES` | `5` | Data fetch interval |

### Frontend Configuration

The frontend uses a proxy configuration in `package.json` to route API requests to the backend:
```json
"proxy": "http://localhost:8000"
```

No additional environment variables are required for development.

## API Overview

The backend exposes a REST API with the following endpoint categories:

| Category | Endpoints | Description |
|----------|-----------|-------------|
| **Dispatch** | `/api/dispatch/latest`, `/api/dispatch/range` | Generator SCADA data |
| **Prices** | `/api/prices/latest`, `/api/prices/history` | Electricity prices |
| **Interconnectors** | `/api/interconnectors/latest`, `/api/interconnectors/history` | Power flows |
| **Generators** | `/api/generators/filter`, `/api/duids` | Generator metadata |
| **Analysis** | `/api/generation/by-fuel`, `/api/summary` | Aggregated data |
| **Ingestion** | `/api/ingest/*` | Manual data import triggers |

See [docs/API.md](docs/API.md) for complete API reference.

## Data Sources

Data is sourced from AEMO's NEMWEB portal:

| Data Type | Update Frequency | Source |
|-----------|------------------|--------|
| Dispatch SCADA | 5 minutes | `/REPORTS/CURRENT/Dispatch_SCADA/` |
| Dispatch Prices | 5 minutes | `/Reports/Current/DispatchIS_Reports/` |
| Trading Prices | 30 minutes | `/Reports/Current/TradingIS_Reports/` |
| Interconnector Flows | 5 minutes | `/Reports/Current/Dispatch_IRSR/` |
| Public Prices | Daily | `/Reports/Current/Public_Prices/` |

## NEM Regions

The application covers all five NEM regions with consistent color coding:

| Region | Code | Color |
|--------|------|-------|
| New South Wales | NSW | Blue (#1f77b4) |
| Victoria | VIC | Orange (#ff7f0e) |
| Queensland | QLD | Green (#2ca02c) |
| South Australia | SA | Red (#d62728) |
| Tasmania | TAS | Purple (#9467bd) |

## Development

### Backend Development

```bash
cd nem-dashboard-backend

# Run with auto-reload
python run.py

# Import full generator data (optional)
python import_geninfo_csv.py

# Test endpoints
curl http://localhost:8000/api/prices/latest
curl http://localhost:8000/api/dispatch/latest?limit=10
```

### Frontend Development

```bash
cd nem-dashboard-frontend

# Start development server
npm start

# Run tests
npm test

# Build for production
npm run build
```

### Adding New Features

See [CLAUDE.md](CLAUDE.md) for detailed development patterns including:
- Adding new API endpoints
- Adding new NEM data sources
- Adding frontend components

## Testing

### Backend
- Manual API testing via curl or browser
- Monitor logs for data ingestion cycles
- Health check: `GET /health`

### Frontend
- React Testing Library configured
- Run tests: `npm test`
- Focus on component rendering and interactions

## Known Limitations

1. Historical data ingestion requires manual API calls
2. Default generator data limited to 10 sample units
3. No authentication/authorization implemented
4. SQLite not recommended for high-concurrency production
5. No automatic data retention/cleanup policy

## Production Deployment

### Requirements
- Python 3.8+ with asyncio support
- Node.js 14+ for frontend build
- Writable database directory
- Network access to NEMWEB

### Recommendations
- Replace SQLite with PostgreSQL for production
- Use nginx as reverse proxy for frontend
- Implement rate limiting for NEMWEB requests
- Add monitoring for data ingestion health
- Configure proper CORS origins in `main.py`

## License

[Add license information]

## Contributing

[Add contribution guidelines]

## Support

For issues or questions, please [add support information].
