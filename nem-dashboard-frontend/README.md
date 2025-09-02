# NEM Dashboard Frontend

Interactive Vizro dashboard for visualizing National Electricity Market (NEM) data with live prices, interconnector flows, and generator analysis.

## Features

- **Live Market Data**: Regional prices and interconnector flows updated every 5 minutes
- **Price History**: Historical price trends and distribution analysis
- **Generator Analysis**: Comprehensive generator output and capacity visualization
- **Interactive Filtering**: Click legend items to filter by fuel source
- **Real-time Updates**: Automatic data refresh from NEMWEB

## Dashboard Pages

### 1. Live Prices & Flows
- Current regional electricity prices ($/MWh)
- Real-time interconnector power flows between states
- Demand data by region

### 2. Price History
- 24-hour price trends across all NEM regions
- Price distribution box plots
- Both dispatch (5-min) and trading (30-min) prices

### 3. Generator Analysis
- Generation output vs capacity scatter plot
- Regional generation breakdown by fuel type
- Interactive fuel source filtering

### 4. All Generators
- Top 100 generators by current output
- Sortable and filterable by fuel source
- Detailed hover information (DUID, capacity, region, technology)

## Quick Start

1. **Install dependencies**:
   ```bash
   pip install vizro plotly pandas python-dotenv
   ```

2. **Start the backend** (required):
   ```bash
   cd ../nem-dashboard-backend
   python run.py
   ```

3. **Start the dashboard**:
   ```bash
   python app.py
   ```

The dashboard will be available at `http://localhost:8050`

## Configuration

Environment variables (optional):
```bash
# Dashboard settings
DASHBOARD_PORT=8050
DASHBOARD_HOST=0.0.0.0
DEBUG=True

# Backend API
BACKEND_URL=http://localhost:8000
```

## Data Sources

The frontend connects to the backend API for:
- Latest regional prices (dispatch and trading)
- Historical price data with date filtering
- Interconnector flows between NEM regions  
- Generator output data with region/fuel filtering
- Database statistics and health status

## Visualization Features

### Interactive Charts
- **Bar Charts**: Regional prices, interconnector flows, generator rankings
- **Scatter Plot**: Generator output vs capacity analysis
- **Box Plots**: Price distribution analysis
- **Line Charts**: Historical price trends

### User Interactions  
- **Legend Filtering**: Click legend items to show/hide data series
- **Hover Tooltips**: Detailed information on chart elements
- **Responsive Design**: Adapts to different screen sizes
- **Real-time Updates**: Data refreshes automatically every 5 minutes

## NEM Regions & Colors
- **NSW**: Blue (#1f77b4) - New South Wales
- **VIC**: Orange (#ff7f0e) - Victoria  
- **QLD**: Green (#2ca02c) - Queensland
- **SA**: Red (#d62728) - South Australia
- **TAS**: Purple (#9467bd) - Tasmania

## Generator Data

The dashboard shows generators classified by:
- **Fuel Source**: Coal, Gas, Wind, Solar, Hydro, Battery, Diesel, Other
- **Technology**: Steam turbines, gas turbines, wind farms, solar PV, etc.
- **Region**: NEM state locations
- **Capacity**: Nameplate capacity in MW
- **Output**: Current generation in MW (SCADA values)

## Monitoring

**Dashboard Health**:
- Data freshness indicators
- Backend connectivity status  
- Automatic error handling with sample data fallbacks

**Usage**:
- Navigate between pages using the sidebar
- Use legend filtering to focus on specific fuel types
- Hover over chart elements for detailed information
- Data updates automatically every 5 minutes

The dashboard provides a comprehensive view of Australia's electricity market with real-time data visualization and analysis capabilities.