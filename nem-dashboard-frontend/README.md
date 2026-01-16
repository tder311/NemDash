# NEM Dashboard Frontend

React-based dashboard for visualizing Australian National Electricity Market (NEM) data with live prices, interactive maps, and historical charts.

## Features

- **Live Prices**: Real-time regional electricity prices with 30-second refresh
- **Interactive Map**: Australia SVG map with region highlighting on hover
- **Price History**: 24-hour historical price charts using Plotly
- **Dark Mode**: Full light/dark theme support
- **Responsive Design**: Adapts to different screen sizes

## Quick Start

```bash
# Install dependencies
npm install

# Start development server (requires backend running)
npm start
```

The application opens at http://localhost:3000.

**Note**: Ensure the backend is running at http://localhost:8000 before starting the frontend.

## Project Structure

```
nem-dashboard-frontend/
├── src/
│   ├── index.js              # React root render
│   ├── index.css             # Global styles, CSS variables
│   ├── App.js                # Main container, routing, dark mode
│   ├── App.css               # App-level styling
│   └── components/
│       ├── LivePricesPage.js      # Live prices + map layout
│       ├── LivePricesPage.css
│       ├── PriceHistoryPage.js    # 24h historical chart
│       ├── PriceHistoryPage.css
│       ├── RegionSidebar.js       # Region price cards
│       ├── RegionSidebar.css
│       ├── RegionCard.js          # Individual region display
│       ├── RegionCard.css
│       ├── AustraliaMap.js        # SVG map component
│       ├── InterconnectorFlow.js  # Power flow visualization
│       └── InterconnectorFlow.css
├── public/
│   ├── index.html            # HTML template
│   └── australia-map.svg     # Australia map SVG
├── package.json              # Dependencies and scripts
└── README.md                 # This file
```

## Components

### App.js - Main Container

Root component managing application state and navigation.

**State:**
- `darkMode` (boolean) - Theme toggle
- `activeTab` (string) - "live" or "history"

**Features:**
- Header with dark mode toggle
- Tab navigation between pages
- Passes darkMode prop to child components

### LivePricesPage.js - Real-time Display

Main dashboard view showing current prices and map.

**State:**
- `prices` - Array of region price data
- `lastUpdated` - Timestamp of last fetch
- `loading` - Boolean loading state
- `hoveredRegion` - Currently highlighted region

**Behavior:**
- Fetches trading prices every 30 seconds
- Combines trading prices with dispatch demand data
- Falls back to sample data on API error

**Layout:**
```
┌─────────────────────────────────────────────┐
│                 Header                       │
├──────────────┬──────────────────────────────┤
│   Region     │                              │
│   Sidebar    │     Australia Map            │
│   (320px)    │     + Region Cards           │
│              │                              │
│   NSW  $85   │         [NSW]                │
│   VIC  $78   │   [SA]         [QLD]         │
│   QLD  $92   │         [VIC]                │
│   ...        │         [TAS]                │
└──────────────┴──────────────────────────────┘
```

### PriceHistoryPage.js - Historical Charts

24-hour price history visualization using Plotly.

**State:**
- `priceData` - Array of historical records
- `loading` - Boolean loading state

**Chart Configuration:**
- 5 line series (one per region)
- X-axis: Time (24-hour window)
- Y-axis: Price ($/MWh)
- Colors: Region-specific
- Interactive: Hover tooltips, legend filtering

### RegionSidebar.js - Region List

Left sidebar displaying region price cards.

**Props:**
- `regions` - Array of region price data
- `darkMode` - Theme flag
- `onRegionHover(regionCode)` - Hover callback
- `onRegionLeave()` - Leave callback

**Features:**
- Color-coded borders matching region colors
- Hover animations (scale, translate, shadow)
- Shimmer effect on hover

### AustraliaMap.js - SVG Map

Interactive Australia map visualization.

**Props:**
- `hoveredRegion` - Region to highlight
- `darkMode` - Theme flag

**Features:**
- Loads SVG dynamically from public folder
- Highlights region path on hover
- Dark mode filter adjustments

### RegionCard.js - Region Display

Individual region price card overlay on map.

**Props:**
- `region` - Region code
- `price` - Current price
- `demand` - Total demand MW
- `color` - Region color
- `position` - CSS position
- `darkMode` - Theme flag

### InterconnectorFlow.js - Flow Lines

Power flow visualization between regions.

**Props:**
- `fromRegion`, `toRegion` - Region codes
- `flow` - MW flow value
- `positions` - Position object
- `darkMode` - Theme flag

**Features:**
- SVG line from region to region
- Direction arrow indicator
- Color coding (green positive, red negative)

## Data Fetching Pattern

Standard pattern used throughout components:

```javascript
const [data, setData] = useState([]);
const [loading, setLoading] = useState(true);

useEffect(() => {
  const fetchData = async () => {
    try {
      const response = await axios.get('/api/endpoint');
      setData(response.data.data || []);
    } catch (error) {
      console.error('Error:', error);
      setData(fallbackData);  // Graceful degradation
    } finally {
      setLoading(false);
    }
  };

  fetchData();
  const interval = setInterval(fetchData, 30000);
  return () => clearInterval(interval);
}, []);
```

## API Integration

The frontend proxies API requests to the backend via `package.json`:

```json
"proxy": "http://localhost:8000"
```

**Key API Calls:**
| Component | Endpoint | Purpose |
|-----------|----------|---------|
| LivePricesPage | `/api/prices/latest?price_type=TRADING` | Current prices |
| LivePricesPage | `/api/prices/latest?price_type=DISPATCH` | Demand data |
| PriceHistoryPage | `/api/prices/history` | 24h history |

## Styling

### CSS Architecture

- **Global styles** (`index.css`) - CSS variables, body styling
- **Component styles** - Scoped CSS files per component
- **Dark mode** - Class conditionals: `className={darkMode ? 'dark' : 'light'}`

### Region Color Scheme

```javascript
const REGION_COLORS = {
  'NSW': '#1f77b4',   // Blue
  'VIC': '#ff7f0e',   // Orange
  'QLD': '#2ca02c',   // Green
  'SA': '#d62728',    // Red
  'TAS': '#9467bd'    // Purple
};
```

### Dark Mode

Light mode (default):
- Background: `#f8f9fa`
- Text: `#333333`

Dark mode:
- Background: `#1a1a1a`
- Text: `#f5f5f5`

## Development

### Scripts

```bash
npm start     # Start development server on :3000
npm run build # Build production bundle
npm test      # Run tests with React Testing Library
npm run eject # Eject from Create React App (irreversible)
```

### Adding Components

1. Create component file in `src/components/`
2. Create matching CSS file
3. Follow useState/useEffect pattern
4. Add darkMode prop support
5. Provide fallback data for errors

### Environment

No environment variables required for development. The proxy in `package.json` handles API routing.

For production builds, configure the API base URL appropriately.

## Dependencies

```json
{
  "axios": "^1.4.0",           // HTTP client
  "plotly.js": "^2.24.1",      // Charting library
  "react": "^18.2.0",          // UI framework
  "react-dom": "^18.2.0",      // DOM rendering
  "react-plotly.js": "^2.6.0", // React Plotly wrapper
  "react-scripts": "5.0.1"     // Build tooling
}
```

## Browser Support

React 18 supports all modern browsers:
- Chrome (latest)
- Firefox (latest)
- Safari (latest)
- Edge (latest)

## Troubleshooting

| Issue | Solution |
|-------|----------|
| Blank page | Check browser console for errors |
| No data | Ensure backend is running on :8000 |
| CORS errors | Verify proxy config in package.json |
| Stale data | Check network tab, verify polling works |
| Map not loading | Check australia-map.svg exists in public/ |

## Production Build

```bash
npm run build
```

Creates optimized bundle in `build/` directory. Deploy with:
- Static file server (nginx, Apache)
- CDN (CloudFront, Cloudflare)
- Platform (Vercel, Netlify)

Configure API URL for production environment.
