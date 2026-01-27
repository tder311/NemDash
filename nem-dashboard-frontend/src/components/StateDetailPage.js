import React, { useState, useEffect, useCallback, useMemo } from 'react';
import Plot from 'react-plotly.js';
import axios from 'axios';
import DatabaseHealthPage from './DatabaseHealthPage';
import PASAPage from './PASAPage';
import './StateDetailPage.css';

const REGION_NAMES = {
  'NSW': 'New South Wales',
  'VIC': 'Victoria',
  'QLD': 'Queensland',
  'SA': 'South Australia',
  'TAS': 'Tasmania'
};

const REGION_COLORS = {
  'NSW': '#1f77b4',
  'VIC': '#ff7f0e',
  'QLD': '#2ca02c',
  'SA': '#d62728',
  'TAS': '#9467bd'
};

const FUEL_COLORS = {
  'Coal': '#4a4a4a',
  'Gas': '#ff9800',
  'Hydro': '#2196f3',
  'Wind': '#4caf50',
  'Solar': '#ffeb3b',
  'Battery': '#9c27b0',
  'Diesel': '#795548',
  'Biomass': '#8bc34a',
  'Unknown': '#9e9e9e'
};

const MONTHS = [
  { value: 1, label: 'Jan' },
  { value: 2, label: 'Feb' },
  { value: 3, label: 'Mar' },
  { value: 4, label: 'Apr' },
  { value: 5, label: 'May' },
  { value: 6, label: 'Jun' },
  { value: 7, label: 'Jul' },
  { value: 8, label: 'Aug' },
  { value: 9, label: 'Sep' },
  { value: 10, label: 'Oct' },
  { value: 11, label: 'Nov' },
  { value: 12, label: 'Dec' }
];

// Helper to get days in a month
const getDaysInMonth = (month, year) => {
  return new Date(year, month, 0).getDate();
};

// Helper to format date as local ISO string (without timezone suffix)
const formatLocalIso = (date) => {
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, '0');
  const day = String(date.getDate()).padStart(2, '0');
  const hours = String(date.getHours()).padStart(2, '0');
  const minutes = String(date.getMinutes()).padStart(2, '0');
  const seconds = String(date.getSeconds()).padStart(2, '0');
  return `${year}-${month}-${day}T${hours}:${minutes}:${seconds}`;
};

// Helper to format duration for display
const formatDuration = (startDate, endDate) => {
  const diffMs = endDate - startDate;
  const diffHours = Math.round(diffMs / (1000 * 60 * 60));
  const diffDays = Math.round(diffMs / (1000 * 60 * 60 * 24));

  if (diffHours <= 48) return `${diffHours} hours`;
  if (diffDays <= 30) return `${diffDays} days`;
  if (diffDays <= 90) return `${Math.round(diffDays / 7)} weeks`;
  if (diffDays <= 365) return `${Math.round(diffDays / 30)} months`;
  return `${Math.round(diffDays / 365)} year${diffDays >= 730 ? 's' : ''}`;
};

function StateDetailPage({ region, darkMode, onBack }) {
  const [priceHistory, setPriceHistory] = useState([]);
  const [generationHistory, setGenerationHistory] = useState([]);
  const [summary, setSummary] = useState(null);
  const [loading, setLoading] = useState(true);
  const [lastUpdated, setLastUpdated] = useState('');
  const [showHealthPage, setShowHealthPage] = useState(false);
  const [showPASAPage, setShowPASAPage] = useState(false);

  // Date range state
  const now = new Date();
  const yesterday = new Date(now);
  yesterday.setDate(yesterday.getDate() - 1);

  const [startDay, setStartDay] = useState(yesterday.getDate());
  const [startMonth, setStartMonth] = useState(yesterday.getMonth() + 1);
  const [startYear, setStartYear] = useState(yesterday.getFullYear());
  const [endDay, setEndDay] = useState(now.getDate());
  const [endMonth, setEndMonth] = useState(now.getMonth() + 1);
  const [endYear, setEndYear] = useState(now.getFullYear());
  const [availableDateRange, setAvailableDateRange] = useState(null);

  // Computed dates
  const startDate = useMemo(() => {
    return new Date(startYear, startMonth - 1, startDay, 0, 0, 0);
  }, [startDay, startMonth, startYear]);

  const endDate = useMemo(() => {
    return new Date(endYear, endMonth - 1, endDay, 23, 59, 59);
  }, [endDay, endMonth, endYear]);

  // Calculate hours from date range (for aggregation logic)
  const hoursFromDateRange = useMemo(() => {
    const diffMs = endDate - startDate;
    return Math.max(1, Math.round(diffMs / (1000 * 60 * 60)));
  }, [startDate, endDate]);

  // Calculate aggregated fuel mix from generation history
  const aggregatedFuelMix = useMemo(() => {
    if (!generationHistory.length) return [];

    const fuelTotals = {};
    generationHistory.forEach(record => {
      const fuel = record.fuel_source;
      fuelTotals[fuel] = (fuelTotals[fuel] || 0) + (record.generation_mw || 0);
    });

    const total = Object.values(fuelTotals).reduce((a, b) => a + b, 0);
    if (total === 0) return [];

    return Object.entries(fuelTotals)
      .map(([fuel, mw]) => ({
        fuel_source: fuel,
        generation_mw: mw,
        percentage: (mw / total) * 100,
        unit_count: 0
      }))
      .sort((a, b) => b.generation_mw - a.generation_mw);
  }, [generationHistory]);

  // Fetch available date range on mount
  useEffect(() => {
    const fetchDateRange = async () => {
      try {
        const response = await axios.get(`/api/region/${region}/data-range`);
        setAvailableDateRange(response.data);
      } catch (error) {
        console.error('Error fetching date range:', error);
      }
    };
    fetchDateRange();
  }, [region]);

  const fetchData = useCallback(async () => {
    try {
      // Use local ISO format (without Z suffix) to avoid timezone mismatch with database
      const startIso = formatLocalIso(startDate);
      const endIso = formatLocalIso(endDate);

      // Fetch all data in parallel
      const [priceResponse, summaryResponse, genHistoryResponse] = await Promise.all([
        axios.get(`/api/region/${region}/prices/history?start_date=${startIso}&end_date=${endIso}&price_type=MERGED`),
        axios.get(`/api/region/${region}/summary`),
        axios.get(`/api/region/${region}/generation/history?start_date=${startIso}&end_date=${endIso}`)
      ]);

      setPriceHistory(priceResponse.data.data || []);
      setSummary(summaryResponse.data);
      setGenerationHistory(genHistoryResponse.data.data || []);
      setLastUpdated(new Date().toLocaleTimeString());
      setLoading(false);
    } catch (error) {
      console.error('Error fetching region data:', error);
      setSummary({
        region: region,
        latest_price: 0,
        total_demand: 0,
        total_generation: 0,
        generator_count: 0
      });
      setPriceHistory([]);
      setGenerationHistory([]);
      setLastUpdated(new Date().toLocaleTimeString());
      setLoading(false);
    }
  }, [region, startDate, endDate]);

  useEffect(() => {
    fetchData();
    const interval = setInterval(fetchData, 60000);
    return () => clearInterval(interval);
  }, [fetchData]);

  const createPriceChartData = () => {
    if (!priceHistory.length) return [];

    return [
      {
        x: priceHistory.map(d => new Date(d.settlementdate)),
        y: priceHistory.map(d => d.price),
        type: 'scatter',
        mode: 'lines',
        name: 'Price ($/MWh)',
        line: {
          color: REGION_COLORS[region],
          width: 2.5,
          shape: 'spline',
          smoothing: 0.8
        },
        fill: 'tozeroy',
        fillcolor: `${REGION_COLORS[region]}15`,
        yaxis: 'y',
        hovertemplate: '$%{y:.2f}/MWh<extra></extra>'
      },
      {
        x: priceHistory.map(d => new Date(d.settlementdate)),
        y: priceHistory.map(d => d.totaldemand),
        type: 'scatter',
        mode: 'lines',
        name: 'Demand (MW)',
        line: {
          color: darkMode ? '#6b7280' : '#9ca3af',
          width: 1.5,
          shape: 'spline',
          smoothing: 0.8
        },
        yaxis: 'y2',
        hovertemplate: '%{y:,.0f} MW<extra></extra>'
      }
    ];
  };

  const createFuelMixChartData = () => {
    // Use aggregated fuel mix from generation history
    if (!aggregatedFuelMix.length) return [];

    const LABEL_THRESHOLD = 5;

    // Only show labels for top 3 or segments >= 5%
    const customText = aggregatedFuelMix.map((f, idx) => {
      if (idx < 3 || f.percentage >= LABEL_THRESHOLD) {
        return `${f.fuel_source}<br>${f.percentage.toFixed(1)}%`;
      }
      return '';
    });

    return [{
      values: aggregatedFuelMix.map(f => f.generation_mw),
      labels: aggregatedFuelMix.map(f => f.fuel_source),
      type: 'pie',
      hole: 0.5,
      marker: {
        colors: aggregatedFuelMix.map(f => FUEL_COLORS[f.fuel_source] || FUEL_COLORS['Unknown'])
      },
      text: customText,
      textinfo: 'text',
      textposition: 'outside',
      hovertemplate: '<b>%{label}</b><br>' +
                    'Generation: %{value:.0f} MW<br>' +
                    'Share: %{percent}<br>' +
                    '<extra></extra>'
    }];
  };

  const priceChartLayout = {
    title: {
      text: `Price & Demand`,
      font: {
        size: 16,
        color: darkMode ? '#e5e7eb' : '#374151',
        family: '-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif'
      },
      x: 0.02,
      xanchor: 'left'
    },
    xaxis: {
      gridcolor: darkMode ? '#374151' : '#f3f4f6',
      color: darkMode ? '#9ca3af' : '#6b7280',
      tickformat: hoursFromDateRange <= 24 ? '%H:%M' : hoursFromDateRange <= 168 ? '%d %b %H:%M' : hoursFromDateRange <= 720 ? '%d %b' : '%b %Y',
      tickfont: { size: 11 },
      linecolor: darkMode ? '#374151' : '#e5e7eb',
      showline: true,
      zeroline: false,
      rangeslider: { visible: false }
    },
    yaxis: {
      title: {
        text: '$/MWh',
        font: { size: 11, color: REGION_COLORS[region] },
        standoff: 10
      },
      gridcolor: darkMode ? '#374151' : '#f3f4f6',
      color: darkMode ? '#9ca3af' : '#6b7280',
      side: 'left',
      tickfont: { size: 11, color: REGION_COLORS[region] },
      zeroline: false
    },
    yaxis2: {
      title: {
        text: 'MW',
        font: { size: 11, color: darkMode ? '#6b7280' : '#9ca3af' },
        standoff: 10
      },
      color: darkMode ? '#6b7280' : '#9ca3af',
      side: 'right',
      overlaying: 'y',
      showgrid: false,
      tickfont: { size: 11 },
      zeroline: false
    },
    plot_bgcolor: 'transparent',
    paper_bgcolor: 'transparent',
    font: {
      color: darkMode ? '#e5e7eb' : '#374151',
      family: '-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif'
    },
    margin: { l: 55, r: 55, t: 40, b: 40 },
    showlegend: true,
    legend: {
      orientation: 'h',
      x: 1,
      xanchor: 'right',
      y: 1.02,
      yanchor: 'bottom',
      bgcolor: 'transparent',
      font: { size: 11 }
    },
    hovermode: 'x unified',
    hoverlabel: {
      bgcolor: darkMode ? '#1f2937' : 'white',
      bordercolor: darkMode ? '#374151' : '#e5e7eb',
      font: { size: 12, family: '-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif' }
    }
  };

  const durationText = formatDuration(startDate, endDate);

  const fuelMixLayout = {
    title: {
      text: `Fuel Mix (${durationText})`,
      font: {
        size: 18,
        color: darkMode ? '#f5f5f5' : '#333'
      }
    },
    plot_bgcolor: darkMode ? '#1e1e1e' : 'white',
    paper_bgcolor: darkMode ? '#1e1e1e' : 'white',
    font: {
      color: darkMode ? '#f5f5f5' : '#333'
    },
    margin: { l: 20, r: 20, t: 50, b: 20 },
    showlegend: true,
    legend: {
      orientation: 'h',
      x: 0.5,
      xanchor: 'center',
      y: -0.1
    }
  };

  const createGenerationHistoryChartData = () => {
    if (!generationHistory.length) return [];

    // Get unique fuel sources and sort by total generation
    const fuelSources = [...new Set(generationHistory.map(d => d.fuel_source))];
    const totalByFuel = {};
    fuelSources.forEach(fuel => {
      totalByFuel[fuel] = generationHistory
        .filter(d => d.fuel_source === fuel)
        .reduce((sum, d) => sum + (d.generation_mw || 0), 0);
    });
    const sortedFuels = fuelSources.sort((a, b) => totalByFuel[b] - totalByFuel[a]);

    // Get unique timestamps
    const timestamps = [...new Set(generationHistory.map(d => d.period))].sort();

    // Create one trace per fuel source
    return sortedFuels.map(fuel => {
      const fuelData = generationHistory.filter(d => d.fuel_source === fuel);
      const dataMap = {};
      fuelData.forEach(d => { dataMap[d.period] = d.generation_mw; });

      const color = FUEL_COLORS[fuel] || FUEL_COLORS['Unknown'];
      return {
        x: timestamps.map(t => new Date(t)),
        y: timestamps.map(t => dataMap[t] || 0),
        type: 'scatter',
        mode: 'lines',
        name: fuel,
        fill: 'tonexty',
        stackgroup: 'generation',
        fillcolor: color + '80',  // Add transparency to fill
        line: {
          color: color,
          width: 1
        },
        hovertemplate: `<b>${fuel}</b><br>` +
                      'Time: %{x}<br>' +
                      'Generation: %{y:.0f} MW<br>' +
                      '<extra></extra>'
      };
    });
  };

  const generationHistoryLayout = {
    title: {
      text: 'Generation by Fuel',
      font: {
        size: 16,
        color: darkMode ? '#e5e7eb' : '#374151',
        family: '-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif'
      },
      x: 0.02,
      xanchor: 'left'
    },
    xaxis: {
      gridcolor: darkMode ? '#374151' : '#f3f4f6',
      color: darkMode ? '#9ca3af' : '#6b7280',
      tickformat: hoursFromDateRange <= 24 ? '%H:%M' : hoursFromDateRange <= 168 ? '%d %b %H:%M' : hoursFromDateRange <= 720 ? '%d %b' : '%b %Y',
      tickfont: { size: 11 },
      linecolor: darkMode ? '#374151' : '#e5e7eb',
      showline: true,
      zeroline: false
    },
    yaxis: {
      title: {
        text: 'MW',
        font: { size: 11, color: darkMode ? '#9ca3af' : '#6b7280' },
        standoff: 10
      },
      gridcolor: darkMode ? '#374151' : '#f3f4f6',
      color: darkMode ? '#9ca3af' : '#6b7280',
      tickfont: { size: 11 },
      zeroline: false
    },
    plot_bgcolor: 'transparent',
    paper_bgcolor: 'transparent',
    font: {
      color: darkMode ? '#e5e7eb' : '#374151',
      family: '-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif'
    },
    margin: { l: 55, r: 55, t: 40, b: 40 },
    showlegend: true,
    legend: {
      orientation: 'h',
      x: 1,
      xanchor: 'right',
      y: 1.02,
      yanchor: 'bottom',
      bgcolor: 'transparent',
      font: { size: 11 }
    },
    hovermode: 'x unified',
    hoverlabel: {
      bgcolor: darkMode ? '#1f2937' : 'white',
      bordercolor: darkMode ? '#374151' : '#e5e7eb',
      font: { size: 12, family: '-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif' }
    }
  };

  // Show database health page if requested
  if (showHealthPage) {
    return (
      <DatabaseHealthPage
        darkMode={darkMode}
        onBack={() => setShowHealthPage(false)}
      />
    );
  }

  // Show PASA forecast page if requested
  if (showPASAPage) {
    return (
      <PASAPage
        region={region}
        darkMode={darkMode}
        onBack={() => setShowPASAPage(false)}
      />
    );
  }

  if (loading) {
    return (
      <div className={`state-detail-container ${darkMode ? 'dark' : 'light'}`}>
        <div className="loading">
          <div className="spinner"></div>
          <p>Loading {REGION_NAMES[region]} data...</p>
        </div>
      </div>
    );
  }

  return (
    <div className={`state-detail-container ${darkMode ? 'dark' : 'light'}`}>
      <div className="state-header">
        <button className="back-button" onClick={onBack}>
          Back to Overview
        </button>
        <h1 className="state-title" style={{ color: REGION_COLORS[region] }}>
          {REGION_NAMES[region]} ({region})
        </h1>
        <div
          className="last-updated clickable"
          onClick={() => setShowHealthPage(true)}
          title="Click to view database health"
        >
          Last Updated: {lastUpdated}
        </div>
      </div>

      <div className="summary-cards">
        <div className="summary-card">
          <div className="card-label">Current Price</div>
          <div className="card-value" style={{ color: REGION_COLORS[region] }}>
            ${summary?.latest_price?.toFixed(2) || '0.00'}
            <span className="card-unit">/MWh</span>
          </div>
        </div>
        <div className="summary-card">
          <div className="card-label">Total Demand</div>
          <div className="card-value">
            {summary?.total_demand?.toFixed(0) || '0'}
            <span className="card-unit">MW</span>
          </div>
        </div>
        <div className="summary-card">
          <div className="card-label">Total Generation</div>
          <div className="card-value">
            {summary?.total_generation?.toFixed(0) || '0'}
            <span className="card-unit">MW</span>
          </div>
        </div>
        <div className="summary-card">
          <div className="card-label">Active Generators</div>
          <div className="card-value">
            {summary?.generator_count || '0'}
          </div>
        </div>
        <div
          className="summary-card clickable"
          onClick={() => setShowPASAPage(true)}
          title="Click to view PASA forecast"
          style={{ cursor: 'pointer' }}
        >
          <div className="card-label">PASA Forecast</div>
          <div className="card-value" style={{ fontSize: '1.2rem' }}>
            View Reserve
            <span className="card-unit">Outlook</span>
          </div>
        </div>
      </div>

      <div className="date-range-panel">
        <div className="date-range-header">
          <label>Time Range:</label>
          <span className="duration-text">({durationText})</span>
        </div>

        {/* Visual Range Slider */}
        {availableDateRange && availableDateRange.earliest_date && availableDateRange.latest_date && (
          <div className="range-slider-container">
            <div className="range-slider-track">
              <div
                className="range-slider-selected"
                style={{
                  left: `${Math.max(0, ((startDate.getTime() - new Date(availableDateRange.earliest_date).getTime()) /
                    (new Date(availableDateRange.latest_date).getTime() - new Date(availableDateRange.earliest_date).getTime())) * 100)}%`,
                  right: `${Math.max(0, 100 - ((endDate.getTime() - new Date(availableDateRange.earliest_date).getTime()) /
                    (new Date(availableDateRange.latest_date).getTime() - new Date(availableDateRange.earliest_date).getTime())) * 100)}%`
                }}
              />
            </div>
            <input
              type="range"
              className="range-slider-input range-slider-start"
              aria-label="start date slider"
              min={new Date(availableDateRange.earliest_date).getTime()}
              max={new Date(availableDateRange.latest_date).getTime()}
              value={startDate.getTime()}
              onChange={(e) => {
                const newDate = new Date(Number(e.target.value));
                setStartDay(newDate.getDate());
                setStartMonth(newDate.getMonth() + 1);
                setStartYear(newDate.getFullYear());
              }}
            />
            <input
              type="range"
              className="range-slider-input range-slider-end"
              aria-label="end date slider"
              min={new Date(availableDateRange.earliest_date).getTime()}
              max={new Date(availableDateRange.latest_date).getTime()}
              value={endDate.getTime()}
              onChange={(e) => {
                const newDate = new Date(Number(e.target.value));
                setEndDay(newDate.getDate());
                setEndMonth(newDate.getMonth() + 1);
                setEndYear(newDate.getFullYear());
              }}
            />
            <div className="range-slider-labels">
              <span>{new Date(availableDateRange.earliest_date).toLocaleDateString('en-AU', { day: 'numeric', month: 'short', year: 'numeric' })}</span>
              <span>{new Date(availableDateRange.latest_date).toLocaleDateString('en-AU', { day: 'numeric', month: 'short', year: 'numeric' })}</span>
            </div>
          </div>
        )}

        {/* Date Dropdowns for precise selection */}
        <div className="date-dropdowns">
          <select
            aria-label="start day"
            value={startDay}
            onChange={(e) => setStartDay(Number(e.target.value))}
          >
            {Array.from({ length: getDaysInMonth(startMonth, startYear) }, (_, i) => i + 1).map(d => (
              <option key={d} value={d}>{d}</option>
            ))}
          </select>
          <select
            aria-label="start month"
            value={startMonth}
            onChange={(e) => setStartMonth(Number(e.target.value))}
          >
            {MONTHS.map(m => (
              <option key={m.value} value={m.value}>{m.label}</option>
            ))}
          </select>
          <select
            aria-label="start year"
            value={startYear}
            onChange={(e) => setStartYear(Number(e.target.value))}
          >
            {Array.from({ length: 5 }, (_, i) => now.getFullYear() - 4 + i).map(y => (
              <option key={y} value={y}>{y}</option>
            ))}
          </select>

          <span className="date-separator">—</span>

          <select
            aria-label="end day"
            value={endDay}
            onChange={(e) => setEndDay(Number(e.target.value))}
          >
            {Array.from({ length: getDaysInMonth(endMonth, endYear) }, (_, i) => i + 1).map(d => (
              <option key={d} value={d}>{d}</option>
            ))}
          </select>
          <select
            aria-label="end month"
            value={endMonth}
            onChange={(e) => setEndMonth(Number(e.target.value))}
          >
            {MONTHS.map(m => (
              <option key={m.value} value={m.value}>{m.label}</option>
            ))}
          </select>
          <select
            aria-label="end year"
            value={endYear}
            onChange={(e) => setEndYear(Number(e.target.value))}
          >
            {Array.from({ length: 5 }, (_, i) => now.getFullYear() - 4 + i).map(y => (
              <option key={y} value={y}>{y}</option>
            ))}
          </select>
        </div>
      </div>

      <div className="charts-container">
        <div className="chart-wrapper price-chart">
          <Plot
            data={createPriceChartData()}
            layout={priceChartLayout}
            style={{ width: '100%', height: '400px' }}
            config={{
              displayModeBar: 'hover',
              displaylogo: false,
              modeBarButtonsToRemove: ['pan2d', 'lasso2d', 'select2d', 'autoScale2d'],
              modeBarButtonsToAdd: [],
              scrollZoom: false
            }}
          />
        </div>

        <div className="chart-wrapper fuel-chart">
          <Plot
            data={createFuelMixChartData()}
            layout={fuelMixLayout}
            style={{ width: '100%', height: '350px' }}
            config={{
              displayModeBar: false,
              displaylogo: false
            }}
          />
        </div>
      </div>

      <div className="chart-wrapper generation-history-chart">
        {generationHistory.length > 0 ? (
          <Plot
            data={createGenerationHistoryChartData()}
            layout={generationHistoryLayout}
            style={{ width: '100%', height: '400px' }}
            config={{
              displayModeBar: 'hover',
              displaylogo: false,
              modeBarButtonsToRemove: ['pan2d', 'lasso2d', 'select2d', 'autoScale2d'],
              scrollZoom: false
            }}
          />
        ) : (
          <div className="no-data-message">
            <h3 style={{ color: darkMode ? '#f5f5f5' : '#333' }}>
              Generation by Fuel Source ({durationText})
            </h3>
            <p style={{ color: darkMode ? '#aaa' : '#666' }}>
              No generation history data available. Generator metadata may need to be imported.
            </p>
          </div>
        )}
      </div>
    </div>
  );
}

export default StateDetailPage;
