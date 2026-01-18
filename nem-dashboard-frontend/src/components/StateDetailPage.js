import React, { useState, useEffect, useCallback } from 'react';
import Plot from 'react-plotly.js';
import axios from 'axios';
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

// Helper to format time range for display
const formatTimeRange = (hours) => {
  if (hours <= 48) return `${hours} Hours`;
  if (hours === 168) return '7 Days';
  if (hours === 720) return '30 Days';
  if (hours === 2160) return '90 Days';
  if (hours === 8760) return '365 Days';
  return `${hours} Hours`;
};

// Helper to get aggregation level description
const getAggregationLabel = (hours) => {
  if (hours < 48) return 'Real-time (5 min)';
  if (hours <= 168) return '30 min averages';
  if (hours <= 720) return 'Hourly averages';
  if (hours <= 2160) return 'Daily averages';
  return 'Weekly averages';
};

function StateDetailPage({ region, darkMode, onBack }) {
  const [priceHistory, setPriceHistory] = useState([]);
  const [fuelMix, setFuelMix] = useState([]);
  const [generationHistory, setGenerationHistory] = useState([]);
  const [summary, setSummary] = useState(null);
  const [loading, setLoading] = useState(true);
  const [timeRange, setTimeRange] = useState(24);
  const [lastUpdated, setLastUpdated] = useState('');

  const fetchData = useCallback(async () => {
    try {
      // Fetch all data in parallel
      const [priceResponse, fuelResponse, summaryResponse, genHistoryResponse] = await Promise.all([
        axios.get(`/api/region/${region}/prices/history?hours=${timeRange}&price_type=MERGED`),
        axios.get(`/api/region/${region}/generation/current`),
        axios.get(`/api/region/${region}/summary`),
        axios.get(`/api/region/${region}/generation/history?hours=${timeRange}`)
      ]);

      setPriceHistory(priceResponse.data.data || []);
      setFuelMix(fuelResponse.data.fuel_mix || []);
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
      setFuelMix([
        { fuel_source: 'Coal', generation_mw: 0, percentage: 0, unit_count: 0 },
        { fuel_source: 'Solar', generation_mw: 0, percentage: 0, unit_count: 0 },
        { fuel_source: 'Wind', generation_mw: 0, percentage: 0, unit_count: 0 }
      ]);
      setPriceHistory([]);
      setGenerationHistory([]);
      setLastUpdated(new Date().toLocaleTimeString());
      setLoading(false);
    }
  }, [region, timeRange]);

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
    if (!fuelMix.length) return [];

    const sortedFuelMix = [...fuelMix].sort((a, b) => b.generation_mw - a.generation_mw);
    const LABEL_THRESHOLD = 5;

    // Only show labels for top 3 or segments >= 5%
    const customText = sortedFuelMix.map((f, idx) => {
      if (idx < 3 || f.percentage >= LABEL_THRESHOLD) {
        return `${f.fuel_source}<br>${f.percentage.toFixed(1)}%`;
      }
      return '';
    });

    return [{
      values: sortedFuelMix.map(f => f.generation_mw),
      labels: sortedFuelMix.map(f => f.fuel_source),
      type: 'pie',
      hole: 0.5,
      marker: {
        colors: sortedFuelMix.map(f => FUEL_COLORS[f.fuel_source] || FUEL_COLORS['Unknown'])
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
      tickformat: timeRange <= 24 ? '%H:%M' : timeRange <= 168 ? '%d %b %H:%M' : timeRange <= 720 ? '%d %b' : '%b %Y',
      tickfont: { size: 11 },
      linecolor: darkMode ? '#374151' : '#e5e7eb',
      showline: true,
      zeroline: false
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

  const fuelMixLayout = {
    title: {
      text: 'Current Fuel Mix',
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
      tickformat: timeRange <= 24 ? '%H:%M' : timeRange <= 168 ? '%d %b %H:%M' : timeRange <= 720 ? '%d %b' : '%b %Y',
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
        <div className="last-updated">Last Updated: {lastUpdated}</div>
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
      </div>

      <div className="time-range-selector">
        <label>Time Range:</label>
        <select value={timeRange} onChange={(e) => setTimeRange(Number(e.target.value))}>
          <option value={6}>6 Hours</option>
          <option value={12}>12 Hours</option>
          <option value={24}>24 Hours</option>
          <option value={48}>48 Hours</option>
          <option value={168}>7 Days</option>
          <option value={720}>30 Days</option>
          <option value={2160}>90 Days</option>
          <option value={8760}>365 Days</option>
        </select>
      </div>

      <div className="charts-container">
        <div className="chart-wrapper price-chart">
          <Plot
            data={createPriceChartData()}
            layout={priceChartLayout}
            style={{ width: '100%', height: '350px' }}
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
              Generation by Fuel Source - Last {formatTimeRange(timeRange)}
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
