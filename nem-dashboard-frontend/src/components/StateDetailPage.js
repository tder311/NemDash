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

function StateDetailPage({ region, darkMode, onBack }) {
  const [priceHistory, setPriceHistory] = useState([]);
  const [fuelMix, setFuelMix] = useState([]);
  const [summary, setSummary] = useState(null);
  const [loading, setLoading] = useState(true);
  const [timeRange, setTimeRange] = useState(24);
  const [lastUpdated, setLastUpdated] = useState('');

  const fetchData = useCallback(async () => {
    try {
      const [priceResponse, fuelResponse, summaryResponse] = await Promise.all([
        axios.get(`/api/region/${region}/prices/history?hours=${timeRange}&price_type=PUBLIC`),
        axios.get(`/api/region/${region}/generation/current`),
        axios.get(`/api/region/${region}/summary`)
      ]);

      setPriceHistory(priceResponse.data.data || []);
      setFuelMix(fuelResponse.data.fuel_mix || []);
      setSummary(summaryResponse.data);
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

    return [{
      x: priceHistory.map(d => new Date(d.settlementdate)),
      y: priceHistory.map(d => d.price),
      type: 'scatter',
      mode: 'lines',
      name: region,
      line: {
        color: REGION_COLORS[region],
        width: 2
      },
      fill: 'tozeroy',
      fillcolor: `${REGION_COLORS[region]}20`,
      hovertemplate: '<b>%{x}</b><br>' +
                    'Price: $%{y:.2f}/MWh<br>' +
                    '<extra></extra>'
    }];
  };

  const createFuelMixChartData = () => {
    if (!fuelMix.length) return [];

    const sortedFuelMix = [...fuelMix].sort((a, b) => b.generation_mw - a.generation_mw);

    return [{
      values: sortedFuelMix.map(f => f.generation_mw),
      labels: sortedFuelMix.map(f => f.fuel_source),
      type: 'pie',
      hole: 0.5,
      marker: {
        colors: sortedFuelMix.map(f => FUEL_COLORS[f.fuel_source] || FUEL_COLORS['Unknown'])
      },
      textinfo: 'label+percent',
      textposition: 'outside',
      hovertemplate: '<b>%{label}</b><br>' +
                    'Generation: %{value:.0f} MW<br>' +
                    'Share: %{percent}<br>' +
                    '<extra></extra>'
    }];
  };

  const priceChartLayout = {
    title: {
      text: `${region} Price History - Last ${timeRange} Hours`,
      font: {
        size: 18,
        color: darkMode ? '#f5f5f5' : '#333'
      }
    },
    xaxis: {
      title: 'Time',
      gridcolor: darkMode ? '#404040' : '#e0e0e0',
      color: darkMode ? '#f5f5f5' : '#333',
      tickformat: '%H:%M'
    },
    yaxis: {
      title: 'Price ($/MWh)',
      gridcolor: darkMode ? '#404040' : '#e0e0e0',
      color: darkMode ? '#f5f5f5' : '#333'
    },
    plot_bgcolor: darkMode ? '#1e1e1e' : 'white',
    paper_bgcolor: darkMode ? '#1e1e1e' : 'white',
    font: {
      color: darkMode ? '#f5f5f5' : '#333'
    },
    margin: { l: 60, r: 30, t: 50, b: 50 },
    showlegend: false
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
        </select>
      </div>

      <div className="charts-container">
        <div className="chart-wrapper price-chart">
          <Plot
            data={createPriceChartData()}
            layout={priceChartLayout}
            style={{ width: '100%', height: '350px' }}
            config={{
              displayModeBar: true,
              displaylogo: false,
              modeBarButtonsToRemove: ['pan2d', 'lasso2d', 'select2d']
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

      <div className="fuel-breakdown-table">
        <h3>Generation by Fuel Source</h3>
        <table>
          <thead>
            <tr>
              <th>Fuel Source</th>
              <th>Generation (MW)</th>
              <th>Share (%)</th>
              <th>Units</th>
            </tr>
          </thead>
          <tbody>
            {fuelMix.map((fuel, index) => (
              <tr key={index}>
                <td>
                  <span
                    className="fuel-indicator"
                    style={{ backgroundColor: FUEL_COLORS[fuel.fuel_source] || FUEL_COLORS['Unknown'] }}
                  />
                  {fuel.fuel_source}
                </td>
                <td>{fuel.generation_mw?.toFixed(1) || '0'}</td>
                <td>{fuel.percentage?.toFixed(1) || '0'}%</td>
                <td>{fuel.unit_count}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

export default StateDetailPage;
