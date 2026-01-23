import React, { useState, useEffect, useCallback } from 'react';
import Plot from 'react-plotly.js';
import axios from 'axios';
import './PASAPage.css';

const REGION_NAMES = {
  'NSW1': 'New South Wales',
  'VIC1': 'Victoria',
  'QLD1': 'Queensland',
  'SA1': 'South Australia',
  'TAS1': 'Tasmania'
};

const REGION_COLORS = {
  'NSW1': '#1f77b4',
  'VIC1': '#ff7f0e',
  'QLD1': '#2ca02c',
  'SA1': '#d62728',
  'TAS1': '#9467bd'
};

const LOR_COLORS = {
  0: '#4caf50',  // Green - No LOR
  1: '#ff9800',  // Orange - LOR1
  2: '#f44336',  // Red - LOR2
  3: '#8b0000'   // Dark Red - LOR3
};

const LOR_LABELS = {
  0: 'No LOR',
  1: 'LOR1 - Low Reserve',
  2: 'LOR2 - Lack of Reserve',
  3: 'LOR3 - Load Shedding Imminent'
};

function PASAPage({ region, darkMode, onBack }) {
  const [pdpasaData, setPdpasaData] = useState([]);
  const [stpasaData, setStpasaData] = useState([]);
  const [pdpasaRunTime, setPdpasaRunTime] = useState(null);
  const [stpasaRunTime, setStpasaRunTime] = useState(null);
  const [loading, setLoading] = useState(true);
  const [lastUpdated, setLastUpdated] = useState('');
  const [activeTab, setActiveTab] = useState('pdpasa');

  // Convert region format (NSW -> NSW1)
  const regionId = region.length === 3 ? region + '1' : region;

  const fetchData = useCallback(async () => {
    try {
      const [pdpasaResponse, stpasaResponse] = await Promise.all([
        axios.get(`/api/pasa/pdpasa/${regionId}`),
        axios.get(`/api/pasa/stpasa/${regionId}`)
      ]);

      setPdpasaData(pdpasaResponse.data.data || []);
      setPdpasaRunTime(pdpasaResponse.data.run_datetime);
      setStpasaData(stpasaResponse.data.data || []);
      setStpasaRunTime(stpasaResponse.data.run_datetime);
      setLastUpdated(new Date().toLocaleTimeString());
      setLoading(false);
    } catch (error) {
      console.error('Error fetching PASA data:', error);
      setPdpasaData([]);
      setStpasaData([]);
      setLastUpdated(new Date().toLocaleTimeString());
      setLoading(false);
    }
  }, [regionId]);

  useEffect(() => {
    fetchData();
    const interval = setInterval(fetchData, 60000);
    return () => clearInterval(interval);
  }, [fetchData]);

  const getMaxLOR = (data) => {
    if (!data || data.length === 0) return 0;
    return Math.max(...data.map(d => d.lorcondition || 0));
  };

  const getMinSurplus = (data) => {
    if (!data || data.length === 0) return null;
    const surpluses = data.filter(d => d.surplusreserve != null).map(d => d.surplusreserve);
    return surpluses.length > 0 ? Math.min(...surpluses) : null;
  };

  const getLORPeriods = (data) => {
    if (!data || data.length === 0) return 0;
    return data.filter(d => (d.lorcondition || 0) > 0).length;
  };

  const createDemandCapacityChart = (data, title) => {
    if (!data || data.length === 0) return { data: [], layout: {} };

    const traces = [
      {
        x: data.map(d => new Date(d.interval_datetime)),
        y: data.map(d => d.demand50),
        type: 'scatter',
        mode: 'lines',
        name: 'Demand (50% POE)',
        line: { color: REGION_COLORS[regionId], width: 2 }
      },
      {
        x: data.map(d => new Date(d.interval_datetime)),
        y: data.map(d => d.aggregatecapacityavailable),
        type: 'scatter',
        mode: 'lines',
        name: 'Available Capacity',
        line: { color: '#4caf50', width: 2 }
      }
    ];

    const layout = {
      title: { text: title, font: { size: 16, color: darkMode ? '#e5e7eb' : '#374151' } },
      xaxis: {
        gridcolor: darkMode ? '#374151' : '#f3f4f6',
        color: darkMode ? '#9ca3af' : '#6b7280',
        tickformat: '%d %b %H:%M'
      },
      yaxis: {
        title: { text: 'MW', font: { size: 11 } },
        gridcolor: darkMode ? '#374151' : '#f3f4f6',
        color: darkMode ? '#9ca3af' : '#6b7280'
      },
      plot_bgcolor: 'transparent',
      paper_bgcolor: 'transparent',
      font: { color: darkMode ? '#e5e7eb' : '#374151' },
      margin: { l: 60, r: 30, t: 40, b: 50 },
      showlegend: true,
      legend: { orientation: 'h', x: 0.5, xanchor: 'center', y: -0.15 },
      hovermode: 'x unified'
    };

    return { data: traces, layout };
  };

  const createReserveChart = (data, title) => {
    if (!data || data.length === 0) return { data: [], layout: {} };

    const traces = [
      {
        x: data.map(d => new Date(d.interval_datetime)),
        y: data.map(d => d.surplusreserve),
        type: 'scatter',
        mode: 'lines',
        name: 'Surplus Reserve',
        line: { color: '#9c27b0', width: 2 },
        fill: 'tozeroy',
        fillcolor: 'rgba(156, 39, 176, 0.1)'
      },
      {
        x: data.map(d => new Date(d.interval_datetime)),
        y: data.map(d => d.calculatedlor1level),
        type: 'scatter',
        mode: 'lines',
        name: 'LOR1 Threshold',
        line: { color: '#ff9800', width: 1, dash: 'dot' }
      },
      {
        x: data.map(d => new Date(d.interval_datetime)),
        y: data.map(d => d.calculatedlor2level),
        type: 'scatter',
        mode: 'lines',
        name: 'LOR2 Threshold',
        line: { color: '#f44336', width: 1, dash: 'dot' }
      }
    ];

    const layout = {
      title: { text: title, font: { size: 16, color: darkMode ? '#e5e7eb' : '#374151' } },
      xaxis: {
        gridcolor: darkMode ? '#374151' : '#f3f4f6',
        color: darkMode ? '#9ca3af' : '#6b7280',
        tickformat: '%d %b %H:%M'
      },
      yaxis: {
        title: { text: 'MW', font: { size: 11 } },
        gridcolor: darkMode ? '#374151' : '#f3f4f6',
        color: darkMode ? '#9ca3af' : '#6b7280'
      },
      plot_bgcolor: 'transparent',
      paper_bgcolor: 'transparent',
      font: { color: darkMode ? '#e5e7eb' : '#374151' },
      margin: { l: 60, r: 30, t: 40, b: 50 },
      showlegend: true,
      legend: { orientation: 'h', x: 0.5, xanchor: 'center', y: -0.15 },
      hovermode: 'x unified'
    };

    return { data: traces, layout };
  };

  const createLORChart = (data, title) => {
    if (!data || data.length === 0) return { data: [], layout: {} };

    const lorColors = data.map(d => LOR_COLORS[d.lorcondition] || LOR_COLORS[0]);

    const traces = [
      {
        x: data.map(d => new Date(d.interval_datetime)),
        y: data.map(d => d.lorcondition || 0),
        type: 'bar',
        name: 'LOR Condition',
        marker: { color: lorColors },
        text: data.map(d => d.lorcondition > 0 ? `LOR${d.lorcondition}` : ''),
        textposition: 'outside'
      }
    ];

    const layout = {
      title: { text: title, font: { size: 16, color: darkMode ? '#e5e7eb' : '#374151' } },
      xaxis: {
        gridcolor: darkMode ? '#374151' : '#f3f4f6',
        color: darkMode ? '#9ca3af' : '#6b7280',
        tickformat: '%d %b %H:%M'
      },
      yaxis: {
        title: { text: 'LOR Level', font: { size: 11 } },
        gridcolor: darkMode ? '#374151' : '#f3f4f6',
        color: darkMode ? '#9ca3af' : '#6b7280',
        tickvals: [0, 1, 2, 3],
        range: [-0.5, 3.5]
      },
      plot_bgcolor: 'transparent',
      paper_bgcolor: 'transparent',
      font: { color: darkMode ? '#e5e7eb' : '#374151' },
      margin: { l: 60, r: 30, t: 40, b: 50 },
      showlegend: false,
      hovermode: 'x unified'
    };

    return { data: traces, layout };
  };

  const renderSummaryCards = (data, runTime, type) => {
    const maxLOR = getMaxLOR(data);
    const minSurplus = getMinSurplus(data);
    const lorPeriods = getLORPeriods(data);

    return (
      <div className="pasa-summary-cards">
        <div className={`pasa-summary-card lor-${maxLOR}`}>
          <div className="card-label">Max LOR Condition</div>
          <div className="card-value" style={{ color: LOR_COLORS[maxLOR] }}>
            {LOR_LABELS[maxLOR]}
          </div>
        </div>
        <div className="pasa-summary-card">
          <div className="card-label">LOR Periods</div>
          <div className="card-value">
            {lorPeriods}
            <span className="card-unit">intervals</span>
          </div>
        </div>
        <div className="pasa-summary-card">
          <div className="card-label">Min Surplus Reserve</div>
          <div className="card-value">
            {minSurplus != null ? minSurplus.toFixed(0) : 'N/A'}
            <span className="card-unit">MW</span>
          </div>
        </div>
        <div className="pasa-summary-card">
          <div className="card-label">Forecast Periods</div>
          <div className="card-value">
            {data.length}
            <span className="card-unit">intervals</span>
          </div>
        </div>
        {runTime && (
          <div className="pasa-summary-card">
            <div className="card-label">Run Time</div>
            <div className="card-value small">
              {new Date(runTime).toLocaleString()}
            </div>
          </div>
        )}
      </div>
    );
  };

  const renderPASASection = (data, runTime, type, label) => {
    const demandChart = createDemandCapacityChart(data, `${label} - Demand vs Capacity`);
    const reserveChart = createReserveChart(data, `${label} - Reserve Margin`);
    const lorChart = createLORChart(data, `${label} - LOR Conditions`);

    return (
      <div className="pasa-section">
        {renderSummaryCards(data, runTime, type)}

        {data.length > 0 ? (
          <>
            <div className="chart-wrapper">
              <Plot
                data={demandChart.data}
                layout={demandChart.layout}
                style={{ width: '100%', height: '300px' }}
                config={{ displayModeBar: 'hover', displaylogo: false }}
              />
            </div>

            <div className="chart-wrapper">
              <Plot
                data={reserveChart.data}
                layout={reserveChart.layout}
                style={{ width: '100%', height: '300px' }}
                config={{ displayModeBar: 'hover', displaylogo: false }}
              />
            </div>

            <div className="chart-wrapper">
              <Plot
                data={lorChart.data}
                layout={lorChart.layout}
                style={{ width: '100%', height: '250px' }}
                config={{ displayModeBar: 'hover', displaylogo: false }}
              />
            </div>
          </>
        ) : (
          <div className="no-data-message">
            <p>No {label} data available for {REGION_NAMES[regionId]}.</p>
            <p>Data will appear after the next ingestion cycle.</p>
          </div>
        )}
      </div>
    );
  };

  if (loading) {
    return (
      <div className={`pasa-container ${darkMode ? 'dark' : 'light'}`}>
        <div className="loading">
          <div className="spinner"></div>
          <p>Loading PASA data for {REGION_NAMES[regionId]}...</p>
        </div>
      </div>
    );
  }

  return (
    <div className={`pasa-container ${darkMode ? 'dark' : 'light'}`}>
      <div className="pasa-header">
        <button className="back-button" onClick={onBack}>
          Back to State Details
        </button>
        <h1 className="pasa-title" style={{ color: REGION_COLORS[regionId] }}>
          {REGION_NAMES[regionId]} PASA Forecast
        </h1>
        <div className="last-updated">
          Last Updated: {lastUpdated}
        </div>
      </div>

      <div className="pasa-tabs">
        <button
          className={`pasa-tab ${activeTab === 'pdpasa' ? 'active' : ''}`}
          onClick={() => setActiveTab('pdpasa')}
        >
          PDPASA (Short-Term)
        </button>
        <button
          className={`pasa-tab ${activeTab === 'stpasa' ? 'active' : ''}`}
          onClick={() => setActiveTab('stpasa')}
        >
          STPASA (Medium-Term)
        </button>
      </div>

      <div className="pasa-content">
        {activeTab === 'pdpasa' && renderPASASection(pdpasaData, pdpasaRunTime, 'pdpasa', 'PDPASA')}
        {activeTab === 'stpasa' && renderPASASection(stpasaData, stpasaRunTime, 'stpasa', 'STPASA')}
      </div>
    </div>
  );
}

export default PASAPage;
