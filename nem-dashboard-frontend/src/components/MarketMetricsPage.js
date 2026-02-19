import React, { useState, useEffect, useCallback, useMemo } from 'react';
import Plot from 'react-plotly.js';
import api from '../api';
import './MarketMetricsPage.css';

const REGIONS = ['NSW', 'VIC', 'QLD', 'SA', 'TAS'];

const REGION_COLORS = {
  'NSW': '#1f77b4',
  'VIC': '#ff7f0e',
  'QLD': '#2ca02c',
  'SA': '#d62728',
  'TAS': '#9467bd'
};

const REGION_NAMES = {
  'NSW': 'New South Wales',
  'VIC': 'Victoria',
  'QLD': 'Queensland',
  'SA': 'South Australia',
  'TAS': 'Tasmania'
};

const CAPTURE_FUELS = [
  { key: 'capture_solar', priceKey: 'capture_price_solar', label: 'Solar' },
  { key: 'capture_wind', priceKey: 'capture_price_wind', label: 'Wind' },
  { key: 'capture_battery', priceKey: 'capture_price_battery', label: 'Battery' },
  { key: 'capture_hydro', priceKey: 'capture_price_hydro', label: 'Hydro' },
  { key: 'capture_gas', priceKey: 'capture_price_gas', label: 'Gas' },
  { key: 'capture_coal', priceKey: 'capture_price_coal', label: 'Coal' }
];

const TB_TYPES = [
  { key: 'tb2_spread', label: 'TB2 (2h)' },
  { key: 'tb4_spread', label: 'TB4 (4h)' },
  { key: 'tb8_spread', label: 'TB8 (8h)' }
];

const PERIODS = ['24h', '7d', '30d', '365d'];

const applyRollingAverage = (values, window) => {
  if (window <= 1) return values;
  return values.map((_, i) => {
    let sum = 0;
    let count = 0;
    for (let j = Math.max(0, i - window + 1); j <= i; j++) {
      if (values[j] != null) {
        sum += values[j];
        count++;
      }
    }
    return count > 0 ? sum / count : null;
  });
};

const formatCaptureRate = (value) => {
  if (value == null) return '—';
  return `${(value * 100).toFixed(1)}%`;
};

const formatCapturePrice = (value) => {
  if (value == null) return '—';
  return `$${value.toFixed(1)}`;
};

const formatSpread = (value) => {
  if (value == null) return '—';
  return `$${value.toFixed(0)}`;
};

const getCaptureClass = (value) => {
  if (value == null) return '';
  const pct = value * 100;
  if (pct >= 110) return 'cell-high';
  if (pct >= 100) return 'cell-above';
  if (pct >= 90) return 'cell-mid';
  return 'cell-low';
};

const getSpreadClass = (value) => {
  if (value == null) return '';
  if (value >= 100) return 'cell-high';
  if (value >= 50) return 'cell-above';
  if (value >= 20) return 'cell-mid';
  return 'cell-low';
};

function MarketMetricsPage({ darkMode }) {
  const [selectedRegion, setSelectedRegion] = useState('NSW');
  const [summaryData, setSummaryData] = useState(null);
  const [initialLoad, setInitialLoad] = useState(true);

  // Detail view state
  const [detailMetric, setDetailMetric] = useState(null); // null = summary view
  const [detailData, setDetailData] = useState({});
  const [detailLoading, setDetailLoading] = useState(false);
  const [visibleDurations, setVisibleDurations] = useState({ daily: true, '7d': true, '30d': true });
  const [captureUnit, setCaptureUnit] = useState('pct'); // 'pct' or 'dollar'

  // Fetch summary data — only show spinner on first load
  const fetchSummary = useCallback(async () => {
    try {
      const res = await api.get(`/api/metrics/summary?region=${selectedRegion}`);
      setSummaryData(res.data.periods);
    } catch (error) {
      console.error('Error fetching summary:', error);
      setSummaryData(null);
    } finally {
      setInitialLoad(false);
    }
  }, [selectedRegion]);

  useEffect(() => {
    fetchSummary();
  }, [fetchSummary]);

  // Fetch detail data (all regions, 365 days)
  const fetchDetail = useCallback(async (metricKey) => {
    setDetailLoading(true);
    try {
      const now = new Date();
      const end = new Date(now);
      end.setDate(end.getDate() - 1);
      const start = new Date(end);
      start.setDate(start.getDate() - 365);

      const fmt = (d) => `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}T00:00:00`;

      const responses = await Promise.all(
        REGIONS.map(region =>
          api.get(`/api/metrics/daily?region=${region}&start_date=${fmt(start)}&end_date=${fmt(end)}`)
            .then(res => ({ region, data: res.data.data || [] }))
            .catch(() => ({ region, data: [] }))
        )
      );

      const newData = {};
      responses.forEach(({ region, data }) => {
        newData[region] = data;
      });
      setDetailData(newData);
    } catch (error) {
      console.error('Error fetching detail:', error);
      setDetailData({});
    } finally {
      setDetailLoading(false);
    }
  }, []);

  const openDetail = (metricKey) => {
    setDetailMetric(metricKey);
    fetchDetail(metricKey);
  };

  const closeDetail = () => {
    setDetailMetric(null);
    setDetailData({});
  };

  // Determine which fuels have data
  const activeFuels = useMemo(() => {
    if (!summaryData) return [];
    return CAPTURE_FUELS.filter(fuel =>
      PERIODS.some(p => summaryData[p] && summaryData[p][fuel.key] != null)
    );
  }, [summaryData]);

  const chartFont = {
    color: darkMode ? '#e5e7eb' : '#374151',
    family: '-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif'
  };

  const plotConfig = {
    displayModeBar: 'hover',
    displaylogo: false,
    modeBarButtonsToRemove: ['pan2d', 'lasso2d', 'select2d', 'autoScale2d'],
    scrollZoom: false
  };

  // Get label and formatting for the detail metric
  const getMetricInfo = (key) => {
    const fuel = CAPTURE_FUELS.find(f => f.key === key);
    if (fuel) return { label: `${fuel.label} Capture Rate`, type: 'capture', suffix: '%', multiplier: 100 };
    const fuelPrice = CAPTURE_FUELS.find(f => f.priceKey === key);
    if (fuelPrice) return { label: `${fuelPrice.label} Capture Price`, type: 'capture_price', prefix: '$', multiplier: 1 };
    const tb = TB_TYPES.find(t => t.key === key);
    if (tb) return { label: `${tb.label} Spread`, type: 'spread', prefix: '$', multiplier: 1 };
    return { label: key, type: 'unknown', multiplier: 1 };
  };

  const createDetailTraces = () => {
    if (!detailMetric) return { traces: [], yRange: null };
    const info = getMetricInfo(detailMetric);
    const traces = [];
    const allAvg7Values = [];

    REGIONS.forEach(region => {
      const data = detailData[region];
      if (!data || data.length === 0) return;

      const hasValues = data.some(d => d[detailMetric] != null);
      if (!hasValues) return;

      const dates = data.map(d => d.metric_date);
      const rawY = data.map(d => d[detailMetric] != null ? d[detailMetric] * info.multiplier : null);
      const avg7 = applyRollingAverage(rawY, 7);
      const avg30 = applyRollingAverage(rawY, 30);

      avg7.forEach(v => { if (v != null) allAvg7Values.push(v); });

      const hoverSuffix = info.type === 'capture' ? '%' : '';
      const hoverPrefix = (info.type === 'spread' || info.type === 'capture_price') ? '$' : '';

      // Determine which duration is the "primary" one for legend/hover
      const primaryDuration = visibleDurations['30d'] ? '30d' : visibleDurations['7d'] ? '7d' : 'daily';

      // Daily line (thin, low opacity)
      if (visibleDurations.daily) {
        traces.push({
          x: dates, y: rawY,
          type: 'scatter', mode: 'lines',
          name: `${region}${primaryDuration === 'daily' ? '' : ' daily'}`,
          legendgroup: region,
          showlegend: primaryDuration === 'daily',
          connectgaps: false,
          line: { color: REGION_COLORS[region], width: primaryDuration === 'daily' ? 1.5 : 1 },
          opacity: primaryDuration === 'daily' ? 0.8 : 0.25,
          hoverinfo: primaryDuration === 'daily' ? undefined : 'skip',
          ...(primaryDuration === 'daily' ? {
            hovertemplate: `<b>${REGION_NAMES[region]}</b> (daily)<br>%{x|%d %b %Y}<br>${hoverPrefix}%{y:.1f}${hoverSuffix}<extra></extra>`
          } : {})
        });
      }

      // 7-day rolling (dashed)
      if (visibleDurations['7d']) {
        traces.push({
          x: dates, y: avg7,
          type: 'scatter', mode: 'lines',
          name: `${region}${primaryDuration === '7d' ? '' : ' 7d'}`,
          legendgroup: region,
          showlegend: primaryDuration === '7d',
          connectgaps: false,
          line: { color: REGION_COLORS[region], width: primaryDuration === '7d' ? 2.5 : 1.5, dash: primaryDuration === '7d' ? undefined : 'dash' },
          hoverinfo: primaryDuration === '7d' ? undefined : 'skip',
          ...(primaryDuration === '7d' ? {
            hovertemplate: `<b>${REGION_NAMES[region]}</b> (7d avg)<br>%{x|%d %b %Y}<br>${hoverPrefix}%{y:.1f}${hoverSuffix}<extra></extra>`
          } : {})
        });
      }

      // 30-day rolling (solid, thick — main line)
      if (visibleDurations['30d']) {
        traces.push({
          x: dates, y: avg30,
          type: 'scatter', mode: 'lines',
          name: region,
          legendgroup: region,
          showlegend: true,
          connectgaps: false,
          line: { color: REGION_COLORS[region], width: 2.5 },
          hovertemplate: `<b>${REGION_NAMES[region]}</b> (30d avg)<br>%{x|%d %b %Y}<br>${hoverPrefix}%{y:.1f}${hoverSuffix}<extra></extra>`
        });
      }
    });

    // 100% baseline for capture rates
    if (info.type === 'capture' && traces.length > 0) {
      const allDates = [];
      REGIONS.forEach(r => {
        if (detailData[r]) {
          detailData[r].forEach(d => {
            if (!allDates.includes(d.metric_date)) allDates.push(d.metric_date);
          });
        }
      });
      allDates.sort();
      traces.push({
        x: allDates, y: allDates.map(() => 100),
        type: 'scatter', mode: 'lines',
        name: '100%',
        line: { color: darkMode ? '#4b5563' : '#d1d5db', width: 1, dash: 'dot' },
        hoverinfo: 'skip',
        showlegend: false
      });
    }

    // Compute y-axis range from 7-day rolling values (excludes extreme daily spikes)
    let yRange = null;
    if (allAvg7Values.length > 0) {
      const sorted = [...allAvg7Values].sort((a, b) => a - b);
      const p5 = sorted[Math.floor(sorted.length * 0.02)];
      const p95 = sorted[Math.floor(sorted.length * 0.98)];
      const span = p95 - p5;
      const padding = span * 0.15;
      yRange = [Math.max(0, p5 - padding), p95 + padding];
    }

    return { traces, yRange };
  };

  const createDetailLayout = (yRange) => {
    if (!detailMetric) return {};
    const info = getMetricInfo(detailMetric);

    return {
      xaxis: {
        gridcolor: darkMode ? '#374151' : '#f3f4f6',
        color: darkMode ? '#9ca3af' : '#6b7280',
        tickformat: '%b %Y',
        tickfont: { size: 11 },
        showline: true,
        zeroline: false
      },
      yaxis: {
        gridcolor: darkMode ? '#374151' : '#f3f4f6',
        color: darkMode ? '#9ca3af' : '#6b7280',
        ticksuffix: info.type === 'capture' ? '%' : '',
        tickprefix: (info.type === 'spread' || info.type === 'capture_price') ? '$' : '',
        tickfont: { size: 11 },
        zeroline: false,
        ...(yRange ? { range: yRange } : {})
      },
      plot_bgcolor: 'transparent',
      paper_bgcolor: 'transparent',
      font: chartFont,
      margin: { l: 60, r: 20, t: 20, b: 40 },
      showlegend: true,
      legend: {
        orientation: 'h', x: 0.5, xanchor: 'center', y: 1.05, yanchor: 'bottom',
        bgcolor: 'transparent', font: { size: 12 }
      },
      hovermode: 'x unified',
      hoverlabel: {
        bgcolor: darkMode ? '#1f2937' : 'white',
        bordercolor: darkMode ? '#374151' : '#e5e7eb',
        font: { size: 12, family: chartFont.family }
      }
    };
  };

  // ---- Render ----

  if (detailMetric) {
    const info = getMetricInfo(detailMetric);
    const isCaptureType = info.type === 'capture' || info.type === 'capture_price';
    // Find the paired key for toggling between rate and price
    const pairedFuel = isCaptureType
      ? CAPTURE_FUELS.find(f => f.key === detailMetric || f.priceKey === detailMetric)
      : null;

    return (
      <div className={`metrics-container ${darkMode ? 'dark' : 'light'}`}>
        <div className="detail-header">
          <button className="back-button" onClick={closeDetail}>Back to Summary</button>
          <h1 className="metrics-title">{info.label}</h1>
          <p className="metrics-subtitle">All regions — rolling averages across NEM</p>
        </div>

        <div className="detail-controls">
          <div className="duration-toggles">
            {[
              { key: 'daily', label: 'Daily' },
              { key: '7d', label: '7d Avg' },
              { key: '30d', label: '30d Avg' }
            ].map(d => (
              <button
                key={d.key}
                className={`duration-toggle ${visibleDurations[d.key] ? 'active' : ''}`}
                onClick={() => setVisibleDurations(prev => ({ ...prev, [d.key]: !prev[d.key] }))}
              >
                {d.label}
              </button>
            ))}
          </div>

          {pairedFuel && (
            <div className="unit-toggles">
              <button
                className={`unit-toggle ${info.type === 'capture' ? 'active' : ''}`}
                onClick={() => setDetailMetric(pairedFuel.key)}
              >% of TWA</button>
              <button
                className={`unit-toggle ${info.type === 'capture_price' ? 'active' : ''}`}
                onClick={() => setDetailMetric(pairedFuel.priceKey)}
              >$/MWh</button>
            </div>
          )}
        </div>

        {detailLoading ? (
          <div className="metrics-loading">
            <div className="spinner"></div>
            <p>Loading history...</p>
          </div>
        ) : (
          <div className="metrics-chart-section">
            {(() => {
              const { traces, yRange } = createDetailTraces();
              return (
                <Plot
                  data={traces}
                  layout={createDetailLayout(yRange)}
                  style={{ width: '100%', height: '500px' }}
                  config={plotConfig}
                />
              );
            })()}
          </div>
        )}
      </div>
    );
  }

  return (
    <div className={`metrics-container ${darkMode ? 'dark' : 'light'}`}>
      <div className="metrics-header">
        <h1 className="metrics-title">Market Metrics</h1>
        <p className="metrics-subtitle">Capture rates and top-bottom price spreads</p>
      </div>

      {/* Region tabs */}
      <div className="region-tabs">
        {REGIONS.map(region => (
          <button
            key={region}
            className={`region-tab ${selectedRegion === region ? 'active' : ''}`}
            onClick={() => setSelectedRegion(region)}
          >
            {region}
          </button>
        ))}
      </div>

      {initialLoad && (
        <div className="metrics-loading">
          <div className="spinner"></div>
          <p>Loading metrics...</p>
        </div>
      )}

      {!initialLoad && !summaryData && (
        <div className="metrics-empty">
          <p>No metrics data available.</p>
        </div>
      )}

      {!initialLoad && summaryData && (
        <>
          {/* Capture Rates Table */}
          <div className="metrics-table-section">
            <div className="section-header">
              <h2 className="section-title">Capture {captureUnit === 'pct' ? 'Rates' : 'Prices'}</h2>
              <div className="unit-toggles">
                <button
                  className={`unit-toggle ${captureUnit === 'pct' ? 'active' : ''}`}
                  onClick={() => setCaptureUnit('pct')}
                >% of TWA</button>
                <button
                  className={`unit-toggle ${captureUnit === 'dollar' ? 'active' : ''}`}
                  onClick={() => setCaptureUnit('dollar')}
                >$/MWh</button>
              </div>
            </div>
            <table className="metrics-table">
              <thead>
                <tr>
                  <th>Fuel</th>
                  {PERIODS.map(p => <th key={p}>{p}</th>)}
                </tr>
              </thead>
              <tbody>
                {activeFuels.map(fuel => (
                  <tr key={fuel.key} className="clickable" onClick={() => openDetail(captureUnit === 'pct' ? fuel.key : fuel.priceKey)}>
                    <td className="fuel-label">{fuel.label}</td>
                    {PERIODS.map(p => {
                      if (captureUnit === 'pct') {
                        const val = summaryData[p]?.[fuel.key];
                        return (
                          <td key={p} className={getCaptureClass(val)}>
                            {formatCaptureRate(val)}
                          </td>
                        );
                      } else {
                        const val = summaryData[p]?.[fuel.priceKey];
                        return (
                          <td key={p}>
                            {formatCapturePrice(val)}
                          </td>
                        );
                      }
                    })}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          {/* TB Spreads Table */}
          <div className="metrics-table-section">
            <h2 className="section-title">Price Shape (TB Spreads)</h2>
            <table className="metrics-table">
              <thead>
                <tr>
                  <th>Spread</th>
                  {PERIODS.map(p => <th key={p}>{p}</th>)}
                </tr>
              </thead>
              <tbody>
                {TB_TYPES.map(tb => (
                  <tr key={tb.key} className="clickable" onClick={() => openDetail(tb.key)}>
                    <td className="fuel-label">{tb.label}</td>
                    {PERIODS.map(p => {
                      const val = summaryData[p]?.[tb.key];
                      return (
                        <td key={p} className={getSpreadClass(val)}>
                          {formatSpread(val)}
                        </td>
                      );
                    })}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </>
      )}
    </div>
  );
}

export default MarketMetricsPage;
