import React, { useState, useEffect, useCallback } from 'react';
import Plot from 'react-plotly.js';
import api from '../api';
import './NetworkPage.css';

const IC_COLORS = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd', '#8c564b'];
const NEAR_LIMIT_MW = 1; // flow within this many MW of a limit gets an alert marker
const UNIT_INFERENCE_DAYS = 14; // lookback window for the unit-inference validation section

const fillFor = (hex, alpha) => {
  const n = parseInt(hex.slice(1), 16);
  return `rgba(${(n >> 16) & 255}, ${(n >> 8) & 255}, ${n & 255}, ${alpha})`;
};

// Distance from the flow to the nearer of the two feasible-envelope bounds; null when any input is missing.
const headroomOf = (flow, exportLimit, importLimit) =>
  flow == null || exportLimit == null || importLimit == null
    ? null
    : Math.min(exportLimit - flow, flow - importLimit);

// Builds the two Plotly traces (feasible-envelope shading + flow line + alert markers) for one interconnector.
function ribbonTraces(interconnectorId, intervals, axisNum, color) {
  const suffix = axisNum === 1 ? '' : String(axisNum);
  const x = intervals.map((d) => new Date(d.interval_datetime));
  const exportLimit = intervals.map((d) => d.exportlimit);
  const importLimit = intervals.map((d) => d.importlimit);
  const flow = intervals.map((d) => d.mwflow);
  const headroom = intervals.map((d, i) => headroomOf(flow[i], exportLimit[i], importLimit[i]));
  const customdata = intervals.map((d, i) => [exportLimit[i], importLimit[i], headroom[i], d.marginalvalue]);

  const alertIdx = headroom.map((h, i) => (h != null && h <= NEAR_LIMIT_MW ? i : -1)).filter((i) => i >= 0);

  const base = { xaxis: `x${suffix}`, yaxis: `y${suffix}` };

  return [
    {
      ...base,
      x, y: exportLimit,
      type: 'scatter', mode: 'lines',
      line: { width: 0 },
      hoverinfo: 'skip', showlegend: false,
      name: `${interconnectorId} export limit`,
    },
    {
      ...base,
      x, y: importLimit,
      type: 'scatter', mode: 'lines',
      line: { width: 0 },
      fill: 'tonexty',
      fillcolor: fillFor(color, 0.12),
      hoverinfo: 'skip', showlegend: false,
      name: `${interconnectorId} feasible envelope`,
    },
    {
      ...base,
      x, y: flow, customdata,
      type: 'scatter', mode: 'lines',
      line: { color, width: 2 },
      showlegend: false,
      name: interconnectorId,
      hovertemplate:
        '%{x|%a %d %b %H:%M}<br>Flow: %{y:.0f} MW' +
        '<br>Export limit: %{customdata[0]:.0f} MW · Import limit: %{customdata[1]:.0f} MW' +
        '<br>Headroom: %{customdata[2]:.0f} MW · Marginal: $%{customdata[3]:.2f}/MWh<extra></extra>',
    },
    {
      ...base,
      x: alertIdx.map((i) => x[i]),
      y: alertIdx.map((i) => flow[i]),
      customdata: alertIdx.map((i) => customdata[i]),
      type: 'scatter', mode: 'markers',
      marker: { color: '#d62728', size: 8, symbol: 'circle' },
      showlegend: false,
      name: `${interconnectorId} near limit`,
      hovertemplate:
        '%{x|%a %d %b %H:%M}<br>Flow: %{y:.0f} MW (near limit)' +
        '<br>Headroom: %{customdata[2]:.0f} MW<extra></extra>',
    },
  ];
}

// Assigns each interconnector a subplot in an independent-axes grid, x-axes linked for shared zoom/pan.
function buildRibbonFigure(ids, dataByIc, darkMode) {
  const cols = ids.length > 2 ? 2 : 1;
  const rows = Math.ceil(ids.length / cols);
  const axisColor = darkMode ? '#f5f5f5' : '#333';
  const gridColor = darkMode ? '#404040' : '#e0e0e0';

  const layout = {
    grid: { rows, columns: cols, pattern: 'independent' },
    plot_bgcolor: darkMode ? '#1a1a1a' : 'white',
    paper_bgcolor: darkMode ? '#1a1a1a' : 'white',
    font: { color: axisColor },
    margin: { l: 60, r: 20, t: 30, b: 40 },
    height: rows * 260,
    showlegend: false,
  };

  const data = ids.flatMap((id, idx) => {
    const axisNum = idx + 1;
    const suffix = axisNum === 1 ? '' : String(axisNum);
    layout[`yaxis${suffix}`] = {
      title: id, gridcolor: gridColor, color: axisColor, zeroline: true, zerolinecolor: gridColor,
    };
    layout[`xaxis${suffix}`] = {
      gridcolor: gridColor, color: axisColor, tickformat: '%a %H:%M',
      ...(axisNum > 1 ? { matches: 'x' } : {}),
    };
    return ribbonTraces(id, dataByIc[id], axisNum, IC_COLORS[idx % IC_COLORS.length]);
  });

  return { data, layout };
}

// Builds the heatmap z/customdata matrices over the union of interval timestamps across all constraints.
function buildHeatmapMatrix(constraints) {
  const allTimes = Array.from(
    new Set(constraints.flatMap((c) => c.intervals.map((iv) => iv.interval_datetime)))
  ).sort();
  const timeIndex = new Map(allTimes.map((t, i) => [t, i]));

  const z = constraints.map(() => allTimes.map(() => null));
  const customdata = constraints.map(() => allTimes.map(() => [null, null, null]));

  constraints.forEach((c, row) => {
    c.intervals.forEach((iv) => {
      const col = timeIndex.get(iv.interval_datetime);
      z[row][col] = Math.log10(1 + Math.abs(iv.marginalvalue ?? 0));
      customdata[row][col] = [iv.marginalvalue, iv.rhs, iv.violationdegree];
    });
  });

  return { x: allTimes.map((t) => new Date(t)), z, customdata };
}

// Builds the dual-line (realised solid, inferred dashed) figure for one DUID's paired series.
function buildUnitInferenceFigure(seriesData, darkMode) {
  const x = seriesData.map((d) => new Date(d.interval_datetime));
  const axisColor = darkMode ? '#f5f5f5' : '#333';
  const gridColor = darkMode ? '#404040' : '#e0e0e0';

  return {
    data: [
      {
        x, y: seriesData.map((d) => d.mw_realised),
        type: 'scatter', mode: 'lines',
        line: { color: '#1f77b4', width: 2 },
        name: 'Realised (SCADA)',
        hovertemplate: '%{x|%a %d %b %H:%M}<br>Realised: %{y:.0f} MW<extra></extra>',
      },
      {
        x, y: seriesData.map((d) => d.mw_inferred),
        type: 'scatter', mode: 'lines',
        line: { color: '#ff7f0e', width: 2, dash: 'dash' },
        name: 'Inferred (backsolved)',
        hovertemplate: '%{x|%a %d %b %H:%M}<br>Inferred: %{y:.0f} MW<extra></extra>',
      },
    ],
    layout: {
      plot_bgcolor: darkMode ? '#1a1a1a' : 'white',
      paper_bgcolor: darkMode ? '#1a1a1a' : 'white',
      font: { color: axisColor },
      margin: { l: 60, r: 20, t: 20, b: 40 },
      height: 320,
      legend: { orientation: 'h', y: -0.2 },
      xaxis: { gridcolor: gridColor, color: axisColor, tickformat: '%a %H:%M' },
      yaxis: { title: 'MW', gridcolor: gridColor, color: axisColor },
    },
  };
}

function NetworkPage({ darkMode }) {
  const [interconnectors, setInterconnectors] = useState({ run_datetime: null, data: {} });
  const [icLoading, setIcLoading] = useState(true);
  const [icError, setIcError] = useState(null);

  const [category, setCategory] = useState('all');
  const [constraints, setConstraints] = useState({ run_datetime: null, constraints: [] });
  const [constraintsLoading, setConstraintsLoading] = useState(true);
  const [constraintsError, setConstraintsError] = useState(null);

  const [units, setUnits] = useState({ units: [] });
  const [unitsLoading, setUnitsLoading] = useState(true);
  const [unitsError, setUnitsError] = useState(null);

  const [selectedDuid, setSelectedDuid] = useState(null);
  const [series, setSeries] = useState(null);
  const [seriesLoading, setSeriesLoading] = useState(false);
  const [seriesError, setSeriesError] = useState(null);

  const fetchInterconnectors = useCallback(async () => {
    setIcLoading(true);
    setIcError(null);
    try {
      const { data } = await api.get('/api/network/interconnectors');
      setInterconnectors(data);
    } catch (err) {
      setIcError(err.response?.data?.detail || 'Failed to load interconnector data.');
      setInterconnectors({ run_datetime: null, data: {} });
    }
    setIcLoading(false);
  }, []);

  const fetchConstraints = useCallback(async () => {
    setConstraintsLoading(true);
    setConstraintsError(null);
    try {
      const { data } = await api.get('/api/network/constraints', { params: { top: 25, category } });
      setConstraints(data);
    } catch (err) {
      setConstraintsError(err.response?.data?.detail || 'Failed to load binding constraints.');
      setConstraints({ run_datetime: null, constraints: [] });
    }
    setConstraintsLoading(false);
  }, [category]);

  const fetchUnits = useCallback(async () => {
    setUnitsLoading(true);
    setUnitsError(null);
    try {
      const { data } = await api.get('/api/network/unit-inference/units', { params: { days: UNIT_INFERENCE_DAYS } });
      setUnits(data);
    } catch (err) {
      setUnitsError(err.response?.data?.detail || 'Failed to load unit-inference DUIDs.');
      setUnits({ units: [] });
    }
    setUnitsLoading(false);
  }, []);

  const fetchSeries = useCallback(async (duid) => {
    setSeriesLoading(true);
    setSeriesError(null);
    try {
      const { data } = await api.get('/api/network/unit-inference/series', {
        params: { duid, days: UNIT_INFERENCE_DAYS },
      });
      setSeries(data);
    } catch (err) {
      setSeriesError(err.response?.data?.detail || `Failed to load unit-inference series for ${duid}.`);
      setSeries(null);
    }
    setSeriesLoading(false);
  }, []);

  useEffect(() => {
    fetchInterconnectors();
  }, [fetchInterconnectors]);

  useEffect(() => {
    fetchConstraints();
  }, [fetchConstraints]);

  useEffect(() => {
    fetchUnits();
  }, [fetchUnits]);

  useEffect(() => {
    if (units.units.length && !selectedDuid) {
      setSelectedDuid(units.units[0].duid);
    }
  }, [units, selectedDuid]);

  useEffect(() => {
    if (selectedDuid) {
      fetchSeries(selectedDuid);
    }
  }, [selectedDuid, fetchSeries]);

  const icIds = Object.keys(interconnectors.data);
  const ribbonFigure = icIds.length ? buildRibbonFigure(icIds, interconnectors.data, darkMode) : null;

  const heatmap = constraints.constraints.length ? buildHeatmapMatrix(constraints.constraints) : null;
  // "Other" constraints carry the raw id as their label — skip the redundant parenthetical.
  const yLabels = constraints.constraints.map(
    (c) => (c.label === c.constraintid ? c.constraintid : `${c.constraintid} (${c.label})`)
  );

  const unitInferenceFigure = series && series.data.length ? buildUnitInferenceFigure(series.data, darkMode) : null;

  return (
    <div className={`network-container ${darkMode ? 'dark' : 'light'}`}>
      <div className="network-section">
        <h2 className="network-section-title">Interconnector flows vs limits</h2>
        {interconnectors.run_datetime && (
          <div className="network-meta">Pre-dispatch run: {new Date(interconnectors.run_datetime).toLocaleString()}</div>
        )}

        {icLoading && (
          <div className="loading">
            <div className="spinner"></div>
            <p>Loading interconnector flows…</p>
          </div>
        )}

        {!icLoading && icError && <div className="network-error">{icError}</div>}

        {!icLoading && !icError && icIds.length === 0 && (
          <div className="network-empty">No interconnector data available yet.</div>
        )}

        {!icLoading && !icError && ribbonFigure && (
          <div className="chart-container">
            <Plot
              data={ribbonFigure.data}
              layout={ribbonFigure.layout}
              useResizeHandler
              style={{ width: '100%' }}
              config={{ displayModeBar: true, displaylogo: false }}
            />
          </div>
        )}
      </div>

      <div className="network-section">
        <h2 className="network-section-title">Binding constraints</h2>
        <div className="network-controls">
          <div className="network-toggle-group">
            {['all', 'network', 'fcas'].map((c) => (
              <button
                key={c}
                className={`network-toggle-btn ${category === c ? 'active' : ''}`}
                onClick={() => setCategory(c)}
              >
                {c === 'all' ? 'All' : c === 'network' ? 'Network' : 'FCAS'}
              </button>
            ))}
          </div>
          {constraints.run_datetime && (
            <span className="network-meta">
              Pre-dispatch run: {new Date(constraints.run_datetime).toLocaleString()}
            </span>
          )}
        </div>

        {constraintsLoading && (
          <div className="loading">
            <div className="spinner"></div>
            <p>Loading binding constraints…</p>
          </div>
        )}

        {!constraintsLoading && constraintsError && <div className="network-error">{constraintsError}</div>}

        {!constraintsLoading && !constraintsError && constraints.constraints.length === 0 && (
          <div className="network-empty">No binding constraints in the latest pre-dispatch run.</div>
        )}

        {!constraintsLoading && !constraintsError && heatmap && (
          <div className="chart-container">
            <Plot
              data={[
                {
                  x: heatmap.x,
                  y: yLabels,
                  z: heatmap.z,
                  customdata: heatmap.customdata,
                  type: 'heatmap',
                  colorscale: 'YlOrRd',
                  hovertemplate:
                    '%{y}<br>%{x|%a %d %b %H:%M}' +
                    '<br>Marginal: $%{customdata[0]:.2f}/MWh<br>RHS: %{customdata[1]:.1f}' +
                    '<br>Violation: %{customdata[2]:.2f}<extra></extra>',
                  colorbar: { title: 'log₁₀(1+|$/MWh|)' },
                },
              ]}
              layout={{
                plot_bgcolor: darkMode ? '#1a1a1a' : 'white',
                paper_bgcolor: darkMode ? '#1a1a1a' : 'white',
                font: { color: darkMode ? '#f5f5f5' : '#333' },
                margin: { l: 220, r: 30, t: 20, b: 60 },
                height: Math.max(300, yLabels.length * 28 + 100),
                xaxis: { tickformat: '%a %H:%M', gridcolor: darkMode ? '#404040' : '#e0e0e0' },
                yaxis: { automargin: true },
              }}
              useResizeHandler
              style={{ width: '100%' }}
              config={{ displayModeBar: true, displaylogo: false }}
            />
          </div>
        )}
      </div>

      <div className="network-section">
        <h2 className="network-section-title">Unit inference (validation)</h2>

        {unitsLoading && (
          <div className="loading">
            <div className="spinner"></div>
            <p>Loading unit-inference DUIDs…</p>
          </div>
        )}

        {!unitsLoading && unitsError && <div className="network-error">{unitsError}</div>}

        {!unitsLoading && !unitsError && units.units.length === 0 && (
          <div className="network-empty">No stored unit-inference rows yet.</div>
        )}

        {!unitsLoading && !unitsError && units.units.length > 0 && (
          <div className="unit-inference-layout">
            <div className="unit-inference-picker">
              {units.units.map((u) => (
                <button
                  key={u.duid}
                  className={
                    `unit-picker-btn ${u.tracking ? 'tracking' : 'non-tracking'} ` +
                    (selectedDuid === u.duid ? 'active' : '')
                  }
                  onClick={() => setSelectedDuid(u.duid)}
                >
                  {u.duid} · corr {u.observed_corr != null ? u.observed_corr.toFixed(2) : 'n/a'} · n={u.n}
                </button>
              ))}
            </div>

            <div className="unit-inference-detail">
              {seriesLoading && (
                <div className="loading">
                  <div className="spinner"></div>
                  <p>Loading series for {selectedDuid}…</p>
                </div>
              )}

              {!seriesLoading && seriesError && <div className="network-error">{seriesError}</div>}

              {!seriesLoading && !seriesError && series && series.data.length === 0 && (
                <div className="network-empty">No paired inferred/realised data for {selectedDuid} yet.</div>
              )}

              {!seriesLoading && !seriesError && unitInferenceFigure && (
                <>
                  <div className="unit-stats-chip">
                    <span>corr {series.stats.corr != null ? series.stats.corr.toFixed(2) : 'n/a'}</span>
                    <span>MAE {series.stats.mae != null ? series.stats.mae.toFixed(1) : 'n/a'} MW</span>
                    <span>n={series.stats.n}</span>
                    <span>{series.stats.quality || 'n/a'}</span>
                  </div>
                  <div className="chart-container">
                    <Plot
                      data={unitInferenceFigure.data}
                      layout={unitInferenceFigure.layout}
                      useResizeHandler
                      style={{ width: '100%' }}
                      config={{ displayModeBar: true, displaylogo: false }}
                    />
                  </div>
                  {series.stats.median_n_equations != null && (
                    <div className="network-meta">
                      Median equations per solve: {series.stats.median_n_equations.toFixed(0)}
                    </div>
                  )}
                </>
              )}
            </div>
          </div>
        )}
      </div>
    </div>
  );
}

export default NetworkPage;
